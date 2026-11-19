using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class IncomingMessageContext : IBehaviorContext
    {
        internal IncomingMessageContext(object message, IMessageHandlerContext handlerContext,
            Type messageType, string contractName, int immediateRetries, int delayedRetries)
        {
            Message = message;
            MessageType = messageType;
            HandlerContext = handlerContext;
            ContractName = contractName;
            ImmediateRetries = immediateRetries;
            DelayedRetries = delayedRetries;
        }

        public object Message { get; private set; }
        internal void UpdateMessageInstance(object newInstance)
        {
            if (newInstance == null) throw new ArgumentNullException(nameof(newInstance));
            if (!MessageType.IsInstanceOfType(newInstance))
                throw new ArgumentException("The replacement must implement the incoming message contract.", nameof(newInstance));
            Message = newInstance;
        }
        public Type MessageType { get; }
        public string ContractName { get; }
        public int ImmediateRetries { get; }
        public int DelayedRetries { get; }
        public bool IsSagaTimeout => Message is SagaTimeoutMessage;
        public bool IsDeferred => IsSagaTimeout || DelayedRetries > 0 ||
            (HandlerContext.Headers.TryGetValue(MessageHeaders.Deferred, out var value) && value is bool deferred && deferred);
        public IMessageHandlerContext HandlerContext { get; }
        public ContextBag Extensions => HandlerContext.Extensions;
        internal ExceptionDispatchInfo DownstreamFailure { get; set; }
        internal bool MessageHandled { get; set; }
        public IDictionary<string, object> Items { get; } = new Dictionary<string, object>();
    }

    public sealed class IncomingPipeline
    {
        readonly List<Func<IncomingMessageContext, Func<Task>, Task>> behaviors =
            new List<Func<IncomingMessageContext, Func<Task>, Task>>();
        readonly List<Func<IncomingMessageContext, IPersistenceSession, Task>> beforeCommit =
            new List<Func<IncomingMessageContext, IPersistenceSession, Task>>();

        public void Use(Func<IncomingMessageContext, Func<Task>, Task> behavior) =>
            behaviors.Add(behavior ?? throw new ArgumentNullException(nameof(behavior)));

        internal void Replace(Func<IncomingMessageContext, Func<Task>, Task> current,
            Func<IncomingMessageContext, Func<Task>, Task> replacement) =>
            behaviors[behaviors.IndexOf(current)] = replacement;

        internal void Remove(Func<IncomingMessageContext, Func<Task>, Task> behavior) => behaviors.Remove(behavior);

        public void BeforeCommit(Func<IncomingMessageContext, IPersistenceSession, Task> callback) =>
            beforeCommit.Add(callback ?? throw new ArgumentNullException(nameof(callback)));

        internal Func<IncomingMessageContext, Func<Task>, Task> Build()
        {
            var callbacks = beforeCommit.ToArray();
            Func<IncomingMessageContext, Func<Task>, Task> pipeline = async (context, invokeHandler) =>
            {
                await invokeHandler().ConfigureAwait(false);
                foreach (var callback in callbacks)
                    await callback(context, context.HandlerContext.PersistenceSession).ConfigureAwait(false);
            };

            for (var index = behaviors.Count - 1; index >= 0; index--)
            {
                var behavior = behaviors[index];
                var next = pipeline;
                pipeline = (context, invokeHandler) =>
                {
                    var invoked = 0;
                    return behavior(context, async () =>
                    {
                        try
                        {
                            if (Interlocked.Exchange(ref invoked, 1) != 0)
                                throw new InvalidOperationException("An incoming behavior can invoke next only once per attempt.");
                            await next(context, invokeHandler).ConfigureAwait(false);
                        }
                        catch (Exception failure)
                        {
                            context.DownstreamFailure = ExceptionDispatchInfo.Capture(failure);
                            throw;
                        }
                    });
                };
            }

            return async (context, invokeHandler) =>
            {
                await pipeline(context, invokeHandler).ConfigureAwait(false);
                // Catching a downstream failure cannot make a partially executed transaction safe to commit.
                context.DownstreamFailure?.Throw();
            };
        }
    }
}
