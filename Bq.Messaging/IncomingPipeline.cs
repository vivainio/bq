using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class IncomingMessageContext
    {
        internal IncomingMessageContext(object message, IMessageHandlerContext handlerContext,
            string contractName, int immediateRetries, int delayedRetries)
        {
            Message = message;
            HandlerContext = handlerContext;
            ContractName = contractName;
            ImmediateRetries = immediateRetries;
            DelayedRetries = delayedRetries;
        }

        public object Message { get; }
        public string ContractName { get; }
        public int ImmediateRetries { get; }
        public int DelayedRetries { get; }
        public bool IsSagaTimeout => Message is SagaTimeoutMessage;
        public bool IsDeferred => IsSagaTimeout || DelayedRetries > 0 ||
            (HandlerContext.Headers.TryGetValue(MessageHeaders.Deferred, out var value) && value is bool deferred && deferred);
        public IMessageHandlerContext HandlerContext { get; }
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
                pipeline = (context, invokeHandler) => behavior(context, () => next(context, invokeHandler));
            }

            return pipeline;
        }
    }
}
