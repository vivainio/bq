using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class IncomingMessageContext : IBehaviorContext
    {
        internal IncomingMessageContext(object message, RabbitEndpoint.HandlerContext handlerContext,
            Type messageType, string contractName, int immediateRetries, int delayedRetries)
        {
            this.handlerContext = handlerContext;
            handlerContext.LogicalMessage = message;
            MessageType = messageType;
            HandlerContext = handlerContext;
            LogicalHeaders = handlerContext.LogicalHeaders;
            ContractName = contractName;
            ImmediateRetries = immediateRetries;
            DelayedRetries = delayedRetries;
        }

        public object Message => handlerContext.LogicalMessage;
        readonly RabbitEndpoint.HandlerContext handlerContext;
        internal void UpdateMessageInstance(object newInstance)
        {
            if (newInstance == null) throw new ArgumentNullException(nameof(newInstance));
            if (!MessageType.IsInstanceOfType(newInstance))
                throw new ArgumentException("The replacement must implement the incoming message contract.", nameof(newInstance));
            handlerContext.LogicalMessage = newInstance;
        }
        public Type MessageType { get; }
        public string ContractName { get; }
        public int ImmediateRetries { get; }
        public int DelayedRetries { get; }
        public bool IsSagaTimeout => Message is SagaTimeoutMessage;
        public bool IsDeferred => IsSagaTimeout || DelayedRetries > 0 ||
            (HandlerContext.Headers.TryGetValue(MessageHeaders.Deferred, out var value) && value is bool deferred && deferred);
        public IMessageHandlerContext HandlerContext { get; }
        internal Dictionary<string, string> LogicalHeaders { get; }
        public ContextBag Extensions => HandlerContext.Extensions;
        internal ExceptionDispatchInfo DownstreamFailure { get; set; }
        internal bool MessageHandled { get; set; }
        public IDictionary<string, object> Items { get; } = new Dictionary<string, object>();
    }

    public sealed class IncomingPhysicalContext : IIncomingPhysicalMessageContext
    {
        internal IncomingPhysicalContext(RabbitEndpoint.HandlerContext handlerContext, IncomingMessage message)
        {
            HandlerContext = handlerContext;
            Message = message;
        }

        internal RabbitEndpoint.HandlerContext HandlerContext { get; }
        public IncomingMessage Message { get; }
        public void UpdateMessage(byte[] body) => Message.UpdateBody(body);
        public string MessageId => Message.MessageId;
        public string ReplyToAddress => HandlerContext.ReplyToAddress;
        public IReadOnlyDictionary<string, string> MessageHeaders => Message.Headers;
        public ContextBag Extensions => HandlerContext.Extensions;
        internal ExceptionDispatchInfo DownstreamFailure { get; set; }
        public Task Send(object message, SendOptions options) => HandlerContext.Send(message, options);
        public Task Send<T>(Action<T> initialize, SendOptions options) => HandlerContext.Send(initialize, options);
        public Task Publish(object message, PublishOptions options) => HandlerContext.Publish(message, options);
        public Task Publish<T>(Action<T> initialize, PublishOptions options) => HandlerContext.Publish(initialize, options);
        public Task Reply(object message, ReplyOptions options) => HandlerContext.Reply(message, options);
        public Task Reply<T>(Action<T> initialize, ReplyOptions options) => HandlerContext.Reply(initialize, options);
        public Task ForwardCurrentMessageTo(string destination) => HandlerContext.ForwardCurrentMessageTo(destination);
    }

    public sealed class IncomingPipeline
    {
        readonly List<Func<IncomingPhysicalContext, Func<Task>, Task>> physicalBehaviors =
            new List<Func<IncomingPhysicalContext, Func<Task>, Task>>();

        internal void UsePhysical(Func<IncomingPhysicalContext, Func<Task>, Task> behavior) =>
            physicalBehaviors.Add(behavior ?? throw new ArgumentNullException(nameof(behavior)));

        internal void ReplacePhysical(Func<IncomingPhysicalContext, Func<Task>, Task> current,
            Func<IncomingPhysicalContext, Func<Task>, Task> replacement) =>
            physicalBehaviors[physicalBehaviors.IndexOf(current)] = replacement;

        internal void RemovePhysical(Func<IncomingPhysicalContext, Func<Task>, Task> behavior) =>
            physicalBehaviors.Remove(behavior);

        internal bool HasPhysicalBehaviors => physicalBehaviors.Count != 0;

        internal Func<IncomingPhysicalContext, Func<Task>, Task> BuildPhysical(PipelineSettings settings)
        {
            var ordered = settings.OrderPhysical(physicalBehaviors).ToArray();
            Func<IncomingPhysicalContext, Func<Task>, Task> pipeline = (context, terminal) => terminal();
            for (var index = ordered.Length - 1; index >= 0; index--)
            {
                var behavior = ordered[index];
                var next = pipeline;
                pipeline = (context, terminal) =>
                {
                    var invoked = 0;
                    return behavior(context, async () =>
                    {
                        try
                        {
                            if (Interlocked.Exchange(ref invoked, 1) != 0)
                                throw new InvalidOperationException("An incoming behavior can invoke next only once per attempt.");
                            await next(context, terminal).ConfigureAwait(false);
                        }
                        catch (Exception failure)
                        {
                            context.DownstreamFailure = ExceptionDispatchInfo.Capture(failure);
                            throw;
                        }
                    });
                };
            }

            return async (context, terminal) =>
            {
                await pipeline(context, terminal).ConfigureAwait(false);
                context.DownstreamFailure?.Throw();
            };
        }

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

        internal Func<IncomingMessageContext, Func<Task>, Task> Build(PipelineSettings settings)
        {
            var callbacks = beforeCommit.ToArray();
            Func<IncomingMessageContext, Func<Task>, Task> pipeline = async (context, invokeHandler) =>
            {
                await invokeHandler().ConfigureAwait(false);
                foreach (var callback in callbacks)
                    await callback(context, context.HandlerContext.PersistenceSession).ConfigureAwait(false);
            };

            var ordered = settings.Order(behaviors).ToArray();
            for (var index = ordered.Length - 1; index >= 0; index--)
            {
                var behavior = ordered[index];
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
