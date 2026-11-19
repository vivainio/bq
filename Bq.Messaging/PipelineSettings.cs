using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IBehaviorContext : IExtendable { }

    public interface IIncomingContext : IBehaviorContext, IMessageProcessingContext { }

    public interface IIncomingLogicalMessageContext : IIncomingContext
    {
        LogicalMessage Message { get; }
        bool MessageHandled { get; set; }
        void UpdateMessageInstance(object newInstance);
    }

    public sealed class LogicalMessage
    {
        internal LogicalMessage(Type type, object instance)
        {
            MessageType = type;
            Instance = instance;
        }

        public Type MessageType { get; }
        public object Instance { get; }
    }

    public interface IBehavior { }

    public interface IBehavior<in TInContext, out TOutContext> : IBehavior
        where TInContext : IBehaviorContext where TOutContext : IBehaviorContext
    {
        Task Invoke(TInContext context, Func<TOutContext, Task> next);
    }

    public abstract class Behavior<TContext> : IBehavior<TContext, TContext> where TContext : IBehaviorContext
    {
        public abstract Task Invoke(TContext context, Func<Task> next);

        public Task Invoke(TContext context, Func<TContext, Task> next)
        {
            if (next == null) throw new ArgumentNullException(nameof(next));
            return Invoke(context, () => next(context));
        }
    }

    public sealed class PipelineSettings
    {
        readonly IncomingPipeline incoming;
        readonly HashSet<string> stepIds = new HashSet<string>(StringComparer.Ordinal);

        internal PipelineSettings(IncomingPipeline incoming) { this.incoming = incoming; }

        public void Register(Type behavior, string description) =>
            Register(TypeName(behavior), behavior, description);

        public void Register(string stepId, Type behavior, string description)
        {
            TypeName(behavior);
            if (behavior.IsAbstract || !typeof(IBehavior).IsAssignableFrom(behavior))
                throw new ArgumentException("A concrete pipeline behavior is required.", nameof(behavior));
            Register(stepId, (IBehavior)Activator.CreateInstance(behavior), description);
        }

        public void Register<T>(T behavior, string description) where T : IBehavior =>
            Register(TypeName(behavior?.GetType()), behavior, description);

        public void Register<T>(string stepId, T behavior, string description) where T : IBehavior
        {
            if (string.IsNullOrWhiteSpace(stepId)) throw new ArgumentException("A step ID is required.", nameof(stepId));
            if (behavior == null) throw new ArgumentNullException(nameof(behavior));
            Func<IncomingMessageContext, Func<Task>, Task> invoke;
            if (behavior is IBehavior<IncomingMessageContext, IncomingMessageContext> native)
                invoke = (context, next) => native.Invoke(context, continuation => Continue(context, continuation, next));
            else if (behavior is IBehavior<IIncomingLogicalMessageContext, IIncomingLogicalMessageContext> logical)
                invoke = (context, next) =>
                {
                    var view = new LogicalContext(context);
                    return logical.Invoke(view, continuation => Continue(view, continuation, next));
                };
            else
                throw new NotSupportedException("Register an incoming logical message behavior or a Bq incoming behavior.");
            if (!stepIds.Add(stepId)) throw new InvalidOperationException("A pipeline step already uses ID " + stepId);
            incoming.Use(invoke);
        }

        static string TypeName(Type behavior) =>
            (behavior ?? throw new ArgumentNullException(nameof(behavior))).FullName;

        static Task Continue(object expected, object supplied, Func<Task> next)
        {
            if (!ReferenceEquals(expected, supplied)) throw new InvalidOperationException("The continuation must use the active incoming context.");
            return next();
        }

        sealed class LogicalContext : IIncomingLogicalMessageContext
        {
            readonly IncomingMessageContext context;

            public LogicalContext(IncomingMessageContext context)
            {
                this.context = context;
            }

            public LogicalMessage Message => new LogicalMessage(context.MessageType, context.Message);
            public bool MessageHandled
            {
                get => context.MessageHandled;
                set => context.MessageHandled = value;
            }
            public void UpdateMessageInstance(object newInstance) => context.UpdateMessageInstance(newInstance);
            public string MessageId => context.HandlerContext.MessageId;
            public string ReplyToAddress => context.HandlerContext.ReplyToAddress;
            public IReadOnlyDictionary<string, string> MessageHeaders => context.HandlerContext.MessageHeaders;
            public ContextBag Extensions => context.Extensions;
            public Task Send(object message, SendOptions options) => context.HandlerContext.Send(message, options);
            public Task Send<T>(Action<T> initialize, SendOptions options) => context.HandlerContext.Send(initialize, options);
            public Task Publish(object message, PublishOptions options) => context.HandlerContext.Publish(message, options);
            public Task Publish<T>(Action<T> initialize, PublishOptions options) => context.HandlerContext.Publish(initialize, options);
            public Task Reply(object message, ReplyOptions options) => context.HandlerContext.Reply(message, options);
            public Task Reply<T>(Action<T> initialize, ReplyOptions options) => context.HandlerContext.Reply(initialize, options);
            public Task ForwardCurrentMessageTo(string destination) => context.HandlerContext.ForwardCurrentMessageTo(destination);
        }
    }
}
