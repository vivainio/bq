using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;

namespace Bq.Messaging.Unicast.Messages
{
    public class MessageMetadata
    {
        public MessageMetadata(Type messageType, Type[] messageHierarchy = null)
        {
            MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
            MessageHierarchy = messageHierarchy ?? new[] { messageType }.Concat(messageType.GetInterfaces()).ToArray();
        }

        public Type MessageType { get; }
        public Type[] MessageHierarchy { get; }
    }
}

namespace Bq.Messaging.Pipeline
{
    public class MessageHandler
    {
        public MessageHandler(Type handlerType, object instance)
        {
            HandlerType = handlerType;
            Instance = instance;
        }

        public Type HandlerType { get; }
        public object Instance { get; set; }
    }
}

namespace Bq.Messaging
{
    using Bq.Messaging.Pipeline;
    using Bq.Messaging.Unicast.Messages;

    /// <summary>The stage around each handler invocation. Saga handlers do not pass through it.</summary>
    public interface IInvokeHandlerContext : IIncomingContext, IMessageHandlerContext
    {
        MessageHandler MessageHandler { get; }
        new Dictionary<string, string> Headers { get; }
        object MessageBeingHandled { get; }
        bool HandlerInvocationAborted { get; }
        MessageMetadata MessageMetadata { get; }
    }

    internal sealed class InvokeHandlerContext : IInvokeHandlerContext, IFailureTracking
    {
        readonly IMessageHandlerContext inner;

        public InvokeHandlerContext(IMessageHandlerContext inner, object handler, object message, Func<IBuilder> builder)
        {
            this.inner = inner;
            MessageHandler = new MessageHandler(handler.GetType(), handler);
            MessageBeingHandled = message;
            MessageMetadata = new MessageMetadata(message.GetType());
            this.builder = builder;
            Headers = inner.MessageHeaders.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        }

        readonly Func<IBuilder> builder;
        public MessageHandler MessageHandler { get; }
        public Dictionary<string, string> Headers { get; }
        public object MessageBeingHandled { get; }
        public bool HandlerInvocationAborted => false;
        public MessageMetadata MessageMetadata { get; }
        public ExceptionDispatchInfo DownstreamFailure { get; set; }
        public IBuilder Builder => builder();
        public ContextBag Extensions => inner.Extensions;
        public string MessageId => inner.MessageId;
        public string ReplyToAddress => inner.ReplyToAddress;
        public IReadOnlyDictionary<string, string> MessageHeaders => inner.MessageHeaders;
        public IUniformSession UniformSession => inner.UniformSession;
        public CancellationToken CancellationToken => inner.CancellationToken;
        public string CorrelationId => inner.CorrelationId;
        IReadOnlyDictionary<string, object> IMessageHandlerContext.Headers => inner.Headers;
        public IPersistenceSession PersistenceSession => inner.PersistenceSession;
        public SynchronizedStorageSession SynchronizedStorageSession => inner.SynchronizedStorageSession;
        public void DoNotContinueDispatchingCurrentMessageToHandlers() => inner.DoNotContinueDispatchingCurrentMessageToHandlers();
        public Task HandleCurrentMessageLater() => inner.HandleCurrentMessageLater();
        public Task ForwardCurrentMessageTo(string destination) => inner.ForwardCurrentMessageTo(destination);
        public Task Send<T>(T message) => inner.Send(message);
        public Task Send<T>(Action<T> initialize) => inner.Send(initialize);
        public Task Send<T>(T message, SendOptions options) => inner.Send(message, options);
        public Task Send(object message, SendOptions options) => inner.Send(message, options);
        public Task Send<T>(Action<T> initialize, SendOptions options) => inner.Send(initialize, options);
        public Task Send(string message, SendOptions options) => inner.Send(message, options);
        public Task Send<T>(string destination, T message) => inner.Send(destination, message);
        public Task SendLocal<T>(T message) => inner.SendLocal(message);
        public Task Publish<T>(T message) => inner.Publish(message);
        public Task Publish<T>(Action<T> initialize) => inner.Publish(initialize);
        public Task Publish<T>(T message, PublishOptions options) => inner.Publish(message, options);
        public Task Publish(object message, PublishOptions options) => inner.Publish(message, options);
        public Task Publish<T>(Action<T> initialize, PublishOptions options) => inner.Publish(initialize, options);
        public Task Reply<T>(T message) => inner.Reply(message);
        public Task Reply<T>(Action<T> initialize) => inner.Reply(initialize);
        public Task Reply<T>(T message, ReplyOptions options) => inner.Reply(message, options);
        public Task Reply(object message, ReplyOptions options) => inner.Reply(message, options);
        public Task Reply<T>(Action<T> initialize, ReplyOptions options) => inner.Reply(initialize, options);
        public Task SendAt<T>(T message, DateTimeOffset dueAt) => inner.SendAt(message, dueAt);
        public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => inner.SendAt(destination, message, dueAt);
        public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => inner.SendLocalAt(message, dueAt);
        public Task PublishAt<T>(T message, DateTimeOffset dueAt) => inner.PublishAt(message, dueAt);
    }

    public sealed partial class EndpointConfiguration
    {
        internal Func<InvokeHandlerContext, Func<Task>, Task> InvokeHandlerChain { get; set; }
    }
}
