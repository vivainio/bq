using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Testing
{
    public enum MessageOperation { Send, SendLocal, Publish, Reply, Forward }

    public sealed class RecordedMessage
    {
        internal RecordedMessage(MessageOperation operation, object message, string destination,
            DateTimeOffset? dueAt, IDictionary<string, object> headers)
        {
            Operation = operation;
            Message = message;
            Destination = destination;
            DueAt = dueAt;
            Headers = new ReadOnlyDictionary<string, object>(new Dictionary<string, object>(headers));
        }

        public MessageOperation Operation { get; }
        public object Message { get; }
        public string Destination { get; }
        public DateTimeOffset? DueAt { get; }
        public IReadOnlyDictionary<string, object> Headers { get; }
        public bool ImmediateDispatch { get; internal set; }
        public string CorrelationId { get; internal set; }
    }

    public sealed class TestableMessageHandlerContext : IMessageHandlerContext, IReplyWithOptions
    {
        readonly ConcurrentQueue<RecordedMessage> messages = new ConcurrentQueue<RecordedMessage>();

        public TestableMessageHandlerContext()
        {
            UniformSession = new Session(messages, new Dictionary<string, object>());
        }

        public IReadOnlyList<RecordedMessage> Messages => messages.ToArray();
        public IUniformSession UniformSession { get; }
        public CancellationToken CancellationToken { get; set; }
        public ContextBag Extensions { get; } = new ContextBag();
        public string MessageId { get; set; }
        public string ReplyToAddress { get; set; }
        public IReadOnlyDictionary<string, string> MessageHeaders => Bq.Messaging.MessageHeaders.AsText(Headers);
        public string CorrelationId { get; set; }
        public IDictionary<string, object> Headers { get; } = new Dictionary<string, object>();
        IReadOnlyDictionary<string, object> IMessageHandlerContext.Headers => new ReadOnlyDictionary<string, object>(Headers);
        public IPersistenceSession PersistenceSession { get; set; }
        public SynchronizedStorageSession SynchronizedStorageSession => PersistenceSession;

        public Task Send<T>(T message, SendOptions options) => UniformSession.Send(message, options);
        public Task Send(object message, SendOptions options) => Send<object>(message, options);
        public Task Send<T>(Action<T> initialize, SendOptions options) =>
            Send(MessageOptionsExtensions.Initialize(initialize), options);
        public Task Send(string message, SendOptions options) => Send<string>(message, options);
        public Task Publish<T>(T message, PublishOptions options) => MessageOptionsExtensions.Publish(this, message, options);
        public Task Publish(object message, PublishOptions options) => Publish<object>(message, options);
        public Task Publish<T>(Action<T> initialize, PublishOptions options) =>
            Publish(MessageOptionsExtensions.Initialize(initialize), options);
        public Task Send<T>(T message) => UniformSession.Send(message);
        public Task Send<T>(string destination, T message) => UniformSession.Send(destination, message);
        public Task SendLocal<T>(T message) => UniformSession.SendLocal(message);
        public Task Publish<T>(T message) => UniformSession.Publish(message);
        public Task SendAt<T>(T message, DateTimeOffset dueAt) => UniformSession.SendAt(message, dueAt);
        public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => UniformSession.SendAt(destination, message, dueAt);
        public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => UniformSession.SendLocalAt(message, dueAt);
        public Task PublishAt<T>(T message, DateTimeOffset dueAt) => UniformSession.PublishAt(message, dueAt);
        public Task ForwardCurrentMessageTo(string destination)
        {
            if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentException("Destination is required.", nameof(destination));
            messages.Enqueue(new RecordedMessage(MessageOperation.Forward, null, destination, null, Headers));
            return Task.CompletedTask;
        }

        public Task Reply<T>(T message, ReplyOptions options) => MessageOptionsExtensions.Reply(this, message, options);
        public Task Reply(object message, ReplyOptions options) => Reply<object>(message, options);
        public Task Reply<T>(Action<T> initialize, ReplyOptions options) =>
            Reply(MessageOptionsExtensions.Initialize(initialize), options);
        public Task Reply<T>(T message) => ((IReplyWithOptions)this).ReplyWithOptions(message, null);

        Task IReplyWithOptions.ReplyWithOptions<T>(T message, ReplyOptions options)
        {
            messages.Enqueue(new RecordedMessage(MessageOperation.Reply, message, options?.Destination ?? ReplyToAddress, null,
                options?.Headers ?? new Dictionary<string, object>())
                { ImmediateDispatch = options?.ImmediateDispatch == true, CorrelationId = options?.CorrelationId ?? CorrelationId });
            return Task.CompletedTask;
        }

        sealed class Session : IUniformSession, IImmediateDispatch
        {
            readonly ConcurrentQueue<RecordedMessage> messages;
            readonly IDictionary<string, object> headers;

            public Session(ConcurrentQueue<RecordedMessage> messages, IDictionary<string, object> headers)
            {
                this.messages = messages;
                this.headers = headers;
            }

            public IUniformSession WithHeader(string name, object value)
            {
                if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Header name is required.", nameof(name));
                return new Session(messages, new Dictionary<string, object>(headers) { [name] = value });
            }

            public Task Send<T>(T message, SendOptions options) => MessageOptionsExtensions.SendUsingOptions(this, message, options);
            public Task Send(object message, SendOptions options) => Send<object>(message, options);
            public Task Send<T>(Action<T> initialize, SendOptions options) =>
                Send(MessageOptionsExtensions.Initialize(initialize), options);
            public Task Send(string message, SendOptions options) => Send<string>(message, options);
            public Task Publish<T>(T message, PublishOptions options) => MessageOptionsExtensions.Publish(this, message, options);
            public Task Publish(object message, PublishOptions options) => Publish<object>(message, options);
            public Task Publish<T>(Action<T> initialize, PublishOptions options) =>
                Publish(MessageOptionsExtensions.Initialize(initialize), options);
            Task IImmediateDispatch.SendImmediately<T>(T message, string destination, bool local)
            {
                messages.Enqueue(new RecordedMessage(local ? MessageOperation.SendLocal : MessageOperation.Send, message, destination, null, headers)
                    { ImmediateDispatch = true });
                return Task.CompletedTask;
            }

            Task IImmediateDispatch.PublishImmediately<T>(T message)
            {
                messages.Enqueue(new RecordedMessage(MessageOperation.Publish, message, null, null, headers) { ImmediateDispatch = true });
                return Task.CompletedTask;
            }

            public Task Send<T>(T message) => Record(MessageOperation.Send, message);
            public Task Send<T>(string destination, T message) => Record(MessageOperation.Send, message, destination);
            public Task SendLocal<T>(T message) => Record(MessageOperation.SendLocal, message);
            public Task Publish<T>(T message) => Record(MessageOperation.Publish, message);
            public Task SendAt<T>(T message, DateTimeOffset dueAt) => Record(MessageOperation.Send, message, dueAt: dueAt);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => Record(MessageOperation.Send, message, destination, dueAt);
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => Record(MessageOperation.SendLocal, message, dueAt: dueAt);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt) => Record(MessageOperation.Publish, message, dueAt: dueAt);

            Task Record(MessageOperation operation, object message, string destination = null, DateTimeOffset? dueAt = null)
            {
                messages.Enqueue(new RecordedMessage(operation, message, destination, dueAt, headers));
                return Task.CompletedTask;
            }
        }
    }
}
