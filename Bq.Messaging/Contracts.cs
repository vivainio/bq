using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    // This is a deliberately small, source-compatible subset, not the NServiceBus assembly.
    public interface IHandleMessages<T>
    {
        Task Handle(T message, IMessageHandlerContext context);
    }

    // Bq's small uniform messaging surface. A session belongs to the scope in
    // which it is created; resolve a fresh session for each application/handler scope.
    public interface IUniformSession
    {
        Task Send<T>(T message);
        Task Send<T>(string destination, T message);
        Task SendLocal<T>(T message);
        Task Publish<T>(T message);
        Task SendAt<T>(T message, DateTimeOffset dueAt);
        Task SendAt<T>(string destination, T message, DateTimeOffset dueAt);
        Task SendLocalAt<T>(T message, DateTimeOffset dueAt);
        Task PublishAt<T>(T message, DateTimeOffset dueAt);
    }

    public interface ITransactionalSession : IUniformSession
    {
        CancellationToken CancellationToken { get; }
        System.Data.Common.DbConnection Connection { get; }
        System.Data.Common.DbTransaction Transaction { get; }
    }

    public interface IMessageHandlerContext
    {
        IUniformSession UniformSession { get; }
        CancellationToken CancellationToken { get; }
        string MessageId { get; }
        string CorrelationId { get; }
        IReadOnlyDictionary<string, object> Headers { get; }
        IPersistenceSession PersistenceSession { get; }
        Task Send<T>(T message);
        Task Send<T>(string destination, T message);
        Task SendLocal<T>(T message);
        Task Publish<T>(T message);
        Task Reply<T>(T message);
        Task SendAt<T>(T message, DateTimeOffset dueAt);
        Task SendAt<T>(string destination, T message, DateTimeOffset dueAt);
        Task SendLocalAt<T>(T message, DateTimeOffset dueAt);
        Task PublishAt<T>(T message, DateTimeOffset dueAt);
    }

    public sealed class EndpointConfiguration
    {
        internal readonly Dictionary<Type, string> Routes = new Dictionary<Type, string>();
        internal readonly Dictionary<string, Registration> Handlers = new Dictionary<string, Registration>();
        internal readonly Dictionary<string, Func<SagaTimeoutMessage, IMessageHandlerContext, Task>> SagaTimeouts = new Dictionary<string, Func<SagaTimeoutMessage, IMessageHandlerContext, Task>>();
        internal bool HasSagas;
        internal bool IsSendOnly { get; private set; }
        public void SendOnly() => IsSendOnly = true;
        public EndpointConfiguration(string name, Uri rabbitMqUri)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("An endpoint name is required.", nameof(name));
            Name = name;
            RabbitMqUri = rabbitMqUri ?? throw new ArgumentNullException(nameof(rabbitMqUri));
        }
        public string Name { get; }
        public Uri RabbitMqUri { get; }
        public string EventsExchange { get; set; } = "bq.events";
        public string ErrorExchange { get; set; } = "bq.errors";
        public IDictionary<string, object> InputQueueArguments { get; } = new Dictionary<string, object>();
        // Maximum concurrently executing deliveries for this endpoint instance.
        public ushort MaxConcurrency { get; set; } = 1;
        // Wait after an empty or failed dispatch poll.
        public TimeSpan DispatchPollInterval { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);
        public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(10);
        public RecoverabilityOptions Recoverability { get; } = new RecoverabilityOptions();
        // Retained for existing applications.
        public int ImmediateRetries { get => Recoverability.ImmediateRetries; set => Recoverability.ImmediateRetries = value; }
        // Every endpoint requires transactional persistence.
        public IPersistenceProvider Persistence { get; set; }
        // Called for non-starting messages that do not match an active saga.
        public Func<Type, string, object, IMessageHandlerContext, Task> SagaNotFound { get; set; }
        public void Route<T>(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
                throw new ArgumentException("A destination queue is required.", nameof(endpoint));
            Routes[typeof(T)] = endpoint;
        }
        public void Handle<T>(Func<IHandleMessages<T>> factory, bool subscribe = false)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            Handle<T>(_ => factory(), subscribe);
        }
        // The factory runs inside the current message scope, allowing scoped services
        // to receive its uniform session without relying on an endpoint startup race.
        public void Handle<T>(Func<IMessageHandlerContext, IHandleMessages<T>> factory, bool subscribe = false)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            var key = MessageType(typeof(T));
            AddRegistration(key, new Registration(typeof(T), subscribe, async (message, context) =>
            {
                var handler = factory(context) ?? throw new InvalidOperationException("Handler factory returned null.");
                try { await handler.Handle((T)message, context).ConfigureAwait(false); }
                finally { (handler as IDisposable)?.Dispose(); }
            }));
        }
        internal void AddRegistration(string name, Registration registration)
        {
            if (Handlers.TryGetValue(name, out var existing)) existing.Add(registration);
            else Handlers.Add(name, registration);
        }
        internal static string MessageType(Type type) => type.FullName ?? throw new ArgumentException("Message type must have a full name.");
    }

    internal sealed class Registration
    {
        readonly List<Func<object, IMessageHandlerContext, Task>> invocations = new List<Func<object, IMessageHandlerContext, Task>>();
        public Registration(Type type, bool subscribe, Func<object, IMessageHandlerContext, Task> invoke)
        { Type = type; Subscribe = subscribe; invocations.Add(invoke); }
        public Type Type { get; }
        public bool Subscribe { get; private set; }
        public void Add(Registration registration)
        {
            if (Type != registration.Type) throw new InvalidOperationException("Message type name collision: " + Type.FullName);
            Subscribe |= registration.Subscribe;
            invocations.AddRange(registration.invocations);
        }
        public async Task Invoke(object message, IMessageHandlerContext context)
        {
            foreach (var invocation in invocations) await invocation(message, context).ConfigureAwait(false);
        }
    }
}
