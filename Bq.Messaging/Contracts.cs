using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    // This is a deliberately small, source-compatible subset, not the NServiceBus assembly.
    public interface IHandleMessages<T>
    {
        Task Handle(T message, IMessageHandlerContext context);
    }

    public interface IMessageHandlerContext
    {
        string MessageId { get; }
        string CorrelationId { get; }
        IReadOnlyDictionary<string, object> Headers { get; }
        IPersistenceSession PersistenceSession { get; }
        Task Send<T>(T message);
        Task Publish<T>(T message);
        Task Reply<T>(T message);
    }

    public sealed class EndpointConfiguration
    {
        internal readonly Dictionary<Type, string> Routes = new Dictionary<Type, string>();
        internal readonly Dictionary<string, Registration> Handlers = new Dictionary<string, Registration>();
        internal readonly Dictionary<string, Func<SagaTimeoutMessage, IMessageHandlerContext, Task>> SagaTimeouts = new Dictionary<string, Func<SagaTimeoutMessage, IMessageHandlerContext, Task>>();
        internal bool HasSagas;
        public EndpointConfiguration(string name, Uri rabbitMqUri)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("An endpoint name is required.", nameof(name));
            Name = name;
            RabbitMqUri = rabbitMqUri ?? throw new ArgumentNullException(nameof(rabbitMqUri));
        }
        public string Name { get; }
        public Uri RabbitMqUri { get; }
        // Immediate retries are per delivery; broker redelivery starts a new attempt sequence.
        public int ImmediateRetries { get; set; } = 2;
        // Optional; when supplied, incoming processing uses a local database transaction.
        public IPersistenceProvider Persistence { get; set; }
        // Called for non-starting messages that do not match an active saga.
        public Func<Type, string, object, IMessageHandlerContext, Task> SagaNotFound { get; set; }
        public void Route<T>(string endpoint) => Routes[typeof(T)] = endpoint;
        public void Handle<T>(Func<IHandleMessages<T>> factory, bool subscribe = false)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            var key = MessageType(typeof(T));
            AddRegistration(key, new Registration(typeof(T), subscribe, async (message, context) =>
            {
                var handler = factory() ?? throw new InvalidOperationException("Handler factory returned null.");
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
