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
        Task Send<T>(T message);
        Task Publish<T>(T message);
        Task Reply<T>(T message);
    }

    public sealed class EndpointConfiguration
    {
        internal readonly Dictionary<Type, string> Routes = new Dictionary<Type, string>();
        internal readonly Dictionary<string, Registration> Handlers = new Dictionary<string, Registration>();
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
        public void Route<T>(string endpoint) => Routes[typeof(T)] = endpoint;
        public void Handle<T>(Func<IHandleMessages<T>> factory, bool subscribe = false)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            var key = MessageType(typeof(T));
            if (Handlers.ContainsKey(key)) throw new InvalidOperationException("A handler is already registered for " + key);
            Handlers.Add(key, new Registration(typeof(T), subscribe, async (message, context) =>
            {
                var handler = factory() ?? throw new InvalidOperationException("Handler factory returned null.");
                try { await handler.Handle((T)message, context).ConfigureAwait(false); }
                finally { (handler as IDisposable)?.Dispose(); }
            }));
        }
        internal static string MessageType(Type type) => type.FullName ?? throw new ArgumentException("Message type must have a full name.");
    }

    internal sealed class Registration
    {
        public Registration(Type type, bool subscribe, Func<object, IMessageHandlerContext, Task> invoke)
        { Type = type; Subscribe = subscribe; Invoke = invoke; }
        public Type Type { get; }
        public bool Subscribe { get; }
        public Func<object, IMessageHandlerContext, Task> Invoke { get; }
    }
}
