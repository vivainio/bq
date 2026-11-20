using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    // Optional markers for host-defined message discovery and routing conventions.
    public interface IMessage { }
    public interface ICommand : IMessage { }
    public interface IEvent : IMessage { }

    // This is a deliberately small, source-compatible subset, not the NServiceBus assembly.
    public interface IHandleMessages<T>
    {
        Task Handle(T message, IMessageHandlerContext context);
    }

    // Bq's small uniform messaging surface. A session belongs to the scope in
    // which it is created; resolve a fresh session for each application/handler scope.
    public interface IUniformSession
    {
        IUniformSession WithHeader(string name, object value);
        Task Send<T>(T message);
        Task Send<T>(Action<T> initialize);
        Task Send<T>(T message, SendOptions options);
        Task Send(object message, SendOptions options);
        Task Send<T>(Action<T> initialize, SendOptions options);
        Task Send(string message, SendOptions options);
        Task Send<T>(string destination, T message);
        Task SendLocal<T>(T message);
        Task Publish<T>(T message);
        Task Publish<T>(Action<T> initialize);
        Task Publish<T>(T message, PublishOptions options);
        Task Publish(object message, PublishOptions options);
        Task Publish<T>(Action<T> initialize, PublishOptions options);
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

    public interface IPipelineContext : IExtendable
    {
        Task Send(object message, SendOptions options);
        Task Send<T>(Action<T> initialize, SendOptions options);
        Task Publish(object message, PublishOptions options);
        Task Publish<T>(Action<T> initialize, PublishOptions options);
    }

    public interface IMessageProcessingContext : IPipelineContext
    {
        string MessageId { get; }
        string ReplyToAddress { get; }
        IReadOnlyDictionary<string, string> MessageHeaders { get; }
        Task Reply(object message, ReplyOptions options);
        Task ForwardCurrentMessageTo(string destination);
        Task Reply<T>(Action<T> initialize, ReplyOptions options);
    }

    public interface IMessageHandlerContext : IMessageProcessingContext
    {
        IUniformSession UniformSession { get; }
        CancellationToken CancellationToken { get; }
        string CorrelationId { get; }
        IReadOnlyDictionary<string, object> Headers { get; }
        IPersistenceSession PersistenceSession { get; }
        SynchronizedStorageSession SynchronizedStorageSession { get; }
        Task Send<T>(T message);
        Task Send<T>(Action<T> initialize);
        Task Send<T>(T message, SendOptions options);
        new Task Send(object message, SendOptions options);
        new Task Send<T>(Action<T> initialize, SendOptions options);
        Task Send(string message, SendOptions options);
        Task Send<T>(string destination, T message);
        Task SendLocal<T>(T message);
        Task Publish<T>(T message);
        Task Publish<T>(Action<T> initialize);
        Task Publish<T>(T message, PublishOptions options);
        new Task Publish(object message, PublishOptions options);
        new Task Publish<T>(Action<T> initialize, PublishOptions options);
        Task Reply<T>(T message);
        Task Reply<T>(Action<T> initialize);
        Task Reply<T>(T message, ReplyOptions options);
        new Task Reply(object message, ReplyOptions options);
        new Task Reply<T>(Action<T> initialize, ReplyOptions options);
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
            Pipeline = new PipelineSettings(Incoming);
        }
        public string Name { get; }
        public Uri RabbitMqUri { get; }
        public Action<RabbitMQ.Client.ConnectionFactory> ConfigureRabbitMqConnection { get; set; }
        public string EventsExchange { get; set; } = "bq.events";
        public string ErrorExchange { get; set; } = "bq.errors";
        public IDictionary<string, object> InputQueueArguments { get; } = new Dictionary<string, object>();
        // Maximum concurrently executing deliveries for this endpoint instance.
        public ushort MaxConcurrency { get; set; } = 1;
        // RabbitMQ prefetch. Null follows MaxConcurrency; zero requests unlimited prefetch.
        public ushort? PrefetchCount { get; set; }
        // Wait after an empty or failed dispatch poll.
        public TimeSpan DispatchPollInterval { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);
        public TimeSpan PublisherConfirmTimeout { get; set; } = TimeSpan.FromSeconds(10);
        // If set, report the first sustained broker or dispatcher failure for this endpoint instance.
        public Func<string, Exception, Task> OnCriticalError { get; set; }
        internal Func<ICriticalErrorContext, Task> CriticalErrorAction { get; set; }
        internal string PublicReturnAddress { get; set; }
        internal string LocalAddress { get; set; }
        internal string ForwardReceivedMessagesAddress { get; set; }
        internal string AuditQueue { get; set; }
        internal MessagePropertyEncryption PropertyEncryption { get; set; }
        internal IMessageBodyEncryptionService BodyEncryption { get; set; }
        internal string AuditExpiration { get; set; }
        internal bool DurableMessages { get; set; } = true;
        internal bool PurgeInputQueueOnStartup { get; set; }
        internal bool UsePreProvisionedTopology { get; set; }
        internal AutoSubscribeSettings AutoSubscriptionSettings { get; } = new AutoSubscribeSettings();
        internal Dictionary<string, object> OutgoingHeaders { get; } = new Dictionary<string, object>(StringComparer.Ordinal);
        public TimeSpan CriticalErrorThreshold { get; set; } = TimeSpan.FromMinutes(10);
        public Action<TransportMessage> MutateIncoming { get; set; }
        internal List<IMutateIncomingTransportMessages> IncomingTransportMutators { get; } = new List<IMutateIncomingTransportMessages>();
        internal List<IMutateOutgoingMessages> OutgoingMessageMutators { get; } = new List<IMutateOutgoingMessages>();
        internal List<IMutateOutgoingTransportMessages> OutgoingTransportMutators { get; } = new List<IMutateOutgoingTransportMessages>();
        public Action<TransportMessage> MutateOutgoing { get; set; }
        public IncomingPipeline Incoming { get; } = new IncomingPipeline();
        public PipelineSettings Pipeline { get; }
        public RecoverabilityOptions Recoverability { get; } = new RecoverabilityOptions();
        // Retained for existing applications.
        public int ImmediateRetries { get => Recoverability.ImmediateRetries; set => Recoverability.ImmediateRetries = value; }
        // Every endpoint requires transactional persistence.
        public IPersistenceProvider Persistence { get; set; }
        // Called for non-starting messages that do not match an active saga.
        public Func<Type, string, object, IMessageHandlerContext, Task> SagaNotFound { get; set; }
        internal List<Func<object, IMessageHandlerContext, Task>> SagaNotFoundHandlers { get; } =
            new List<Func<object, IMessageHandlerContext, Task>>();
        readonly ConventionsBuilder conventions = new ConventionsBuilder();

        public ConventionsBuilder Conventions() => conventions;

        public void Route<T>(string endpoint) => Route(typeof(T), endpoint);

        public void Route(Type messageType, string endpoint)
        {
            if (messageType == null) throw new ArgumentNullException(nameof(messageType));
            if (string.IsNullOrWhiteSpace(endpoint))
                throw new ArgumentException("A destination queue is required.", nameof(endpoint));
            Routes[messageType] = endpoint;
        }
        public void Handle<T>(Func<IHandleMessages<T>> factory) =>
            RegisterHandler<T>(Adapt(factory), subscribe: null);

        public void Handle<T>(Func<IHandleMessages<T>> factory, bool subscribe) =>
            RegisterHandler<T>(Adapt(factory), subscribe);

        static Func<IMessageHandlerContext, IHandleMessages<T>> Adapt<T>(Func<IHandleMessages<T>> factory)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            return _ => factory();
        }

        public void Handle<T>(Func<IMessageHandlerContext, IHandleMessages<T>> factory) =>
            RegisterHandler(factory, subscribe: null);

        public void Handle<T>(Func<IMessageHandlerContext, IHandleMessages<T>> factory, bool subscribe) =>
            RegisterHandler(factory, subscribe);

        // Factories run inside the message scope and can resolve scoped services.
        void RegisterHandler<T>(Func<IMessageHandlerContext, IHandleMessages<T>> factory, bool? subscribe)
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            var key = MessageType(typeof(T));
            AddRegistration(key, new Registration(typeof(T), subscribe == true, async (message, context) =>
            {
                var handler = factory(context) ?? throw new InvalidOperationException("Handler factory returned null.");
                try { await handler.Handle((T)message, context).ConfigureAwait(false); }
                finally { (handler as IDisposable)?.Dispose(); }
            }, useEventConvention: !subscribe.HasValue));
        }
        internal void AddRegistration(string name, Registration registration)
        {
            if (Handlers.TryGetValue(name, out var existing)) existing.Add(registration);
            else Handlers.Add(name, registration);
        }
        internal readonly Dictionary<Type, string[]> ContractNames = new Dictionary<Type, string[]>();

        public void RegisterMessageContract<T>(string name, params string[] receiveAliases)
        {
            if (typeof(T) == typeof(SagaTimeoutMessage))
                throw new ArgumentException("The saga timeout contract is reserved.");
            if (receiveAliases == null) throw new ArgumentNullException(nameof(receiveAliases));
            var names = new string[receiveAliases.Length + 1];
            names[0] = name;
            Array.Copy(receiveAliases, 0, names, 1, receiveAliases.Length);
            foreach (var value in names)
                if (string.IsNullOrWhiteSpace(value) || System.Text.Encoding.UTF8.GetByteCount(value) > 255 ||
                    value.IndexOfAny(new[] { '*', '#', '\0' }) >= 0 || value == MessageType(typeof(SagaTimeoutMessage)))
                    throw new ArgumentException("Contract names must be nonempty AMQP names without wildcards or reserved values.");
            ContractNames.Add(typeof(T), names);
        }

        internal static string MessageType(Type type) => type.FullName ?? throw new ArgumentException("Message type must have a full name.");
    }

    internal sealed class Registration
    {
        readonly List<Func<object, IMessageHandlerContext, Task>> invocations = new List<Func<object, IMessageHandlerContext, Task>>();
        public Registration(Type type, bool subscribe, Func<object, IMessageHandlerContext, Task> invoke, bool useEventConvention = false, bool useSagaEventConvention = false)
        {
            Type = type;
            Subscribe = subscribe;
            UseEventConvention = useEventConvention;
            UseSagaEventConvention = useSagaEventConvention;
            invocations.Add(invoke);
        }
        public Type Type { get; }
        public bool Subscribe { get; private set; }
        public bool UseEventConvention { get; private set; }
        public bool UseSagaEventConvention { get; private set; }
        public void Add(Registration registration)
        {
            if (Type != registration.Type) throw new InvalidOperationException("Message type name collision: " + Type.FullName);
            Subscribe |= registration.Subscribe;
            UseEventConvention |= registration.UseEventConvention;
            UseSagaEventConvention |= registration.UseSagaEventConvention;
            invocations.AddRange(registration.invocations);
        }
        public async Task Invoke(object message, IMessageHandlerContext context)
        {
            foreach (var invocation in invocations) await invocation(message, context).ConfigureAwait(false);
        }
    }
}
