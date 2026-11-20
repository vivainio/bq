using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Bq.Messaging
{
    public sealed class RabbitEndpoint : IEndpointInstance
    {
        readonly string localAddress;
        readonly string forwardReceivedMessagesAddress;
        readonly string auditQueue;
        readonly string auditExpiration;
        const string AuditExpirationHeader = "x-bq-outbox-audit-expiration";
        readonly string eventsExchange;
        readonly string errorExchange;
        const string ContentType = "application/x-msgpack";
        static readonly MessagePackSerializerOptions SerializerOptions = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
        readonly EndpointConfiguration configuration;
        readonly Action<TransportMessage> mutateIncoming;
        readonly IMutateIncomingTransportMessages[] incomingTransportMutators;
        readonly Action<TransportMessage> mutateOutgoing;
        readonly IMutateOutgoingMessages[] outgoingMessageMutators;
        readonly IMutateOutgoingTransportMessages[] outgoingTransportMutators;
        readonly Func<IncomingMessageContext, Func<Task>, Task> incoming;
        readonly Dictionary<Type, string> wireNames = new Dictionary<Type, string>();
        readonly Dictionary<string, Registration> receivers = new Dictionary<string, Registration>();
        readonly bool sendOnly;
        readonly TimeSpan dispatchPollInterval;
        readonly CriticalErrorMonitor criticalErrors;
        readonly TimeSpan publisherConfirmTimeout;
        readonly IConnection connection;
        readonly IModel consumerChannel;
        readonly IModel publisherChannel;
        readonly object publishLock = new object();
        readonly object consumerLock = new object();
        readonly object lifecycleLock = new object();
        readonly AsyncLocal<TransactionalSession> externalTransaction = new AsyncLocal<TransactionalSession>();
        readonly AsyncLocal<UniformScope> uniformScope = new AsyncLocal<UniformScope>();
        readonly CancellationTokenSource forceStop = new CancellationTokenSource();
        readonly TaskCompletionSource<bool> drained = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        string consumerTag;
        int activeDeliveries;
        bool shutdownRequested;
        Task stopTask;
        readonly CancellationTokenSource stopping = new CancellationTokenSource();
        Task dispatcher;
        bool returned;
        volatile bool disposed;

        RabbitEndpoint(EndpointConfiguration configuration)
        {
            this.configuration = configuration;
            localAddress = configuration.LocalAddress ?? configuration.Name;
            forwardReceivedMessagesAddress = configuration.ForwardReceivedMessagesAddress;
            auditQueue = configuration.AuditQueue;
            auditExpiration = configuration.AuditExpiration;
            if (forwardReceivedMessagesAddress == localAddress || auditQueue == localAddress)
                throw new ArgumentException("The forwarding and audit destinations must differ from the endpoint input queue.", nameof(configuration));
            mutateIncoming = configuration.MutateIncoming;
            incomingTransportMutators = configuration.IncomingTransportMutators.ToArray();
            mutateOutgoing = configuration.MutateOutgoing;
            outgoingMessageMutators = configuration.OutgoingMessageMutators.ToArray();
            outgoingTransportMutators = configuration.OutgoingTransportMutators.ToArray();
            incoming = configuration.Incoming.Build();
            var owners = new Dictionary<string, Type>();
            var types = configuration.ContractNames.Keys.Concat(configuration.Handlers.Values.Select(handler => handler.Type))
                .Concat(configuration.Routes.Keys).Distinct();
            foreach (var type in types)
            {
                var names = configuration.ContractNames.TryGetValue(type, out var configured)
                    ? configured : new[] { EndpointConfiguration.MessageType(type) };
                wireNames.Add(type, names[0]);
                configuration.Handlers.TryGetValue(EndpointConfiguration.MessageType(type), out var handler);
                foreach (var name in names)
                {
                    if (owners.TryGetValue(name, out var owner) && owner != type)
                        throw new InvalidOperationException("Message contract name collision: " + name);
                    owners[name] = type;
                    if (handler != null && handler.Type == type) receivers[name] = handler;
                }
            }
            sendOnly = configuration.IsSendOnly;
            eventsExchange = configuration.EventsExchange;
            errorExchange = configuration.ErrorExchange;
            dispatchPollInterval = configuration.DispatchPollInterval;
            publisherConfirmTimeout = configuration.PublisherConfirmTimeout;
            var factory = new ConnectionFactory { Uri = configuration.RabbitMqUri };
            configuration.ConfigureRabbitMqConnection?.Invoke(factory);
            factory.DispatchConsumersAsync = true;
            factory.ConsumerDispatchConcurrency = configuration.MaxConcurrency;
            factory.NetworkRecoveryInterval = configuration.NetworkRecoveryInterval;
            connection = factory.CreateConnection();
            try
            {
                publisherChannel = connection.CreateModel();
                if (configuration.UsePreProvisionedTopology) publisherChannel.ExchangeDeclarePassive(eventsExchange);
                else publisherChannel.ExchangeDeclare(eventsExchange, ExchangeType.Topic, true);
                publisherChannel.ConfirmSelect();
                publisherChannel.BasicReturn += (sender, args) => { returned = true; };
                if (!sendOnly)
                {
                    consumerChannel = connection.CreateModel();
                    var errorQueue = configuration.Recoverability.ErrorQueue ?? configuration.Name + ".error";
                    if (configuration.UsePreProvisionedTopology)
                    {
                        consumerChannel.ExchangeDeclarePassive(errorExchange);
                        consumerChannel.QueueDeclarePassive(errorQueue);
                        consumerChannel.QueueDeclarePassive(localAddress);
                    }
                    else
                    {
                        consumerChannel.ExchangeDeclare(errorExchange, ExchangeType.Direct, true);
                        consumerChannel.QueueDeclare(errorQueue, true, false, false);
                        consumerChannel.QueueBind(errorQueue, errorExchange, localAddress);
                        consumerChannel.QueueDeclare(localAddress, true, false, false, new Dictionary<string, object>(configuration.InputQueueArguments)
                        {
                            ["x-dead-letter-exchange"] = errorExchange,
                            ["x-dead-letter-routing-key"] = localAddress
                        });
                    }
                    if (configuration.PurgeInputQueueOnStartup) consumerChannel.QueuePurge(localAddress);
                    var conventions = configuration.Conventions().Conventions;
                    var autoSubscribe = configuration.AutoSubscriptionSettings;
                    var subscribed = new HashSet<Registration>(receivers.Values.Distinct().Where(registration =>
                    {
                        var sagaConvention = registration.UseSagaEventConvention && !autoSubscribe.SagaSubscriptionsDisabled;
                        return registration.Subscribe || ((registration.UseEventConvention || sagaConvention) &&
                            !autoSubscribe.IsDisabled(registration.Type) &&
                            conventions.IsSubscribedEvent(registration.Type, sagaConvention));
                    }));
                    foreach (var entry in receivers.Where(entry => subscribed.Contains(entry.Value)))
                        consumerChannel.QueueBind(localAddress, eventsExchange, entry.Key);
                    consumerChannel.BasicQos(0, configuration.PrefetchCount ?? configuration.MaxConcurrency, false);
                    var consumer = new AsyncEventingBasicConsumer(consumerChannel);
                    consumer.Received += OnReceived;
                    consumerTag = consumerChannel.BasicConsume(localAddress, false, consumer);
                }
                var callback = configuration.OnCriticalError;
                var contextCallback = configuration.CriticalErrorAction;
                if (contextCallback != null)
                    callback = (error, exception) => contextCallback(new CriticalErrorContext(Stop, error, exception));
                if (callback != null)
                    criticalErrors = new CriticalErrorMonitor(callback, configuration.CriticalErrorThreshold, stopping.Token);

                var outbox = Task.Run(() => DispatchPending(false, stopping.Token));
                dispatcher = sendOnly ? outbox : Task.WhenAll(outbox, Task.Run(() => DispatchPending(true, stopping.Token)));
                // A host callback can stop the endpoint, so shutdown must not wait for the callback.
                if (criticalErrors != null) _ = Task.Run(() => criticalErrors.Run(() => connection.IsOpen));
            }
            catch { connection.Dispose(); throw; }
        }

        public static RabbitEndpoint Start(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrWhiteSpace(configuration.EventsExchange) ||
                string.IsNullOrWhiteSpace(configuration.ErrorExchange) ||
                configuration.EventsExchange == configuration.ErrorExchange)
                throw new ArgumentException("Configure distinct, nonempty event and error exchange names.");
            if (configuration.InputQueueArguments.ContainsKey("x-dead-letter-exchange") ||
                configuration.InputQueueArguments.ContainsKey("x-dead-letter-routing-key"))
                throw new ArgumentException("Bq configures dead-letter routing. Use ErrorExchange and Recoverability.ErrorQueue.");
            if (configuration.IsSendOnly && (configuration.Handlers.Count > 0 || configuration.HasSagas))
                throw new InvalidOperationException("A send-only endpoint cannot register message handlers or sagas.");
            if (configuration.Persistence == null || !(configuration.Persistence is IRecoveryPersistence)) throw new InvalidOperationException("Transactional persistence with recovery support is required.");
            if (configuration.Recoverability.DelayedRetries == null || configuration.Recoverability.DelayedRetries.Any(delay => delay < TimeSpan.Zero || delay > TimeSpan.FromDays(24))) throw new ArgumentException("Delayed retry intervals must be between zero and 24 days.");
            if (configuration.Recoverability.ErrorQueue != null && string.IsNullOrWhiteSpace(configuration.Recoverability.ErrorQueue)) throw new ArgumentException("Error queue cannot be empty.");
            if ((configuration.Recoverability.ErrorQueue ?? configuration.Name + ".error") == (configuration.LocalAddress ?? configuration.Name)) throw new ArgumentException("Error queue must differ from the endpoint queue.");
            if (configuration.DispatchPollInterval < TimeSpan.FromMilliseconds(1) ||
                configuration.DispatchPollInterval > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(configuration.DispatchPollInterval));
            if (configuration.PublisherConfirmTimeout < TimeSpan.FromMilliseconds(1) ||
                configuration.PublisherConfirmTimeout > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(configuration.PublisherConfirmTimeout));
            if (configuration.NetworkRecoveryInterval < TimeSpan.FromMilliseconds(1) ||
                configuration.NetworkRecoveryInterval > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(configuration.NetworkRecoveryInterval));
            if ((configuration.OnCriticalError != null || configuration.CriticalErrorAction != null) &&
                configuration.CriticalErrorThreshold <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(configuration.CriticalErrorThreshold));
            if (configuration.MaxConcurrency == 0) throw new ArgumentOutOfRangeException(nameof(configuration.MaxConcurrency));
            if (configuration.ImmediateRetries < 0) throw new ArgumentOutOfRangeException(nameof(configuration.ImmediateRetries));
            return new RabbitEndpoint(configuration);
        }
        // Create inside each DI/request/handler scope rather than caching the result.
        public IUniformSession CreateUniformSession()
        {
            lock (lifecycleLock)
            {
                var scope = uniformScope.Value;
                var transaction = externalTransaction.Value;
                if (disposed || (shutdownRequested && scope == null && transaction == null)) throw new InvalidOperationException("Endpoint is stopping or stopped.");
                if (scope != null && Volatile.Read(ref scope.Active) == 0) throw new InvalidOperationException("Message handling scope has ended.");
                if (transaction != null) return transaction;
                return new UniformSession(this, scope);
            }
        }
        public Task ExecuteTransaction(Func<ITransactionalSession, Task> action) =>
            ExecuteTransaction(action, CancellationToken.None);

        public Task ExecuteTransaction(Func<ITransactionalSession, Task> action, CancellationToken cancellationToken)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            return ExecuteTransaction(async session =>
            {
                await action(session).ConfigureAwait(false);
                return true;
            }, cancellationToken);
        }

        public Task<TResult> ExecuteTransaction<TResult>(Func<ITransactionalSession, Task<TResult>> action) =>
            ExecuteTransaction(action, CancellationToken.None);

        public async Task<TResult> ExecuteTransaction<TResult>(Func<ITransactionalSession, Task<TResult>> action, CancellationToken cancellationToken)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            cancellationToken.ThrowIfCancellationRequested();
            if (uniformScope.Value != null || externalTransaction.Value != null)
                throw new InvalidOperationException("Use the existing session inside a messaging transaction.");
            lock (lifecycleLock)
            {
                if (shutdownRequested) throw new InvalidOperationException("Endpoint is stopping or stopped.");
                activeDeliveries++;
            }
            try
            {
                using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, forceStop.Token))
                using (var persistence = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                {
                    cancellation.Token.ThrowIfCancellationRequested();
                    var session = new TransactionalSession(this, persistence, cancellation.Token);
                    externalTransaction.Value = session;
                    TResult result;
                    try { result = await action(session).ConfigureAwait(false); }
                    finally
                    {
                        session.End();
                        externalTransaction.Value = null;
                    }
                    cancellation.Token.ThrowIfCancellationRequested();
                    persistence.Commit();
                    return result;
                }
            }
            finally { CompleteActiveWork(); }
        }

        sealed class TransactionalSession : ITransactionalSession, IImmediateDispatch, IEnvelopeSession
        {
            readonly RabbitEndpoint endpoint;
            readonly IPersistenceSession persistence;
            readonly TransactionalSession root;
            readonly IDictionary<string, object> headers;
            volatile bool ended;

            public TransactionalSession(RabbitEndpoint endpoint, IPersistenceSession persistence, CancellationToken cancellationToken)
                : this(endpoint, persistence, cancellationToken, null, null) { }

            TransactionalSession(RabbitEndpoint endpoint, IPersistenceSession persistence, CancellationToken cancellationToken,
                TransactionalSession root, IDictionary<string, object> headers)
            {
                this.endpoint = endpoint;
                this.persistence = persistence;
                CancellationToken = cancellationToken;
                this.root = root ?? this;
                this.headers = headers ?? new Dictionary<string, object>();
            }

            IPersistenceSession ActivePersistence => !root.ended && ReferenceEquals(endpoint.externalTransaction.Value, root)
                ? root.persistence : throw new InvalidOperationException("The transactional session is outside its active scope.");
            public CancellationToken CancellationToken { get; }
            public System.Data.Common.DbConnection Connection => ActivePersistence.Connection;
            public System.Data.Common.DbTransaction Transaction => ActivePersistence.Transaction;
            public IUniformSession WithHeader(string name, object value) =>
                new TransactionalSession(endpoint, persistence, CancellationToken, root, AddHeader(headers, name, value));
            IUniformSession IEnvelopeSession.WithEnvelope(MessageOptions options) =>
                new TransactionalSession(endpoint, persistence, CancellationToken, root, options.ApplyEnvelope(headers));
            public Task Send<T>(T message, SendOptions options) => MessageOptionsExtensions.SendUsingOptions(this, message, options);
            public Task Send(object message, SendOptions options) => Send<object>(message, options);
            public Task Send<T>(Action<T> initialize, SendOptions options) =>
                Send(MessageOptionsExtensions.Initialize(initialize), options);
            public Task Send(string message, SendOptions options) => Send<string>(message, options);
            public Task Publish<T>(T message, PublishOptions options) => MessageOptionsExtensions.Publish(this, message, options);
            public Task Publish(object message, PublishOptions options) => Publish<object>(message, options);
            public Task Publish<T>(Action<T> initialize, PublishOptions options) =>
                Publish(MessageOptionsExtensions.Initialize(initialize), options);
            public Task Send<T>(Action<T> initialize) => Send(MessageOptionsExtensions.Initialize(initialize));
            public Task Send<T>(T message)
            {
                return Send(endpoint.RouteFor(message), message);
            }
            public async Task Send<T>(string destination, T message)
            {
                var persistence = ActivePersistence;
                var outgoing = await endpoint.Prepare(message, "", RequireDestination(destination), null, true, headers).ConfigureAwait(false);
                await persistence.StoreOutgoing(outgoing).ConfigureAwait(false);
            }
            public Task SendLocal<T>(T message) => Send(endpoint.localAddress, message);
            public Task SendAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.RouteFor(message), message, dueAt);
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.localAddress, message, dueAt);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => SendAt(destination, message, dueAt, headers);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt, IDictionary<string, object> messageHeaders) =>
                endpoint.StoreScheduled(ActivePersistence, destination, message, dueAt.UtcDateTime, null, messageHeaders);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt) => PublishAt(message, dueAt, headers);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt, IDictionary<string, object> messageHeaders) =>
                endpoint.StoreScheduledEvent(ActivePersistence, message, dueAt.UtcDateTime, null, messageHeaders);
            public Task Publish<T>(Action<T> initialize) => Publish(MessageOptionsExtensions.Initialize(initialize));
            public async Task Publish<T>(T message)
            {
                var persistence = ActivePersistence;
                var outgoing = await endpoint.Prepare(message, endpoint.eventsExchange, endpoint.WireName(ContractType(message)),
                    null, false, headers).ConfigureAwait(false);
                await persistence.StoreOutgoing(outgoing).ConfigureAwait(false);
            }
            Task IImmediateDispatch.SendImmediately<T>(T message, string destination, bool local)
            {
                _ = ActivePersistence;
                return endpoint.SendCore(message, null, local ? endpoint.localAddress : destination, headers);
            }
            Task IImmediateDispatch.PublishImmediately<T>(T message)
            {
                _ = ActivePersistence;
                return endpoint.PublishCore(message, null, headers);
            }
            public void End() => root.ended = true;
        }

        static IDictionary<string, object> AddHeader(IDictionary<string, object> current, string name, object value)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Message header name cannot be empty.", nameof(name));
            if (name.StartsWith("x-bq-", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("The x-bq- header prefix is reserved.", nameof(name));
            if (Encoding.UTF8.GetByteCount(name) > 255)
                throw new ArgumentException("Message header names cannot exceed 255 UTF-8 bytes.", nameof(name));
            var result = new Dictionary<string, object>(current);
            result[name] = value;
            return result;
        }

        public Task SendAt<T>(T message, DateTimeOffset dueAt) => SendAt(RouteFor(message), message, dueAt);
        public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => SendAt(localAddress, message, dueAt);
        public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) =>
            ExecuteTransaction(session => session.SendAt(destination, message, dueAt));

        Task SendAtWithHeaders<T>(string destination, T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
            ExecuteTransaction(session => ((TransactionalSession)session).SendAt(destination, message, dueAt, headers));

        public Task PublishAt<T>(T message, DateTimeOffset dueAt) =>
            ExecuteTransaction(session => session.PublishAt(message, dueAt));

        Task PublishAtWithHeaders<T>(T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
            ExecuteTransaction(session => ((TransactionalSession)session).PublishAt(message, dueAt, headers));

        async Task StoreScheduledEvent<T>(IPersistenceSession session, T message, DateTime dueUtc, string correlationId,
            IDictionary<string, object> headers = null) =>
            await session.StoreTimeout(
                await Prepare(message, eventsExchange, WireName(ContractType(message)), correlationId, false, headers, deferred: true).ConfigureAwait(false), dueUtc).ConfigureAwait(false);

        async Task StoreScheduled<T>(IPersistenceSession session, string destination, T message, DateTime dueUtc, string correlationId,
            IDictionary<string, object> headers = null) =>
            await session.StoreTimeout(
                await Prepare(message, "", RequireDestination(destination), correlationId, true, headers, deferred: true).ConfigureAwait(false), dueUtc).ConfigureAwait(false);

        public Task Subscribe(Type eventType, SubscribeOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            return ChangeSubscription(eventType, subscribe: true);
        }

        public Task Unsubscribe(Type eventType, UnsubscribeOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            return ChangeSubscription(eventType, subscribe: false);
        }

        Task ChangeSubscription(Type eventType, bool subscribe)
        {
            if (eventType == null) throw new ArgumentNullException(nameof(eventType));
            if (sendOnly) throw new InvalidOperationException("A send-only endpoint has no queue to subscribe.");
            var names = receivers.Where(entry => entry.Value.Type == eventType).Select(entry => entry.Key).ToArray();
            if (names.Length == 0)
                throw new InvalidOperationException("No handler registered for " + eventType.FullName);

            lock (consumerLock)
            {
                lock (lifecycleLock)
                    if (shutdownRequested) throw new InvalidOperationException("Endpoint is stopping or stopped.");
                foreach (var name in names)
                {
                    if (subscribe) consumerChannel.QueueBind(localAddress, eventsExchange, name);
                    else consumerChannel.QueueUnbind(localAddress, eventsExchange, name);
                }
            }
            return Task.CompletedTask;
        }

        public Task Send<T>(T message, SendOptions options) => CreateUniformSession().Send(message, options);
        public Task Send(object message, SendOptions options) => Send<object>(message, options);
        public Task Send<T>(Action<T> initialize, SendOptions options) =>
            Send(MessageOptionsExtensions.Initialize(initialize), options);
        public Task Send(string message, SendOptions options) => Send<string>(message, options);
        public Task Publish<T>(T message, PublishOptions options) => CreateUniformSession().Publish(message, options);
        public Task Publish(object message, PublishOptions options) => Publish<object>(message, options);
        public Task Publish<T>(Action<T> initialize, PublishOptions options) =>
            Publish(MessageOptionsExtensions.Initialize(initialize), options);
        public Task Send<T>(T message) => SendCore(message, null, null);
        public Task Send<T>(Action<T> initialize) => Send(MessageOptionsExtensions.Initialize(initialize));
        public Task Send<T>(string destination, T message) => SendCore(message, null, RequireDestination(destination));
        static string RequireDestination(string destination)
        {
            if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentException("A destination queue is required.", nameof(destination));
            return destination;
        }
        public Task SendLocal<T>(T message) => SendCore(message, null, localAddress);
        public Task Publish<T>(T message) => PublishCore(message, null);
        public Task Publish<T>(Action<T> initialize) => Publish(MessageOptionsExtensions.Initialize(initialize));
        internal Task SendCore<T>(T message, string correlationId, string destination, IDictionary<string, object> headers = null)
        {
            return Dispatch(message, "", destination ?? RouteFor(message), correlationId, true, headers);
        }
        static Type ContractType<T>(T message) => typeof(T) == typeof(object)
            ? InterfaceMessage.ContractFor((message ?? throw new ArgumentNullException(nameof(message))).GetType())
            : typeof(T);

        string RouteFor<T>(T message)
        {
            var contract = ContractType(message);
            if (configuration.Routes.TryGetValue(contract, out var destination)) return destination;
            throw new InvalidOperationException("No route for " + contract.FullName);
        }

        string WireName(Type type) => wireNames.TryGetValue(type, out var name)
            ? name : EndpointConfiguration.MessageType(type);

        internal Task PublishCore<T>(T message, string correlationId, IDictionary<string, object> headers = null) =>
            Dispatch(message, eventsExchange, WireName(ContractType(message)), correlationId, false, headers);
        async Task<OutgoingMessage> Prepare<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory,
            IDictionary<string, object> headers = null, bool deferred = false)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            var copiedHeaders = new Dictionary<string, object>(configuration.OutgoingHeaders);
            if (headers != null)
                foreach (var header in headers) copiedHeaders[header.Key] = header.Value;
            copiedHeaders.TryGetValue(MessageOptions.WireIdHeader, out var customId);
            copiedHeaders.TryGetValue(MessageOptions.ReplyAddressHeader, out var replyAddress);
            copiedHeaders.Remove(MessageOptions.WireIdHeader);
            copiedHeaders.Remove(MessageOptions.ReplyAddressHeader);
            copiedHeaders[MessageHeaders.TimeSent] = DateTimeOffset.UtcNow.ToString("O", System.Globalization.CultureInfo.InvariantCulture);
            if (deferred) copiedHeaders[MessageHeaders.Deferred] = true;
            var scope = uniformScope.Value;
            var contract = ContractType(message);
            var causingContext = scope != null && Volatile.Read(ref scope.Active) != 0 ? scope.Context : null;
            object outgoingMessage = message;
            foreach (var mutator in outgoingMessageMutators)
            {
                var original = MessageHeaders.AsText(copiedHeaders);
                var mutation = new MutateOutgoingMessageContext(outgoingMessage,
                    original.ToDictionary(header => header.Key, header => header.Value, StringComparer.Ordinal),
                    causingContext?.LogicalMessage, causingContext?.MessageHeaders);
                await mutator.MutateOutgoing(mutation).ConfigureAwait(false);
                if (!contract.IsInstanceOfType(mutation.OutgoingMessage))
                    throw new InvalidOperationException("An outgoing replacement must implement the declared message contract.");
                RecoveryWire.ApplyTextHeaderEdits(copiedHeaders, original, mutation.OutgoingHeaders);
                outgoingMessage = mutation.OutgoingMessage;
            }
            var serializationType = InterfaceMessage.SerializationType(contract);
            var value = InterfaceMessage.SerializationValue(contract, serializationType, outgoingMessage);
            var transport = new TransportMessage(WireName(contract),
                MessagePackSerializer.Serialize(serializationType, value, SerializerOptions), copiedHeaders,
                causingContext);
            mutateOutgoing?.Invoke(transport);
            foreach (var mutator in outgoingTransportMutators)
            {
                var original = MessageHeaders.AsText(transport.Headers);
                var mutation = new MutateOutgoingTransportMessageContext(transport.Body, outgoingMessage,
                    original.ToDictionary(header => header.Key, header => header.Value, StringComparer.Ordinal),
                    causingContext?.LogicalMessage, causingContext?.MessageHeaders);
                await mutator.MutateOutgoing(mutation).ConfigureAwait(false);
                RecoveryWire.ApplyTextHeaderEdits(transport.Headers, original, mutation.OutgoingHeaders);
                transport.Body = mutation.OutgoingBody;
            }
            transport.Headers.Remove(MessageOptions.WireIdHeader);
            transport.Headers.Remove(MessageOptions.ReplyAddressHeader);
            transport.Headers.Remove(AuditExpirationHeader);
            if (customId != null) transport.Headers[MessageOptions.WireIdHeader] = customId;
            if (string.IsNullOrWhiteSpace(transport.TypeName) || transport.Body == null)
                throw new InvalidOperationException("An outgoing transport message requires a type name and body.");
            foreach (var header in copiedHeaders.Keys)
                if (string.IsNullOrWhiteSpace(header)) throw new ArgumentException("Message header names cannot be empty.", nameof(headers));
            RecoveryWire.EncodeHeaders(copiedHeaders); // Validate the AMQP field types before storing or publishing.
            var id = Guid.NewGuid().ToString("N");
            return new OutgoingMessage { Id = id, CorrelationId = correlationId ?? (string)customId ?? id,
                ReplyTo = (string)replyAddress ?? configuration.PublicReturnAddress ?? (sendOnly ? null : localAddress),
                Type = transport.TypeName, Exchange = exchange, RoutingKey = routingKey,
                Mandatory = mandatory, Headers = copiedHeaders, Body = transport.Body };
        }
        async Task Dispatch<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory,
            IDictionary<string, object> headers = null)
        {
            PublishPrepared(await Prepare(message, exchange, routingKey, correlationId, mandatory, headers).ConfigureAwait(false));
        }
        void PublishPrepared(OutgoingMessage message)
        {
            lock (publishLock)
            {
                if (disposed) throw new ObjectDisposedException(nameof(RabbitEndpoint));
                var props = publisherChannel.CreateBasicProperties();
                props.Persistent = true;
                props.MessageId = message.Id;
                props.CorrelationId = message.CorrelationId;
                props.ReplyTo = message.ReplyTo;
                props.Type = message.Type;
                props.ContentType = ContentType;
                if (message.Headers != null && message.Headers.Count > 0)
                {
                    var headers = new Dictionary<string, object>(message.Headers);
                    // A forwarded delivery has its own outbox record ID but keeps its original wire ID.
                    if (headers.TryGetValue(MessageOptions.WireIdHeader, out var originalId) && originalId is string id)
                        props.MessageId = id;
                    headers.Remove(MessageOptions.WireIdHeader);
                    if (headers.TryGetValue(AuditExpirationHeader, out var expiration) && expiration is string ttl)
                        props.Expiration = ttl;
                    headers.Remove(AuditExpirationHeader);
                    props.Headers = headers;
                }
                returned = false;
                publisherChannel.BasicPublish(message.Exchange, message.RoutingKey, message.Mandatory, props, message.Body);
                if (!publisherChannel.WaitForConfirms(publisherConfirmTimeout)) throw new InvalidOperationException("RabbitMQ did not confirm the publish.");
                if (returned) throw new InvalidOperationException("Message was unroutable: " + message.RoutingKey);
            }
        }
        async Task DispatchPending(bool recoveries, CancellationToken cancellation)
        {
            var probe = recoveries ? criticalErrors?.Recovery : criticalErrors?.Outbox;
            while (!cancellation.IsCancellationRequested)
            {
                try
                {
                    probe?.Started();
                    var dispatched = await DispatchNext(recoveries).ConfigureAwait(false);
                    probe?.Succeeded();
                    if (dispatched) continue;
                }
                catch (Exception ex)
                {
                    probe?.Failed(ex);
                    if (!cancellation.IsCancellationRequested)
                        System.Diagnostics.Trace.TraceError("{0} dispatch failed: {1}", recoveries ? "Recovery" : "Outbox", ex);
                }
                try { await Task.Delay(dispatchPollInterval, cancellation).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }
        async Task<bool> DispatchNext(bool recoveries)
        {
            var persistence = configuration.Persistence;
            var endpoint = configuration.Name;
            if (persistence is ILockedDispatchPersistence locked)
            {
                if (!recoveries) return await locked.DispatchOutgoing(endpoint, PublishPrepared).ConfigureAwait(false);
                RecoveryRecord published = null;
                var dispatched = await locked.DispatchRecovery(endpoint, record =>
                {
                    PublishRecovery(record);
                    published = record;
                }).ConfigureAwait(false);
                if (dispatched) await NotifyErrorQueue(published).ConfigureAwait(false);
                return dispatched;
            }

            if (recoveries)
            {
                var store = (IRecoveryPersistence)persistence;
                var recovery = await store.NextRecovery(endpoint).ConfigureAwait(false);
                if (recovery == null) return false;
                PublishRecovery(recovery);
                await store.MarkRecoveryDispatched(endpoint, recovery.RecoveryId).ConfigureAwait(false);
                await NotifyErrorQueue(recovery).ConfigureAwait(false);
            }
            else
            {
                var message = await persistence.NextOutgoing(endpoint).ConfigureAwait(false);
                if (message == null) return false;
                PublishPrepared(message);
                await persistence.MarkDispatched(endpoint, message.Id).ConfigureAwait(false);
            }
            return true;
        }

        async Task NotifyErrorQueue(RecoveryRecord record)
        {
            var notification = configuration.Recoverability.ErrorQueueNotification;
            if (!record.IsError || notification == null) return;
            try
            {
                var envelope = RecoveryWire.Decode(record.Payload);
                var message = new IncomingMessage(record.MessageId, RecoveryWire.Headers(envelope), envelope.Body);
                var exception = new Exception(record.ExceptionMessage);
                exception.Data[nameof(record.ExceptionType)] = record.ExceptionType;
                exception.Data[nameof(record.ExceptionDetail)] = record.ExceptionDetail;
                await notification(new FailedMessage(message.MessageId, message.Headers, message.Body, exception,
                    record.ErrorQueue)).ConfigureAwait(false);
            }
            catch (Exception notificationFailure)
            {
                System.Diagnostics.Trace.TraceError("Error queue notification failed for message {0}: {1}",
                    record.MessageId, notificationFailure);
            }
        }

        static string RecoveryId(string messageId, int cycle)
        {
            using (var sha = SHA256.Create())
            {
                var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(messageId + "\n" + cycle.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                return BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant();
            }
        }
        void PublishRecovery(RecoveryRecord record)
        {
            var envelope = RecoveryWire.Decode(record.Payload);
            lock (publishLock)
            {
                if (disposed) throw new ObjectDisposedException(nameof(RabbitEndpoint));
                var props = publisherChannel.CreateBasicProperties();
                RecoveryWire.Apply(envelope, props);
                props.Persistent = true;
                returned = false;
                publisherChannel.BasicPublish("", record.IsError ? record.ErrorQueue : localAddress, true, props, envelope.Body);
                if (!publisherChannel.WaitForConfirms(publisherConfirmTimeout)) throw new InvalidOperationException("RabbitMQ did not confirm the publish.");
                if (returned) throw new InvalidOperationException("Recovery message was unroutable.");
            }
        }
        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(int limit = 100) => ((IRecoveryPersistence)configuration.Persistence).ListErrors(configuration.Name, limit);
        public async Task<bool> ReplayError(string recoveryId)
        {
            if (string.IsNullOrEmpty(recoveryId)) throw new ArgumentException("Recovery ID is required.", nameof(recoveryId));
            // Look up a specific record independently of the inspection page limit.
            var original = await ((IRecoveryPersistence)configuration.Persistence).GetError(configuration.Name, recoveryId).ConfigureAwait(false);
            if (original == null) return false;
            var envelope = RecoveryWire.Decode(original.Payload);
            RecoveryWire.SetOriginalId(envelope, original.MessageId);
            envelope.MessageId = Guid.NewGuid().ToString("N");
            var replay = new RecoveryRecord { RecoveryId = "replay-" + Guid.NewGuid().ToString("N"), MessageId = envelope.MessageId,
                Cycle = 0, FailedAtUtc = DateTime.UtcNow, DueAtUtc = DateTime.UtcNow, Payload = RecoveryWire.Encode(envelope) };
            return await ((IRecoveryPersistence)configuration.Persistence).ReplayError(configuration.Name, recoveryId, replay).ConfigureAwait(false);
        }
        async Task OnReceived(object sender, BasicDeliverEventArgs delivery)
        {
            lock (lifecycleLock)
            {
                // The broker will requeue a delivery left unacknowledged when the channel closes.
                if (shutdownRequested) return;
                activeDeliveries++;
            }
            try { await ProcessDelivery(delivery).ConfigureAwait(false); }
            finally { CompleteActiveWork(); }
        }
        void CompleteActiveWork()
        {
            lock (lifecycleLock)
            {
                activeDeliveries--;
                if (shutdownRequested && activeDeliveries == 0) drained.TrySetResult(true);
            }
        }
        async Task ProcessDelivery(BasicDeliverEventArgs delivery)
        {
            var props = delivery.BasicProperties;
            var cycle = 0;
            var currentId = props.MessageId;
            Exception failure = null;
            RecoveryAction action = null;
            var unrecoverable = false;
            try
            {
                try { cycle = RecoveryWire.Cycle(props); }
                catch { unrecoverable = true; throw; }
                if (string.IsNullOrEmpty(currentId)) throw new InvalidOperationException("Message ID is required.");
                var typeName = props.Type;
                var body = delivery.Body;
                var headers = props.Headers;
                if (mutateIncoming != null || incomingTransportMutators.Length != 0)
                {
                    // Mutators work on a separate copy so recovery can replay the original wire bytes.
                    var transport = new TransportMessage(typeName, body.ToArray(),
                        RecoveryWire.DecodeHeaders(RecoveryWire.EncodeHeaders(headers)));
                    mutateIncoming?.Invoke(transport);
                    foreach (var mutator in incomingTransportMutators)
                    {
                        var original = MessageHeaders.AsText(transport.Headers);
                        var mutation = new MutateIncomingTransportMessageContext(transport.Body,
                            original.ToDictionary(header => header.Key, header => header.Value, StringComparer.Ordinal));
                        await mutator.MutateIncoming(mutation).ConfigureAwait(false);
                        RecoveryWire.ApplyTextHeaderEdits(transport.Headers, original, mutation.Headers);
                        transport.Body = mutation.Body;
                    }
                    typeName = transport.TypeName;
                    headers = transport.Headers;
                    body = transport.Body ?? throw new InvalidOperationException("An incoming transport message requires a body.");
                }
                if (props.ContentType != ContentType || typeName == null || !receivers.TryGetValue(typeName, out var registration))
                { unrecoverable = true; throw new InvalidOperationException("Unknown message type or content type."); }
                object message;
                try { message = MessagePackSerializer.Deserialize(InterfaceMessage.SerializationType(registration.Type), body, SerializerOptions); }
                catch (Exception ex) { unrecoverable = true; throw new InvalidOperationException("Message deserialization failed.", ex); }
                for (var attempt = 0; ; attempt++)
                {
                    HandlerContext context = null;
                    try
                    {
                        using (var session = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                        {
                            var recovery = session as IRecoverySession ?? throw new InvalidOperationException("Persistence session does not support recovery.");
                            if (!await recovery.HasRecovery(configuration.Name, RecoveryId(currentId, cycle)).ConfigureAwait(false) &&
                                await session.TryClaimMessage(configuration.Name, currentId).ConfigureAwait(false))
                            {
                                context = new HandlerContext(this, props, delivery.Body, session, headers);
                                var previous = uniformScope.Value;
                                var scope = new UniformScope(context);
                                uniformScope.Value = scope;
                                try
                                {
                                    var incomingContext = new IncomingMessageContext(message, context, registration.Type, typeName, attempt, cycle);
                                    await incoming(incomingContext, async () =>
                                    {
                                        await registration.Invoke(incomingContext.Message, context).ConfigureAwait(false);
                                        incomingContext.MessageHandled = true;
                                    }).ConfigureAwait(false);
                                    if (incomingContext.MessageHandled && !incomingContext.IsSagaTimeout)
                                    {
                                        if (forwardReceivedMessagesAddress != null)
                                            await context.ForwardCurrentMessageTo(forwardReceivedMessagesAddress).ConfigureAwait(false);
                                        if (auditQueue != null)
                                            await context.ForwardCurrentMessageTo(auditQueue, auditExpiration).ConfigureAwait(false);
                                    }
                                }
                                finally { Volatile.Write(ref scope.Active, 0); uniformScope.Value = previous; }
                            }
                            forceStop.Token.ThrowIfCancellationRequested();
                            session.Commit();
                        }
                        break;
                    }
                    catch (Exception ex) when (!forceStop.IsCancellationRequested)
                    {
                        action = ChooseRecovery(ex, attempt, cycle, headers, currentId, delivery.Body, context?.Extensions, allowImmediate: true);
                        if (!action.IsImmediate) throw;
                        System.Diagnostics.Trace.TraceWarning("Immediate retry {0} for message {1}: {2}", attempt + 1, currentId, ex);
                        var notification = configuration.Recoverability.ImmediateRetryNotification;
                        if (notification != null)
                        {
                            try
                            {
                                var snapshot = new IncomingMessage(currentId, headers, delivery.Body);
                                await notification(new ImmediateRetryMessage(snapshot.MessageId, snapshot.Headers, snapshot.Body,
                                    ex, attempt + 1)).ConfigureAwait(false);
                            }
                            catch (Exception notificationFailure)
                            {
                                System.Diagnostics.Trace.TraceError("Immediate retry notification failed for message {0}: {1}",
                                    currentId, notificationFailure);
                            }
                        }
                        // Every attempt must deserialize afresh, since a failed handler may mutate the object.
                        action = null;
                        message = MessagePackSerializer.Deserialize(InterfaceMessage.SerializationType(registration.Type), body, SerializerOptions);
                    }
                }
                AcknowledgeCommittedDelivery(delivery.DeliveryTag);
                return;
            }
            catch (Exception ex) { failure = ex; }
            if (forceStop.IsCancellationRequested) return;
            try
            {
                var options = configuration.Recoverability;
                action = unrecoverable ? RecoveryAction.MoveToError()
                    : action ?? ChooseRecovery(failure, 0, cycle, props.Headers, currentId, delivery.Body, null, allowImmediate: false);
                if (action.DiscardReason != null && !string.IsNullOrEmpty(currentId))
                {
                    using (var session = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                    {
                        await session.TryClaimMessage(configuration.Name, currentId).ConfigureAwait(false);
                        session.Commit();
                    }
                    System.Diagnostics.Trace.TraceWarning("Message {0} discarded: {1}", currentId, action.DiscardReason);
                    AcknowledgeCommittedDelivery(delivery.DeliveryTag);
                    return;
                }
                var isError = !action.Delay.HasValue;
                var envelope = RecoveryWire.Capture(props, delivery.Body);
                // A missing message ID cannot be deduplicated across redelivery, but the
                // failed message can still be persisted without losing its body.
                if (string.IsNullOrEmpty(currentId)) { currentId = Guid.NewGuid().ToString("N"); envelope.MessageId = currentId; isError = true; }
                RecoveryWire.SetFailure(envelope, failure, cycle, configuration.Name);
                if (!isError) RecoveryWire.SetCycle(envelope, checked(cycle + 1));
                else if (options.CustomizeErrorHeaders != null)
                {
                    try { RecoveryWire.CustomizeHeaders(envelope, options.CustomizeErrorHeaders); }
                    catch (Exception customizationFailure)
                    {
                        System.Diagnostics.Trace.TraceError("Error header customization failed for message {0}: {1}",
                            currentId, customizationFailure);
                    }
                }
                var now = DateTime.UtcNow;
                var record = new RecoveryRecord { RecoveryId = RecoveryId(currentId, cycle), MessageId = currentId, Cycle = cycle,
                    IsError = isError, ErrorQueue = action.ErrorQueue ?? options.ErrorQueue ?? configuration.Name + ".error", ExceptionType = failure.GetType().FullName,
                    ExceptionMessage = failure.Message, ExceptionDetail = failure.ToString(), FailedAtUtc = now,
                    DueAtUtc = isError ? now : now.Add(action.Delay.Value), Payload = RecoveryWire.Encode(envelope) };
                using (var session = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                {
                    var recovery = session as IRecoverySession ?? throw new InvalidOperationException("Persistence session does not support recovery.");
                    await recovery.StoreRecovery(configuration.Name, record).ConfigureAwait(false);
                    session.Commit();
                }
                System.Diagnostics.Trace.TraceError("Message {0} failed; {1}: {2}", currentId, isError ? "error queue" : "delayed retry", failure);
                var notification = options.DelayedRetryNotification;
                if (!isError && notification != null)
                {
                    try
                    {
                        var snapshot = new IncomingMessage(currentId, props.Headers, delivery.Body);
                        await notification(new DelayedRetryMessage(snapshot.MessageId, snapshot.Headers, snapshot.Body,
                            failure, cycle + 1)).ConfigureAwait(false);
                    }
                    catch (Exception notificationFailure)
                    {
                        System.Diagnostics.Trace.TraceError("Delayed retry notification failed for message {0}: {1}",
                            currentId, notificationFailure);
                    }
                }
            }
            catch (Exception handoffError)
            {
                System.Diagnostics.Trace.TraceError("Failed to persist recovery for message {0}; requeuing: {1}", currentId, handoffError);
                await Task.Delay(1000).ConfigureAwait(false);
                lock (consumerLock) { if (!disposed && consumerChannel.IsOpen) consumerChannel.BasicNack(delivery.DeliveryTag, false, true); }
                return;
            }
            AcknowledgeCommittedDelivery(delivery.DeliveryTag);
        }

        RecoveryAction ChooseRecovery(Exception failure, int attempt, int cycle,
            IDictionary<string, object> headers, string messageId, ReadOnlyMemory<byte> body,
            ReadOnlyContextBag extensions, bool allowImmediate)
        {
            if (IsUnrecoverable(failure)) return RecoveryAction.MoveToError();
            var options = configuration.Recoverability;
            try
            {
                RecoveryAction proposed;
                if (options.CompatiblePolicy != null)
                {
                    var config = new RecoverabilityConfig(new ImmediateConfig(options.ImmediateRetries),
                        new DelayedConfig(options.DelayedRetries),
                        new FailedConfig(options.ErrorQueue ?? configuration.Name + ".error", options.UnrecoverableExceptionTypes));
                    var context = new ErrorContext(failure, attempt, cycle, new IncomingMessage(messageId, headers, body), extensions);
                    proposed = options.CompatiblePolicy(config, context)?.ToBqAction();
                }
                else proposed = options.Policy?.Invoke(new RecoveryContext(failure, attempt, cycle, headers));
                if (proposed?.ErrorQueue == localAddress)
                    throw new InvalidOperationException("Error queue must differ from the endpoint queue.");
                if (proposed != null && (allowImmediate || !proposed.IsImmediate)) return proposed;
            }
            catch (Exception policyError)
            {
                System.Diagnostics.Trace.TraceError("Recoverability policy failed: {0}", policyError);
            }

            if (options.UnrecoverableExceptionTypes.Any(type => type.IsInstanceOfType(failure))) return RecoveryAction.MoveToError();
            if (allowImmediate && attempt < options.ImmediateRetries) return RecoveryAction.ImmediateRetry();
            return cycle < options.DelayedRetries.Length ? RecoveryAction.DelayedRetry(options.DelayedRetries[cycle])
                : RecoveryAction.MoveToError();
        }

        bool IsUnrecoverable(Exception failure)
        {
            try
            {
                return configuration.Recoverability.IsUnrecoverable?.Invoke(failure) ?? false;
            }
            catch (Exception policyError)
            {
                System.Diagnostics.Trace.TraceError("Unrecoverable exception classifier failed: {0}", policyError);
                return false;
            }
        }

        void AcknowledgeCommittedDelivery(ulong deliveryTag)
        {
            try
            {
                lock (consumerLock)
                {
                    if (!disposed) consumerChannel.BasicAck(deliveryTag, false);
                }
            }
            catch (Exception ex)
            {
                // The database decision is final. If RabbitMQ redelivers, the inbox
                // or recovery record prevents the committed work from running again.
                System.Diagnostics.Trace.TraceError("Failed to acknowledge committed delivery {0}: {1}", deliveryTag, ex);
            }
        }
        // Stops accepting new deliveries, waits for active handlers, and then closes the connection.
        // Cancellation forces closure and leaves unfinished deliveries for broker redelivery.
        public Task Stop() => StopAsync();

        public Task StopAsync(CancellationToken cancellationToken = default)
        {
            lock (lifecycleLock)
            {
                if (stopTask != null) return stopTask;
                if (disposed) return Task.CompletedTask;
                shutdownRequested = true;
                if (activeDeliveries == 0) drained.TrySetResult(true);
                stopTask = Task.Run(() => StopCore(cancellationToken));
                return stopTask;
            }
        }
        async Task StopCore(CancellationToken cancellationToken)
        {
            try
            {
                // BasicCancel is synchronous in the netstandard2.0 RabbitMQ client.
                var cancel = Task.Run(() => { lock (consumerLock) { if (consumerChannel?.IsOpen == true) consumerChannel.BasicCancel(consumerTag); } });
                await WaitFor(cancel, cancellationToken).ConfigureAwait(false);
                await WaitFor(drained.Task, cancellationToken).ConfigureAwait(false);
                stopping.Cancel();
                if (dispatcher != null) await WaitFor(dispatcher, cancellationToken).ConfigureAwait(false);
                Close(false);
            }
            catch
            {
                Close(true);
                throw;
            }
        }
        static async Task WaitFor(Task task, CancellationToken cancellation)
        {
            if (!cancellation.CanBeCanceled) { await task.ConfigureAwait(false); return; }
            var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellation.Register(() => cancelled.TrySetResult(true)))
            {
                if (await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false) != task) throw new OperationCanceledException(cancellation);
                await task.ConfigureAwait(false);
            }
        }
        void Close(bool forced)
        {
            lock (lifecycleLock)
            {
                if (disposed) return;
                disposed = true;
                shutdownRequested = true;
                if (activeDeliveries == 0) drained.TrySetResult(true);
            }
            if (forced)
            {
                try { forceStop.Cancel(); }
                catch (AggregateException ex) { System.Diagnostics.Trace.TraceError("Handler cancellation callback failed: {0}", ex); }
            }
            stopping.Cancel();
            if (forced) connection.Abort(TimeSpan.FromMilliseconds(500));
            else connection.Dispose();
        }
        // Dispose is immediate; use StopAsync to drain active handlers.
        public void Dispose() => Close(true);
        sealed class UniformScope
        {
            public UniformScope(HandlerContext context) { Context = context; Active = 1; }
            public HandlerContext Context { get; }
            public int Active;
        }
        sealed class UniformSession : IUniformSession, IImmediateDispatch, IEnvelopeSession
        {
            readonly RabbitEndpoint endpoint;
            readonly UniformScope scope;
            readonly IDictionary<string, object> headers;
            public UniformSession(RabbitEndpoint endpoint, UniformScope scope) : this(endpoint, scope, null) { }
            UniformSession(RabbitEndpoint endpoint, UniformScope scope, IDictionary<string, object> headers)
            { this.endpoint = endpoint; this.scope = scope; this.headers = headers ?? new Dictionary<string, object>(); }
            public IUniformSession WithHeader(string name, object value) => new UniformSession(endpoint, scope, AddHeader(headers, name, value));
            IUniformSession IEnvelopeSession.WithEnvelope(MessageOptions options) =>
                new UniformSession(endpoint, scope, options.ApplyEnvelope(headers));
            HandlerContext Validate()
            {
                lock (endpoint.lifecycleLock)
                {
                    if (endpoint.disposed) throw new InvalidOperationException("Endpoint has stopped.");
                    if (scope == null)
                    {
                        if (endpoint.shutdownRequested || endpoint.uniformScope.Value != null || endpoint.externalTransaction.Value != null)
                            throw new InvalidOperationException("An outside uniform session cannot be used inside a messaging transaction or after shutdown starts.");
                        return null;
                    }
                    if (Volatile.Read(ref scope.Active) == 0 || !ReferenceEquals(endpoint.uniformScope.Value, scope))
                        throw new InvalidOperationException("Uniform session is outside its message handling scope.");
                    return scope.Context;
                }
            }
            Task IImmediateDispatch.SendImmediately<T>(T message, string destination, bool local)
            {
                var context = Validate();
                return endpoint.SendCore(message, context?.CorrelationId, local ? endpoint.localAddress : destination, headers);
            }
            Task IImmediateDispatch.PublishImmediately<T>(T message)
            {
                var context = Validate();
                return endpoint.PublishCore(message, context?.CorrelationId, headers);
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
            public Task Send<T>(Action<T> initialize) => Send(MessageOptionsExtensions.Initialize(initialize));
            public Task Send<T>(T message) { var context = Validate(); return context == null ? endpoint.SendCore(message, null, null, headers) : context.SendWithHeaders(message, null, headers); }
            public Task Send<T>(string destination, T message)
            {
                var context = Validate();
                return context == null ? endpoint.SendCore(message, null, RequireDestination(destination), headers) : context.SendWithHeaders(message, RequireDestination(destination), headers);
            }
            public Task SendLocal<T>(T message) { var context = Validate(); return context == null ? endpoint.SendCore(message, null, endpoint.localAddress, headers) : context.SendWithHeaders(message, endpoint.localAddress, headers); }
            public Task Publish<T>(Action<T> initialize) => Publish(MessageOptionsExtensions.Initialize(initialize));
            public Task Publish<T>(T message) { var context = Validate(); return context == null ? endpoint.PublishCore(message, null, headers) : context.PublishWithHeaders(message, headers); }
            public Task SendAt<T>(T message, DateTimeOffset dueAt)
            {
                return SendAt(endpoint.RouteFor(message), message, dueAt);
            }
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.SendAtWithHeaders(destination, message, dueAt, headers) : context.SendAtWithHeaders(destination, message, dueAt, headers);
            }
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.SendAtWithHeaders(endpoint.localAddress, message, dueAt, headers) : context.SendAtWithHeaders(endpoint.localAddress, message, dueAt, headers);
            }
            public Task PublishAt<T>(T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.PublishAtWithHeaders(message, dueAt, headers) : context.PublishAtWithHeaders(message, dueAt, headers);
            }
        }
        internal sealed class HandlerContext : IMessageHandlerContext, IReplyWithOptions
        {
            readonly RabbitEndpoint endpoint;
            readonly string replyTo;
            readonly IBasicProperties wireProperties;
            readonly ReadOnlyMemory<byte> wireBody;

            public HandlerContext(RabbitEndpoint endpoint, IBasicProperties properties, ReadOnlyMemory<byte> wireBody,
                IPersistenceSession session, IDictionary<string, object> headers)
            {
                this.endpoint = endpoint;
                wireProperties = properties;
                this.wireBody = wireBody;
                PersistenceSession = session;
                replyTo = properties.ReplyTo;
                MessageId = properties.MessageId;
                CorrelationId = properties.CorrelationId ?? properties.MessageId;
                Headers = new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(
                    new Dictionary<string, object>(headers ?? new Dictionary<string, object>()));
                LogicalHeaders = Bq.Messaging.MessageHeaders.ToTextDictionary(Headers);
                MessageHeaders = new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(LogicalHeaders);
            }
            public IUniformSession UniformSession => endpoint.CreateUniformSession();
            public CancellationToken CancellationToken => endpoint.forceStop.Token;
            public IPersistenceSession PersistenceSession { get; }
            public SynchronizedStorageSession SynchronizedStorageSession => PersistenceSession;
            public ContextBag Extensions { get; } = new ContextBag();
            public string ReplyToAddress => replyTo;
            public IReadOnlyDictionary<string, string> MessageHeaders { get; }
            internal object LogicalMessage { get; set; }
            internal Dictionary<string, string> LogicalHeaders { get; }
            public string MessageId { get; }
            public string CorrelationId { get; }
            public IReadOnlyDictionary<string, object> Headers { get; }
            public Task Send<T>(T message, SendOptions options) => UniformSession.Send(message, options);
            public Task Send(object message, SendOptions options) => Send<object>(message, options);
            public Task Send<T>(Action<T> initialize, SendOptions options) =>
                Send(MessageOptionsExtensions.Initialize(initialize), options);
            public Task Send(string message, SendOptions options) => Send<string>(message, options);
            public Task Publish<T>(T message, PublishOptions options) => MessageOptionsExtensions.Publish(this, message, options);
            public Task Publish(object message, PublishOptions options) => Publish<object>(message, options);
            public Task Publish<T>(Action<T> initialize, PublishOptions options) =>
                Publish(MessageOptionsExtensions.Initialize(initialize), options);
            public Task Send<T>(T message) => SendWithHeaders(message, null, null);
            public Task Send<T>(Action<T> initialize) => Send(MessageOptionsExtensions.Initialize(initialize));
            internal async Task SendWithHeaders<T>(T message, string destination, IDictionary<string, object> headers)
            {
                destination = destination ?? endpoint.RouteFor(message);
                RequireDestination(destination);
                await PersistenceSession.StoreOutgoing(
                    await endpoint.Prepare(message, "", destination, CorrelationId, true, headers).ConfigureAwait(false)).ConfigureAwait(false);
            }
            public Task Send<T>(string destination, T message) => SendWithHeaders(message, RequireDestination(destination), null);
            public Task SendLocal<T>(T message) => SendWithHeaders(message, endpoint.localAddress, null);
            public Task Publish<T>(T message) => PublishWithHeaders(message, null);
            public Task Publish<T>(Action<T> initialize) => Publish(MessageOptionsExtensions.Initialize(initialize));
            internal async Task PublishWithHeaders<T>(T message, IDictionary<string, object> headers) =>
                await PersistenceSession.StoreOutgoing(await endpoint.Prepare(message, endpoint.eventsExchange,
                    endpoint.WireName(ContractType(message)), CorrelationId, false, headers).ConfigureAwait(false)).ConfigureAwait(false);
            internal Task SendAtWithHeaders<T>(string destination, T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
                endpoint.StoreScheduled(PersistenceSession, destination, message, dueAt.UtcDateTime, CorrelationId, headers);
            internal Task PublishAtWithHeaders<T>(T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
                endpoint.StoreScheduledEvent(PersistenceSession, message, dueAt.UtcDateTime, CorrelationId, headers);
            public Task SendAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.RouteFor(message), message, dueAt);
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.localAddress, message, dueAt);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => SendAtWithHeaders(destination, message, dueAt, null);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt) => PublishAtWithHeaders(message, dueAt, null);

            internal Task ScheduleSagaTimeout(SagaTimeoutMessage timeout, DateTime dueUtc)
            {
                if (PersistenceSession == null) throw new InvalidOperationException("Saga timeouts require persistence.");
                return endpoint.StoreScheduled(PersistenceSession, endpoint.localAddress, timeout, dueUtc, CorrelationId);
            }
            public Task ForwardCurrentMessageTo(string destination) => ForwardCurrentMessageTo(destination, null);

            internal Task ForwardCurrentMessageTo(string destination, string expiration)
            {
                var scope = endpoint.uniformScope.Value;
                if (scope == null || !ReferenceEquals(scope.Context, this) || Volatile.Read(ref scope.Active) == 0)
                    throw new InvalidOperationException("Forwarding requires the active message handling scope.");
                RequireDestination(destination);
                var headers = new Dictionary<string, object>(wireProperties.Headers ?? new Dictionary<string, object>())
                {
                    [MessageOptions.WireIdHeader] = MessageId
                };
                headers.Remove(AuditExpirationHeader);
                if (expiration != null) headers[AuditExpirationHeader] = expiration;
                return PersistenceSession.StoreOutgoing(new OutgoingMessage
                {
                    Id = Guid.NewGuid().ToString("N"), CorrelationId = wireProperties.CorrelationId, ReplyTo = replyTo,
                    Type = wireProperties.Type, Exchange = "", RoutingKey = destination,
                    Mandatory = true, Headers = headers, Body = wireBody.ToArray()
                });
            }

            public Task Reply<T>(T message, ReplyOptions options) => MessageOptionsExtensions.Reply(this, message, options);
            public Task Reply(object message, ReplyOptions options) => Reply<object>(message, options);
            public Task Reply<T>(Action<T> initialize, ReplyOptions options) =>
                Reply(MessageOptionsExtensions.Initialize(initialize), options);
            public Task Reply<T>(T message) => ((IReplyWithOptions)this).ReplyWithOptions(message, null);
            public Task Reply<T>(Action<T> initialize) => Reply(MessageOptionsExtensions.Initialize(initialize));

            async Task IReplyWithOptions.ReplyWithOptions<T>(T message, ReplyOptions options)
            {
                var destination = options?.Destination ?? replyTo;
                if (string.IsNullOrEmpty(destination)) throw new InvalidOperationException("Message has no reply address.");
                var correlationId = options?.CorrelationId ?? CorrelationId;
                var headers = options?.ApplyEnvelope(options.Headers);
                if (PersistenceSession == null || options?.ImmediateDispatch == true)
                    await endpoint.SendCore(message, correlationId, destination, headers).ConfigureAwait(false);
                else
                    await PersistenceSession.StoreOutgoing(
                        await endpoint.Prepare(message, "", destination, correlationId, true, headers).ConfigureAwait(false)).ConfigureAwait(false);
            }
        }
    }
}
