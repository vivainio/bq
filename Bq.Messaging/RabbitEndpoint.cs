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
    public sealed class RabbitEndpoint : IDisposable
    {
        readonly string eventsExchange;
        readonly string errorExchange;
        const string ContentType = "application/x-msgpack";
        static readonly MessagePackSerializerOptions SerializerOptions = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
        readonly EndpointConfiguration configuration;
        readonly Func<IncomingMessageContext, Func<Task>, Task> incoming;
        readonly Dictionary<Type, string> wireNames = new Dictionary<Type, string>();
        readonly Dictionary<string, Registration> receivers = new Dictionary<string, Registration>();
        readonly bool sendOnly;
        readonly TimeSpan dispatchPollInterval;
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
                publisherChannel.ExchangeDeclare(eventsExchange, ExchangeType.Topic, true);
                publisherChannel.ConfirmSelect();
                publisherChannel.BasicReturn += (sender, args) => { returned = true; };
                if (!sendOnly)
                {
                    consumerChannel = connection.CreateModel();
                    consumerChannel.ExchangeDeclare(errorExchange, ExchangeType.Direct, true);
                    var errorQueue = configuration.Recoverability.ErrorQueue ?? configuration.Name + ".error";
                    consumerChannel.QueueDeclare(errorQueue, true, false, false);
                    consumerChannel.QueueBind(errorQueue, errorExchange, configuration.Name);
                    consumerChannel.QueueDeclare(configuration.Name, true, false, false, new Dictionary<string, object>(configuration.InputQueueArguments)
                    {
                        ["x-dead-letter-exchange"] = errorExchange,
                        ["x-dead-letter-routing-key"] = configuration.Name
                    });
                    foreach (var entry in receivers.Where(x => x.Value.Subscribe))
                        consumerChannel.QueueBind(configuration.Name, eventsExchange, entry.Key);
                    consumerChannel.BasicQos(0, configuration.PrefetchCount ?? configuration.MaxConcurrency, false);
                    var consumer = new AsyncEventingBasicConsumer(consumerChannel);
                    consumer.Received += OnReceived;
                    consumerTag = consumerChannel.BasicConsume(configuration.Name, false, consumer);
                }
                var outbox = Task.Run(() => DispatchPending(false, stopping.Token));
                dispatcher = sendOnly ? outbox : Task.WhenAll(outbox, Task.Run(() => DispatchPending(true, stopping.Token)));
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
            if (configuration.Recoverability.ErrorQueue == configuration.Name) throw new ArgumentException("Error queue must differ from the endpoint queue.");
            if (configuration.DispatchPollInterval < TimeSpan.FromMilliseconds(1) ||
                configuration.DispatchPollInterval > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(configuration.DispatchPollInterval));
            if (configuration.PublisherConfirmTimeout < TimeSpan.FromMilliseconds(1) ||
                configuration.PublisherConfirmTimeout > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(configuration.PublisherConfirmTimeout));
            if (configuration.NetworkRecoveryInterval < TimeSpan.FromMilliseconds(1) ||
                configuration.NetworkRecoveryInterval > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(configuration.NetworkRecoveryInterval));
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

        sealed class TransactionalSession : ITransactionalSession
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
            public Task Send<T>(T message)
            {
                if (!endpoint.configuration.Routes.TryGetValue(typeof(T), out var destination))
                    throw new InvalidOperationException("No route for " + typeof(T).FullName);
                return Send(destination, message);
            }
            public Task Send<T>(string destination, T message) =>
                ActivePersistence.StoreOutgoing(endpoint.Prepare(message, "", RequireDestination(destination), null, true, headers));
            public Task SendLocal<T>(T message) => Send(endpoint.configuration.Name, message);
            public Task SendAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.RouteFor<T>(), message, dueAt);
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.configuration.Name, message, dueAt);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => SendAt(destination, message, dueAt, headers);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt, IDictionary<string, object> messageHeaders) =>
                endpoint.StoreScheduled(ActivePersistence, destination, message, dueAt.UtcDateTime, null, messageHeaders);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt) => PublishAt(message, dueAt, headers);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt, IDictionary<string, object> messageHeaders) =>
                endpoint.StoreScheduledEvent(ActivePersistence, message, dueAt.UtcDateTime, null, messageHeaders);
            public Task Publish<T>(T message) => ActivePersistence.StoreOutgoing(endpoint.Prepare(
                message, endpoint.eventsExchange, endpoint.WireName(typeof(T)), null, false, headers));
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

        public Task SendAt<T>(T message, DateTimeOffset dueAt) => SendAt(RouteFor<T>(), message, dueAt);
        public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => SendAt(configuration.Name, message, dueAt);
        public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) =>
            ExecuteTransaction(session => session.SendAt(destination, message, dueAt));

        Task SendAtWithHeaders<T>(string destination, T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
            ExecuteTransaction(session => ((TransactionalSession)session).SendAt(destination, message, dueAt, headers));

        public Task PublishAt<T>(T message, DateTimeOffset dueAt) =>
            ExecuteTransaction(session => session.PublishAt(message, dueAt));

        Task PublishAtWithHeaders<T>(T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
            ExecuteTransaction(session => ((TransactionalSession)session).PublishAt(message, dueAt, headers));

        Task StoreScheduledEvent<T>(IPersistenceSession session, T message, DateTime dueUtc, string correlationId,
            IDictionary<string, object> headers = null) =>
            session.StoreTimeout(Prepare(message, eventsExchange, WireName(typeof(T)), correlationId, false, headers), dueUtc);

        Task StoreScheduled<T>(IPersistenceSession session, string destination, T message, DateTime dueUtc, string correlationId,
            IDictionary<string, object> headers = null) =>
            session.StoreTimeout(Prepare(message, "", RequireDestination(destination), correlationId, true, headers), dueUtc);

        public Task Send<T>(T message) => SendCore(message, null, null);
        public Task Send<T>(string destination, T message) => SendCore(message, null, RequireDestination(destination));
        static string RequireDestination(string destination)
        {
            if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentException("A destination queue is required.", nameof(destination));
            return destination;
        }
        public Task SendLocal<T>(T message) => SendCore(message, null, configuration.Name);
        public Task Publish<T>(T message) => PublishCore(message, null);
        internal Task SendCore<T>(T message, string correlationId, string destination, IDictionary<string, object> headers = null)
        {
            return Dispatch(message, "", destination ?? RouteFor<T>(), correlationId, true, headers);
        }
        string RouteFor<T>()
        {
            if (configuration.Routes.TryGetValue(typeof(T), out var destination)) return destination;
            throw new InvalidOperationException("No route for " + typeof(T).FullName);
        }

        string WireName(Type type) => wireNames.TryGetValue(type, out var name)
            ? name : EndpointConfiguration.MessageType(type);

        internal Task PublishCore<T>(T message, string correlationId, IDictionary<string, object> headers = null) =>
            Dispatch(message, eventsExchange, WireName(typeof(T)), correlationId, false, headers);
        OutgoingMessage Prepare<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory,
            IDictionary<string, object> headers = null)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            var copiedHeaders = headers == null ? new Dictionary<string, object>() : new Dictionary<string, object>(headers);
            foreach (var header in copiedHeaders.Keys)
                if (string.IsNullOrWhiteSpace(header)) throw new ArgumentException("Message header names cannot be empty.", nameof(headers));
            RecoveryWire.EncodeHeaders(copiedHeaders); // Validate the AMQP field types before storing or publishing.
            var id = Guid.NewGuid().ToString("N");
            return new OutgoingMessage { Id = id, CorrelationId = correlationId ?? id, ReplyTo = sendOnly ? null : configuration.Name,
                Type = WireName(typeof(T)), Exchange = exchange, RoutingKey = routingKey,
                Mandatory = mandatory, Headers = copiedHeaders, Body = MessagePackSerializer.Serialize(typeof(T), message, SerializerOptions) };
        }
        Task Dispatch<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory,
            IDictionary<string, object> headers = null)
        {
            PublishPrepared(Prepare(message, exchange, routingKey, correlationId, mandatory, headers));
            return Task.CompletedTask;
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
                    props.Headers = new Dictionary<string, object>(message.Headers);
                returned = false;
                publisherChannel.BasicPublish(message.Exchange, message.RoutingKey, message.Mandatory, props, message.Body);
                if (!publisherChannel.WaitForConfirms(publisherConfirmTimeout)) throw new InvalidOperationException("RabbitMQ did not confirm the publish.");
                if (returned) throw new InvalidOperationException("Message was unroutable: " + message.RoutingKey);
            }
        }
        async Task DispatchPending(bool recoveries, CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                try
                {
                    if (recoveries)
                    {
                        var store = (IRecoveryPersistence)configuration.Persistence;
                        var recovery = await store.NextRecovery(configuration.Name).ConfigureAwait(false);
                        if (recovery != null)
                        {
                            PublishRecovery(recovery);
                            await store.MarkRecoveryDispatched(configuration.Name, recovery.RecoveryId).ConfigureAwait(false);
                            continue;
                        }
                    }
                    else
                    {
                        var message = await configuration.Persistence.NextOutgoing(configuration.Name).ConfigureAwait(false);
                        if (message != null)
                        {
                            PublishPrepared(message);
                            await configuration.Persistence.MarkDispatched(configuration.Name, message.Id).ConfigureAwait(false);
                            continue;
                        }
                    }
                }
                catch (Exception ex) { if (!cancellation.IsCancellationRequested) System.Diagnostics.Trace.TraceError("{0} dispatch failed: {1}", recoveries ? "Recovery" : "Outbox", ex); }
                try { await Task.Delay(dispatchPollInterval, cancellation).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
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
                publisherChannel.BasicPublish("", record.IsError ? record.ErrorQueue : configuration.Name, true, props, envelope.Body);
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
            var unrecoverable = false;
            try
            {
                try { cycle = RecoveryWire.Cycle(props); }
                catch { unrecoverable = true; throw; }
                if (string.IsNullOrEmpty(currentId)) throw new InvalidOperationException("Message ID is required.");
                if (props.ContentType != ContentType || props.Type == null || !receivers.TryGetValue(props.Type, out var registration))
                { unrecoverable = true; throw new InvalidOperationException("Unknown message type or content type."); }
                object message;
                try { message = MessagePackSerializer.Deserialize(registration.Type, delivery.Body, SerializerOptions); }
                catch (Exception ex) { unrecoverable = true; throw new InvalidOperationException("Message deserialization failed.", ex); }
                for (var attempt = 0; ; attempt++)
                {
                    try
                    {
                        using (var session = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                        {
                            var recovery = session as IRecoverySession ?? throw new InvalidOperationException("Persistence session does not support recovery.");
                            if (!await recovery.HasRecovery(configuration.Name, RecoveryId(currentId, cycle)).ConfigureAwait(false) &&
                                await session.TryClaimMessage(configuration.Name, currentId).ConfigureAwait(false))
                            {
                                var context = new HandlerContext(this, props, session);
                                var previous = uniformScope.Value;
                                var scope = new UniformScope(context);
                                uniformScope.Value = scope;
                                try
                                {
                                    var incomingContext = new IncomingMessageContext(message, context);
                                    await incoming(incomingContext, () => registration.Invoke(message, context)).ConfigureAwait(false);
                                }
                                finally { Volatile.Write(ref scope.Active, 0); uniformScope.Value = previous; }
                            }
                            forceStop.Token.ThrowIfCancellationRequested();
                            session.Commit();
                        }
                        break;
                    }
                    catch (Exception ex) when (!forceStop.IsCancellationRequested && attempt < configuration.ImmediateRetries && !IsUnrecoverable(ex))
                    {
                        System.Diagnostics.Trace.TraceWarning("Immediate retry {0} for message {1}: {2}", attempt + 1, currentId, ex);
                        // Every attempt must deserialize afresh, since a failed handler may mutate the object.
                        message = MessagePackSerializer.Deserialize(registration.Type, delivery.Body, SerializerOptions);
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
                unrecoverable = unrecoverable || IsUnrecoverable(failure);
                var isError = unrecoverable || cycle >= options.DelayedRetries.Length;
                var envelope = RecoveryWire.Capture(props, delivery.Body);
                // A missing message ID cannot be deduplicated across redelivery, but the
                // failed message can still be persisted without losing its body.
                if (string.IsNullOrEmpty(currentId)) { currentId = Guid.NewGuid().ToString("N"); envelope.MessageId = currentId; isError = true; }
                RecoveryWire.SetFailure(envelope, failure, cycle, configuration.Name);
                if (!isError) RecoveryWire.SetCycle(envelope, checked(cycle + 1));
                var now = DateTime.UtcNow;
                var record = new RecoveryRecord { RecoveryId = RecoveryId(currentId, cycle), MessageId = currentId, Cycle = cycle,
                    IsError = isError, ErrorQueue = options.ErrorQueue ?? configuration.Name + ".error", ExceptionType = failure.GetType().FullName,
                    ExceptionMessage = failure.Message, ExceptionDetail = failure.ToString(), FailedAtUtc = now,
                    DueAtUtc = isError ? now : now.Add(options.DelayedRetries[cycle]), Payload = RecoveryWire.Encode(envelope) };
                using (var session = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                {
                    var recovery = session as IRecoverySession ?? throw new InvalidOperationException("Persistence session does not support recovery.");
                    await recovery.StoreRecovery(configuration.Name, record).ConfigureAwait(false);
                    session.Commit();
                }
                System.Diagnostics.Trace.TraceError("Message {0} failed; {1}: {2}", currentId, isError ? "error queue" : "delayed retry", failure);

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
        sealed class UniformSession : IUniformSession
        {
            readonly RabbitEndpoint endpoint;
            readonly UniformScope scope;
            readonly IDictionary<string, object> headers;
            public UniformSession(RabbitEndpoint endpoint, UniformScope scope) : this(endpoint, scope, null) { }
            UniformSession(RabbitEndpoint endpoint, UniformScope scope, IDictionary<string, object> headers)
            { this.endpoint = endpoint; this.scope = scope; this.headers = headers ?? new Dictionary<string, object>(); }
            public IUniformSession WithHeader(string name, object value) => new UniformSession(endpoint, scope, AddHeader(headers, name, value));
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
            public Task Send<T>(T message) { var context = Validate(); return context == null ? endpoint.SendCore(message, null, null, headers) : context.SendWithHeaders(message, null, headers); }
            public Task Send<T>(string destination, T message)
            {
                var context = Validate();
                return context == null ? endpoint.SendCore(message, null, RequireDestination(destination), headers) : context.SendWithHeaders(message, RequireDestination(destination), headers);
            }
            public Task SendLocal<T>(T message) { var context = Validate(); return context == null ? endpoint.SendCore(message, null, endpoint.configuration.Name, headers) : context.SendWithHeaders(message, endpoint.configuration.Name, headers); }
            public Task Publish<T>(T message) { var context = Validate(); return context == null ? endpoint.PublishCore(message, null, headers) : context.PublishWithHeaders(message, headers); }
            public Task SendAt<T>(T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.SendAtWithHeaders(endpoint.RouteFor<T>(), message, dueAt, headers) : context.SendAtWithHeaders(endpoint.RouteFor<T>(), message, dueAt, headers);
            }
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.SendAtWithHeaders(destination, message, dueAt, headers) : context.SendAtWithHeaders(destination, message, dueAt, headers);
            }
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.SendAtWithHeaders(endpoint.configuration.Name, message, dueAt, headers) : context.SendAtWithHeaders(endpoint.configuration.Name, message, dueAt, headers);
            }
            public Task PublishAt<T>(T message, DateTimeOffset dueAt)
            {
                var context = Validate();
                return context == null ? endpoint.PublishAtWithHeaders(message, dueAt, headers) : context.PublishAtWithHeaders(message, dueAt, headers);
            }
        }
        internal sealed class HandlerContext : IMessageHandlerContext
        {
            readonly RabbitEndpoint endpoint;
            readonly string replyTo;
            public HandlerContext(RabbitEndpoint endpoint, IBasicProperties properties, IPersistenceSession session)
            {
                this.endpoint = endpoint;
                PersistenceSession = session;
                replyTo = properties.ReplyTo;
                MessageId = properties.MessageId;
                CorrelationId = properties.CorrelationId ?? properties.MessageId;
                Headers = new Dictionary<string, object>(properties.Headers ?? new Dictionary<string, object>());
            }
            public IUniformSession UniformSession => endpoint.CreateUniformSession();
            public CancellationToken CancellationToken => endpoint.forceStop.Token;
            public IPersistenceSession PersistenceSession { get; }
            public string MessageId { get; }
            public string CorrelationId { get; }
            public IReadOnlyDictionary<string, object> Headers { get; }
            public Task Send<T>(T message) => SendWithHeaders(message, null, null);
            internal Task SendWithHeaders<T>(T message, string destination, IDictionary<string, object> headers)
            {
                if (destination == null && !endpoint.configuration.Routes.TryGetValue(typeof(T), out destination))
                    throw new InvalidOperationException("No route for " + typeof(T).FullName);
                RequireDestination(destination);
                return PersistenceSession.StoreOutgoing(endpoint.Prepare(message, "", destination, CorrelationId, true, headers));
            }
            public Task Send<T>(string destination, T message) => SendWithHeaders(message, RequireDestination(destination), null);
            public Task SendLocal<T>(T message) => SendWithHeaders(message, endpoint.configuration.Name, null);
            public Task Publish<T>(T message) => PublishWithHeaders(message, null);
            internal Task PublishWithHeaders<T>(T message, IDictionary<string, object> headers) =>
                PersistenceSession.StoreOutgoing(endpoint.Prepare(message, endpoint.eventsExchange,
                    endpoint.WireName(typeof(T)), CorrelationId, false, headers));
            internal Task SendAtWithHeaders<T>(string destination, T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
                endpoint.StoreScheduled(PersistenceSession, destination, message, dueAt.UtcDateTime, CorrelationId, headers);
            internal Task PublishAtWithHeaders<T>(T message, DateTimeOffset dueAt, IDictionary<string, object> headers) =>
                endpoint.StoreScheduledEvent(PersistenceSession, message, dueAt.UtcDateTime, CorrelationId, headers);
            public Task SendAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.RouteFor<T>(), message, dueAt);
            public Task SendLocalAt<T>(T message, DateTimeOffset dueAt) => SendAt(endpoint.configuration.Name, message, dueAt);
            public Task SendAt<T>(string destination, T message, DateTimeOffset dueAt) => SendAtWithHeaders(destination, message, dueAt, null);
            public Task PublishAt<T>(T message, DateTimeOffset dueAt) => PublishAtWithHeaders(message, dueAt, null);

            internal Task ScheduleSagaTimeout(SagaTimeoutMessage timeout, DateTime dueUtc)
            {
                if (PersistenceSession == null) throw new InvalidOperationException("Saga timeouts require persistence.");
                return endpoint.StoreScheduled(PersistenceSession, endpoint.configuration.Name, timeout, dueUtc, CorrelationId);
            }
            public Task Reply<T>(T message)
            {
                if (string.IsNullOrEmpty(replyTo)) throw new InvalidOperationException("Message has no reply address.");
                return PersistenceSession == null ? endpoint.SendCore(message, CorrelationId, replyTo) :
                    PersistenceSession.StoreOutgoing(endpoint.Prepare(message, "", replyTo, CorrelationId, true));
            }
        }
    }
}
