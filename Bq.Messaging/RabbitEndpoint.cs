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
        const string EventsExchange = "bq.events";
        const string ErrorExchange = "bq.errors";
        const string ContentType = "application/x-msgpack";
        static readonly MessagePackSerializerOptions SerializerOptions = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
        readonly EndpointConfiguration configuration;
        readonly IConnection connection;
        readonly IModel consumerChannel;
        readonly IModel publisherChannel;
        readonly object publishLock = new object();
        readonly object consumerLock = new object();
        readonly CancellationTokenSource stopping = new CancellationTokenSource();
        Task dispatcher;
        bool returned;
        bool disposed;

        RabbitEndpoint(EndpointConfiguration configuration)
        {
            this.configuration = configuration;
            var factory = new ConnectionFactory { Uri = configuration.RabbitMqUri, DispatchConsumersAsync = true };
            connection = factory.CreateConnection();
            try
            {
                consumerChannel = connection.CreateModel();
                publisherChannel = connection.CreateModel();
                consumerChannel.ExchangeDeclare(EventsExchange, ExchangeType.Topic, true);
                consumerChannel.ExchangeDeclare(ErrorExchange, ExchangeType.Direct, true);
                var errorQueue = configuration.Recoverability.ErrorQueue ?? configuration.Name + ".error";
                consumerChannel.QueueDeclare(errorQueue, true, false, false);
                consumerChannel.QueueBind(errorQueue, ErrorExchange, configuration.Name);
                consumerChannel.QueueDeclare(configuration.Name, true, false, false, new Dictionary<string, object>
                {
                    ["x-dead-letter-exchange"] = ErrorExchange,
                    ["x-dead-letter-routing-key"] = configuration.Name
                });
                foreach (var entry in configuration.Handlers.Where(x => x.Value.Subscribe))
                    consumerChannel.QueueBind(configuration.Name, EventsExchange, entry.Key);
                consumerChannel.BasicQos(0, 1, false);
                publisherChannel.ConfirmSelect();
                publisherChannel.BasicReturn += (sender, args) => { returned = true; };
                var consumer = new AsyncEventingBasicConsumer(consumerChannel);
                consumer.Received += OnReceived;
                consumerChannel.BasicConsume(configuration.Name, false, consumer);
                if (configuration.Persistence != null) dispatcher = Task.Run(() => DispatchOutbox(stopping.Token));
            }
            catch { connection.Dispose(); throw; }
        }

        public static RabbitEndpoint Start(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (configuration.Persistence == null || !(configuration.Persistence is IRecoveryPersistence)) throw new InvalidOperationException("Transactional persistence with recovery support is required.");
            if (configuration.Recoverability.DelayedRetries == null || configuration.Recoverability.DelayedRetries.Any(delay => delay < TimeSpan.Zero || delay > TimeSpan.FromDays(24))) throw new ArgumentException("Delayed retry intervals must be between zero and 24 days.");
            if (configuration.Recoverability.ErrorQueue != null && string.IsNullOrWhiteSpace(configuration.Recoverability.ErrorQueue)) throw new ArgumentException("Error queue cannot be empty.");
            if (configuration.Recoverability.ErrorQueue == configuration.Name) throw new ArgumentException("Error queue must differ from the endpoint queue.");
            if (configuration.ImmediateRetries < 0) throw new ArgumentOutOfRangeException(nameof(configuration.ImmediateRetries));
            return new RabbitEndpoint(configuration);
        }
        public Task Send<T>(T message) => SendCore(message, null, null);
        public Task Publish<T>(T message) => PublishCore(message, null);
        internal Task SendCore<T>(T message, string correlationId, string destination)
        {
            if (destination == null && !configuration.Routes.TryGetValue(typeof(T), out destination))
                throw new InvalidOperationException("No route for " + typeof(T).FullName);
            return Dispatch(message, "", destination, correlationId, true);
        }
        internal Task PublishCore<T>(T message, string correlationId) =>
            Dispatch(message, EventsExchange, EndpointConfiguration.MessageType(typeof(T)), correlationId, false);
        OutgoingMessage Prepare<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            var id = Guid.NewGuid().ToString("N");
            return new OutgoingMessage { Id = id, CorrelationId = correlationId ?? id, ReplyTo = configuration.Name,
                Type = EndpointConfiguration.MessageType(typeof(T)), Exchange = exchange, RoutingKey = routingKey,
                Mandatory = mandatory, Body = MessagePackSerializer.Serialize(typeof(T), message, SerializerOptions) };
        }
        Task Dispatch<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory)
        {
            PublishPrepared(Prepare(message, exchange, routingKey, correlationId, mandatory));
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
                returned = false;
                publisherChannel.BasicPublish(message.Exchange, message.RoutingKey, message.Mandatory, props, message.Body);
                if (!publisherChannel.WaitForConfirms(TimeSpan.FromSeconds(10))) throw new InvalidOperationException("RabbitMQ did not confirm the publish.");
                if (returned) throw new InvalidOperationException("Message was unroutable: " + message.RoutingKey);
            }
        }
        async Task DispatchOutbox(CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                try
                {
                    var recovery = await ((IRecoveryPersistence)configuration.Persistence).NextRecovery(configuration.Name).ConfigureAwait(false);
                    if (recovery != null)
                    {
                        PublishRecovery(recovery);
                        await ((IRecoveryPersistence)configuration.Persistence).MarkRecoveryDispatched(configuration.Name, recovery.RecoveryId).ConfigureAwait(false);
                        continue;
                    }
                    var message = await configuration.Persistence.NextOutgoing(configuration.Name).ConfigureAwait(false);
                    if (message != null)
                    {
                        PublishPrepared(message);
                        await configuration.Persistence.MarkDispatched(configuration.Name, message.Id).ConfigureAwait(false);
                        continue;
                    }
                }
                catch (Exception ex) { if (!cancellation.IsCancellationRequested) System.Diagnostics.Trace.TraceError("Outbox dispatch failed: {0}", ex); }
                try { await Task.Delay(1000, cancellation).ConfigureAwait(false); }
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
                if (!publisherChannel.WaitForConfirms(TimeSpan.FromSeconds(10))) throw new InvalidOperationException("RabbitMQ did not confirm the publish.");
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
                if (props.ContentType != ContentType || props.Type == null || !configuration.Handlers.TryGetValue(props.Type, out var registration))
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
                                await registration.Invoke(message, new HandlerContext(this, props, session)).ConfigureAwait(false);
                            session.Commit();
                        }
                        break;
                    }
                    catch (Exception ex) when (attempt < configuration.ImmediateRetries && !(configuration.Recoverability.IsUnrecoverable?.Invoke(ex) ?? false))
                    {
                        System.Diagnostics.Trace.TraceWarning("Immediate retry {0} for message {1}: {2}", attempt + 1, currentId, ex);
                        // Every attempt must deserialize afresh, since a failed handler may mutate the object.
                        message = MessagePackSerializer.Deserialize(registration.Type, delivery.Body, SerializerOptions);
                    }
                }
                lock (consumerLock) { if (!disposed) consumerChannel.BasicAck(delivery.DeliveryTag, false); }
                return;
            }
            catch (Exception ex) { failure = ex; }
            try
            {
                var options = configuration.Recoverability;
                unrecoverable |= options.IsUnrecoverable?.Invoke(failure) ?? false;
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
                lock (consumerLock) { if (!disposed) consumerChannel.BasicAck(delivery.DeliveryTag, false); }
            }
            catch (Exception handoffError)
            {
                System.Diagnostics.Trace.TraceError("Failed to persist recovery for message {0}; requeuing: {1}", currentId, handoffError);
                await Task.Delay(1000).ConfigureAwait(false);
                lock (consumerLock) { if (!disposed && consumerChannel.IsOpen) consumerChannel.BasicNack(delivery.DeliveryTag, false, true); }
            }
        }
        public void Dispose()
        {
            lock (consumerLock) lock (publishLock)
            {
                if (disposed) return;
                disposed = true;
                stopping.Cancel();
            }
            connection.Dispose();
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
            public IPersistenceSession PersistenceSession { get; }
            public string MessageId { get; }
            public string CorrelationId { get; }
            public IReadOnlyDictionary<string, object> Headers { get; }
            public Task Send<T>(T message)
            {
                if (PersistenceSession == null) return endpoint.SendCore(message, CorrelationId, null);
                if (!endpoint.configuration.Routes.TryGetValue(typeof(T), out var destination)) throw new InvalidOperationException("No route for " + typeof(T).FullName);
                return PersistenceSession.StoreOutgoing(endpoint.Prepare(message, "", destination, CorrelationId, true));
            }
            public Task Publish<T>(T message) => PersistenceSession == null ? endpoint.PublishCore(message, CorrelationId) :
                PersistenceSession.StoreOutgoing(endpoint.Prepare(message, EventsExchange, EndpointConfiguration.MessageType(typeof(T)), CorrelationId, false));
            internal Task ScheduleSagaTimeout(SagaTimeoutMessage timeout, DateTime dueUtc)
            {
                if (PersistenceSession == null) throw new InvalidOperationException("Saga timeouts require persistence.");
                return PersistenceSession.StoreTimeout(endpoint.Prepare(timeout, "", endpoint.configuration.Name, CorrelationId, true), dueUtc);
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
