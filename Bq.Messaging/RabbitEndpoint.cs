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
        readonly object lifecycleLock = new object();
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
            var factory = new ConnectionFactory { Uri = configuration.RabbitMqUri, DispatchConsumersAsync = true, ConsumerDispatchConcurrency = configuration.MaxConcurrency };
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
                consumerChannel.BasicQos(0, configuration.MaxConcurrency, false);
                publisherChannel.ConfirmSelect();
                publisherChannel.BasicReturn += (sender, args) => { returned = true; };
                var consumer = new AsyncEventingBasicConsumer(consumerChannel);
                consumer.Received += OnReceived;
                consumerTag = consumerChannel.BasicConsume(configuration.Name, false, consumer);
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
                if (disposed || (shutdownRequested && scope == null)) throw new InvalidOperationException("Endpoint is stopping or stopped.");
                if (scope != null && Volatile.Read(ref scope.Active) == 0) throw new InvalidOperationException("Message handling scope has ended.");
                return new UniformSession(this, scope);
            }
        }
        public Task Send<T>(T message) => SendCore(message, null, null);
        public Task SendLocal<T>(T message) => SendCore(message, null, configuration.Name);
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
            lock (lifecycleLock)
            {
                // The broker will requeue a delivery left unacknowledged when the channel closes.
                if (shutdownRequested) return;
                activeDeliveries++;
            }
            try { await ProcessDelivery(delivery).ConfigureAwait(false); }
            finally
            {
                lock (lifecycleLock)
                {
                    activeDeliveries--;
                    if (shutdownRequested && activeDeliveries == 0) drained.TrySetResult(true);
                }
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
                            {
                                var context = new HandlerContext(this, props, session);
                                var previous = uniformScope.Value;
                                var scope = new UniformScope(context);
                                uniformScope.Value = scope;
                                try { await registration.Invoke(message, context).ConfigureAwait(false); }
                                finally { Volatile.Write(ref scope.Active, 0); uniformScope.Value = previous; }
                            }
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
            if (forceStop.IsCancellationRequested) return;
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
                var cancel = Task.Run(() => { lock (consumerLock) { if (consumerChannel.IsOpen) consumerChannel.BasicCancel(consumerTag); } });
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
            public UniformSession(RabbitEndpoint endpoint, UniformScope scope) { this.endpoint = endpoint; this.scope = scope; }
            HandlerContext Validate()
            {
                lock (endpoint.lifecycleLock)
                {
                    if (endpoint.disposed) throw new InvalidOperationException("Endpoint has stopped.");
                    if (scope == null)
                    {
                        if (endpoint.shutdownRequested || endpoint.uniformScope.Value != null)
                            throw new InvalidOperationException("An outside uniform session cannot be used in a handler or after shutdown starts.");
                        return null;
                    }
                    if (Volatile.Read(ref scope.Active) == 0 || !ReferenceEquals(endpoint.uniformScope.Value, scope))
                        throw new InvalidOperationException("Uniform session is outside its message handling scope.");
                    return scope.Context;
                }
            }
            public Task Send<T>(T message) { var context = Validate(); return context == null ? endpoint.Send(message) : context.Send(message); }
            public Task SendLocal<T>(T message) { var context = Validate(); return context == null ? endpoint.SendLocal(message) : context.SendLocal(message); }
            public Task Publish<T>(T message) { var context = Validate(); return context == null ? endpoint.Publish(message) : context.Publish(message); }
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
            public Task Send<T>(T message)
            {
                if (PersistenceSession == null) return endpoint.SendCore(message, CorrelationId, null);
                if (!endpoint.configuration.Routes.TryGetValue(typeof(T), out var destination)) throw new InvalidOperationException("No route for " + typeof(T).FullName);
                return PersistenceSession.StoreOutgoing(endpoint.Prepare(message, "", destination, CorrelationId, true));
            }
            public Task SendLocal<T>(T message) => PersistenceSession == null ? endpoint.SendCore(message, CorrelationId, endpoint.configuration.Name) :
                PersistenceSession.StoreOutgoing(endpoint.Prepare(message, "", endpoint.configuration.Name, CorrelationId, true));
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
