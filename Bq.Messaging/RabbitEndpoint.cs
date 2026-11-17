using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
                consumerChannel.QueueDeclare(configuration.Name + ".error", true, false, false);
                consumerChannel.QueueBind(configuration.Name + ".error", ErrorExchange, configuration.Name);
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
            if (configuration.HasSagas && configuration.Persistence == null) throw new InvalidOperationException("Saga handling requires persistence.");
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
                publisherChannel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
                if (returned) throw new InvalidOperationException("Message was unroutable: " + message.RoutingKey);
            }
        }
        async Task DispatchOutbox(CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                try
                {
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
        async Task OnReceived(object sender, BasicDeliverEventArgs delivery)
        {
            try
            {
                var props = delivery.BasicProperties;
                if (props.ContentType != ContentType || props.Type == null || !configuration.Handlers.TryGetValue(props.Type, out var registration))
                    throw new InvalidOperationException("Unknown message type or content type.");
                for (var attempt = 0; ; attempt++)
                {
                    try
                    {
                        var message = MessagePackSerializer.Deserialize(registration.Type, delivery.Body, SerializerOptions);
                        if (configuration.Persistence == null)
                            await registration.Invoke(message, new HandlerContext(this, props, null)).ConfigureAwait(false);
                        else
                        {
                            using (var session = await configuration.Persistence.OpenSession().ConfigureAwait(false))
                            {
                                if (await session.TryClaimMessage(configuration.Name, props.MessageId).ConfigureAwait(false))
                                    await registration.Invoke(message, new HandlerContext(this, props, session)).ConfigureAwait(false);
                                session.Commit();
                            }
                        }
                        break;
                    }
                    catch when (attempt < configuration.ImmediateRetries) { }
                }
                lock (consumerLock) { if (!disposed) consumerChannel.BasicAck(delivery.DeliveryTag, false); }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Message processing failed: {0}", ex);
                lock (consumerLock) { if (!disposed && consumerChannel.IsOpen) consumerChannel.BasicNack(delivery.DeliveryTag, false, false); }
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
