using System;
using System.Collections.Generic;
using System.Linq;
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
            }
            catch { connection.Dispose(); throw; }
        }

        public static RabbitEndpoint Start(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
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
        Task Dispatch<T>(T message, string exchange, string routingKey, string correlationId, bool mandatory)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            var body = MessagePackSerializer.Serialize(typeof(T), message, SerializerOptions);
            lock (publishLock)
            {
                if (disposed) throw new ObjectDisposedException(nameof(RabbitEndpoint));
                var props = publisherChannel.CreateBasicProperties();
                props.Persistent = true;
                props.MessageId = Guid.NewGuid().ToString("N");
                props.CorrelationId = correlationId ?? props.MessageId;
                props.ReplyTo = configuration.Name;
                props.Type = EndpointConfiguration.MessageType(typeof(T));
                props.ContentType = ContentType;
                returned = false;
                publisherChannel.BasicPublish(exchange, routingKey, mandatory, props, body);
                publisherChannel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
                if (returned) throw new InvalidOperationException("Message was unroutable: " + routingKey);
            }
            return Task.CompletedTask;
        }
        async Task OnReceived(object sender, BasicDeliverEventArgs delivery)
        {
            try
            {
                var props = delivery.BasicProperties;
                if (props.ContentType != ContentType || props.Type == null || !configuration.Handlers.TryGetValue(props.Type, out var registration))
                    throw new InvalidOperationException("Unknown message type or content type.");
                var message = MessagePackSerializer.Deserialize(registration.Type, delivery.Body, SerializerOptions);
                var context = new HandlerContext(this, props);
                for (var attempt = 0; ; attempt++)
                {
                    try { await registration.Invoke(message, context).ConfigureAwait(false); break; }
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
                connection.Dispose();
            }
        }
        sealed class HandlerContext : IMessageHandlerContext
        {
            readonly RabbitEndpoint endpoint;
            readonly string replyTo;
            public HandlerContext(RabbitEndpoint endpoint, IBasicProperties properties)
            {
                this.endpoint = endpoint;
                replyTo = properties.ReplyTo;
                MessageId = properties.MessageId;
                CorrelationId = properties.CorrelationId ?? properties.MessageId;
                Headers = new Dictionary<string, object>(properties.Headers ?? new Dictionary<string, object>());
            }
            public string MessageId { get; }
            public string CorrelationId { get; }
            public IReadOnlyDictionary<string, object> Headers { get; }
            public Task Send<T>(T message) => endpoint.SendCore(message, CorrelationId, null);
            public Task Publish<T>(T message) => endpoint.PublishCore(message, CorrelationId);
            public Task Reply<T>(T message)
            {
                if (string.IsNullOrEmpty(replyTo)) throw new InvalidOperationException("Message has no reply address.");
                return endpoint.SendCore(message, CorrelationId, replyTo);
            }
        }
    }
}
