using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;
using Bq.Messaging.Transport;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Bq.Messaging.Transport
{
    public class PushRuntimeSettings
    {
        public static PushRuntimeSettings Default { get; } = new PushRuntimeSettings();

        public PushRuntimeSettings() : this(1) { }

        public PushRuntimeSettings(int maxConcurrency)
        {
            if (maxConcurrency < 1) throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
            MaxConcurrency = maxConcurrency;
        }

        public int MaxConcurrency { get; }
    }

    public class MessageContext
    {
        public MessageContext(string messageId, Dictionary<string, string> headers, byte[] body, TransportTransaction transportTransaction,
            CancellationTokenSource receiveCancellationTokenSource, ContextBag context)
        {
            MessageId = messageId;
            Headers = headers;
            Body = body;
            TransportTransaction = transportTransaction;
            ReceiveCancellationTokenSource = receiveCancellationTokenSource;
            Extensions = context;
        }

        public string MessageId { get; }
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }
        public TransportTransaction TransportTransaction { get; }
        public CancellationTokenSource ReceiveCancellationTokenSource { get; }
        public ContextBag Extensions { get; }
    }
}

namespace Bq.Messaging
{
    internal sealed class SatelliteReceiver
    {
        public SatelliteReceiver(string name, string address, PushRuntimeSettings settings,
            Func<RecoverabilityConfig, ErrorContext, RecoverabilityAction> policy, Func<IBuilder, MessageContext, Task> onMessage)
        {
            Name = name;
            Address = address;
            Settings = settings;
            Policy = policy;
            OnMessage = onMessage;
        }

        public string Name { get; }
        public string Address { get; }
        public PushRuntimeSettings Settings { get; }
        public Func<RecoverabilityConfig, ErrorContext, RecoverabilityAction> Policy { get; }
        public Func<IBuilder, MessageContext, Task> OnMessage { get; }
        public IModel Channel { get; set; }
        public string ConsumerTag { get; set; }
        public readonly object Gate = new object();
    }

    public sealed partial class EndpointConfiguration
    {
        internal readonly List<SatelliteReceiver> Satellites = new List<SatelliteReceiver>();
    }

    public sealed partial class RabbitEndpoint
    {
        void StartSatellites()
        {
            foreach (var satellite in configuration.Satellites)
            {
                var channel = connection.CreateModel();
                satellite.Channel = channel;
                if (configuration.UsePreProvisionedTopology) channel.QueueDeclarePassive(satellite.Address);
                else channel.QueueDeclare(satellite.Address, true, false, false);
                channel.BasicQos(0, (ushort)Math.Min(ushort.MaxValue, satellite.Settings.MaxConcurrency), false);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.Received += (sender, delivery) => OnSatelliteReceived(satellite, delivery);
                satellite.ConsumerTag = channel.BasicConsume(satellite.Address, false, consumer);
            }
        }

        void CancelSatellites()
        {
            foreach (var satellite in configuration.Satellites)
                lock (satellite.Gate)
                    if (satellite.Channel?.IsOpen == true && satellite.ConsumerTag != null) satellite.Channel.BasicCancel(satellite.ConsumerTag);
        }

        async Task OnSatelliteReceived(SatelliteReceiver satellite, BasicDeliverEventArgs delivery)
        {
            lock (lifecycleLock)
            {
                if (shutdownRequested) return;
                activeDeliveries++;
            }
            try { await ProcessSatelliteDelivery(satellite, delivery).ConfigureAwait(false); }
            finally { CompleteActiveWork(); }
        }

        void SatelliteAck(SatelliteReceiver satellite, ulong tag, bool requeue, bool acknowledge)
        {
            try
            {
                lock (satellite.Gate)
                {
                    if (disposed) return;
                    if (acknowledge) satellite.Channel.BasicAck(tag, false);
                    else satellite.Channel.BasicNack(tag, false, requeue);
                }
            }
            catch (Exception ex) { System.Diagnostics.Trace.TraceError("Satellite {0} could not settle delivery {1}: {2}", satellite.Name, tag, ex); }
        }

        async Task ProcessSatelliteDelivery(SatelliteReceiver satellite, BasicDeliverEventArgs delivery)
        {
            var props = delivery.BasicProperties;
            var headers = MessageHeaders.ToTextDictionary(props.Headers ?? new Dictionary<string, object>());
            var body = delivery.Body.ToArray();
            var id = props.MessageId ?? delivery.DeliveryTag.ToString();
            var options = configuration.Recoverability;
            var errorQueue = options.ErrorQueue ?? configuration.Name + ".error";
            for (var attempt = 0; ; attempt++)
            {
                Exception failure;
                try
                {
                    using (var builder = rootBuilder.CreateChildBuilder())
                        await satellite.OnMessage(builder, new MessageContext(id, new Dictionary<string, string>(headers), body,
                            new TransportTransaction(), new CancellationTokenSource(), new ContextBag())).ConfigureAwait(false);
                    SatelliteAck(satellite, delivery.DeliveryTag, false, true);
                    return;
                }
                catch (Exception ex) when (!forceStop.IsCancellationRequested) { failure = ex; }
                catch (Exception) { return; }

                RecoveryAction action;
                try
                {
                    var config = new RecoverabilityConfig(new ImmediateConfig(options.ImmediateRetries), new DelayedConfig(options.DelayedRetries),
                        new FailedConfig(errorQueue, options.UnrecoverableExceptionTypes));
                    var context = new ErrorContext(failure, attempt, 0, new IncomingMessage(id, new Dictionary<string, string>(headers), body), new ContextBag());
                    action = satellite.Policy(config, context)?.ToBqAction() ?? RecoveryAction.MoveToError();
                }
                catch (Exception policyFailure)
                {
                    System.Diagnostics.Trace.TraceError("Satellite {0} recoverability policy failed: {1}", satellite.Name, policyFailure);
                    action = RecoveryAction.MoveToError();
                }

                if (action.IsImmediate) continue;
                if (action.DiscardReason != null)
                {
                    SatelliteAck(satellite, delivery.DeliveryTag, false, true);
                    return;
                }
                if (action.Delay.HasValue)
                {
                    // Hold the unacknowledged delivery for the delay, then hand it back to the queue.
                    try { await Task.Delay(action.Delay.Value, forceStop.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }
                    SatelliteAck(satellite, delivery.DeliveryTag, true, false);
                    return;
                }
                var target = action.ErrorQueue ?? errorQueue;
                lock (satellite.Gate)
                {
                    satellite.Channel.QueueDeclare(target, true, false, false);
                    var failed = satellite.Channel.CreateBasicProperties();
                    failed.Persistent = true;
                    failed.MessageId = props.MessageId;
                    failed.Type = props.Type;
                    failed.ContentType = props.ContentType;
                    failed.Headers = new Dictionary<string, object>(props.Headers ?? new Dictionary<string, object>())
                    {
                        ["x-bq-exception-type"] = failure.GetType().FullName,
                        ["x-bq-exception-message"] = failure.Message,
                        ["x-bq-failed-queue"] = satellite.Address
                    };
                    satellite.Channel.BasicPublish("", target, failed, body);
                }
                SatelliteAck(satellite, delivery.DeliveryTag, false, true);
                return;
            }
        }
    }
}
