using System;
using System.Collections.Generic;
using System.Globalization;
using Bq.Messaging.Configuration.AdvancedExtensibility;
using Bq.Messaging.Settings;

namespace Bq.Messaging
{
    public enum TransportTransactionMode { None, ReceiveOnly, SendsAtomicWithReceive, TransactionScope }

    public abstract class TransportDefinition { }

    /// <summary>The RabbitMQ transport. Bq always uses RabbitMQ, so this marks the transport for <c>UseTransport</c>.</summary>
    public sealed class RabbitMQTransport : TransportDefinition { }

    public class TransportExtensions : ExposeSettings
    {
        public TransportExtensions(SettingsHolder settings) : base(settings) { }

        public TransportExtensions ConnectionString(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("A connection string is required.", nameof(connectionString));
            return ConnectionString(() => connectionString);
        }

        public TransportExtensions ConnectionString(Func<string> connectionString)
        {
            this.GetSettings().Set(TransportKeys.ConnectionString, connectionString ?? throw new ArgumentNullException(nameof(connectionString)));
            return this;
        }

        public TransportExtensions ConnectionStringName(string name) =>
            throw new NotSupportedException("Bq has no configuration-file connection string lookup. Pass the value to ConnectionString.");

        /// <summary>Bq processes each message in a database transaction, which matches ReceiveOnly. Other modes are rejected.</summary>
        public TransportExtensions Transactions(TransportTransactionMode transportTransactionMode)
        {
            if (transportTransactionMode != TransportTransactionMode.ReceiveOnly)
                throw new NotSupportedException("Bq supports only " + nameof(TransportTransactionMode.ReceiveOnly) +
                    ". Atomic sends come from the transactional outbox, which is always on.");
            return this;
        }
    }

    public class TransportExtensions<T> : TransportExtensions where T : TransportDefinition
    {
        public TransportExtensions(SettingsHolder settings) : base(settings) { }

        public new TransportExtensions<T> ConnectionString(string connectionString) { base.ConnectionString(connectionString); return this; }
        public new TransportExtensions<T> ConnectionString(Func<string> connectionString) { base.ConnectionString(connectionString); return this; }
        public new TransportExtensions<T> ConnectionStringName(string name) { base.ConnectionStringName(name); return this; }
        public new TransportExtensions<T> Transactions(TransportTransactionMode mode) { base.Transactions(mode); return this; }

        /// <summary>Mirrors the RabbitMQ transport option, mapped to <see cref="EndpointConfiguration.CriticalErrorThreshold"/>.</summary>
        public TransportExtensions<T> TimeToWaitBeforeTriggeringCircuitBreaker(TimeSpan waitTime)
        {
            if (waitTime <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(waitTime));
            this.GetSettings().Set(TransportKeys.CircuitBreaker, waitTime);
            return this;
        }
    }

    internal static class TransportKeys
    {
        public const string ConnectionString = "Bq.Transport.ConnectionString";
        public const string CircuitBreaker = "Bq.Transport.CircuitBreaker";
    }

    public static class UseTransportExtensions
    {
        public static TransportExtensions<T> UseTransport<T>(this EndpointConfiguration configuration) where T : TransportDefinition, new()
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (typeof(T) != typeof(RabbitMQTransport))
                throw new NotSupportedException("Bq supports only the RabbitMQ transport.");
            return new TransportExtensions<T>(configuration.GetSettings());
        }

        public static TransportExtensions UseTransport(this EndpointConfiguration configuration, Type transportType)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (transportType != typeof(RabbitMQTransport)) throw new NotSupportedException("Bq supports only the RabbitMQ transport.");
            return new TransportExtensions(configuration.GetSettings());
        }
    }

    public sealed partial class EndpointConfiguration
    {
        /// <summary>Applies <c>UseTransport</c> settings. Called when the endpoint starts, so connection string callbacks run late.</summary>
        internal void ApplyTransportSettings()
        {
            if (Settings.TryGet<Func<string>>(TransportKeys.ConnectionString, out var connectionString))
                RabbitMqUri = TransportConnectionString.ToUri(connectionString());
            if (Settings.TryGet<TimeSpan>(TransportKeys.CircuitBreaker, out var wait)) CriticalErrorThreshold = wait;
        }
    }

    static class TransportConnectionString
    {
        /// <summary>Accepts an amqp(s) URI or the NServiceBus RabbitMQ <c>key=value;key=value</c> form.</summary>
        public static Uri ToUri(string connectionString)
        {
            if (connectionString == null) throw new InvalidOperationException("The transport connection string callback returned null.");
            if (connectionString.StartsWith("amqp://", StringComparison.OrdinalIgnoreCase) ||
                connectionString.StartsWith("amqps://", StringComparison.OrdinalIgnoreCase))
                return new Uri(connectionString);
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var index = part.IndexOf('=');
                if (index <= 0) throw new FormatException("Invalid connection string segment: " + part);
                values[part.Substring(0, index).Trim()] = part.Substring(index + 1).Trim();
            }
            string Get(string key, string fallback) => values.TryGetValue(key, out var value) && value.Length > 0 ? value : fallback;
            var tls = Get("useTls", "false").Equals("true", StringComparison.OrdinalIgnoreCase);
            var host = Get("host", "localhost");
            var port = Get("port", tls ? "5671" : "5672");
            var user = Uri.EscapeDataString(Get("username", "guest"));
            var password = Uri.EscapeDataString(Get("password", "guest"));
            var vhost = Uri.EscapeDataString(Get("virtualhost", "/"));
            return new Uri(string.Format(CultureInfo.InvariantCulture, "{0}://{1}:{2}@{3}:{4}/{5}",
                tls ? "amqps" : "amqp", user, password, host, port, vhost));
        }
    }
}
