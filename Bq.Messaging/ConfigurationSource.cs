using System;
using System.Collections.Generic;

namespace Bq.Messaging.Config.ConfigurationSource
{
    public interface IConfigurationSource
    {
        T GetConfiguration<T>() where T : class, new();
    }

    public interface IProvideConfiguration<out T> where T : class, new()
    {
        T GetConfiguration();
    }
}

namespace Bq.Messaging.Transport
{
    /// <summary>A message ready for a transport, as in NServiceBus. Bq stores its own <see cref="Bq.Messaging.OutgoingMessage"/> records in persistence.</summary>
    public class OutgoingMessage
    {
        public OutgoingMessage(string messageId, Dictionary<string, string> headers, byte[] body)
        {
            MessageId = messageId ?? throw new ArgumentNullException(nameof(messageId));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
            Body = body ?? throw new ArgumentNullException(nameof(body));
        }

        public string MessageId { get; }
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }
    }
}

namespace Bq.Messaging
{
    public sealed partial class EndpointConfiguration
    {
        public void CustomConfigurationSource(Config.ConfigurationSource.IConfigurationSource configurationSource) =>
            throw new NotSupportedException("Bq reads no configuration sections such as UnicastBusConfig. Configure the endpoint in code.");
    }
}
