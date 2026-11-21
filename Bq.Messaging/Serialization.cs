using System;
using System.Collections.Generic;
using System.IO;
using Bq.Messaging.Configuration.AdvancedExtensibility;
using Bq.Messaging.Serialization;
using Bq.Messaging.Settings;
using MessagePack;
using MessagePack.Resolvers;

namespace Bq.Messaging.Serialization
{
    public interface IMessageSerializer
    {
        void Serialize(object message, Stream stream);
        object[] Deserialize(Stream stream, IList<Type> messageTypes = null);
        string ContentType { get; }
    }

    public abstract class SerializationDefinition
    {
        public abstract Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings);
    }

    public class SerializationExtensions<T> : ExposeSettings where T : SerializationDefinition
    {
        public SerializationExtensions(SettingsHolder settings, SettingsHolder serializerSettings) : base(settings) { }
    }

    /// <summary>The only wire format Bq reads and writes. Selecting it explicitly is accepted and changes nothing.</summary>
    public sealed class MessagePackSerialization : SerializationDefinition
    {
        static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);

        public override Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings) => mapper => new Serializer();

        sealed class Serializer : IMessageSerializer
        {
            public string ContentType => "application/x-msgpack";

            public void Serialize(object message, Stream stream) =>
                MessagePackSerializer.Serialize(message.GetType(), stream, message, Options);

            public object[] Deserialize(Stream stream, IList<Type> messageTypes = null)
            {
                var type = messageTypes != null && messageTypes.Count > 0 ? messageTypes[0] : typeof(object);
                return new[] { MessagePackSerializer.Deserialize(type, stream, Options) };
            }
        }
    }

    /// <summary>Present so existing configuration compiles. Bq's wire format is MessagePack, so selecting it fails at configuration time.</summary>
    public sealed class XmlSerializer : SerializationDefinition
    {
        public override Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings) =>
            throw new NotSupportedException("Bq messages use MessagePack on the wire. Remove UseSerialization<XmlSerializer>().");
    }

    /// <summary>Present so existing configuration compiles. Bq's wire format is MessagePack, so selecting it fails at configuration time.</summary>
    public sealed class JsonSerializer : SerializationDefinition
    {
        public override Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings) =>
            throw new NotSupportedException("Bq messages use MessagePack on the wire. Remove UseSerialization<JsonSerializer>().");
    }
}

namespace Bq.Messaging
{
    public static class SerializationConfigExtensions
    {
        public static SerializationExtensions<T> UseSerialization<T>(this EndpointConfiguration configuration) where T : SerializationDefinition, new() =>
            configuration.UseSerialization(new T());

        public static SerializationExtensions<T> UseSerialization<T>(this EndpointConfiguration configuration, T serializationDefinition)
            where T : SerializationDefinition
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (serializationDefinition == null) throw new ArgumentNullException(nameof(serializationDefinition));
            serializationDefinition.Configure(configuration.GetSettings()); // Unsupported formats fail here, not at the first message.
            return new SerializationExtensions<T>(configuration.GetSettings(), new SettingsHolder());
        }

        public static SerializationExtensions<T> AddDeserializer<T>(this EndpointConfiguration configuration) where T : SerializationDefinition, new() =>
            throw new NotSupportedException("Bq reads only MessagePack messages.");

        public static SerializationExtensions<T> AddDeserializer<T>(this EndpointConfiguration configuration, T serializationDefinition)
            where T : SerializationDefinition =>
            throw new NotSupportedException("Bq reads only MessagePack messages.");
    }
}
