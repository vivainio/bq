using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        /// <summary>The format Bq uses on the wire for this definition.</summary>
        internal virtual IWireFormat CreateWireFormat() =>
            throw new NotSupportedException(GetType().Name + " cannot be used on the Bq wire. Use MessagePackSerialization or JsonSerializer.");
    }

    /// <summary>Turns a message into wire bytes and back, for one content type.</summary>
    internal interface IWireFormat
    {
        string ContentType { get; }
        byte[] Serialize(Type type, object value);
        object Deserialize(Type type, ReadOnlyMemory<byte> body);
    }

    public class SerializationExtensions<T> : ExposeSettings where T : SerializationDefinition
    {
        public SerializationExtensions(SettingsHolder settings, SettingsHolder serializerSettings) : base(settings) { }
    }

    /// <summary>The default wire format: compact MessagePack. Selecting it explicitly is accepted and changes nothing.</summary>
    public sealed class MessagePackSerialization : SerializationDefinition
    {
        static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);

        internal static readonly IWireFormat Format = new MessagePackFormat();

        public override Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings) => mapper => new Serializer();

        internal override IWireFormat CreateWireFormat() => Format;

        sealed class MessagePackFormat : IWireFormat
        {
            public string ContentType => "application/x-msgpack";
            public byte[] Serialize(Type type, object value) => MessagePackSerializer.Serialize(type, value, Options);
            public object Deserialize(Type type, ReadOnlyMemory<byte> body) => MessagePackSerializer.Deserialize(type, body, Options);
        }

        sealed class Serializer : IMessageSerializer
        {
            public string ContentType => Format.ContentType;

            public void Serialize(object message, Stream stream) =>
                MessagePackSerializer.Serialize(message.GetType(), stream, message, Options);

            public object[] Deserialize(Stream stream, IList<Type> messageTypes = null)
            {
                var type = messageTypes != null && messageTypes.Count > 0 ? messageTypes[0] : typeof(object);
                return new[] { MessagePackSerializer.Deserialize(type, stream, Options) };
            }
        }
    }

    /// <summary>
    /// JSON on the wire, as <c>application/json</c>. Messages need public properties with setters, and polymorphic or
    /// interface-typed properties are not supported. Property names match case-insensitively when reading.
    /// </summary>
    public sealed class JsonSerializer : SerializationDefinition
    {
        static readonly System.Text.Json.JsonSerializerOptions Options = new System.Text.Json.JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        internal static readonly IWireFormat Format = new JsonFormat();

        public override Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings) => mapper => new Serializer();

        internal override IWireFormat CreateWireFormat() => Format;

        sealed class JsonFormat : IWireFormat
        {
            public string ContentType => "application/json";
            public byte[] Serialize(Type type, object value) => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value, type, Options);

            public object Deserialize(Type type, ReadOnlyMemory<byte> body) =>
                System.Text.Json.JsonSerializer.Deserialize(body.Span, type, Options);
        }

        sealed class Serializer : IMessageSerializer
        {
            public string ContentType => Format.ContentType;

            public void Serialize(object message, Stream stream)
            {
                var bytes = Format.Serialize(message.GetType(), message);
                stream.Write(bytes, 0, bytes.Length);
            }

            public object[] Deserialize(Stream stream, IList<Type> messageTypes = null)
            {
                var type = messageTypes != null && messageTypes.Count > 0 ? messageTypes[0] : typeof(object);
                using (var buffer = new MemoryStream())
                {
                    stream.CopyTo(buffer);
                    return new[] { Format.Deserialize(type, buffer.ToArray()) };
                }
            }
        }
    }

    /// <summary>Present so existing configuration compiles. Bq's wire format is MessagePack, so selecting it fails at configuration time.</summary>
    public sealed class XmlSerializer : SerializationDefinition
    {
        public override Func<IMessageMapper, IMessageSerializer> Configure(ReadOnlySettings settings) =>
            throw new NotSupportedException("Bq messages use MessagePack on the wire. Remove UseSerialization<XmlSerializer>().");
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
            configuration.SetWireFormat(serializationDefinition.CreateWireFormat());
            return new SerializationExtensions<T>(configuration.GetSettings(), new SettingsHolder());
        }

        /// <summary>Also accept messages in this format. Senders keep using the format chosen with <c>UseSerialization</c>.</summary>
        public static SerializationExtensions<T> AddDeserializer<T>(this EndpointConfiguration configuration) where T : SerializationDefinition, new() =>
            configuration.AddDeserializer(new T());

        public static SerializationExtensions<T> AddDeserializer<T>(this EndpointConfiguration configuration, T serializationDefinition)
            where T : SerializationDefinition
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (serializationDefinition == null) throw new ArgumentNullException(nameof(serializationDefinition));
            serializationDefinition.Configure(configuration.GetSettings());
            configuration.AddWireFormat(serializationDefinition.CreateWireFormat());
            return new SerializationExtensions<T>(configuration.GetSettings(), new SettingsHolder());
        }
    }

    public sealed partial class EndpointConfiguration
    {
        IWireFormat wireFormat = MessagePackSerialization.Format;
        readonly List<IWireFormat> extraWireFormats = new List<IWireFormat>();

        internal void SetWireFormat(IWireFormat format) => wireFormat = format;
        internal void AddWireFormat(IWireFormat format) => extraWireFormats.Add(format);
        internal IWireFormat WireFormat => wireFormat;

        /// <summary>The formats the endpoint reads, by content type: the one it writes plus any added with <c>AddDeserializer</c>.</summary>
        internal Dictionary<string, IWireFormat> ReadableFormats() =>
            new[] { wireFormat }.Concat(extraWireFormats).GroupBy(format => format.ContentType).ToDictionary(group => group.Key, group => group.First(), StringComparer.Ordinal);
    }
}
