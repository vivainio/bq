using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Bq.Messaging.Configuration.AdvancedExtensibility;
using Bq.Messaging.DataBus;
using Bq.Messaging.Settings;
using MessagePack;
using MessagePack.Resolvers;

namespace Bq.Messaging
{
    /// <summary>A message property whose value travels through the data bus. Only the key goes over the wire.</summary>
    public interface IDataBusProperty
    {
        string Key { get; set; }
        bool HasValue { get; set; }
        object GetValue();
        void SetValue(object value);
    }

    [Serializable]
    public class DataBusProperty<T> : IDataBusProperty
    {
        public DataBusProperty() { }

        public DataBusProperty(T value)
        {
            Value = value;
            HasValue = true;
        }

        public string Key { get; set; }
        public bool HasValue { get; set; }

        /// <summary>Never serialized into the message: the data bus carries it.</summary>
        [IgnoreMember]
        [System.Text.Json.Serialization.JsonIgnore]
        public T Value { get; private set; }

        public object GetValue() => Value;

        public void SetValue(object value)
        {
            Value = (T)value;
            HasValue = true;
        }
    }

    public static class DataBusConventionExtensions
    {
        public static ConventionsBuilder DefiningDataBusPropertiesAs(this ConventionsBuilder builder, Func<PropertyInfo, bool> definesDataBusProperty)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            builder.Conventions.DataBusPropertyRule = definesDataBusProperty ?? throw new ArgumentNullException(nameof(definesDataBusProperty));
            return builder;
        }
    }
}

namespace Bq.Messaging.DataBus
{
    public interface IDataBus
    {
        /// <summary>Stores the stream and returns the key that retrieves it, until the time to be received passes.</summary>
        Task<string> Put(Stream stream, TimeSpan timeToBeReceived);
        Task<Stream> Get(string key);
        Task Start();
    }

    public interface IDataBusSerializer
    {
        void Serialize(object dataBusProperty, Stream stream);
        object Deserialize(Stream stream);
    }

    public abstract class DataBusDefinition
    {
        protected abstract Type ProvidedByFeature();
        internal virtual IDataBus Create(ReadOnlySettings settings) =>
            throw new NotSupportedException(GetType().Name + " cannot create a data bus. Register an IDataBus in the container instead.");
    }

    public class DataBusExtensions : ExposeSettings
    {
        public DataBusExtensions(SettingsHolder settings) : base(settings) { }
    }

    public class DataBusExtensions<T> : DataBusExtensions where T : DataBusDefinition
    {
        public DataBusExtensions(SettingsHolder settings) : base(settings) { }
    }

    sealed class FileShareDataBusImplementation : IDataBus
    {
        readonly string basePath;
        int puts;

        public FileShareDataBusImplementation(string basePath) { this.basePath = basePath; }

        public Task Start()
        {
            Directory.CreateDirectory(basePath);
            RemoveExpired();
            return Task.CompletedTask;
        }

        public async Task<string> Put(Stream stream, TimeSpan timeToBeReceived)
        {
            var key = Path.Combine(DateTime.UtcNow.ToString("yyyy-MM-dd_HH"), Guid.NewGuid().ToString("N")).Replace('\\', '/');
            var file = FileFor(key);
            Directory.CreateDirectory(Path.GetDirectoryName(file));
            using (var output = new FileStream(file, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, useAsync: true))
                await stream.CopyToAsync(output).ConfigureAwait(false);
            if (timeToBeReceived != TimeSpan.MaxValue)
                File.WriteAllText(file + ".expires", DateTime.UtcNow.Add(timeToBeReceived).Ticks.ToString());
            if (System.Threading.Interlocked.Increment(ref puts) % 100 == 0) RemoveExpired();
            return key;
        }

        public Task<Stream> Get(string key)
        {
            var file = FileFor(key);
            if (!File.Exists(file)) throw new FileNotFoundException("The data bus value is missing or expired: " + key);
            return Task.FromResult<Stream>(new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true));
        }

        string FileFor(string key)
        {
            var full = Path.GetFullPath(Path.Combine(basePath, key));
            // Keys come off the wire, so they must not escape the base directory.
            if (!full.StartsWith(Path.GetFullPath(basePath), StringComparison.Ordinal))
                throw new InvalidOperationException("The data bus key points outside the data bus directory.");
            return full;
        }

        void RemoveExpired()
        {
            try
            {
                foreach (var marker in Directory.EnumerateFiles(basePath, "*.expires", SearchOption.AllDirectories))
                {
                    if (!long.TryParse(File.ReadAllText(marker), out var ticks) || new DateTime(ticks, DateTimeKind.Utc) > DateTime.UtcNow) continue;
                    File.Delete(marker.Substring(0, marker.Length - ".expires".Length));
                    File.Delete(marker);
                }
            }
            catch (IOException ex) { System.Diagnostics.Trace.TraceWarning("Data bus cleanup failed: {0}", ex.Message); }
        }
    }

    sealed class MessagePackDataBusSerializer : IDataBusSerializer
    {
        static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);

        readonly Type valueType;
        public MessagePackDataBusSerializer(Type valueType) { this.valueType = valueType; }
        public void Serialize(object dataBusProperty, Stream stream) => MessagePackSerializer.Serialize(valueType, stream, dataBusProperty, Options);
        public object Deserialize(Stream stream) => MessagePackSerializer.Deserialize(valueType, stream, Options);
    }
}

namespace Bq.Messaging
{
    using Bq.Messaging.DataBus;

    /// <summary>Stores data bus values as files below a directory that every endpoint in the exchange can reach.</summary>
    public sealed class FileShareDataBus : DataBusDefinition
    {
        internal const string BasePathKey = "Bq.DataBus.FileShare.BasePath";

        protected override Type ProvidedByFeature() => typeof(FileShareDataBus);

        internal override IDataBus Create(ReadOnlySettings settings)
        {
            if (!settings.TryGet<string>(BasePathKey, out var path))
                throw new InvalidOperationException("Call BasePath on UseDataBus<FileShareDataBus>() first.");
            return new FileShareDataBusImplementation(path);
        }
    }

    public static class ConfigureFileShareDataBus
    {
        public static DataBusExtensions<FileShareDataBus> BasePath(this DataBusExtensions<FileShareDataBus> config, string basePath)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrWhiteSpace(basePath)) throw new ArgumentException("A base path is required.", nameof(basePath));
            config.GetSettings().Set(FileShareDataBus.BasePathKey, basePath);
            return config;
        }
    }

    public static class UseDataBusExtensions
    {
        public static DataBusExtensions<T> UseDataBus<T>(this EndpointConfiguration configuration) where T : DataBusDefinition, new()
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.GetSettings().Set(DataBusKeys.Definition, typeof(T));
            return new DataBusExtensions<T>(configuration.GetSettings());
        }

        public static DataBusExtensions UseDataBus(this EndpointConfiguration configuration, Type dataBusType)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (dataBusType == null || dataBusType.IsAbstract || !typeof(DataBusDefinition).IsAssignableFrom(dataBusType))
                throw new ArgumentException("A concrete DataBusDefinition type is required.", nameof(dataBusType));
            configuration.GetSettings().Set(DataBusKeys.Definition, dataBusType);
            return new DataBusExtensions(configuration.GetSettings());
        }
    }

    static class DataBusKeys
    {
        public const string Definition = "Bq.DataBus.Definition";
    }

    public sealed partial class EndpointConfiguration
    {
        /// <summary>The data bus selected by <c>UseDataBus</c>, or null. A container-registered <see cref="IDataBus"/> is checked first by the endpoint.</summary>
        internal IDataBus CreateConfiguredDataBus() =>
            Settings.TryGet<Type>(DataBusKeys.Definition, out var type)
                ? ((DataBusDefinition)Activator.CreateInstance(type)).Create(Settings)
                : null;
    }

    public sealed partial class RabbitEndpoint
    {
        static readonly ConcurrentDictionary<Type, PropertyInfo[]> dataBusProperties = new ConcurrentDictionary<Type, PropertyInfo[]>();

        PropertyInfo[] DataBusPropertiesOf(Type contract)
        {
            var conventions = configuration.Conventions().Conventions;
            return dataBusProperties.GetOrAdd(contract, type =>
                type.GetProperties(BindingFlags.Instance | BindingFlags.Public).Concat(type.GetInterfaces().SelectMany(i => i.GetProperties()))
                    .Where(property => property.CanRead && property.GetIndexParameters().Length == 0 && conventions.IsDataBusProperty(property)).ToArray());
        }

        IDataBusSerializer SerializerFor(IDataBusProperty property)
        {
            var valueType = property.GetType().IsGenericType ? property.GetType().GetGenericArguments()[0] : typeof(object);
            return dataBusSerializer ?? new Bq.Messaging.DataBus.MessagePackDataBusSerializer(valueType);
        }

        /// <summary>Uploads data bus property values and records their keys, just before a message is serialized.</summary>
        async Task StoreDataBusProperties(object message, Type contract, TimeSpan timeToBeReceived)
        {
            foreach (var property in DataBusPropertiesOf(contract))
            {
                var value = property.GetValue(message);
                if (value == null) continue;
                var dataBusProperty = value as IDataBusProperty ?? throw new InvalidOperationException(
                    "Data bus property " + contract.Name + "." + property.Name + " must be an IDataBusProperty such as DataBusProperty<T>.");
                if (!dataBusProperty.HasValue) continue;
                var bus = dataBus ?? throw new InvalidOperationException("Configure a data bus with UseDataBus to send " + contract.Name + "." + property.Name + ".");
                using (var stream = new MemoryStream())
                {
                    SerializerFor(dataBusProperty).Serialize(dataBusProperty.GetValue(), stream);
                    stream.Position = 0;
                    dataBusProperty.Key = await bus.Put(stream, timeToBeReceived).ConfigureAwait(false);
                }
            }
        }

        /// <summary>Downloads data bus values for a received message.</summary>
        async Task FetchDataBusProperties(object message, Type contract)
        {
            foreach (var property in DataBusPropertiesOf(contract))
            {
                if (!(property.GetValue(message) is IDataBusProperty dataBusProperty) || string.IsNullOrEmpty(dataBusProperty.Key)) continue;
                var bus = dataBus ?? throw new InvalidOperationException("Configure a data bus with UseDataBus to receive " + contract.Name + "." + property.Name + ".");
                using (var stream = await bus.Get(dataBusProperty.Key).ConfigureAwait(false))
                    dataBusProperty.SetValue(SerializerFor(dataBusProperty).Deserialize(stream));
            }
        }
    }
}
