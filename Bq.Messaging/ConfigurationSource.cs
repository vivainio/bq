using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Bq.Messaging.Config;
using Bq.Messaging.Config.ConfigurationSource;
using Bq.Messaging.Logging;

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

namespace Bq.Messaging.Config
{
    public enum KeyFormat { Ascii, Base64 }

    /// <summary>The <c>AuditConfig</c> section: where processed messages are copied.</summary>
    public class AuditConfig : ConfigurationSection
    {
        [ConfigurationProperty("QueueName", IsRequired = true)]
        public string QueueName
        {
            get => (string)this["QueueName"];
            set => this["QueueName"] = value;
        }

        [ConfigurationProperty("OverrideTimeToBeReceived", IsRequired = false, DefaultValue = "00:00:00")]
        public TimeSpan OverrideTimeToBeReceived
        {
            get => (TimeSpan)this["OverrideTimeToBeReceived"];
            set => this["OverrideTimeToBeReceived"] = value;
        }
    }

    /// <summary>The <c>Logging</c> section: the minimum level of <see cref="DefaultFactory"/>.</summary>
    public class Logging : ConfigurationSection
    {
        [ConfigurationProperty("Threshold", IsRequired = false, DefaultValue = "Info")]
        public string Threshold
        {
            get => (string)this["Threshold"];
            set => this["Threshold"] = value;
        }
    }

    /// <summary>The <c>MessageForwardingInCaseOfFaultConfig</c> section: the error queue.</summary>
    public class MessageForwardingInCaseOfFaultConfig : ConfigurationSection
    {
        [ConfigurationProperty("ErrorQueue", IsRequired = true)]
        public string ErrorQueue
        {
            get => (string)this["ErrorQueue"];
            set => this["ErrorQueue"] = value;
        }
    }

    /// <summary>One <c>add</c> element of <c>MessageEndpointMappings</c>: which messages go to which endpoint.</summary>
    public class MessageEndpointMapping : ConfigurationElement, IComparable<MessageEndpointMapping>
    {
        [ConfigurationProperty("Messages", IsRequired = false)]
        public string Messages
        {
            get => (string)this["Messages"];
            set => this["Messages"] = value;
        }

        [ConfigurationProperty("Endpoint", IsRequired = true)]
        public string Endpoint
        {
            get => (string)this["Endpoint"];
            set => this["Endpoint"] = value;
        }

        [ConfigurationProperty("Assembly", IsRequired = false)]
        public string AssemblyName
        {
            get => (string)this["Assembly"];
            set => this["Assembly"] = value;
        }

        [ConfigurationProperty("Type", IsRequired = false)]
        public string TypeFullName
        {
            get => (string)this["Type"];
            set => this["Type"] = value;
        }

        [ConfigurationProperty("Namespace", IsRequired = false)]
        public string Namespace
        {
            get => (string)this["Namespace"];
            set => this["Namespace"] = value;
        }

        /// <summary>Orders the more specific mappings (type, then namespace, then assembly) first.</summary>
        public int CompareTo(MessageEndpointMapping other)
        {
            if (other == null) return -1;
            return Specificity(other).CompareTo(Specificity(this));
        }

        int Specificity(MessageEndpointMapping mapping) =>
            !string.IsNullOrEmpty(mapping.TypeFullName) || (mapping.Messages ?? "").Contains(",") ? 3
            : !string.IsNullOrEmpty(mapping.Namespace) ? 2 : 1;

        /// <summary>Resolves the mapping to message types and reports each with its endpoint.</summary>
        public void Configure(Action<Type, string> mapTypeToEndpoint) => Configure(mapTypeToEndpoint, _ => true);

        public void Configure(Action<Type, string> mapTypeToEndpoint, Func<Type, bool> isMessageType)
        {
            if (mapTypeToEndpoint == null) throw new ArgumentNullException(nameof(mapTypeToEndpoint));
            if (string.IsNullOrWhiteSpace(Endpoint)) throw new ConfigurationErrorsException("A message endpoint mapping needs an Endpoint.");
            var messages = Messages;
            var assemblyName = AssemblyName;
            var typeName = TypeFullName;
            if (!string.IsNullOrEmpty(messages))
            {
                if (!string.IsNullOrEmpty(assemblyName) || !string.IsNullOrEmpty(typeName) || !string.IsNullOrEmpty(Namespace))
                    throw new ConfigurationErrorsException("Use either Messages or Assembly, Type and Namespace in a mapping, not both.");
                // "Namespace.Type, Assembly" names one type; a bare name is an assembly.
                if (messages.Contains(",")) typeName = messages;
                else assemblyName = messages;
            }
            if (!string.IsNullOrEmpty(typeName))
            {
                var type = Type.GetType(typeName, throwOnError: false) ?? (!string.IsNullOrEmpty(assemblyName)
                    ? Load(assemblyName)?.GetType(typeName, throwOnError: false) : null) ??
                    throw new ConfigurationErrorsException("The message type " + typeName + " could not be loaded.");
                mapTypeToEndpoint(type, Endpoint);
                return;
            }
            if (string.IsNullOrEmpty(assemblyName)) throw new ConfigurationErrorsException("A message endpoint mapping needs Messages, Type or Assembly.");
            var assembly = Load(assemblyName) ?? throw new ConfigurationErrorsException("The assembly " + assemblyName + " could not be loaded.");
            IEnumerable<Type> types;
            try { types = assembly.GetTypes(); }
            catch (System.Reflection.ReflectionTypeLoadException ex) { types = ex.Types.Where(type => type != null); }
            foreach (var type in types.Where(type => type.IsPublic || type.IsNestedPublic))
                if ((string.IsNullOrEmpty(Namespace) || type.Namespace == Namespace) && isMessageType(type)) mapTypeToEndpoint(type, Endpoint);
        }

        static System.Reflection.Assembly Load(string name)
        {
            var loaded = AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(assembly =>
                string.Equals(assembly.GetName().Name, name, StringComparison.OrdinalIgnoreCase));
            if (loaded != null) return loaded;
            try { return System.Reflection.Assembly.Load(name); }
            catch (Exception) { return null; }
        }

        public override string ToString() => (Messages ?? TypeFullName ?? AssemblyName ?? "") + "->" + Endpoint;
    }

    [ConfigurationCollection(typeof(MessageEndpointMapping), AddItemName = "add", CollectionType = ConfigurationElementCollectionType.BasicMap)]
    public class MessageEndpointMappingCollection : ConfigurationElementCollection
    {
        public override ConfigurationElementCollectionType CollectionType => ConfigurationElementCollectionType.BasicMap;
        protected override string ElementName => "add";
        protected override ConfigurationElement CreateNewElement() => new MessageEndpointMapping();
        protected override object GetElementKey(ConfigurationElement element) => element.ToString();

        public MessageEndpointMapping this[int index] => (MessageEndpointMapping)BaseGet(index);
        public void Add(MessageEndpointMapping mapping) => BaseAdd(mapping);
        public void Clear() => BaseClear();
        public int IndexOf(MessageEndpointMapping mapping) => BaseIndexOf(mapping);
        public void Remove(MessageEndpointMapping mapping) => BaseRemove(GetElementKey(mapping));
        public void RemoveAt(int index) => BaseRemoveAt(index);
    }

    /// <summary>The <c>UnicastBusConfig</c> section: message routes and forwarding.</summary>
    public class UnicastBusConfig : ConfigurationSection
    {
        [ConfigurationProperty("ForwardReceivedMessagesTo", IsRequired = false)]
        public string ForwardReceivedMessagesTo
        {
            get => (string)this["ForwardReceivedMessagesTo"];
            set => this["ForwardReceivedMessagesTo"] = value;
        }

        [ConfigurationProperty("TimeToBeReceivedOnForwardedMessages", IsRequired = false, DefaultValue = "00:00:00")]
        public TimeSpan TimeToBeReceivedOnForwardedMessages
        {
            get => (TimeSpan)this["TimeToBeReceivedOnForwardedMessages"];
            set => this["TimeToBeReceivedOnForwardedMessages"] = value;
        }

        [ConfigurationProperty("TimeoutManagerAddress", IsRequired = false)]
        public string TimeoutManagerAddress
        {
            get => (string)this["TimeoutManagerAddress"];
            set => this["TimeoutManagerAddress"] = value;
        }

        [ConfigurationProperty("MessageEndpointMappings", IsRequired = false)]
        public MessageEndpointMappingCollection MessageEndpointMappings
        {
            get => (MessageEndpointMappingCollection)this["MessageEndpointMappings"];
            set => this["MessageEndpointMappings"] = value;
        }
    }

    /// <summary>Reads sections from the application's configuration file.</summary>
    public sealed class AppConfigConfigurationSource : IConfigurationSource
    {
        public T GetConfiguration<T>() where T : class, new()
        {
            try { return ConfigurationManager.GetSection(typeof(T).Name) as T; }
            catch (ConfigurationErrorsException) { throw; }
        }
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
        IConfigurationSource customConfigurationSource;
        readonly Dictionary<Type, object> configurationProviders = new Dictionary<Type, object>();

        /// <summary>
        /// Replaces the application configuration file as the source of sections such as <c>UnicastBusConfig</c>.
        /// Scanned <see cref="IProvideConfiguration{T}"/> types still take precedence for their section.
        /// </summary>
        public void CustomConfigurationSource(IConfigurationSource configurationSource) =>
            customConfigurationSource = configurationSource ?? throw new ArgumentNullException(nameof(configurationSource));

        internal void AddConfigurationProvider(Type sectionType, object provider) => configurationProviders[sectionType] = provider;

        internal T GetConfigSection<T>() where T : class, new()
        {
            if (configurationProviders.TryGetValue(typeof(T), out var provider))
                return (T)typeof(IProvideConfiguration<>).MakeGenericType(typeof(T)).GetMethod("GetConfiguration").Invoke(provider, null);
            return (customConfigurationSource ?? new AppConfigConfigurationSource()).GetConfiguration<T>();
        }

        /// <summary>Applies the configuration sections to settings that code has not already set. Called when the endpoint starts.</summary>
        internal void ApplyConfigurationSections()
        {
            Settings.Set("Bq.ConfigSections", (Func<Type, object>)(type =>
                typeof(EndpointConfiguration).GetMethod(nameof(GetConfigSection), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
                    .MakeGenericMethod(type).Invoke(this, null)));
            var unicast = GetConfigSection<UnicastBusConfig>();
            if (unicast != null)
            {
                var isMessage = new Func<Type, bool>(Conventions().Conventions.IsMessageType);
                // Specific mappings first, so a type mapping beats an assembly mapping, and code beats the file.
                foreach (var mapping in unicast.MessageEndpointMappings.Cast<MessageEndpointMapping>().OrderBy(m => m, Comparer<MessageEndpointMapping>.Create((a, b) => a.CompareTo(b))))
                    mapping.Configure((type, endpoint) => { if (!Routes.ContainsKey(type)) Routes[type] = endpoint; }, isMessage);
                if (ForwardReceivedMessagesAddress == null && !string.IsNullOrWhiteSpace(unicast.ForwardReceivedMessagesTo))
                    ForwardReceivedMessagesAddress = unicast.ForwardReceivedMessagesTo;
            }
            var fault = GetConfigSection<MessageForwardingInCaseOfFaultConfig>();
            if (fault != null && Recoverability.ErrorQueue == null && !string.IsNullOrWhiteSpace(fault.ErrorQueue))
                Recoverability.ErrorQueue = fault.ErrorQueue;
            var audit = GetConfigSection<AuditConfig>();
            if (audit != null && AuditQueue == null && !string.IsNullOrWhiteSpace(audit.QueueName))
            {
                AuditQueue = audit.QueueName;
                if (audit.OverrideTimeToBeReceived > TimeSpan.Zero) AuditExpiration = TimeToBeReceivedAttribute.ToExpiration(audit.OverrideTimeToBeReceived);
            }
            var logging = GetConfigSection<Config.Logging>();
            if (logging != null && Enum.TryParse<LogLevel>(logging.Threshold, true, out var level))
                LogManager.Use<DefaultFactory>().Level(level);
        }
    }
}
