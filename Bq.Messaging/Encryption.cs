using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Bq.Messaging.Config;
using MessagePack;

namespace Bq.Messaging
{
    public class EncryptedValue
    {
        public string EncryptedBase64Value { get; set; }
        public string Base64Iv { get; set; }
    }

    /// <summary>
    /// A string that travels encrypted. Set <see cref="Value"/> before sending; only <see cref="EncryptedValue"/> goes on the
    /// wire, and the receiver reads the decrypted <see cref="Value"/>.
    /// </summary>
    [Serializable]
    public class WireEncryptedString
    {
        [IgnoreMember]
        [System.Text.Json.Serialization.JsonIgnore]
        public string Value { get; set; }

        public EncryptedValue EncryptedValue { get; set; }
    }

    public interface IEncryptionService
    {
        EncryptedValue Encrypt(string value, IOutgoingLogicalMessageContext context);
        string Decrypt(EncryptedValue encryptedValue, IIncomingLogicalMessageContext context);
    }
}

namespace Bq.Messaging.Config
{
    public class RijndaelExpiredKey : ConfigurationElement
    {
        [ConfigurationProperty("Key", IsRequired = true)]
        public string Key
        {
            get => (string)this["Key"];
            set => this["Key"] = value;
        }

        [ConfigurationProperty("KeyIdentifier", IsRequired = false)]
        public string KeyIdentifier
        {
            get => (string)this["KeyIdentifier"];
            set => this["KeyIdentifier"] = value;
        }

        [ConfigurationProperty("KeyFormat", IsRequired = false, DefaultValue = KeyFormat.Ascii)]
        public KeyFormat KeyFormat
        {
            get => (KeyFormat)this["KeyFormat"];
            set => this["KeyFormat"] = value;
        }
    }

    [ConfigurationCollection(typeof(RijndaelExpiredKey), AddItemName = "add", CollectionType = ConfigurationElementCollectionType.BasicMap)]
    public class RijndaelExpiredKeyCollection : ConfigurationElementCollection
    {
        public override ConfigurationElementCollectionType CollectionType => ConfigurationElementCollectionType.BasicMap;
        protected override string ElementName => "add";
        protected override ConfigurationElement CreateNewElement() => new RijndaelExpiredKey();
        protected override object GetElementKey(ConfigurationElement element) => ((RijndaelExpiredKey)element).Key;

        public RijndaelExpiredKey this[int index] => (RijndaelExpiredKey)BaseGet(index);
        public void Add(RijndaelExpiredKey key) => BaseAdd(key);
        public void Clear() => BaseClear();
        public int IndexOf(RijndaelExpiredKey key) => BaseIndexOf(key);
        public void Remove(RijndaelExpiredKey key) => BaseRemove(GetElementKey(key));
        public void RemoveAt(int index) => BaseRemoveAt(index);
    }

    /// <summary>The <c>RijndaelEncryptionServiceConfig</c> section: the current key and the keys that only decrypt.</summary>
    public class RijndaelEncryptionServiceConfig : ConfigurationSection
    {
        [ConfigurationProperty("Key", IsRequired = true)]
        public string Key
        {
            get => (string)this["Key"];
            set => this["Key"] = value;
        }

        [ConfigurationProperty("KeyIdentifier", IsRequired = false)]
        public string KeyIdentifier
        {
            get => (string)this["KeyIdentifier"];
            set => this["KeyIdentifier"] = value;
        }

        [ConfigurationProperty("KeyFormat", IsRequired = false, DefaultValue = KeyFormat.Ascii)]
        public KeyFormat KeyFormat
        {
            get => (KeyFormat)this["KeyFormat"];
            set => this["KeyFormat"] = value;
        }

        [ConfigurationProperty("ExpiredKeys", IsRequired = false)]
        public RijndaelExpiredKeyCollection ExpiredKeys
        {
            get => (RijndaelExpiredKeyCollection)this["ExpiredKeys"];
            set => this["ExpiredKeys"] = value;
        }
    }
}

namespace Bq.Messaging
{
    public static class ConfigureRijndaelEncryptionService
    {
        /// <summary>Encrypts <see cref="WireEncryptedString"/> properties with one key and can decrypt with the expired keys.</summary>
        public static void RijndaelEncryptionService(this EndpointConfiguration configuration, string encryptionKeyIdentifier, byte[] encryptionKey,
            IList<byte[]> expiredKeys = null)
        {
            if (encryptionKey == null) throw new ArgumentNullException(nameof(encryptionKey));
            configuration.RijndaelEncryptionService(encryptionKeyIdentifier,
                new Dictionary<string, byte[]> { [encryptionKeyIdentifier ?? throw new ArgumentNullException(nameof(encryptionKeyIdentifier))] = encryptionKey }, expiredKeys);
        }

        public static void RijndaelEncryptionService(this EndpointConfiguration configuration, string encryptionKeyIdentifier,
            IDictionary<string, byte[]> keys, IList<byte[]> expiredKeys = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var service = new RijndaelEncryptionServiceImplementation(encryptionKeyIdentifier, keys, expiredKeys ?? new List<byte[]>());
            configuration.RegisterEncryptionService(() => service);
        }

        /// <summary>Reads the <c>RijndaelEncryptionServiceConfig</c> section from the configuration source.</summary>
        public static void RijndaelEncryptionService(this EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var section = configuration.GetConfigSection<RijndaelEncryptionServiceConfig>() ??
                throw new InvalidOperationException("The RijndaelEncryptionServiceConfig section is missing.");
            if (string.IsNullOrWhiteSpace(section.Key)) throw new ConfigurationErrorsException("The Rijndael key is empty.");
            byte[] Decode(string key, KeyFormat format) => format == KeyFormat.Base64 ? Convert.FromBase64String(key) : Encoding.ASCII.GetBytes(key);
            var expired = section.ExpiredKeys?.Cast<RijndaelExpiredKey>().Select(key => Decode(key.Key, key.KeyFormat)).ToList() ?? new List<byte[]>();
            configuration.RijndaelEncryptionService(section.KeyIdentifier ?? "", Decode(section.Key, section.KeyFormat), expired);
        }

        public static void RegisterEncryptionService(this EndpointConfiguration configuration, Func<IEncryptionService> encryptionServiceFactory)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.SetEncryptionServiceFactory(encryptionServiceFactory ?? throw new ArgumentNullException(nameof(encryptionServiceFactory)));
        }
    }

    sealed class RijndaelEncryptionServiceImplementation : IEncryptionService
    {
        public const string KeyIdentifierHeader = "NServiceBus.RijndaelKeyIdentifier";
        readonly string keyIdentifier;
        readonly IDictionary<string, byte[]> keys;
        readonly IList<byte[]> expiredKeys;

        public RijndaelEncryptionServiceImplementation(string keyIdentifier, IDictionary<string, byte[]> keys, IList<byte[]> expiredKeys)
        {
            this.keyIdentifier = keyIdentifier;
            this.keys = keys ?? throw new ArgumentNullException(nameof(keys));
            this.expiredKeys = expiredKeys;
            if (!keys.ContainsKey(keyIdentifier ?? "")) throw new ArgumentException("The key identifier must name one of the keys.", nameof(keyIdentifier));
            foreach (var key in keys.Values.Concat(expiredKeys)) Validate(key);
        }

        static void Validate(byte[] key)
        {
            if (key == null || (key.Length != 16 && key.Length != 24 && key.Length != 32))
                throw new ArgumentException("Rijndael keys must be 16, 24 or 32 bytes.");
        }

        public EncryptedValue Encrypt(string value, IOutgoingLogicalMessageContext context)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            context.Headers[KeyIdentifierHeader] = keyIdentifier;
            using (var aes = Aes.Create())
            {
                aes.Key = keys[keyIdentifier];
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                aes.GenerateIV();
                using (var encryptor = aes.CreateEncryptor())
                {
                    var plain = Encoding.UTF8.GetBytes(value);
                    var cipher = encryptor.TransformFinalBlock(plain, 0, plain.Length);
                    return new EncryptedValue { EncryptedBase64Value = Convert.ToBase64String(cipher), Base64Iv = Convert.ToBase64String(aes.IV) };
                }
            }
        }

        public string Decrypt(EncryptedValue encryptedValue, IIncomingLogicalMessageContext context)
        {
            if (encryptedValue == null) throw new ArgumentNullException(nameof(encryptedValue));
            var candidates = new List<byte[]>();
            // The sender names its key; keys that were retired since may still have encrypted messages in flight.
            if (context.Headers.TryGetValue(KeyIdentifierHeader, out var id) && keys.TryGetValue(id, out var named)) candidates.Add(named);
            candidates.AddRange(keys.Values.Where(key => !candidates.Contains(key)));
            candidates.AddRange(expiredKeys);
            foreach (var key in candidates)
            {
                try { return Decrypt(encryptedValue, key); }
                catch (CryptographicException) { }
            }
            throw new InvalidOperationException("The value could not be decrypted with the current or expired keys.");
        }

        static string Decrypt(EncryptedValue value, byte[] key)
        {
            using (var aes = Aes.Create())
            {
                aes.Key = key;
                aes.IV = Convert.FromBase64String(value.Base64Iv);
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                using (var decryptor = aes.CreateDecryptor())
                {
                    var cipher = Convert.FromBase64String(value.EncryptedBase64Value);
                    return Encoding.UTF8.GetString(decryptor.TransformFinalBlock(cipher, 0, cipher.Length));
                }
            }
        }
    }

    public sealed partial class EndpointConfiguration
    {
        Func<IEncryptionService> encryptionServiceFactory;

        internal void SetEncryptionServiceFactory(Func<IEncryptionService> factory) => encryptionServiceFactory = factory;
        internal IEncryptionService CreateEncryptionService() => encryptionServiceFactory?.Invoke();
    }

    public sealed partial class RabbitEndpoint
    {
        static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, PropertyInfo[]> wireEncryptedProperties =
            new System.Collections.Concurrent.ConcurrentDictionary<Type, PropertyInfo[]>();

        static PropertyInfo[] WireEncryptedPropertiesOf(Type contract) =>
            wireEncryptedProperties.GetOrAdd(contract, type =>
                type.GetProperties(BindingFlags.Instance | BindingFlags.Public).Concat(type.GetInterfaces().SelectMany(i => i.GetProperties()))
                    .Where(property => property.CanRead && property.PropertyType == typeof(WireEncryptedString)).ToArray());

        void EncryptWireStrings(object message, Type contract, IOutgoingLogicalMessageContext context)
        {
            foreach (var property in WireEncryptedPropertiesOf(contract))
            {
                var wire = (WireEncryptedString)property.GetValue(message);
                if (wire?.Value == null) continue;
                var service = encryptionService ?? throw new InvalidOperationException(
                    "Configure an encryption service, for example RijndaelEncryptionService, to send " + contract.Name + "." + property.Name + ".");
                wire.EncryptedValue = service.Encrypt(wire.Value, context);
            }
        }

        void DecryptWireStrings(object message, Type contract, IIncomingLogicalMessageContext context)
        {
            foreach (var property in WireEncryptedPropertiesOf(contract))
            {
                var wire = (WireEncryptedString)property.GetValue(message);
                if (wire?.EncryptedValue == null) continue;
                var service = encryptionService ?? throw new InvalidOperationException(
                    "Configure an encryption service, for example RijndaelEncryptionService, to read " + contract.Name + "." + property.Name + ".");
                wire.Value = service.Decrypt(wire.EncryptedValue, context);
            }
        }
    }
}
