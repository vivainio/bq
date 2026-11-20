using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;

namespace Bq.Messaging
{
    public interface IMessagePropertyEncryptionService
    {
        string Encrypt(string plaintext);
        string Decrypt(string ciphertext);
    }

    public static class MessagePropertyEncryptionExtensions
    {
        public static void EnableMessagePropertyEncryption(this EndpointConfiguration endpointConfiguration,
            IMessagePropertyEncryptionService encryptionService, Func<PropertyInfo, bool> encryptedPropertyConvention)
        {
            if (endpointConfiguration == null) throw new ArgumentNullException(nameof(endpointConfiguration));
            if (encryptionService == null) throw new ArgumentNullException(nameof(encryptionService));
            if (encryptedPropertyConvention == null) throw new ArgumentNullException(nameof(encryptedPropertyConvention));
            endpointConfiguration.PropertyEncryption = new MessagePropertyEncryption(encryptionService, encryptedPropertyConvention);
        }
    }

    internal sealed class MessagePropertyEncryption
    {
        readonly IMessagePropertyEncryptionService service;
        readonly Func<PropertyInfo, bool> convention;
        readonly ConcurrentDictionary<Type, PropertyInfo[]> selected = new ConcurrentDictionary<Type, PropertyInfo[]>();

        public MessagePropertyEncryption(IMessagePropertyEncryptionService service, Func<PropertyInfo, bool> convention)
        {
            this.service = service;
            this.convention = convention;
        }

        public PropertyInfo[] Properties(Type contract) => selected.GetOrAdd(contract, FindProperties);

        PropertyInfo[] FindProperties(Type contract)
        {
            var properties = contract.GetProperties(BindingFlags.Instance | BindingFlags.Public).AsEnumerable();
            if (contract.IsInterface)
                properties = properties.Concat(contract.GetInterfaces().SelectMany(type => type.GetProperties()));

            var matches = properties.Where(convention).ToArray();
            foreach (var property in matches)
                if (property.PropertyType != typeof(string) || property.GetMethod?.IsPublic != true ||
                    property.SetMethod?.IsPublic != true || property.GetIndexParameters().Length != 0)
                    throw new InvalidOperationException("Encryption requires a public read/write string property: " + property.Name);
            return matches.GroupBy(property => property.Name, StringComparer.Ordinal).Select(group => group.First()).ToArray();
        }

        public void Transform(object message, PropertyInfo[] properties, bool encrypt)
        {
            foreach (var property in properties)
            {
                if (!(property.GetValue(message) is string value)) continue;
                var transformed = encrypt ? service.Encrypt(value) : service.Decrypt(value);
                if (transformed == null) throw new InvalidOperationException("The message encryption service returned null.");
                property.SetValue(message, transformed);
            }
        }
    }
}
