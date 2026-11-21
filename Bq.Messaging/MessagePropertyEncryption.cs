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

    [AttributeUsage(AttributeTargets.Property, Inherited = true)]
    public sealed class EncryptedPropertyAttribute : Attribute { }

    public static class MessagePropertyEncryptionExtensions
    {
        public static void EnableMessagePropertyEncryption(this EndpointConfiguration endpointConfiguration,
            IMessagePropertyEncryptionService encryptionService) =>
            EnableMessagePropertyEncryption(endpointConfiguration, encryptionService,
                property => endpointConfiguration.Conventions().Conventions.IsEncryptedProperty(property));

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
        static readonly ConcurrentDictionary<Type, bool> markedContracts = new ConcurrentDictionary<Type, bool>();
        readonly IMessagePropertyEncryptionService service;
        readonly Func<PropertyInfo, bool> convention;
        readonly ConcurrentDictionary<Type, PropertyInfo[]> selected = new ConcurrentDictionary<Type, PropertyInfo[]>();

        public MessagePropertyEncryption(IMessagePropertyEncryptionService service, Func<PropertyInfo, bool> convention)
        {
            this.service = service;
            this.convention = convention;
        }

        public static PropertyInfo[] Properties(Type contract, MessagePropertyEncryption settings)
        {
            if (settings != null) return settings.selected.GetOrAdd(contract, settings.FindProperties);
            if (markedContracts.GetOrAdd(contract, type => ContractProperties(type).Any(IsMarked) ||
                type.GetInterfaces().Any(iface => iface.GetProperties().Any(IsMarked))))
                throw new InvalidOperationException("Configure message property encryption for " + contract.FullName + ".");
            return Array.Empty<PropertyInfo>();
        }

        static bool IsMarked(PropertyInfo property) =>
            Attribute.IsDefined(property, typeof(EncryptedPropertyAttribute), inherit: true);

        static System.Collections.Generic.IEnumerable<PropertyInfo> ContractProperties(Type contract)
        {
            var properties = contract.GetProperties(BindingFlags.Instance | BindingFlags.Public).AsEnumerable();
            return contract.IsInterface
                ? properties.Concat(contract.GetInterfaces().SelectMany(type => type.GetProperties()))
                : properties;
        }

        PropertyInfo[] FindProperties(Type contract)
        {
            var properties = ContractProperties(contract).ToArray();
            var matches = properties.Where(property => IsMarked(property) || convention(property));
            if (!contract.IsInterface)
                matches = matches.Concat(ImplementedMarkedProperties(contract, properties));
            var selectedProperties = matches.ToArray();
            foreach (var property in selectedProperties)
                if (property.PropertyType != typeof(string) || property.GetMethod?.IsPublic != true ||
                    property.SetMethod?.IsPublic != true || property.GetIndexParameters().Length != 0)
                    throw new InvalidOperationException("Encryption requires a public read/write string property: " + property.Name);
            return selectedProperties.GroupBy(property => property.Name, StringComparer.Ordinal).Select(group => group.First()).ToArray();
        }

        static System.Collections.Generic.IEnumerable<PropertyInfo> ImplementedMarkedProperties(Type contract, PropertyInfo[] properties)
        {
            foreach (var iface in contract.GetInterfaces())
            {
                foreach (var property in iface.GetProperties().Where(IsMarked))
                {
                    var map = contract.GetInterfaceMap(iface);
                    var getterIndex = Array.IndexOf(map.InterfaceMethods, property.GetMethod);
                    var setterIndex = Array.IndexOf(map.InterfaceMethods, property.SetMethod);
                    var implementation = getterIndex >= 0 && setterIndex >= 0
                        ? properties.FirstOrDefault(candidate => candidate.GetMethod == map.TargetMethods[getterIndex] &&
                            candidate.SetMethod == map.TargetMethods[setterIndex])
                        : null;
                    if (implementation == null)
                        throw new InvalidOperationException("A marked interface property needs a public implementation: " + property.Name);
                    yield return implementation;
                }
            }
        }

        public void Transform(object message, PropertyInfo[] properties, bool encrypt)
        {
            foreach (var property in properties)
            {
                if (!(property.GetValue(message) is string value)) continue;
                string transformed;
                try { transformed = encrypt ? service.Encrypt(value) : service.Decrypt(value); }
                catch (Exception)
                {
                    // Provider diagnostics can contain keys or message values.
                    throw new InvalidOperationException(encrypt ? "Message property encryption failed." : "Message property decryption failed.");
                }
                if (transformed == null) throw new InvalidOperationException("The message encryption service returned null.");
                property.SetValue(message, transformed);
            }
        }
    }
}
