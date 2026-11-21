using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Bq.Messaging
{
    public interface IMessageCreator
    {
        T CreateInstance<T>();
        T CreateInstance<T>(Action<T> action);
        object CreateInstance(Type messageType);
    }

    public interface IMessageMapper : IMessageCreator
    {
        Type GetMappedTypeFor(Type t);
        Type GetMappedTypeFor(string typeName);
        void Initialize(IEnumerable<Type> types);
    }

    public sealed class MessageMapper : IMessageMapper
    {
        readonly ConcurrentDictionary<string, Type> knownTypes = new ConcurrentDictionary<string, Type>(StringComparer.Ordinal);

        public void Initialize(IEnumerable<Type> types)
        {
            if (types == null) throw new ArgumentNullException(nameof(types));
            foreach (var type in types)
            {
                var mapped = GetMappedTypeFor(type);
                Remember(type);
                Remember(mapped);
            }
        }

        public Type GetMappedTypeFor(Type t)
        {
            if (t == null) throw new ArgumentNullException(nameof(t));
            return t.IsInterface ? InterfaceMessage.SerializationType(t) : InterfaceMessage.ContractFor(t);
        }

        public Type GetMappedTypeFor(string typeName)
        {
            if (typeName == null) throw new ArgumentNullException(nameof(typeName));
            return knownTypes.TryGetValue(typeName, out var type) ? type : null;
        }

        void Remember(Type type)
        {
            if (type.FullName != null && knownTypes.GetOrAdd(type.FullName, type) != type)
                throw new InvalidOperationException("Multiple message types use the name " + type.FullName + ".");
            if (type.AssemblyQualifiedName != null) knownTypes.TryAdd(type.AssemblyQualifiedName, type);
        }

        public T CreateInstance<T>() => MessageCreator.Default.CreateInstance<T>();
        public T CreateInstance<T>(Action<T> action) => MessageCreator.Default.CreateInstance(action);
        public object CreateInstance(Type messageType) => MessageCreator.Default.CreateInstance(messageType);
    }

    public sealed class MessageCreator : IMessageCreator
    {
        internal static MessageCreator Default { get; } = new MessageCreator();

        public T CreateInstance<T>() => (T)CreateInstance(typeof(T));

        public T CreateInstance<T>(Action<T> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            if (typeof(T).IsValueType)
                throw new ArgumentException("Use a constructed value for a value-type message. An Action would receive a copy.", nameof(action));
            var message = CreateInstance<T>();
            action(message);
            return message;
        }

        public object CreateInstance(Type messageType) =>
            InterfaceMessage.Create(messageType ?? throw new ArgumentNullException(nameof(messageType)));
    }
}
