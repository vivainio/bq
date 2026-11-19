using System;

namespace Bq.Messaging
{
    public interface IMessageCreator
    {
        T CreateInstance<T>();
        T CreateInstance<T>(Action<T> action);
        object CreateInstance(Type messageType);
    }

    public sealed class MessageCreator : IMessageCreator
    {
        internal static MessageCreator Default { get; } = new MessageCreator();

        public T CreateInstance<T>() => (T)CreateInstance(typeof(T));

        public T CreateInstance<T>(Action<T> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            var message = CreateInstance<T>();
            action(message);
            return message;
        }

        public object CreateInstance(Type messageType) =>
            InterfaceMessage.Create(messageType ?? throw new ArgumentNullException(nameof(messageType)));
    }
}
