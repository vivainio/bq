using System;

namespace Bq.Messaging
{
    public sealed class Conventions
    {
        internal Func<Type, bool> MessageRule { get; set; }
        internal Func<Type, bool> CommandRule { get; set; }
        internal Func<Type, bool> EventRule { get; set; }

        public bool IsMessageType(Type type) => Match(type, MessageRule, typeof(IMessage)) ||
            IsCommandType(type) || IsEventType(type);

        public bool IsCommandType(Type type) => Match(type, CommandRule, typeof(ICommand));
        public bool IsEventType(Type type) => Match(type, EventRule, typeof(IEvent));

        static bool Match(Type type, Func<Type, bool> rule, Type marker)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            return rule?.Invoke(type) ?? marker.IsAssignableFrom(type);
        }
    }

    public sealed class ConventionsBuilder
    {
        public Conventions Conventions { get; } = new Conventions();

        public ConventionsBuilder DefiningMessagesAs(Func<Type, bool> definesMessageType)
        {
            Conventions.MessageRule = definesMessageType ?? throw new ArgumentNullException(nameof(definesMessageType));
            return this;
        }

        public ConventionsBuilder DefiningCommandsAs(Func<Type, bool> definesCommandType)
        {
            Conventions.CommandRule = definesCommandType ?? throw new ArgumentNullException(nameof(definesCommandType));
            return this;
        }

        public ConventionsBuilder DefiningEventsAs(Func<Type, bool> definesEventType)
        {
            Conventions.EventRule = definesEventType ?? throw new ArgumentNullException(nameof(definesEventType));
            return this;
        }
    }
}
