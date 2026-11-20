using System;
using System.Collections.Generic;
using System.Linq;

namespace Bq.Messaging
{
    public interface IMessageConvention
    {
        string Name { get; }
        bool IsCommandType(Type type);
        bool IsEventType(Type type);
        bool IsMessageType(Type type);
    }

    public sealed class Conventions
    {
        readonly List<Func<Type, bool>> systemMessageRules = new List<Func<Type, bool>>();
        internal List<IMessageConvention> Additional { get; } = new List<IMessageConvention>();
        internal Func<Type, bool> MessageRule { get; set; }
        internal Func<Type, bool> CommandRule { get; set; }
        internal Func<Type, bool> EventRule { get; set; }
        internal Func<Type, TimeSpan> TimeToBeReceivedRule { get; set; }

        public void AddSystemMessagesConventions(Func<Type, bool> definesMessageType) =>
            systemMessageRules.Add(definesMessageType ?? throw new ArgumentNullException(nameof(definesMessageType)));

        public bool IsInSystemConventionList(Type t)
        {
            if (t == null) throw new ArgumentNullException(nameof(t));
            return systemMessageRules.Any(rule => rule(t));
        }

        public bool IsMessageType(Type type) => IsInSystemConventionList(type) || Match(type, MessageRule, typeof(IMessage)) ||
            Additional.Any(convention => convention.IsMessageType(type)) || IsCommandType(type) || IsEventType(type);

        public bool IsCommandType(Type type) => Match(type, CommandRule, typeof(ICommand)) ||
            Additional.Any(convention => convention.IsCommandType(type));
        public bool IsEventType(Type type) => Match(type, EventRule, typeof(IEvent)) ||
            Additional.Any(convention => convention.IsEventType(type));

        internal bool IsSubscribedEvent(Type type, bool includeMarker) =>
            (EventRule?.Invoke(type) ?? (includeMarker && typeof(IEvent).IsAssignableFrom(type))) ||
            Additional.Any(convention => convention.IsEventType(type));

        static bool Match(Type type, Func<Type, bool> rule, Type marker)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            return rule?.Invoke(type) ?? marker.IsAssignableFrom(type);
        }
    }

    public sealed class ConventionsBuilder
    {
        public Conventions Conventions { get; } = new Conventions();

        public ConventionsBuilder Add(IMessageConvention messageConvention)
        {
            Conventions.Additional.Add(messageConvention ?? throw new ArgumentNullException(nameof(messageConvention)));
            return this;
        }

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

    public static class TimeToBeReceivedConventionExtensions
    {
        public static ConventionsBuilder DefiningTimeToBeReceivedAs(this ConventionsBuilder builder,
            Func<Type, TimeSpan> retrieveTimeToBeReceived)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            builder.Conventions.TimeToBeReceivedRule = retrieveTimeToBeReceived ??
                throw new ArgumentNullException(nameof(retrieveTimeToBeReceived));
            return builder;
        }
    }
}
