using System;
using System.Collections.Generic;

namespace Bq.Messaging
{
    public class ConversationId
    {
        ConversationId(string value) => Value = value;

        internal string Value { get; }
        public static ConversationId Default => new ConversationId(Guid.NewGuid().ToString());

        public static ConversationId Custom(string customValue)
        {
            if (string.IsNullOrWhiteSpace(customValue)) throw new ArgumentNullException(nameof(customValue));
            return new ConversationId(customValue);
        }
    }

    public class OutgoingLogicalMessage
    {
        public OutgoingLogicalMessage(Type messageType, object message)
        {
            MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
            Instance = message ?? throw new ArgumentNullException(nameof(message));
        }

        public Type MessageType { get; }
        public object Instance { get; }
    }

    public class ConversationIdStrategyContext
    {
        public ConversationIdStrategyContext(OutgoingLogicalMessage message, IReadOnlyDictionary<string, string> headers)
        {
            Message = message ?? throw new ArgumentNullException(nameof(message));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        }

        public OutgoingLogicalMessage Message { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
    }

    public static class MessageCausationConfigurationExtensions
    {
        public static void CustomConversationIdStrategy(this EndpointConfiguration endpointConfiguration,
            Func<ConversationIdStrategyContext, ConversationId> customStrategy)
        {
            if (endpointConfiguration == null) throw new ArgumentNullException(nameof(endpointConfiguration));
            endpointConfiguration.ConversationIdStrategy = customStrategy ?? throw new ArgumentNullException(nameof(customStrategy));
        }
    }
}
