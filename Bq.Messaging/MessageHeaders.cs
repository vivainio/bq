using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace Bq.Messaging
{
    public static class MessageHeaders
    {
        public const string TimeSent = "x-bq-time-sent";
        public const string Deferred = "x-bq-deferred";
        public const string ConversationId = "Bq.ConversationId";
        public const string PreviousConversationId = "Bq.PreviousConversationId";
        public const string RelatedTo = "Bq.RelatedTo";

        internal static void AttachCausation(IDictionary<string, object> headers, IMessageProcessingContext incoming,
            Type messageType, object message, Func<ConversationIdStrategyContext, ConversationId> strategy)
        {
            var startNew = headers.TryGetValue(ConversationRoutingExtensions.NewConversation, out var requestedId);
            headers.Remove(ConversationRoutingExtensions.NewConversation);
            var inheritedId = incoming?.FindHeader(ConversationId);
            if (incoming?.MessageId != null) headers[RelatedTo] = incoming.MessageId;
            if ((startNew || inheritedId != null) && headers.ContainsKey(ConversationId))
                throw new InvalidOperationException("Use StartNewConversation to set a new conversation ID without a conversation header override.");

            if (startNew)
            {
                if (inheritedId != null) headers[PreviousConversationId] = inheritedId;
                headers[ConversationId] = (string)requestedId ?? GenerateId();
            }
            else if (!headers.ContainsKey(ConversationId))
            {
                headers[ConversationId] = inheritedId ?? GenerateId();
            }

            string GenerateId()
            {
                if (strategy == null) return Bq.Messaging.ConversationId.Default.Value;
                var context = new ConversationIdStrategyContext(new OutgoingLogicalMessage(messageType, message), AsText(headers));
                Bq.Messaging.ConversationId id;
                try { id = strategy(context); }
                catch (Exception exception)
                {
                    throw new InvalidOperationException("The custom conversation ID strategy failed.", exception);
                }
                return id?.Value ?? throw new InvalidOperationException("The custom conversation ID strategy returned null.");
            }
        }

        internal static IReadOnlyDictionary<string, string> AsText(IEnumerable<KeyValuePair<string, object>> headers) =>
            new ReadOnlyDictionary<string, string>(ToTextDictionary(headers));

        internal static Dictionary<string, string> ToTextDictionary(IEnumerable<KeyValuePair<string, object>> headers)
        {
            var result = new Dictionary<string, string>();
            foreach (var header in headers)
            {
                var value = AsText(header.Value);
                if (value != null) result.Add(header.Key, value);
            }
            return result;
        }

        static string AsText(object value)
        {
            if (value is byte[] bytes) return Encoding.UTF8.GetString(bytes);
            if (value is string text) return text;
            return value is IConvertible ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
        }

        public static string FindHeader(this IMessageHandlerContext context, string name) =>
            FindHeader((IMessageProcessingContext)context, name);

        public static string GetHeader(this IMessageHandlerContext context, string name) =>
            GetHeader((IMessageProcessingContext)context, name);

        public static string FindHeader(this IMessageProcessingContext context, string name) =>
            context.MessageHeaders.TryGetValue(name, out var value) ? value : null;

        public static string GetHeader(this IMessageProcessingContext context, string name) =>
            context.FindHeader(name) ?? throw new KeyNotFoundException("Message header is missing or has no text representation: " + name);
    }
}
