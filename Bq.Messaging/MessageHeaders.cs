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

        internal static IReadOnlyDictionary<string, string> AsText(IEnumerable<KeyValuePair<string, object>> headers)
        {
            var result = new Dictionary<string, string>();
            foreach (var header in headers)
            {
                var value = AsText(header.Value);
                if (value != null) result.Add(header.Key, value);
            }
            return new ReadOnlyDictionary<string, string>(result);
        }

        static string AsText(object value)
        {
            if (value is byte[] bytes) return Encoding.UTF8.GetString(bytes);
            if (value is string text) return text;
            return value is IConvertible ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
        }

        public static string FindHeader(this IMessageHandlerContext context, string name) =>
            context.MessageHeaders.TryGetValue(name, out var value) ? value : null;

        public static string GetHeader(this IMessageHandlerContext context, string name) =>
            context.FindHeader(name) ?? throw new KeyNotFoundException("Message header is missing or has no text representation: " + name);
    }
}
