using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Bq.Messaging
{
    public static class MessageHeaders
    {
        public const string TimeSent = "x-bq-time-sent";
        public const string Deferred = "x-bq-deferred";

        public static string FindHeader(this IMessageHandlerContext context, string name)
        {
            if (!context.Headers.TryGetValue(name, out var value) || value == null) return null;
            if (value is byte[] bytes) return Encoding.UTF8.GetString(bytes);
            if (value is string text) return text;
            return value is IConvertible ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
        }

        public static string GetHeader(this IMessageHandlerContext context, string name) =>
            context.FindHeader(name) ?? throw new KeyNotFoundException("Message header is missing or has no text representation: " + name);
    }
}
