using System;
using System.Globalization;

namespace Bq.Messaging
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, Inherited = true)]
    public sealed class TimeToBeReceivedAttribute : Attribute
    {
        public TimeToBeReceivedAttribute(string timeSpan)
        {
            TimeToBeReceived = TimeSpan.Parse(timeSpan, CultureInfo.InvariantCulture);
            if (TimeToBeReceived < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeSpan));
        }

        public TimeSpan TimeToBeReceived { get; }

        internal static string ToExpiration(TimeSpan duration) =>
            Math.Ceiling(duration.TotalMilliseconds).ToString("0", CultureInfo.InvariantCulture);
    }
}
