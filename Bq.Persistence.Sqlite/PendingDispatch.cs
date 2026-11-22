using System;

namespace Bq.Persistence.Sqlite
{
    public enum DispatchKind { Outbox, Recovery, Error }

    public sealed class PendingDispatch
    {
        internal PendingDispatch(string messageId, DispatchKind kind, string exchange, string routingKey, DateTime? dueAtUtc,
            int attempts = 0, string lastError = null, DateTime? retryAfterUtc = null)
        {
            Attempts = attempts;
            LastError = lastError;
            RetryAfterUtc = retryAfterUtc;
            MessageId = messageId;
            Kind = kind;
            Exchange = exchange;
            RoutingKey = routingKey;
            DueAtUtc = dueAtUtc;
        }

        public string MessageId { get; }
        public DispatchKind Kind { get; }
        public string Exchange { get; }
        public string RoutingKey { get; }
        public DateTime? DueAtUtc { get; }
        /// <summary>How many times publishing this row was rejected, for example because no queue accepted it.</summary>
        public int Attempts { get; }
        /// <summary>The reason for the last rejection, or null.</summary>
        public string LastError { get; }
        /// <summary>While in the future, the dispatcher skips this row. Null if it was never rejected.</summary>
        public DateTime? RetryAfterUtc { get; }
    }
}
