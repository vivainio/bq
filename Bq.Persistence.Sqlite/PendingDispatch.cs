using System;

namespace Bq.Persistence.Sqlite
{
    public enum DispatchKind { Outbox, SagaTimeout, Recovery, Error }

    public sealed class PendingDispatch
    {
        internal PendingDispatch(string messageId, DispatchKind kind, string routingKey, DateTime? dueAtUtc)
        {
            MessageId = messageId;
            Kind = kind;
            RoutingKey = routingKey;
            DueAtUtc = dueAtUtc;
        }

        public string MessageId { get; }
        public DispatchKind Kind { get; }
        public string RoutingKey { get; }
        public DateTime? DueAtUtc { get; }
    }
}
