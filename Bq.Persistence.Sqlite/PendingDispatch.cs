using System;

namespace Bq.Persistence.Sqlite
{
    public enum DispatchKind { Outbox, SagaTimeout, Recovery, Error, ScheduledSend }

    public sealed class PendingDispatch
    {
        internal PendingDispatch(string messageId, DispatchKind kind, string exchange, string routingKey, DateTime? dueAtUtc)
        {
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
    }
}
