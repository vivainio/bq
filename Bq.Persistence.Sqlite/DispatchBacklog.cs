namespace Bq.Persistence.Sqlite
{
    public sealed class DispatchBacklog
    {
        internal DispatchBacklog(long outgoing, long dueRecoveries, long scheduledRecoveries)
        {
            Outgoing = outgoing;
            DueRecoveries = dueRecoveries;
            ScheduledRecoveries = scheduledRecoveries;
        }

        /// <summary>Outbox rows not yet published, including delayed sends and saga timeouts, which the broker holds once published.</summary>
        public long Outgoing { get; }
        public long DueRecoveries { get; }
        public long ScheduledRecoveries { get; }
    }
}
