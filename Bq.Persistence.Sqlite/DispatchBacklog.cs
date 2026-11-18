namespace Bq.Persistence.Sqlite
{
    public sealed class DispatchBacklog
    {
        internal DispatchBacklog(long outgoing, long dueTimeouts, long scheduledTimeouts,
            long dueRecoveries, long scheduledRecoveries)
        {
            Outgoing = outgoing;
            DueTimeouts = dueTimeouts;
            ScheduledTimeouts = scheduledTimeouts;
            DueRecoveries = dueRecoveries;
            ScheduledRecoveries = scheduledRecoveries;
        }

        public long Outgoing { get; }
        public long DueTimeouts { get; }
        public long ScheduledTimeouts { get; }
        public long DueRecoveries { get; }
        public long ScheduledRecoveries { get; }
    }
}
