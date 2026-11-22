using System;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    /// <summary>
    /// How long an endpoint keeps the records that only exist to make delivery safe. An endpoint removes older records in the
    /// background, in small batches. Set a retention to null to keep that kind of record forever.
    /// </summary>
    public sealed class RetentionOptions
    {
        /// <summary>
        /// How long a handled message's ID stays in the inbox. The inbox is what stops a redelivered message from running twice, so
        /// keep this longer than any redelivery can happen, for example a broker queue that is replayed after an outage.
        /// </summary>
        public TimeSpan? InboxRetention { get; set; } = TimeSpan.FromDays(30);

        /// <summary>
        /// How long finished recovery records stay: retries that were sent and failed messages that were replayed. A failed message
        /// still waiting for a replay is never removed.
        /// </summary>
        public TimeSpan? RecoveryRetention { get; set; } = TimeSpan.FromDays(30);

        /// <summary>How often the cleanup runs. The first run comes after the shorter of this and one minute.</summary>
        public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

        internal bool Enabled => InboxRetention.HasValue || RecoveryRetention.HasValue;

        internal void Validate()
        {
            if (InboxRetention.HasValue && InboxRetention.Value <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(InboxRetention), "Inbox retention must be positive, or null to keep every record.");
            if (RecoveryRetention.HasValue && RecoveryRetention.Value <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(RecoveryRetention), "Recovery retention must be positive, or null to keep every record.");
            if (CleanupInterval < TimeSpan.FromMilliseconds(1) || CleanupInterval > TimeSpan.FromMilliseconds(int.MaxValue))
                throw new ArgumentOutOfRangeException(nameof(CleanupInterval));
        }
    }

    /// <summary>
    /// Implemented by providers that can remove old inbox and recovery records. Each call removes at most <c>maxRows</c> and returns how
    /// many it removed, so the endpoint can repeat until a call removes fewer.
    /// </summary>
    public interface IRetentionPersistence
    {
        /// <summary>Removes inbox rows claimed before the cutoff.</summary>
        Task<int> PurgeInbox(string endpoint, DateTime claimedBeforeUtc, int maxRows);

        /// <summary>
        /// Removes recovery records that failed before the cutoff and are finished: a retry that was sent, or a failed message that was
        /// replayed. A failed message that has not been replayed stays.
        /// </summary>
        Task<int> PurgeRecovery(string endpoint, DateTime failedBeforeUtc, int maxRows);
    }

    public sealed partial class EndpointConfiguration
    {
        /// <summary>How long inbox and recovery records are kept before the endpoint removes them.</summary>
        public RetentionOptions Retention { get; } = new RetentionOptions();
    }
}
