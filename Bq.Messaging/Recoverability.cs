using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class RecoverabilityOptions
    {
        public int ImmediateRetries { get; set; } = 2;
        public TimeSpan[] DelayedRetries { get; set; } = { TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30) };
        public string ErrorQueue { get; set; }
        public Func<Exception, bool> IsUnrecoverable { get; set; }
    }
    // Payload is an encoded AMQP message snapshot. RecoveryId uniquely identifies
    // one failed delivery cycle, allowing a handoff to be committed only once.
    public sealed class RecoveryRecord
    {
        public string RecoveryId { get; set; }
        public string MessageId { get; set; }
        public int Cycle { get; set; }
        public bool IsError { get; set; }
        // True once persistence records the broker confirmation.
        public bool Dispatched { get; set; }
        public string ErrorQueue { get; set; }
        public string ExceptionType { get; set; }
        public string ExceptionMessage { get; set; }
        public string ExceptionDetail { get; set; }
        public DateTime FailedAtUtc { get; set; }
        public DateTime DueAtUtc { get; set; }
        public byte[] Payload { get; set; }
    }
    public interface IRecoverySession
    {
        Task<bool> HasRecovery(string endpoint, string recoveryId);
        Task StoreRecovery(string endpoint, RecoveryRecord record);
    }
    public interface IRecoveryPersistence
    {
        Task<RecoveryRecord> NextRecovery(string endpoint);
        Task MarkRecoveryDispatched(string endpoint, string recoveryId);
        Task<RecoveryRecord> GetError(string endpoint, string recoveryId);
        Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit);
        // Atomically enqueue a replay and mark the error replayed. The caller supplies
        // a new delivery identity while retaining the original identity in metadata.
        Task<bool> ReplayError(string endpoint, string recoveryId, RecoveryRecord replay);
    }
}
