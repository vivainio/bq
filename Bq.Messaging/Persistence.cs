using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    // Persistence providers can use this codec to store outgoing AMQP headers.
    public static class MessageHeaderCodec
    {
        public static byte[] Encode(System.Collections.Generic.IDictionary<string, object> headers) => RecoveryWire.EncodeHeaders(headers);
        public static System.Collections.Generic.IDictionary<string, object> Decode(byte[] bytes) => RecoveryWire.DecodeHeaders(bytes);
    }

    public sealed class OutgoingMessage
    {
        public string Id { get; set; }
        public string CorrelationId { get; set; }
        public string ReplyTo { get; set; }
        public string Type { get; set; }
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
        public bool Mandatory { get; set; }
        public System.Collections.Generic.IDictionary<string, object> Headers { get; set; }
        public byte[] Body { get; set; }
    }

    // Application data access shares the connection and transaction owned by the endpoint.
    public interface SynchronizedStorageSession
    {
        DbConnection Connection { get; }
        DbTransaction Transaction { get; }
    }

    // One session per processing attempt. The provider must make the inbox claim,
    // application changes, saga operations and outbox inserts one local transaction.
    public interface IPersistenceSession : SynchronizedStorageSession, IDisposable
    {
        Task<bool> TryClaimMessage(string endpoint, string messageId);
        Task StoreOutgoing(OutgoingMessage message);
        Task StoreTimeout(OutgoingMessage message, DateTime dueUtc);
        Task<SagaRecord> LoadSaga(string sagaType, string sagaId);
        Task SaveSaga(string sagaType, string sagaId, byte[] data, long? expectedVersion);
        Task DeleteSaga(string sagaType, string sagaId, long expectedVersion);
        void Commit();
    }
    public sealed class SagaRecord
    {
        public byte[] Data { get; set; }
        public long Version { get; set; }
    }
    public interface IPersistenceProvider
    {
        Task<IPersistenceSession> OpenSession();
        // A dispatcher may publish the same record more than once. IDs must remain stable.
        Task<OutgoingMessage> NextOutgoing(string endpoint);
        Task MarkDispatched(string endpoint, string messageId);
    }
    // A provider can hold a row lock from selection through broker confirmation.
    // Return true after dispatch is recorded, or false when no row is ready.
    // If publishing throws, release the lock and leave the row pending.
    public interface ILockedDispatchPersistence
    {
        Task<bool> DispatchOutgoing(string endpoint, Action<OutgoingMessage> publish);
        Task<bool> DispatchRecovery(string endpoint, Action<RecoveryRecord> publish);
    }

    /// <summary>
    /// Publishing failed because of this message, for example a mandatory message no queue accepted, and not because the broker
    /// is unavailable. A provider that records retry state defers the row and lets the rows behind it go first.
    /// </summary>
    public sealed class OutgoingMessageRejectedException : InvalidOperationException
    {
        public OutgoingMessageRejectedException(string messageId, string message) : base(message) { MessageId = messageId; }
        public string MessageId { get; }
    }

    /// <summary>
    /// Implemented by providers that keep retry state on outgoing rows. After a rejected publish the dispatcher records the
    /// failure, and <c>NextOutgoing</c> skips that row until its retry time so one bad row cannot hold back the others.
    /// </summary>
    public interface IOutgoingFailureTracking
    {
        Task RecordOutgoingFailure(string endpoint, string messageId, Exception failure);
    }

    /// <summary>
    /// Implemented by providers that can hand the dispatcher several ready rows at once, so the endpoint can publish them all and wait for
    /// one broker confirm instead of one per row. Rows come in the order they would come one at a time, from one table per call.
    /// </summary>
    public interface IBatchOutgoingPersistence
    {
        Task<System.Collections.Generic.IReadOnlyList<OutgoingMessage>> NextOutgoingBatch(string endpoint, int maxCount);
        Task MarkDispatched(string endpoint, System.Collections.Generic.IReadOnlyCollection<string> messageIds);
    }

    /// <summary>
    /// The batch form of <see cref="ILockedDispatchPersistence"/>. The provider locks up to <c>maxCount</c> ready rows, passes them to
    /// <c>publish</c>, which returns the IDs of the rows the broker rejected, then removes the other rows and records a failure on the
    /// rejected ones before it releases the lock. It returns how many rows it handled, or 0 when none were ready. If <c>publish</c> throws,
    /// the rows stay pending.
    /// </summary>
    public interface ILockedBatchDispatchPersistence
    {
        Task<int> DispatchOutgoingBatch(string endpoint, int maxCount,
            Func<System.Collections.Generic.IReadOnlyList<OutgoingMessage>, System.Collections.Generic.IReadOnlyCollection<string>> publish);
    }

    /// <summary>
    /// How long a rejected outgoing row waits before it is tried again: 100 milliseconds, doubling, up to 30 seconds. A
    /// queue that appears a moment later is found quickly, and a row that never succeeds is retried every 30 seconds.
    /// </summary>
    public static class OutgoingRetry
    {
        public static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(100);
        public static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);

        public static TimeSpan Delay(int attempts)
        {
            if (attempts < 1) attempts = 1;
            var delay = TimeSpan.FromTicks(BaseDelay.Ticks * (1L << Math.Min(attempts - 1, 10)));
            return delay > MaxDelay ? MaxDelay : delay;
        }

        public static string Describe(Exception failure)
        {
            var text = failure?.Message ?? "";
            return text.Length <= 500 ? text : text.Substring(0, 500);
        }
    }

    public sealed class SagaConcurrencyException : Exception
    {
        public SagaConcurrencyException(string message) : base(message) { }
    }
}
