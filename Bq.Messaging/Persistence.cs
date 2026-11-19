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

    // One session per processing attempt. The provider must make the inbox claim,
    // application changes, saga operations and outbox inserts one local transaction.
    public interface IPersistenceSession : IDisposable
    {
        DbConnection Connection { get; }
        DbTransaction Transaction { get; }
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

    public sealed class SagaConcurrencyException : Exception
    {
        public SagaConcurrencyException(string message) : base(message) { }
    }
}
