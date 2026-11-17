using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class OutgoingMessage
    {
        public string Id { get; set; }
        public string CorrelationId { get; set; }
        public string ReplyTo { get; set; }
        public string Type { get; set; }
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
        public bool Mandatory { get; set; }
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
    public sealed class SagaConcurrencyException : Exception
    {
        public SagaConcurrencyException(string message) : base(message) { }
    }
}
