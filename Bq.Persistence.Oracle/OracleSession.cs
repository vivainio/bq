using System;
using System.Data.Common;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Persistence.Oracle
{
    public sealed partial class OraclePersistence
    {
        sealed class Session : IPersistenceSession, IRecoverySession
        {
            readonly OraclePersistence owner;

            public Session(OraclePersistence owner, DbConnection connection, DbTransaction transaction)
            {
                this.owner = owner;
                Connection = connection;
                Transaction = transaction;
            }

            public DbConnection Connection { get; }
            public DbTransaction Transaction { get; }

            public async Task<bool> TryClaimMessage(string name, string messageId)
            {
                owner.CheckEndpoint(name);
                if (string.IsNullOrEmpty(messageId)) throw new ArgumentException("A message ID is required.", nameof(messageId));
                return await Execute(Connection, Transaction, @"INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(bq_inbox, bq_inbox_pk) */
                    INTO bq_inbox (endpoint, message_id, claimed_ticks) VALUES (:endpoint, :id, :claimed)",
                    ("endpoint", name), ("id", messageId), ("claimed", DateTime.UtcNow.Ticks)).ConfigureAwait(false) == 1;
            }

            public async Task StoreOutgoing(OutgoingMessage message)
            {
                await Execute(Connection, Transaction, @"INSERT INTO bq_outbox
                    (endpoint, id, correlation_id, reply_to, type_name, exchange_name, routing_key, mandatory, body, headers)
                    VALUES (:endpoint, :id, :correlation, :reply_to, :type_name, :exchange_name, :routing_key, :mandatory, :body, :headers)",
                    ("endpoint", owner.endpoint), ("id", message.Id), ("correlation", message.CorrelationId), ("reply_to", message.ReplyTo),
                    ("type_name", message.Type), ("exchange_name", message.Exchange), ("routing_key", message.RoutingKey),
                    ("mandatory", message.Mandatory ? 1 : 0), ("body", message.Body),
                    ("headers", MessageHeaderCodec.Encode(message.Headers))).ConfigureAwait(false);
            }

            public async Task<SagaRecord> LoadSaga(string sagaType, string sagaId)
            {
                using (var command = Command(Connection, Transaction, "SELECT saga_version, saga_data FROM bq_saga " +
                    "WHERE endpoint = :endpoint AND saga_type = :saga_type AND saga_id = :id",
                    ("endpoint", owner.endpoint), ("saga_type", sagaType), ("id", sagaId)))
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    return await reader.ReadAsync().ConfigureAwait(false)
                        ? new SagaRecord { Version = reader.GetInt64(0), Data = Bytes(reader, 1) } : null;
            }

            public async Task SaveSaga(string sagaType, string sagaId, byte[] data, long? expectedVersion)
            {
                var rows = expectedVersion.HasValue
                    ? await Execute(Connection, Transaction, @"UPDATE bq_saga SET saga_data = :data, saga_version = saga_version + 1
                        WHERE endpoint = :endpoint AND saga_type = :saga_type AND saga_id = :id AND saga_version = :version",
                        ("data", data), ("endpoint", owner.endpoint), ("saga_type", sagaType), ("id", sagaId),
                        ("version", expectedVersion.Value)).ConfigureAwait(false)
                    : await Execute(Connection, Transaction, @"INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(bq_saga, bq_saga_pk) */
                        INTO bq_saga (endpoint, saga_type, saga_id, saga_version, saga_data) VALUES (:endpoint, :saga_type, :id, 1, :data)",
                        ("endpoint", owner.endpoint), ("saga_type", sagaType), ("id", sagaId), ("data", data)).ConfigureAwait(false);
                if (rows != 1) throw new SagaConcurrencyException("Saga was changed concurrently.");
            }

            public async Task DeleteSaga(string sagaType, string sagaId, long expectedVersion)
            {
                var rows = await Execute(Connection, Transaction, @"DELETE FROM bq_saga
                    WHERE endpoint = :endpoint AND saga_type = :saga_type AND saga_id = :id AND saga_version = :version",
                    ("endpoint", owner.endpoint), ("saga_type", sagaType), ("id", sagaId), ("version", expectedVersion)).ConfigureAwait(false);
                if (rows != 1) throw new SagaConcurrencyException("Saga was changed concurrently.");
            }

            public async Task<bool> HasRecovery(string name, string recoveryId)
            {
                owner.CheckEndpoint(name);
                using (var command = Command(Connection, Transaction, "SELECT 1 FROM bq_recovery WHERE endpoint = :endpoint AND recovery_id = :id",
                    ("endpoint", name), ("id", recoveryId)))
                    return await command.ExecuteScalarAsync().ConfigureAwait(false) != null;
            }

            public async Task StoreRecovery(string name, RecoveryRecord record)
            {
                owner.CheckEndpoint(name);
                await owner.InsertRecovery(Connection, Transaction, record).ConfigureAwait(false);
            }

            public void Commit() => Transaction.Commit();

            public void Dispose()
            {
                try { Transaction.Dispose(); }
                finally { Connection.Dispose(); }
            }
        }
    }
}
