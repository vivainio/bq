using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Persistence.Sqlite
{
    // Accepts an ADO.NET SQLite connection factory; the host selects the driver.
    public sealed class SqlitePersistence : IPersistenceProvider
    {
        readonly Func<DbConnection> factory;
        readonly string endpoint;
        public SqlitePersistence(Func<DbConnection> factory, string endpoint)
        {
            this.factory = factory ?? throw new ArgumentNullException(nameof(factory));
            this.endpoint = !string.IsNullOrWhiteSpace(endpoint) ? endpoint : throw new ArgumentException("Endpoint is required.", nameof(endpoint));
        }
        async Task<DbConnection> Open()
        {
            var connection = factory();
            try { await connection.OpenAsync().ConfigureAwait(false); return connection; }
            catch { connection.Dispose(); throw; }
        }
        public async Task Initialize()
        {
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = c.CreateCommand())
            {
                cmd.CommandText = @"CREATE TABLE IF NOT EXISTS bq_inbox (endpoint TEXT NOT NULL, message_id TEXT NOT NULL, PRIMARY KEY(endpoint,message_id));
CREATE TABLE IF NOT EXISTS bq_outbox (endpoint TEXT NOT NULL, id TEXT NOT NULL, correlation_id TEXT, reply_to TEXT, type TEXT NOT NULL, exchange_name TEXT NOT NULL, routing_key TEXT NOT NULL, mandatory INTEGER NOT NULL, body BLOB NOT NULL, PRIMARY KEY(endpoint,id));
CREATE TABLE IF NOT EXISTS bq_saga_timeout (endpoint TEXT NOT NULL, id TEXT NOT NULL, correlation_id TEXT, reply_to TEXT, type TEXT NOT NULL, exchange_name TEXT NOT NULL, routing_key TEXT NOT NULL, mandatory INTEGER NOT NULL, body BLOB NOT NULL, due_ticks INTEGER NOT NULL, PRIMARY KEY(endpoint,id));
CREATE INDEX IF NOT EXISTS bq_saga_timeout_due ON bq_saga_timeout(endpoint,due_ticks);
CREATE TABLE IF NOT EXISTS bq_saga (endpoint TEXT NOT NULL, saga_type TEXT NOT NULL, saga_id TEXT NOT NULL, version INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(endpoint,saga_type,saga_id));";
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        public async Task<IPersistenceSession> OpenSession()
        {
            var c = await Open().ConfigureAwait(false);
            try { return new Session(c, c.BeginTransaction(IsolationLevel.Serializable), endpoint); }
            catch { c.Dispose(); throw; }
        }
        static DbCommand Command(DbConnection c, DbTransaction tx, string sql, params object[] parameters)
        {
            var cmd = c.CreateCommand(); cmd.Transaction = tx; cmd.CommandText = sql;
            for (int i = 0; i < parameters.Length; i += 2)
            {
                var p = cmd.CreateParameter(); p.ParameterName = (string)parameters[i]; p.Value = parameters[i + 1] ?? DBNull.Value; cmd.Parameters.Add(p);
            }
            return cmd;
        }
        void CheckEndpoint(string name) { if (name != endpoint) throw new InvalidOperationException("Persistence endpoint does not match configured endpoint."); }
        public async Task<OutgoingMessage> NextOutgoing(string name)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "SELECT id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body FROM bq_saga_timeout WHERE endpoint=@e AND due_ticks<=@now UNION ALL SELECT id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body FROM bq_outbox WHERE endpoint=@e LIMIT 1", "@e", endpoint, "@now", DateTime.UtcNow.Ticks))
            using (var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (!await r.ReadAsync().ConfigureAwait(false)) return null;
                return new OutgoingMessage { Id = r.GetString(0), CorrelationId = r.IsDBNull(1) ? null : r.GetString(1), ReplyTo = r.IsDBNull(2) ? null : r.GetString(2), Type = r.GetString(3), Exchange = r.GetString(4), RoutingKey = r.GetString(5), Mandatory = Convert.ToInt32(r.GetValue(6)) != 0, Body = (byte[])r.GetValue(7) };
            }
        }
        public async Task MarkDispatched(string name, string id)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "DELETE FROM bq_outbox WHERE endpoint=@e AND id=@id; DELETE FROM bq_saga_timeout WHERE endpoint=@e AND id=@id", "@e", endpoint, "@id", id)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        sealed class Session : IPersistenceSession
        {
            readonly string endpoint;
            public Session(DbConnection c, DbTransaction tx, string endpoint) { Connection = c; Transaction = tx; this.endpoint = endpoint; }
            public DbConnection Connection { get; }
            public DbTransaction Transaction { get; }
            public async Task<bool> TryClaimMessage(string name, string id)
            {
                if (name != endpoint) throw new InvalidOperationException("Persistence endpoint mismatch.");
                if (string.IsNullOrEmpty(id)) throw new InvalidOperationException("A message ID is required for deduplication.");
                using (var cmd = Command(Connection, Transaction, "INSERT OR IGNORE INTO bq_inbox(endpoint,message_id) VALUES(@e,@id)", "@e", endpoint, "@id", id)) return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false) == 1;
            }
            public async Task StoreOutgoing(OutgoingMessage m)
            {
                using (var cmd = Command(Connection, Transaction, "INSERT INTO bq_outbox(endpoint,id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body) VALUES(@e,@id,@c,@r,@t,@x,@k,@m,@b)", "@e", endpoint, "@id", m.Id, "@c", m.CorrelationId, "@r", m.ReplyTo, "@t", m.Type, "@x", m.Exchange, "@k", m.RoutingKey, "@m", m.Mandatory ? 1 : 0, "@b", m.Body)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            public async Task StoreTimeout(OutgoingMessage m, DateTime dueUtc)
            {
                if (dueUtc.Kind != DateTimeKind.Utc) throw new ArgumentException("Timeout must use UTC.", nameof(dueUtc));
                using (var cmd = Command(Connection, Transaction, "INSERT INTO bq_saga_timeout(endpoint,id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body,due_ticks) VALUES(@e,@id,@c,@r,@t,@x,@k,@m,@b,@due)", "@e", endpoint, "@id", m.Id, "@c", m.CorrelationId, "@r", m.ReplyTo, "@t", m.Type, "@x", m.Exchange, "@k", m.RoutingKey, "@m", m.Mandatory ? 1 : 0, "@b", m.Body, "@due", dueUtc.Ticks)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            public async Task<SagaRecord> LoadSaga(string type, string id)
            {
                using (var cmd = Command(Connection, Transaction, "SELECT version,data FROM bq_saga WHERE endpoint=@e AND saga_type=@t AND saga_id=@id", "@e", endpoint, "@t", type, "@id", id))
                using (var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                { if (!await r.ReadAsync().ConfigureAwait(false)) return null; return new SagaRecord { Version = Convert.ToInt64(r.GetValue(0)), Data = (byte[])r.GetValue(1) }; }
            }
            public async Task SaveSaga(string type, string id, byte[] data, long? version)
            {
                var sql = version.HasValue ? "UPDATE bq_saga SET data=@data,version=version+1 WHERE endpoint=@e AND saga_type=@t AND saga_id=@id AND version=@v" : "INSERT OR IGNORE INTO bq_saga(endpoint,saga_type,saga_id,version,data) VALUES(@e,@t,@id,1,@data)";
                using (var cmd = Command(Connection, Transaction, sql, "@e", endpoint, "@t", type, "@id", id, "@data", data, "@v", version))
                    if (await cmd.ExecuteNonQueryAsync().ConfigureAwait(false) != 1) throw new SagaConcurrencyException("Saga was changed concurrently.");
            }
            public async Task DeleteSaga(string type, string id, long version)
            {
                using (var cmd = Command(Connection, Transaction, "DELETE FROM bq_saga WHERE endpoint=@e AND saga_type=@t AND saga_id=@id AND version=@v", "@e", endpoint, "@t", type, "@id", id, "@v", version))
                    if (await cmd.ExecuteNonQueryAsync().ConfigureAwait(false) != 1) throw new SagaConcurrencyException("Saga was changed concurrently.");
            }
            public void Commit() => Transaction.Commit();
            public void Dispose() { Transaction.Dispose(); Connection.Dispose(); }
        }
    }
}
