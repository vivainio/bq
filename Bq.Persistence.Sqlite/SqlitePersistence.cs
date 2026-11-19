using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Persistence.Sqlite
{
    // Accepts an ADO.NET SQLite connection factory; the host selects the driver.
    public sealed class SqlitePersistence : IPersistenceProvider, IRecoveryPersistence
    {
        readonly Func<DbConnection> factory;
        readonly string endpoint;
        int outgoingPoll;
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
CREATE TABLE IF NOT EXISTS bq_outbox (endpoint TEXT NOT NULL, id TEXT NOT NULL, correlation_id TEXT, reply_to TEXT, type TEXT NOT NULL, exchange_name TEXT NOT NULL, routing_key TEXT NOT NULL, mandatory INTEGER NOT NULL, body BLOB NOT NULL, headers BLOB, PRIMARY KEY(endpoint,id));
CREATE TABLE IF NOT EXISTS bq_saga_timeout (endpoint TEXT NOT NULL, id TEXT NOT NULL, correlation_id TEXT, reply_to TEXT, type TEXT NOT NULL, exchange_name TEXT NOT NULL, routing_key TEXT NOT NULL, mandatory INTEGER NOT NULL, body BLOB NOT NULL, due_ticks INTEGER NOT NULL, headers BLOB, PRIMARY KEY(endpoint,id));
CREATE INDEX IF NOT EXISTS bq_saga_timeout_due ON bq_saga_timeout(endpoint,due_ticks);
CREATE TABLE IF NOT EXISTS bq_recovery (endpoint TEXT NOT NULL, recovery_id TEXT NOT NULL, message_id TEXT NOT NULL, cycle INTEGER NOT NULL, is_error INTEGER NOT NULL, error_queue TEXT, exception_type TEXT, exception_message TEXT, exception_detail TEXT, failed_ticks INTEGER NOT NULL, due_ticks INTEGER NOT NULL, payload BLOB NOT NULL, dispatched INTEGER NOT NULL DEFAULT 0, replayed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(endpoint,recovery_id));
CREATE INDEX IF NOT EXISTS bq_recovery_errors ON bq_recovery(endpoint,is_error,replayed,failed_ticks DESC,recovery_id DESC);
CREATE INDEX IF NOT EXISTS bq_recovery_due ON bq_recovery(endpoint,dispatched,due_ticks);
CREATE TABLE IF NOT EXISTS bq_saga (endpoint TEXT NOT NULL, saga_type TEXT NOT NULL, saga_id TEXT NOT NULL, version INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(endpoint,saga_type,saga_id));";
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                EnsureColumn(c, "bq_outbox", "headers");
                EnsureColumn(c, "bq_saga_timeout", "headers");
            }
        }
        static void EnsureColumn(DbConnection connection, string table, string column)
        {
            var found = false;
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "PRAGMA table_info(" + table + ")";
                using (var reader = command.ExecuteReader())
                    while (reader.Read())
                        if (string.Equals(reader.GetString(1), column, StringComparison.OrdinalIgnoreCase)) { found = true; break; }
            }
            if (!found)
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "ALTER TABLE " + table + " ADD COLUMN " + column + " BLOB";
                    command.ExecuteNonQuery();
                }
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
        public async Task<DispatchBacklog> GetDispatchBacklog()
        {
            using (var connection = await Open().ConfigureAwait(false))
            using (var command = Command(connection, null, @"SELECT
    (SELECT COUNT(*) FROM bq_outbox WHERE endpoint=@e),
    (SELECT COUNT(*) FROM bq_saga_timeout WHERE endpoint=@e AND due_ticks<=@now),
    (SELECT COUNT(*) FROM bq_saga_timeout WHERE endpoint=@e AND due_ticks>@now),
    (SELECT COUNT(*) FROM bq_recovery WHERE endpoint=@e AND dispatched=0 AND due_ticks<=@now),
    (SELECT COUNT(*) FROM bq_recovery WHERE endpoint=@e AND dispatched=0 AND due_ticks>@now)",
                "@e", endpoint, "@now", DateTime.UtcNow.Ticks))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                await reader.ReadAsync().ConfigureAwait(false);
                return new DispatchBacklog(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2),
                    reader.GetInt64(3), reader.GetInt64(4));
            }
        }

        public Task<System.Collections.Generic.IReadOnlyList<PendingDispatch>> ListPendingDispatches(int limit = 100) =>
            ListPendingDispatches(null, limit);

        public Task<System.Collections.Generic.IReadOnlyList<PendingDispatch>> ListPendingDispatches(DispatchKind? kind, int limit = 100) =>
            ListDispatches(kind, limit, null);

        public Task<System.Collections.Generic.IReadOnlyList<PendingDispatch>> ListReadyDispatches(DispatchKind? kind = null, int limit = 100) =>
            ListDispatches(kind, limit, DateTime.UtcNow.Ticks);

        async Task<System.Collections.Generic.IReadOnlyList<PendingDispatch>> ListDispatches(DispatchKind? kind, int limit, long? dueBy)
        {
            if (kind.HasValue && !Enum.IsDefined(typeof(DispatchKind), kind.Value))
                throw new ArgumentOutOfRangeException(nameof(kind));
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            var result = new System.Collections.Generic.List<PendingDispatch>();
            using (var connection = await Open().ConfigureAwait(false))
            using (var command = Command(connection, null, @"SELECT id, kind, destination, due_ticks, exchange_name FROM (
    SELECT id, 'Outbox' AS kind, routing_key AS destination, NULL AS due_ticks, exchange_name FROM bq_outbox WHERE endpoint=@e
    UNION ALL
    SELECT id, CASE WHEN type=@saga THEN 'SagaTimeout' ELSE 'ScheduledSend' END, routing_key, due_ticks, exchange_name FROM bq_saga_timeout WHERE endpoint=@e
    UNION ALL
    SELECT message_id, CASE WHEN is_error=1 THEN 'Error' ELSE 'Recovery' END,
        CASE WHEN is_error=1 THEN error_queue ELSE endpoint END, due_ticks, ''
        FROM bq_recovery WHERE endpoint=@e AND dispatched=0
) WHERE (@kind IS NULL OR kind=@kind) AND (@dueBy IS NULL OR due_ticks IS NULL OR due_ticks<=@dueBy)
ORDER BY COALESCE(due_ticks, 0), kind, id LIMIT @limit",
                "@e", endpoint, "@limit", limit, "@kind", kind?.ToString(), "@dueBy", dueBy, "@saga", typeof(SagaTimeoutMessage).FullName))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var due = reader.IsDBNull(3) ? (DateTime?)null : new DateTime(reader.GetInt64(3), DateTimeKind.Utc);
                    result.Add(new PendingDispatch(reader.GetString(0),
                        (DispatchKind)Enum.Parse(typeof(DispatchKind), reader.GetString(1)),
                        reader.GetString(4), reader.IsDBNull(2) ? null : reader.GetString(2), due));
                }
            }
            return result;
        }

        public async Task<OutgoingMessage> NextOutgoing(string name)
        {
            CheckEndpoint(name);
            // Alternate preference so a sustained backlog or an unroutable head in one
            // category does not prevent attempts in the other category.
            var preferTimeout = System.Threading.Interlocked.Increment(ref outgoingPoll) & 1;
            const string fields = "id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body,headers";
            var sql = "SELECT " + fields + @" FROM (
    SELECT * FROM (SELECT " + fields + @", 1 AS category FROM bq_saga_timeout
        WHERE endpoint=@e AND due_ticks<=@now ORDER BY due_ticks,id LIMIT 1)
    UNION ALL
    SELECT * FROM (SELECT " + fields + @", 0 AS category FROM bq_outbox
        WHERE endpoint=@e ORDER BY rowid LIMIT 1)
) ORDER BY (category=@preferred) DESC LIMIT 1";
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, sql, "@e", endpoint, "@now", DateTime.UtcNow.Ticks,
                "@preferred", preferTimeout))
            using (var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (!await r.ReadAsync().ConfigureAwait(false)) return null;
                return new OutgoingMessage { Id = r.GetString(0), CorrelationId = r.IsDBNull(1) ? null : r.GetString(1), ReplyTo = r.IsDBNull(2) ? null : r.GetString(2), Type = r.GetString(3), Exchange = r.GetString(4), RoutingKey = r.GetString(5), Mandatory = Convert.ToInt32(r.GetValue(6)) != 0, Body = (byte[])r.GetValue(7), Headers = MessageHeaderCodec.Decode(r.IsDBNull(8) ? null : (byte[])r.GetValue(8)) };
            }
        }
        public async Task MarkDispatched(string name, string id)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "DELETE FROM bq_outbox WHERE endpoint=@e AND id=@id; DELETE FROM bq_saga_timeout WHERE endpoint=@e AND id=@id", "@e", endpoint, "@id", id)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        static RecoveryRecord ReadRecovery(DbDataReader r) => new RecoveryRecord
        {
            RecoveryId = r.GetString(0), MessageId = r.GetString(1), Cycle = Convert.ToInt32(r.GetValue(2)), IsError = Convert.ToInt32(r.GetValue(3)) != 0,
            ErrorQueue = r.IsDBNull(4) ? null : r.GetString(4), ExceptionType = r.IsDBNull(5) ? null : r.GetString(5),
            ExceptionMessage = r.IsDBNull(6) ? null : r.GetString(6), ExceptionDetail = r.IsDBNull(7) ? null : r.GetString(7),
            FailedAtUtc = new DateTime(Convert.ToInt64(r.GetValue(8)), DateTimeKind.Utc), DueAtUtc = new DateTime(Convert.ToInt64(r.GetValue(9)), DateTimeKind.Utc), Payload = (byte[])r.GetValue(10),
            Dispatched = Convert.ToInt32(r.GetValue(11)) != 0
        };
        const string RecoveryColumns = "recovery_id,message_id,cycle,is_error,error_queue,exception_type,exception_message,exception_detail,failed_ticks,due_ticks,payload,dispatched";
        static DbCommand InsertRecovery(DbConnection c, DbTransaction tx, string endpoint, RecoveryRecord r) => Command(c, tx,
            "INSERT OR IGNORE INTO bq_recovery(endpoint,recovery_id,message_id,cycle,is_error,error_queue,exception_type,exception_message,exception_detail,failed_ticks,due_ticks,payload) VALUES(@e,@r,@id,@cycle,@error,@queue,@type,@message,@detail,@failed,@due,@payload)",
            "@e", endpoint, "@r", r.RecoveryId, "@id", r.MessageId, "@cycle", r.Cycle, "@error", r.IsError ? 1 : 0, "@queue", r.ErrorQueue,
            "@type", r.ExceptionType, "@message", r.ExceptionMessage, "@detail", r.ExceptionDetail, "@failed", r.FailedAtUtc.Ticks, "@due", r.DueAtUtc.Ticks, "@payload", r.Payload);
        public async Task<RecoveryRecord> NextRecovery(string name)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "SELECT " + RecoveryColumns + " FROM bq_recovery WHERE endpoint=@e AND dispatched=0 AND due_ticks<=@now ORDER BY due_ticks,recovery_id LIMIT 1", "@e", endpoint, "@now", DateTime.UtcNow.Ticks))
            using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                return await reader.ReadAsync().ConfigureAwait(false) ? ReadRecovery(reader) : null;
        }
        public async Task MarkRecoveryDispatched(string name, string id)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "UPDATE bq_recovery SET dispatched=1 WHERE endpoint=@e AND recovery_id=@id", "@e", endpoint, "@id", id)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        public async Task<RecoveryRecord> GetError(string name, string id)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "SELECT " + RecoveryColumns + " FROM bq_recovery WHERE endpoint=@e AND recovery_id=@id AND is_error=1 AND replayed=0", "@e", endpoint, "@id", id))
            using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                return await reader.ReadAsync().ConfigureAwait(false) ? ReadRecovery(reader) : null;
        }
        public Task<System.Collections.Generic.IReadOnlyList<RecoveryRecord>> ListErrors(string name, int limit) =>
            ListErrors(name, limit, null);

        // Pass the last record from the previous page to continue the scan.
        public async Task<System.Collections.Generic.IReadOnlyList<RecoveryRecord>> ListErrors(string name, int limit, RecoveryRecord after)
        {
            CheckEndpoint(name);
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            var result = new System.Collections.Generic.List<RecoveryRecord>();
            using (var c = await Open().ConfigureAwait(false))
            using (var cmd = Command(c, null, "SELECT " + RecoveryColumns + " FROM bq_recovery WHERE endpoint=@e AND is_error=1 AND replayed=0 " +
                "AND (@ticks IS NULL OR failed_ticks<@ticks OR (failed_ticks=@ticks AND recovery_id<@after)) " +
                "ORDER BY failed_ticks DESC,recovery_id DESC LIMIT @limit",
                "@e", endpoint, "@limit", limit, "@ticks", after?.FailedAtUtc.Ticks, "@after", after?.RecoveryId))
            using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                while (await reader.ReadAsync().ConfigureAwait(false)) result.Add(ReadRecovery(reader));
            return result;
        }
        public async Task<bool> ReplayError(string name, string id, RecoveryRecord replay)
        {
            CheckEndpoint(name);
            using (var c = await Open().ConfigureAwait(false))
            using (var tx = c.BeginTransaction(IsolationLevel.Serializable))
            {
                using (var cmd = Command(c, tx, "UPDATE bq_recovery SET replayed=1 WHERE endpoint=@e AND recovery_id=@id AND is_error=1 AND replayed=0", "@e", endpoint, "@id", id))
                    if (await cmd.ExecuteNonQueryAsync().ConfigureAwait(false) != 1) return false;
                using (var cmd = InsertRecovery(c, tx, endpoint, replay)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                tx.Commit();
                return true;
            }
        }
        sealed class Session : IPersistenceSession, IRecoverySession
        {
            readonly string endpoint;
            public Session(DbConnection c, DbTransaction tx, string endpoint) { Connection = c; Transaction = tx; this.endpoint = endpoint; }
            public DbConnection Connection { get; }
            public DbTransaction Transaction { get; }
            public async Task<bool> HasRecovery(string name, string id)
            {
                if (name != endpoint) throw new InvalidOperationException("Persistence endpoint mismatch.");
                using (var cmd = Command(Connection, Transaction, "SELECT COUNT(*) FROM bq_recovery WHERE endpoint=@e AND recovery_id=@id", "@e", endpoint, "@id", id))
                    return Convert.ToInt32(await cmd.ExecuteScalarAsync().ConfigureAwait(false)) != 0;
            }
            public async Task StoreRecovery(string name, RecoveryRecord record)
            {
                if (name != endpoint) throw new InvalidOperationException("Persistence endpoint mismatch.");
                using (var cmd = InsertRecovery(Connection, Transaction, endpoint, record)) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            public async Task<bool> TryClaimMessage(string name, string id)
            {
                if (name != endpoint) throw new InvalidOperationException("Persistence endpoint mismatch.");
                if (string.IsNullOrEmpty(id)) throw new InvalidOperationException("A message ID is required for deduplication.");
                using (var cmd = Command(Connection, Transaction, "INSERT OR IGNORE INTO bq_inbox(endpoint,message_id) VALUES(@e,@id)", "@e", endpoint, "@id", id)) return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false) == 1;
            }
            public async Task StoreOutgoing(OutgoingMessage m)
            {
                using (var cmd = Command(Connection, Transaction, "INSERT INTO bq_outbox(endpoint,id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body,headers) VALUES(@e,@id,@c,@r,@t,@x,@k,@m,@b,@h)", "@e", endpoint, "@id", m.Id, "@c", m.CorrelationId, "@r", m.ReplyTo, "@t", m.Type, "@x", m.Exchange, "@k", m.RoutingKey, "@m", m.Mandatory ? 1 : 0, "@b", m.Body, "@h", MessageHeaderCodec.Encode(m.Headers))) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            public async Task StoreTimeout(OutgoingMessage m, DateTime dueUtc)
            {
                if (dueUtc.Kind != DateTimeKind.Utc) throw new ArgumentException("Timeout must use UTC.", nameof(dueUtc));
                using (var cmd = Command(Connection, Transaction, "INSERT INTO bq_saga_timeout(endpoint,id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body,due_ticks,headers) VALUES(@e,@id,@c,@r,@t,@x,@k,@m,@b,@due,@h)", "@e", endpoint, "@id", m.Id, "@c", m.CorrelationId, "@r", m.ReplyTo, "@t", m.Type, "@x", m.Exchange, "@k", m.RoutingKey, "@m", m.Mandatory ? 1 : 0, "@b", m.Body, "@due", dueUtc.Ticks, "@h", MessageHeaderCodec.Encode(m.Headers))) await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
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
