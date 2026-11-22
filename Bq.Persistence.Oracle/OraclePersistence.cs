using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Persistence.Oracle
{
    public sealed partial class OraclePersistence : IPersistenceProvider, IRecoveryPersistence, ILockedDispatchPersistence, IOutgoingFailureTracking, IRetentionPersistence, ILockedBatchDispatchPersistence
    {
        const string Outbox = "bq_outbox";
        const string OutgoingColumns = "id, correlation_id, reply_to, type_name, exchange_name, routing_key, mandatory, body, headers";
        readonly Func<DbConnection> connectionFactory;
        readonly string endpoint;

        // The factory must return a new, closed connection owned by Bq.
        public OraclePersistence(Func<DbConnection> connectionFactory, string endpoint)
        {
            this.connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            this.endpoint = string.IsNullOrWhiteSpace(endpoint)
                ? throw new ArgumentException("Endpoint is required.", nameof(endpoint)) : endpoint;
        }

        async Task<DbConnection> Open()
        {
            var connection = connectionFactory() ?? throw new InvalidOperationException("The Oracle connection factory returned null.");
            try
            {
                await connection.OpenAsync().ConfigureAwait(false);
                return connection;
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }

        public async Task<IPersistenceSession> OpenSession()
        {
            var connection = await Open().ConfigureAwait(false);
            try { return new Session(this, connection, connection.BeginTransaction(IsolationLevel.ReadCommitted)); }
            catch { connection.Dispose(); throw; }
        }

        void CheckEndpoint(string name)
        {
            if (!string.Equals(name, endpoint, StringComparison.Ordinal))
                throw new InvalidOperationException("Persistence endpoint does not match configured endpoint.");
        }

        // Bind in SQL order so both named and positional Oracle binding work.
        static DbCommand Command(DbConnection connection, DbTransaction transaction, string sql,
            params (string Name, object Value)[] parameters)
        {
            if (transaction != null && !ReferenceEquals(transaction.Connection, connection))
                throw new InvalidOperationException("The Oracle command must use its transaction's connection.");

            // Oracle commands participate in the local transaction on their connection.
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = sql;
                foreach (var (name, value) in parameters)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = name;
                    if (value is byte[]) SetLobType(parameter, "Blob");
                    else if (name == "exception_message" || name == "exception_detail") SetLobType(parameter, "NClob");
                    else if (value == null || value is string) parameter.DbType = DbType.String;
                    else if (value is long) parameter.DbType = DbType.Int64;
                    else if (value is int) parameter.DbType = DbType.Int32;
                    parameter.Value = value ?? DBNull.Value;
                    command.Parameters.Add(parameter);
                }
                return command;
            }
            catch
            {
                command.Dispose();
                throw;
            }
        }

        static void SetLobType(DbParameter parameter, string typeName)
        {
            // DbType.Binary can bind as RAW, and DbType.String as VARCHAR2. Both drivers
            // expose the Oracle-specific types through their OracleDbType property.
            var property = parameter.GetType().GetProperty("OracleDbType");
            if (property == null || !property.CanWrite || !property.PropertyType.IsEnum)
                throw new NotSupportedException("The Oracle driver must expose OracleDbType for LOB parameters.");
            property.SetValue(parameter, Enum.Parse(property.PropertyType, typeName, ignoreCase: true));
        }

        static async Task<int> Execute(DbConnection connection, DbTransaction transaction, string sql,
            params (string Name, object Value)[] parameters)
        {
            using (var command = Command(connection, transaction, sql, parameters))
                return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        static string Text(DbDataReader reader, int index) => reader.IsDBNull(index) ? null : reader.GetString(index);
        static byte[] Bytes(DbDataReader reader, int index)
        {
            if (reader.GetValue(index) is byte[] bytes) return bytes;

            var buffer = new byte[checked((int)reader.GetBytes(index, 0, null, 0, 0))];
            var offset = 0;
            while (offset < buffer.Length)
            {
                var count = checked((int)reader.GetBytes(index, offset, buffer, offset, buffer.Length - offset));
                if (count == 0) throw new InvalidOperationException("Oracle returned an incomplete BLOB.");
                offset += count;
            }
            return buffer;
        }

        static OutgoingMessage ReadOutgoing(DbDataReader reader) => new OutgoingMessage
        {
            Id = reader.GetString(0), CorrelationId = Text(reader, 1), ReplyTo = Text(reader, 2),
            Type = reader.GetString(3), Exchange = Text(reader, 4) ?? "", RoutingKey = reader.GetString(5),
            Mandatory = reader.GetInt32(6) != 0, Body = Bytes(reader, 7), Headers = MessageHeaderCodec.Decode(Bytes(reader, 8))
        };

        static string FirstRow(string table, string columns, string filter, string order, string lockFilter)
        {
            if (lockFilter == null)
                return $"SELECT {columns} FROM {table} WHERE {filter} ORDER BY {order} FETCH FIRST 1 ROWS ONLY";

            // Limit the row IDs in a subquery. Oracle 12c–21c reject FETCH FIRST on the
            // same query block as FOR UPDATE. Recheck eligibility on the locked row.
            return $"SELECT {columns} FROM {table} WHERE ROWID = (" +
                $"SELECT chosen_id FROM (SELECT ROWID AS chosen_id FROM {table} WHERE {filter} ORDER BY {order}) WHERE ROWNUM = 1)" +
                $" AND {lockFilter} FOR UPDATE SKIP LOCKED";
        }

        async Task<OutgoingMessage> FindOutgoing(DbConnection connection, DbTransaction transaction)
        {
            const string table = Outbox;
            // A row whose publish was rejected is skipped until its retry time, so it cannot hold back the rows behind it.
            const string filter = "endpoint = :endpoint AND retry_after_ticks <= :retry_now";
            const string order = "enqueue_order";
            var now = DateTime.UtcNow.Ticks;
            var args = new List<(string, object)> { ("endpoint", endpoint) };
            args.Add(("retry_now", now));
            var lockFilter = transaction == null ? null : "endpoint = :locked_endpoint AND retry_after_ticks <= :locked_retry_now";
            if (lockFilter != null) { args.Add(("locked_endpoint", endpoint)); args.Add(("locked_retry_now", now)); }
            var sql = FirstRow(table, OutgoingColumns, filter, order, lockFilter);
            using (var command = Command(connection, transaction, sql, args.ToArray()))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                return await reader.ReadAsync().ConfigureAwait(false) ? ReadOutgoing(reader) : null;
        }

        public async Task<OutgoingMessage> NextOutgoing(string name)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
                return await FindOutgoing(connection, null).ConfigureAwait(false);
        }

        Task<int> DeleteOutgoing(DbConnection connection, DbTransaction transaction, string table, string id) =>
            Execute(connection, transaction, $"DELETE FROM {table} WHERE endpoint = :endpoint AND id = :id", ("endpoint", endpoint), ("id", id));

        public async Task MarkDispatched(string name, string messageId)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                await DeleteOutgoing(connection, transaction, Outbox, messageId).ConfigureAwait(false);
                transaction.Commit();
            }
        }

        public async Task<bool> DispatchOutgoing(string name, Action<OutgoingMessage> publish)
        {
            CheckEndpoint(name);
            if (publish == null) throw new ArgumentNullException(nameof(publish));
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var message = await FindOutgoing(connection, transaction).ConfigureAwait(false);
                if (message == null) return false;
                try { publish(message); }
                catch (OutgoingMessageRejectedException rejected)
                {
                    // The failure belongs to this row. Record it while the row is still locked and report progress, so the
                    // dispatcher moves on to the rows behind it.
                    await RecordFailure(connection, transaction, Outbox, message.Id, rejected).ConfigureAwait(false);
                    transaction.Commit();
                    return true;
                }
                await DeleteOutgoing(connection, transaction, Outbox, message.Id).ConfigureAwait(false);
                transaction.Commit();
                return true;
            }
        }

        // Up to maxCount ready rows of one table, locked for this transaction, in dispatch order. A row another dispatcher has locked is skipped.
        async Task<List<OutgoingMessage>> FindOutgoingBatch(DbConnection connection, DbTransaction transaction, int maxCount)
        {
            const string table = Outbox;
            const string filter = "endpoint = :endpoint AND retry_after_ticks <= :retry_now";
            const string order = "enqueue_order";
            var now = DateTime.UtcNow.Ticks;
            var args = new List<(string, object)> { ("endpoint", endpoint) };
            args.Add(("retry_now", now));
            args.Add(("max", maxCount));
            args.Add(("locked_endpoint", endpoint));
            args.Add(("locked_retry_now", now));
            // Choose the row IDs in a subquery and lock them in the outer query, since Oracle 12c-21c reject FETCH FIRST beside FOR UPDATE.
            var sql = $"SELECT {OutgoingColumns} FROM {table} WHERE ROWID IN (" +
                $"SELECT chosen_id FROM (SELECT ROWID AS chosen_id FROM {table} WHERE {filter} ORDER BY {order}) WHERE ROWNUM <= :max)" +
                $" AND endpoint = :locked_endpoint AND retry_after_ticks <= :locked_retry_now ORDER BY {order} FOR UPDATE SKIP LOCKED";
            var rows = new List<OutgoingMessage>();
            using (var command = Command(connection, transaction, sql, args.ToArray()))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                while (await reader.ReadAsync().ConfigureAwait(false)) rows.Add(ReadOutgoing(reader));
            return rows;
        }

        public async Task<int> DispatchOutgoingBatch(string name, int maxCount, Func<IReadOnlyList<OutgoingMessage>, IReadOnlyCollection<string>> publish)
        {
            CheckEndpoint(name);
            if (maxCount < 1) throw new ArgumentOutOfRangeException(nameof(maxCount));
            if (publish == null) throw new ArgumentNullException(nameof(publish));
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var rows = await FindOutgoingBatch(connection, transaction, maxCount).ConfigureAwait(false);
                if (rows.Count == 0) return 0;
                var rejected = new HashSet<string>(publish(rows) ?? new string[0], StringComparer.Ordinal);
                foreach (var id in rejected)
                    await RecordFailure(connection, transaction, Outbox, id, new OutgoingMessageRejectedException(id, "Message was unroutable.")).ConfigureAwait(false);
                var sent = rows.Select(row => row.Id).Where(id => !rejected.Contains(id)).ToList();
                for (var offset = 0; offset < sent.Count; offset += 100)
                {
                    var chunk = sent.Skip(offset).Take(100).ToList();
                    var args = new List<(string, object)> { ("endpoint", endpoint) };
                    for (var i = 0; i < chunk.Count; i++) args.Add(("id" + i, chunk[i]));
                    await Execute(connection, transaction,
                        $"DELETE FROM {Outbox} WHERE endpoint = :endpoint AND id IN (" + string.Join(", ", chunk.Select((id, i) => ":id" + i)) + ")",
                        args.ToArray()).ConfigureAwait(false);
                }
                transaction.Commit();
                return rows.Count;
            }
        }

        // The delay doubles with each rejection, as OutgoingRetry.Delay does: 100 ms times 2^attempts with the old attempt count, up to 30 seconds.
        Task<int> RecordFailure(DbConnection connection, DbTransaction transaction, string table, string id, Exception failure) =>
            Execute(connection, transaction,
                $"UPDATE {table} SET attempts = attempts + 1, " +
                "retry_after_ticks = :now + LEAST(:max, POWER(2, LEAST(attempts, 10)) * :base), last_error = :error " +
                "WHERE endpoint = :endpoint AND id = :id",
                ("now", DateTime.UtcNow.Ticks), ("max", OutgoingRetry.MaxDelay.Ticks), ("base", OutgoingRetry.BaseDelay.Ticks), ("error", OutgoingRetry.Describe(failure)),
                ("endpoint", endpoint), ("id", id));

        public async Task<int> PurgeInbox(string name, DateTime claimedBeforeUtc, int maxRows)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var removed = await Execute(connection, transaction,
                    "DELETE FROM bq_inbox WHERE endpoint = :endpoint AND claimed_ticks < :cutoff AND ROWNUM <= :max",
                    ("endpoint", endpoint), ("cutoff", claimedBeforeUtc.Ticks), ("max", maxRows)).ConfigureAwait(false);
                transaction.Commit();
                return removed;
            }
        }

        public async Task<int> PurgeRecovery(string name, DateTime failedBeforeUtc, int maxRows)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var removed = await Execute(connection, transaction,
                    "DELETE FROM bq_recovery WHERE endpoint = :endpoint AND dispatched = 1 AND failed_ticks < :cutoff " +
                    "AND (is_error = 0 OR replayed = 1) AND ROWNUM <= :max",
                    ("endpoint", endpoint), ("cutoff", failedBeforeUtc.Ticks), ("max", maxRows)).ConfigureAwait(false);
                transaction.Commit();
                return removed;
            }
        }

        public async Task RecordOutgoingFailure(string name, string messageId, Exception failure)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                await RecordFailure(connection, transaction, Outbox, messageId, failure).ConfigureAwait(false);
                transaction.Commit();
            }
        }
    }
}
