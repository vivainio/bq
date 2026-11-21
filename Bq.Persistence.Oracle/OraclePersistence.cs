using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Persistence.Oracle
{
    public sealed partial class OraclePersistence : IPersistenceProvider, IRecoveryPersistence, ILockedDispatchPersistence, IOutgoingFailureTracking
    {
        const string Outbox = "bq_outbox";
        const string Scheduled = "bq_saga_timeout";
        const string OutgoingColumns = "id, correlation_id, reply_to, type_name, exchange_name, routing_key, mandatory, body, headers";
        readonly Func<DbConnection> connectionFactory;
        readonly string endpoint;
        int outgoingPoll;

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

        async Task<OutgoingMessage> FindOutgoing(DbConnection connection, DbTransaction transaction, string table)
        {
            var scheduled = table == Scheduled;
            // A row whose publish was rejected is skipped until its retry time, so it cannot hold back the rows behind it.
            var filter = scheduled ? "endpoint = :endpoint AND due_ticks <= :now AND retry_after_ticks <= :retry_now"
                : "endpoint = :endpoint AND retry_after_ticks <= :retry_now";
            var order = scheduled ? "due_ticks, id" : "enqueue_order";
            var now = DateTime.UtcNow.Ticks;
            var args = new List<(string, object)> { ("endpoint", endpoint) };
            if (scheduled) args.Add(("now", now));
            args.Add(("retry_now", now));
            var lockFilter = transaction == null ? null : "endpoint = :locked_endpoint AND retry_after_ticks <= :locked_retry_now";
            if (lockFilter != null) { args.Add(("locked_endpoint", endpoint)); args.Add(("locked_retry_now", now)); }
            var sql = FirstRow(table, OutgoingColumns, filter, order, lockFilter);
            using (var command = Command(connection, transaction, sql, args.ToArray()))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                return await reader.ReadAsync().ConfigureAwait(false) ? ReadOutgoing(reader) : null;
        }

        async Task<(OutgoingMessage Message, string Table)> SelectOutgoing(DbConnection connection, DbTransaction transaction)
        {
            var scheduledFirst = (Interlocked.Increment(ref outgoingPoll) & 1) != 0;
            var first = scheduledFirst ? Scheduled : Outbox;
            var second = scheduledFirst ? Outbox : Scheduled;
            var message = await FindOutgoing(connection, transaction, first).ConfigureAwait(false);
            return message != null ? (message, first) : (await FindOutgoing(connection, transaction, second).ConfigureAwait(false), second);
        }

        public async Task<OutgoingMessage> NextOutgoing(string name)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
                return (await SelectOutgoing(connection, null).ConfigureAwait(false)).Message;
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
                await DeleteOutgoing(connection, transaction, Scheduled, messageId).ConfigureAwait(false);
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
                var (message, table) = await SelectOutgoing(connection, transaction).ConfigureAwait(false);
                if (message == null) return false;
                try { publish(message); }
                catch (OutgoingMessageRejectedException rejected)
                {
                    // The failure belongs to this row. Record it while the row is still locked and report progress, so the
                    // dispatcher moves on to the rows behind it.
                    await RecordFailure(connection, transaction, table, message.Id, rejected).ConfigureAwait(false);
                    transaction.Commit();
                    return true;
                }
                await DeleteOutgoing(connection, transaction, table, message.Id).ConfigureAwait(false);
                transaction.Commit();
                return true;
            }
        }

        // The delay doubles with each rejection, as OutgoingRetry.Delay does: 2^attempts seconds with the old attempt count, up to a minute.
        Task<int> RecordFailure(DbConnection connection, DbTransaction transaction, string table, string id, Exception failure) =>
            Execute(connection, transaction,
                $"UPDATE {table} SET attempts = attempts + 1, " +
                "retry_after_ticks = :now + LEAST(:max, POWER(2, LEAST(attempts, 10)) * 10000000), last_error = :error " +
                "WHERE endpoint = :endpoint AND id = :id",
                ("now", DateTime.UtcNow.Ticks), ("max", OutgoingRetry.MaxDelay.Ticks), ("error", OutgoingRetry.Describe(failure)),
                ("endpoint", endpoint), ("id", id));

        public async Task RecordOutgoingFailure(string name, string messageId, Exception failure)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                if (await RecordFailure(connection, transaction, Outbox, messageId, failure).ConfigureAwait(false) == 0)
                    await RecordFailure(connection, transaction, Scheduled, messageId, failure).ConfigureAwait(false);
                transaction.Commit();
            }
        }
    }
}
