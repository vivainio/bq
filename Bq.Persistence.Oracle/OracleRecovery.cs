using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Persistence.Oracle
{
    public sealed partial class OraclePersistence
    {
        const string RecoveryTable = "bq_recovery";
        const string RecoveryColumns = "recovery_id, message_id, retry_cycle, is_error, error_queue, exception_type, " +
            "exception_message, exception_detail, failed_ticks, due_ticks, payload, dispatched";
        const string ErrorFilter = "endpoint = :endpoint AND is_error = 1 AND replayed = 0";

        static RecoveryRecord ReadRecovery(DbDataReader reader) => new RecoveryRecord
        {
            RecoveryId = reader.GetString(0), MessageId = reader.GetString(1), Cycle = reader.GetInt32(2),
            IsError = reader.GetInt32(3) != 0, ErrorQueue = Text(reader, 4), ExceptionType = Text(reader, 5),
            ExceptionMessage = Text(reader, 6), ExceptionDetail = Text(reader, 7),
            FailedAtUtc = new DateTime(reader.GetInt64(8), DateTimeKind.Utc),
            DueAtUtc = new DateTime(reader.GetInt64(9), DateTimeKind.Utc),
            Payload = Bytes(reader, 10), Dispatched = reader.GetInt32(11) != 0
        };

        Task<int> InsertRecovery(DbConnection connection, DbTransaction transaction, RecoveryRecord record) =>
            Execute(connection, transaction, @"INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(bq_recovery, bq_recovery_pk) */
                INTO bq_recovery (endpoint, recovery_id, message_id, retry_cycle, is_error, error_queue, exception_type,
                    exception_message, exception_detail, failed_ticks, due_ticks, payload)
                VALUES (:endpoint, :recovery_id, :message_id, :cycle, :is_error, :error_queue, :exception_type,
                    :exception_message, :exception_detail, :failed_ticks, :due_ticks, :payload)",
                ("endpoint", endpoint), ("recovery_id", record.RecoveryId), ("message_id", record.MessageId),
                ("cycle", record.Cycle), ("is_error", record.IsError ? 1 : 0), ("error_queue", record.ErrorQueue),
                ("exception_type", record.ExceptionType), ("exception_message", record.ExceptionMessage),
                ("exception_detail", record.ExceptionDetail), ("failed_ticks", record.FailedAtUtc.Ticks),
                ("due_ticks", record.DueAtUtc.Ticks), ("payload", record.Payload));

        async Task<RecoveryRecord> FindRecovery(DbConnection connection, DbTransaction transaction)
        {
            var lockFilter = transaction == null ? null : "dispatched = 0";
            var sql = FirstRow(RecoveryTable, RecoveryColumns, "endpoint = :endpoint AND dispatched = 0 AND due_ticks <= :now",
                "due_ticks, recovery_id", lockFilter);
            using (var command = Command(connection, transaction, sql, ("endpoint", endpoint), ("now", DateTime.UtcNow.Ticks)))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                return await reader.ReadAsync().ConfigureAwait(false) ? ReadRecovery(reader) : null;
        }

        public async Task<RecoveryRecord> NextRecovery(string name)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
                return await FindRecovery(connection, null).ConfigureAwait(false);
        }

        Task<int> MarkRecovery(DbConnection connection, DbTransaction transaction, string recoveryId) =>
            Execute(connection, transaction, "UPDATE bq_recovery SET dispatched = 1 WHERE endpoint = :endpoint AND recovery_id = :id",
                ("endpoint", endpoint), ("id", recoveryId));

        public async Task MarkRecoveryDispatched(string name, string recoveryId)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                await MarkRecovery(connection, transaction, recoveryId).ConfigureAwait(false);
                transaction.Commit();
            }
        }

        public async Task<bool> DispatchRecovery(string name, Action<RecoveryRecord> publish)
        {
            CheckEndpoint(name);
            if (publish == null) throw new ArgumentNullException(nameof(publish));
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var record = await FindRecovery(connection, transaction).ConfigureAwait(false);
                if (record == null) return false;
                publish(record);
                await MarkRecovery(connection, transaction, record.RecoveryId).ConfigureAwait(false);
                transaction.Commit();
                return true;
            }
        }

        public async Task<RecoveryRecord> GetError(string name, string recoveryId)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var command = Command(connection, null, $"SELECT {RecoveryColumns} FROM bq_recovery WHERE {ErrorFilter} AND recovery_id = :id",
                ("endpoint", endpoint), ("id", recoveryId)))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                return await reader.ReadAsync().ConfigureAwait(false) ? ReadRecovery(reader) : null;
        }

        public async Task<IReadOnlyList<RecoveryRecord>> ListErrors(string name, int limit)
        {
            CheckEndpoint(name);
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            var records = new List<RecoveryRecord>();
            using (var connection = await Open().ConfigureAwait(false))
            using (var command = Command(connection, null, $"SELECT {RecoveryColumns} FROM bq_recovery WHERE {ErrorFilter} " +
                "ORDER BY failed_ticks DESC, recovery_id DESC FETCH FIRST :page_size ROWS ONLY", ("endpoint", endpoint), ("page_size", limit)))
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                while (await reader.ReadAsync().ConfigureAwait(false)) records.Add(ReadRecovery(reader));
            return records;
        }

        public async Task<bool> ReplayError(string name, string recoveryId, RecoveryRecord replay)
        {
            CheckEndpoint(name);
            using (var connection = await Open().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var marked = await Execute(connection, transaction, "UPDATE bq_recovery SET replayed = 1 WHERE " +
                    ErrorFilter + " AND recovery_id = :id", ("endpoint", endpoint), ("id", recoveryId)).ConfigureAwait(false);
                if (marked != 1 || await InsertRecovery(connection, transaction, replay).ConfigureAwait(false) != 1)
                    return false;
                transaction.Commit();
                return true;
            }
        }
    }
}
