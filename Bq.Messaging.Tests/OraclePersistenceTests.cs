using System;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Oracle;

namespace Bq.Messaging.Tests
{
    partial class Program
    {
        static async Task OraclePersistenceTests()
        {
            var connectionString = Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_STRING");
            if (string.IsNullOrEmpty(connectionString)) return;

            var assembly = System.Reflection.Assembly.LoadFrom(Environment.GetEnvironmentVariable("BQ_ORACLE_DRIVER_ASSEMBLY"));
            var connectionType = assembly.GetType(Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_TYPE"), throwOnError: true);
            DbConnection Connect()
            {
                var connection = (DbConnection)Activator.CreateInstance(connectionType);
                connection.ConnectionString = connectionString;
                return connection;
            }

            var endpoint = "bq-oracle-test-" + Guid.NewGuid().ToString("N");
            var store = new OraclePersistence(Connect, endpoint);
            var body = Enumerable.Range(0, 65536).Select(value => (byte)value).ToArray();
            var message = new OutgoingMessage
            {
                Id = "outgoing", Type = "bq.oracle.test", Exchange = "", RoutingKey = endpoint, Body = body
            };
            try
            {
                using (var session = await store.OpenSession())
                {
                    Assert(ReferenceEquals(session.Connection, session.Transaction.Connection), "Oracle session exposes the transaction connection");
                    Assert(await session.TryClaimMessage(endpoint, "incoming"), "Oracle claims a new delivery");
                    await session.StoreOutgoing(message);
                    await session.SaveSaga("workflow", "key", body, null);
                }
                Assert(await store.NextOutgoing(endpoint) == null, "Oracle rolls back uncommitted outbox changes");
                using (var session = await store.OpenSession())
                {
                    Assert(await session.TryClaimMessage(endpoint, "incoming"), "Oracle rolls back uncommitted inbox claims");
                    Assert(await session.LoadSaga("workflow", "key") == null, "Oracle rolls back uncommitted saga changes");
                    await session.StoreOutgoing(message);
                    await session.SaveSaga("workflow", "key", body, null);
                    session.Commit();
                }
                using (var session = await store.OpenSession())
                {
                    Assert(!await session.TryClaimMessage(endpoint, "incoming"), "Oracle ignores an existing inbox claim");
                    var saga = await session.LoadSaga("workflow", "key");
                    Assert(saga.Version == 1 && saga.Data.SequenceEqual(body), "Oracle returns the committed saga BLOB");
                    await session.SaveSaga("workflow", "key", body, 1);
                    try
                    {
                        await session.DeleteSaga("workflow", "key", 1);
                        throw new Exception("Expected stale saga version to fail.");
                    }
                    catch (SagaConcurrencyException) { }
                    session.Commit();
                }
                try
                {
                    await store.DispatchOutgoing(endpoint, _ => throw new InvalidOperationException("broker failure"));
                    throw new Exception("Expected the broker callback to fail.");
                }
                catch (InvalidOperationException ex) when (ex.Message == "broker failure") { }
                Assert(await store.DispatchOutgoing(endpoint, outgoing =>
                    Assert(outgoing.Exchange == "" && outgoing.Body.SequenceEqual(body), "Oracle preserves default exchange and BLOB")),
                    "Oracle retries an unconfirmed outbox publication");
                Assert(await store.NextOutgoing(endpoint) == null, "Oracle removes the confirmed outbox entry");

                var now = DateTime.UtcNow;
                message.Id = "scheduled";
                using (var session = await store.OpenSession())
                {
                    await session.StoreTimeout(message, now.AddMinutes(30));
                    message.Id = "due";
                    await session.StoreTimeout(message, now);
                    session.Commit();
                }
                Assert(await store.DispatchOutgoing(endpoint, outgoing => Assert(outgoing.Id == "due", "Oracle selects due scheduled sends")),
                    "Oracle dispatches a due scheduled send");
                Assert(await store.NextOutgoing(endpoint) == null, "Oracle leaves future scheduled sends pending");

                var error = new RecoveryRecord
                {
                    RecoveryId = "error", MessageId = "incoming", IsError = true, ErrorQueue = "errors",
                    ExceptionType = "Example", ExceptionMessage = "Échec", ExceptionDetail = new string('火', 10000),
                    FailedAtUtc = now, DueAtUtc = now, Payload = body
                };
                using (var session = await store.OpenSession())
                {
                    var recovery = (IRecoverySession)session;
                    await recovery.StoreRecovery(endpoint, error);
                    await recovery.StoreRecovery(endpoint, error);
                    session.Commit();
                }
                Assert((await store.ListErrors(endpoint, 1)).Single().ExceptionDetail == error.ExceptionDetail,
                    "Oracle stores a large Unicode exception detail and deduplicates recovery entries");
                Assert(await store.DispatchRecovery(endpoint, record => Assert(record.Payload.SequenceEqual(body), "Oracle recovery restores BLOB")),
                    "Oracle sends the pending recovery record");
                var replay = new RecoveryRecord { RecoveryId = "replay", MessageId = "replay", FailedAtUtc = now, DueAtUtc = now, Payload = body };
                Assert(await store.ReplayError(endpoint, error.RecoveryId, replay), "Oracle atomically enqueues replay");
                Assert(!await store.ReplayError(endpoint, error.RecoveryId, replay), "Oracle replay is idempotent");
                Assert((await store.NextRecovery(endpoint)).RecoveryId == replay.RecoveryId, "Oracle exposes pending replay");
            }
            finally
            {
                using (var connection = Connect())
                {
                    await connection.OpenAsync();
                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var table in new[] { "bq_inbox", "bq_outbox", "bq_saga_timeout", "bq_saga", "bq_recovery" })
                        {
                            using (var command = connection.CreateCommand())
                            {
                                command.CommandText = "DELETE FROM " + table + " WHERE endpoint = :endpoint";
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = "endpoint";
                                parameter.Value = endpoint;
                                command.Parameters.Add(parameter);
                                await command.ExecuteNonQueryAsync();
                            }
                        }
                        transaction.Commit();
                    }
                }
            }
            Console.WriteLine("Oracle persistence integration tests passed.");
        }
    }
}
