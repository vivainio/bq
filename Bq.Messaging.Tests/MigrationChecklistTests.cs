using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    partial class Program
    {
        static async Task MigrationPipelineTests(Uri uri)
        {
            var name = "bq.test.migration." + Guid.NewGuid().ToString("N");
            var path = Path.Combine(Path.GetTempPath(), name + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();
            using (var db = Sqlite(path))
            {
                await db.OpenAsync();
                using (var command = db.CreateCommand())
                {
                    command.CommandText = "CREATE TABLE application_work (name TEXT PRIMARY KEY)";
                    await command.ExecuteNonQueryAsync();
                }
            }

            var config = new EndpointConfiguration(name, uri) { Persistence = store, ImmediateRetries = 0 };
            config.Recoverability.DelayedRetries = new[] { TimeSpan.FromHours(1) };
            config.Recoverability.Policy = failure =>
            {
                if (failure.Exception.Message == "retry the unit" && failure.ImmediateRetries == 0 && failure.DelayedRetries == 0)
                    return RecoveryAction.ImmediateRetry();
                return RecoveryAction.MoveToError();
            };
            config.Incoming.Use(async (incoming, next) =>
            {
                var message = (Command)incoming.Message;
                if (message.Text == "skip") return;
                if (message.Text == "retry")
                    incoming.Items.Add("retry", incoming.ImmediateRetries);
                incoming.Items.Add("unit", message.Text);
                try { await next(); }
                catch (InvalidOperationException ex) when (message.Text == "replace")
                {
                    throw new FatalFailureException("replacement: " + ex.Message);
                }
                if (message.Text == "after-next") throw new FatalFailureException("behavior failed after next");
            });
            config.Incoming.Use((incoming, next) =>
            {
                Assert((string)incoming.Items["unit"] == ((Command)incoming.Message).Text,
                    "incoming behaviors run in registration order and share items");
                return next();
            });
            config.Incoming.BeforeCommit(async (incoming, session) =>
            {
                var unit = (string)incoming.Items["unit"];
                await InsertApplicationWork(session, unit + ":commit");
                if (unit == "before-commit") throw new FatalFailureException("unit of work save failed");
                if (unit == "retry" && (int)incoming.Items["retry"] == 0)
                {
                    ((Command)incoming.Message).Text = "mutated during failed attempt";
                    throw new InvalidOperationException("retry the unit");
                }
            });
            config.Handle(() => new Handler<Command>(async (message, context) =>
            {
                await InsertApplicationWork(context.PersistenceSession, message.Text + ":handler");
                // An absent destination keeps the committed outbox entry available for inspection.
                await context.Send(name + ".absent", new LocalMessage { Text = message.Text });
                if (message.Text == "replace") throw new InvalidOperationException("handler failure");
            }));

            try
            {
                using (var endpoint = RabbitEndpoint.Start(config))
                {
                    foreach (var scenario in new[] { "skip", "replace", "before-commit", "after-next", "commit", "retry" })
                        await endpoint.SendLocal(new Command { Text = scenario });

                    await WaitFor(async () => (await endpoint.ListErrors()).Count == 3 &&
                        await CountMigrationRows(path, "bq_inbox") == 3, "pipeline cases reach durable outcomes");
                    var errors = await endpoint.ListErrors();
                    Assert(errors.All(error => error.ExceptionType == typeof(FatalFailureException).FullName) &&
                        errors.Select(error => error.ExceptionMessage).OrderBy(message => message).SequenceEqual(
                            new[] { "behavior failed after next", "replacement: handler failure", "unit of work save failed" }),
                        "recoverability sees callback and behavior exceptions, including a replacement");

                    using (var db = Sqlite(path))
                    {
                        await db.OpenAsync();
                        using (var command = db.CreateCommand())
                        {
                            command.CommandText = "SELECT name FROM application_work ORDER BY name";
                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "commit:commit", "successful callback committed");
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "commit:handler", "successful handler committed");
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "retry:commit", "retried callback uses the original message");
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "retry:handler", "retry reruns work in a new transaction");
                                Assert(!await reader.ReadAsync(), "failed and short-circuited paths left no application rows");
                            }
                        }
                    }

                    var pending = await store.NextOutgoing(name);
                    Assert(await CountMigrationRows(path, "bq_outbox") == 2 && pending?.RoutingKey == name + ".absent",
                        "only successful attempts committed outbox messages");
                }
                Console.WriteLine("Migration behavior and unit-of-work tests passed.");
            }
            finally
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDelete(name);
                    channel.QueueDelete(name + ".error");
                }
                File.Delete(path);
            }
        }

        static async Task PhasedMigrationTests(Uri uri)
        {
            var prefix = "bq.test.cutover." + Guid.NewGuid().ToString("N");
            var senderName = prefix + ".sender";
            var receiverName = prefix + ".receiver";
            var legacyQueue = prefix + ".legacy";
            var legacyExchange = prefix + ".xml";
            var pilotExchange = prefix + ".msgpack";
            var path = Path.Combine(Path.GetTempPath(), prefix + ".db");
            var senderStore = new SqlitePersistence(() => Sqlite(path), senderName);
            var receiverStore = new SqlitePersistence(() => Sqlite(path), receiverName);
            await senderStore.Initialize();
            await receiverStore.Initialize();
            var sender = new EndpointConfiguration(senderName, uri) { Persistence = senderStore, EventsExchange = pilotExchange };
            sender.SendOnly();
            var receiver = new EndpointConfiguration(receiverName, uri) { Persistence = receiverStore, EventsExchange = pilotExchange };
            var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.Handle(() => new Handler<Event>((message, context) =>
            {
                Assert(context.GetHeader("migration-group") == "first", "publish option reaches the migrated subscriber");
                received.TrySetResult(message.Text);
                return Task.CompletedTask;
            }), subscribe: true);

            using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                try
                {
                    channel.ExchangeDeclare(legacyExchange, ExchangeType.Topic, true);
                    channel.QueueDeclare(legacyQueue, true, false, false);
                    var contract = typeof(Event).FullName;
                    channel.QueueBind(legacyQueue, legacyExchange, contract);
                    using (var producer = RabbitEndpoint.Start(sender))
                    using (var subscriber = RabbitEndpoint.Start(receiver))
                    {
                        const string legacyXml = "<Event><Text>legacy</Text></Event>";
                        var properties = channel.CreateBasicProperties();
                        properties.MessageId = Guid.NewGuid().ToString("N");
                        properties.Type = contract;
                        properties.ContentType = "text/xml";
                        channel.ConfirmSelect();
                        channel.BasicPublish(legacyExchange, contract, properties, System.Text.Encoding.UTF8.GetBytes(legacyXml));
                        Assert(channel.WaitForConfirms(TimeSpan.FromSeconds(5)), "legacy publication confirmed");
                        var publish = new PublishOptions();
                        publish.SetHeader("migration-group", "first");
                        await producer.CreateUniformSession().Publish(new Event { Text = "pilot" }, publish);

                        Assert(await Within(received.Task) == "pilot", "send-only Bq publisher reaches the migrated subscriber");
                        var legacy = channel.BasicGet(legacyQueue, true);
                        Assert(legacy != null && legacy.BasicProperties.ContentType == "text/xml" &&
                            System.Text.Encoding.UTF8.GetString(legacy.Body.ToArray()) == legacyXml,
                            "legacy subscriber receives the original XML");
                        Assert(channel.BasicGet(legacyQueue, true) == null && (await subscriber.ListErrors()).Count == 0,
                            "wire formats stay on their own exchanges during phased cutover");
                    }
                }
                finally
                {
                    channel.QueueDelete(receiverName);
                    channel.QueueDelete(receiverName + ".error");
                    channel.QueueDelete(legacyQueue);
                    channel.ExchangeDelete(legacyExchange);
                    channel.ExchangeDelete(pilotExchange);
                    File.Delete(path);
                }
            }
            Console.WriteLine("Phased migration routing and wire isolation tests passed.");
        }

        static async Task InsertApplicationWork(IPersistenceSession session, string work)
        {
            using (var command = session.Connection.CreateCommand())
            {
                command.Transaction = session.Transaction;
                command.CommandText = "INSERT INTO application_work (name) VALUES (@name)";
                DbParameter parameter = command.CreateParameter();
                parameter.ParameterName = "@name";
                parameter.Value = work;
                command.Parameters.Add(parameter);
                await command.ExecuteNonQueryAsync();
            }
        }

        static async Task<int> CountMigrationRows(string path, string table)
        {
            using (var db = Sqlite(path))
            {
                await db.OpenAsync();
                using (var command = db.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT(*) FROM " + table;
                    return Convert.ToInt32(await command.ExecuteScalarAsync());
                }
            }
        }
    }
}
