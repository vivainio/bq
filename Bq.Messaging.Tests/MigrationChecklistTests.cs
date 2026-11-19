using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public interface IPilotMessage { string Text { get; set; } }
    public interface IPilotEvent : IPilotMessage, IEvent { int Count { get; set; } }

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
            config.MutateOutgoing = transport =>
            {
                if (transport.TypeName != typeof(LocalMessage).FullName || transport.Headers.ContainsKey("immediate")) return;
                Assert(transport.IncomingMessage != null, "outbox mutator sees the incoming handler context");
                transport.TypeName = "migration.framed";
                transport.Headers["migration-framing"] = "v1";
                transport.Body = new byte[] { 0x7e }.Concat(transport.Body).ToArray();
            };
            config.Recoverability.DelayedRetries = new[] { TimeSpan.FromHours(1) };
            config.Recoverability.Policy = failure =>
            {
                if (failure.Exception.Message == "retry the unit" && failure.ImmediateRetries == 0 && failure.DelayedRetries == 0)
                    return RecoveryAction.ImmediateRetry();
                return RecoveryAction.MoveToError();
            };
            var currentScope = new AsyncLocal<IncomingMessageContext>();
            var closedScopes = 0;
            var resolvedHandlers = 0;
            config.Incoming.Use(async (incoming, next) =>
            {
                Assert(currentScope.Value == null, "message attempt starts with a clean host scope");
                currentScope.Value = incoming;
                try { await next(); }
                finally
                {
                    currentScope.Value = null;
                    Interlocked.Increment(ref closedScopes);
                }
            });
            config.Incoming.Use(async (incoming, next) =>
            {
                var message = (Command)incoming.Message;
                if (message.Text == "skip") return;
                if (message.Text == "retry")
                    incoming.Items.Add("retry", incoming.ImmediateRetries);
                incoming.Items.Add("unit", message.Text);
                try
                {
                    await next();
                    if (message.Text == "duplicate-next") await next();
                }
                catch (InvalidOperationException ex) when (message.Text == "replace")
                {
                    throw new FatalFailureException("replacement: " + ex.Message);
                }
                catch (InvalidOperationException) when (message.Text == "swallow")
                {
                    // Returning successfully from a behavior must not commit a failed handler's work.
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
                Assert(ReferenceEquals(currentScope.Value, incoming), "before-commit callback runs inside the host scope");
                var unit = (string)incoming.Items["unit"];
                await InsertApplicationWork(session, unit + ":commit");
                if (unit == "before-commit") throw new FatalFailureException("unit of work save failed");
                if (unit == "retry" && (int)incoming.Items["retry"] == 0)
                {
                    ((Command)incoming.Message).Text = "mutated during failed attempt";
                    throw new InvalidOperationException("retry the unit");
                }
            });
            config.Handle<Command>(handlerContext =>
            {
                var scope = currentScope.Value;
                Assert(scope != null && ReferenceEquals(scope.HandlerContext, handlerContext),
                    "handler factory resolves inside the host scope for this attempt");
                Interlocked.Increment(ref resolvedHandlers);
                return new Handler<Command>(async (message, context) =>
                {
                    await InsertApplicationWork(context.PersistenceSession, message.Text + ":handler");
                    // An absent destination keeps the committed outbox entry available for inspection.
                    await context.Send(name + ".absent", new LocalMessage { Text = message.Text });
                    if (message.Text == "replace")
                    {
                        var send = new SendOptions();
                        send.SetDestination(name + ".immediate");
                        send.SetHeader("immediate", true);
                        send.RequireImmediateDispatch();
                        await context.Send(new LocalMessage { Text = message.Text }, send);
                        throw new InvalidOperationException("handler failure");
                    }
                    if (message.Text == "swallow") throw new InvalidOperationException("caught handler failure");
                });
            });

            try
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                using (var endpoint = RabbitEndpoint.Start(config))
                {
                    channel.QueueDeclare(name + ".immediate", false, true, true);
                    foreach (var scenario in new[] { "skip", "replace", "before-commit", "after-next", "commit", "retry", "swallow", "duplicate-next" })
                        await endpoint.SendLocal(new Command { Text = scenario });

                    await WaitFor(async () => (await endpoint.ListErrors()).Count == 5 &&
                        await CountMigrationRows(path, "bq_inbox") == 3, "pipeline cases reach durable outcomes");
                    Assert(Volatile.Read(ref closedScopes) == 9 && Volatile.Read(ref resolvedHandlers) == 8,
                        "each attempt closes its host scope and the short-circuited attempt skips handler resolution");
                    var errors = await endpoint.ListErrors();
                    Assert(errors.Where(error => error.ExceptionType == typeof(InvalidOperationException).FullName)
                        .Select(error => error.ExceptionMessage).OrderBy(message => message).SequenceEqual(
                            new[] { "An incoming behavior can invoke next only once per attempt.", "caught handler failure" }),
                        "recoverability sees the caught failure and duplicate continuation");
                    Assert(errors.Where(error => error.ExceptionType == typeof(FatalFailureException).FullName)
                        .Select(error => error.ExceptionMessage).OrderBy(message => message).SequenceEqual(
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

                    var direct = channel.BasicGet(name + ".immediate", true);
                    Assert(direct != null && Equals(direct.BasicProperties.Headers["immediate"], true),
                        "immediate send publishes with headers even when the handler transaction rolls back");
                    var pending = await store.NextOutgoing(name);
                    Assert(await CountMigrationRows(path, "bq_outbox") == 2 && pending?.RoutingKey == name + ".absent",
                        "only successful attempts committed outbox messages");
                    Assert(pending.Type == "migration.framed" && pending.Body[0] == 0x7e &&
                        Equals(pending.Headers["migration-framing"], "v1"),
                        "outbox stores the mutated contract name, header, and wire body");
                    var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
                    var storedMessage = MessagePackSerializer.Deserialize<LocalMessage>(pending.Body.Skip(1).ToArray(), options);
                    Assert(storedMessage.Text == "commit", "stored wire body frames the committed message");
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

        static async Task StalledDispatcherTest(Uri uri)
        {
            var name = "bq.test.stalled-dispatch." + Guid.NewGuid().ToString("N");
            var path = Path.Combine(Path.GetTempPath(), name + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();

            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var notification = new TaskCompletionSource<(string Source, Exception Error)>(TaskCreationOptions.RunContinuationsAsynchronously);
            var config = new EndpointConfiguration(name, uri)
            {
                Persistence = new FaultPersistence(store) { OutgoingGate = release.Task },
                CriticalErrorThreshold = TimeSpan.FromMilliseconds(250),
                OnCriticalError = (source, error) =>
                {
                    notification.TrySetResult((source, error));
                    return Task.CompletedTask;
                }
            };
            config.SendOnly();

            try
            {
                using (var endpoint = RabbitEndpoint.Start(config))
                {
                    try
                    {
                        var failure = await Within(notification.Task);
                        Assert(failure.Source == "Outbox dispatcher" && failure.Error is TimeoutException && !release.Task.IsCompleted,
                            "critical error callback identifies an outbox poll that has not returned");
                    }
                    finally
                    {
                        release.TrySetResult(true);
                        await endpoint.StopAsync();
                    }
                }
            }
            finally
            {
                File.Delete(path);
            }
            Console.WriteLine("Stalled dispatcher critical-error test passed.");
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
            var commandReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            RegisterPilotHandler(sender, receiver, new Handler<IPilotEvent>((message, context) =>
            {
                Assert(context.MessageHeaders["migration-group"] == "first", "publish option reaches the migrated subscriber");
                Assert(message.Count == 7, "interface event properties round-trip through MessagePack");
                received.TrySetResult(message.Text);
                return Task.CompletedTask;
            }));
            RegisterPilotHandler(sender, receiver, new Handler<Command>((message, context) =>
            {
                commandReceived.TrySetResult(message.Text);
                return Task.CompletedTask;
            }));

            using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                try
                {
                    channel.ExchangeDeclare(legacyExchange, ExchangeType.Topic, true);
                    channel.QueueDeclare(legacyQueue, true, false, false);
                    var contract = typeof(IPilotEvent).FullName;
                    channel.QueueBind(legacyQueue, legacyExchange, contract);
                    IStartableEndpoint startable = await Endpoint.Create(sender);
                    using (var producer = await startable.Start())
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
                        IMessageSession messaging = producer;
                        await messaging.Publish<IPilotEvent>(message =>
                        {
                            message.Text = "pilot";
                            message.Count = 7;
                        }, publish);
                        object pilotCommand = new Command { Text = "pilot command" };
                        await messaging.Send(pilotCommand, new SendOptions());
                        Assert(await Within(commandReceived.Task) == "pilot command", "command convention configures the migrated route");
                        Assert(await Within(received.Task) == "pilot", "event convention subscribes the migrated receiver");
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

        static void RegisterPilotHandler<T>(EndpointConfiguration sender, EndpointConfiguration receiver, IHandleMessages<T> handler)
            where T : IMessage
        {
            if (typeof(ICommand).IsAssignableFrom(typeof(T))) sender.Route<T>(receiver.Name);
            receiver.Handle(() => handler, subscribe: typeof(IEvent).IsAssignableFrom(typeof(T)));
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
