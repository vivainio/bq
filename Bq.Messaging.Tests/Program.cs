using System;
using System.Threading.Tasks;
using System.Threading;
using System.Data.Common;
using System.IO;
using Bq.Persistence.Sqlite;
using Bq.Messaging;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public class Command { public string Text { get; set; } }
    public class Event { public string Text { get; set; } }
    public class LocalMessage { public string Text { get; set; } }
    public class Response { public string Text { get; set; } }
    public class Failed { public string Text { get; set; } }
    public class RecoverCommand { public string Text { get; set; } }
    public class FatalFailureException : Exception { public FatalFailureException(string text) : base(text) { } }
    public class OldContract { public string Text { get; set; } }
    public class NewContract { public string Text { get; set; } public int Added { get; set; } }
    public class OrderSaga { public string Status { get; set; } }
    public class StartOrder { public string OrderId { get; set; } public int DelayMs { get; set; } public bool FailFirst { get; set; } }
    public class ChangeOrder { public string OrderId { get; set; } public bool Complete { get; set; } }
    public class OrderTimeout { public string OrderId { get; set; } }
    public class OrderState { public string Status { get; set; } public int TimeoutCount { get; set; } }
    public class OrderWorkflow : Saga<OrderState>, IAmStartedByMessages<StartOrder>, IHandleMessages<ChangeOrder>, IHandleTimeouts<OrderTimeout>
    {
        public static int StartAttempts;
        public static int TimeoutCalls;
        public async Task Handle(StartOrder message, IMessageHandlerContext context)
        {
            Data.Status = "started";
            if (message.DelayMs > 0) await RequestTimeout(TimeSpan.FromMilliseconds(message.DelayMs), new OrderTimeout { OrderId = message.OrderId });
            if (message.FailFirst && Interlocked.Increment(ref StartAttempts) == 1) throw new Exception("simulate rollback");
        }
        public Task Handle(ChangeOrder message, IMessageHandlerContext context)
        {
            if (message.Complete) MarkAsComplete(); else Data.Status = "changed";
            return Task.CompletedTask;
        }
        public Task Timeout(OrderTimeout message, IMessageHandlerContext context)
        {
            Interlocked.Increment(ref TimeoutCalls);
            Data.TimeoutCount++;
            return Task.CompletedTask;
        }
    }
    class Handler<T> : IHandleMessages<T>
    {
        readonly Func<T, IMessageHandlerContext, Task> action;
        public Handler(Func<T, IMessageHandlerContext, Task> action) { this.action = action; }
        public Task Handle(T value, IMessageHandlerContext context) => action(value, context);
    }
    class FaultPersistence : IPersistenceProvider, IRecoveryPersistence
    {
        readonly SqlitePersistence inner;
        public int FailNextStore;
        public FaultPersistence(SqlitePersistence inner) { this.inner = inner; }
        public async Task<IPersistenceSession> OpenSession() => new FaultSession(await inner.OpenSession(), this);
        public Task<OutgoingMessage> NextOutgoing(string endpoint) => inner.NextOutgoing(endpoint);
        public Task MarkDispatched(string endpoint, string id) => inner.MarkDispatched(endpoint, id);
        public Task<RecoveryRecord> NextRecovery(string endpoint) => inner.NextRecovery(endpoint);
        public Task MarkRecoveryDispatched(string endpoint, string id) => inner.MarkRecoveryDispatched(endpoint, id);
        public Task<RecoveryRecord> GetError(string endpoint, string id) => inner.GetError(endpoint, id);
        public Task<System.Collections.Generic.IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit) => inner.ListErrors(endpoint, limit);
        public Task<bool> ReplayError(string endpoint, string id, RecoveryRecord record) => inner.ReplayError(endpoint, id, record);
        sealed class FaultSession : IPersistenceSession, IRecoverySession
        {
            readonly IPersistenceSession session; readonly FaultPersistence owner;
            public FaultSession(IPersistenceSession session, FaultPersistence owner) { this.session = session; this.owner = owner; }
            public DbConnection Connection => session.Connection;
            public DbTransaction Transaction => session.Transaction;
            public Task<bool> TryClaimMessage(string endpoint, string id) => session.TryClaimMessage(endpoint, id);
            public Task StoreOutgoing(OutgoingMessage message) => session.StoreOutgoing(message);
            public Task StoreTimeout(OutgoingMessage message, DateTime due) => session.StoreTimeout(message, due);
            public Task<SagaRecord> LoadSaga(string type, string id) => session.LoadSaga(type, id);
            public Task SaveSaga(string type, string id, byte[] data, long? version) => session.SaveSaga(type, id, data, version);
            public Task DeleteSaga(string type, string id, long version) => session.DeleteSaga(type, id, version);
            public Task<bool> HasRecovery(string endpoint, string id) => ((IRecoverySession)session).HasRecovery(endpoint, id);
            public Task StoreRecovery(string endpoint, RecoveryRecord record)
            {
                if (Interlocked.Exchange(ref owner.FailNextStore, 0) == 1) throw new Exception("simulated storage outage");
                return ((IRecoverySession)session).StoreRecovery(endpoint, record);
            }
            public void Commit() => session.Commit();
            public void Dispose() => session.Dispose();
        }
    }
    class Program
    {
        static async Task Main()
        {
            var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
            var bytes = MessagePackSerializer.Serialize(new OldContract { Text = "hello" }, options);
            var newer = MessagePackSerializer.Deserialize<NewContract>(bytes, options);
            Assert(newer.Text == "hello" && newer.Added == 0, "MessagePack versioning");
            var older = MessagePackSerializer.Deserialize<OldContract>(MessagePackSerializer.Serialize(new NewContract { Text = "new", Added = 42 }, options), options);
            Assert(older.Text == "new", "MessagePack unknown property");
            Console.WriteLine("MessagePack tests passed on " + Environment.Version);
            await PersistenceTests();
            var uri = Environment.GetEnvironmentVariable("BQ_RABBITMQ_URI");
            if (string.IsNullOrEmpty(uri)) { Console.WriteLine("RabbitMQ tests skipped: set BQ_RABBITMQ_URI."); return; }
            await BrokerTests(new Uri(uri));
            await SagaTests(new Uri(uri));
            await RecoverabilityTests(new Uri(uri));
            await LifecycleTests.Run(new Uri(uri));
            await UniformSessionTests.Run(new Uri(uri), Sqlite);
            var outageDirectory = Environment.GetEnvironmentVariable("BQ_BROKER_OUTAGE_DIR");
            if (!string.IsNullOrEmpty(outageDirectory)) await BrokerOutageTest(new Uri(uri), outageDirectory);
        }
        static DbConnection Sqlite(string path)
        {
            var type = Type.GetType("Mono.Data.Sqlite.SqliteConnection, Mono.Data.Sqlite, Version=4.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756", true);
            var connection = (DbConnection)Activator.CreateInstance(type);
            connection.ConnectionString = "Data Source=" + path + ";Default Timeout=5";
            return connection;
        }
        static async Task PersistenceTests()
        {
            var path = Path.Combine(Path.GetTempPath(), "bq-" + Guid.NewGuid().ToString("N") + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), "test");
            try
            {
                await store.Initialize();
                using (var connection = Sqlite(path))
                {
                    await connection.OpenAsync();
                    using (var cmd = connection.CreateCommand()) { cmd.CommandText = "CREATE TABLE business (value TEXT NOT NULL)"; await cmd.ExecuteNonQueryAsync(); }
                }
                using (var session = await store.OpenSession())
                {
                    Assert(await session.TryClaimMessage("test", "rollback"), "claim rollback");
                    using (var cmd = session.Connection.CreateCommand()) { cmd.Transaction = session.Transaction; cmd.CommandText = "INSERT INTO business(value) VALUES('rollback')"; await cmd.ExecuteNonQueryAsync(); }
                    await session.SaveSaga("s1", new OrderSaga { Status = "rolled back" });
                    await session.StoreOutgoing(new OutgoingMessage { Id = "rolled", Type = "t", Exchange = "", RoutingKey = "q", Body = new byte[] { 1 } });
                }
                using (var session = await store.OpenSession())
                {
                    Assert(await session.TryClaimMessage("test", "rollback"), "rollback restores claim");
                    Assert(await session.LoadSaga<OrderSaga>("s1") == null, "saga rollback");
                    using (var cmd = session.Connection.CreateCommand()) { cmd.Transaction = session.Transaction; cmd.CommandText = "SELECT COUNT(*) FROM business"; Assert(Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 0, "business rollback"); }
                    using (var cmd = session.Connection.CreateCommand()) { cmd.Transaction = session.Transaction; cmd.CommandText = "INSERT INTO business(value) VALUES('committed')"; await cmd.ExecuteNonQueryAsync(); }
                    await session.SaveSaga("s1", new OrderSaga { Status = "created" });
                    await session.StoreOutgoing(new OutgoingMessage { Id = "sent", Type = "t", Exchange = "", RoutingKey = "q", Body = new byte[] { 2 } });
                    session.Commit();
                }
                using (var session = await store.OpenSession())
                {
                    Assert(!await session.TryClaimMessage("test", "rollback"), "deduplication");
                    using (var cmd = session.Connection.CreateCommand()) { cmd.Transaction = session.Transaction; cmd.CommandText = "SELECT COUNT(*) FROM business"; Assert(Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1, "business commit"); }
                    var saga = await session.LoadSaga<OrderSaga>("s1");
                    Assert(saga.Data.Status == "created" && saga.Version == 1, "saga reload");
                    await session.SaveSaga("s1", new OrderSaga { Status = "updated" }, saga.Version);
                    session.Commit();
                }
                using (var session = await store.OpenSession())
                {
                    bool conflict = false;
                    try { await session.SaveSaga("s1", new OrderSaga(), 1); }
                    catch (SagaConcurrencyException) { conflict = true; }
                    Assert(conflict, "stale saga version");
                    var saga = await session.LoadSaga<OrderSaga>("s1");
                    Assert(saga.Version == 2 && saga.Data.Status == "updated", "saga update");
                    await session.DeleteSaga<OrderSaga>("s1", saga.Version);
                    session.Commit();
                }
                Assert((await store.GetDispatchBacklog()).Outgoing == 1, "only committed outgoing message is pending");
                var pending = await store.ListPendingDispatches(1);
                Assert(pending.Count == 1 && pending[0].MessageId == "sent" &&
                    pending[0].Kind == DispatchKind.Outbox && pending[0].Exchange == "" && pending[0].RoutingKey == "q",
                    "pending dispatch lists committed message metadata");
                Assert((await store.NextOutgoing("test")).Id == "sent", "outbox commit and rollback");
                await store.MarkDispatched("test", "sent");
                Assert(await store.NextOutgoing("test") == null, "outbox dispatch");
                Assert((await store.GetDispatchBacklog()).Outgoing == 0, "dispatched item leaves backlog");
                using (var session = await store.OpenSession())
                {
                    await session.StoreOutgoing(new OutgoingMessage { Id = "ordinary", Type = "t",
                        Exchange = "", RoutingKey = "q", Body = new byte[] { 1 } });
                    await session.StoreTimeout(new OutgoingMessage { Id = "timeout", Type = "t",
                        Exchange = "", RoutingKey = "q", Body = new byte[] { 1 } }, DateTime.UtcNow.AddSeconds(-1));
                    session.Commit();
                }
                Assert((await store.NextOutgoing("test")).Id == "timeout" &&
                    (await store.NextOutgoing("test")).Id == "ordinary",
                    "outgoing polls alternate between due timeouts and ordinary messages");
                await store.MarkDispatched("test", "ordinary");
                await store.MarkDispatched("test", "timeout");
                using (var session = await store.OpenSession())
                {
                    await session.StoreTimeout(new OutgoingMessage { Id = "later", Type = typeof(SagaTimeoutMessage).FullName,
                        Exchange = "", RoutingKey = "q", Body = new byte[] { 1 } }, DateTime.UtcNow.AddHours(1));
                    session.Commit();
                }
                Assert((await store.ListPendingDispatches(DispatchKind.SagaTimeout)).Count == 1 &&
                    (await store.ListReadyDispatches(DispatchKind.SagaTimeout)).Count == 0,
                    "ready inspection excludes scheduled timeouts");
                var failedAt = DateTime.UtcNow;
                using (var session = await store.OpenSession())
                {
                    foreach (var id in new[] { "a", "b", "c" })
                        await ((IRecoverySession)session).StoreRecovery("test", new RecoveryRecord
                        {
                            RecoveryId = id, MessageId = id, IsError = true, ErrorQueue = "test.error",
                            FailedAtUtc = failedAt, DueAtUtc = failedAt, Payload = new byte[] { 1 }
                        });
                    session.Commit();
                }
                var errorsPending = await store.ListReadyDispatches(DispatchKind.Error, 2);
                Assert(errorsPending.Count == 2 && errorsPending[0].Kind == DispatchKind.Error &&
                    errorsPending[0].DueAtUtc == failedAt, "pending errors include scheduled UTC time");
                Assert((await store.ListPendingDispatches(DispatchKind.Recovery)).Count == 0,
                    "pending recovery filter excludes errors");
                var firstPage = await store.ListErrors("test", 2);
                Assert(firstPage.Count == 2 && firstPage[0].RecoveryId == "c" &&
                    firstPage[1].RecoveryId == "b", "errors with matching timestamps have stable order");
                Assert(!firstPage[0].Dispatched, "error starts pending");
                await store.MarkRecoveryDispatched("test", "c");
                Assert((await store.GetError("test", "c")).Dispatched, "retained error shows recorded dispatch");
                var secondPage = await store.ListErrors("test", 2, firstPage[1]);
                Assert(secondPage.Count == 1 && secondPage[0].RecoveryId == "a",
                    "error cursor reaches remaining older records without overlap");
                Console.WriteLine("SQLite transaction and saga tests passed.");
            }
            finally { if (File.Exists(path)) File.Delete(path); }
        }
        static async Task BrokerTests(Uri uri)
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderName = "bq.test.sender." + suffix;
            var receiverName = "bq.test.receiver." + suffix;
            var subscriberName = "bq.test.subscriber." + suffix;
            var gotCommand = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var gotEvent1 = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var gotEvent2 = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var gotReply = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var gotLocal = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var attempts = 0;
            var senderPath = Path.Combine(Path.GetTempPath(), "bq-sender-" + suffix + ".db");
            var senderStore = new SqlitePersistence(() => Sqlite(senderPath), senderName); await senderStore.Initialize();
            var subscriberPath = Path.Combine(Path.GetTempPath(), "bq-subscriber-" + suffix + ".db");
            var subscriberStore = new SqlitePersistence(() => Sqlite(subscriberPath), subscriberName); await subscriberStore.Initialize();
            var sender = new EndpointConfiguration(senderName, uri) { Persistence = senderStore };
            sender.Route<Command>(receiverName);
            sender.Route<Failed>(receiverName);
            sender.Handle(() => new Handler<Response>((m, c) => { gotReply.TrySetResult(m.Text); return Task.CompletedTask; }));
            var dbPath = Path.Combine(Path.GetTempPath(), "bq-broker-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(dbPath), receiverName);
            await store.Initialize();
            var eventsExchange = "bq.test.events." + suffix;
            var errorExchange = "bq.test.errors." + suffix;
            var receiver = new EndpointConfiguration(receiverName, uri)
                { Persistence = store, EventsExchange = eventsExchange, ErrorExchange = errorExchange };
            receiver.InputQueueArguments["x-message-ttl"] = 120000;
            receiver.Handle(() => new Handler<Command>(async (m, c) => { gotCommand.TrySetResult(m.Text); await c.Reply(new Response { Text = m.Text }); await c.SendLocal(new LocalMessage { Text = m.Text }); }));
            receiver.Handle(() => new Handler<LocalMessage>((m, c) => { gotLocal.TrySetResult(m.Text); return Task.CompletedTask; }));
            receiver.Handle(() => new Handler<Event>((m, c) => { gotEvent1.TrySetResult(m.Text); return Task.CompletedTask; }), true);
            receiver.Recoverability.DelayedRetries = new TimeSpan[0];
            receiver.Handle(() => new Handler<Failed>((m, c) => { attempts++; throw new Exception("expected"); }));
            var subscriber = new EndpointConfiguration(subscriberName, uri) { Persistence = subscriberStore, EventsExchange = eventsExchange };
            subscriber.Handle(() => new Handler<Event>((m, c) => { gotEvent2.TrySetResult(m.Text); return Task.CompletedTask; }), true);
            try
            {
                using (var a = RabbitEndpoint.Start(sender))
                using (var b = RabbitEndpoint.Start(receiver))
                using (var c = RabbitEndpoint.Start(subscriber))
                {
                    var rollback = new InvalidOperationException("abort application transaction");
                    var outsideSession = a.CreateUniformSession();
                    try
                    {
                        await a.ExecuteTransaction(async session =>
                        {
                            using (var sql = session.Connection.CreateCommand())
                            {
                                sql.Transaction = session.Transaction;
                                sql.CommandText = "CREATE TABLE transaction_probe(value TEXT)";
                                await sql.ExecuteNonQueryAsync();
                            }
                            var rejected = false;
                            try { await outsideSession.Send(receiverName, new Command { Text = "out of scope" }); }
                            catch (InvalidOperationException) { rejected = true; }
                            Assert(rejected, "an outside session cannot bypass the application transaction");
                            await a.CreateUniformSession().Send(receiverName, new Command { Text = "rolled back" });
                            throw rollback;
                        });
                        throw new Exception("Expected application transaction to fail");
                    }
                    catch (InvalidOperationException ex) when (ReferenceEquals(ex, rollback)) { }
                    Assert((await senderStore.ListPendingDispatches()).Count == 0, "rollback discards uniform-session outbox message");

                    using (var cancellation = new CancellationTokenSource())
                    {
                        try
                        {
                            await a.ExecuteTransaction(async session =>
                            {
                                await session.Send(receiverName, new Command { Text = "canceled" });
                                cancellation.Cancel();
                                Assert(session.CancellationToken.IsCancellationRequested, "session observes request cancellation");
                            }, cancellation.Token);
                            throw new Exception("Expected application transaction cancellation");
                        }
                        catch (OperationCanceledException) { }
                    }
                    Assert((await senderStore.ListPendingDispatches()).Count == 0, "cancellation discards outgoing message");

                    var dueAt = DateTimeOffset.UtcNow.AddSeconds(2);
                    var transactionResult = await a.ExecuteTransaction(async session =>
                    {
                        using (var sql = session.Connection.CreateCommand())
                        {
                            sql.Transaction = session.Transaction;
                            sql.CommandText = "CREATE TABLE transaction_probe(value TEXT); INSERT INTO transaction_probe VALUES ('committed')";
                            await sql.ExecuteNonQueryAsync();
                        }
                        await a.CreateUniformSession().SendAt(new Command { Text = "command" }, dueAt);
                        return "committed";
                    });
                    using (var db = Sqlite(senderPath))
                    {
                        await db.OpenAsync();
                        using (var sql = db.CreateCommand())
                        {
                            sql.CommandText = "SELECT value FROM transaction_probe";
                            Assert((string)await sql.ExecuteScalarAsync() == transactionResult, "application transaction returns committed business data");
                        }
                    }
                    var producerName = "bq.test.producer." + suffix;
                    var producerStore = new SqlitePersistence(() => Sqlite(senderPath), producerName);
                    var producerConfig = new EndpointConfiguration(producerName, uri) { Persistence = producerStore, EventsExchange = eventsExchange };
                    var connectionConfigured = false;
                    producerConfig.ConfigureRabbitMqConnection = factory =>
                    {
                        factory.RequestedHeartbeat = TimeSpan.FromSeconds(15);
                        connectionConfigured = true;
                    };
                    producerConfig.SendOnly();
                    using (var producer = RabbitEndpoint.Start(producerConfig))
                    {
                        Assert(connectionConfigured, "RabbitMQ connection customization runs at startup");
                        using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                        using (var probe = connection.CreateModel())
                        {
                            var missing = false;
                            try { probe.QueueDeclarePassive(producerName); }
                            catch (RabbitMQ.Client.Exceptions.OperationInterruptedException ex) when (ex.ShutdownReason.ReplyCode == 404)
                            {
                                missing = true;
                            }
                            Assert(missing, "send-only endpoint has no input queue");
                        }
                        using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                        using (var queueOwner = connection.CreateModel())
                        {
                            queueOwner.QueueDeclare(producerName, true, false, false);
                            try
                            {
                                await producer.SendLocal(new LocalMessage { Text = "shared endpoint" });
                                Assert(queueOwner.BasicGet(producerName, true) != null,
                                    "send-only local send reaches the existing named queue");
                                await producer.SendLocalAt(new LocalMessage { Text = "scheduled local" },
                                    DateTimeOffset.UtcNow.AddMilliseconds(500));
                                await WaitFor(() => Task.FromResult(queueOwner.BasicGet(producerName, true) != null),
                                    "send-only scheduled local send reaches the existing named queue");
                            }
                            finally { queueOwner.QueueDelete(producerName); }
                        }
                        await producer.PublishAt(new Event { Text = "event" }, DateTimeOffset.UtcNow.AddSeconds(3));
                        await producer.StopAsync();
                    }
                    Assert((await producerStore.ListPendingDispatches(DispatchKind.ScheduledSend)).Count == 1 &&
                        !gotEvent1.Task.IsCompleted && !gotEvent2.Task.IsCompleted,
                        "scheduled event remains in the database after stopping the producer");
                    using (var restartedProducer = RabbitEndpoint.Start(producerConfig))
                    {
                        Assert(await Within(gotEvent1.Task) == "event" && await Within(gotEvent2.Task) == "event",
                            "scheduled event reaches both subscribers after producer restart");
                        await restartedProducer.StopAsync();
                    }
                    await a.Send(new Failed());
                    Assert(await Within(gotCommand.Task) == "command", "command");
                    Assert(DateTimeOffset.UtcNow >= dueAt, "scheduled command delivered after due time");
                    Assert(await Within(gotReply.Task) == "command", "reply");
                    Assert(await Within(gotLocal.Task) == "command", "send local");
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(receiverName, true, false, false,
                            new System.Collections.Generic.Dictionary<string, object>
                            {
                                ["x-dead-letter-exchange"] = errorExchange,
                                ["x-dead-letter-routing-key"] = receiverName,
                                ["x-message-ttl"] = 120000
                            });
                        BasicGetResult failed = null;
                        for (int i = 0; i < 100 && failed == null; i++) { failed = channel.BasicGet(receiverName + ".error", true); if (failed == null) await Task.Delay(100); }
                        Assert(failed != null && attempts == 3, "retries and error queue");
                    }
                }
                Console.WriteLine("RabbitMQ integration tests passed.");
            }
            finally
            {
                if (File.Exists(dbPath)) File.Delete(dbPath);
                if (File.Exists(senderPath)) File.Delete(senderPath);
                if (File.Exists(subscriberPath)) File.Delete(subscriberPath);
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    foreach (var name in new[] { senderName, receiverName, subscriberName }) { channel.QueueDelete(name); channel.QueueDelete(name + ".error"); }
                    channel.ExchangeDelete(eventsExchange);
                    channel.ExchangeDelete(errorExchange);
                }
            }
        }
        static async Task<SagaStoredState<OrderState>> ReadOrder(SqlitePersistence store, string id)
        {
            using (var session = await store.OpenSession())
            {
                var record = await session.LoadSaga(typeof(OrderWorkflow).FullName, id);
                if (record == null) return null;
                var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
                return MessagePackSerializer.Deserialize<SagaStoredState<OrderState>>(record.Data, options);
            }
        }
        static async Task WaitFor(Func<Task<bool>> condition, string description)
        {
            for (var i = 0; i < 150; i++) { if (await condition()) return; await Task.Delay(100); }
            throw new Exception("Timed out: " + description);
        }
        static async Task SagaTests(Uri uri)
        {
            var suffix = Guid.NewGuid().ToString("N");
            var endpointName = "bq.test.saga." + suffix;
            var senderName = "bq.test.saga.sender." + suffix;
            var path = Path.Combine(Path.GetTempPath(), "bq-saga-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), endpointName);
            await store.Initialize();
            var missingCount = 0;
            var otherHandlerCount = 0;
            var config = new EndpointConfiguration(endpointName, uri) { Persistence = store };
            config.SagaNotFound = (sagaType, key, message, context) => { if (key == "missing") Interlocked.Increment(ref missingCount); return Task.CompletedTask; };
            config.Handle(() => new Handler<ChangeOrder>((message, context) => { Interlocked.Increment(ref otherHandlerCount); return Task.CompletedTask; }));
            config.RegisterSaga<OrderWorkflow, OrderState>(() => new OrderWorkflow(), mappings =>
            {
                mappings.Start<StartOrder>(m => m.OrderId);
                mappings.Handle<ChangeOrder>(m => m.OrderId);
                mappings.Timeout<OrderTimeout>();
            });
            var senderPath = Path.Combine(Path.GetTempPath(), "bq-saga-sender-" + suffix + ".db");
            var senderStore = new SqlitePersistence(() => Sqlite(senderPath), senderName); await senderStore.Initialize();
            var sender = new EndpointConfiguration(senderName, uri) { Persistence = senderStore };
            sender.Route<StartOrder>(endpointName);
            sender.Route<ChangeOrder>(endpointName);
            try
            {
                using (var a = RabbitEndpoint.Start(sender))
                {
                    using (var sagaEndpoint = RabbitEndpoint.Start(config))
                    {
                        await a.Send(new ChangeOrder { OrderId = "missing" });
                        await a.Send(new StartOrder { OrderId = "restart", DelayMs = 3000, FailFirst = true });
                        await WaitFor(async () => await ReadOrder(store, "restart") != null, "start saga");
                        Assert(OrderWorkflow.StartAttempts == 2, "handler retry starts new transaction");
                        Assert(await ReadOrder(store, "missing") == null && missingCount == 1, "non-start message invokes not-found callback");
                        Assert(otherHandlerCount == 1, "ordinary handler and saga share message");
                    }
                    // Timeout is stored in SQLite and is still due after endpoint restart.
                    using (var sagaEndpoint = RabbitEndpoint.Start(config))
                    {
                        await WaitFor(async () => (await ReadOrder(store, "restart"))?.Data.TimeoutCount == 1, "timeout after restart");
                        await Task.Delay(1500);
                        Assert((await ReadOrder(store, "restart")).Data.TimeoutCount == 1, "failed attempt did not leave an extra timeout");
                        await a.Send(new ChangeOrder { OrderId = "restart" });
                        await WaitFor(async () => (await ReadOrder(store, "restart"))?.Data.Status == "changed", "correlated update");
                        await a.Send(new StartOrder { OrderId = "reuse", DelayMs = 3500 });
                        await WaitFor(async () => await ReadOrder(store, "reuse") != null, "start old instance");
                        var original = (await ReadOrder(store, "reuse")).InstanceId;
                        await a.Send(new ChangeOrder { OrderId = "reuse", Complete = true });
                        await WaitFor(async () => await ReadOrder(store, "reuse") == null, "complete saga");
                        await a.Send(new StartOrder { OrderId = "reuse" });
                        await WaitFor(async () => { var state = await ReadOrder(store, "reuse"); return state != null && state.InstanceId != original; }, "start replacement instance");
                        await Task.Delay(5000);
                        var replacement = await ReadOrder(store, "reuse");
                        Assert(replacement != null && replacement.Data.TimeoutCount == 0, "old timeout ignored by replacement");
                        Assert(OrderWorkflow.TimeoutCalls == 1, "only committed valid timeout invoked");
                    }
                }
                Console.WriteLine("Saga lifecycle and durable timeout tests passed.");
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
                if (File.Exists(senderPath)) File.Delete(senderPath);
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                    foreach (var name in new[] { senderName, endpointName }) { channel.QueueDelete(name); channel.QueueDelete(name + ".error"); }
            }
        }
        static async Task RecoverabilityTests(Uri uri)
        {
            var suffix = Guid.NewGuid().ToString("N");
            var name = "bq.test.recover." + suffix;
            var errorName = name + ".custom-error";
            var path = Path.Combine(Path.GetTempPath(), "bq-recover-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();
            var faultStore = new FaultPersistence(store);
            var config = new EndpointConfiguration(name, uri) { Persistence = faultStore };
            config.ImmediateRetries = 0;
            config.Recoverability.DelayedRetries = new[] { TimeSpan.FromSeconds(3) };
            config.Recoverability.ErrorQueue = errorName;
            config.Recoverability.IsUnrecoverable = ex =>
            {
                if (ex.Message == "persistent failure") throw new InvalidOperationException("test classifier failure");
                return ex is FatalFailureException;
            };
            var attempts = 0;
            var successes = 0;
            var replayAllowed = false;
            var handoffAttempts = 0;
            var outboxProbe = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            config.MutateOutgoing = message =>
            {
                if (message.TypeName != typeof(LocalMessage).FullName) return;
                Assert(message.IncomingMessage != null, "outgoing mutator sees causing message");
                message.TypeName = "mutated.local";
                message.Headers["mutated"] = true;
                message.Body[0] ^= 0xff;
            };
            config.MutateIncoming = message =>
            {
                if (message.TypeName == "mutated.local")
                {
                    message.TypeName = typeof(LocalMessage).FullName;
                    message.Body[0] ^= 0xff;
                }
                if (message.Headers.ContainsKey("protected"))
                {
                    message.Body[0] ^= 0xff;
                    message.Headers.Remove("protected");
                }
            };
            config.Incoming.Use(async (context, next) =>
            {
                if (context.Message is Command) context.Items["stage"] = "before handler";
                await next();
                if (context.Message is Command)
                    Assert((string)context.Items["stage"] == "before commit", "before-commit callback runs within behavior scope");
            });
            config.Incoming.BeforeCommit((context, session) =>
            {
                if (context.Message is Command)
                {
                    Assert((string)context.Items["stage"] == "before handler", "behavior items available to callback");
                    Assert(session.Transaction != null, "before-commit callback has active transaction");
                    context.Items["stage"] = "before commit";
                }
                return Task.CompletedTask;
            });
            config.Handle(() => new Handler<Command>((message, context) =>
                context.SendLocal(new LocalMessage { Text = message.Text })));
            config.Handle(() => new Handler<LocalMessage>((message, context) =>
            {
                if (message.Text == "outbox probe")
                {
                    Assert(context.Headers.ContainsKey("mutated"), "mutated headers survive outbox dispatch");
                    outboxProbe.TrySetResult(true);
                }
                return Task.CompletedTask;
            }));
            var exhaustedAttempts = 0;
            string seenId = null, seenCorrelation = null, seenHeader = null;
            config.Handle(() => new Handler<RecoverCommand>((m, context) =>
            {
                Interlocked.Increment(ref attempts);
                if (m.Text == "retry" && Volatile.Read(ref attempts) == 1) throw new Exception("temporary failure");
                if (m.Text == "fatal" && !replayAllowed) throw new FatalFailureException("permanent failure");
                if (m.Text == "exhaust") { Interlocked.Increment(ref exhaustedAttempts); throw new Exception("persistent failure"); }
                if (m.Text == "handoff" && Interlocked.Increment(ref handoffAttempts) == 1) throw new Exception("recoverability storage unavailable");
                seenId = context.MessageId; seenCorrelation = context.CorrelationId;
                if (context.Headers.TryGetValue("custom", out var h)) seenHeader = System.Text.Encoding.UTF8.GetString((byte[])h);
                Interlocked.Increment(ref successes);
                return Task.CompletedTask;
            }));
            var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
            try
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    var body = MessagePackSerializer.Serialize(new RecoverCommand { Text = "retry" }, options);
                    var props = channel.CreateBasicProperties();
                    props.MessageId = "retry-" + suffix; props.Type = typeof(RecoverCommand).FullName; props.ContentType = "application/x-msgpack";
                    props.CorrelationId = "correlation"; props.Headers = new System.Collections.Generic.Dictionary<string, object> { ["custom"] = "hello" };
                    using (var endpoint = RabbitEndpoint.Start(config))
                    {
                        channel.BasicPublish("", name, props, body);
                        await WaitFor(async () => { using (var c = Sqlite(path)) { await c.OpenAsync(); using (var cmd = c.CreateCommand()) { cmd.CommandText = "SELECT COUNT(*) FROM bq_recovery WHERE cycle=0"; return Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1; } } }, "retry persisted");
                        // Duplicate the same delivery after the decision has committed. It must not run again.
                        channel.BasicPublish("", name, props, body);
                        await Task.Delay(400);
                        Assert(attempts == 1, "duplicate original skipped after retry decision");
                    }
                    await Task.Delay(3200);
                    using (var endpoint = RabbitEndpoint.Start(config))
                    {
                        await WaitFor(() => Task.FromResult(Volatile.Read(ref successes) == 1), "retry after restart");
                        Assert(attempts == 2 && seenId == props.MessageId && seenCorrelation == "correlation" && seenHeader == "hello", "retry retains identity and headers");
                        // Replay the successful retry cycle. Only the inbox can suppress this
                        // delivery, because this cycle did not create a recovery decision.
                        props.Headers["x-bq-retry-cycle"] = 1;
                        channel.BasicPublish("", name, props, body);
                        var fatalProps = channel.CreateBasicProperties();
                        fatalProps.MessageId = "fatal-" + suffix; fatalProps.Type = typeof(RecoverCommand).FullName; fatalProps.ContentType = "application/x-msgpack";
                        var protectedBody = MessagePackSerializer.Serialize(new RecoverCommand { Text = "fatal" }, options);
                        protectedBody[0] ^= 0xff;
                        fatalProps.Headers = new System.Collections.Generic.Dictionary<string, object> { ["protected"] = true };
                        channel.BasicPublish("", name, fatalProps, protectedBody);
                        await WaitFor(async () => (await endpoint.ListErrors()).Count == 1, "error recorded");
                        var errors = await endpoint.ListErrors();
                        Assert(errors[0].MessageId == fatalProps.MessageId && errors[0].ExceptionType.Contains("FatalFailureException"), "error metadata");
                        Assert(attempts == 3, "committed duplicate is skipped and fatal failure runs once");
                        BasicGetResult error = null;
                        await WaitFor(() => { error = channel.BasicGet(errorName, true); return Task.FromResult(error != null); }, "error queue delivery");
                        Assert(error.BasicProperties.MessageId == fatalProps.MessageId, "error queue identity");
                        Assert(error.BasicProperties.Headers.ContainsKey("protected") && error.Body.Span[0] == protectedBody[0],
                            "recovery preserves the original wire body and headers");
                        Assert(error.BasicProperties.Headers.ContainsKey("x-bq-failure-detail"), "error queue failure metadata");
                        replayAllowed = true;
                        Assert(await endpoint.ReplayError(errors[0].RecoveryId), "error replay scheduled");
                        Assert(!await endpoint.ReplayError(errors[0].RecoveryId), "error replay idempotent");
                        await WaitFor(() => Task.FromResult(Volatile.Read(ref successes) == 2), "error replay processed");
                        Assert(seenId != fatalProps.MessageId, "replay has fresh message ID");
                        faultStore.FailNextStore = 1;
                        var handoff = channel.CreateBasicProperties(); handoff.MessageId = "handoff-" + suffix; handoff.Type = typeof(RecoverCommand).FullName; handoff.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", name, handoff, MessagePackSerializer.Serialize(new RecoverCommand { Text = "handoff" }, options));
                        await WaitFor(() => Task.FromResult(Volatile.Read(ref successes) == 3), "failed recovery handoff requeued");
                        Assert(handoffAttempts == 2, "storage failure did not acknowledge delivery");
                        // An unavailable error queue must leave the database handoff pending.
                        replayAllowed = false;
                        channel.QueueDelete(errorName);
                        var missingError = channel.CreateBasicProperties(); missingError.MessageId = "missing-error-" + suffix; missingError.Type = typeof(RecoverCommand).FullName; missingError.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", name, missingError, MessagePackSerializer.Serialize(new RecoverCommand { Text = "fatal" }, options));
                        await WaitFor(async () => (await endpoint.ListErrors()).Count == 1, "error retained while destination unavailable");
                        await Task.Delay(1500);
                        Assert(await store.NextRecovery(name) != null, "unroutable error remains pending");
                        await endpoint.SendLocal(new Command { Text = "outbox probe" });
                        await WaitFor(() => Task.FromResult(outboxProbe.Task.IsCompleted),
                            "ordinary outbox dispatch while error queue is missing");
                        channel.QueueDeclare(errorName, true, false, false);
                        BasicGetResult restored = null;
                        await WaitFor(() => { restored = channel.BasicGet(errorName, true); return Task.FromResult(restored != null); }, "pending error delivered after destination restored");
                        Assert(restored.BasicProperties.MessageId == missingError.MessageId, "restored error retains identity");
                        var exhausted = channel.CreateBasicProperties(); exhausted.MessageId = "exhaust-" + suffix; exhausted.Type = typeof(RecoverCommand).FullName; exhausted.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", name, exhausted, MessagePackSerializer.Serialize(new RecoverCommand { Text = "exhaust" }, options));
                        await WaitFor(async () => (await endpoint.ListErrors()).Count == 2, "delayed retries exhaust even when the classifier throws");
                        Assert(exhaustedAttempts == 2, "one immediate cycle plus one delayed cycle");
                    }
                }
                Console.WriteLine("Durable recoverability and replay tests passed.");
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel()) { channel.QueueDelete(name); channel.QueueDelete(errorName); }
            }
        }
        static async Task BrokerOutageTest(Uri uri, string directory)
        {
            var suffix = Guid.NewGuid().ToString("N");
            var name = "bq.test.outage." + suffix;
            var path = Path.Combine(Path.GetTempPath(), "bq-outage-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();
            var config = new EndpointConfiguration(name, uri) { Persistence = store, ImmediateRetries = 0 };
            config.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(250);
            config.Recoverability.DelayedRetries = new[] { TimeSpan.FromSeconds(3) };
            var calls = 0;
            config.Handle(() => new Handler<RecoverCommand>((message, context) =>
            {
                if (Interlocked.Increment(ref calls) == 1) throw new Exception("test broker interruption");
                return Task.CompletedTask;
            }));
            var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
            try
            {
                using (var endpoint = RabbitEndpoint.Start(config))
                {
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        var props = channel.CreateBasicProperties(); props.MessageId = suffix; props.Type = typeof(RecoverCommand).FullName; props.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", name, props, MessagePackSerializer.Serialize(new RecoverCommand { Text = "outage" }, options));
                    }
                    await WaitFor(async () => { using (var c = Sqlite(path)) { await c.OpenAsync(); using (var cmd = c.CreateCommand()) { cmd.CommandText = "SELECT COUNT(*) FROM bq_recovery"; return Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1; } } }, "outage retry scheduled");
                    File.WriteAllText(Path.Combine(directory, "ready"), "ready");
                    await WaitFor(() => Task.FromResult(File.Exists(Path.Combine(directory, "restarted"))), "broker restart");
                    await WaitFor(() => Task.FromResult(Volatile.Read(ref calls) == 2), "retry after broker restart");
                }
                Console.WriteLine("Broker interruption test passed.");
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel()) { channel.QueueDelete(name); channel.QueueDelete(name + ".error"); }
            }
        }
        static async Task<T> Within<T>(Task<T> task)
        {
            if (await Task.WhenAny(task, Task.Delay(10000)) != task) throw new Exception("Timed out waiting for delivery.");
            return await task;
        }
        static void Assert(bool value, string name) { if (!value) throw new Exception("Failed: " + name); }
    }
}
