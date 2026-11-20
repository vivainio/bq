using System;
using System.Threading.Tasks;
using System.Threading;
using System.Data.Common;
using System.IO;
using System.Linq;
using Bq.Persistence.Sqlite;
using Bq.Messaging;
using Bq.Testing;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public class Command : ICommand { public string Text { get; set; } }
    public class Event : IEvent { public string Text { get; set; } }
    public class LocalMessage { public string Text { get; set; } }
    public class Response { public string Text { get; set; } }
    public class Failed { public string Text { get; set; } }
    public class RecoverCommand { public string Text { get; set; } }
    public class FatalFailureException : Exception { public FatalFailureException(string text) : base(text) { } }
    public class OldContract { public string Text { get; set; } }
    public class NewContract { public string Text { get; set; } public int Added { get; set; } }
    public class OrderSaga { public string Status { get; set; } }
    public class StartOrder { public string OrderId { get; set; } public int DelayMs { get; set; } public DateTimeOffset? TimeoutAt { get; set; } public bool FailFirst { get; set; } }
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
            if (message.TimeoutAt.HasValue) await RequestTimeout(message.TimeoutAt.Value, new OrderTimeout { OrderId = message.OrderId });
            else if (message.DelayMs > 0) await RequestTimeout(TimeSpan.FromMilliseconds(message.DelayMs), new OrderTimeout { OrderId = message.OrderId });
            else if (message.OrderId == "empty-timeout") await RequestTimeout<OrderTimeout>(context, TimeSpan.Zero);
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
    class FaultPersistence : IPersistenceProvider, IRecoveryPersistence, ILockedDispatchPersistence
    {
        readonly SqlitePersistence inner;
        public int FailNextStore;
        public Task OutgoingGate { get; set; }
        public FaultPersistence(SqlitePersistence inner) { this.inner = inner; }
        public async Task<IPersistenceSession> OpenSession() => new FaultSession(await inner.OpenSession(), this);
        Task<OutgoingMessage> IPersistenceProvider.NextOutgoing(string endpoint) =>
            throw new InvalidOperationException("Expected the provider-controlled outgoing dispatch path.");
        Task IPersistenceProvider.MarkDispatched(string endpoint, string id) =>
            throw new InvalidOperationException("Expected the provider to record outgoing dispatch.");
        Task<RecoveryRecord> IRecoveryPersistence.NextRecovery(string endpoint) =>
            throw new InvalidOperationException("Expected the provider-controlled recovery dispatch path.");
        Task IRecoveryPersistence.MarkRecoveryDispatched(string endpoint, string id) =>
            throw new InvalidOperationException("Expected the provider to record recovery dispatch.");

        // Exercise the endpoint contract with SQLite and a single dispatcher per stream.
        public async Task<bool> DispatchOutgoing(string endpoint, Action<OutgoingMessage> publish)
        {
            if (OutgoingGate != null) await OutgoingGate;
            var message = await inner.NextOutgoing(endpoint);
            if (message == null) return false;
            publish(message);
            await inner.MarkDispatched(endpoint, message.Id);
            return true;
        }

        public async Task<bool> DispatchRecovery(string endpoint, Action<RecoveryRecord> publish)
        {
            var record = await inner.NextRecovery(endpoint);
            if (record == null) return false;
            publish(record);
            await inner.MarkRecoveryDispatched(endpoint, record.RecoveryId);
            return true;
        }

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
    partial class Program
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
            await TestingSupportTests();
            await PersistenceTests();
            await OraclePersistenceTests();
            var uri = Environment.GetEnvironmentVariable("BQ_RABBITMQ_URI");
            if (string.IsNullOrEmpty(uri)) { Console.WriteLine("RabbitMQ tests skipped: set BQ_RABBITMQ_URI."); return; }
            await BrokerTests(new Uri(uri));
            await SagaTests(new Uri(uri));
            await RecoverabilityTests(new Uri(uri));
            await StalledDispatcherTest(new Uri(uri));
            await MigrationPipelineTests(new Uri(uri));
            await PhasedMigrationTests(new Uri(uri));
            await OrderWorkflowEndToEndTest(new Uri(uri));
            await LifecycleTests.Run(new Uri(uri));
            await UniformSessionTests.Run(new Uri(uri), Sqlite);
            var outageDirectory = Environment.GetEnvironmentVariable("BQ_BROKER_OUTAGE_DIR");
            if (!string.IsNullOrEmpty(outageDirectory)) await BrokerOutageTest(new Uri(uri), outageDirectory);
        }
        static async Task TestingSupportTests()
        {
            IMessageCreator creator = new MessageCreator();
            Assert(creator.CreateInstance<Command>() != null && creator.CreateInstance(typeof(IPilotEvent)) is IPilotEvent,
                "message creator supports concrete classes and interface contracts");
            var sendOptions = new SendOptions();
            sendOptions.GetExtensions().Set("operation", "checkout");
            Assert(sendOptions.GetExtensions().Get<string>("operation") == "checkout" &&
                !new SendOptions().GetExtensions().TryGet<string>("operation", out _),
                "extension data belongs to one options instance");
            var subscriptionOptions = new SubscribeOptions();
            subscriptionOptions.GetExtensions().Set(42);
            Assert(subscriptionOptions.GetExtensions().Get<int>() == 42, "subscription options expose the same extension API");
            Assert(sendOptions.GetDestination() == null && !sendOptions.IsRoutingToThisEndpoint() && sendOptions.GetMessageId() == null,
                "unset send options report no explicit routing or identity");
            sendOptions.RouteToThisEndpoint();
            sendOptions.SetMessageId("custom-id");
            sendOptions.RouteReplyTo("reply");
            sendOptions.SetHeader("attempt", 3);
            sendOptions.DelayDeliveryWith(TimeSpan.FromSeconds(2));
            Assert(sendOptions.IsRoutingToThisEndpoint() && sendOptions.GetMessageId() == "custom-id" && sendOptions.GetReplyToRoute() == "reply" &&
                sendOptions.GetHeaders()["attempt"] == "3" && sendOptions.GetDeliveryDelay() == TimeSpan.FromSeconds(2) && sendOptions.GetDeliveryDate() == null,
                "send option readers expose explicitly configured values");
            var deliveryDate = DateTimeOffset.UtcNow.AddMinutes(1);
            sendOptions.DoNotDeliverBefore(deliveryDate);
            Assert(sendOptions.GetDeliveryDelay() == null && sendOptions.GetDeliveryDate() == deliveryDate,
                "absolute scheduling replaces the relative delay");
            var replyOptions = new ReplyOptions();
            replyOptions.SetDestination("receiver");
            replyOptions.RouteReplyTo("return");
            Assert(replyOptions.GetDestination() == "receiver" && replyOptions.GetReplyToRoute() == "return",
                "reply readers distinguish destination and return address");

            var configuration = new EndpointConfiguration("config-test", new Uri("amqp://localhost"));
            configuration.OverridePublicReturnAddress(new string('é', 127) + "a");
            var oversizedAddressRejected = false;
            try { configuration.OverridePublicReturnAddress(new string('é', 128)); }
            catch (ArgumentException error) when (error.ParamName == "address") { oversizedAddressRejected = true; }
            Assert(oversizedAddressRejected, "public return address validates the AMQP field size in UTF-8 bytes");
            configuration.LimitMessageProcessingConcurrencyTo(4);
            configuration.SendFailedMessagesTo("config-test.error");
            configuration.Recoverability()
                .Immediate(retries => retries.NumberOfRetries(1))
                .Delayed(retries => retries.NumberOfRetries(2).TimeIncrease(TimeSpan.FromSeconds(3)));
            Assert(configuration.MaxConcurrency == 4 && configuration.Recoverability.ErrorQueue == "config-test.error" &&
                configuration.ImmediateRetries == 1 &&
                configuration.Recoverability.DelayedRetries.SequenceEqual(new[] { TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(6) }),
                "fluent configuration applies concurrency, error queue, and retry settings");

            var context = new TestableMessageHandlerContext();
            IMessageHandlerContext handler = context;
            handler.Extensions.Set("shared", 4);
            var child = new ContextBag(handler.Extensions);
            child.Set("shared", 5);
            Assert(child.Get<int>("shared") == 5 && handler.Extensions.Get<int>("shared") == 4, "child bag shadows parent values");
            child.Remove("shared");
            Assert(child.Get<int>("shared") == 4 && ReferenceEquals(child.GetOrCreate<Command>(), child.Get<Command>()),
                "context bag supports inherited values and typed creation");
            var dueAt = DateTimeOffset.UtcNow.AddMinutes(5);
            var send = new SendOptions();
            send.DoNotDeliverBefore(dueAt);
            send.SetHeader("tenant", "one");
            await handler.Send("orders", new Command(), send);
            var reply = new ReplyOptions();
            reply.SetHeader("tenant", "two");
            await handler.Reply(new Response(), reply);
            await handler.Publish(new Event());
            context.Headers["text-bytes"] = System.Text.Encoding.UTF8.GetBytes("café");
            context.Headers["attempt"] = 3;
            context.Headers["nested"] = new System.Collections.Generic.Dictionary<string, object>();
            context.ReplyToAddress = "orders.reply";
            Assert(handler.MessageHeaders["text-bytes"] == "café" && handler.GetHeader("attempt") == "3" &&
                !handler.MessageHeaders.ContainsKey("nested") && handler.ReplyToAddress == "orders.reply",
                "handler test context exposes textual headers and the reply address");
            var messages = context.Messages;
            Assert(messages.Count == 3 && messages[0].Destination == "orders" && messages[0].DueAt == dueAt &&
                (string)messages[0].Headers["tenant"] == "one", "handler testing captures scheduled send and headers");
            Assert(messages[1].Operation == MessageOperation.Reply && (string)messages[1].Headers["tenant"] == "two" &&
                messages[2].Operation == MessageOperation.Publish && messages[2].Headers.Count == 0, "handler testing captures reply and publish");

            send.SetDestination("orders");
            var initialized = new TestableMessageHandlerContext();
            var pipelineRecorder = new TestableMessageHandlerContext();
            IPipelineContext pipeline = pipelineRecorder;
            await pipeline.Send<Command>("orders", command => command.Text = "direct");
            await pipeline.SendLocal<Command>(command => command.Text = "local");
            Assert(pipelineRecorder.Messages[0].Destination == "orders" &&
                ((Command)pipelineRecorder.Messages[0].Message).Text == "direct" &&
                pipelineRecorder.Messages[1].Operation == MessageOperation.SendLocal &&
                ((Command)pipelineRecorder.Messages[1].Message).Text == "local",
                "pipeline send shortcuts preserve destination and initializer");
            IMessageHandlerContext initializerContext = initialized;
            await initializerContext.Send<Command>(command => command.Text = "created", send);
            Action<IPilotEvent> publishMessage = message => message.Text = "published";
            await initializerContext.Publish(publishMessage);
            await initializerContext.Reply<Response>(response => response.Text = "replied", reply);
            Assert(((Command)initialized.Messages[0].Message).Text == "created" &&
                initialized.Messages[0].Destination == "orders" && initialized.Messages[0].DueAt == dueAt, "message initializer preserves send options");
            Assert(((IPilotEvent)initialized.Messages[1].Message).Text == "published" &&
                ((Response)initialized.Messages[2].Message).Text == "replied" &&
                (string)initialized.Messages[2].Headers["tenant"] == "two", "publish and reply initializers populate their contracts");

            var saga = new SagaTest<OrderWorkflow, OrderState>(new OrderWorkflow());
            await saga.Handle(new StartOrder { OrderId = "unit", TimeoutAt = dueAt });
            Assert(saga.Data.Status == "started" && saga.Timeouts.Count == 1 && saga.Timeouts[0].DueAt == dueAt,
                "saga testing captures state and timeout");
            await saga.Timeout((OrderTimeout)saga.Timeouts[0].State);
            await saga.Handle(new ChangeOrder { OrderId = "unit", Complete = true });
            Assert(saga.Completed && saga.Data.TimeoutCount == 1, "saga testing invokes timeout and exposes completion");
            var emptyTimeout = new SagaTest<OrderWorkflow, OrderState>(new OrderWorkflow());
            await emptyTimeout.Handle(new StartOrder { OrderId = "empty-timeout" });
            Assert(emptyTimeout.Timeouts.Count == 1 && emptyTimeout.Timeouts[0].State is OrderTimeout,
                "timeout overload constructs its state");
            OrderWorkflow.TimeoutCalls = 0;
            Console.WriteLine("Transport-free handler and saga tests passed.");
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
            sender.Handle(() => new Handler<Response>((m, c) =>
            {
                Assert(c.Headers.ContainsKey("reply-option"), "reply carries option headers");
                gotReply.TrySetResult(m.Text);
                return Task.CompletedTask;
            }));
            var dbPath = Path.Combine(Path.GetTempPath(), "bq-broker-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(dbPath), receiverName);
            await store.Initialize();
            var eventsExchange = "bq.test.events." + suffix;
            var errorExchange = "bq.test.errors." + suffix;
            var receiver = new EndpointConfiguration(receiverName, uri)
                { Persistence = store, EventsExchange = eventsExchange, ErrorExchange = errorExchange };
            receiver.InputQueueArguments["x-message-ttl"] = 120000;
            receiver.Incoming.Use((context, next) =>
            {
                if (context.Message is Command command && command.Text == "command")
                {
                    Assert(context.IsDeferred && !context.IsSagaTimeout, "scheduled command metadata");
                    Assert(context.ContractName == typeof(Command).FullName, "incoming contract name");
                    Assert(DateTimeOffset.TryParse(context.HandlerContext.GetHeader(MessageHeaders.TimeSent), out _),
                        "scheduled send retains time sent");
                }
                return next();
            });
            receiver.Handle(() => new Handler<Command>(async (m, c) =>
            {
                if (m.Text == "command") Assert(c.Headers.ContainsKey("scheduled-option"), "scheduled send carries option headers");
                gotCommand.TrySetResult(m.Text);
                var options = new ReplyOptions();
                options.SetHeader("reply-option", true);
                object response = new Response { Text = m.Text };
                await c.Reply(response, options);
                await c.SendLocal(new LocalMessage { Text = m.Text });
            }));
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
                        var options = new SendOptions();
                        options.DoNotDeliverBefore(dueAt);
                        options.SetHeader("scheduled-option", true);
                        await a.CreateUniformSession().SendWithOptions(new Command { Text = "command" }, options);
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
                    producerConfig.DisableDurableMessages();
                    producerConfig.OverridePublicReturnAddress(producerName + ".public");
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
                                IMessageSession session = producer;
                                await session.SendLocal<LocalMessage>(message => message.Text = "shared endpoint");
                                var local = queueOwner.BasicGet(producerName, true);
                                Assert(local != null && local.BasicProperties.ReplyTo == producerName + ".public" &&
                                    local.BasicProperties.DeliveryMode == 1,
                                    "send-only local send advertises the configured return address while routing to the named queue");
                                var delay = TimeSpan.FromMilliseconds(500);
                                var options = new SendOptions();
                                options.DelayDeliveryWith(delay);
                                options.SetHeader("relative-delay", true);
                                var requestedAt = DateTimeOffset.UtcNow;
                                options.RouteToThisEndpoint();
                                options.SetMessageId("scheduled-local-id");
                                options.RouteReplyTo(receiverName);
                                await producer.CreateUniformSession().Send(new LocalMessage { Text = "scheduled local" }, options);
                                BasicGetResult scheduled = null;
                                await WaitFor(() =>
                                {
                                    scheduled = queueOwner.BasicGet(producerName, true);
                                    return Task.FromResult(scheduled != null);
                                }, "send-only scheduled local send reaches the existing named queue");
                                Assert(DateTimeOffset.UtcNow >= requestedAt.Add(delay) &&
                                    Equals(scheduled.BasicProperties.Headers["relative-delay"], true),
                                    "relative delivery delay and custom header survive durable scheduling");
                                Assert(scheduled.BasicProperties.MessageId == "scheduled-local-id" &&
                                    scheduled.BasicProperties.CorrelationId == "scheduled-local-id" &&
                                    scheduled.BasicProperties.ReplyTo == receiverName && scheduled.BasicProperties.DeliveryMode == 1 &&
                                    !scheduled.BasicProperties.Headers.ContainsKey("x-bq-override-reply-to"),
                                    "explicit wire identity and reply address survive scheduling on a send-only endpoint");
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
            config.Incoming.Use((incoming, next) =>
            {
                if (incoming.Message is SagaTimeoutMessage)
                {
                    Assert(incoming.IsSagaTimeout && incoming.IsDeferred, "saga timeout is classified as deferred");
                    Assert(incoming.ContractName == typeof(SagaTimeoutMessage).FullName &&
                        incoming.ImmediateRetries == 0 && incoming.DelayedRetries == 0,
                        "fresh saga timeout carries its contract and zero retry counts");
                    Assert(DateTimeOffset.TryParse(incoming.HandlerContext.GetHeader(MessageHeaders.TimeSent), out _),
                        "saga timeout retains the time it was requested");
                }
                return next();
            });
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
                        var timeoutAt = DateTimeOffset.UtcNow.AddSeconds(3).ToOffset(TimeSpan.FromHours(2));
                        await a.Send(new StartOrder { OrderId = "restart", TimeoutAt = timeoutAt, FailFirst = true });
                        await WaitFor(async () => await ReadOrder(store, "restart") != null, "start saga");
                        Assert(OrderWorkflow.StartAttempts == 2, "handler retry starts new transaction");
                        var pending = await store.ListPendingDispatches(DispatchKind.SagaTimeout);
                        Assert(pending.Count == 1 && pending[0].DueAtUtc == timeoutAt.UtcDateTime, "absolute timeout preserves due time across retry");
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
            var localAddress = name + ".input";
            var errorName = name + ".custom-error";
            var path = Path.Combine(Path.GetTempPath(), "bq-recover-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();
            var faultStore = new FaultPersistence(store);
            var config = new EndpointConfiguration(name, uri) { Persistence = faultStore };
            config.OverrideLocalAddress(localAddress);
            var criticalError = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            config.CriticalErrorThreshold = TimeSpan.FromSeconds(1);
            config.OnCriticalError = (source, error) =>
            {
                criticalError.TrySetResult(source);
                return Task.CompletedTask;
            };
            config.SendFailedMessagesTo(errorName);
            var delayedNotification = new TaskCompletionSource<DelayedRetryMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            var delayedNotifications = 0;
            var failedNotification = new TaskCompletionSource<FailedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            config.Recoverability()
                .Failed(failed => failed.HeaderCustomization(headers =>
                {
                    headers.Remove("remove-on-error");
                    headers["error-owner"] = "operations";
                }))
                .Failed(failed => failed.OnMessageSentToErrorQueue(message =>
                {
                    if (message.Exception.Message != "permanent failure") return Task.CompletedTask;
                    failedNotification.TrySetResult(message);
                    throw new InvalidOperationException("test error queue notification failure");
                }))
                .Immediate(retries => retries.NumberOfRetries(0))
                .Delayed(retries => retries.NumberOfRetries(1).TimeIncrease(TimeSpan.FromSeconds(3)))
                .Delayed(retries => retries.OnMessageBeingRetried(retry =>
                {
                    if (retry.Exception.Message != "temporary failure") return Task.CompletedTask;
                    Interlocked.Increment(ref delayedNotifications);
                    delayedNotification.TrySetResult(retry);
                    throw new InvalidOperationException("test delayed notification failure");
                }))
                .AddUnrecoverableException<FatalFailureException>();
            config.Recoverability.IsUnrecoverable = ex =>
            {
                if (ex.Message == "persistent failure") throw new InvalidOperationException("test classifier failure");
                return false;
            };
            var policyCalls = 0;
            config.Recoverability.Policy = context =>
            {
                if (context.Exception.Message != "temporary failure") return null;
                Assert(context.ImmediateRetries == 0 && context.DelayedRetries == 0, "initial retry policy counters");
                Assert(context.Headers.ContainsKey("custom"), "retry policy sees incoming headers");
                Interlocked.Increment(ref policyCalls);
                return RecoveryAction.DelayedRetry(TimeSpan.FromSeconds(2));
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
                if (context.Message is RecoverCommand retry && retry.Text == "retry")
                {
                    var expectedCycle = Volatile.Read(ref attempts) == 0 ? 0 : 1;
                    Assert(context.ImmediateRetries == 0 && context.DelayedRetries == expectedCycle &&
                        context.IsDeferred == (expectedCycle == 1) && !context.IsSagaTimeout,
                        "recovery cycle populates incoming retry and deferred metadata");
                }
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
                        channel.BasicPublish("", localAddress, props, body);
                        await WaitFor(async () => { using (var c = Sqlite(path)) { await c.OpenAsync(); using (var cmd = c.CreateCommand()) { cmd.CommandText = "SELECT COUNT(*) FROM bq_recovery WHERE cycle=0"; return Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1; } } }, "retry persisted");
                        var notification = await Within(delayedNotification.Task);
                        Assert(notification.MessageId == props.MessageId && notification.RetryAttempt == 1 &&
                            notification.Headers["custom"] == "hello" && notification.Body.SequenceEqual(body),
                            "delayed retry notification identifies the failed delivery");
                        // Duplicate the same delivery after the decision has committed. It must not run again.
                        channel.BasicPublish("", localAddress, props, body);
                        await Task.Delay(400);
                        Assert(attempts == 1 && policyCalls == 1 && delayedNotifications == 1,
                            "duplicate original skipped after retry decision");
                    }
                    await Task.Delay(3200);
                    using (var endpoint = RabbitEndpoint.Start(config))
                    {
                        await WaitFor(() => Task.FromResult(Volatile.Read(ref successes) == 1), "retry after restart");
                        Assert(attempts == 2 && seenId == props.MessageId && seenCorrelation == "correlation" && seenHeader == "hello", "retry retains identity and headers");
                        // Replay the successful retry cycle. Only the inbox can suppress this
                        // delivery, because this cycle did not create a recovery decision.
                        props.Headers["x-bq-retry-cycle"] = 1;
                        channel.BasicPublish("", localAddress, props, body);
                        var fatalProps = channel.CreateBasicProperties();
                        fatalProps.MessageId = "fatal-" + suffix; fatalProps.Type = typeof(RecoverCommand).FullName; fatalProps.ContentType = "application/x-msgpack";
                        var protectedBody = MessagePackSerializer.Serialize(new RecoverCommand { Text = "fatal" }, options);
                        protectedBody[0] ^= 0xff;
                        fatalProps.Headers = new System.Collections.Generic.Dictionary<string, object>
                        {
                            ["protected"] = true,
                            ["remove-on-error"] = "original"
                        };
                        channel.BasicPublish("", localAddress, fatalProps, protectedBody);
                        await WaitFor(async () => (await endpoint.ListErrors()).Count == 1, "error recorded");
                        var errors = await endpoint.ListErrors();
                        Assert(errors[0].MessageId == fatalProps.MessageId && errors[0].ExceptionType.Contains("FatalFailureException"), "error metadata");
                        Assert(attempts == 3, "committed duplicate is skipped and fatal failure runs once");
                        BasicGetResult error = null;
                        await WaitFor(() => { error = channel.BasicGet(errorName, true); return Task.FromResult(error != null); }, "error queue delivery");
                        var failedMessage = await Within(failedNotification.Task);
                        Assert(failedMessage.MessageId == fatalProps.MessageId && failedMessage.ErrorQueue == errorName &&
                            failedMessage.Body.SequenceEqual(protectedBody) &&
                            (string)failedMessage.Exception.Data["ExceptionType"] == typeof(FatalFailureException).FullName,
                            "error queue notification carries the published message and persisted failure");
                        Assert(error.BasicProperties.MessageId == fatalProps.MessageId, "error queue identity");
                        Assert(error.BasicProperties.Headers["protected"] is bool protectedHeader && protectedHeader &&
                            error.Body.Span[0] == protectedBody[0],
                            "recovery preserves the original wire body and headers");
                        Assert(failedMessage.Headers["error-owner"] == "operations" &&
                            System.Text.Encoding.UTF8.GetString((byte[])error.BasicProperties.Headers["error-owner"]) == "operations" &&
                            !error.BasicProperties.Headers.ContainsKey("remove-on-error"),
                            "error header customization is persisted and published");
                        Assert(error.BasicProperties.Headers.ContainsKey("x-bq-failure-detail"), "error queue failure metadata");
                        replayAllowed = true;
                        Assert(await endpoint.ReplayError(errors[0].RecoveryId), "error replay scheduled");
                        Assert(!await endpoint.ReplayError(errors[0].RecoveryId), "error replay idempotent");
                        await WaitFor(() => Task.FromResult(Volatile.Read(ref successes) == 2), "error replay processed");
                        Assert(seenId != fatalProps.MessageId, "replay has fresh message ID");
                        faultStore.FailNextStore = 1;
                        var handoff = channel.CreateBasicProperties(); handoff.MessageId = "handoff-" + suffix; handoff.Type = typeof(RecoverCommand).FullName; handoff.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", localAddress, handoff, MessagePackSerializer.Serialize(new RecoverCommand { Text = "handoff" }, options));
                        await WaitFor(() => Task.FromResult(Volatile.Read(ref successes) == 3), "failed recovery handoff requeued");
                        Assert(handoffAttempts == 2, "storage failure did not acknowledge delivery");
                        // An unavailable error queue must leave the database handoff pending.
                        replayAllowed = false;
                        channel.QueueDelete(errorName);
                        var missingError = channel.CreateBasicProperties(); missingError.MessageId = "missing-error-" + suffix; missingError.Type = typeof(RecoverCommand).FullName; missingError.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", localAddress, missingError, MessagePackSerializer.Serialize(new RecoverCommand { Text = "fatal" }, options));
                        await WaitFor(async () => (await endpoint.ListErrors()).Count == 1, "error retained while destination unavailable");
                        await Task.Delay(1500);
                        Assert(await store.NextRecovery(name) != null, "unroutable error remains pending");
                        Assert(await Within(criticalError.Task) == "Recovery dispatcher", "sustained dispatch failure invokes critical error callback");
                        await endpoint.SendLocal(new Command { Text = "outbox probe" });
                        await WaitFor(() => Task.FromResult(outboxProbe.Task.IsCompleted),
                            "ordinary outbox dispatch while error queue is missing");
                        channel.QueueDeclare(errorName, true, false, false);
                        BasicGetResult restored = null;
                        await WaitFor(() => { restored = channel.BasicGet(errorName, true); return Task.FromResult(restored != null); }, "pending error delivered after destination restored");
                        Assert(restored.BasicProperties.MessageId == missingError.MessageId, "restored error retains identity");
                        await WaitFor(async () => (await endpoint.ListErrors()).Single().Dispatched,
                            "provider records dispatch after the broker accepts the recovery message");
                        var exhausted = channel.CreateBasicProperties(); exhausted.MessageId = "exhaust-" + suffix; exhausted.Type = typeof(RecoverCommand).FullName; exhausted.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", localAddress, exhausted, MessagePackSerializer.Serialize(new RecoverCommand { Text = "exhaust" }, options));
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
                using (var channel = connection.CreateModel()) { channel.QueueDelete(localAddress); channel.QueueDelete(errorName); }
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
            config.CriticalErrorThreshold = TimeSpan.FromMilliseconds(250);
            var criticalError = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            config.OnCriticalError = (source, error) =>
            {
                criticalError.TrySetResult(source);
                return Task.CompletedTask;
            };
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
                    Assert(await Within(criticalError.Task) == "RabbitMQ connection", "sustained broker outage invokes the critical error callback");
                    File.WriteAllText(Path.Combine(directory, "critical-error"), "observed");
                    await WaitFor(() => Task.FromResult(File.Exists(Path.Combine(directory, "restarted"))), "broker restart");
                    await WaitFor(() => Task.FromResult(Volatile.Read(ref calls) == 2), "retry after broker restart");
                }
                Console.WriteLine("Broker interruption and critical-error test passed.");
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
