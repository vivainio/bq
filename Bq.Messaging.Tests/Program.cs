using System;
using System.Threading.Tasks;
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
    public class Response { public string Text { get; set; } }
    public class Failed { public string Text { get; set; } }
    public class OldContract { public string Text { get; set; } }
    public class NewContract { public string Text { get; set; } public int Added { get; set; } }
    public class OrderSaga { public string Status { get; set; } }
    class Handler<T> : IHandleMessages<T>
    {
        readonly Func<T, IMessageHandlerContext, Task> action;
        public Handler(Func<T, IMessageHandlerContext, Task> action) { this.action = action; }
        public Task Handle(T value, IMessageHandlerContext context) => action(value, context);
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
                Assert((await store.NextOutgoing("test")).Id == "sent", "outbox commit and rollback");
                await store.MarkDispatched("test", "sent");
                Assert(await store.NextOutgoing("test") == null, "outbox dispatch");
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
            var attempts = 0;
            var sender = new EndpointConfiguration(senderName, uri);
            sender.Route<Command>(receiverName);
            sender.Route<Failed>(receiverName);
            sender.Handle(() => new Handler<Response>((m, c) => { gotReply.TrySetResult(m.Text); return Task.CompletedTask; }));
            var dbPath = Path.Combine(Path.GetTempPath(), "bq-broker-" + suffix + ".db");
            var store = new SqlitePersistence(() => Sqlite(dbPath), receiverName);
            await store.Initialize();
            var receiver = new EndpointConfiguration(receiverName, uri) { Persistence = store };
            receiver.Handle(() => new Handler<Command>(async (m, c) => { gotCommand.TrySetResult(m.Text); await c.Reply(new Response { Text = m.Text }); }));
            receiver.Handle(() => new Handler<Event>((m, c) => { gotEvent1.TrySetResult(m.Text); return Task.CompletedTask; }), true);
            receiver.Handle(() => new Handler<Failed>((m, c) => { attempts++; throw new Exception("expected"); }));
            var subscriber = new EndpointConfiguration(subscriberName, uri);
            subscriber.Handle(() => new Handler<Event>((m, c) => { gotEvent2.TrySetResult(m.Text); return Task.CompletedTask; }), true);
            try
            {
                using (var a = RabbitEndpoint.Start(sender))
                using (var b = RabbitEndpoint.Start(receiver))
                using (var c = RabbitEndpoint.Start(subscriber))
                {
                    await a.Send(new Command { Text = "command" });
                    await a.Publish(new Event { Text = "event" });
                    await a.Send(new Failed());
                    Assert(await Within(gotCommand.Task) == "command", "command");
                    Assert(await Within(gotReply.Task) == "command", "reply");
                    Assert(await Within(gotEvent1.Task) == "event" && await Within(gotEvent2.Task) == "event", "event fanout");
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
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
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                    foreach (var name in new[] { senderName, receiverName, subscriberName }) { channel.QueueDelete(name); channel.QueueDelete(name + ".error"); }
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
