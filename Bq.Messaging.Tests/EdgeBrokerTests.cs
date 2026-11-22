using System;
using Bq.Messaging.MultiTenant;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging.Features;
using Bq.Messaging.UnitOfWork;
using Bq.Persistence.Sqlite;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public class EdgeUowMessage { public string Text { get; set; } }

    [TimeToBeReceived("00:00:30")]
    public class EdgeTtlMessage { public string Text { get; set; } }

    public sealed class EdgeReceiveFailure : Behavior<ITransportReceiveContext>
    {
        public static int Seen;
        public override Task Invoke(ITransportReceiveContext context, Func<Task> next)
        {
            if (!context.Message.Headers.ContainsKey("edge-receive-fail")) return next();
            Interlocked.Increment(ref Seen);
            throw new InvalidOperationException("receive behavior failed");
        }
    }

    public sealed class EdgeAlwaysAbort : Behavior<ITransportReceiveContext>
    {
        public static int Seen;
        public override Task Invoke(ITransportReceiveContext context, Func<Task> next)
        {
            if (!context.Message.Headers.ContainsKey("edge-always-abort")) return next();
            Interlocked.Increment(ref Seen);
            context.AbortReceiveOperation();
            return Task.CompletedTask;
        }
    }

    public sealed class EdgeConcurrencyProbe : Behavior<IInvokeHandlerContext>
    {
        public static int Current, Peak, Invoked;
        public override async Task Invoke(IInvokeHandlerContext context, Func<Task> next)
        {
            var now = Interlocked.Increment(ref Current);
            int peak;
            while ((peak = Volatile.Read(ref Peak)) < now && Interlocked.CompareExchange(ref Peak, now, peak) != peak) { }
            Interlocked.Increment(ref Invoked);
            try { await next(); } finally { Interlocked.Decrement(ref Current); }
        }
    }

    public sealed class EdgeReverseEncryption : Bq.Messaging.IEncryptionService
    {
        public static int Encrypted, Decrypted;
        static string Reverse(string value) { var chars = value.ToCharArray(); Array.Reverse(chars); return new string(chars); }
        public Bq.Messaging.EncryptedValue Encrypt(string value, IOutgoingLogicalMessageContext context)
        {
            Interlocked.Increment(ref Encrypted);
            return new Bq.Messaging.EncryptedValue { EncryptedBase64Value = Reverse(value), Base64Iv = "iv" };
        }
        public string Decrypt(Bq.Messaging.EncryptedValue encryptedValue, IIncomingLogicalMessageContext context)
        {
            Interlocked.Increment(ref Decrypted);
            return Reverse(encryptedValue.EncryptedBase64Value);
        }
    }

    public sealed class EdgeMemoryDataBus : Bq.Messaging.DataBus.IDataBus
    {
        public readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> Values = new System.Collections.Concurrent.ConcurrentDictionary<string, byte[]>();
        public bool FailGet;
        public int Gets;
        public Task Start() => Task.CompletedTask;
        public Task<string> Put(System.IO.Stream stream, TimeSpan timeToBeReceived)
        {
            var key = Guid.NewGuid().ToString("N");
            using (var copy = new System.IO.MemoryStream()) { stream.CopyTo(copy); Values[key] = copy.ToArray(); }
            return Task.FromResult(key);
        }
        public Task<System.IO.Stream> Get(string key)
        {
            Interlocked.Increment(ref Gets);
            if (FailGet) throw new System.IO.IOException("data bus unavailable");
            return Task.FromResult<System.IO.Stream>(new System.IO.MemoryStream(Values[key]));
        }
    }

    public sealed class EdgeCountingDataBusSerializer : Bq.Messaging.DataBus.IDataBusSerializer
    {
        public static int Serialized, Deserialized;
        public void Serialize(object dataBusProperty, System.IO.Stream stream)
        {
            Interlocked.Increment(ref Serialized);
            var bytes = (byte[])dataBusProperty;
            stream.Write(bytes, 0, bytes.Length);
        }
        public object Deserialize(System.IO.Stream stream)
        {
            Interlocked.Increment(ref Deserialized);
            using (var copy = new System.IO.MemoryStream()) { stream.CopyTo(copy); return copy.ToArray(); }
        }
    }

    // Delegates to a SQLite provider but holds outgoing dispatch until the gate opens.
    public sealed class EdgeHeldPersistence : IPersistenceProvider, IRecoveryPersistence, ILockedDispatchPersistence
    {
        readonly Bq.Persistence.Sqlite.SqlitePersistence inner;
        readonly Task gate;
        public EdgeHeldPersistence(Bq.Persistence.Sqlite.SqlitePersistence inner, Task gate) { this.inner = inner; this.gate = gate; }
        public Task<IPersistenceSession> OpenSession() => inner.OpenSession();
        Task<OutgoingMessage> IPersistenceProvider.NextOutgoing(string endpoint) => throw new InvalidOperationException("Expected provider-controlled dispatch.");
        Task IPersistenceProvider.MarkDispatched(string endpoint, string id) => throw new InvalidOperationException("Expected provider-controlled dispatch.");
        Task<RecoveryRecord> IRecoveryPersistence.NextRecovery(string endpoint) => throw new InvalidOperationException("Expected provider-controlled dispatch.");
        Task IRecoveryPersistence.MarkRecoveryDispatched(string endpoint, string id) => throw new InvalidOperationException("Expected provider-controlled dispatch.");
        public async Task<bool> DispatchOutgoing(string endpoint, Action<OutgoingMessage> publish)
        {
            await gate;
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
        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit) => inner.ListErrors(endpoint, limit);
        public Task<bool> ReplayError(string endpoint, string id, RecoveryRecord record) => inner.ReplayError(endpoint, id, record);
    }

    public sealed class EdgeInvokeSender : Behavior<IInvokeHandlerContext>
    {
        public static string ForwardTarget;
        public static int Triggers;
        public static bool PublishAtSupported;
        public override async Task Invoke(IInvokeHandlerContext context, Func<Task> next)
        {
            if (!(context.MessageBeingHandled is EdgeUowMessage trigger) || trigger.Text != "trigger") { await next(); return; }
            Interlocked.Increment(ref Triggers);
            EdgeUowMessage Make(string text) => new EdgeUowMessage { Text = text };
            var destination = context.ReplyToAddress;
            await context.Send(Make("send"));
            await context.Send<EdgeUowMessage>(m => m.Text = "send-init");
            await context.Send(Make("send-opts"), new SendOptions());
            await context.Send((object)Make("send-object-opts"), new SendOptions());
            await context.Send<EdgeUowMessage>(m => m.Text = "send-init-opts", new SendOptions());
            await context.Send(destination, Make("send-dest"));
            await context.SendLocal(Make("send-local"));
            await context.Publish(Make("publish"));
            await context.Publish<EdgeUowMessage>(m => m.Text = "publish-init");
            await context.Publish(Make("publish-opts"), new PublishOptions());
            await context.Publish((object)Make("publish-object-opts"), new PublishOptions());
            await context.Publish<EdgeUowMessage>(m => m.Text = "publish-init-opts", new PublishOptions());
            await context.Reply(Make("reply"));
            await context.Reply<EdgeUowMessage>(m => m.Text = "reply-init");
            await context.Reply(Make("reply-opts"), new ReplyOptions());
            await context.Reply((object)Make("reply-object-opts"), new ReplyOptions());
            await context.Reply<EdgeUowMessage>(m => m.Text = "reply-init-opts", new ReplyOptions());
            var due = DateTimeOffset.UtcNow.AddMilliseconds(200);
            await context.SendAt(Make("send-at"), due);
            await context.SendAt(destination, Make("send-at-dest"), due);
            await context.SendLocalAt(Make("local-at"), due);
            if (PublishAtSupported) await context.PublishAt(Make("publish-at"), due);
            await context.ForwardCurrentMessageTo(ForwardTarget);
            await next();
        }
    }

    // Delegates to a provider but fails the first MarkDispatched calls, as a crash after publishing would.
    public sealed class EdgeMarkFailsPersistence : IPersistenceProvider, IRecoveryPersistence
    {
        readonly Bq.Persistence.Sqlite.SqlitePersistence inner;
        int failures;
        public int Handed;
        public EdgeMarkFailsPersistence(Bq.Persistence.Sqlite.SqlitePersistence inner, int failures) { this.inner = inner; this.failures = failures; }
        public Task<RecoveryRecord> NextRecovery(string endpoint) => inner.NextRecovery(endpoint);
        public Task MarkRecoveryDispatched(string endpoint, string recoveryId) => inner.MarkRecoveryDispatched(endpoint, recoveryId);
        public Task<RecoveryRecord> GetError(string endpoint, string recoveryId) => inner.GetError(endpoint, recoveryId);
        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit) => inner.ListErrors(endpoint, limit);
        public Task<bool> ReplayError(string endpoint, string recoveryId, RecoveryRecord replay) => inner.ReplayError(endpoint, recoveryId, replay);
        public Task<IPersistenceSession> OpenSession() => inner.OpenSession();
        public async Task<OutgoingMessage> NextOutgoing(string endpoint)
        {
            var message = await inner.NextOutgoing(endpoint);
            if (message != null) Interlocked.Increment(ref Handed);
            return message;
        }
        public Task MarkDispatched(string endpoint, string messageId)
        {
            if (Interlocked.Decrement(ref failures) >= 0) throw new InvalidOperationException("simulated crash before the dispatch was recorded");
            return inner.MarkDispatched(endpoint, messageId);
        }
    }

    // Counts what an Oracle provider hands to the dispatcher, so duplicate publishing is visible and not hidden by the inbox.
    public sealed class EdgeCountingOracle : IPersistenceProvider, IRecoveryPersistence, ILockedDispatchPersistence, ILockedBatchDispatchPersistence
    {
        readonly Bq.Persistence.Oracle.OraclePersistence inner;
        readonly int[] published;
        public EdgeCountingOracle(Bq.Persistence.Oracle.OraclePersistence inner, int[] published) { this.inner = inner; this.published = published; }
        public Task<IPersistenceSession> OpenSession() => inner.OpenSession();
        public Task<OutgoingMessage> NextOutgoing(string endpoint) => inner.NextOutgoing(endpoint);
        public Task MarkDispatched(string endpoint, string messageId) => inner.MarkDispatched(endpoint, messageId);
        public Task<RecoveryRecord> NextRecovery(string endpoint) => inner.NextRecovery(endpoint);
        public Task MarkRecoveryDispatched(string endpoint, string recoveryId) => inner.MarkRecoveryDispatched(endpoint, recoveryId);
        public Task<RecoveryRecord> GetError(string endpoint, string recoveryId) => inner.GetError(endpoint, recoveryId);
        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit) => inner.ListErrors(endpoint, limit);
        public Task<bool> ReplayError(string endpoint, string recoveryId, RecoveryRecord replay) => inner.ReplayError(endpoint, recoveryId, replay);
        public Task<bool> DispatchOutgoing(string endpoint, Action<OutgoingMessage> publish) =>
            inner.DispatchOutgoing(endpoint, message => { Interlocked.Increment(ref published[0]); publish(message); });
        public Task<bool> DispatchRecovery(string endpoint, Action<RecoveryRecord> publish) => inner.DispatchRecovery(endpoint, publish);
        public Task<int> DispatchOutgoingBatch(string endpoint, int maxCount, Func<IReadOnlyList<OutgoingMessage>, IReadOnlyCollection<string>> publish) =>
            inner.DispatchOutgoingBatch(endpoint, maxCount, rows => { Interlocked.Add(ref published[0], rows.Count); return publish(rows); });
    }

    // Delegates to SQLite and counts the batches the dispatcher asked for.
    public sealed class EdgeBatchCounting : IPersistenceProvider, IRecoveryPersistence, IOutgoingFailureTracking, IBatchOutgoingPersistence
    {
        readonly Bq.Persistence.Sqlite.SqlitePersistence inner;
        public int Batches, Largest, Singles;
        public EdgeBatchCounting(Bq.Persistence.Sqlite.SqlitePersistence inner) { this.inner = inner; }
        public Task<IPersistenceSession> OpenSession() => inner.OpenSession();
        public async Task<OutgoingMessage> NextOutgoing(string endpoint)
        {
            var next = await inner.NextOutgoing(endpoint);
            if (next != null) Interlocked.Increment(ref Singles);
            return next;
        }
        public Task MarkDispatched(string endpoint, string messageId) => inner.MarkDispatched(endpoint, messageId);
        public async Task<IReadOnlyList<OutgoingMessage>> NextOutgoingBatch(string endpoint, int maxCount)
        {
            var batch = await inner.NextOutgoingBatch(endpoint, maxCount);
            if (batch.Count > 0)
            {
                Interlocked.Increment(ref Batches);
                int seen;
                while ((seen = Volatile.Read(ref Largest)) < batch.Count && Interlocked.CompareExchange(ref Largest, batch.Count, seen) != seen) { }
            }
            return batch;
        }
        public Task MarkDispatched(string endpoint, IReadOnlyCollection<string> messageIds) => inner.MarkDispatched(endpoint, messageIds);
        public Task RecordOutgoingFailure(string endpoint, string messageId, Exception failure) => inner.RecordOutgoingFailure(endpoint, messageId, failure);
        public Task<RecoveryRecord> NextRecovery(string endpoint) => inner.NextRecovery(endpoint);
        public Task MarkRecoveryDispatched(string endpoint, string recoveryId) => inner.MarkRecoveryDispatched(endpoint, recoveryId);
        public Task<RecoveryRecord> GetError(string endpoint, string recoveryId) => inner.GetError(endpoint, recoveryId);
        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit) => inner.ListErrors(endpoint, limit);
        public Task<bool> ReplayError(string endpoint, string recoveryId, RecoveryRecord replay) => inner.ReplayError(endpoint, recoveryId, replay);
    }

    // Lets the broker cases reach the shared retention scenario in EdgeCaseTests.
    static class RetentionCheck
    {
        public static Task Run<T>(T store, string endpoint) where T : IPersistenceProvider, IRecoveryPersistence, IRetentionPersistence =>
            EdgeCaseTests.RetentionScenario(store, endpoint);
    }

    // The ambient value a delivery behavior sets, as a tenant would be.
    public static class EdgeAmbient { public static readonly System.Threading.AsyncLocal<string> Value = new System.Threading.AsyncLocal<string>(); }

    public sealed class EdgeAmbientBehavior : Behavior<IDeliveryContext>
    {
        public static int Calls;
        public static string Mode = "pass";
        public override async Task Invoke(IDeliveryContext context, Func<Task> next)
        {
            System.Threading.Interlocked.Increment(ref Calls);
            context.Headers.TryGetValue("tenant", out var tenant);
            if (Mode == "reject" || tenant == null) throw new InvalidOperationException("no tenant on message " + context.MessageId);
            if (Mode == "skip") return;
            EdgeAmbient.Value.Value = tenant;
            try { await next(); }
            finally { EdgeAmbient.Value.Value = null; }
        }
    }

    static class EdgeOutbox
    {
        public static string NewPath(string name) => System.IO.Path.Combine(System.IO.Path.GetTempPath(), name + ".db");

        public static System.Data.Common.DbConnection Connect(string path)
        {
            var type = Type.GetType("Mono.Data.Sqlite.SqliteConnection, Mono.Data.Sqlite, Version=4.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756", true);
            var connection = (System.Data.Common.DbConnection)Activator.CreateInstance(type);
            connection.ConnectionString = "Data Source=" + path + ";Default Timeout=5";
            return connection;
        }

        public static async Task<Bq.Persistence.Sqlite.SqlitePersistence> Store(string path, string endpoint)
        {
            var store = new Bq.Persistence.Sqlite.SqlitePersistence(() => Connect(path), endpoint);
            await store.Initialize();
            return store;
        }

        // Writes committed outbox rows without dispatching them, in the order given.
        public static async Task Seed(IPersistenceProvider store, string queue, IEnumerable<string> texts, bool mandatory = true)
        {
            var format = new Bq.Messaging.Serialization.MessagePackSerialization().CreateWireFormat();
            using (var session = await store.OpenSession())
            {
                foreach (var text in texts)
                    await session.StoreOutgoing(new OutgoingMessage
                    {
                        Id = Guid.NewGuid().ToString("N"), Type = EndpointConfiguration.MessageType(typeof(EdgeUowMessage)), Exchange = "", RoutingKey = queue,
                        Mandatory = mandatory, Headers = new Dictionary<string, object>(), Body = format.Serialize(typeof(EdgeUowMessage), new EdgeUowMessage { Text = text })
                    });
                session.Commit();
            }
        }

        // The first stored outbox row as written, whether or not the dispatcher would currently offer it.
        public static async Task<OutgoingMessage> FirstStored(string path)
        {
            using (var connection = Connect(path))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT id,correlation_id,reply_to,type,exchange_name,routing_key,mandatory,body,headers FROM bq_outbox ORDER BY rowid LIMIT 1";
                    using (var r = await command.ExecuteReaderAsync())
                    {
                        if (!await r.ReadAsync()) return null;
                        return new OutgoingMessage
                        {
                            Id = r.GetString(0), CorrelationId = r.IsDBNull(1) ? null : r.GetString(1), ReplyTo = r.IsDBNull(2) ? null : r.GetString(2), Type = r.GetString(3),
                            Exchange = r.GetString(4), RoutingKey = r.GetString(5), Mandatory = Convert.ToInt32(r.GetValue(6)) != 0, Body = (byte[])r.GetValue(7),
                            Headers = MessageHeaderCodec.Decode(r.IsDBNull(8) ? null : (byte[])r.GetValue(8))
                        };
                    }
                }
            }
        }

        public static async Task<long> Count(string path, string table)
        {
            using (var connection = Connect(path))
            {
                await connection.OpenAsync();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT(*) FROM " + table;
                    return Convert.ToInt64(await command.ExecuteScalarAsync());
                }
            }
        }
    }

    public sealed class EdgeUnit : IManageUnitsOfWork
    {
        public static readonly List<string> Log = new List<string>();
        public static bool FailEnd;
        readonly string name;
        public EdgeUnit(string name) { this.name = name; }
        public Task Begin() { lock (Log) Log.Add("begin:" + name); return Task.CompletedTask; }
        public Task End(Exception ex = null)
        {
            lock (Log) Log.Add("end:" + name + ":" + (ex == null ? "ok" : ex.GetType().Name));
            if (FailEnd && ex != null && name == "inner") throw new InvalidOperationException("end failed");
            return Task.CompletedTask;
        }
    }

    public sealed class EdgeSatelliteModes : Feature
    {
        public static string Queue;
        public static string Mode;
        public static readonly List<string> Calls = new List<string>();
        protected override void Setup(FeatureConfigurationContext context) =>
            context.AddSatelliteReceiver("edge-modes", Queue, Bq.Messaging.Transport.PushRuntimeSettings.Default,
                (config, error) =>
                {
                    switch (Mode)
                    {
                        case "immediate": return RecoverabilityAction.ImmediateRetry();
                        case "delayed": return RecoverabilityAction.DelayedRetry(TimeSpan.FromMilliseconds(100));
                        case "discard": return RecoverabilityAction.Discard("edge discard");
                        default: return RecoverabilityAction.MoveToError(config.Failed.ErrorQueue);
                    }
                },
                (builder, message) =>
                {
                    int n;
                    lock (Calls) { Calls.Add(Mode); n = Calls.Count; }
                    if (n == 1) throw new InvalidOperationException("first attempt fails");
                    return Task.CompletedTask;
                });
    }

    static class EdgeBrokerTests
    {
        static readonly List<(string Name, Func<Task> Body)> cases = new List<(string, Func<Task>)>();
        static void That(bool v, string m) { if (!v) throw new Exception(m); }
        static Task Case(string name, Func<Task> body) { cases.Add((name, body)); return Task.CompletedTask; }
        static async Task<T> Within<T>(Task<T> task)
        {
            if (await Task.WhenAny(task, Task.Delay(15000)) != task) throw new Exception("Timed out waiting for the result.");
            return await task;
        }
        // The simulator confirms a publish at once. A real broker confirms durable messages one at a time, so the dispatcher
        // sends roughly ten rows a second there, and backlog cases are sized for it.
        static bool OnSimulator => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("BQ_RABBITSIM_HTTP"));

        // Sets rabbit-sim's fault injection to the behaviour in the JSON body; what it leaves out is off, so Chaos() turns it all off.
        // False when there is no simulator (a real broker). The simulator is always the latest one, so a refusal is an error.
        static async Task<bool> Chaos(string json = "{}")
        {
            var http = Environment.GetEnvironmentVariable("BQ_RABBITSIM_HTTP");
            if (string.IsNullOrEmpty(http)) return false;
            using (var client = new System.Net.Http.HttpClient())
            {
                var response = await client.PostAsync(http + "/chaos", new System.Net.Http.StringContent(json, System.Text.Encoding.UTF8, "application/json"));
                if (!response.IsSuccessStatusCode) throw new InvalidOperationException("rabbit-sim refused POST /chaos " + json + ": " + (int)response.StatusCode);
                return true;
            }
        }

        // Moves the simulator's clock forward, which lets queue TTLs expire and delayed messages through at once. The clock is shared by every
        // test, and absolute delivery times follow it, so a test that advances it must call ResetClock when it ends. False when there is no simulator.
        static async Task<bool> AdvanceClock(int milliseconds) => await PostClock("/chaos/clock/advance?ms=" + milliseconds);
        static async Task<bool> ResetClock() => await PostClock("/chaos/clock/reset");
        static async Task<bool> PostClock(string path)
        {
            var http = Environment.GetEnvironmentVariable("BQ_RABBITSIM_HTTP");
            if (string.IsNullOrEmpty(http)) return false;
            using (var client = new System.Net.Http.HttpClient())
            {
                var response = await client.PostAsync(http + path, null);
                if (!response.IsSuccessStatusCode) throw new InvalidOperationException("rabbit-sim refused POST " + path + ": " + (int)response.StatusCode);
                return true;
            }
        }

        static async Task Eventually(Func<bool> condition, string what, int seconds = 15)
        {
            for (var i = 0; i < seconds * 10; i++) { if (condition()) return; await Task.Delay(100); }
            throw new Exception("Timed out: " + what);
        }
        static async Task<BasicGetResult> GetFrom(IModel channel, string queue)
        {
            BasicGetResult r = null;
            await Eventually(() => (r = channel.BasicGet(queue, true)) != null, "a message on " + queue);
            return r;
        }

        // True when the broker is RabbitMQ 4.4 or later, which holds a delayed publish. Delayed publishes are tested only there.
        public static bool BrokerHasNativeDelay(Uri uri)
        {
            using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
            {
                var version = connection.ServerProperties != null && connection.ServerProperties.TryGetValue("version", out var raw) && raw is byte[] bytes
                    ? System.Text.Encoding.UTF8.GetString(bytes) : null;
                return version != null && Version.TryParse(version.Split('-')[0], out var parsed) && parsed >= new Version(4, 4);
            }
        }

        // Native delayed delivery needs quorum queues, so endpoints that receive delayed messages ask for one.
        static void UseNativeDelayWhenAvailable(EndpointConfiguration configuration)
        {
            if (nativeDelay) configuration.InputQueueArguments["x-queue-type"] = "quorum";
        }
        static bool nativeDelay;

        public static async Task<IReadOnlyList<(string Name, Func<Task> Body)>> Collect(Uri uri)
        {
            cases.Clear();
            nativeDelay = BrokerHasNativeDelay(uri);
            await Case("units of work end in reverse order and see the failure", async () =>
            {
                EdgeUnit.Log.Clear(); EdgeUnit.FailEnd = false;
                var name = "bq.test.edge.uow." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), ImmediateRetries = 0 };
                configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                configuration.Route(typeof(EdgeUowMessage), name);
                configuration.RegisterUnitOfWork(() => new EdgeUnit("outer"));
                configuration.RegisterUnitOfWork(() => new EdgeUnit("inner"));
                var handled = 0;
                var lastError = new List<Exception>();
                configuration.Notifications.Errors.MessageSentToErrorQueue += (sender, message) => { lock (lastError) lastError.Add(message.Exception); };
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) =>
                {
                    Interlocked.Increment(ref handled);
                    if (m.Text == "fail") throw new ApplicationException("handler failed");
                    return Task.CompletedTask;
                }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    await endpoint.Send(new EdgeUowMessage { Text = "ok" });
                    await Eventually(() => { lock (EdgeUnit.Log) return EdgeUnit.Log.Count == 4; }, "the successful attempt");
                    That(string.Join(",", EdgeUnit.Log) == "begin:outer,begin:inner,end:inner:ok,end:outer:ok", "success order: " + string.Join(",", EdgeUnit.Log));
                    EdgeUnit.Log.Clear();
                    await endpoint.Send(new EdgeUowMessage { Text = "fail" });
                    await GetFrom(channel, name + ".error");
                    That(string.Join(",", EdgeUnit.Log) == "begin:outer,begin:inner,end:inner:ApplicationException,end:outer:ApplicationException",
                        "failure order: " + string.Join(",", EdgeUnit.Log));
                    EdgeUnit.Log.Clear();
                    EdgeUnit.FailEnd = true;
                    await endpoint.Send(new EdgeUowMessage { Text = "fail" });
                    await GetFrom(channel, name + ".error");
                    await Eventually(() => { lock (lastError) return lastError.Count == 2; }, "the error notification");
                    var combined = lastError[1];
                    That((string)combined.Data["ExceptionType"] == typeof(AggregateException).FullName &&
                        combined.Message.Contains("handler failed") && combined.Message.Contains("end failed"),
                        "an End failure combines with the original: " + combined.Message);
                    That(EdgeUnit.Log.Contains("end:outer:ApplicationException"),
                        "the remaining units still end with the original failure: " + string.Join(",", EdgeUnit.Log));
                }
            });

            await Case("a TimeToBeReceived message expires on the broker clock", async () =>
            {
                var http = Environment.GetEnvironmentVariable("BQ_RABBITSIM_HTTP");
                if (string.IsNullOrEmpty(http)) { Console.WriteLine("TTL test skipped: set BQ_RABBITSIM_HTTP."); return; }
                var name = "bq.test.edge.ttl." + Guid.NewGuid().ToString("N");
                var sink = name + ".sink";
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                configuration.Route(typeof(EdgeTtlMessage), sink);
                using (var endpoint = RabbitEndpoint.Start(configuration))
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(sink, true, false, false);
                    await endpoint.Send(new EdgeTtlMessage { Text = "short-lived" });
                    await Eventually(() => channel.MessageCount(sink) == 1, "the message to reach the sink queue");
                    using (var client = new System.Net.Http.HttpClient())
                    {
                        var probe = await client.PostAsync(http + "/chaos/clock/advance?ms=10000", null);
                        if (probe.StatusCode == System.Net.HttpStatusCode.NotFound)
                        {
                            Console.WriteLine("TTL test skipped: this rabbit-sim has no /chaos/clock endpoint.");
                            return;
                        }
                        await Task.Delay(300);
                        That(channel.MessageCount(sink) == 1, "the message is still alive before its time to be received passes");
                        await client.PostAsync(http + "/chaos/clock/advance?ms=30000", null);
                    }
                    await Eventually(() => channel.MessageCount(sink) == 0, "the message to expire after the clock passes its TTL");
                }
            });

            await Case("a failure inside a receive behavior goes through recoverability", async () =>
            {
                EdgeReceiveFailure.Seen = 0;
                var name = "bq.test.edge.receivefail." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), ImmediateRetries = 1 };
                configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                configuration.Route(typeof(EdgeUowMessage), name);
                configuration.Pipeline.Register("edge-receive-fail", new EdgeReceiveFailure(), "Fail in the receive stage");
                var handled = 0;
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                var errored = new TaskCompletionSource<FailedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                configuration.Notifications.Errors.MessageSentToErrorQueue += (sender, message) => errored.TrySetResult(message);
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    var options = new SendOptions();
                    options.SetHeader("edge-receive-fail", "1");
                    await endpoint.Send(new EdgeUowMessage { Text = "x" }, options);
                    var failed = await Within(errored.Task);
                    That(failed.Exception.Message.Contains("receive behavior failed"), "the failure reaches the error notification: " + failed.Exception.Message);
                    That(Volatile.Read(ref EdgeReceiveFailure.Seen) == 2 && Volatile.Read(ref handled) == 0, "one attempt plus one immediate retry, and the handler never ran: " + EdgeReceiveFailure.Seen);
                }
            });
            await Case("an abort on every attempt redelivers without counting failures", async () =>
            {
                EdgeAlwaysAbort.Seen = 0;
                var name = "bq.test.edge.alwaysabort." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), ImmediateRetries = 0 };
                configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                configuration.Route(typeof(EdgeUowMessage), name);
                configuration.Pipeline.Register("edge-always-abort", new EdgeAlwaysAbort(), "Always abort");
                var handled = 0;
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    var options = new SendOptions();
                    options.SetHeader("edge-always-abort", "1");
                    await endpoint.Send(new EdgeUowMessage { Text = "x" }, options);
                    await Eventually(() => Volatile.Read(ref EdgeAlwaysAbort.Seen) >= 5, "repeated redelivery after each abort");
                    That(Volatile.Read(ref handled) == 0, "the handler never runs");
                    That(channel.MessageCount(name + ".error") == 0, "an abort is not a failure");
                }
            });
            await Case("MaxConcurrency above one processes in parallel with pipeline behaviors", async () =>
            {
                EdgeConcurrencyProbe.Current = EdgeConcurrencyProbe.Peak = EdgeConcurrencyProbe.Invoked = 0;
                var name = "bq.test.edge.concurrency." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), MaxConcurrency = 4 };
                configuration.Route(typeof(EdgeUowMessage), name);
                configuration.Pipeline.Register("edge-concurrency", new EdgeConcurrencyProbe(), "Measure concurrency");
                configuration.RegisterUnitOfWork(() => new EdgeUnit("concurrent"));
                var handled = 0;
                configuration.Handle(() => new Handler<EdgeUowMessage>(async (m, c) => { await Task.Delay(150); Interlocked.Increment(ref handled); }));
                EdgeUnit.Log.Clear();
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    for (var i = 0; i < 12; i++) await endpoint.Send(new EdgeUowMessage { Text = i.ToString() });
                    await Eventually(() => Volatile.Read(ref handled) == 12, "all twelve messages");
                    That(Volatile.Read(ref EdgeConcurrencyProbe.Peak) > 1 && EdgeConcurrencyProbe.Peak <= 4, "peak concurrency was " + EdgeConcurrencyProbe.Peak);
                    That(EdgeConcurrencyProbe.Invoked == 12, "each message passed through the behavior once: " + EdgeConcurrencyProbe.Invoked);
                    lock (EdgeUnit.Log) That(EdgeUnit.Log.Count(l => l == "begin:concurrent") == 12 && EdgeUnit.Log.Count(l => l == "end:concurrent:ok") == 12, "every attempt began and ended its unit of work");
                }
            });

            // Runs a burst through an endpoint while the simulator is slow to deliver and to act on acks. Returns the peak
            // number of handlers running at once and how often each message was handled.
            async Task<(int Peak, System.Collections.Concurrent.ConcurrentDictionary<string, int> Seen)> SlowBroker(string label, ushort maxConcurrency, ushort? prefetch, int messages, int handlerMs)
            {
                var name = "bq.test.edge.slow." + label + "." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), MaxConcurrency = maxConcurrency, PrefetchCount = prefetch };
                configuration.Route(typeof(EdgeUowMessage), name);
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
                int active = 0, peak = 0;
                configuration.Handle(() => new Handler<EdgeUowMessage>(async (m, c) =>
                {
                    var now = Interlocked.Increment(ref active);
                    int seenPeak;
                    while ((seenPeak = Volatile.Read(ref peak)) < now && Interlocked.CompareExchange(ref peak, now, seenPeak) != seenPeak) { }
                    await Task.Delay(handlerMs);
                    seen.AddOrUpdate(m.Text, 1, (key, n) => n + 1);
                    Interlocked.Decrement(ref active);
                }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    for (var i = 0; i < messages; i++) await endpoint.Send(new EdgeUowMessage { Text = i.ToString() });
                    await Eventually(() => seen.Count == messages, "all " + messages + " messages", 30);
                    await Task.Delay(500);
                }
                return (peak, seen);
            }

            await Case("slow acks and slow delivery keep concurrency within MaxConcurrency and handle every message once", async () =>
            {
                if (!await Chaos()) { Console.WriteLine("Slow-ack test skipped: needs rabbit-sim (BQ_RABBITSIM_HTTP)."); return; }
                try
                {
                    await Chaos("{\"deliver_delay_ms\":40,\"ack_delay_ms\":250}");
                    var run = await SlowBroker("acks", 4, null, 16, 60);
                    That(run.Peak >= 2 && run.Peak <= 4, "peak concurrency was " + run.Peak);
                    That(run.Seen.Count == 16 && run.Seen.Values.All(n => n == 1), "each message was handled once: " + string.Join(",", run.Seen.Where(p => p.Value != 1).Select(p => p.Key + "x" + p.Value)));
                }
                finally { await Chaos(); }
            });
            await Case("prefetch bounds the messages in flight even when MaxConcurrency is higher", async () =>
            {
                if (!await Chaos()) { Console.WriteLine("Prefetch test skipped: needs rabbit-sim (BQ_RABBITSIM_HTTP)."); return; }
                try
                {
                    await Chaos("{\"deliver_delay_ms\":30,\"ack_delay_ms\":150}");
                    var run = await SlowBroker("prefetch", 6, 2, 10, 80);
                    That(run.Peak <= 2, "peak concurrency was " + run.Peak + " with prefetch 2");
                    That(run.Seen.Count == 10 && run.Seen.Values.All(n => n == 1), "each message was handled once");
                }
                finally { await Chaos(); }
            });
            await Case("a handler slower than the delivery delay neither loses nor repeats a message", async () =>
            {
                if (!await Chaos()) { Console.WriteLine("Slow-handler test skipped: needs rabbit-sim (BQ_RABBITSIM_HTTP)."); return; }
                try
                {
                    await Chaos("{\"deliver_delay_ms\":50}");
                    var run = await SlowBroker("handler", 2, null, 8, 300);
                    That(run.Peak <= 2, "peak concurrency was " + run.Peak);
                    That(run.Seen.Count == 8 && run.Seen.Values.All(n => n == 1), "each message was handled once");
                }
                finally { await Chaos(); }
            });

            await Case("endpoints with the same name on different virtual hosts keep their messages apart", async () =>
            {
                if (!OnSimulator) { Console.WriteLine("Virtual host test skipped: real brokers need the virtual hosts created first."); return; }
                Uri OnVhost(string vhost) => new UriBuilder(uri) { Path = "/" + vhost }.Uri;
                var suffix = Guid.NewGuid().ToString("N");
                var (hostA, hostB) = ("bq-a-" + suffix, "bq-b-" + suffix);
                var name = "bq.test.edge.vhost." + suffix;
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();
                RabbitEndpoint Start(string vhost)
                {
                    var configuration = new EndpointConfiguration(name, OnVhost(vhost)) { Persistence = new InMemoryPersistenceProvider(name) };
                    configuration.Route(typeof(EdgeUowMessage), name);
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen[m.Text] = vhost; return Task.CompletedTask; }));
                    return RabbitEndpoint.Start(configuration);
                }
                using (var endpointA = Start(hostA))
                using (var endpointB = Start(hostB))
                {
                    for (var i = 0; i < 3; i++)
                    {
                        await endpointA.Send(new EdgeUowMessage { Text = "a" + i });
                        await endpointB.Send(new EdgeUowMessage { Text = "b" + i });
                    }
                    await Eventually(() => seen.Count == 6, "all six messages");
                    await Task.Delay(300);
                    That(seen.Count == 6, "no message was duplicated or lost: " + seen.Count);
                    That(seen.All(p => p.Key.StartsWith("a") ? p.Value == hostA : p.Value == hostB), "each endpoint handled only its own vhost's messages: " + string.Join(",", seen.Select(p => p.Key + "@" + p.Value)));
                }
            });

            await Case("a delay is split into levels, and the routing key and entry exchange follow its bits", () =>
            {
                That(DelayTopology.Seconds(TimeSpan.FromMilliseconds(1)) == 1, "a delay rounds up to a whole second");
                That(DelayTopology.Seconds(TimeSpan.FromSeconds(10)) == 10, "ten seconds is ten");
                That(DelayTopology.EntryExchange(TimeSpan.FromSeconds(10)) == "bq.v1.delay-level-03", "10 = 1010 enters at level 3, found " + DelayTopology.EntryExchange(TimeSpan.FromSeconds(10)));
                That(DelayTopology.EntryExchange(TimeSpan.FromSeconds(1)) == "bq.v1.delay-level-00", "one second enters at level 0");
                That(DelayTopology.EntryExchange(DelayTopology.MaxDelay) == "bq.v1.delay-level-27", "the longest delay enters at level 27");
                var key = DelayTopology.RoutingKey(TimeSpan.FromSeconds(10), "orders.q");
                That(key == string.Join(".", Enumerable.Repeat("0", 24)) + ".1.0.1.0.orders.q", "the key is 28 bits then the destination: " + key);
                That(DelayTopology.DestinationBindingKey("orders.q") == string.Join(".", Enumerable.Repeat("*", 28)) + ".orders.q", "the destination binds after 28 wildcard bits");
                That(DelayTopology.MaxDelay == TimeSpan.FromSeconds(268435455), "the longest delay is 2^28-1 seconds");
                foreach (var bad in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1), DelayTopology.MaxDelay + TimeSpan.FromSeconds(1) })
                {
                    var rejected = false;
                    try { DelayTopology.Seconds(bad); } catch (ArgumentOutOfRangeException) { rejected = true; }
                    That(rejected, "a delay of " + bad + " is refused");
                }
                return Task.CompletedTask;
            });
            await Case("delayed messages travel the levels and arrive in order of their due time", async () =>
            {
              try
              {
                var queue = "bq.test.edge.delay." + Guid.NewGuid().ToString("N");
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var model = connection.CreateModel())
                {
                    DelayTopology.Declare(model);
                    model.QueueDeclare(queue, true, false, false);
                    DelayTopology.BindDestination(model, queue);
                    void Delay(string body, TimeSpan delay)
                    {
                        var props = model.CreateBasicProperties();
                        props.Persistent = true;
                        model.BasicPublish(DelayTopology.EntryExchange(delay), DelayTopology.RoutingKey(delay, queue), props, System.Text.Encoding.UTF8.GetBytes(body));
                    }
                    string Take() { var r = model.BasicGet(queue, true); return r == null ? null : System.Text.Encoding.UTF8.GetString(r.Body.ToArray()); }
                    // Sent long first and short second, so arriving in due-time order proves each delay is honoured on its own.
                    var started = DateTime.UtcNow;
                    var scale = OnSimulator ? 1 : 0;
                    Delay("long", TimeSpan.FromSeconds(OnSimulator ? 10 : 3));
                    Delay("short", TimeSpan.FromSeconds(OnSimulator ? 2 : 1));
                    await Task.Delay(300);
                    That(Take() == null, "nothing arrives before its delay");
                    if (OnSimulator)
                    {
                        await AdvanceClock(2000);
                        string first = null;
                        await Eventually(() => (first = Take()) != null, "the short message after two simulated seconds", 5);
                        That(first == "short", "the shorter delay arrives first, got " + first);
                        await AdvanceClock(7000);
                        await Task.Delay(300);
                        That(Take() == null, "the long message is still waiting after nine simulated seconds");
                        await AdvanceClock(1000);
                    }
                    else
                    {
                        string first = null;
                        await Eventually(() => (first = Take()) != null, "the short message", 8);
                        That(first == "short", "the shorter delay arrives first, got " + first);
                        That(DateTime.UtcNow - started >= TimeSpan.FromMilliseconds(800), "the short message was held for about its delay: " + (DateTime.UtcNow - started));
                    }
                    string second = null;
                    await Eventually(() => (second = Take()) != null, "the long message", 10);
                    That(second == "long", "the longer delay arrives second, got " + second);
                    if (!OnSimulator) That(DateTime.UtcNow - started >= TimeSpan.FromMilliseconds(2500), "the long message was held for about its delay: " + (DateTime.UtcNow - started));
                    model.QueueDelete(queue);
                }
              }
              finally { await ResetClock(); }
            });

            foreach (var delayMode in new[] { DelayedDeliveryMode.Levels, DelayedDeliveryMode.Native })
            {
                var mode = delayMode;
                await Case("a delayed send is held by the broker until it is due, with " + mode + " delay", async () =>
                {
                    if (mode == DelayedDeliveryMode.Native && !nativeDelay) { Console.WriteLine("Native delay test skipped: needs RabbitMQ 4.4 or later."); return; }
                    var name = "bq.test.edge.delayed." + mode + "." + Guid.NewGuid().ToString("N");
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), DelayedDelivery = mode };
                    if (mode == DelayedDeliveryMode.Native) configuration.InputQueueArguments["x-queue-type"] = "quorum";
                    var handled = new System.Collections.Concurrent.ConcurrentQueue<(string Text, DateTime At)>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { handled.Enqueue((m.Text, DateTime.UtcNow)); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        var due = DateTimeOffset.UtcNow.AddMilliseconds(2500);
                        await endpoint.SendLocalAt(new EdgeUowMessage { Text = "later" }, due);
                        await endpoint.SendLocal(new EdgeUowMessage { Text = "now" });
                        await Eventually(() => handled.Count == 1, "the immediate message", 5);
                        await Task.Delay(1000);
                        That(handled.Count == 1 && handled.First().Text == "now", "the delayed message has not arrived after one second");
                        await Eventually(() => handled.Count == 2, "the delayed message once due", 10);
                        That(handled.Last().Text == "later" && handled.Last().At >= due.UtcDateTime, "it arrives after its due time: " + (handled.Last().At - due.UtcDateTime));
                        await Task.Delay(500);
                        That(handled.Count == 2, "it arrives once");
                    }
                });
            }
            await Case("with delay levels a delayed publish is refused, because levels deliver to one queue", async () =>
            {
                var name = "bq.test.edge.levelspublish." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), DelayedDelivery = DelayedDeliveryMode.Levels };
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    var refused = false;
                    try { await endpoint.PublishAt(new EdgeUowMessage { Text = "event" }, DateTimeOffset.UtcNow.AddMinutes(1)); }
                    catch (NotSupportedException) { refused = true; }
                    That(refused, "PublishAt throws NotSupportedException");
                }
            });
            await Case("a delayed publish reaches every subscriber when due, with native delay", async () =>
            {
                if (!nativeDelay) { Console.WriteLine("Delayed publish test skipped: needs RabbitMQ 4.4 or later."); return; }
                var suffix = Guid.NewGuid().ToString("N");
                var events = "bq.test.events." + suffix;
                var received = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>();
                EndpointConfiguration Subscriber(string name)
                {
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), EventsExchange = events, DelayedDelivery = DelayedDeliveryMode.Native };
                    configuration.InputQueueArguments["x-queue-type"] = "quorum";
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { received[name] = DateTime.UtcNow; return Task.CompletedTask; }), true);
                    return configuration;
                }
                var (one, two) = ("bq.test.edge.sub1." + suffix, "bq.test.edge.sub2." + suffix);
                var publisherName = "bq.test.edge.pub." + suffix;
                var publisher = new EndpointConfiguration(publisherName, uri) { Persistence = new InMemoryPersistenceProvider(publisherName), EventsExchange = events, DelayedDelivery = DelayedDeliveryMode.Native };
                publisher.SendOnly();
                using (var first = RabbitEndpoint.Start(Subscriber(one)))
                using (var second = RabbitEndpoint.Start(Subscriber(two)))
                using (var endpoint = RabbitEndpoint.Start(publisher))
                {
                    var due = DateTimeOffset.UtcNow.AddMilliseconds(2000);
                    await endpoint.PublishAt(new EdgeUowMessage { Text = "event" }, due);
                    await Task.Delay(800);
                    That(received.IsEmpty, "no subscriber has the event before it is due");
                    await Eventually(() => received.Count == 2, "both subscribers to get the event", 10);
                    That(received.Values.All(at => at >= due.UtcDateTime), "each got it after the due time");
                }
            });
            await Case("native delay refuses an input queue that is not a quorum queue, because a classic queue would ignore the delay", () =>
            {
                if (!nativeDelay) { Console.WriteLine("Native delay guard test skipped: needs RabbitMQ 4.4 or later."); return Task.CompletedTask; }
                var name = "bq.test.edge.nativeclassic." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), DelayedDelivery = DelayedDeliveryMode.Native };
                var refused = false;
                try { RabbitEndpoint.Start(configuration).Dispose(); } catch (InvalidOperationException ex) { refused = ex.Message.Contains("quorum"); }
                That(refused, "start-up fails and names quorum queues");
                return Task.CompletedTask;
            });

            await Case("a delivery behavior's ambient value reaches the session, the handler and the failure handling", async () =>
            {
                var name = "bq.test.edge.delivery." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var opened = new System.Collections.Concurrent.ConcurrentQueue<string>();
                    var recording = false;
                    var store = new Bq.Persistence.Sqlite.SqlitePersistence(() => { if (recording) opened.Enqueue(EdgeAmbient.Value.Value ?? "(none)"); return EdgeOutbox.Connect(path); }, name);
                    await store.Initialize();
                    recording = true;
                    EdgeAmbientBehavior.Mode = "pass"; EdgeAmbientBehavior.Calls = 0;
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store, ImmediateRetries = 0 };
                    configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                    configuration.Pipeline.Register("edge-ambient", new EdgeAmbientBehavior(), "Set an ambient value");
                    var handled = new System.Collections.Concurrent.ConcurrentQueue<string>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) =>
                    {
                        handled.Enqueue(m.Text + "@" + EdgeAmbient.Value.Value);
                        if (m.Text == "fail") throw new ApplicationException("handler failed");
                        return Task.CompletedTask;
                    }));
                    var errored = new TaskCompletionSource<FailedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                    configuration.Notifications.Errors.MessageSentToErrorQueue += (sender, message) => errored.TrySetResult(message);
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        var format = new Bq.Messaging.Serialization.MessagePackSerialization().CreateWireFormat();
                        void Deliver(string text, string tenant)
                        {
                            var props = channel.CreateBasicProperties();
                            props.MessageId = Guid.NewGuid().ToString("N"); props.Type = EndpointConfiguration.MessageType(typeof(EdgeUowMessage));
                            props.ContentType = format.ContentType; props.Persistent = true;
                            if (tenant != null) props.Headers = new Dictionary<string, object> { ["tenant"] = tenant };
                            channel.BasicPublish("", name, props, format.Serialize(typeof(EdgeUowMessage), new EdgeUowMessage { Text = text }));
                        }
                        Deliver("ok", "acme");
                        await Eventually(() => handled.Count == 1, "the first message", 10);
                        That(handled.Single() == "ok@acme", "the handler sees the tenant the behavior set: " + handled.Single());
                        Deliver("fail", "globex");
                        var failed = await Task.WhenAny(errored.Task, Task.Delay(10000));
                        That(failed == errored.Task, "the failed message goes to the error queue");
                        // The dispatcher's own polling opens connections too, with no tenant, so look only at the tenants that were set.
                        That(opened.Count(tenant => tenant == "acme") >= 1, "the session for the first message saw its tenant: " + string.Join(",", opened));
                        That(opened.Count(tenant => tenant == "globex") >= 2,
                            "the failing message's processing session and the one that records its failure both saw its tenant: " + string.Join(",", opened));
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("a delivery behavior that throws sends the message to the error queue without opening the database", async () =>
            {
                var name = "bq.test.edge.deliveryreject." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var opened = 0;
                    var store = new Bq.Persistence.Sqlite.SqlitePersistence(() => { System.Threading.Interlocked.Increment(ref opened); return EdgeOutbox.Connect(path); }, name);
                    await store.Initialize();
                    EdgeAmbientBehavior.Mode = "pass"; EdgeAmbientBehavior.Calls = 0;
                    // Polling is slowed so that, once it has started, the only thing that could open a connection is the delivery.
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store, DispatchPollInterval = TimeSpan.FromHours(1) };
                    configuration.Pipeline.Register("edge-ambient", new EdgeAmbientBehavior(), "Set an ambient value");
                    var handled = 0;
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { System.Threading.Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        await Task.Delay(800);
                        var baseline = opened;
                        var format = new Bq.Messaging.Serialization.MessagePackSerialization().CreateWireFormat();
                        var props = channel.CreateBasicProperties();
                        props.MessageId = "no-tenant-1"; props.Type = EndpointConfiguration.MessageType(typeof(EdgeUowMessage));
                        props.ContentType = format.ContentType; props.Persistent = true;
                        channel.BasicPublish("", name, props, format.Serialize(typeof(EdgeUowMessage), new EdgeUowMessage { Text = "x" }));
                        BasicGetResult failed = null;
                        await Eventually(() => (failed = channel.BasicGet(name + ".error", true)) != null, "the message in the error queue", 10);
                        var headers = failed.BasicProperties.Headers;
                        That(headers.ContainsKey("x-bq-failure-message") && System.Text.Encoding.UTF8.GetString((byte[])headers["x-bq-failure-message"]).Contains("no tenant"),
                            "the reason is in the headers of the error copy");
                        That(System.Text.Encoding.UTF8.GetString((byte[])headers[Bq.Messaging.Faults.FaultsHeaderKeys.FailedQ]) == name, "the failing endpoint is named");
                        That(failed.BasicProperties.MessageId == "no-tenant-1", "the message ID is kept");
                        await Task.Delay(500);
                        That(handled == 0 && opened == baseline, "the handler never ran and the database was not opened for the delivery: handled " + handled + ", opened " + (opened - baseline) + " more");
                        That(channel.MessageCount(name) == 0, "the delivery was acknowledged, so it is not redelivered");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("a delivery behavior that returns without calling next discards the message", async () =>
            {
                var name = "bq.test.edge.deliveryskip." + Guid.NewGuid().ToString("N");
                EdgeAmbientBehavior.Mode = "skip"; EdgeAmbientBehavior.Calls = 0;
                try
                {
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                    configuration.Pipeline.Register("edge-ambient", new EdgeAmbientBehavior(), "Skip the message");
                    var handled = 0;
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { System.Threading.Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        var format = new Bq.Messaging.Serialization.MessagePackSerialization().CreateWireFormat();
                        var props = channel.CreateBasicProperties();
                        props.MessageId = "skipped-1"; props.Type = EndpointConfiguration.MessageType(typeof(EdgeUowMessage));
                        props.ContentType = format.ContentType; props.Headers = new Dictionary<string, object> { ["tenant"] = "acme" };
                        channel.BasicPublish("", name, props, format.Serialize(typeof(EdgeUowMessage), new EdgeUowMessage { Text = "x" }));
                        await Eventually(() => EdgeAmbientBehavior.Calls == 1, "the behavior to see the delivery", 10);
                        await Task.Delay(800);
                        That(handled == 0 && channel.MessageCount(name) == 0 && channel.MessageCount(name + ".error") == 0, "it was acknowledged and neither handled nor sent to the error queue");
                    }
                }
                finally { EdgeAmbientBehavior.Mode = "pass"; }
            });

            // ---- multi-tenant: one database per tenant ----
            // An endpoint whose tenants each have a SQLite file. "down" is a tenant whose database cannot be opened.
            async Task<(RabbitEndpoint Endpoint, Bq.Messaging.MultiTenant.TenantPersistence Persistence, string Name, Func<string, string> Path,
                System.Collections.Concurrent.ConcurrentQueue<string> Handled, EndpointConfiguration Configuration)> TenantEndpoint(string label, string[] tenants, string[] catalog = null)
            {
                var name = "bq.test.edge.tenant." + label + "." + Guid.NewGuid().ToString("N");
                string PathOf(string tenant) => EdgeOutbox.NewPath(name + "." + tenant);
                var configuration = new EndpointConfiguration(name, uri) { ImmediateRetries = 0 };
                configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                configuration.Route(typeof(EdgeUowMessage), name);
                var persistence = configuration.UseTenants(tenant => tenant == "down"
                    ? new Bq.Persistence.Sqlite.SqlitePersistence(() => throw new System.IO.IOException("the database of tenant 'down' is unreachable"), name)
                    : new Bq.Persistence.Sqlite.SqlitePersistence(() => EdgeOutbox.Connect(PathOf(tenant)), name),
                    options => { options.Tenants = () => Task.FromResult<IReadOnlyList<string>>(catalog ?? tenants); options.MaxIdleInterval = TimeSpan.FromSeconds(1); });
                foreach (var tenant in tenants.Where(t => t != "down")) await persistence.InitializeTenant(tenant);
                var handled = new System.Collections.Concurrent.ConcurrentQueue<string>();
                configuration.Handle(() => new Handler<EdgeUowMessage>(async (m, c) =>
                {
                    handled.Enqueue(m.Text + "@" + Bq.Messaging.MultiTenant.Tenant.Current);
                    if (m.Text == "boom") throw new ApplicationException("boom");
                    if (m.Text == "first")
                    {
                        await c.SendLocal(new EdgeUowMessage { Text = "follow-up" });
                        await c.SendLocalAt(new EdgeUowMessage { Text = "delayed" }, DateTimeOffset.UtcNow.AddMilliseconds(1500));
                    }
                }));
                return (RabbitEndpoint.Start(configuration), persistence, name, PathOf, handled, configuration);
            }
            void DeleteTenantFiles(string name, params string[] tenants)
            {
                foreach (var tenant in tenants) { var file = EdgeOutbox.NewPath(name + "." + tenant); if (System.IO.File.Exists(file)) System.IO.File.Delete(file); }
            }

            await Case("each tenant's messages, follow-ups and delayed messages stay in its own database", async () =>
            {
                var t = await TenantEndpoint("isolation", new[] { "acme", "globex" });
                try
                {
                    using (t.Endpoint)
                    {
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("acme")) await t.Endpoint.Send(new EdgeUowMessage { Text = "first" });
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("globex")) await t.Endpoint.Send(new EdgeUowMessage { Text = "other" });
                        await Eventually(() => t.Handled.Count == 4, "four messages: acme's first, follow-up and delayed, and globex's", 20);
                        var seen = t.Handled.OrderBy(x => x).ToArray();
                        That(seen.SequenceEqual(new[] { "delayed@acme", "first@acme", "follow-up@acme", "other@globex" }),
                            "every message was handled under its own tenant, which a follow-up and a delayed message keep: " + string.Join(",", seen));
                    }
                    That(await EdgeOutbox.Count(t.Path("acme"), "bq_inbox") == 3, "acme's database holds its three messages: " + await EdgeOutbox.Count(t.Path("acme"), "bq_inbox"));
                    That(await EdgeOutbox.Count(t.Path("globex"), "bq_inbox") == 1, "globex's database holds only its own: " + await EdgeOutbox.Count(t.Path("globex"), "bq_inbox"));
                    That(await EdgeOutbox.Count(t.Path("acme"), "bq_outbox") == 0 && await EdgeOutbox.Count(t.Path("globex"), "bq_outbox") == 0, "both outboxes were published");
                    // Retention visits every tenant in the catalogue, with no tenant set.
                    var purged = await t.Persistence.PurgeInbox(t.Name, DateTime.UtcNow.AddDays(1), 1000);
                    That(purged == 4, "retention removed the inbox rows of both tenants: " + purged);
                    That(await EdgeOutbox.Count(t.Path("acme"), "bq_inbox") == 0 && await EdgeOutbox.Count(t.Path("globex"), "bq_inbox") == 0, "and none are left");
                }
                finally { DeleteTenantFiles(t.Name, "acme", "globex"); }
            });
            await Case("a transaction opened inside a tenant scope stores its outbox rows in that tenant's database", async () =>
            {
                var t = await TenantEndpoint("transaction", new[] { "acme", "globex" });
                try
                {
                    using (t.Endpoint)
                    {
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("globex"))
                            await t.Endpoint.ExecuteTransaction(async session => await session.SendLocal(new EdgeUowMessage { Text = "in-transaction" }));
                        await Eventually(() => t.Handled.Contains("in-transaction@globex"), "the message sent in the transaction to be handled under its tenant", 15);
                    }
                    That(await EdgeOutbox.Count(t.Path("globex"), "bq_inbox") == 1 && await EdgeOutbox.Count(t.Path("acme"), "bq_inbox") == 0,
                        "only globex's database was used");
                }
                finally { DeleteTenantFiles(t.Name, "acme", "globex"); }
            });
            await Case("work with no tenant fails closed, and a delivery with no tenant or an unknown one goes to the error queue", async () =>
            {
                var t = await TenantEndpoint("reject", new[] { "acme" });
                try
                {
                    using (t.Endpoint)
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        Exception refusal = null;
                        try { await t.Endpoint.Send(new EdgeUowMessage { Text = "nobody" }); } catch (Exception ex) { refusal = ex; }
                        That(refusal != null, "a send outside any tenant scope is refused");
                        That(refusal is Bq.Messaging.MultiTenant.TenantNotSetException || refusal?.InnerException is Bq.Messaging.MultiTenant.TenantNotSetException,
                            "and the reason is that no tenant is set: " + refusal);
                        // An explicit tenant header is left alone, so a send on behalf of a named tenant works without a scope.
                        var explicitSend = new SendOptions();
                        explicitSend.SetHeader("tenant-id", "acme");
                        await t.Endpoint.Send(new EdgeUowMessage { Text = "explicit" }, explicitSend);
                        await Eventually(() => t.Handled.Contains("explicit@acme"), "the explicitly addressed message to be handled under acme", 10);
                        t.Handled.TryDequeue(out _);
                        var format = new Bq.Messaging.Serialization.MessagePackSerialization().CreateWireFormat();
                        void Deliver(string id, string tenant)
                        {
                            var props = channel.CreateBasicProperties();
                            props.MessageId = id; props.Type = EndpointConfiguration.MessageType(typeof(EdgeUowMessage));
                            props.ContentType = format.ContentType; props.Persistent = true;
                            if (tenant != null) props.Headers = new Dictionary<string, object> { ["tenant-id"] = tenant };
                            channel.BasicPublish("", t.Name, props, format.Serialize(typeof(EdgeUowMessage), new EdgeUowMessage { Text = id }));
                        }
                        Deliver("no-tenant", null);
                        Deliver("unknown-tenant", "initech");
                        var errors = new Dictionary<string, string>();
                        await Eventually(() =>
                        {
                            BasicGetResult r;
                            while ((r = channel.BasicGet(t.Name + ".error", true)) != null)
                                errors[r.BasicProperties.MessageId] = System.Text.Encoding.UTF8.GetString((byte[])r.BasicProperties.Headers["x-bq-failure-message"]);
                            return errors.Count == 2;
                        }, "both rejected messages in the error queue", 15);
                        That(errors["no-tenant"].Contains("no tenant") || errors["no-tenant"].Contains("tenant-id"), "a missing tenant is named: " + errors["no-tenant"]);
                        That(errors["unknown-tenant"].Contains("initech"), "an unknown tenant is named: " + errors["unknown-tenant"]);
                        That(t.Handled.IsEmpty, "no handler ran");
                    }
                }
                finally { DeleteTenantFiles(t.Name, "acme"); }
            });
            await Case("failed messages are inspected and replayed for one tenant, the current one", async () =>
            {
                var t = await TenantEndpoint("errors", new[] { "acme", "globex" });
                try
                {
                    using (t.Endpoint)
                    {
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("acme")) await t.Endpoint.Send(new EdgeUowMessage { Text = "boom" });
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("acme"))
                        {
                            IReadOnlyList<RecoveryRecord> errors = null;
                            await Eventually(() => (errors = t.Endpoint.ListErrors().GetAwaiter().GetResult()).Count == 1, "the failure to be recorded in acme's database", 15);
                            That(errors.Single().ExceptionMessage == "boom", "the record is acme's failure");
                        }
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("globex"))
                            That((await t.Endpoint.ListErrors()).Count == 0, "another tenant sees none of acme's failures");
                        var refused = false;
                        try { await t.Endpoint.ListErrors(); } catch (Bq.Messaging.MultiTenant.TenantNotSetException) { refused = true; }
                        That(refused, "inspection with no tenant is refused");
                    }
                }
                finally { DeleteTenantFiles(t.Name, "acme", "globex"); }
            });
            await Case("a tenant whose database is down does not stop the others", async () =>
            {
                var t = await TenantEndpoint("down", new[] { "acme", "down" });
                try
                {
                    using (t.Endpoint)
                    {
                        // Give the dispatcher time to try the tenant that is down, so the work below has to get past it.
                        await Task.Delay(1500);
                        using (Bq.Messaging.MultiTenant.Tenant.Scope("acme")) await t.Endpoint.Send(new EdgeUowMessage { Text = "first" });
                        await Eventually(() => t.Handled.Count == 3, "acme's three messages while another tenant's database is down", 20);
                        That(t.Handled.All(h => h.EndsWith("@acme")), "all were acme's: " + string.Join(",", t.Handled));
                    }
                }
                finally { DeleteTenantFiles(t.Name, "acme"); }
            });
            await Case("rows left in a tenant's outbox by a crash are published by the background sweep", async () =>
            {
                var t = await TenantEndpoint("sweep", new[] { "acme", "globex" });
                try
                {
                    using (t.Endpoint)
                    {
                        // Rows written straight into globex's database, as a process that crashed before publishing would leave them, with no activity in the tenant.
                        var store = new Bq.Persistence.Sqlite.SqlitePersistence(() => EdgeOutbox.Connect(t.Path("globex")), t.Name);
                        var format = new Bq.Messaging.Serialization.MessagePackSerialization().CreateWireFormat();
                        using (var session = await store.OpenSession())
                        {
                            foreach (var text in new[] { "left-1", "left-2" })
                                await session.StoreOutgoing(new OutgoingMessage
                                {
                                    Id = Guid.NewGuid().ToString("N"), Type = EndpointConfiguration.MessageType(typeof(EdgeUowMessage)), Exchange = "", RoutingKey = t.Name, Mandatory = true,
                                    Headers = new Dictionary<string, object> { ["tenant-id"] = "globex" }, Body = format.Serialize(typeof(EdgeUowMessage), new EdgeUowMessage { Text = text })
                                });
                            session.Commit();
                        }
                        await Eventually(() => t.Handled.Count == 2, "both left-behind rows to be published and handled", 20);
                        That(t.Handled.OrderBy(x => x).SequenceEqual(new[] { "left-1@globex", "left-2@globex" }), "handled under globex: " + string.Join(",", t.Handled));
                    }
                }
                finally { DeleteTenantFiles(t.Name, "acme", "globex"); }
            });

            foreach (var viaContainer in new[] { false, true })
            {
                var container = viaContainer;
                await Case("custom encryption service " + (container ? "registered in the container" : "registered with a factory"), async () =>
                {
                    EdgeReverseEncryption.Encrypted = EdgeReverseEncryption.Decrypted = 0;
                    var name = "bq.test.edge.encrypt." + Guid.NewGuid().ToString("N");
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                    if (container) configuration.RegisterComponents(components => components.RegisterSingleton<Bq.Messaging.IEncryptionService>(new EdgeReverseEncryption()));
                    else configuration.RegisterEncryptionService(() => new EdgeReverseEncryption());
                    configuration.Route(typeof(SecretMessage), name);
                    var wire = "";
                    configuration.MutateOutgoing = transport => { if (transport.TypeName == typeof(SecretMessage).FullName) wire = System.Text.Encoding.UTF8.GetString(transport.Body); };
                    var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                    configuration.Handle(() => new Handler<SecretMessage>((m, c) => { seen.TrySetResult(m.Secret.Value); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await endpoint.Send(new SecretMessage { Label = "l", Secret = new Bq.Messaging.WireEncryptedString { Value = "abc123" } });
                        That(await Within(seen.Task) == "abc123", "the handler reads the decrypted value");
                        That(wire.Contains("321cba") && !wire.Contains("abc123"), "the custom service's output is on the wire");
                        That(EdgeReverseEncryption.Encrypted == 1 && EdgeReverseEncryption.Decrypted == 1, "the service ran once each way");
                    }
                });
            }
            await Case("a container-registered data bus and serializer carry data bus values", async () =>
            {
                EdgeCountingDataBusSerializer.Serialized = EdgeCountingDataBusSerializer.Deserialized = 0;
                var bus = new EdgeMemoryDataBus();
                var name = "bq.test.edge.databus." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                configuration.RegisterComponents(components =>
                {
                    components.RegisterSingleton<Bq.Messaging.DataBus.IDataBus>(bus);
                    components.RegisterSingleton<Bq.Messaging.DataBus.IDataBusSerializer>(new EdgeCountingDataBusSerializer());
                });
                configuration.Route(typeof(DataBusMessage), name);
                var seen = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
                configuration.Handle(() => new Handler<DataBusMessage>((m, c) => { seen.TrySetResult(m.Attachment.Value.Length); return Task.CompletedTask; }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    await endpoint.Send(new DataBusMessage { Text = "t", Attachment = new Bq.Messaging.DataBusProperty<byte[]>(new byte[5000]) });
                    That(await Within(seen.Task) == 5000, "the value arrives through the container data bus");
                    That(bus.Values.Count == 1 && bus.Values.Values.Single().Length == 5000, "the value was stored in the custom bus");
                    That(EdgeCountingDataBusSerializer.Serialized == 1 && EdgeCountingDataBusSerializer.Deserialized == 1, "the custom serializer was used");
                }
            });
            await Case("a data bus download failure goes through recoverability", async () =>
            {
                var bus = new EdgeMemoryDataBus();
                var name = "bq.test.edge.databusfail." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), ImmediateRetries = 1 };
                configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                configuration.RegisterComponents(components => components.RegisterSingleton<Bq.Messaging.DataBus.IDataBus>(bus));
                configuration.Route(typeof(DataBusMessage), name);
                var handled = 0;
                configuration.Handle(() => new Handler<DataBusMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                var errored = new TaskCompletionSource<FailedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                configuration.Notifications.Errors.MessageSentToErrorQueue += (sender, message) => errored.TrySetResult(message);
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    bus.FailGet = true;
                    await endpoint.Send(new DataBusMessage { Text = "t", Attachment = new Bq.Messaging.DataBusProperty<byte[]>(new byte[10]) });
                    var failed = await Within(errored.Task);
                    That(failed.Exception.Message.Contains("data bus unavailable"), "the download failure is reported: " + failed.Exception.Message);
                    That(Volatile.Read(ref handled) == 0 && bus.Gets == 2, "the handler never ran and the download was retried once: " + bus.Gets);
                }
            });

            await Case("a saga with a timeout runs over JSON", async () =>
            {
                // OrderWorkflow's counters are shared with other groups that run in this process, so leave them as found.
                var savedTimeouts = OrderWorkflow.TimeoutCalls;
                var savedStarts = OrderWorkflow.StartAttempts;
                OrderWorkflow.TimeoutCalls = 0;
                try
                {
                var name = "bq.test.edge.jsonsaga." + Guid.NewGuid().ToString("N");
                var memory = new InMemoryPersistenceProvider(name);
                var configuration = new EndpointConfiguration(name, uri) { Persistence = memory };
                configuration.UseSerialization<Bq.Messaging.Serialization.JsonSerializer>();
                configuration.Route(typeof(StartOrder), name);
                configuration.Route(typeof(ChangeOrder), name);
                configuration.RegisterSaga<OrderWorkflow, OrderState>();
                var wire = new List<string>();
                configuration.MutateOutgoing = transport => { lock (wire) wire.Add(System.Text.Encoding.UTF8.GetString(transport.Body)); };
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    await endpoint.Send(new StartOrder { OrderId = "json-1", DelayMs = 200 });
                    await Eventually(() => Volatile.Read(ref OrderWorkflow.TimeoutCalls) == 1, "the saga timeout to fire");
                    await endpoint.Send(new ChangeOrder { OrderId = "json-1", Complete = true });
                    using (var session = await memory.OpenSession())
                        await Eventually(() => session.LoadSaga(typeof(OrderWorkflow).FullName, "json-1").GetAwaiter().GetResult() == null, "the completed saga to be deleted");
                    lock (wire) That(wire.Count >= 3 && wire.All(w => w.StartsWith("{")), "every message, the timeout included, is JSON on the wire: " + string.Join(" | ", wire));
                }
                }
                finally
                {
                    OrderWorkflow.TimeoutCalls = savedTimeouts;
                    OrderWorkflow.StartAttempts = savedStarts;
                }
            });

            await Case("an outbox row keeps its wire format when the endpoint changes format", async () =>
            {
                var name = "bq.test.edge.formatchange." + Guid.NewGuid().ToString("N");
                var sink = name + ".sink";
                var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), name + ".db");
                System.Data.Common.DbConnection Connect()
                {
                    var type = Type.GetType("Mono.Data.Sqlite.SqliteConnection, Mono.Data.Sqlite, Version=4.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756", true);
                    var connection = (System.Data.Common.DbConnection)Activator.CreateInstance(type);
                    connection.ConnectionString = "Data Source=" + path + ";Default Timeout=5";
                    return connection;
                }
                try
                {
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(sink, true, false, false);
                        var holdStore = new Bq.Persistence.Sqlite.SqlitePersistence(Connect, name);
                        await holdStore.Initialize();
                        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                        var json = new EndpointConfiguration(name, uri) { Persistence = new EdgeHeldPersistence(holdStore, gate.Task) };
                        json.UseSerialization<Bq.Messaging.Serialization.JsonSerializer>();
                        json.Route(typeof(EdgeUowMessage), sink);
                        using (var first = RabbitEndpoint.Start(json))
                        {
                            await first.ExecuteTransaction(session => session.Send(new EdgeUowMessage { Text = "stored-as-json" }));
                            That(channel.MessageCount(sink) == 0, "the held dispatcher has not published the row");
                        }
                        gate.TrySetResult(true);
                        var store = new Bq.Persistence.Sqlite.SqlitePersistence(Connect, name);
                        await store.Initialize();
                        var messagePack = new EndpointConfiguration(name, uri) { Persistence = store };
                        messagePack.Route(typeof(EdgeUowMessage), sink);
                        using (var second = RabbitEndpoint.Start(messagePack))
                        {
                            BasicGetResult delivered = null;
                            await Eventually(() => (delivered = channel.BasicGet(sink, true)) != null, "the stored row to be published after the restart");
                            var body = System.Text.Encoding.UTF8.GetString(delivered.Body.ToArray());
                            That(body.StartsWith("{") && body.Contains("stored-as-json"), "the body is still JSON: " + body);
                            That(delivered.BasicProperties.ContentType == "application/json", "the content type describes the stored body: " + delivered.BasicProperties.ContentType);
                        }
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });

            await Case("a forwarded message keeps the format it arrived in", async () =>
            {
                var name = "bq.test.edge.forwardformat." + Guid.NewGuid().ToString("N");
                var forward = name + ".forward";
                var receiver = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), ForwardReceivedMessagesAddress = forward };
                receiver.AddDeserializer<Bq.Messaging.Serialization.JsonSerializer>();
                receiver.Handle(() => new Handler<EdgeUowMessage>((m, c) => Task.CompletedTask));
                var senderName = name + ".sender";
                var sender = new EndpointConfiguration(senderName, uri) { Persistence = new InMemoryPersistenceProvider(senderName) };
                sender.UseSerialization<Bq.Messaging.Serialization.JsonSerializer>();
                sender.Route(typeof(EdgeUowMessage), name);
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(forward, true, false, false);
                    using (var receiving = RabbitEndpoint.Start(receiver))
                    using (var sending = RabbitEndpoint.Start(sender))
                    {
                        await sending.Send(new EdgeUowMessage { Text = "forwarded-json" });
                        BasicGetResult copy = null;
                        await Eventually(() => (copy = channel.BasicGet(forward, true)) != null, "the forwarded copy");
                        That(System.Text.Encoding.UTF8.GetString(copy.Body.ToArray()).StartsWith("{"), "the forwarded body is JSON");
                        That(copy.BasicProperties.ContentType == "application/json", "the forwarded copy says so: " + copy.BasicProperties.ContentType);
                    }
                }
            });

            await Case("a transactional session sends every kind of message when it commits", async () =>
            {
                var name = "bq.test.edge.transactional." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                UseNativeDelayWhenAvailable(configuration);
                configuration.Route(typeof(EdgeUowMessage), name);
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) =>
                {
                    seen[m.Text] = c.MessageHeaders.TryGetValue("h", out var header) ? header : "";
                    return Task.CompletedTask;
                }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    await endpoint.ExecuteTransaction(async session =>
                    {
                        await session.SendLocal(new EdgeUowMessage { Text = "local" });
                        await session.Send(new EdgeUowMessage { Text = "routed" });
                        await session.Send<EdgeUowMessage>(m => m.Text = "initialized");
                        await session.Send(name, new EdgeUowMessage { Text = "addressed" });
                        await session.SendAt(new EdgeUowMessage { Text = "at" }, DateTimeOffset.UtcNow.AddMilliseconds(200));
                        await session.SendLocalAt(new EdgeUowMessage { Text = "local-at" }, DateTimeOffset.UtcNow.AddMilliseconds(200));
                        await session.SendAt(name, new EdgeUowMessage { Text = "addressed-at" }, DateTimeOffset.UtcNow.AddMilliseconds(200));
                        var options = new SendOptions();
                        options.SetHeader("h", "from-options");
                        await session.Send(new EdgeUowMessage { Text = "options" }, options);
                        await session.WithHeader("h", "scoped").Send(new EdgeUowMessage { Text = "scoped" });
                        var immediate = new SendOptions();
                        immediate.RequireImmediateDispatch();
                        await session.Send(new EdgeUowMessage { Text = "immediate" }, immediate);
                        // Nothing subscribes to these events, so they only need to be accepted and stored without error.
                        await session.Publish(new EdgeUowMessage { Text = "published" });
                        await session.Publish<EdgeUowMessage>(m => m.Text = "published-init");
                        await session.Publish(new EdgeUowMessage { Text = "published-opts" }, new PublishOptions());
                        if (nativeDelay) await session.PublishAt(new EdgeUowMessage { Text = "published-at" }, DateTimeOffset.UtcNow.AddMilliseconds(200));
                        await session.WithHeader("h", "scoped-at").SendAt(new EdgeUowMessage { Text = "scoped-at" }, DateTimeOffset.UtcNow.AddMilliseconds(200));
                        await session.WithHeader("h", "scoped-local").SendLocal(new EdgeUowMessage { Text = "scoped-local" });
                    });
                    await Eventually(() => seen.Count == 12, "all twelve messages to be handled, saw " + string.Join(",", seen.Keys));
                    That(seen["options"] == "from-options" && seen["scoped"] == "scoped", "headers set through options and WithHeader arrive");
                }
            });

            await Case("an invoke-handler behavior reaches every context operation", async () =>
            {
                EdgeInvokeSender.Triggers = 0;
                var name = "bq.test.edge.invokectx." + Guid.NewGuid().ToString("N");
                EdgeInvokeSender.ForwardTarget = name + ".forward";
                EdgeInvokeSender.PublishAtSupported = nativeDelay;
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                UseNativeDelayWhenAvailable(configuration);
                configuration.Route(typeof(EdgeUowMessage), name);
                configuration.Pipeline.Register("edge-invoke-sender", new EdgeInvokeSender(), "Send from the invoke stage");
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, bool>();
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen[m.Text] = true; return Task.CompletedTask; }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(EdgeInvokeSender.ForwardTarget, true, false, false);
                    await endpoint.Send(new EdgeUowMessage { Text = "trigger" });
                    var expected = new[] { "trigger", "send", "send-init", "send-opts", "send-object-opts", "send-init-opts", "send-dest", "send-local",
                        "reply", "reply-init", "reply-opts", "reply-object-opts", "reply-init-opts", "send-at", "send-at-dest", "local-at" };
                    await Eventually(() => expected.All(seen.ContainsKey), "every message to be handled; missing " + string.Join(",", expected.Where(e => !seen.ContainsKey(e))));
                    await Eventually(() => channel.MessageCount(EdgeInvokeSender.ForwardTarget) == 1, "the forwarded copy");
                    That(Volatile.Read(ref EdgeInvokeSender.Triggers) == 1, "the behavior ran once for the trigger");
                }
            });

            await Case("message session extension methods and their static forms send through an endpoint", async () =>
            {
                var name = "bq.test.edge.sessionext." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                configuration.Route(typeof(EdgeUowMessage), name);
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, bool>();
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen[m.Text] = true; return Task.CompletedTask; }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    IMessageSession session = endpoint;
                    await IMessageSessionExtensions.Send(session, new EdgeUowMessage { Text = "ext-send" });
                    await IMessageSessionExtensions.Send(session, name, new EdgeUowMessage { Text = "ext-dest" });
                    await IMessageSessionExtensions.Send<EdgeUowMessage>(session, name, m => m.Text = "ext-dest-init");
                    await IMessageSessionExtensions.SendLocal(session, new EdgeUowMessage { Text = "ext-local" });
                    await IMessageSessionExtensions.SendLocal<EdgeUowMessage>(session, m => m.Text = "ext-local-init");
                    await IMessageSessionExtensions.Send<EdgeUowMessage>(session, m => m.Text = "ext-init");
                    await IMessageSessionExtensions.Publish(session, new EdgeUowMessage { Text = "ext-publish" });
                    await IMessageSessionExtensions.Publish<EdgeUowMessage>(session);
                    await IMessageSessionExtensions.Publish<EdgeUowMessage>(session, m => m.Text = "ext-publish-init");
                    await MessageOptionsExtensions.Send(session, new EdgeUowMessage { Text = "static-send" });
                    await MessageOptionsExtensions.Send(session, name, new EdgeUowMessage { Text = "static-dest" });
                    await MessageOptionsExtensions.Send<EdgeUowMessage>(session, name, m => m.Text = "static-dest-init");
                    await MessageOptionsExtensions.SendLocal(session, new EdgeUowMessage { Text = "static-local" });
                    await MessageOptionsExtensions.SendLocal<EdgeUowMessage>(session, m => m.Text = "static-local-init");
                    await MessageOptionsExtensions.Send<EdgeUowMessage>(session, m => m.Text = "static-init");
                    await MessageOptionsExtensions.Publish(session, new EdgeUowMessage { Text = "static-publish" });
                    await MessageOptionsExtensions.Publish<EdgeUowMessage>(session);
                    await MessageOptionsExtensions.Publish<EdgeUowMessage>(session, m => m.Text = "static-publish-init");
                    var expected = new[] { "ext-send", "ext-dest", "ext-dest-init", "ext-local", "ext-local-init", "ext-init",
                        "static-send", "static-dest", "static-dest-init", "static-local", "static-local-init", "static-init" };
                    await Eventually(() => expected.All(seen.ContainsKey), "every message to be handled; missing " + string.Join(",", expected.Where(e => !seen.ContainsKey(e))));
                    var send = new SendOptions(); send.SetDestination("d"); send.RouteReplyTo("r"); send.SetMessageId("id"); send.SetHeader("k", "v"); send.DelayDeliveryWith(TimeSpan.FromSeconds(1));
                    var reply = new ReplyOptions(); reply.SetDestination("d"); reply.RouteReplyTo("r");
                    var local = new SendOptions(); local.RouteToThisEndpoint(); local.DoNotDeliverBefore(DateTimeOffset.UtcNow);
                    That(MessageOptionsExtensions.GetDestination(send) == "d" && MessageOptionsExtensions.GetDestination(reply) == "d" &&
                        MessageOptionsExtensions.GetReplyToRoute(send) == "r" && MessageOptionsExtensions.GetReplyToRoute(reply) == "r", "routing getters");
                    That(MessageOptionsExtensions.GetMessageId(send) == "id" && MessageOptionsExtensions.GetHeaders(send)["k"] == "v", "id and header getters");
                    That(MessageOptionsExtensions.GetDeliveryDelay(send) == TimeSpan.FromSeconds(1) && MessageOptionsExtensions.GetDeliveryDate(local).HasValue &&
                        MessageOptionsExtensions.IsRoutingToThisEndpoint(local), "delivery getters");
                }
            });

            await Case("an endpoint's uniform session sends every kind of message", async () =>
            {
                var name = "bq.test.edge.uniform." + Guid.NewGuid().ToString("N");
                var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                UseNativeDelayWhenAvailable(configuration);
                configuration.Route(typeof(EdgeUowMessage), name);
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen[m.Text] = c.MessageHeaders.TryGetValue("h", out var header) ? header : ""; return Task.CompletedTask; }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                {
                    var session = endpoint.CreateUniformSession();
                    var due = DateTimeOffset.UtcNow.AddMilliseconds(200);
                    await session.Send(new EdgeUowMessage { Text = "u-send" });
                    await session.Send(name, new EdgeUowMessage { Text = "u-dest" });
                    await session.SendLocal(new EdgeUowMessage { Text = "u-local" });
                    await session.Send<EdgeUowMessage>(m => m.Text = "u-init");
                    await session.Send(new EdgeUowMessage { Text = "u-opts" }, new SendOptions());
                    await session.Send((object)new EdgeUowMessage { Text = "u-object-opts" }, new SendOptions());
                    await session.Send<EdgeUowMessage>(m => m.Text = "u-init-opts", new SendOptions());
                    await session.SendAt(new EdgeUowMessage { Text = "u-at" }, due);
                    await session.SendAt(name, new EdgeUowMessage { Text = "u-at-dest" }, due);
                    await session.SendLocalAt(new EdgeUowMessage { Text = "u-local-at" }, due);
                    await session.Publish(new EdgeUowMessage { Text = "u-publish" });
                    await session.Publish<EdgeUowMessage>(m => m.Text = "u-publish-init");
                    await session.Publish(new EdgeUowMessage { Text = "u-publish-opts" }, new PublishOptions());
                    if (nativeDelay) await session.PublishAt(new EdgeUowMessage { Text = "u-publish-at" }, due);
                    await session.WithHeader("h", "scoped").Send(new EdgeUowMessage { Text = "u-header" });
                    var expected = new[] { "u-send", "u-dest", "u-local", "u-init", "u-opts", "u-object-opts", "u-init-opts", "u-at", "u-at-dest", "u-local-at", "u-header" };
                    await Eventually(() => expected.All(seen.ContainsKey), "every message to be handled; missing " + string.Join(",", expected.Where(e => !seen.ContainsKey(e))));
                    That(seen["u-header"] == "scoped", "a header added to the session travels with its messages");
                }
            });
            foreach (var failing in new[] { "throwing policy", "policy naming the endpoint's own queue" })
            {
                var mode = failing;
                await Case("a recoverability " + mode + " falls back to the default", async () =>
                {
                    var name = "bq.test.edge.policyfail." + Guid.NewGuid().ToString("N");
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name), ImmediateRetries = 0 };
                    configuration.Recoverability.DelayedRetries = new TimeSpan[0];
                    if (mode == "throwing policy") configuration.Recoverability.Policy = context => throw new InvalidOperationException("policy bug");
                    else configuration.Recoverability.CustomPolicy((config, error) => RecoverabilityAction.MoveToError(name));
                    configuration.Route(typeof(EdgeUowMessage), name);
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => throw new ApplicationException("handler failed")));
                    var errored = new TaskCompletionSource<FailedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                    configuration.Notifications.Errors.MessageSentToErrorQueue += (sender, message) => errored.TrySetResult(message);
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await endpoint.Send(new EdgeUowMessage { Text = "x" });
                        var failed = await Within(errored.Task);
                        That(failed.ErrorQueue == name + ".error", "the message reaches the default error queue: " + failed.ErrorQueue);
                    }
                });
            }

            await Case("seeded outbox rows are published in the order they were stored", async () =>
            {
                var name = "bq.test.edge.outbox.order." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var expected = Enumerable.Range(0, 200).Select(i => i.ToString("D4")).ToList();
                    await EdgeOutbox.Seed(store, name, expected);
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store };
                    var received = new List<string>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { lock (received) received.Add(m.Text); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await Eventually(() => { lock (received) return received.Count == expected.Count; }, "all " + expected.Count + " rows to be handled", OnSimulator ? 15 : 60);
                        lock (received) That(received.SequenceEqual(expected), "rows arrive in stored order, first out of order: " + string.Join(",", received.Where((t, i) => t != expected[i]).Take(3)));
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the dispatched rows to leave the outbox");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("the dispatcher sends a backlog in batches, with one confirm for each", async () =>
            {
                var name = "bq.test.edge.outbox.batches." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var counting = new EdgeBatchCounting(store);
                    var expected = Enumerable.Range(0, 120).Select(i => i.ToString("D4")).ToList();
                    await EdgeOutbox.Seed(store, name, expected);
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = counting, OutboxBatchSize = 50 };
                    var received = new List<string>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { lock (received) received.Add(m.Text); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await Eventually(() => { lock (received) return received.Count == expected.Count; }, "all rows to be handled", OnSimulator ? 15 : 60);
                        lock (received) That(received.SequenceEqual(expected), "batching keeps the stored order");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the outbox to empty");
                        That(counting.Largest == 50 && counting.Batches >= 3 && counting.Batches <= 6 && counting.Singles == 0,
                            "rows came in batches of up to 50, not one at a time: " + counting.Batches + " batches, largest " + counting.Largest + ", " + counting.Singles + " singles");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("a batch size of one confirms every row on its own", async () =>
            {
                var name = "bq.test.edge.outbox.single." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var counting = new EdgeBatchCounting(store);
                    await EdgeOutbox.Seed(store, name, Enumerable.Range(0, 10).Select(i => "s" + i));
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = counting, OutboxBatchSize = 1 };
                    var handled = 0;
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await Eventually(() => Volatile.Read(ref handled) == 10, "all ten rows to be handled");
                        That(counting.Batches == 0 && counting.Singles >= 10, "no batch was used: " + counting.Batches + " batches, " + counting.Singles + " singles");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("an unroutable row in the middle of a batch is deferred and the rest of the batch is sent", async () =>
            {
                var name = "bq.test.edge.outbox.midbatch." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    await EdgeOutbox.Seed(store, name, new[] { "before" });
                    await EdgeOutbox.Seed(store, name + ".nowhere", new[] { "unroutable" });
                    await EdgeOutbox.Seed(store, name, new[] { "after1", "after2" });
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store };
                    var received = new List<string>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { lock (received) received.Add(m.Text); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        await Eventually(() => { lock (received) return received.Count == 3; }, "the three routable rows of the batch to be sent");
                        lock (received) That(received.SequenceEqual(new[] { "before", "after1", "after2" }), "in stored order");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 1, "only the unroutable row to remain");
                        var pending = (await store.ListPendingDispatches(DispatchKind.Outbox, 10)).Single();
                        That(pending.Attempts >= 1 && pending.LastError != null, "it is deferred with its reason: " + pending.LastError);
                        channel.QueueDeclare(name + ".nowhere", true, false, false);
                        await Eventually(() => channel.MessageCount(name + ".nowhere") == 1, "the deferred row to be delivered once its queue exists");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });

            await Case("batching beats one confirm per row when confirms are slow", async () =>
            {
                if (!await Chaos()) { Console.WriteLine("Slow-confirm test skipped: needs rabbit-sim (BQ_RABBITSIM_HTTP)."); return; }
                async Task<double> Drain(int batchSize)
                {
                    var name = "bq.test.edge.slowconfirm." + batchSize + "." + Guid.NewGuid().ToString("N");
                    var path = EdgeOutbox.NewPath(name);
                    try
                    {
                        var store = await EdgeOutbox.Store(path, name);
                        await EdgeOutbox.Seed(store, name, Enumerable.Range(0, 100).Select(i => "r" + i));
                        var configuration = new EndpointConfiguration(name, uri) { Persistence = store, OutboxBatchSize = batchSize };
                        var handled = 0;
                        configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                        var watch = System.Diagnostics.Stopwatch.StartNew();
                        using (var endpoint = RabbitEndpoint.Start(configuration))
                        {
                            await Eventually(() => Volatile.Read(ref handled) == 100, "all 100 rows with batch size " + batchSize, 60);
                            return watch.Elapsed.TotalSeconds;
                        }
                    }
                    finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
                }
                try
                {
                    await Chaos("{\"confirm_delay_ms\":25}");
                    var single = await Drain(1);
                    var batched = await Drain(100);
                    Console.WriteLine("With a 25 ms confirm, 100 rows took " + single.ToString("0.0") + "s one confirm at a time and " + batched.ToString("0.0") + "s in batches.");
                    That(single >= 2.0, "one confirm per row pays the delay for every row: " + single.ToString("0.0") + "s");
                    That(batched * 3 < single, "a batch pays it once: " + batched.ToString("0.0") + "s against " + single.ToString("0.0") + "s");
                }
                finally { await Chaos(); }
            });
            await Case("a batch the broker declines stays pending and is published again, and the inbox absorbs the duplicates", async () =>
            {
                if (!await Chaos("{\"nack_next\":0}")) { Console.WriteLine("Nack test skipped: needs rabbit-sim (BQ_RABBITSIM_HTTP)."); return; }
                var name = "bq.test.edge.nackbatch." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var counting = new EdgeBatchCounting(store);
                    await EdgeOutbox.Seed(store, name, Enumerable.Range(0, 5).Select(i => "n" + i));
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = counting };
                    configuration.DispatchPollInterval = TimeSpan.FromMilliseconds(50);
                    var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen.AddOrUpdate(m.Text, 1, (key, n) => n + 1); return Task.CompletedTask; }));
                    await Chaos("{\"nack_next\":1}");
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await Eventually(() => seen.Count == 5, "all five rows to be handled after the declined publish");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the outbox to empty");
                        await Task.Delay(500);
                        That(counting.Batches >= 2, "the batch was fetched again after the broker declined one of its rows: " + counting.Batches);
                        That(seen.Values.All(n => n == 1), "the rows that were published twice were handled once: " + string.Join(",", seen.Where(p => p.Value != 1).Select(p => p.Key + "x" + p.Value)));
                    }
                }
                finally { await Chaos("{\"nack_next\":0}"); if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("a confirm that never comes in time raises the critical error and recovers when confirms are fast again", async () =>
            {
                if (!await Chaos()) { Console.WriteLine("Stalled-confirm test skipped: needs rabbit-sim (BQ_RABBITSIM_HTTP)."); return; }
                var name = "bq.test.edge.stalledconfirm." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    await EdgeOutbox.Seed(store, name, Enumerable.Range(0, 3).Select(i => "c" + i));
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store, PublisherConfirmTimeout = TimeSpan.FromMilliseconds(200), CriticalErrorThreshold = TimeSpan.FromMilliseconds(500) };
                    configuration.DispatchPollInterval = TimeSpan.FromMilliseconds(50);
                    var critical = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                    configuration.OnCriticalError = (error, exception) => { critical.TrySetResult(error); return Task.CompletedTask; };
                    var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen.AddOrUpdate(m.Text, 1, (key, n) => n + 1); return Task.CompletedTask; }));
                    await Chaos("{\"confirm_delay_ms\":1500}");
                    var traced = new System.IO.StringWriter();
                    var listener = new System.Diagnostics.TextWriterTraceListener(traced);
                    System.Diagnostics.Trace.Listeners.Add(listener);
                    try
                    {
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        var first = await Task.WhenAny(critical.Task, Task.Delay(10000));
                        listener.Flush();
                        That(first == critical.Task, "no critical error within 10s; handled " + seen.Count + ", outbox rows " + await EdgeOutbox.Count(path, "bq_outbox") + "; trace: " + traced.ToString().Replace(Environment.NewLine, " | "));
                        That(await critical.Task == "Outbox dispatcher", "the callback names the outbox dispatcher");
                        await Chaos();
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the rows to be recorded once confirms are fast again", 30);
                        await Eventually(() => seen.Count == 3, "all three rows to be handled");
                        await Task.Delay(1500);
                        That(seen.Values.All(n => n == 1), "publishing repeatedly while unconfirmed handled each row once: " + string.Join(",", seen.Select(p => p.Key + "x" + p.Value)));
                    }
                    }
                    finally { System.Diagnostics.Trace.Listeners.Remove(listener); }
                }
                finally { await Chaos(); if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });

            await Case("a large outbox backlog drains completely and exactly once", async () =>
            {
                var name = "bq.test.edge.outbox.volume." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var total = 1500;
                    await EdgeOutbox.Seed(store, name, Enumerable.Range(0, total).Select(i => "v" + i));
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store, MaxConcurrency = 8 };
                    var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen.AddOrUpdate(m.Text, 1, (key, n) => n + 1); return Task.CompletedTask; }));
                    var started = DateTime.UtcNow;
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        for (var i = 0; i < 1200 && seen.Count < total; i++) await Task.Delay(100);
                        That(seen.Count == total, "all rows were handled, got " + seen.Count + " of " + total);
                        Console.WriteLine("Outbox backlog of " + total + " drained in " + (DateTime.UtcNow - started).TotalSeconds.ToString("0.0") + "s on " + (OnSimulator ? "the simulator" : "a real broker") + ".");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the outbox to empty");
                        await Task.Delay(500);
                        That(seen.Values.All(n => n == 1), "no row was handled twice: " + string.Join(",", seen.Where(p => p.Value != 1).Take(3).Select(p => p.Key)));
                        // Delivered outbox rows are removed at once. Inbox rows stay for the retention period, 30 days by default.
                        That(await EdgeOutbox.Count(path, "bq_outbox") == 0, "delivered rows leave the outbox");
                        That(await EdgeOutbox.Count(path, "bq_inbox") == total, "the inbox keeps a row for every handled message within the retention period, found " + await EdgeOutbox.Count(path, "bq_inbox"));
                        That(await EdgeOutbox.Count(path, "bq_recovery") == 0, "no recovery records were created by successful handling");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("a crash between publishing and recording the dispatch causes a duplicate publish that the inbox absorbs", async () =>
            {
                var name = "bq.test.edge.outbox.crash." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    await EdgeOutbox.Seed(store, name, new[] { "only-once" });
                    var crashing = new EdgeMarkFailsPersistence(store, failures: 2);
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = crashing };
                    var handled = 0;
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    {
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the row to be recorded as dispatched on the third attempt");
                        await Eventually(() => Volatile.Read(ref handled) >= 1, "the message to be handled");
                        await Task.Delay(1000);
                        That(crashing.Handed >= 3, "the same row was handed to the dispatcher again after each failed recording: " + crashing.Handed);
                        That(Volatile.Read(ref handled) == 1, "the receiver's inbox absorbed the duplicate publishes, handled " + handled);
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("two dispatchers on one database deliver every row exactly once", async () =>
            {
                var name = "bq.test.edge.outbox.twonodes." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var storeOne = await EdgeOutbox.Store(path, name);
                    var storeTwo = await EdgeOutbox.Store(path, name);
                    const int total = 120;
                    await EdgeOutbox.Seed(storeOne, name, Enumerable.Range(0, total).Select(i => "n" + i));
                    var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
                    EndpointConfiguration Node(IPersistenceProvider store)
                    {
                        var configuration = new EndpointConfiguration(name, uri) { Persistence = store };
                        configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen.AddOrUpdate(m.Text, 1, (key, n) => n + 1); return Task.CompletedTask; }));
                        return configuration;
                    }
                    using (var first = RabbitEndpoint.Start(Node(storeOne)))
                    using (var second = RabbitEndpoint.Start(Node(storeTwo)))
                    {
                        for (var i = 0; i < 300 && seen.Count < total; i++) await Task.Delay(100);
                        That(seen.Count == total, "every row was handled, got " + seen.Count + " of " + total);
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the outbox to empty");
                        await Task.Delay(1000);
                        That(seen.Values.All(n => n == 1), "the inbox kept each row to one handling: " + string.Join(",", seen.Where(p => p.Value != 1).Take(3).Select(p => p.Key + "x" + p.Value)));
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("two dispatchers on Oracle publish every row exactly once", async () =>
            {
                var connectionString = Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_STRING");
                if (string.IsNullOrEmpty(connectionString)) { Console.WriteLine("Oracle two-dispatcher test skipped: set BQ_ORACLE_*."); return; }
                var assembly = System.Reflection.Assembly.LoadFrom(Environment.GetEnvironmentVariable("BQ_ORACLE_DRIVER_ASSEMBLY"));
                var connectionType = assembly.GetType(Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_TYPE"), throwOnError: true);
                System.Data.Common.DbConnection Connect()
                {
                    var connection = (System.Data.Common.DbConnection)Activator.CreateInstance(connectionType);
                    connection.ConnectionString = connectionString;
                    return connection;
                }
                var name = "bq-oracle-twonodes-" + Guid.NewGuid().ToString("N");
                const int total = 150;
                var seeding = new Bq.Persistence.Oracle.OraclePersistence(Connect, name);
                await EdgeOutbox.Seed(seeding, name, Enumerable.Range(0, total).Select(i => "o" + i));
                var published = new int[1];
                var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
                EndpointConfiguration Node()
                {
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new EdgeCountingOracle(new Bq.Persistence.Oracle.OraclePersistence(Connect, name), published) };
                    configuration.DispatchPollInterval = TimeSpan.FromMilliseconds(50);
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { seen.AddOrUpdate(m.Text, 1, (key, n) => n + 1); return Task.CompletedTask; }));
                    return configuration;
                }
                using (var first = RabbitEndpoint.Start(Node()))
                using (var second = RabbitEndpoint.Start(Node()))
                {
                    for (var i = 0; i < 600 && seen.Count < total; i++) await Task.Delay(100);
                    That(seen.Count == total, "every row was handled, got " + seen.Count + " of " + total);
                    await Task.Delay(1500);
                    That(Volatile.Read(ref published[0]) == total, "row locking kept each row to one publish, saw " + published[0] + " publishes for " + total + " rows");
                    That(seen.Values.All(n => n == 1), "and each was handled once");
                }
            });
            await Case("an unroutable row at the head of the outbox is deferred so the rows behind it are sent", async () =>
            {
                var name = "bq.test.edge.outbox.poison." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    await EdgeOutbox.Seed(store, name + ".nowhere", new[] { "unroutable" });
                    var behind = Enumerable.Range(0, 5).Select(i => "behind" + i).ToList();
                    await EdgeOutbox.Seed(store, name, behind);
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store };
                    var received = new List<string>();
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { lock (received) received.Add(m.Text); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        await Eventually(() => { lock (received) return received.Count == behind.Count; }, "the rows behind the unroutable one to be sent");
                        lock (received) That(received.SequenceEqual(behind), "they keep their own order");
                        // The handlers have seen the rows, but their outbox entries are removed a moment later.
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 1, "only the deferred row to remain pending");
                        var pending = (await store.ListPendingDispatches(DispatchKind.Outbox, 10)).Single();
                        That(pending.RoutingKey == name + ".nowhere" && pending.Attempts >= 1 && pending.LastError.Contains("unroutable") && pending.RetryAfterUtc.HasValue,
                            "the row stays pending with its attempt count and reason: attempts " + pending.Attempts + ", " + pending.LastError);
                        // Once the queue exists the deferred row is retried and delivered.
                        channel.QueueDeclare(name + ".nowhere", true, false, false);
                        await Eventually(() => channel.MessageCount(name + ".nowhere") == 1, "the deferred row to be delivered after its retry time");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_outbox").GetAwaiter().GetResult() == 0, "the outbox to empty");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("an unroutable row is deferred on Oracle too", async () =>
            {
                var connectionString = Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_STRING");
                if (string.IsNullOrEmpty(connectionString)) { Console.WriteLine("Oracle deferral test skipped: set BQ_ORACLE_*."); return; }
                var assembly = System.Reflection.Assembly.LoadFrom(Environment.GetEnvironmentVariable("BQ_ORACLE_DRIVER_ASSEMBLY"));
                var connectionType = assembly.GetType(Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_TYPE"), throwOnError: true);
                System.Data.Common.DbConnection Connect()
                {
                    var connection = (System.Data.Common.DbConnection)Activator.CreateInstance(connectionType);
                    connection.ConnectionString = connectionString;
                    return connection;
                }
                var name = "bq-oracle-poison-" + Guid.NewGuid().ToString("N");
                var store = new Bq.Persistence.Oracle.OraclePersistence(Connect, name);
                await EdgeOutbox.Seed(store, name + ".nowhere", new[] { "unroutable" });
                var behind = Enumerable.Range(0, 5).Select(i => "behind" + i).ToList();
                await EdgeOutbox.Seed(store, name, behind);
                var configuration = new EndpointConfiguration(name, uri) { Persistence = store };
                configuration.DispatchPollInterval = TimeSpan.FromMilliseconds(50);
                var received = new List<string>();
                configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { lock (received) received.Add(m.Text); return Task.CompletedTask; }));
                using (var endpoint = RabbitEndpoint.Start(configuration))
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    await Eventually(() => { lock (received) return received.Count == behind.Count; }, "the rows behind the unroutable one to be sent on Oracle");
                    lock (received) That(received.SequenceEqual(behind), "they keep their own order");
                    channel.QueueDeclare(name + ".nowhere", true, false, false);
                    await Eventually(() => channel.MessageCount(name + ".nowhere") == 1, "the deferred row to be delivered after its retry time");
                }
            });

            await Case("an endpoint removes old inbox and finished recovery rows in the background", async () =>
            {
                var name = "bq.test.edge.retention." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store, ImmediateRetries = 0 };
                    configuration.Recoverability.DelayedRetries = new[] { TimeSpan.FromMilliseconds(100) };
                    configuration.DispatchPollInterval = TimeSpan.FromMilliseconds(50);
                    configuration.Retention.InboxRetention = TimeSpan.FromSeconds(1);
                    configuration.Retention.RecoveryRetention = TimeSpan.FromSeconds(1);
                    configuration.Retention.CleanupInterval = TimeSpan.FromMilliseconds(100);
                    configuration.Route(typeof(EdgeUowMessage), name);
                    var handled = 0;
                    var failedOnce = 0;
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) =>
                    {
                        if (m.Text == "flaky" && Interlocked.Increment(ref failedOnce) == 1) throw new ApplicationException("first attempt fails");
                        Interlocked.Increment(ref handled);
                        return Task.CompletedTask;
                    }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        for (var i = 0; i < 3; i++) await endpoint.Send(new EdgeUowMessage { Text = "ok" + i });
                        await endpoint.Send(new EdgeUowMessage { Text = "flaky" });
                        await Eventually(() => Volatile.Read(ref handled) == 4, "all four messages to be handled");
                        That(await EdgeOutbox.Count(path, "bq_inbox") > 0, "handled messages are in the inbox at first");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_inbox").GetAwaiter().GetResult() == 0, "old inbox rows to be removed");
                        await Eventually(() => EdgeOutbox.Count(path, "bq_recovery").GetAwaiter().GetResult() == 0, "the sent retry record to be removed");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("retention can be switched off so the inbox keeps every row", async () =>
            {
                var name = "bq.test.edge.retentionoff." + Guid.NewGuid().ToString("N");
                var path = EdgeOutbox.NewPath(name);
                try
                {
                    var store = await EdgeOutbox.Store(path, name);
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = store };
                    configuration.Retention.InboxRetention = null;
                    configuration.Retention.RecoveryRetention = null;
                    configuration.Retention.CleanupInterval = TimeSpan.FromMilliseconds(100);
                    configuration.Route(typeof(EdgeUowMessage), name);
                    var handled = 0;
                    configuration.Handle(() => new Handler<EdgeUowMessage>((m, c) => { Interlocked.Increment(ref handled); return Task.CompletedTask; }));
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    {
                        await endpoint.Send(new EdgeUowMessage { Text = "kept" });
                        await Eventually(() => Volatile.Read(ref handled) == 1, "the message to be handled");
                        await Task.Delay(1500);
                        That(await EdgeOutbox.Count(path, "bq_inbox") == 1, "the inbox row is kept");
                    }
                }
                finally { if (System.IO.File.Exists(path)) System.IO.File.Delete(path); }
            });
            await Case("Oracle removes old inbox and finished recovery records", async () =>
            {
                var connectionString = Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_STRING");
                if (string.IsNullOrEmpty(connectionString)) { Console.WriteLine("Oracle retention test skipped: set BQ_ORACLE_*."); return; }
                var assembly = System.Reflection.Assembly.LoadFrom(Environment.GetEnvironmentVariable("BQ_ORACLE_DRIVER_ASSEMBLY"));
                var connectionType = assembly.GetType(Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_TYPE"), throwOnError: true);
                System.Data.Common.DbConnection Connect()
                {
                    var connection = (System.Data.Common.DbConnection)Activator.CreateInstance(connectionType);
                    connection.ConnectionString = connectionString;
                    return connection;
                }
                var name = "bq-oracle-retention-" + Guid.NewGuid().ToString("N");
                await RetentionCheck.Run(new Bq.Persistence.Oracle.OraclePersistence(Connect, name), name);
            });

            foreach (var mode in new[] { "immediate", "delayed", "discard", "error" })
            {
                var current = mode;
                await Case("satellite recoverability: " + current, async () =>
                {
                    var name = "bq.test.edge.sat." + current + "." + Guid.NewGuid().ToString("N");
                    EdgeSatelliteModes.Queue = name + ".satellite";
                    EdgeSatelliteModes.Mode = current;
                    EdgeSatelliteModes.Calls.Clear();
                    var configuration = new EndpointConfiguration(name, uri) { Persistence = new InMemoryPersistenceProvider(name) };
                    configuration.EnableFeature<EdgeSatelliteModes>();
                    using (var endpoint = RabbitEndpoint.Start(configuration))
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        var props = channel.CreateBasicProperties();
                        props.MessageId = Guid.NewGuid().ToString("N");
                        channel.BasicPublish("", EdgeSatelliteModes.Queue, false, props, new byte[] { 1 });
                        if (current == "error")
                        {
                            await GetFrom(channel, name + ".error");
                            That(EdgeSatelliteModes.Calls.Count == 1, "an error move stops after one attempt");
                        }
                        else if (current == "discard")
                        {
                            await Eventually(() => EdgeSatelliteModes.Calls.Count == 1, "the first attempt");
                            await Task.Delay(500);
                            That(channel.BasicGet(name + ".error", true) == null && channel.BasicGet(EdgeSatelliteModes.Queue, true) == null, "a discarded message is gone");
                        }
                        else
                        {
                            await Eventually(() => EdgeSatelliteModes.Calls.Count == 2, "the retried attempt");
                            That(true, "retried");
                        }
                    }
                });
            }

            return cases.ToList();
        }

        public static async Task RunOne(Uri uri, string name)
        {
            var found = (await Collect(uri)).Single(c => c.Name == name);
            // The simulator's clock is shared and stays advanced, which would make a delivery time look due, so every case starts at real time.
            await ResetClock();
            await found.Body();
        }

        public static async Task Run(Uri uri)
        {
            var failures = new List<string>();
            var all = await Collect(uri);
            foreach (var (name, body) in all)
            {
                try { await ResetClock(); await body(); }
                catch (Exception ex) { failures.Add(name + ": " + ex.Message); }
            }
            Console.WriteLine("Edge broker tests: " + (all.Count - failures.Count) + " passed, " + failures.Count + " failed.");
            if (failures.Count > 0) throw new Exception("Failed edge broker cases:" + Environment.NewLine + string.Join(Environment.NewLine, failures));
        }
    }
}
