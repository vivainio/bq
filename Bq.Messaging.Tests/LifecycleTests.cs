using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    // An in-memory provider isolates concurrent dispatch from SQLite's single-writer lock.
    sealed class LifecycleStore : IPersistenceProvider, IRecoveryPersistence
    {
        readonly ConcurrentDictionary<string, bool> inbox = new ConcurrentDictionary<string, bool>();
        public Task<IPersistenceSession> OpenSession() => Task.FromResult<IPersistenceSession>(new Session(inbox));
        public Task<OutgoingMessage> NextOutgoing(string e) => Task.FromResult<OutgoingMessage>(null);
        public Task MarkDispatched(string e, string id) => Task.CompletedTask;
        public Task<RecoveryRecord> NextRecovery(string e) => Task.FromResult<RecoveryRecord>(null);
        public Task MarkRecoveryDispatched(string e, string id) => Task.CompletedTask;
        public Task<RecoveryRecord> GetError(string e, string id) => Task.FromResult<RecoveryRecord>(null);
        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string e, int l) => Task.FromResult<IReadOnlyList<RecoveryRecord>>(new List<RecoveryRecord>());
        public Task<bool> ReplayError(string e, string id, RecoveryRecord r) => Task.FromResult(false);
        sealed class Session : IPersistenceSession, IRecoverySession
        {
            readonly ConcurrentDictionary<string, bool> inbox;
            string id;
            public Session(ConcurrentDictionary<string, bool> inbox) { this.inbox = inbox; }
            public DbConnection Connection => null;
            public DbTransaction Transaction => null;
            public Task<bool> TryClaimMessage(string e, string id) { this.id = e + "/" + id; return Task.FromResult(!inbox.ContainsKey(this.id)); }
            public Task<bool> HasRecovery(string e, string id) => Task.FromResult(false);
            public Task StoreRecovery(string e, RecoveryRecord r) => throw new NotSupportedException();
            public Task StoreOutgoing(OutgoingMessage m) => throw new NotSupportedException();
            public Task StoreTimeout(OutgoingMessage m, DateTime t) => throw new NotSupportedException();
            public Task<SagaRecord> LoadSaga(string t, string id) => throw new NotSupportedException();
            public Task SaveSaga(string t, string id, byte[] data, long? version) => throw new NotSupportedException();
            public Task DeleteSaga(string t, string id, long version) => throw new NotSupportedException();
            public void Commit() { if (id != null) inbox.TryAdd(id, true); }
            public void Dispose() { }
        }
    }
    static class LifecycleTests
    {
        static TaskCompletionSource<bool> Signal() => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        static async Task Within(Task task, string label)
        {
            if (await Task.WhenAny(task, Task.Delay(10000)) != task) throw new Exception("Timeout: " + label);
            await task;
        }
        static void Assert(bool value, string label) => SoftChecks.Assert(value, label);
        static void Publish(IModel channel, string queue, string text)
        {
            var properties = channel.CreateBasicProperties();
            properties.MessageId = Guid.NewGuid().ToString("N");
            properties.Type = typeof(Command).FullName;
            properties.ContentType = "application/x-msgpack";
            var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
            channel.BasicPublish("", queue, properties, MessagePackSerializer.Serialize(new Command { Text = text }, options));
        }
        public static async Task Run(Uri uri)
        {
            var queue = "bq.lifecycle." + Guid.NewGuid().ToString("N");
            var store = new LifecycleStore();
            var bothStarted = Signal(); var release = Signal(); var third = Signal();
            int started = 0, active = 0, maxActive = 0, stale = 0;
            var config = new EndpointConfiguration(queue, uri) { Persistence = store, MaxConcurrency = 2 };
            config.Handle(() => new Handler<Command>(async (m, c) =>
            {
                if (m.Text == "stale") Interlocked.Increment(ref stale);
                var count = Interlocked.Increment(ref active);
                if (count > maxActive) maxActive = count;
                if (Interlocked.Increment(ref started) == 2) bothStarted.TrySetResult(true);
                if (m.Text == "third") third.TrySetResult(true);
                await release.Task;
                Interlocked.Decrement(ref active);
            }));
            try
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    using (var setup = RabbitEndpoint.Start(config)) await setup.StopAsync();
                    config.DoNotCreateQueues();
                    config.InputQueueArguments["x-max-length"] = 100;
                    channel.ConfirmSelect();
                    Publish(channel, queue, "stale");
                    Assert(channel.WaitForConfirms(TimeSpan.FromSeconds(5)) &&
                        channel.QueueDeclarePassive(queue).MessageCount == 1, "startup purge test has a queued message");
                    config.PurgeOnStartup(true);
                    var endpoint = RabbitEndpoint.Start(config);
                    config.PurgeOnStartup(false);
                    Publish(channel, queue, "first"); Publish(channel, queue, "second"); Publish(channel, queue, "third");
                    await Within(bothStarted.Task, "two handlers run concurrently");
                    await Task.Delay(250);
                    Assert(started == 2 && maxActive == 2, "prefetch limits concurrency");
                    var stop = endpoint.StopAsync();
                    await Task.Delay(250);
                    Assert(!stop.IsCompleted, "graceful stop waits for handlers");
                    release.TrySetResult(true);
                    await Within(stop, "graceful stop");
                    Assert(ReferenceEquals(stop, endpoint.StopAsync()), "stop is idempotent");
                    endpoint.Dispose();
                    using (var restarted = RabbitEndpoint.Start(config))
                    {
                        await Within(third.Task, "pending delivery after restart");
                        await restarted.StopAsync();
                    }
                    Assert(Volatile.Read(ref stale) == 0, "startup purge removes the queued delivery before consumer startup");

                    var cancelled = Signal(); var entered = Signal(); var deliveredAgain = Signal();
                    var forced = new EndpointConfiguration(queue, uri) { Persistence = store };
                    forced.Handle(() => new Handler<Command>(async (m, c) =>
                    {
                        entered.TrySetResult(true);
                        var completed = Signal();
                        using (c.CancellationToken.Register(() => { cancelled.TrySetResult(true); completed.TrySetResult(true); }))
                            await completed.Task;
                    }));
                    var forcedEndpoint = RabbitEndpoint.Start(forced);
                    Publish(channel, queue, "forced");
                    await Within(entered.Task, "forced stop handler enters");
                    using (var timeout = new CancellationTokenSource(200))
                    {
                        try { await forcedEndpoint.StopAsync(timeout.Token); throw new Exception("Expected cancellation"); }
                        catch (OperationCanceledException) { }
                    }
                    await Within(cancelled.Task, "forced stop cancels handler token");
                    forcedEndpoint.Dispose();
                    var restart = new EndpointConfiguration(queue, uri) { Persistence = store };
                    restart.Handle(() => new Handler<Command>((m, c) => { deliveredAgain.TrySetResult(true); return Task.CompletedTask; }));
                    using (var restarted = RabbitEndpoint.Start(restart))
                    {
                        await Within(deliveredAgain.Task, "handler that returns after forced cancellation is redelivered");
                        await restarted.StopAsync();
                    }
                }
                Console.WriteLine("Lifecycle and concurrency tests passed.");
            }
            finally
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel()) { channel.QueueDelete(queue); channel.QueueDelete(queue + ".error"); }
            }
        }
    }
}
