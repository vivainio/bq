using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging.Features;
using Bq.Messaging.UnitOfWork;
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
        static readonly List<string> failures = new List<string>();
        static int passed;
        static void That(bool v, string m) { if (!v) throw new Exception(m); }
        static async Task Case(string name, Func<Task> body)
        {
            try { await body(); passed++; } catch (Exception ex) { failures.Add(name + ": " + ex.Message); }
        }
        static async Task<T> Within<T>(Task<T> task)
        {
            if (await Task.WhenAny(task, Task.Delay(15000)) != task) throw new Exception("Timed out waiting for the result.");
            return await task;
        }
        static async Task Eventually(Func<bool> condition, string what)
        {
            for (var i = 0; i < 150; i++) { if (condition()) return; await Task.Delay(100); }
            throw new Exception("Timed out: " + what);
        }
        static async Task<BasicGetResult> GetFrom(IModel channel, string queue)
        {
            BasicGetResult r = null;
            await Eventually(() => (r = channel.BasicGet(queue, true)) != null, "a message on " + queue);
            return r;
        }

        public static async Task Run(Uri uri)
        {
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
                        await client.PostAsync(http + "/chaos/clock/advance?ms=10000", null);
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

            Console.WriteLine("Edge broker tests: " + passed + " passed, " + failures.Count + " failed.");
            if (failures.Count > 0) throw new Exception("Failed edge broker cases:" + Environment.NewLine + string.Join(Environment.NewLine, failures));
        }
    }
}
