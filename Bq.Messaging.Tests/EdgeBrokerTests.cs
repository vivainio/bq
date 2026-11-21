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
