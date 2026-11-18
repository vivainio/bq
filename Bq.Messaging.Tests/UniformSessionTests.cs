using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public class UniformStart { public bool Fail { get; set; } }
    public class UniformWork { public string Text { get; set; } }
    static class UniformSessionTests
    {
        static async Task Within(Task task, string label)
        {
            if (await Task.WhenAny(task, Task.Delay(12000)) != task) throw new Exception("Timeout: " + label);
            await task;
        }
        static void Assert(bool value, string label) { if (!value) throw new Exception("Failed: " + label); }
        public static async Task Run(Uri uri, Func<string, System.Data.Common.DbConnection> createConnection)
        {
            var name = "bq.uniform." + Guid.NewGuid().ToString("N");
            var path = Path.Combine(Path.GetTempPath(), name + ".db");
            var store = new SqlitePersistence(() => createConnection(path), name);
            await store.Initialize();
            var config = new EndpointConfiguration(name, uri) { Persistence = store, ImmediateRetries = 0 };
            config.Recoverability.DelayedRetries = new TimeSpan[0];
            var processed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var errorAttempted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int received = 0;
            string parentCorrelation = null;
            IUniformSession cachedFromHandler = null;
            IUniformSession cachedFromOutside = null;
            RabbitEndpoint endpoint = null;
            config.Handle<UniformStart>(context =>
            {
                // Construct scoped dependencies from the current handler context.
                var session = context.UniformSession;
                return new Handler<UniformStart>(async (message, context) =>
                {
                    parentCorrelation = context.CorrelationId;
                    cachedFromHandler = session;
                    bool rejected = false;
                    try { await cachedFromOutside.SendLocal(new UniformWork { Text = "leak" }); }
                    catch (InvalidOperationException) { rejected = true; }
                    Assert(rejected, "outside session rejected within handler");
                    await Task.Yield();
                    await session.SendLocal(new UniformWork { Text = "committed" });
                    if (message.Fail) { errorAttempted.TrySetResult(true); throw new Exception("rollback"); }
                });
            });
            config.Handle(() => new Handler<UniformWork>((message, context) =>
            {
                Interlocked.Increment(ref received);
                processed.TrySetResult(context.CorrelationId);
                return Task.CompletedTask;
            }));
            try
            {
                endpoint = RabbitEndpoint.Start(config);
                using (endpoint)
                {
                    cachedFromOutside = endpoint.CreateUniformSession();
                    await cachedFromOutside.SendLocal(new UniformStart());
                    await Within(processed.Task, "uniform SendLocal delivery");
                    Assert(await processed.Task == parentCorrelation, "uniform session preserves handler correlation");
                    bool rejected = false;
                    try { await cachedFromHandler.SendLocal(new UniformWork()); }
                    catch (InvalidOperationException) { rejected = true; }
                    Assert(rejected, "handler session rejected after its scope ends");
                    await cachedFromOutside.SendLocal(new UniformStart { Fail = true });
                    await Within(errorAttempted.Task, "failed handler ran");
                    for (var i = 0; i < 120 && (await endpoint.ListErrors()).Count == 0; i++) await Task.Delay(100);
                    Assert((await endpoint.ListErrors()).Count == 1, "failed handler recorded");
                    await Task.Delay(1200);
                    Assert(received == 1, "failed handler did not dispatch its local message");
                    await endpoint.StopAsync();
                    rejected = false;
                    try { await cachedFromOutside.SendLocal(new UniformStart()); }
                    catch (InvalidOperationException) { rejected = true; }
                    Assert(rejected, "outside session rejected after endpoint shutdown");
                }
                Console.WriteLine("Uniform session and local send tests passed.");
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel()) { channel.QueueDelete(name); channel.QueueDelete(name + ".error"); }
            }
        }
    }
}
