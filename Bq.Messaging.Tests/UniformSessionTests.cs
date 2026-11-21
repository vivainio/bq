using System;
using System.Linq;
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
            config.Route<UniformWork>(name);
            config.CustomConversationIdStrategy(context =>
            {
                Assert(context.Message.MessageType == typeof(UniformStart) && context.Message.Instance is UniformStart,
                    "strategy sees the outgoing contract and runs only when an ID is needed");
                Assert(!context.Headers.ContainsKey("x-bq-start-conversation"), "strategy sees only outgoing headers");
                return context.Headers.TryGetValue("workflow-id", out var workflowId)
                    ? ConversationId.Custom(workflowId)
                    : ConversationId.Default;
            });
            var encryption = new TestBodyEncryption();
            config.EnableMessageEncryption(encryption);
            config.Recoverability.DelayedRetries = new TimeSpan[0];
            var processed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var newConversation = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var errorAttempted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int received = 0;
            string parentCorrelation = null;
            string parentConversation = null;
            string parentMessageId = null;
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
                    parentConversation = context.GetHeader(MessageHeaders.ConversationId);
                    parentMessageId = context.MessageId;
                    cachedFromHandler = session;
                    bool rejected = false;
                    try { await cachedFromOutside.SendLocal(new UniformWork { Text = "leak" }); }
                    catch (InvalidOperationException) { rejected = true; }
                    Assert(rejected, "outside session rejected within handler");
                    await Task.Yield();
                    Action<UniformWork> initialize = work => work.Text = "committed";
                    await session.Send(initialize);
                    var options = new SendOptions();
                    options.StartNewConversation("separate-workflow");
                    options.DelayDeliveryWith(TimeSpan.Zero);
                    await session.Send(new UniformWork { Text = "separate" }, options);
                    if (message.Fail) { errorAttempted.TrySetResult(true); throw new Exception("rollback"); }
                });
            });
            config.Handle(() => new Handler<UniformWork>((message, context) =>
            {
                Assert(context.GetHeader(MessageHeaders.RelatedTo) == parentMessageId, "child records its causing message");
                Assert(!context.Headers.ContainsKey("x-bq-start-conversation"), "send option stays off the wire");
                Interlocked.Increment(ref received);
                if (message.Text == "separate")
                {
                    Assert(context.GetHeader(MessageHeaders.PreviousConversationId) == parentConversation, "scheduled send records the previous conversation");
                    newConversation.TrySetResult(context.GetHeader(MessageHeaders.ConversationId));
                }
                else
                {
                    Assert(message.Text == "committed", "session initializes the message before sending");
                    Assert(context.GetHeader(MessageHeaders.ConversationId) == parentConversation, "child inherits the conversation");
                    processed.TrySetResult(context.CorrelationId);
                }
                return Task.CompletedTask;
            }));
            try
            {
                endpoint = RabbitEndpoint.Start(config);
                using (endpoint)
                {
                    cachedFromOutside = endpoint.CreateUniformSession();
                    var start = new SendOptions();
                    start.RouteToThisEndpoint();
                    start.StartNewConversation();
                    start.DoNotDeliverBefore(DateTimeOffset.UtcNow);
                    start.SetHeader("workflow-id", "root-workflow");
                    await cachedFromOutside.Send(new UniformStart(), start);
                    await Within(processed.Task, "uniform SendLocal delivery");
                    await Within(newConversation.Task, "scheduled delivery in a new conversation");
                    Assert(parentConversation == "root-workflow" && await newConversation.Task == "separate-workflow",
                        "the strategy supplies the root ID and an explicit new conversation bypasses it");
                    Assert(await processed.Task == parentCorrelation, "uniform session preserves handler correlation");
                    bool rejected = false;
                    try { await cachedFromHandler.SendLocal(new UniformWork()); }
                    catch (InvalidOperationException) { rejected = true; }
                    Assert(rejected, "handler session rejected after its scope ends");
                    await cachedFromOutside.SendLocal(new UniformStart { Fail = true });
                    await Within(errorAttempted.Task, "failed handler ran");
                    Assert(Guid.TryParse(parentConversation, out _), "strategy can request a generated ID");
                    for (var i = 0; i < 120 && (await endpoint.ListErrors()).Count == 0; i++) await Task.Delay(100);
                    Assert((await endpoint.ListErrors()).Count == 1, "failed handler recorded");
                    await Task.Delay(1200);
                    Assert(received == 2, "failed handler did not dispatch its local or scheduled messages");
                    Assert(encryption.Decrypted >= 3, "direct and transactional messages pass through body decryption");
                    using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        var invalidBody = new byte[] { 0xff, 0x00, 0x7e };
                        var properties = channel.CreateBasicProperties();
                        properties.MessageId = "invalid-encrypted-body";
                        properties.Type = typeof(UniformStart).FullName;
                        properties.ContentType = "application/x-msgpack";
                        channel.BasicPublish("", name, properties, invalidBody);
                        BasicGetResult error = null;
                        for (var i = 0; i < 120; i++)
                        {
                            error = channel.BasicGet(name + ".error", true);
                            if (error?.BasicProperties.MessageId == properties.MessageId) break;
                            await Task.Delay(100);
                        }
                        Assert(error?.BasicProperties.MessageId == properties.MessageId &&
                            error.Body.ToArray().SequenceEqual(invalidBody), "decryption failure preserves the original broker body");
                        var failure = (await endpoint.ListErrors()).Single(record => record.MessageId == properties.MessageId);
                        Assert(failure.ExceptionMessage == "Message body decryption failed." &&
                            !failure.ExceptionDetail.Contains("sensitive provider diagnostic"),
                            "body decryption diagnostics are sanitized before recovery");
                    }
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

        sealed class TestBodyEncryption : IMessageBodyEncryptionService
        {
            int decrypted;
            public int Decrypted => Volatile.Read(ref decrypted);

            public byte[] Encrypt(byte[] body) => System.Text.Encoding.ASCII.GetBytes(Convert.ToBase64String(body));

            public byte[] Decrypt(byte[] body)
            {
                if (body.Length > 0 && body[0] == 0xff) throw new FormatException("sensitive provider diagnostic");
                var decoded = Convert.FromBase64String(System.Text.Encoding.ASCII.GetString(body));
                Interlocked.Increment(ref decrypted);
                return decoded;
            }
        }
    }
}
