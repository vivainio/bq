using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public sealed class BeginFulfillment { public string OrderId { get; set; } }
    public sealed class FulfillmentStarted { public string OrderId { get; set; } }
    public sealed class FulfillmentRecorded { public string OrderId { get; set; } }
    public sealed class FulfillmentDeadline { public string OrderId { get; set; } }
    public sealed class FulfillmentCompleted { public string OrderId { get; set; } }
    public sealed class WorkflowBarrier { }

    public sealed class FulfillmentState
    {
        public string OrderId { get; set; }
        public bool Recorded { get; set; }
        public bool DeadlineReached { get; set; }
    }

    public sealed class FulfillmentSaga : Saga<FulfillmentState>, IAmStartedByMessages<BeginFulfillment>,
        IHandleMessages<FulfillmentRecorded>, IHandleTimeouts<FulfillmentDeadline>
    {
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<FulfillmentState> mapper)
        {
            mapper.MapSaga(data => data.OrderId).ToMessage<BeginFulfillment>(message => message.OrderId);
            mapper.ConfigureMapping<FulfillmentRecorded>(message => message.OrderId).ToSaga(data => data.OrderId);
        }

        public async Task Handle(BeginFulfillment message, IMessageHandlerContext context)
        {
            if (Data.OrderId != message.OrderId) throw new InvalidOperationException("Saga data must start with its correlation property.");
            await context.Publish(new FulfillmentStarted { OrderId = Data.OrderId });
            await RequestTimeout(context, TimeSpan.FromMilliseconds(250), new FulfillmentDeadline { OrderId = Data.OrderId });
        }

        public Task Handle(FulfillmentRecorded message, IMessageHandlerContext context)
        {
            Data.Recorded = true;
            return CompleteIfReady(message.OrderId, context);
        }

        public Task Timeout(FulfillmentDeadline message, IMessageHandlerContext context)
        {
            Data.DeadlineReached = true;
            return CompleteIfReady(message.OrderId, context);
        }

        async Task CompleteIfReady(string orderId, IMessageHandlerContext context)
        {
            if (!Data.Recorded || !Data.DeadlineReached) return;
            await context.Publish(new FulfillmentCompleted { OrderId = orderId });
            MarkAsComplete();
        }
    }

    partial class Program
    {
        static async Task OrderWorkflowEndToEndTest(Uri uri)
        {
            var prefix = "bq.test.order." + Guid.NewGuid().ToString("N");
            var intakeName = prefix + ".intake";
            var fulfillmentName = prefix + ".fulfillment";
            var recordingName = prefix + ".recording";
            var exchange = prefix + ".events";
            var observer = prefix + ".observer";
            var intakePath = Path.Combine(Path.GetTempPath(), intakeName + ".db");
            var fulfillmentPath = Path.Combine(Path.GetTempPath(), fulfillmentName + ".db");
            var recordingPath = Path.Combine(Path.GetTempPath(), recordingName + ".db");
            var intakeStore = new SqlitePersistence(() => Sqlite(intakePath), intakeName);
            var fulfillmentStore = new SqlitePersistence(() => Sqlite(fulfillmentPath), fulfillmentName);
            var recordingStore = new SqlitePersistence(() => Sqlite(recordingPath), recordingName);
            await intakeStore.Initialize();
            await fulfillmentStore.Initialize();
            await recordingStore.Initialize();
            await CreateWorkflowTable(intakePath);
            await CreateWorkflowTable(recordingPath);

            var intake = new EndpointConfiguration(intakeName, uri) { Persistence = intakeStore, EventsExchange = exchange };
            intake.SendOnly();
            intake.Route<BeginFulfillment>(fulfillmentName);
            var fulfillment = new EndpointConfiguration(fulfillmentName, uri) { Persistence = fulfillmentStore, EventsExchange = exchange };
            fulfillment.RegisterSaga<FulfillmentSaga, FulfillmentState>();

            var recording = new EndpointConfiguration(recordingName, uri)
            {
                Persistence = recordingStore, EventsExchange = exchange, ImmediateRetries = 0,
                DispatchPollInterval = TimeSpan.FromMilliseconds(100)
            };
            recording.Recoverability.DelayedRetries = new[] { TimeSpan.FromSeconds(4) };
            recording.Route<FulfillmentRecorded>(fulfillmentName);
            var attempts = 0;
            var retryMessageId = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            recording.Handle(() => new Handler<FulfillmentStarted>(async (message, context) =>
            {
                await InsertApplicationWork(context.PersistenceSession, message.OrderId);
                await context.Send<FulfillmentRecorded>(recorded => recorded.OrderId = message.OrderId);
                if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("recording temporarily unavailable");
                retryMessageId.TrySetResult(context.MessageId);
            }), subscribe: true);
            var barrier = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            recording.Handle(() => new Handler<WorkflowBarrier>((message, context) =>
            {
                barrier.TrySetResult(true);
                return Task.CompletedTask;
            }));

            using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                try
                {
                    channel.ExchangeDeclare(exchange, ExchangeType.Topic, true);
                    channel.QueueDeclare(observer, false, false, true);
                    channel.QueueBind(observer, exchange, typeof(FulfillmentStarted).FullName);
                    channel.QueueBind(observer, exchange, typeof(FulfillmentCompleted).FullName);
                    channel.ConfirmSelect();
                    using (var receiver = RabbitEndpoint.Start(fulfillment))
                    using (var producer = RabbitEndpoint.Start(intake))
                    {
                        const string orderId = "order-1";
                        using (var firstRecorder = RabbitEndpoint.Start(recording))
                        {
                            await producer.ExecuteTransaction(async session =>
                            {
                                using (var command = session.Connection.CreateCommand())
                                {
                                    command.Transaction = session.Transaction;
                                    command.CommandText = "INSERT INTO application_work (name) VALUES ('order-1')";
                                    await command.ExecuteNonQueryAsync();
                                }
                                await session.Send(new BeginFulfillment { OrderId = orderId });
                            });
                            await WaitFor(async () => await CountMigrationRows(recordingPath, "bq_recovery") == 1,
                                "workflow retry durably scheduled");
                            await firstRecorder.StopAsync();
                        }

                        Assert(await CountMigrationRows(intakePath, "application_work") == 1, "intake committed its business row");
                        Assert(await CountMigrationRows(recordingPath, "application_work") == 0 &&
                            await CountMigrationRows(recordingPath, "bq_inbox") == 0 &&
                            await CountMigrationRows(recordingPath, "bq_outbox") == 0,
                            "failed recording attempt rolled back its business row, inbox claim and response");
                        Assert(await CountMigrationRows(fulfillmentPath, "bq_saga") == 1,
                            "fulfillment saga is still waiting for recording confirmation");
                        var original = channel.BasicGet(observer, true);
                        Assert(original != null && original.BasicProperties.Type == typeof(FulfillmentStarted).FullName,
                            "capture the original event for broker redelivery");

                        using (var restarted = RabbitEndpoint.Start(recording))
                        {
                            Assert(await Within(retryMessageId.Task) == original.BasicProperties.MessageId,
                                "the restarted endpoint retries the original event identity");
                            BasicGetResult completion = null;
                            await WaitFor(() =>
                            {
                                completion = channel.BasicGet(observer, true);
                                return Task.FromResult(completion != null);
                            }, "workflow completion event");
                            var options = MessagePack.MessagePackSerializerOptions.Standard
                                .WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
                            Assert(completion.BasicProperties.Type == typeof(FulfillmentCompleted).FullName &&
                                MessagePack.MessagePackSerializer.Deserialize<FulfillmentCompleted>(completion.Body, options).OrderId == orderId,
                                "the saga publishes one correlated completion after confirmation and timeout");
                            Assert(await CountMigrationRows(fulfillmentPath, "bq_saga") == 0 &&
                                await CountMigrationRows(fulfillmentPath, "bq_saga_timeout") == 0,
                                "completion removes persisted saga state and the timeout is consumed");

                            channel.BasicPublish("", recordingName, original.BasicProperties, original.Body);
                            // Also redeliver the successful retry cycle to exercise inbox deduplication.
                            original.BasicProperties.Headers["x-bq-retry-cycle"] = 1;
                            channel.BasicPublish("", recordingName, original.BasicProperties, original.Body);
                            Assert(channel.WaitForConfirms(TimeSpan.FromSeconds(5)), "duplicate deliveries reached the recording queue");
                            await producer.Send(recordingName, new WorkflowBarrier());
                            await Within(barrier.Task);
                            await restarted.StopAsync();
                            Assert(Volatile.Read(ref attempts) == 2 &&
                                await CountMigrationRows(recordingPath, "application_work") == 1 &&
                                await CountMigrationRows(recordingPath, "bq_outbox") == 0 &&
                                await CountMigrationRows(recordingPath, "bq_recovery") == 1 &&
                                channel.BasicGet(observer, true) == null,
                                "redelivery creates no duplicate business work or outgoing messages");
                        }
                    }
                }
                finally
                {
                    foreach (var queue in new[] { fulfillmentName, recordingName })
                    {
                        channel.QueueDelete(queue);
                        channel.QueueDelete(queue + ".error");
                    }
                    channel.QueueDelete(observer);
                    channel.ExchangeDelete(exchange);
                    foreach (var path in new[] { intakePath, fulfillmentPath, recordingPath }) File.Delete(path);
                }
            }
            Console.WriteLine("Cross-endpoint order workflow and restart test passed.");
        }

        static async Task CreateWorkflowTable(string path)
        {
            using (var db = Sqlite(path))
            {
                await db.OpenAsync();
                using (var command = db.CreateCommand())
                {
                    command.CommandText = "CREATE TABLE application_work (name TEXT PRIMARY KEY)";
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
