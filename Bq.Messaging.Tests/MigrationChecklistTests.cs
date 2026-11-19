using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;

namespace Bq.Messaging.Tests
{
    public sealed class RuntimeSubscriptionEvent { public int Value { get; set; } }

    sealed class PilotMessageConvention : IMessageConvention
    {
        public string Name => nameof(PilotMessageConvention);
        public bool IsCommandType(Type type) => type == typeof(Command);
        public bool IsEventType(Type type) => type == typeof(IPilotEvent);
        public bool IsMessageType(Type type) => type == typeof(WorkflowBarrier);
    }

    public interface IPilotMessage { string Text { get; set; } }
    public interface IPilotEvent : IPilotMessage { int Count { get; set; } }
    public sealed class ReplacementPilotEvent : IPilotEvent
    {
        public string Text { get; set; }
        public int Count { get; set; }
    }

    public sealed class ObservePilotEvent : Behavior<IIncomingLogicalMessageContext>
    {
        public override async Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next)
        {
            if (context.Message.MessageType == typeof(IPilotEvent) && context.Message.Instance is IPilotEvent pilot)
            {
                context.Extensions.TryGet<int>("pilot-behaviors", out var count);
                context.Extensions.Set("pilot-behaviors", count + 1);
                if (count == 0)
                {
                    context.Headers["migration-group"] = "observed";
                    context.UpdateMessageInstance(new ReplacementPilotEvent { Text = pilot.Text + " observed", Count = pilot.Count });
                    var options = new ReplyOptions();
                    options.SetDestination(context.GetHeader("migration-reply"));
                    await context.Reply<Response>(response => response.Text = context.MessageId, options);
                }
                else if (!(context.Message.Instance is ReplacementPilotEvent) || context.Headers["migration-group"] != "observed")
                {
                    throw new InvalidOperationException("The next behavior must see the replacement instance.");
                }
            }
            if (context.MessageHandled) throw new InvalidOperationException("The handler has not run yet.");
            await next();
            if (!context.MessageHandled) throw new InvalidOperationException("The handler completed but the context did not report it.");
        }
    }

    public sealed class UnexpectedPilotBehavior : Behavior<IIncomingLogicalMessageContext>
    {
        public override Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next) =>
            throw new InvalidOperationException("A replaced or removed behavior ran.");
    }

    partial class Program
    {
        static async Task MigrationPipelineTests(Uri uri)
        {
            var name = "bq.test.migration." + Guid.NewGuid().ToString("N");
            var path = Path.Combine(Path.GetTempPath(), name + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();
            using (var db = Sqlite(path))
            {
                await db.OpenAsync();
                using (var command = db.CreateCommand())
                {
                    command.CommandText = "CREATE TABLE application_work (name TEXT PRIMARY KEY)";
                    await command.ExecuteNonQueryAsync();
                }
            }

            var config = new EndpointConfiguration(name, uri) { Persistence = store, ImmediateRetries = 0 };
            config.MutateOutgoing = transport =>
            {
                if (transport.TypeName != typeof(LocalMessage).FullName || transport.Headers.ContainsKey("immediate")) return;
                Assert(transport.IncomingMessage != null, "outbox mutator sees the incoming handler context");
                transport.TypeName = "migration.framed";
                transport.Headers["migration-framing"] = "v1";
                transport.Body = new byte[] { 0x7e }.Concat(transport.Body).ToArray();
            };
            config.Recoverability.DelayedRetries = new[] { TimeSpan.FromHours(1) };
            ImmediateRetryMessage retryNotification = null;
            config.Recoverability()
                .Immediate(settings => settings.OnMessageBeingRetried(retry =>
                {
                    retryNotification = retry;
                    Array.Clear(retry.Body, 0, retry.Body.Length);
                    throw new InvalidOperationException("notification failure");
                }))
                .AddUnrecoverableException(typeof(FatalFailureException))
                .CustomPolicy((settings, failure) =>
                {
                    if (failure.Exception.Message == "retry the unit")
                    {
                        Assert(failure.ImmediateProcessingFailures == 1 && failure.DelayedDeliveriesPerformed == 0 &&
                            failure.Message.Body.Length > 0 && failure.Message.MessageId != null &&
                            failure.Extensions.Get<IncomingMessageContext>() != null,
                            "custom policy receives counters, the wire message and attempt extensions");
                        return RecoverabilityAction.ImmediateRetry();
                    }
                    if (failure.Exception.Message == "unit of work save failed")
                        return DefaultRecoverabilityPolicy.Invoke(settings, failure);
                    if (failure.Exception.Message == "discard this message") return RecoverabilityAction.Discard("test drop");
                    if (failure.Exception.Message == "replacement: handler failure")
                        return RecoverabilityAction.MoveToError(name + ".alternate-error");
                    return RecoverabilityAction.MoveToError(settings.Failed.ErrorQueue);
                });
            var currentScope = new AsyncLocal<IncomingMessageContext>();
            var closedScopes = 0;
            string forwardedId = null;
            var forwardDestinations = new[] { name + ".forward.first", name + ".forward.second" };
            var resolvedHandlers = 0;
            config.Incoming.Use(async (incoming, next) =>
            {
                Assert(currentScope.Value == null, "message attempt starts with a clean host scope");
                currentScope.Value = incoming;
                Assert(!incoming.Extensions.TryGet<IncomingMessageContext>(out _), "retry starts with a fresh extensions bag");
                incoming.Extensions.Set(incoming);
                try { await next(); }
                finally
                {
                    currentScope.Value = null;
                    Interlocked.Increment(ref closedScopes);
                }
            });
            config.Incoming.Use(async (incoming, next) =>
            {
                var message = (Command)incoming.Message;
                if (message.Text == "skip") return;
                if (message.Text == "retry")
                {
                    incoming.Items.Add("retry", incoming.ImmediateRetries);
                    if (incoming.ImmediateRetries == 1)
                        Assert(retryNotification?.RetryAttempt == 1 && retryNotification.MessageId == incoming.HandlerContext.MessageId &&
                            retryNotification.Exception.Message == "retry the unit" && retryNotification.Body.Length > 0,
                            "retry notification receives the failure and its snapshot before the next attempt");
                }
                incoming.Items.Add("unit", message.Text);
                try
                {
                    await next();
                    if (message.Text == "duplicate-next") await next();
                }
                catch (InvalidOperationException ex) when (message.Text == "replace")
                {
                    throw new FatalFailureException("replacement: " + ex.Message);
                }
                catch (InvalidOperationException) when (message.Text == "swallow")
                {
                    // Returning successfully from a behavior must not commit a failed handler's work.
                }
                if (message.Text == "after-next") throw new FatalFailureException("behavior failed after next");
            });
            config.Incoming.Use((incoming, next) =>
            {
                Assert((string)incoming.Items["unit"] == ((Command)incoming.Message).Text,
                    "incoming behaviors run in registration order and share items");
                return next();
            });
            config.Incoming.BeforeCommit(async (incoming, session) =>
            {
                Assert(ReferenceEquals(currentScope.Value, incoming) &&
                    ReferenceEquals(incoming.Extensions.Get<IncomingMessageContext>(), incoming),
                    "before-commit callback shares the host scope and extensions bag");
                var unit = (string)incoming.Items["unit"];
                await InsertApplicationWork(session, unit + ":commit");
                if (unit == "before-commit") throw new FatalFailureException("unit of work save failed");
                if (unit == "retry" && (int)incoming.Items["retry"] == 0)
                {
                    ((Command)incoming.Message).Text = "mutated during failed attempt";
                    throw new InvalidOperationException("retry the unit");
                }
            });
            config.Handle<Command>(handlerContext =>
            {
                var scope = currentScope.Value;
                Assert(scope != null && ReferenceEquals(scope.HandlerContext, handlerContext) &&
                    ReferenceEquals(handlerContext.Extensions.Get<IncomingMessageContext>(), scope),
                    "handler factory resolves inside the host scope for this attempt");
                Interlocked.Increment(ref resolvedHandlers);
                return new Handler<Command>(async (message, context) =>
                {
                    Assert(ReferenceEquals(context.SynchronizedStorageSession, context.PersistenceSession),
                        "the synchronized storage view shares the handler transaction");
                    await InsertApplicationWork(context.SynchronizedStorageSession, message.Text + ":handler");
                    if (message.Text == "commit" || message.Text == "replace")
                    {
                        if (message.Text == "commit") forwardedId = context.MessageId;
                        foreach (var destination in forwardDestinations) await context.ForwardCurrentMessageTo(destination);
                    }
                    // An absent destination keeps the committed outbox entry available for inspection.
                    await context.Send(name + ".absent", new LocalMessage { Text = message.Text });
                    if (message.Text == "replace")
                    {
                        var send = new SendOptions();
                        send.SetDestination(name + ".immediate");
                        send.SetHeader("immediate", true);
                        send.RequireImmediateDispatch();
                        await context.Send(new LocalMessage { Text = message.Text }, send);
                        var publish = new PublishOptions();
                        publish.SetHeader("immediate", true);
                        publish.RequireImmediateDispatch();
                        await context.Publish(new Event { Text = message.Text }, publish);
                        var reply = new ReplyOptions();
                        reply.SetDestination(name + ".immediate");
                        reply.SetMessageId("immediate-reply-id");
                        reply.RouteReplyTo(name + ".reply-next");
                        reply.SetHeader("immediate", true);
                        reply.RequireImmediateDispatch();
                        await context.Reply(new Response { Text = message.Text }, reply);
                        throw new InvalidOperationException("handler failure");
                    }
                    if (message.Text == "discard") throw new InvalidOperationException("discard this message");
                    if (message.Text == "swallow") throw new InvalidOperationException("caught handler failure");
                });
            });

            try
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                using (var endpoint = RabbitEndpoint.Start(config))
                {
                    foreach (var destination in forwardDestinations)
                        channel.QueueDeclare(destination, false, true, true);
                    channel.QueueDeclare(name + ".immediate", false, true, true);
                    channel.QueueBind(name + ".immediate", config.EventsExchange, typeof(Event).FullName);
                    channel.QueueDeclare(name + ".alternate-error", false, true, true);
                    foreach (var scenario in new[] { "skip", "replace", "before-commit", "after-next", "commit", "retry", "swallow", "duplicate-next", "discard" })
                        await endpoint.SendLocal(new Command { Text = scenario });

                    await WaitFor(async () => (await endpoint.ListErrors()).Count == 5 &&
                        await CountMigrationRows(path, "bq_inbox") == 4, "pipeline cases reach durable outcomes");
                    Assert(Volatile.Read(ref closedScopes) == 10 && Volatile.Read(ref resolvedHandlers) == 9,
                        "each attempt closes its host scope and the short-circuited attempt skips handler resolution");
                    var errors = await endpoint.ListErrors();
                    Assert(errors.Where(error => error.ExceptionType == typeof(InvalidOperationException).FullName)
                        .Select(error => error.ExceptionMessage).OrderBy(message => message).SequenceEqual(
                            new[] { "An incoming behavior can invoke next only once per attempt.", "caught handler failure" }),
                        "recoverability sees the caught failure and duplicate continuation");
                    Assert(errors.Where(error => error.ExceptionType == typeof(FatalFailureException).FullName)
                        .Select(error => error.ExceptionMessage).OrderBy(message => message).SequenceEqual(
                            new[] { "behavior failed after next", "replacement: handler failure", "unit of work save failed" }),
                        "recoverability sees callback and behavior exceptions, including a replacement");

                    using (var db = Sqlite(path))
                    {
                        await db.OpenAsync();
                        using (var command = db.CreateCommand())
                        {
                            command.CommandText = "SELECT name FROM application_work ORDER BY name";
                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "commit:commit", "successful callback committed");
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "commit:handler", "successful handler committed");
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "retry:commit", "retried callback uses the original message");
                                Assert(await reader.ReadAsync() && reader.GetString(0) == "retry:handler", "retry reruns work in a new transaction");
                                Assert(!await reader.ReadAsync(), "failed and short-circuited paths left no application rows");
                            }
                        }
                    }

                    foreach (var contract in new[] { typeof(LocalMessage), typeof(Event), typeof(Response) })
                    {
                        var direct = channel.BasicGet(name + ".immediate", true);
                        Assert(direct != null && direct.BasicProperties.Type == contract.FullName &&
                            Equals(direct.BasicProperties.Headers["immediate"], true),
                            "immediate sends, publishes and replies survive handler rollback with their headers");
                        if (contract == typeof(Response))
                            Assert(direct.BasicProperties.MessageId == "immediate-reply-id" &&
                                direct.BasicProperties.ReplyTo == name + ".reply-next", "reply options can set identity and return address");
                    }
                    BasicGetResult alternateError = null;
                    await WaitFor(() =>
                    {
                        alternateError = channel.BasicGet(name + ".alternate-error", true);
                        return Task.FromResult(alternateError != null);
                    }, "custom policy routes the failed message to its alternate queue");
                    Assert(errors.Single(error => error.ErrorQueue == name + ".alternate-error").MessageId ==
                        alternateError.BasicProperties.MessageId, "the alternate error queue receives the recorded failure");

                    var serialized = MessagePackSerializer.Serialize(new Command { Text = "commit" },
                        MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance));
                    foreach (var destination in forwardDestinations)
                    {
                        BasicGetResult forwarded = null;
                        await WaitFor(() =>
                        {
                            forwarded = channel.BasicGet(destination, true);
                            return Task.FromResult(forwarded != null);
                        }, "forwarded command reaches " + destination);
                        Assert(forwarded.BasicProperties.MessageId == forwardedId &&
                            forwarded.BasicProperties.Type == typeof(Command).FullName &&
                            !forwarded.BasicProperties.Headers.ContainsKey("x-bq-forwarded-message-id") &&
                            forwarded.Body.ToArray().SequenceEqual(serialized),
                            "both forwards preserve the original identity and wire body");
                        Assert(channel.BasicGet(destination, true) == null, "the failed attempt did not forward a message");
                    }
                    await WaitFor(async () => await CountMigrationRows(path, "bq_outbox") == 2,
                        "the broker confirmed both forward records");

                    var pending = await store.NextOutgoing(name);
                    Assert(await CountMigrationRows(path, "bq_outbox") == 2 && pending?.RoutingKey == name + ".absent",
                        "only successful attempts committed outbox messages");
                    Assert(pending.Type == "migration.framed" && pending.Body[0] == 0x7e &&
                        Equals(pending.Headers["migration-framing"], "v1"),
                        "outbox stores the mutated contract name, header, and wire body");
                    var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
                    var storedMessage = MessagePackSerializer.Deserialize<LocalMessage>(pending.Body.Skip(1).ToArray(), options);
                    Assert(storedMessage.Text == "commit", "stored wire body frames the committed message");
                }
                Console.WriteLine("Migration behavior and unit-of-work tests passed.");
            }
            finally
            {
                using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDelete(name);
                    channel.QueueDelete(name + ".error");
                }
                File.Delete(path);
            }
        }

        static async Task StalledDispatcherTest(Uri uri)
        {
            var name = "bq.test.stalled-dispatch." + Guid.NewGuid().ToString("N");
            var path = Path.Combine(Path.GetTempPath(), name + ".db");
            var store = new SqlitePersistence(() => Sqlite(path), name);
            await store.Initialize();

            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var notification = new TaskCompletionSource<ICriticalErrorContext>(TaskCreationOptions.RunContinuationsAsynchronously);
            var stopped = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var config = new EndpointConfiguration(name, uri)
            {
                Persistence = new FaultPersistence(store) { OutgoingGate = release.Task },
                CriticalErrorThreshold = TimeSpan.FromMilliseconds(250)
            };
            config.DefineCriticalErrorAction(async failure =>
            {
                notification.TrySetResult(failure);
                await failure.Stop();
                stopped.TrySetResult(true);
            });
            config.SendOnly();

            try
            {
                using (var endpoint = RabbitEndpoint.Start(config))
                {
                    try
                    {
                        var failure = await Within(notification.Task);
                        Assert(failure.Error == "Outbox dispatcher" && failure.Exception is TimeoutException && !release.Task.IsCompleted,
                            "critical error callback identifies an outbox poll that has not returned");
                    }
                    finally
                    {
                        release.TrySetResult(true);
                        await endpoint.StopAsync();
                    }
                    Assert(await Within(stopped.Task), "the critical error action can stop the endpoint");
                }
            }
            finally
            {
                File.Delete(path);
            }
            Console.WriteLine("Stalled dispatcher critical-error test passed.");
        }

        static async Task PhasedMigrationTests(Uri uri)
        {
            var prefix = "bq.test.cutover." + Guid.NewGuid().ToString("N");
            var senderName = prefix + ".sender";
            var receiverName = prefix + ".receiver";
            var legacyQueue = prefix + ".legacy";
            var legacyExchange = prefix + ".xml";
            var pilotExchange = prefix + ".msgpack";
            var path = Path.Combine(Path.GetTempPath(), prefix + ".db");
            var senderStore = new SqlitePersistence(() => Sqlite(path), senderName);
            var receiverStore = new SqlitePersistence(() => Sqlite(path), receiverName);
            await senderStore.Initialize();
            await receiverStore.Initialize();
            var sender = new EndpointConfiguration(senderName, uri) { Persistence = senderStore, EventsExchange = pilotExchange };
            sender.SendOnly();
            var receiver = new EndpointConfiguration(receiverName, uri) { Persistence = receiverStore, EventsExchange = pilotExchange };
            receiver.Pipeline.Register("first-observer", typeof(UnexpectedPilotBehavior), "Replace before startup");
            receiver.Pipeline.Register("removed-observer", new UnexpectedPilotBehavior(), "Remove before startup");
            receiver.Pipeline.Remove("removed-observer");
            receiver.Incoming.Use((context, next) =>
            {
                if (context.MessageType == typeof(IPilotEvent))
                    Assert(context.Extensions.Get<int>("pilot-behaviors") == 1, "replacement retains its pipeline position");
                return next();
            });
            receiver.Pipeline.Register("second-observer", new ObservePilotEvent(), "Verify registration by instance");
            receiver.Pipeline.Replace("first-observer", typeof(ObservePilotEvent), "Inspect the declared event contract");
            var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var commandReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.Handle(() => new Handler<IPilotEvent>((message, context) =>
            {
                Assert(context.MessageHeaders["migration-group"] == "observed" &&
                    System.Text.Encoding.UTF8.GetString((byte[])context.Headers["migration-group"]) == "first",
                    "logical header edits reach the handler while raw AMQP headers retain the received value");
                Assert(message.Count == 7, "interface event properties round-trip through MessagePack");
                Assert(context.Extensions.Get<int>("pilot-behaviors") == 2, "registered logical behaviors see the declared interface contract");
                received.TrySetResult(message.Text);
                return Task.CompletedTask;
            }));
            var senderConventions = sender.Conventions().Add(new PilotMessageConvention()).Conventions;
            Assert(senderConventions.IsMessageType(typeof(WorkflowBarrier)) && senderConventions.IsMessageType(typeof(Command)),
                "custom message and command conventions classify contracts");
            if (sender.Conventions().Conventions.IsCommandType(typeof(Command))) sender.Route(typeof(Command), receiverName);
            receiver.Handle(() => new Handler<Command>((message, context) =>
            {
                commandReceived.TrySetResult(message.Text);
                return Task.CompletedTask;
            }));

            const string runtimeContract = "migration.runtime";
            const string runtimeAlias = "migration.runtime.previous";
            const string barrierContract = "migration.runtime.barrier";
            sender.RegisterMessageContract<RuntimeSubscriptionEvent>(runtimeContract);
            receiver.RegisterMessageContract<RuntimeSubscriptionEvent>(runtimeContract, runtimeAlias);
            receiver.RegisterMessageContract<WorkflowBarrier>(barrierContract);
            var runtimeDeliveries = 0;
            var subscribed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var barrier = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver.Handle(() => new Handler<RuntimeSubscriptionEvent>((message, context) =>
            {
                if (Interlocked.Increment(ref runtimeDeliveries) == 2) subscribed.TrySetResult(true);
                return Task.CompletedTask;
            }), subscribe: false);
            receiver.Conventions().DefiningEventsAs(type => type == typeof(RuntimeSubscriptionEvent)).Add(new PilotMessageConvention());
            receiver.Handle(() => new Handler<WorkflowBarrier>((message, context) =>
            {
                barrier.TrySetResult(true);
                return Task.CompletedTask;
            }));

            using (var connection = new ConnectionFactory { Uri = uri }.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                try
                {
                    channel.ExchangeDeclare(legacyExchange, ExchangeType.Topic, true);
                    channel.QueueDeclare(legacyQueue, true, false, false);
                    var replyQueue = prefix + ".behavior-reply";
                    channel.QueueDeclare(replyQueue, false, true, true);
                    var contract = typeof(IPilotEvent).FullName;
                    channel.QueueBind(legacyQueue, legacyExchange, contract);
                    IStartableEndpoint startable = await Endpoint.Create(sender);
                    using (var producer = await startable.Start())
                    using (var subscriber = RabbitEndpoint.Start(receiver))
                    {
                        const string legacyXml = "<Event><Text>legacy</Text></Event>";
                        var properties = channel.CreateBasicProperties();
                        properties.MessageId = Guid.NewGuid().ToString("N");
                        properties.Type = contract;
                        properties.ContentType = "text/xml";
                        channel.ConfirmSelect();
                        channel.BasicPublish(legacyExchange, contract, properties, System.Text.Encoding.UTF8.GetBytes(legacyXml));
                        Assert(channel.WaitForConfirms(TimeSpan.FromSeconds(5)), "legacy publication confirmed");
                        var publish = new PublishOptions();
                        publish.SetHeader("migration-group", "first");
                        publish.SetHeader("migration-reply", replyQueue);
                        publish.SetMessageId(prefix + ".pilot-event");
                        IMessageSession messaging = producer;
                        IMessageCreator creator = new MessageCreator();
                        var created = creator.CreateInstance<IPilotEvent>(message =>
                        {
                            message.Text = "pilot";
                            message.Count = 7;
                        });
                        await messaging.Publish(created, publish);
                        Action<Command> initializeCommand = command => command.Text = "pilot command";
                        await producer.Send(initializeCommand, new SendOptions());
                        Assert(await Within(commandReceived.Task) == "pilot command", "command convention configures the migrated route");
                        Assert(await Within(received.Task) == "pilot observed", "the migrated handler receives the replacement event");
                        BasicGetResult behaviorReply = null;
                        await WaitFor(() =>
                        {
                            behaviorReply = channel.BasicGet(replyQueue, true);
                            return Task.FromResult(behaviorReply != null);
                        }, "logical behavior replies via the transactional outbox");
                        Assert(behaviorReply.BasicProperties.Type == typeof(Response).FullName &&
                            behaviorReply.BasicProperties.CorrelationId == prefix + ".pilot-event",
                            "logical behavior reply uses the incoming message identity");
                        var legacy = channel.BasicGet(legacyQueue, true);
                        Assert(legacy != null && legacy.BasicProperties.ContentType == "text/xml" &&
                            System.Text.Encoding.UTF8.GetString(legacy.Body.ToArray()) == legacyXml,
                            "legacy subscriber receives the original XML");
                        Assert(channel.BasicGet(legacyQueue, true) == null && (await subscriber.ListErrors()).Count == 0,
                            "wire formats stay on their own exchanges during phased cutover");

                        await messaging.Publish(new RuntimeSubscriptionEvent { Value = 0 }, new PublishOptions());
                        await subscriber.Subscribe<RuntimeSubscriptionEvent>();
                        await subscriber.Subscribe(typeof(RuntimeSubscriptionEvent), new SubscribeOptions());
                        await messaging.Publish(new RuntimeSubscriptionEvent { Value = 1 }, new PublishOptions());
                        PublishSubscriptionProbe(channel, pilotExchange, runtimeAlias, runtimeAlias, new RuntimeSubscriptionEvent { Value = 2 });
                        await Within(subscribed.Task);

                        await subscriber.Unsubscribe(typeof(RuntimeSubscriptionEvent), new UnsubscribeOptions());
                        await messaging.Publish(new RuntimeSubscriptionEvent { Value = 3 }, new PublishOptions());
                        PublishSubscriptionProbe(channel, pilotExchange, runtimeAlias, runtimeAlias, new RuntimeSubscriptionEvent { Value = 4 });
                        PublishSubscriptionProbe(channel, "", receiverName, barrierContract, new WorkflowBarrier());
                        await Within(barrier.Task);
                        Assert(Volatile.Read(ref runtimeDeliveries) == 2, "unsubscribe removes both the event contract and its alias");
                    }
                }
                finally
                {
                    channel.QueueDelete(receiverName);
                    channel.QueueDelete(receiverName + ".error");
                    channel.QueueDelete(legacyQueue);
                    channel.ExchangeDelete(legacyExchange);
                    channel.ExchangeDelete(pilotExchange);
                    File.Delete(path);
                }
            }
            Console.WriteLine("Phased migration routing and wire isolation tests passed.");
        }

        static void PublishSubscriptionProbe<T>(IModel channel, string exchange, string route, string contract, T message)
        {
            var properties = channel.CreateBasicProperties();
            properties.MessageId = Guid.NewGuid().ToString("N");
            properties.ContentType = "application/x-msgpack";
            properties.Type = contract;
            var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
            channel.BasicPublish(exchange, route, properties, MessagePackSerializer.Serialize(message, options));
            Assert(channel.WaitForConfirms(TimeSpan.FromSeconds(5)), "subscription test publication confirmed");
        }

        static async Task InsertApplicationWork(SynchronizedStorageSession session, string work)
        {
            using (var command = session.Connection.CreateCommand())
            {
                command.Transaction = session.Transaction;
                command.CommandText = "INSERT INTO application_work (name) VALUES (@name)";
                DbParameter parameter = command.CreateParameter();
                parameter.ParameterName = "@name";
                parameter.Value = work;
                command.Parameters.Add(parameter);
                await command.ExecuteNonQueryAsync();
            }
        }

        static async Task<int> CountMigrationRows(string path, string table)
        {
            using (var db = Sqlite(path))
            {
                await db.OpenAsync();
                using (var command = db.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT(*) FROM " + table;
                    return Convert.ToInt32(await command.ExecuteScalarAsync());
                }
            }
        }
    }
}
