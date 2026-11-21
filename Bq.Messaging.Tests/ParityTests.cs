using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Messaging.Faults;
using Bq.Extensions.CastleWindsor;
using Bq.Persistence.Sqlite;
using Castle.Windsor;
using Castle.MicroKernel.Registration;

namespace Bq.Messaging.Tests
{
    public class WindsorCommand { public string Text { get; set; } }
    public sealed class WindsorDependency { public string Value => "from-windsor"; }
    public sealed class WindsorHandler : IHandleMessages<WindsorCommand>
    {
        public static readonly TaskCompletionSource<string> Seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly WindsorDependency dependency;
        public WindsorHandler(WindsorDependency dependency) { this.dependency = dependency; }
        public Task Handle(WindsorCommand message, IMessageHandlerContext context)
        {
            Seen.TrySetResult(message.Text + ":" + dependency.Value);
            return Task.CompletedTask;
        }
    }

    public class SagaStart { public string OrderId { get; set; } }
    public class SagaDone { public string OrderId { get; set; } }
    public class ScannedSagaState { public string OrderId { get; set; } public int Steps { get; set; } }
    public sealed class ScannedSaga : Saga<ScannedSagaState>, IAmStartedByMessages<SagaStart>, IHandleMessages<SagaDone>
    {
        public static readonly TaskCompletionSource<int> Completed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly ScannedDependency dependency;
        public ScannedSaga(ScannedDependency dependency) { this.dependency = dependency; }
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<ScannedSagaState> mapper) =>
            mapper.MapSaga(data => data.OrderId).ToMessage<SagaStart>(m => m.OrderId).ToMessage<SagaDone>(m => m.OrderId);
        public Task Handle(SagaStart message, IMessageHandlerContext context)
        {
            Data.Steps++;
            return context.SendLocal(new SagaDone { OrderId = message.OrderId });
        }
        public Task Handle(SagaDone message, IMessageHandlerContext context)
        {
            Data.Steps += dependency.Value == "injected" ? 10 : 0;
            Completed.TrySetResult(Data.Steps);
            MarkAsComplete();
            return Task.CompletedTask;
        }
    }

    public class LegacyState { public string OrderId { get; set; } }
    public sealed class LegacyMappedSaga : Saga<LegacyState>, IAmStartedByMessages<StartOrder>
    {
        protected override void ConfigureHowToFindSaga(IConfigureHowToFindSagaWithMessage mapper) =>
            new ToSagaExpression<LegacyState, StartOrder>(mapper, message => message.OrderId).ToSaga(data => data.OrderId);
        public Task Handle(StartOrder message, IMessageHandlerContext context) => Task.CompletedTask;
    }

    public class ExternalCommand { public string Text { get; set; } }
    public sealed class ExternalHandler : IHandleMessages<ExternalCommand>
    {
        public static readonly TaskCompletionSource<string> Seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly ScannedDependency dependency;
        public ExternalHandler(ScannedDependency dependency) { this.dependency = dependency; }
        public Task Handle(ExternalCommand message, IMessageHandlerContext context)
        {
            Seen.TrySetResult(message.Text + ":" + dependency.Value);
            return Task.CompletedTask;
        }
    }
    public sealed class CountingInitializer : INeedInitialization
    {
        public static int Customized;
        public void Customize(EndpointConfiguration configuration) => Interlocked.Increment(ref Customized);
    }

    public class OrderedMessage { public string Text { get; set; } }
    public sealed class OrderedHandlerA : IHandleMessages<OrderedMessage>
    {
        public static readonly ConcurrentQueue<string> Order = new ConcurrentQueue<string>();
        public Task Handle(OrderedMessage message, IMessageHandlerContext context) { Order.Enqueue("A"); return Task.CompletedTask; }
    }
    public sealed class OrderedHandlerB : IHandleMessages<OrderedMessage>
    {
        public Task Handle(OrderedMessage message, IMessageHandlerContext context) { OrderedHandlerA.Order.Enqueue("B"); return Task.CompletedTask; }
    }

    public class SendProbe : ICommand { public string Text { get; set; } }
    public class PublishProbe : IEvent { public string Text { get; set; } }
    public class FailingProbe { public string Text { get; set; } }

    public sealed class SatelliteFeature : Bq.Messaging.Features.Feature
    {
        public static string Queue;
        public static readonly ConcurrentQueue<string> Received = new ConcurrentQueue<string>();
        protected override void Setup(Bq.Messaging.Features.FeatureConfigurationContext context) =>
            context.AddSatelliteReceiver("probe-satellite", Queue, Bq.Messaging.Transport.PushRuntimeSettings.Default,
                (config, error) => error.Exception.Message == "satellite failure"
                    ? (RecoverabilityAction)RecoverabilityAction.MoveToError(config.Failed.ErrorQueue) : RecoverabilityAction.Discard("unexpected"),
                (builder, message) =>
                {
                    var text = System.Text.Encoding.UTF8.GetString(message.Body);
                    Received.Enqueue(text + ":" + (builder != null) + ":" + message.Headers["probe"]);
                    if (text == "fail") throw new InvalidOperationException("satellite failure");
                    return Task.CompletedTask;
                });
    }

    public sealed class AbortOnceBehavior : Behavior<ITransportReceiveContext>
    {
        public static int Seen;
        public override Task Invoke(ITransportReceiveContext context, Func<Task> next)
        {
            if (!context.Message.Headers.ContainsKey("abort-once")) return next();
            if (Interlocked.Increment(ref Seen) == 1)
            {
                context.AbortReceiveOperation();
                return Task.CompletedTask;
            }
            return next();
        }
    }

    public sealed class OutgoingSendMarker : Behavior<IOutgoingSendContext>
    {
        public override Task Invoke(IOutgoingSendContext context, Func<Task> next) { context.Headers["stage-send"] = "1"; return next(); }
    }
    public sealed class OutgoingPublishMarker : Behavior<IOutgoingPublishContext>
    {
        public override Task Invoke(IOutgoingPublishContext context, Func<Task> next) { context.Headers["stage-publish"] = "1"; return next(); }
    }
    public sealed class OutgoingBodyRewriter : Behavior<IOutgoingPhysicalMessageContext>
    {
        public static int Rewrites;
        public override Task Invoke(IOutgoingPhysicalMessageContext context, Func<Task> next)
        {
            if (context.Headers.ContainsKey("stage-send") || context.Headers.ContainsKey("stage-publish"))
            {
                Interlocked.Increment(ref Rewrites);
                context.UpdateMessage(context.Body.ToArray());
                context.Headers["stage-physical"] = "1";
            }
            return next();
        }
    }

    partial class Program
    {
        static Type[] AllHandlersExcept(params Type[] allowed) => typeof(Program).Assembly.GetTypes()
            .Where(type => !type.IsAbstract && !allowed.Contains(type) &&
                (typeof(Saga).IsAssignableFrom(type) || type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandleMessages<>))))
            .ToArray();

        static void ParityUnitTests()
        {
            var legacy = Bq.Messaging.Sagas.SagaMetadata.Create(typeof(LegacyMappedSaga));
            Assert(legacy.TryGetCorrelationProperty(out var legacyCorrelation) && legacyCorrelation.Name == "OrderId",
                "the pre-7 ConfigureHowToFindSaga hook maps messages to saga properties");

            var error = new ErrorContext(new InvalidOperationException("x"), new System.Collections.Generic.Dictionary<string, string>(), "m1", new byte[] { 1 },
                new Bq.Messaging.Transport.TransportTransaction(), 3);
            Assert(error.ImmediateProcessingFailures == 3 && error.Message.MessageId == "m1" && error.TransportTransaction != null,
                "ErrorContext constructs from transport data");
            var memory = new InMemoryPersistenceProvider("memory-probe");
            using (var first = memory.OpenSession().GetAwaiter().GetResult())
            using (var second = memory.OpenSession().GetAwaiter().GetResult())
            {
                Assert(first.TryClaimMessage("memory-probe", "m1").GetAwaiter().GetResult(), "an in-memory session claims a message");
                Assert(!second.TryClaimMessage("memory-probe", "m1").GetAwaiter().GetResult(), "a second session cannot claim it while the first holds it");
                first.StoreOutgoing(new OutgoingMessage { Id = "o1", Type = "t", Exchange = "", RoutingKey = "q", Body = new byte[] { 1 } }).GetAwaiter().GetResult();
                first.SaveSaga("saga", "k", new byte[] { 1 }, null).GetAwaiter().GetResult();
                Assert(memory.NextOutgoing("memory-probe").GetAwaiter().GetResult() == null, "staged writes are invisible before commit");
                first.Commit();
                Assert(memory.NextOutgoing("memory-probe").GetAwaiter().GetResult()?.Id == "o1", "committed writes become visible");
            }
            using (var third = memory.OpenSession().GetAwaiter().GetResult())
            {
                Assert(!third.TryClaimMessage("memory-probe", "m1").GetAwaiter().GetResult(), "committed claims deduplicate later messages");
                var saga = third.LoadSaga("saga", "k").GetAwaiter().GetResult();
                Assert(saga != null && saga.Version == 1, "committed sagas load with their version");
                var conflict = false;
                try { third.SaveSaga("saga", "k", new byte[] { 2 }, 5).GetAwaiter().GetResult(); } catch (SagaConcurrencyException) { conflict = true; }
                Assert(conflict, "a stale saga version fails");
            }
            using (var abandoned = memory.OpenSession().GetAwaiter().GetResult())
                Assert(abandoned.TryClaimMessage("memory-probe", "m2").GetAwaiter().GetResult(), "a new message claims");
            using (var retry = memory.OpenSession().GetAwaiter().GetResult())
                Assert(retry.TryClaimMessage("memory-probe", "m2").GetAwaiter().GetResult(), "an uncommitted session releases its claims");
            Assert(Bq.Messaging.Faults.FaultsHeaderKeys.FailedQ == "x-bq-failed-endpoint", "fault header keys exist");
            var outgoing = new Bq.Messaging.Transport.OutgoingMessage("id", new System.Collections.Generic.Dictionary<string, string>(), new byte[0]);
            Assert(outgoing.MessageId == "id", "the transport outgoing message type constructs");

            var conventions = new EndpointConfiguration("conv-databus", new Uri("amqp://localhost/")).Conventions();
            Assert(!conventions.Conventions.IsDataBusProperty(typeof(Response).GetProperty("Text")), "no property is a DataBus property");
            var refused = false;
            try { conventions.DefiningDataBusPropertiesAs(property => true); } catch (NotSupportedException) { refused = true; }
            Assert(refused, "DataBus conventions fail at configuration time");
            refused = false;
            try { new EndpointConfiguration("conv-source", new Uri("amqp://localhost/")).CustomConfigurationSource(null); } catch (NotSupportedException) { refused = true; }
            Assert(refused, "configuration sources fail at configuration time");

            // Scanner options: assembly exclusion, and nested directory scanning of a copy of an assembly.
            var excludedScan = new EndpointConfiguration("scan-exclude", new Uri("amqp://localhost/"));
            excludedScan.AssemblyScanner().ScanFileSystemAssemblies = false;
            excludedScan.AssemblyScanner().ExcludeTypes(AllHandlersExcept(typeof(OrderedHandlerA)));
            excludedScan.ExcludeAssemblies("Bq.Messaging.Tests");
            excludedScan.ApplyAssemblyScanning();
            Assert(excludedScan.Handlers.Count == 0, "ExcludeAssemblies keeps an assembly out of the scan");
            var includedScan = new EndpointConfiguration("scan-include", new Uri("amqp://localhost/"));
            includedScan.AssemblyScanner().ScanFileSystemAssemblies = false;
            includedScan.AssemblyScanner().ExcludeTypes(AllHandlersExcept(typeof(OrderedHandlerA)));
            includedScan.ApplyAssemblyScanning();
            Assert(includedScan.Handlers.Count == 1, "the same scan finds the handler when the assembly is not excluded");

            var scanRoot = Path.Combine(Path.GetTempPath(), "bq-scan-" + Guid.NewGuid().ToString("N"));
            var nested = Path.Combine(scanRoot, "plugins", "deep");
            Directory.CreateDirectory(nested);
            File.Copy(typeof(SqlitePersistence).Assembly.Location, Path.Combine(nested, "Nested.Copy.dll"));
            System.Collections.Generic.List<string> Scan(bool nestedDirectories)
            {
                var scan = new EndpointConfiguration("scan-nested", new Uri("amqp://localhost/"));
                scan.AssemblyScanner().ScanAppDomainAssemblies = false;
                scan.AssemblyScanner().AdditionalAssemblyScanningPath = scanRoot;
                scan.AssemblyScanner().ScanAssembliesInNestedDirectories = nestedDirectories;
                return scan.ScannedAssemblies(new System.Collections.Generic.HashSet<string> { "Bq.Persistence.Sqlite" })
                    .Select(assembly => assembly.GetName().Name).ToList();
            }
            Assert(!Scan(false).Contains("Bq.Persistence.Sqlite"), "nested directories are not scanned by default");
            Assert(Scan(true).Contains("Bq.Persistence.Sqlite"), "ScanAssembliesInNestedDirectories scans below the path");
            Directory.Delete(scanRoot, true);
        }

        static async Task ParityIntegrationTests(Uri uri)
        {
            ParityUnitTests();
            // Scanned saga and handlers, configured only through NServiceBus-style entry points, with file system scanning on.
            var sagaName = "bq.test.parity.saga." + Guid.NewGuid().ToString("N");
            var sagaPath = Path.Combine(Path.GetTempPath(), sagaName + ".db");
            var sagaConfig = new EndpointConfiguration(sagaName, new Uri("amqp://placeholder.invalid/"));
            sagaConfig.UseTransport<RabbitMQTransport>().ConnectionString(() => uri.ToString());
            sagaConfig.UsePersistence<SqlitePersistenceDefinition>().ConnectionFactory(() => Sqlite(sagaPath));
            sagaConfig.EnableInstallers();
            sagaConfig.Route(typeof(SagaStart), sagaName);
            sagaConfig.Route(typeof(SagaDone), sagaName);
            sagaConfig.RegisterComponents(components => components.ConfigureComponent<ScannedDependency>(DependencyLifecycle.SingleInstance));
            sagaConfig.AssemblyScanner().ExcludeTypes(AllHandlersExcept(typeof(ScannedSaga)));
            sagaConfig.ExcludeAssemblies("Bq.Persistence.Oracle.dll");
            using (var endpoint = await Endpoint.Start(sagaConfig))
            {
                await endpoint.Send(new SagaStart { OrderId = "scanned-saga" });
                Assert(await Within(ScannedSaga.Completed.Task) == 11, "a scanned saga runs with container-injected dependencies and persisted state");
            }

            // The same discovery through Windsor as the endpoint container.
            var windsorName = "bq.test.parity.windsor." + Guid.NewGuid().ToString("N");
            var windsorPath = Path.Combine(Path.GetTempPath(), windsorName + ".db");
            var windsorStore = new SqlitePersistence(() => Sqlite(windsorPath), windsorName);
            await windsorStore.Initialize();
            using (var windsor = new WindsorContainer())
            {
                windsor.Register(Component.For<WindsorDependency>().LifestyleSingleton());
                var windsorConfig = new EndpointConfiguration(windsorName, uri) { Persistence = windsorStore };
                windsorConfig.UseContainer<WindsorBuilder>(c => c.ExistingContainer(windsor));
                windsorConfig.Route(typeof(WindsorCommand), windsorName);
                windsorConfig.AssemblyScanner().ExcludeTypes(AllHandlersExcept(typeof(WindsorHandler)));
                windsorConfig.AssemblyScanner().ScanFileSystemAssemblies = false;
                using (var endpoint = await Endpoint.Start(windsorConfig))
                {
                    await endpoint.Send(new WindsorCommand { Text = "windsor" });
                    Assert(await Within(WindsorHandler.Seen.Task) == "windsor:from-windsor",
                        "an endpoint started with UseContainer<WindsorBuilder> resolves scanned handlers from Windsor");
                }
            }

            // ExecuteTheseHandlersFirst orders scanned handlers for one message type.
            var orderName = "bq.test.parity.order." + Guid.NewGuid().ToString("N");
            var orderPath = Path.Combine(Path.GetTempPath(), orderName + ".db");
            var orderStore = new SqlitePersistence(() => Sqlite(orderPath), orderName);
            await orderStore.Initialize();
            var orderConfig = new EndpointConfiguration(orderName, uri) { Persistence = orderStore };
            orderConfig.Route(typeof(OrderedMessage), orderName);
            orderConfig.ExecuteTheseHandlersFirst(typeof(OrderedHandlerB));
            orderConfig.AssemblyScanner().ExcludeTypes(AllHandlersExcept(typeof(OrderedHandlerA), typeof(OrderedHandlerB)));
            orderConfig.AssemblyScanner().ScanFileSystemAssemblies = false;
            using (var endpoint = await Endpoint.Start(orderConfig))
            {
                await endpoint.Send(new OrderedMessage { Text = "order" });
                await WaitFor(() => Task.FromResult(OrderedHandlerA.Order.Count == 2), "both scanned handlers run");
                Assert(string.Join("", OrderedHandlerA.Order) == "BA", "ExecuteTheseHandlersFirst runs the listed handler first");
            }

            // A host that owns the container: Bq registers into it at Create and uses its builder at Start.
            var externalName = "bq.test.parity.external." + Guid.NewGuid().ToString("N");
            var externalPath = Path.Combine(Path.GetTempPath(), externalName + ".db");
            var externalStore = new SqlitePersistence(() => Sqlite(externalPath), externalName);
            await externalStore.Initialize();
            var externalConfig = new EndpointConfiguration(externalName, uri) { Persistence = externalStore };
            externalConfig.Route(typeof(ExternalCommand), externalName);
            externalConfig.AssemblyScanner().ExcludeTypes(AllHandlersExcept(typeof(ExternalHandler)));
            externalConfig.AssemblyScanner().ScanFileSystemAssemblies = false;
            var hostContainer = new Bq.Messaging.Container.DefaultContainer();
            var host = new Bq.Messaging.Container.ContainerBuilder(hostContainer);
            host.RegisterSingleton(new ScannedDependency());
            var customizedBefore = Volatile.Read(ref CountingInitializer.Customized);
            var startable = EndpointWithExternallyManagedContainer.Create(externalConfig, host);
            Assert(Volatile.Read(ref CountingInitializer.Customized) > customizedBefore, "INeedInitialization types run when the endpoint is created");
            Assert(host.HasComponent<ExternalHandler>(), "the endpoint registers its handlers with the host container");
            using (var endpoint = await startable.Start(host))
            {
                Assert(ReferenceEquals(startable.MessageSession.Value, endpoint), "the lazy message session resolves after start");
                await endpoint.Send(new ExternalCommand { Text = "external" });
                Assert(await Within(ExternalHandler.Seen.Task) == "external:injected", "handlers resolve from the host-owned builder");
            }
            hostContainer.Dispose();

            // Handlers and behaviors can raise a critical error through the container.
            var criticalName = "bq.test.parity.critical." + Guid.NewGuid().ToString("N");
            var criticalPath = Path.Combine(Path.GetTempPath(), criticalName + ".db");
            var criticalStore = new SqlitePersistence(() => Sqlite(criticalPath), criticalName);
            await criticalStore.Initialize();
            var criticalConfig = new EndpointConfiguration(criticalName, uri) { Persistence = criticalStore };
            var criticalRaised = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            criticalConfig.OnCriticalError = (error, exception) =>
            {
                criticalRaised.TrySetResult(error + ":" + exception.Message);
                return Task.CompletedTask;
            };
            criticalConfig.Route(typeof(SendProbe), criticalName);
            criticalConfig.Handle(() => new Handler<SendProbe>((message, context) => Task.CompletedTask));
            criticalConfig.Incoming.Use((incoming, next) =>
            {
                incoming.Builder.Build<CriticalError>().Raise("behavior failure", new InvalidOperationException("critical probe"));
                return next();
            });
            using (var endpoint = RabbitEndpoint.Start(criticalConfig))
            {
                await endpoint.Send(new SendProbe { Text = "critical" });
                Assert(await Within(criticalRaised.Task) == "behavior failure:critical probe", "CriticalError.Raise reaches the configured critical error action");
            }

            // Outgoing send and publish stages and physical body updates, observed by the receiving handlers.
            var outName = "bq.test.parity.outgoing." + Guid.NewGuid().ToString("N");
            var outPath = Path.Combine(Path.GetTempPath(), outName + ".db");
            var outStore = new SqlitePersistence(() => Sqlite(outPath), outName);
            await outStore.Initialize();
            var outConfig = new EndpointConfiguration(outName, uri) { Persistence = outStore };
            outConfig.Route(typeof(SendProbe), outName);
            outConfig.Pipeline.Register("stage-send", new OutgoingSendMarker(), "Mark sends");
            outConfig.Pipeline.Register("stage-publish", builder => new OutgoingPublishMarker(), "Mark publishes");
            outConfig.Pipeline.Register("stage-physical", new OutgoingBodyRewriter(), "Rewrite bodies");
            var sendSeen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var publishSeen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            outConfig.Handle(() => new Handler<SendProbe>((message, context) =>
            {
                sendSeen.TrySetResult(context.GetHeader("stage-send") + context.GetHeader("stage-physical"));
                return Task.CompletedTask;
            }));
            outConfig.Handle(() => new Handler<PublishProbe>((message, context) =>
            {
                publishSeen.TrySetResult(context.GetHeader("stage-publish") + context.GetHeader("stage-physical"));
                return Task.CompletedTask;
            }), subscribe: true);
            using (var endpoint = RabbitEndpoint.Start(outConfig))
            {
                await endpoint.Send(new SendProbe { Text = "send" });
                await endpoint.Publish(new PublishProbe { Text = "publish" });
                Assert(await Within(sendSeen.Task) == "11", "send and physical stage behaviors run for sends");
                Assert(await Within(publishSeen.Task) == "11", "publish and physical stage behaviors run for publishes");
                Assert(Volatile.Read(ref OutgoingBodyRewriter.Rewrites) == 2, "the physical stage accepts body updates");
            }

            // The whole endpoint runs on in-memory persistence, selected like any NServiceBus persistence.
            var memoryName = "bq.test.parity.memory." + Guid.NewGuid().ToString("N");
            var memoryConfig = new EndpointConfiguration(memoryName, uri);
            memoryConfig.UsePersistence<InMemoryPersistence>();
            memoryConfig.Route(typeof(SendProbe), memoryName);
            var memoryHandled = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            memoryConfig.Handle(() => new Handler<SendProbe>((message, context) =>
            {
                memoryHandled.TrySetResult(message.Text);
                return Task.CompletedTask;
            }));
            using (var endpoint = RabbitEndpoint.Start(memoryConfig))
            {
                await endpoint.Send(new SendProbe { Text = "in-memory" });
                Assert(await Within(memoryHandled.Task) == "in-memory", "an endpoint runs end to end on in-memory persistence");
            }

            // Aborting the receive operation rolls back and redelivers the message without a failure.
            var abortName = "bq.test.parity.abort." + Guid.NewGuid().ToString("N");
            var abortPath = Path.Combine(Path.GetTempPath(), abortName + ".db");
            var abortStore = new SqlitePersistence(() => Sqlite(abortPath), abortName);
            await abortStore.Initialize();
            var abortConfig = new EndpointConfiguration(abortName, uri) { Persistence = abortStore, ImmediateRetries = 0 };
            abortConfig.Recoverability.DelayedRetries = new TimeSpan[0];
            abortConfig.Route(typeof(SendProbe), abortName);
            abortConfig.Pipeline.Register("abort-once", new AbortOnceBehavior(), "Abort the first receive");
            var abortHandled = 0;
            var abortDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            abortConfig.Handle(() => new Handler<SendProbe>((message, context) =>
            {
                Interlocked.Increment(ref abortHandled);
                abortDone.TrySetResult(true);
                return Task.CompletedTask;
            }));
            using (var endpoint = RabbitEndpoint.Start(abortConfig))
            {
                var abortOptions = new SendOptions();
                abortOptions.SetHeader("abort-once", "1");
                await endpoint.Send(new SendProbe { Text = "abort" }, abortOptions);
                Assert(await Within(abortDone.Task), "an aborted receive is redelivered and then handled");
                Assert(Volatile.Read(ref AbortOnceBehavior.Seen) == 2 && Volatile.Read(ref abortHandled) == 1,
                    "the handler ran once, after the aborted attempt rolled back");
            }

            // A satellite receiver reads its own queue with its own delegate and recoverability policy.
            var satelliteName = "bq.test.parity.satellite." + Guid.NewGuid().ToString("N");
            var satellitePath = Path.Combine(Path.GetTempPath(), satelliteName + ".db");
            var satelliteStore = new SqlitePersistence(() => Sqlite(satellitePath), satelliteName);
            await satelliteStore.Initialize();
            SatelliteFeature.Queue = satelliteName + ".satellite";
            var satelliteConfig = new EndpointConfiguration(satelliteName, uri) { Persistence = satelliteStore };
            satelliteConfig.EnableFeature<SatelliteFeature>();
            using (var endpoint = RabbitEndpoint.Start(satelliteConfig))
            using (var connection = new RabbitMQ.Client.ConnectionFactory { Uri = uri }.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                foreach (var text in new[] { "ok", "fail" })
                {
                    var properties = channel.CreateBasicProperties();
                    properties.MessageId = Guid.NewGuid().ToString("N");
                    properties.Headers = new System.Collections.Generic.Dictionary<string, object> { ["probe"] = "yes" };
                    channel.BasicPublish("", SatelliteFeature.Queue, false, properties, System.Text.Encoding.UTF8.GetBytes(text));
                }
                await WaitFor(() => Task.FromResult(SatelliteFeature.Received.Count == 2), "the satellite receives both messages");
                Assert(SatelliteFeature.Received.Contains("ok:True:yes"), "the satellite delegate gets a builder, body and headers");
                RabbitMQ.Client.BasicGetResult failed = null;
                await WaitFor(() =>
                {
                    failed = channel.BasicGet(satelliteName + ".error", true);
                    return Task.FromResult(failed != null);
                }, "a failed satellite message moves to the error queue");
                Assert(System.Text.Encoding.UTF8.GetString(failed.Body.ToArray()) == "fail" &&
                    System.Text.Encoding.UTF8.GetString((byte[])failed.BasicProperties.Headers["x-bq-exception-message"]) == "satellite failure",
                    "the error queue copy keeps the body and records the failure");
            }

            // Delayed retry and error queue notifications.
            var failName = "bq.test.parity.failures." + Guid.NewGuid().ToString("N");
            var failPath = Path.Combine(Path.GetTempPath(), failName + ".db");
            var failStore = new SqlitePersistence(() => Sqlite(failPath), failName);
            await failStore.Initialize();
            var failConfig = new EndpointConfiguration(failName, uri) { Persistence = failStore, ImmediateRetries = 0 };
            failConfig.Recoverability.DelayedRetries = new[] { TimeSpan.FromMilliseconds(100) };
            failConfig.DispatchPollInterval = TimeSpan.FromMilliseconds(50);
            failConfig.Route(typeof(FailingProbe), failName);
            DelayedRetryMessage delayed = null;
            var errored = new TaskCompletionSource<FailedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            failConfig.Notifications.Errors.MessageHasBeenSentToDelayedRetries += (sender, message) => delayed = message;
            failConfig.Notifications.Errors.MessageSentToErrorQueue += (sender, message) => errored.TrySetResult(message);
            failConfig.Handle(() => new Handler<FailingProbe>((message, context) => throw new InvalidOperationException("probe failure")));
            using (var endpoint = RabbitEndpoint.Start(failConfig))
            {
                await endpoint.Send(new FailingProbe { Text = "fail" });
                var failed = await Within(errored.Task);
                Assert(delayed != null && delayed.RetryAttempt == 1 && delayed.Exception.Message == "probe failure",
                    "the delayed retry event fires before the error queue event");
                Assert(failed.Exception.Message == "probe failure" && failed.ErrorQueue == failName + ".error",
                    "the error queue event reports the failure and the queue");
            }

            Console.WriteLine("Parity integration tests passed.");
        }
    }
}
