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
using RabbitMQ.Client;

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

    sealed class FakeLogicalContext : IIncomingLogicalMessageContext
    {
        public FakeLogicalContext(System.Collections.Generic.Dictionary<string, string> headers) { Headers = headers; }
        public System.Collections.Generic.Dictionary<string, string> Headers { get; }
        public LogicalMessage Message => null;
        public bool MessageHandled { get; set; }
        public void UpdateMessageInstance(object newInstance) { }
        public string MessageId => null;
        public string ReplyToAddress => null;
        public System.Collections.Generic.IReadOnlyDictionary<string, string> MessageHeaders => Headers;
        public ContextBag Extensions { get; } = new ContextBag();
        public Bq.Messaging.ObjectBuilder.IBuilder Builder => null;
        public Task Send(object message, SendOptions options) => Task.CompletedTask;
        public Task Send<T>(Action<T> initialize, SendOptions options) => Task.CompletedTask;
        public Task Publish(object message, PublishOptions options) => Task.CompletedTask;
        public Task Publish<T>(Action<T> initialize, PublishOptions options) => Task.CompletedTask;
        public Task Reply(object message, ReplyOptions options) => Task.CompletedTask;
        public Task Reply<T>(Action<T> initialize, ReplyOptions options) => Task.CompletedTask;
        public Task ForwardCurrentMessageTo(string destination) => Task.CompletedTask;
    }

    sealed class XmlConfigurationSource : Bq.Messaging.Config.ConfigurationSource.IConfigurationSource
    {
        readonly System.Collections.Generic.Dictionary<string, string> sections;
        public XmlConfigurationSource(System.Collections.Generic.Dictionary<string, string> sections) { this.sections = sections; }

        public T GetConfiguration<T>() where T : class, new()
        {
            if (!sections.TryGetValue(typeof(T).Name, out var xml)) return null;
            var section = new T();
            using (var reader = System.Xml.XmlReader.Create(new StringReader(xml)))
            {
#if NETFRAMEWORK
                // .NET Framework expects the reader on the section element; .NET reads forward to it itself.
                reader.MoveToContent();
#endif
                try
                {
                    typeof(System.Configuration.ConfigurationSection).GetMethod("DeserializeSection",
                        System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).Invoke(section, new object[] { reader });
                }
                catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException != null)
                {
                    // Surface the section's own error, such as a ConfigurationErrorsException for a missing required attribute.
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                }
            }
            return section;
        }
    }

    sealed class ErrorQueueProvider : Bq.Messaging.Config.ConfigurationSource.IProvideConfiguration<Bq.Messaging.Config.MessageForwardingInCaseOfFaultConfig>
    {
        public Bq.Messaging.Config.MessageForwardingInCaseOfFaultConfig GetConfiguration() =>
            new Bq.Messaging.Config.MessageForwardingInCaseOfFaultConfig { ErrorQueue = "from.provider" };
    }

    public class DataBusMessage
    {
        public string Text { get; set; }
        public DataBusProperty<byte[]> Attachment { get; set; }
    }

    public class SecretMessage
    {
        public string Label { get; set; }
        public WireEncryptedString Secret { get; set; }
    }

    public enum JsonKind { First, Second }

    public class JsonLine { public string Sku { get; set; } public int Quantity { get; set; } }

    public class JsonProbe
    {
        public string Text { get; set; }
        public int Count { get; set; }
        public Guid Id { get; set; }
        public DateTimeOffset At { get; set; }
        public JsonKind Kind { get; set; }
        public byte[] Blob { get; set; }
        public System.Collections.Generic.List<JsonLine> Lines { get; set; }
        public WireEncryptedString Secret { get; set; }
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

    public sealed class ConstraintProbe : Behavior<IOutgoingLogicalMessageContext>
    {
        public static readonly ConcurrentDictionary<string, string> Seen = new ConcurrentDictionary<string, string>();
        public override Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next)
        {
            if (context.Message.Instance is SendProbe probe)
            {
                var constraints = Bq.Messaging.DeliveryConstraints.DeliveryConstraintContextExtensions.GetDeliveryConstraints(context.Extensions);
                Seen[probe.Text] = string.Join(",", constraints.Select(constraint => constraint.GetType().Name).OrderBy(name => name));
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
            var sections = new System.Collections.Generic.Dictionary<string, string>
            {
                ["UnicastBusConfig"] = @"<UnicastBusConfig ForwardReceivedMessagesTo=""forward.queue"">
  <MessageEndpointMappings>
    <add Messages=""Bq.Messaging.Tests"" Endpoint=""by.assembly"" />
    <add Messages=""Bq.Messaging.Tests.WindsorCommand, Bq.Messaging.Tests"" Endpoint=""by.type"" />
  </MessageEndpointMappings>
</UnicastBusConfig>",
                ["MessageForwardingInCaseOfFaultConfig"] = @"<MessageForwardingInCaseOfFaultConfig ErrorQueue=""from.file"" />",
                ["AuditConfig"] = @"<AuditConfig QueueName=""audit.queue"" OverrideTimeToBeReceived=""00:10:00"" />"
            };
            var fromFile = new EndpointConfiguration("config-file", new Uri("amqp://localhost/"));
            fromFile.CustomConfigurationSource(new XmlConfigurationSource(sections));
            fromFile.ApplyConfigurationSections();
            Assert(fromFile.Routes[typeof(WindsorCommand)] == "by.type" && fromFile.Routes[typeof(SendProbe)] == "by.assembly",
                "UnicastBusConfig mappings route a named type and every message type of an assembly");
            Assert(!fromFile.Routes.ContainsKey(typeof(OrderedMessage)), "assembly mappings route only message types, by convention");
            Assert(fromFile.ForwardReceivedMessagesAddress == "forward.queue" && fromFile.Recoverability.ErrorQueue == "from.file" &&
                fromFile.AuditQueue == "audit.queue" && fromFile.AuditExpiration != null, "forwarding, error queue and audit sections apply");
            var coded = new EndpointConfiguration("config-coded", new Uri("amqp://localhost/"));
            coded.Route(typeof(SendProbe), "by.code");
            coded.Recoverability.ErrorQueue = "by.code.error";
            coded.CustomConfigurationSource(new XmlConfigurationSource(sections));
            coded.AddConfigurationProvider(typeof(Bq.Messaging.Config.MessageForwardingInCaseOfFaultConfig), new ErrorQueueProvider());
            coded.ApplyConfigurationSections();
            Assert(coded.Routes[typeof(SendProbe)] == "by.code" && coded.Recoverability.ErrorQueue == "by.code.error", "code configuration beats the file");
            var provided = new EndpointConfiguration("config-provided", new Uri("amqp://localhost/"));
            provided.CustomConfigurationSource(new XmlConfigurationSource(sections));
            provided.AddConfigurationProvider(typeof(Bq.Messaging.Config.MessageForwardingInCaseOfFaultConfig), new ErrorQueueProvider());
            provided.ApplyConfigurationSections();
            Assert(provided.Recoverability.ErrorQueue == "from.provider", "a configuration provider beats the configuration source");
            Assert(Bq.Messaging.Features.SettingsExtensions.GetConfigSection<Bq.Messaging.Config.AuditConfig>(
                Bq.Messaging.Configuration.AdvancedExtensibility.AdvancedExtensibilityExtensions.GetSettings(fromFile)).QueueName == "audit.queue",
                "features read sections through the settings");

            var keyOne = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
            var keyTwo = Enumerable.Range(101, 32).Select(i => (byte)i).ToArray();
            var rijndael = new EndpointConfiguration("rijndael-test", new Uri("amqp://localhost/"));
            rijndael.RijndaelEncryptionService("k1", keyOne, new System.Collections.Generic.List<byte[]> { keyTwo });
            var rijndaelService = rijndael.CreateEncryptionService();
            var stage = new OutgoingContext(null, () => null, "id", typeof(SecretMessage), new SecretMessage(),
                new System.Collections.Generic.Dictionary<string, string>(), new ContextBag(), new Bq.Messaging.Routing.RoutingStrategy[0]);
            var encrypted = rijndaelService.Encrypt("top secret", stage);
            Assert(encrypted.EncryptedBase64Value != "top secret" && stage.Headers["NServiceBus.RijndaelKeyIdentifier"] == "k1", "encryption hides the value and names the key");
            Assert(rijndaelService.Decrypt(encrypted, new FakeLogicalContext(stage.Headers)) == "top secret", "decryption restores the value");
            var retired = new EndpointConfiguration("rijndael-old", new Uri("amqp://localhost/"));
            retired.RijndaelEncryptionService("k2", keyTwo);
            var oldStage = new OutgoingContext(null, () => null, "id", typeof(SecretMessage), new SecretMessage(),
                new System.Collections.Generic.Dictionary<string, string>(), new ContextBag(), new Bq.Messaging.Routing.RoutingStrategy[0]);
            var oldValue = retired.CreateEncryptionService().Encrypt("from the old key", oldStage);
            Assert(rijndaelService.Decrypt(oldValue, new FakeLogicalContext(new System.Collections.Generic.Dictionary<string, string>())) == "from the old key",
                "expired keys still decrypt messages that are in flight");
            var badKey = false;
            try { new EndpointConfiguration("rijndael-bad", new Uri("amqp://localhost/")).RijndaelEncryptionService("k", new byte[5]); } catch (ArgumentException) { badKey = true; }
            Assert(badKey, "keys of the wrong size are rejected");
            var rijndaelSections = new System.Collections.Generic.Dictionary<string, string>
            {
                ["RijndaelEncryptionServiceConfig"] = @"<RijndaelEncryptionServiceConfig Key=""gdgdOq4ZZTLlJRa4pZjS1TXKEf/kRMu5e8Wy3x4AwpU="" KeyFormat=""Base64"" KeyIdentifier=""cfg"">
  <ExpiredKeys><add Key=""abcdefghijklmnop"" /></ExpiredKeys>
</RijndaelEncryptionServiceConfig>"
            };
            var fromSection = new EndpointConfiguration("rijndael-section", new Uri("amqp://localhost/"));
            fromSection.CustomConfigurationSource(new XmlConfigurationSource(rijndaelSections));
            fromSection.RijndaelEncryptionService();
            var sectionStage = new OutgoingContext(null, () => null, "id", typeof(SecretMessage), new SecretMessage(),
                new System.Collections.Generic.Dictionary<string, string>(), new ContextBag(), new Bq.Messaging.Routing.RoutingStrategy[0]);
            fromSection.CreateEncryptionService().Encrypt("x", sectionStage);
            Assert(sectionStage.Headers["NServiceBus.RijndaelKeyIdentifier"] == "cfg", "the Rijndael section supplies the key and identifier");

            var outgoing = new Bq.Messaging.Transport.OutgoingMessage("id", new System.Collections.Generic.Dictionary<string, string>(), new byte[0]);
            Assert(outgoing.MessageId == "id", "the transport outgoing message type constructs");

            var conventions = new EndpointConfiguration("conv-databus", new Uri("amqp://localhost/")).Conventions();
            Assert(!conventions.Conventions.IsDataBusProperty(typeof(Response).GetProperty("Text")), "only IDataBusProperty properties are data bus properties by default");
            conventions.DefiningDataBusPropertiesAs(property => property.Name == "Text");
            Assert(conventions.Conventions.IsDataBusProperty(typeof(Response).GetProperty("Text")), "the data bus property convention matches properties");
            var refused = false;
            try { new EndpointConfiguration("conv-source", new Uri("amqp://localhost/")).CustomConfigurationSource(null); } catch (ArgumentNullException) { refused = true; }
            Assert(refused, "a missing configuration source is rejected");

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

        // Runs a whole endpoint on a real Oracle database. It needs the same environment variables as the provider checks.
        static async Task OracleEndpointTests(Uri uri)
        {
            var connectionString = Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_STRING");
            if (string.IsNullOrEmpty(connectionString)) return;
            var assembly = System.Reflection.Assembly.LoadFrom(Environment.GetEnvironmentVariable("BQ_ORACLE_DRIVER_ASSEMBLY"));
            var connectionType = assembly.GetType(Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_TYPE"), throwOnError: true);
            System.Data.Common.DbConnection Connect()
            {
                var connection = (System.Data.Common.DbConnection)Activator.CreateInstance(connectionType);
                connection.ConnectionString = connectionString;
                return connection;
            }

            var name = "bq-oracle-endpoint-" + Guid.NewGuid().ToString("N");
            var configuration = new EndpointConfiguration(name, uri) { Persistence = new Bq.Persistence.Oracle.OraclePersistence(Connect, name), ImmediateRetries = 0 };
            configuration.Recoverability.DelayedRetries = new[] { TimeSpan.FromMilliseconds(100) };
            configuration.DispatchPollInterval = TimeSpan.FromMilliseconds(100);
            configuration.Route(typeof(SendProbe), name);
            configuration.Route(typeof(StartOrder), name);
            configuration.Route(typeof(ChangeOrder), name);
            var okSeen = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var retriedSeen = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            var attempts = 0;
            configuration.Handle(() => new Handler<SendProbe>((message, context) =>
            {
                if (message.Text == "ok") okSeen.TrySetResult(true);
                if (message.Text == "retry")
                {
                    var attempt = Interlocked.Increment(ref attempts);
                    if (attempt == 1) throw new InvalidOperationException("fail the first delivery on Oracle");
                    retriedSeen.TrySetResult(attempt);
                }
                return Task.CompletedTask;
            }));
            configuration.RegisterSaga<OrderWorkflow, OrderState>();
            OrderWorkflow.TimeoutCalls = 0;
            using (var endpoint = RabbitEndpoint.Start(configuration))
            {
                await endpoint.Send(new SendProbe { Text = "ok" });
                Assert(await Within(okSeen.Task), "an endpoint handles a message on Oracle persistence");
                await endpoint.Send(new SendProbe { Text = "retry" });
                Assert(await Within(retriedSeen.Task) == 2, "a failed message recovers through the Oracle recovery table");
                await endpoint.Send(new StartOrder { OrderId = "oracle-order", DelayMs = 300 });
                await WaitFor(() => Task.FromResult(Volatile.Read(ref OrderWorkflow.TimeoutCalls) == 1), "a saga timeout fires from the Oracle timeout table");
                await endpoint.Send(new ChangeOrder { OrderId = "oracle-order", Complete = true });
            }
            Console.WriteLine("Oracle endpoint tests passed.");
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

            // A WireEncryptedString property is encrypted on the wire and decrypted for the handler.
            var secretName = "bq.test.parity.secret." + Guid.NewGuid().ToString("N");
            var secretPath = Path.Combine(Path.GetTempPath(), secretName + ".db");
            var secretStore = new SqlitePersistence(() => Sqlite(secretPath), secretName);
            await secretStore.Initialize();
            var secretConfig = new EndpointConfiguration(secretName, uri) { Persistence = secretStore };
            secretConfig.RijndaelEncryptionService("k1", Enumerable.Range(1, 32).Select(i => (byte)i).ToArray());
            secretConfig.Route(typeof(SecretMessage), secretName);
            var wireText = string.Empty;
            secretConfig.MutateOutgoing = transport =>
            {
                if (transport.TypeName == typeof(SecretMessage).FullName) wireText = System.Text.Encoding.UTF8.GetString(transport.Body);
            };
            var secretSeen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            secretConfig.Handle(() => new Handler<SecretMessage>((message, context) =>
            {
                secretSeen.TrySetResult(message.Secret.Value + ":" + context.GetHeader("NServiceBus.RijndaelKeyIdentifier"));
                return Task.CompletedTask;
            }));
            using (var endpoint = RabbitEndpoint.Start(secretConfig))
            {
                await endpoint.Send(new SecretMessage { Label = "s", Secret = new WireEncryptedString { Value = "launch codes" } });
                Assert(await Within(secretSeen.Task) == "launch codes:k1", "the handler reads the decrypted value");
                Assert(!wireText.Contains("launch codes") && wireText.Contains("EncryptedBase64Value"), "only the encrypted value goes on the wire");
            }

            // JSON on the wire, read by an endpoint that also accepts it next to its own MessagePack.
            var jsonKey = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
            var jsonReceiverName = "bq.test.parity.json.receiver." + Guid.NewGuid().ToString("N");
            var jsonSenderName = "bq.test.parity.json.sender." + Guid.NewGuid().ToString("N");
            var jsonPlainName = "bq.test.parity.json.plain." + Guid.NewGuid().ToString("N");
            SqlitePersistence JsonStore(string endpointName)
            {
                var jsonPath = Path.Combine(Path.GetTempPath(), endpointName + ".db");
                var store = new SqlitePersistence(() => Sqlite(jsonPath), endpointName);
                store.Initialize().GetAwaiter().GetResult();
                return store;
            }
            var jsonReceiver = new EndpointConfiguration(jsonReceiverName, uri) { Persistence = JsonStore(jsonReceiverName) };
            jsonReceiver.AddDeserializer<Bq.Messaging.Serialization.JsonSerializer>();
            jsonReceiver.RijndaelEncryptionService("k1", jsonKey);
            var jsonSeen = new System.Collections.Concurrent.ConcurrentQueue<JsonProbe>();
            var jsonTwo = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            jsonReceiver.Handle(() => new Handler<JsonProbe>((message, context) =>
            {
                jsonSeen.Enqueue(message);
                if (jsonSeen.Count == 2) jsonTwo.TrySetResult(true);
                return Task.CompletedTask;
            }));
            var jsonSender = new EndpointConfiguration(jsonSenderName, uri) { Persistence = JsonStore(jsonSenderName) };
            jsonSender.UseSerialization<Bq.Messaging.Serialization.JsonSerializer>();
            jsonSender.RijndaelEncryptionService("k1", jsonKey);
            jsonSender.Route(typeof(JsonProbe), jsonReceiverName);
            var jsonWire = string.Empty;
            jsonSender.MutateOutgoing = transport => { if (transport.TypeName == typeof(JsonProbe).FullName) jsonWire = System.Text.Encoding.UTF8.GetString(transport.Body); };
            var jsonPlainSender = new EndpointConfiguration(jsonSenderName + ".plain", uri) { Persistence = JsonStore(jsonSenderName + ".plain") };
            jsonPlainSender.RijndaelEncryptionService("k1", jsonKey);
            jsonPlainSender.Route(typeof(JsonProbe), jsonReceiverName);
            var jsonId = Guid.NewGuid();
            var jsonAt = new DateTimeOffset(2026, 11, 21, 12, 30, 0, TimeSpan.FromHours(2));
            JsonProbe MakeProbe(string text) => new JsonProbe
            {
                Text = text, Count = 7, Id = jsonId, At = jsonAt, Kind = JsonKind.Second, Blob = new byte[] { 1, 2, 3 },
                Lines = new System.Collections.Generic.List<JsonLine> { new JsonLine { Sku = "A-1", Quantity = 2 } },
                Secret = new WireEncryptedString { Value = "json secret" }
            };
            using (var receiver = RabbitEndpoint.Start(jsonReceiver))
            using (var sender = RabbitEndpoint.Start(jsonSender))
            using (var plainSender = RabbitEndpoint.Start(jsonPlainSender))
            {
                await sender.Send(MakeProbe("json"));
                await plainSender.Send(MakeProbe("msgpack"));
                await Within(jsonTwo.Task);
                Assert(jsonWire.StartsWith("{") && jsonWire.Contains("\"Text\":\"json\"") && !jsonWire.Contains("json secret"),
                    "the sender writes readable JSON and still hides encrypted strings");
                foreach (var probe in jsonSeen)
                    Assert(probe.Count == 7 && probe.Id == jsonId && probe.At == jsonAt && probe.Kind == JsonKind.Second && probe.Blob.SequenceEqual(new byte[] { 1, 2, 3 }) &&
                        probe.Lines.Single().Sku == "A-1" && probe.Lines.Single().Quantity == 2 && probe.Secret.Value == "json secret",
                        "a message round trips through " + probe.Text + " with every property type");
            }

            // An endpoint that does not accept JSON rejects it without retrying.
            var plainReceiver = new EndpointConfiguration(jsonPlainName, uri) { Persistence = JsonStore(jsonPlainName) };
            plainReceiver.Handle(() => new Handler<JsonProbe>((message, context) => Task.CompletedTask));
            var plainJsonSender = new EndpointConfiguration(jsonPlainName + ".sender", uri) { Persistence = JsonStore(jsonPlainName + ".sender") };
            plainJsonSender.UseSerialization<Bq.Messaging.Serialization.JsonSerializer>();
            plainJsonSender.Route(typeof(JsonProbe), jsonPlainName);
            using (var receiver = RabbitEndpoint.Start(plainReceiver))
            using (var sender = RabbitEndpoint.Start(plainJsonSender))
            using (var adminConnection = new RabbitMQ.Client.ConnectionFactory { Uri = uri }.CreateConnection())
            using (var adminChannel = adminConnection.CreateModel())
            {
                var rejectedProbe = MakeProbe("rejected");
                rejectedProbe.Secret = null;
                await sender.Send(rejectedProbe);
                RabbitMQ.Client.BasicGetResult rejectedCopy = null;
                await WaitFor(() =>
                {
                    rejectedCopy = adminChannel.BasicGet(jsonPlainName + ".error", true);
                    return Task.FromResult(rejectedCopy != null);
                }, "a message in a format the endpoint does not read moves to the error queue");
                Assert(rejectedCopy.BasicProperties.ContentType == "application/json", "the rejected message keeps its content type for replay");
            }

            // Large values travel through the file share data bus, and only their keys go on the wire.
            var busRoot = Path.Combine(Path.GetTempPath(), "bq-databus-" + Guid.NewGuid().ToString("N"));
            var busName = "bq.test.parity.databus." + Guid.NewGuid().ToString("N");
            var busPath = Path.Combine(Path.GetTempPath(), busName + ".db");
            var busStore = new SqlitePersistence(() => Sqlite(busPath), busName);
            await busStore.Initialize();
            var busConfig = new EndpointConfiguration(busName, uri) { Persistence = busStore };
            busConfig.UseDataBus<FileShareDataBus>().BasePath(busRoot);
            busConfig.Route(typeof(DataBusMessage), busName);
            var wireBytes = 0;
            busConfig.MutateOutgoing = transport => { if (transport.TypeName == typeof(DataBusMessage).FullName) wireBytes = transport.Body.Length; };
            var busReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            busConfig.Handle(() => new Handler<DataBusMessage>((message, context) =>
            {
                busReceived.TrySetResult(message.Text + ":" + message.Attachment.Value.Length + ":" + message.Attachment.Value[1000] + ":" + (message.Attachment.Key != null));
                return Task.CompletedTask;
            }));
            var attachment = new byte[300000];
            attachment[1000] = 42;
            using (var endpoint = RabbitEndpoint.Start(busConfig))
            {
                await endpoint.Send(new DataBusMessage { Text = "bus", Attachment = new DataBusProperty<byte[]>(attachment) });
                Assert(await Within(busReceived.Task) == "bus:300000:42:True", "a data bus value reaches the handler with its key");
                Assert(wireBytes > 0 && wireBytes < 1000, "only the key travels in the message");
            }
            var bus = new Bq.Messaging.DataBus.FileShareDataBusImplementationProbe(busRoot).Create();
            var expiring = bus.Put(new MemoryStream(new byte[] { 1 }), TimeSpan.FromMilliseconds(1)).GetAwaiter().GetResult();
            await Task.Delay(50);
            bus.Start().GetAwaiter().GetResult();
            var expired = false;
            try { bus.Get(expiring).GetAwaiter().GetResult(); } catch (FileNotFoundException) { expired = true; }
            Assert(expired, "expired data bus values are removed");
            var escaped = false;
            try { bus.Get("../outside").GetAwaiter().GetResult(); } catch (InvalidOperationException) { escaped = true; }
            Assert(escaped, "data bus keys cannot leave the base directory");

            // Outgoing send and publish stages and physical body updates, observed by the receiving handlers.
            var outName = "bq.test.parity.outgoing." + Guid.NewGuid().ToString("N");
            var outPath = Path.Combine(Path.GetTempPath(), outName + ".db");
            var outStore = new SqlitePersistence(() => Sqlite(outPath), outName);
            await outStore.Initialize();
            var outConfig = new EndpointConfiguration(outName, uri) { Persistence = outStore };
            outConfig.Route(typeof(SendProbe), outName);
            outConfig.Pipeline.Register("constraint-probe", new ConstraintProbe(), "Record delivery constraints");
            outConfig.Conventions().DefiningTimeToBeReceivedAs(type => type == typeof(SendProbe) ? TimeSpan.FromMinutes(5) : TimeSpan.MaxValue);
            outConfig.Pipeline.Register("stage-send", new OutgoingSendMarker(), "Mark sends");
            outConfig.Pipeline.Register("stage-publish", builder => new OutgoingPublishMarker(), "Mark publishes");
            outConfig.Pipeline.Register("stage-physical", new OutgoingBodyRewriter(), "Rewrite bodies");
            var sendSeen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var publishSeen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var delayedSeen = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            outConfig.Handle(() => new Handler<SendProbe>((message, context) =>
            {
                if (message.Text == "delayed") delayedSeen.TrySetResult(true);
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
                var delayedOptions = new SendOptions();
                delayedOptions.DelayDeliveryWith(TimeSpan.FromMilliseconds(300));
                await endpoint.Send(new SendProbe { Text = "delayed" }, delayedOptions);
                Assert(await Within(delayedSeen.Task), "the delayed send is delivered");
                Assert(ConstraintProbe.Seen["send"] == "DiscardIfNotReceivedBefore", "time to be received becomes a delivery constraint");
                Assert(ConstraintProbe.Seen["delayed"] == "DiscardIfNotReceivedBefore,DoNotDeliverBefore", "a delayed send carries a DoNotDeliverBefore constraint");
                Assert(Volatile.Read(ref OutgoingBodyRewriter.Rewrites) >= 2, "the physical stage accepts body updates");
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

            // Pre-provisioned topology: Bq declares nothing and checks that the host created every exchange and queue.
            var provisionedName = "bq.test.parity.provisioned." + Guid.NewGuid().ToString("N");
            var provisionedPath = Path.Combine(Path.GetTempPath(), provisionedName + ".db");
            var provisionedStore = new SqlitePersistence(() => Sqlite(provisionedPath), provisionedName);
            await provisionedStore.Initialize();
            EndpointConfiguration Provisioned()
            {
                var configuration = new EndpointConfiguration(provisionedName, uri) { Persistence = provisionedStore };
                configuration.UsePreProvisionedTopology = true;
                configuration.MakeInstanceUniquelyAddressable("p1");
                configuration.Route(typeof(SendProbe), provisionedName);
                return configuration;
            }
            var provisionedConfig = Provisioned();
            var provisionedHandled = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            provisionedConfig.Handle(() => new Handler<SendProbe>((message, context) =>
            {
                provisionedHandled.TrySetResult(message.Text);
                return Task.CompletedTask;
            }));
            var missing = false;
            try { RabbitEndpoint.Start(Provisioned()).Dispose(); }
            catch (Exception) { missing = true; }
            Assert(missing, "starting against a broker that lacks the pre-provisioned topology fails");
            using (var admin = new RabbitMQ.Client.ConnectionFactory { Uri = uri }.CreateConnection())
            using (var adminChannel = admin.CreateModel())
            {
                adminChannel.ExchangeDeclare(provisionedConfig.EventsExchange, RabbitMQ.Client.ExchangeType.Topic, true);
                adminChannel.ExchangeDeclare(provisionedConfig.ErrorExchange, RabbitMQ.Client.ExchangeType.Direct, true);
                adminChannel.QueueDeclare(provisionedName + ".error", true, false, false);
                adminChannel.QueueBind(provisionedName + ".error", provisionedConfig.ErrorExchange, provisionedName);
                var queueArguments = new System.Collections.Generic.Dictionary<string, object>
                {
                    ["x-dead-letter-exchange"] = provisionedConfig.ErrorExchange,
                    ["x-dead-letter-routing-key"] = provisionedName
                };
                adminChannel.QueueDeclare(provisionedName, true, false, false, queueArguments);
                adminChannel.QueueDeclare(provisionedName + "-p1", true, false, false, queueArguments);
            }
            using (var endpoint = RabbitEndpoint.Start(provisionedConfig))
            {
                var toInstance = new SendOptions();
                toInstance.RouteToThisInstance();
                await endpoint.Send(new SendProbe { Text = "provisioned" }, toInstance);
                Assert(await Within(provisionedHandled.Task) == "provisioned", "an endpoint runs on a pre-provisioned topology, including its instance queue");
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
