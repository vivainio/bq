using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging.Features;
using Bq.Messaging.ObjectBuilder;
using Bq.Extensions.CastleWindsor;
using Bq.Messaging.DeliveryConstraints;
using Bq.Messaging.DelayedDelivery;

namespace Bq.Messaging.Tests
{
    // Edge and failure paths that need no broker. Each case is reported on its own, so one failure does not hide the rest.
    public sealed class EdgeFeatureBase : Feature
    {
        public static int Setups;
        public EdgeFeatureBase() { EnableByDefault(); }
        protected override void Setup(FeatureConfigurationContext context) { Setups++; }
    }
    public sealed class EdgeFeatureAnyOf : Feature
    {
        public static bool Ran;
        public EdgeFeatureAnyOf() { DependsOnAtLeastOne(typeof(EdgeFeatureBase), typeof(EdgeFeatureNeverEnabled)); }
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeFeatureNeverEnabled : Feature
    {
        protected override void Setup(FeatureConfigurationContext context) { }
    }
    public sealed class EdgeFeatureAnyOfNone : Feature
    {
        public static bool Ran;
        public EdgeFeatureAnyOfNone() { DependsOnAtLeastOne(typeof(EdgeFeatureNeverEnabled)); }
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeFeatureAnyOfNames : Feature
    {
        public static bool Ran;
        public EdgeFeatureAnyOfNames() { DependsOnAtLeastOne("EdgeFeatureBase", "Missing"); }
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeFeatureOptional : Feature
    {
        public static int OptionalSetupOrder, BaseSetupOrder;
        public static int Counter;
        public EdgeFeatureOptional() { DependsOnOptionally<EdgeFeatureLate>(); DependsOnOptionally("NoSuchFeature"); }
        protected override void Setup(FeatureConfigurationContext context) { OptionalSetupOrder = ++Counter; }
    }
    public sealed class EdgeFeatureLate : Feature
    {
        protected override void Setup(FeatureConfigurationContext context) { EdgeFeatureOptional.BaseSetupOrder = ++EdgeFeatureOptional.Counter; }
    }
    public sealed class EdgeFeatureFailingPrerequisite : Feature
    {
        public static bool Ran;
        public EdgeFeatureFailingPrerequisite() { Prerequisite(context => false, "never satisfied"); }
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeFeatureDependsOnFailed : Feature
    {
        public static bool Ran;
        public EdgeFeatureDependsOnFailed() { DependsOn<EdgeFeatureFailingPrerequisite>(); }
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeFeatureEnablesOther : Feature
    {
        public EdgeFeatureEnablesOther() { Defaults(settings => settings.EnableFeatureByDefault<EdgeFeatureEnabledByDefaults>()); }
        protected override void Setup(FeatureConfigurationContext context) { }
    }
    public sealed class EdgeFeatureEnabledByDefaults : Feature
    {
        public static bool Ran;
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeFeatureByName : Feature
    {
        public static bool Ran;
        public EdgeFeatureByName() { DependsOn("EdgeFeatureBase"); }
        protected override void Setup(FeatureConfigurationContext context) { Ran = true; }
    }
    public sealed class EdgeStartupTask : FeatureStartupTask
    {
        public static readonly List<string> Events = new List<string>();
        readonly string name; readonly bool failStop;
        public EdgeStartupTask(string name, bool failStop = false) { this.name = name; this.failStop = failStop; }
        protected override Task OnStart(IMessageSession session) { Events.Add("start:" + name); return Task.CompletedTask; }
        protected override Task OnStop(IMessageSession session)
        {
            Events.Add("stop:" + name);
            if (failStop) throw new InvalidOperationException("stop failed: " + name);
            return Task.CompletedTask;
        }
    }
    public sealed class EdgeStartupFeature : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            context.RegisterStartupTask(new EdgeStartupTask("one", failStop: true));
            context.RegisterStartupTask(() => new EdgeStartupTask("two"));
            context.RegisterStartupTask(builder => new EdgeStartupTask("three", failStop: true));
        }
    }

    public sealed class EdgeLogicalBehavior : Behavior<IIncomingLogicalMessageContext>
    {
        public override Task Invoke(IIncomingLogicalMessageContext context, Func<Task> next) => next();
    }
    public sealed class EdgeInvokeBehavior : Behavior<IInvokeHandlerContext>
    {
        public override Task Invoke(IInvokeHandlerContext context, Func<Task> next) => next();
    }
    public sealed class EdgeReceiveBehavior : Behavior<ITransportReceiveContext>
    {
        public override Task Invoke(ITransportReceiveContext context, Func<Task> next) => next();
    }
    public sealed class EdgeOutgoingBehavior : Behavior<IOutgoingLogicalMessageContext>
    {
        public override Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next) => next();
    }
    public sealed class EdgeOutgoingSendBehavior : Behavior<IOutgoingSendContext>
    {
        public override Task Invoke(IOutgoingSendContext context, Func<Task> next) => next();
    }
    public sealed class EdgePhysicalBehavior : Behavior<IIncomingPhysicalMessageContext>
    {
        public override Task Invoke(IIncomingPhysicalMessageContext context, Func<Task> next) => next();
    }

    public sealed class EdgeCaseLogProbe { }
    public sealed class EdgeFeatureHolder { }
    public sealed class EdgePlainPersistence : IPersistenceProvider
    {
        public Task<IPersistenceSession> OpenSession() => throw new NotSupportedException();
        public Task<OutgoingMessage> NextOutgoing(string endpoint) => Task.FromResult<OutgoingMessage>(null);
        public Task MarkDispatched(string endpoint, string messageId) => Task.CompletedTask;
    }
    public interface IEdgeService { }
    public sealed class EdgeServiceOne : IEdgeService { }
    public sealed class EdgeServiceTwo : IEdgeService { }
    public interface IEdgeFactoryService { }
    public sealed class EdgeFactoryService : IEdgeFactoryService { }
    public sealed class EdgeScopedService : IDisposable
    {
        public static int Disposed;
        public void Dispose() { Disposed++; }
    }
    public sealed class EdgeInstallableProvider : IPersistenceProvider, IInstallablePersistence
    {
        readonly InMemoryPersistenceProvider inner = new InMemoryPersistenceProvider("edge");
        public Task Initialize() { EdgePersistenceA.Initialized++; return Task.CompletedTask; }
        public Task<IPersistenceSession> OpenSession() => inner.OpenSession();
        public Task<OutgoingMessage> NextOutgoing(string endpoint) => inner.NextOutgoing(endpoint);
        public Task MarkDispatched(string endpoint, string messageId) => inner.MarkDispatched(endpoint, messageId);
    }
    public sealed class EdgePersistenceA : PersistenceDefinition
    {
        public static int Initialized;
        public EdgePersistenceA() { Supports<StorageType.Sagas>(settings => { }); Defaults(settings => settings.Set("edge-default", "applied")); }
        public override IPersistenceProvider CreateProvider(Bq.Messaging.Settings.ReadOnlySettings settings) => new EdgeInstallableProvider();
    }
    public sealed class EdgePersistenceB : PersistenceDefinition
    {
        public override IPersistenceProvider CreateProvider(Bq.Messaging.Settings.ReadOnlySettings settings) => new InMemoryPersistenceProvider("b");
    }
    public sealed class EdgeConstraint : Bq.Messaging.DeliveryConstraints.DeliveryConstraint { }
    public sealed class EdgeSagaData : IContainSagaData
    {
        public Guid Id { get; set; }
        public string Originator { get; set; }
        public string OriginalMessageId { get; set; }
    }

    static class EdgeCaseTests
    {
        static readonly List<(string Name, Func<Task> Body)> cases = new List<(string, Func<Task>)>();

        static void Case(string name, Action body) => cases.Add((name, () => { body(); return Task.CompletedTask; }));

        static Task CaseAsync(string name, Func<Task> body) { cases.Add((name, body)); return Task.CompletedTask; }

        static void That(bool value, string message) { if (!value) throw new Exception(message); }

        static void Throws<T>(Action action, string name) where T : Exception
        {
            try { action(); }
            catch (T) { return; }
            catch (Exception ex) { throw new Exception(name + " threw " + ex.GetType().Name + " instead of " + typeof(T).Name); }
            throw new Exception(name + " did not throw " + typeof(T).Name);
        }

        static EndpointConfiguration Configuration(string name) => new EndpointConfiguration(name, new Uri("amqp://localhost/"));

        static Bq.Messaging.Settings.SettingsHolder SettingsOf(EndpointConfiguration configuration) =>
            Bq.Messaging.Configuration.AdvancedExtensibility.AdvancedExtensibilityExtensions.GetSettings(configuration);

        public static async Task<IReadOnlyList<(string Name, Func<Task> Body)>> Collect()
        {
            cases.Clear();
            RunFeatureCases();
            RunPipelineCases();
            RunLoggingCases();
            await RunSettingsAndTestingCases();
            RunSerializationCases();
            await RunSagaSpiCases();
            await RunSpiTypeCases();
            RunConfigurationCases();
            await RunContainerAndPersistenceCases();
            RunStartValidationCases();
            await RunInMemoryCases();
            return cases.ToList();
        }

        public static async Task RunOne(string name)
        {
            var found = (await Collect()).Single(c => c.Name == name);
            await found.Body();
        }

        public static async Task Run()
        {
            var failures = new List<string>();
            var all = await Collect();
            foreach (var (name, body) in all)
            {
                try { await body(); }
                catch (Exception ex) { failures.Add(name + ": " + ex.Message); }
            }
            Console.WriteLine("Edge case tests: " + (all.Count - failures.Count) + " passed, " + failures.Count + " failed.");
            if (failures.Count > 0) throw new Exception("Failed edge cases:" + Environment.NewLine + string.Join(Environment.NewLine, failures));
        }

        static void RunFeatureCases()
        {
            Case("DependsOnAtLeastOne activates when one dependency is active", () =>
            {
                var configuration = Configuration("edge-any");
                configuration.EnableFeature<EdgeFeatureAnyOf>();
                configuration.EnableFeature<EdgeFeatureBase>();
                configuration.ActivateFeatures();
                That(EdgeFeatureAnyOf.Ran && SettingsOf(configuration).IsFeatureActive<EdgeFeatureAnyOf>(), "the feature should be active");
            });
            Case("DependsOnAtLeastOne deactivates when none is enabled", () =>
            {
                var configuration = Configuration("edge-any-none");
                configuration.EnableFeature<EdgeFeatureAnyOfNone>();
                configuration.EnableFeature<EdgeFeatureNeverEnabled>();
                configuration.DisableFeature<EdgeFeatureNeverEnabled>();
                configuration.ActivateFeatures();
                That(!EdgeFeatureAnyOfNone.Ran && !SettingsOf(configuration).IsFeatureActive<EdgeFeatureAnyOfNone>(), "the feature should not run");
            });
            Case("DependsOnAtLeastOne resolves features by name", () =>
            {
                var configuration = Configuration("edge-any-names");
                configuration.EnableFeature<EdgeFeatureAnyOfNames>();
                configuration.EnableFeature<EdgeFeatureBase>();
                configuration.ActivateFeatures();
                That(EdgeFeatureAnyOfNames.Ran, "a dependency named by string should count");
            });
            Case("DependsOn by name resolves the named feature", () =>
            {
                var configuration = Configuration("edge-by-name");
                configuration.EnableFeature<EdgeFeatureByName>();
                configuration.EnableFeature<EdgeFeatureBase>();
                configuration.ActivateFeatures();
                That(EdgeFeatureByName.Ran, "the feature should run after the named dependency");
            });
            Case("DependsOnOptionally sets up after an enabled optional dependency", () =>
            {
                EdgeFeatureOptional.Counter = 0;
                var configuration = Configuration("edge-optional");
                configuration.EnableFeature<EdgeFeatureOptional>();
                configuration.EnableFeature<EdgeFeatureLate>();
                configuration.ActivateFeatures();
                That(EdgeFeatureOptional.BaseSetupOrder == 1 && EdgeFeatureOptional.OptionalSetupOrder == 2,
                    "the optional dependency should be set up first when it is enabled");
            });
            Case("DependsOnOptionally does not wait for a disabled or unknown feature", () =>
            {
                var configuration = Configuration("edge-optional-absent");
                configuration.EnableFeature<EdgeFeatureOptional>();
                configuration.ActivateFeatures();
                That(SettingsOf(configuration).IsFeatureActive<EdgeFeatureOptional>(), "the feature should activate without its optional dependency");
            });
            Case("a failing prerequisite deactivates the feature and its dependents", () =>
            {
                var configuration = Configuration("edge-prerequisite");
                configuration.EnableFeature<EdgeFeatureFailingPrerequisite>();
                configuration.EnableFeature<EdgeFeatureDependsOnFailed>();
                configuration.ActivateFeatures();
                var settings = SettingsOf(configuration);
                That(!EdgeFeatureFailingPrerequisite.Ran && !EdgeFeatureDependsOnFailed.Ran, "neither feature should run");
                That(!settings.IsFeatureActive<EdgeFeatureFailingPrerequisite>() && !settings.IsFeatureActive<EdgeFeatureDependsOnFailed>(), "neither should be active");
            });
            Case("Defaults can enable another feature", () =>
            {
                var configuration = Configuration("edge-defaults");
                configuration.EnableFeature<EdgeFeatureEnablesOther>();
                configuration.EnableFeature<EdgeFeatureEnabledByDefaults>();
                configuration.DisableFeature<EdgeFeatureEnabledByDefaults>();
                configuration.EnableFeature<EdgeFeatureEnabledByDefaults>();
                configuration.ActivateFeatures();
                That(EdgeFeatureEnabledByDefaults.Ran, "the feature enabled through Defaults should run");
            });
            Case("an explicit disable beats a feature default", () =>
            {
                var configuration = Configuration("edge-default-disabled");
                configuration.DisableFeature<EdgeFeatureBase>();
                configuration.ActivateFeatures();
                That(!SettingsOf(configuration).IsFeatureActive<EdgeFeatureBase>(), "an explicitly disabled feature must stay inactive");
            });
            Case("activation runs once", () =>
            {
                var configuration = Configuration("edge-once");
                configuration.EnableFeature<EdgeFeatureBase>();
                var before = EdgeFeatureBase.Setups;
                configuration.ActivateFeatures();
                configuration.ActivateFeatures();
                That(EdgeFeatureBase.Setups == before + 1, "setup should run exactly once");
            });
            Case("a feature type must be concrete with a public parameterless constructor", () =>
            {
                var configuration = Configuration("edge-bad-feature");
                Throws<ArgumentException>(() => configuration.EnableFeature(typeof(string)), "a non-feature type");
                Throws<ArgumentException>(() => configuration.EnableFeature(typeof(Feature)), "the abstract base");
            });
            Case("satellite receiver arguments are validated", () =>
            {
                var configuration = Configuration("edge-satellite");
                configuration.EnableFeature<EdgeSatelliteArgs>();
                Throws<InvalidOperationException>(() => configuration.ActivateFeatures(), "duplicate satellite address");
            });
            Case("feature startup tasks stop in reverse order and report the first stop failure", () =>
            {
                EdgeStartupTask.Events.Clear();
                var configuration = Configuration("edge-startup");
                configuration.EnableFeature<EdgeStartupFeature>();
                configuration.ActivateFeatures();
                configuration.StartFeatureTasks(new Bq.Messaging.Container.ContainerBuilder(configuration.BuildContainer()), null).GetAwaiter().GetResult();
                string stopFailure = null;
                try { configuration.StopFeatureTasks(null).GetAwaiter().GetResult(); } catch (InvalidOperationException ex) { stopFailure = ex.Message; }
                That(string.Join(",", EdgeStartupTask.Events) == "start:one,start:two,start:three,stop:three,stop:two,stop:one", "order was " + string.Join(",", EdgeStartupTask.Events));
                That(stopFailure == "stop failed: three", "the first failure in stop order should surface, got " + stopFailure);
            });
        }

        static void RunPipelineCases()
        {
            Case("Replace and Remove work for every stage", () =>
            {
                var pipeline = Configuration("edge-pipeline").Pipeline;
                pipeline.Register("logical", new EdgeLogicalBehavior(), "logical");
                pipeline.Register("invoke", new EdgeInvokeBehavior(), "invoke");
                pipeline.Register("receive", new EdgeReceiveBehavior(), "receive");
                pipeline.Register("outgoing", new EdgeOutgoingBehavior(), "outgoing");
                pipeline.Register("send", new EdgeOutgoingSendBehavior(), "send");
                pipeline.Register("physical", new EdgePhysicalBehavior(), "physical");
                pipeline.Replace("logical", new EdgeLogicalBehavior(), "replaced");
                pipeline.Replace("invoke", new EdgeInvokeBehavior(), "replaced");
                pipeline.Replace("receive", new EdgeReceiveBehavior(), "replaced");
                pipeline.Replace("outgoing", new EdgeOutgoingBehavior(), "replaced");
                pipeline.Replace("send", new EdgeOutgoingSendBehavior(), "replaced");
                pipeline.Replace("physical", new EdgePhysicalBehavior(), "replaced");
                foreach (var id in new[] { "logical", "invoke", "receive", "outgoing", "send", "physical" }) pipeline.Remove(id);
                // A removed id can be registered again.
                pipeline.Register("logical", new EdgeLogicalBehavior(), "again");
                pipeline.Register("invoke", new EdgeInvokeBehavior(), "again");
            });
            Case("Replace rejects a behavior for a different stage", () =>
            {
                var pipeline = Configuration("edge-stage-check").Pipeline;
                pipeline.Register("invoke", new EdgeInvokeBehavior(), "invoke");
                pipeline.Register("receive", new EdgeReceiveBehavior(), "receive");
                pipeline.Register("outgoing", new EdgeOutgoingBehavior(), "outgoing");
                pipeline.Register("physical", new EdgePhysicalBehavior(), "physical");
                pipeline.Register("logical", new EdgeLogicalBehavior(), "logical");
                Throws<InvalidOperationException>(() => pipeline.Replace("invoke", new EdgeReceiveBehavior(), "x"), "invoke replaced by receive");
                Throws<InvalidOperationException>(() => pipeline.Replace("receive", new EdgeInvokeBehavior(), "x"), "receive replaced by invoke");
                Throws<InvalidOperationException>(() => pipeline.Replace("outgoing", new EdgeOutgoingSendBehavior(), "x"), "outgoing logical replaced by send");
                Throws<InvalidOperationException>(() => pipeline.Replace("outgoing", new EdgeLogicalBehavior(), "x"), "outgoing replaced by incoming");
                Throws<InvalidOperationException>(() => pipeline.Replace("physical", new EdgeLogicalBehavior(), "x"), "physical replaced by logical");
                Throws<InvalidOperationException>(() => pipeline.Replace("logical", new EdgePhysicalBehavior(), "x"), "logical replaced by physical");
                Throws<InvalidOperationException>(() => pipeline.Replace("logical", new EdgeOutgoingBehavior(), "x"), "logical replaced by outgoing");
            });
            Case("Replace, Remove and Register validate step ids", () =>
            {
                var pipeline = Configuration("edge-ids").Pipeline;
                pipeline.Register("only", new EdgeLogicalBehavior(), "only");
                Throws<InvalidOperationException>(() => pipeline.Register("only", new EdgeLogicalBehavior(), "dup"), "duplicate id");
                Throws<InvalidOperationException>(() => pipeline.Replace("missing", new EdgeLogicalBehavior(), "x"), "unknown id");
                Throws<InvalidOperationException>(() => pipeline.Replace("missing", () => new EdgeLogicalBehavior(), "x"), "unknown id with a factory");
                Throws<ArgumentException>(() => pipeline.Register(" ", new EdgeLogicalBehavior(), "x"), "blank id");
                Throws<ArgumentException>(() => pipeline.Replace(null, new EdgeLogicalBehavior(), "x"), "null id");
            });
        }

        static void RunLoggingCases()
        {
            Case("default logging filters by level, writes a daily file and validates arguments", () =>
            {
                var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "bq-log-" + Guid.NewGuid().ToString("N"));
                try
                {
                    var factory = Bq.Messaging.Logging.LogManager.Use<Bq.Messaging.Logging.DefaultFactory>();
                    factory.Level(Bq.Messaging.Logging.LogLevel.Warn);
                    factory.Directory(directory);
                    var log = Bq.Messaging.Logging.LogManager.GetLogger<EdgeCaseLogProbe>();
                    That(!log.IsDebugEnabled && !log.IsInfoEnabled && log.IsWarnEnabled && log.IsErrorEnabled && log.IsFatalEnabled, "levels below Warn are disabled");
                    log.Debug("d"); log.Debug("d", null); log.DebugFormat("d{0}", 1);
                    log.Info("i"); log.Info("i", null); log.InfoFormat("i{0}", 1);
                    log.Warn("warn-line"); log.Warn("w", new Exception("w")); log.WarnFormat("w{0}", 1);
                    log.Error("error-line"); log.Error("e", new Exception("boom")); log.ErrorFormat("e{0}", 1);
                    log.Fatal("fatal-line"); log.Fatal("f", new Exception("f")); log.FatalFormat("f{0}", 1);
                    var text = string.Concat(System.IO.Directory.GetFiles(directory).Select(System.IO.File.ReadAllText));
                    That(text.Contains("warn-line") && text.Contains("error-line") && text.Contains("fatal-line") && text.Contains("boom"), "warnings and above are written");
                    That(!text.Contains("Debug") && !text.Contains(" Info "), "lower levels are not written");
                    That(Bq.Messaging.Logging.LogManager.GetLogger("named") != null, "loggers can be named");
                    Throws<ArgumentException>(() => Bq.Messaging.Logging.LogManager.GetLogger(""), "an empty logger name");
                    Throws<ArgumentNullException>(() => Bq.Messaging.Logging.LogManager.GetLogger((Type)null), "a null type");
                    Throws<ArgumentException>(() => factory.Directory(" "), "a blank directory");
                    Throws<ArgumentNullException>(() => Bq.Messaging.Logging.LogManager.UseFactory(null), "a null factory");
                }
                finally
                {
                    Bq.Messaging.Logging.LogManager.Use<Bq.Messaging.Logging.DefaultFactory>();
                    if (System.IO.Directory.Exists(directory)) System.IO.Directory.Delete(directory, true);
                }
            });
        }

        static async Task RunSettingsAndTestingCases()
        {
            Case("settings holder get, default, override and clear semantics", () =>
            {
                var settings = new Bq.Messaging.Settings.SettingsHolder();
                Throws<ArgumentException>(() => settings.Set("", 1), "an empty key");
                Throws<System.Collections.Generic.KeyNotFoundException>(() => settings.Get("missing"), "a missing key");
                settings.SetDefault("k", "default");
                That(settings.Get<string>("k") == "default" && !settings.HasExplicitValue("k") && settings.HasSetting("k"), "a default is visible but not explicit");
                settings.Set("k", "override");
                That(settings.Get<string>("k") == "override" && settings.HasExplicitValue("k"), "an override wins");
                That(settings.TryGet<string>("k", out var text) && text == "override", "TryGet finds a typed value");
                That(!settings.TryGet<int>("k", out var number) && number == 0, "TryGet fails for the wrong type");
                That(!settings.TryGet<string>("none", out _), "TryGet fails for a missing key");
                That(settings.GetOrDefault<string>("none") == null && settings.GetOrDefault<string>("k") == "override", "GetOrDefault");
                settings.Set<EdgeCaseLogProbe>(new EdgeCaseLogProbe());
                That(settings.HasSetting<EdgeCaseLogProbe>() && settings.HasExplicitValue<EdgeCaseLogProbe>() && settings.Get<EdgeCaseLogProbe>() != null, "typed keys");
                That(settings.TryGet<EdgeCaseLogProbe>(out _) && settings.GetOrDefault<EdgeCaseLogProbe>() != null, "typed TryGet and GetOrDefault");
                settings.SetDefault<EdgeUowMessage>(new EdgeUowMessage { Text = "d" });
                That(settings.GetOrCreate<EdgeUowMessage>().Text == "d", "GetOrCreate returns an existing default");
                That(settings.GetOrCreate<EdgeFeatureHolder>() == settings.GetOrCreate<EdgeFeatureHolder>(), "GetOrCreate creates once");
                settings.Clear();
                That(!settings.HasSetting("k"), "Clear removes everything");
            });
            await CaseAsync("testable handler context records every send, publish, reply and forward", async () =>
            {
                var context = new Bq.Testing.TestableMessageHandlerContext { MessageId = "in-1", ReplyToAddress = "reply.queue", CorrelationId = "corr" };
                var message = new EdgeUowMessage { Text = "m" };
                await context.Send(message);
                await context.Send<EdgeUowMessage>(m => m.Text = "init");
                await context.Send("dest", message);
                await context.SendLocal(message);
                await context.Publish(message);
                await context.Publish<EdgeUowMessage>(m => m.Text = "pub");
                await context.SendAt(message, DateTimeOffset.UtcNow.AddHours(1));
                await context.SendAt("dest", message, DateTimeOffset.UtcNow.AddHours(1));
                await context.SendLocalAt(message, DateTimeOffset.UtcNow.AddHours(1));
                await context.PublishAt(message, DateTimeOffset.UtcNow.AddHours(1));
                var send = new SendOptions(); send.SetHeader("h", "v"); send.SetDestination("opt.dest"); send.SetMessageId("custom-id");
                await context.Send(message, send);
                await context.Send((object)message, new SendOptions());
                await context.Send<EdgeUowMessage>(m => m.Text = "x", new SendOptions());
                await context.Send("string-body", new SendOptions());
                var delayed = new SendOptions(); delayed.DelayDeliveryWith(TimeSpan.FromMinutes(5)); delayed.RouteToThisEndpoint();
                await context.Send(message, delayed);
                var immediate = new SendOptions(); immediate.RequireImmediateDispatch(); immediate.RouteToThisEndpoint();
                await context.Send(message, immediate);
                await context.Publish(message, new PublishOptions());
                await context.Publish((object)message, new PublishOptions());
                await context.Publish<EdgeUowMessage>(m => m.Text = "p", new PublishOptions());
                var publishImmediately = new PublishOptions(); publishImmediately.RequireImmediateDispatch();
                await context.Publish(message, publishImmediately);
                await context.Reply(message);
                await context.Reply<EdgeUowMessage>(m => m.Text = "r");
                var reply = new ReplyOptions(); reply.SetHeader("rh", "rv"); reply.RequireImmediateDispatch();
                await context.Reply(message, reply);
                await context.Reply((object)message, new ReplyOptions());
                await context.Reply<EdgeUowMessage>(m => m.Text = "ro", new ReplyOptions());
                await context.ForwardCurrentMessageTo("forward.queue");
                await context.HandleCurrentMessageLater();
                context.DoNotContinueDispatchingCurrentMessageToHandlers();
                var recorded = context.Messages;
                That(recorded.Count == 26, "recorded " + recorded.Count + " messages");
                That(recorded[2].Destination == "dest" && recorded[3].Operation == Bq.Testing.MessageOperation.SendLocal, "destinations and operations are recorded");
                That(recorded[6].DueAt.HasValue && recorded[9].Operation == Bq.Testing.MessageOperation.Publish && recorded[9].DueAt.HasValue, "scheduled sends and publishes record the due time");
                That(recorded[10].Headers["h"].Equals("v") && recorded[10].Destination == "opt.dest" && recorded[10].MessageId == "custom-id", "send options headers, destination and id");
                That(recorded[14].ImmediateDispatch || recorded[15].ImmediateDispatch, "immediate dispatch is recorded");
                var replies = recorded.Where(r => r.Operation == Bq.Testing.MessageOperation.Reply).ToList();
                That(replies.Count == 5 && replies[0].Destination == "reply.queue" && replies[0].CorrelationId == "corr", "replies go to the reply address with the correlation id");
                That(replies[2].Headers["rh"].Equals("rv") && replies[2].ImmediateDispatch, "reply options headers and immediate dispatch");
                That(recorded.Last().Operation == Bq.Testing.MessageOperation.Forward && recorded.Last().Destination == "forward.queue", "forward is recorded");
                That(context.HandleCurrentMessageLaterCalled && context.HandlerDispatchStopped, "flags are recorded");
                Throws<ArgumentException>(() => context.ForwardCurrentMessageTo(" ").GetAwaiter().GetResult(), "a blank forward destination");
                context.Headers["x"] = "y";
                That(context.MessageHeaders["x"] == "y", "text headers reflect the incoming headers");
                var withHeader = context.UniformSession.WithHeader("scoped", "1");
                await withHeader.Send(message);
                That(context.Messages.Last().Headers["scoped"].Equals("1"), "a scoped session adds headers");
                Throws<ArgumentException>(() => context.UniformSession.WithHeader(" ", "1"), "a blank header name");
                await ((IMessageHandlerContext)context).Send<EdgeUowMessage>(m => { });
                await context.PublishAt(message, DateTimeOffset.UtcNow);
            });
            Case("send and reply options validate their inputs", () =>
            {
                var send = new SendOptions();
                Throws<ArgumentException>(() => send.SetHeader(" ", "x"), "a blank header name");
                Throws<ArgumentException>(() => send.SetHeader("x-bq-reserved", "x"), "a reserved header prefix");
                Throws<ArgumentException>(() => send.SetDestination(" "), "a blank destination");
                Throws<ArgumentOutOfRangeException>(() => send.DelayDeliveryWith(TimeSpan.FromSeconds(-1)), "a negative delay");
                send.DelayDeliveryWith(TimeSpan.FromSeconds(1));
                send.DoNotDeliverBefore(DateTimeOffset.UtcNow);
                That(send.GetDeliveryDate().HasValue && !send.GetDeliveryDelay().HasValue, "a later date replaces an earlier delay");
                var immediateOptions = new SendOptions(); immediateOptions.RequireImmediateDispatch();
                Throws<InvalidOperationException>(() => immediateOptions.DelayDeliveryWith(TimeSpan.FromSeconds(1)), "a delay after immediate dispatch");
                Throws<InvalidOperationException>(() => immediateOptions.DoNotDeliverBefore(DateTimeOffset.UtcNow), "a date after immediate dispatch");
                Throws<InvalidOperationException>(() => send.RequireImmediateDispatch(), "immediate dispatch after scheduling");
                var scheduled = new SendOptions();
                scheduled.DoNotDeliverBefore(DateTimeOffset.UtcNow.AddMinutes(1));
                That(scheduled.GetDeliveryDate().HasValue && !scheduled.GetDeliveryDelay().HasValue, "a date is recorded");
                var local = new SendOptions(); local.RouteToThisEndpoint();
                That(local.IsRoutingToThisEndpoint(), "local routing is recorded");
                Throws<InvalidOperationException>(() => local.SetDestination("somewhere"), "a destination after local routing");
                var reply = new ReplyOptions();
                reply.RouteReplyTo("r"); reply.SetDestination("d");
                That(reply.GetReplyToRoute() == "r" && reply.GetDestination() == "d", "reply routing is recorded");
                var conversation = new SendOptions(); conversation.StartNewConversation("conv-1");
                conversation.RouteReplyTo("back"); conversation.SetMessageId("id-1");
                That(conversation.GetMessageId() == "id-1" && conversation.GetReplyToRoute() == "back", "options keep ids and reply addresses");
                conversation.SetHeader("k", "v");
                That(conversation.GetHeaders()["k"] == "v", "text headers are readable");
            });
        }

        static void RunSerializationCases()
        {
            foreach (var definition in new Bq.Messaging.Serialization.SerializationDefinition[] { new Bq.Messaging.Serialization.JsonSerializer(), new Bq.Messaging.Serialization.MessagePackSerialization() })
            {
                var current = definition;
                Case("serializer round trip: " + current.GetType().Name, () =>
                {
                    var serializer = current.Configure(new Bq.Messaging.Settings.SettingsHolder())(null);
                    var message = new EdgeUowMessage { Text = "hello" };
                    using (var stream = new System.IO.MemoryStream())
                    {
                        serializer.Serialize(message, stream);
                        stream.Position = 0;
                        var read = serializer.Deserialize(stream, new[] { typeof(EdgeUowMessage) });
                        That(read.Length == 1 && ((EdgeUowMessage)read[0]).Text == "hello", "the typed message round trips");
                    }
                    var wire = current.CreateWireFormat();
                    var bytes = wire.Serialize(typeof(EdgeUowMessage), message);
                    That(((EdgeUowMessage)wire.Deserialize(typeof(EdgeUowMessage), bytes)).Text == "hello", "the wire format round trips");
                    That(wire.ContentType == serializer.ContentType && !string.IsNullOrEmpty(wire.ContentType), "content types agree");
                    using (var stream = new System.IO.MemoryStream(bytes))
                    {
                        var untyped = serializer.Deserialize(stream);
                        That(untyped.Length == 1, "reading without a type list still returns one value");
                    }
                });
            }
            Case("rijndael expired key collection operations", () =>
            {
                var collection = new Bq.Messaging.Config.RijndaelExpiredKeyCollection();
                var first = new Bq.Messaging.Config.RijndaelExpiredKey { Key = "k1", KeyIdentifier = "id1" };
                var second = new Bq.Messaging.Config.RijndaelExpiredKey { Key = "k2" };
                collection.Add(first); collection.Add(second);
                That(collection.Count == 2 && collection[1].Key == "k2" && collection.IndexOf(second) == 1, "keys are stored in order");
                That(first.KeyIdentifier == "id1" && first.KeyFormat == Bq.Messaging.Config.KeyFormat.Ascii, "the key format defaults to ASCII");
                collection.Remove(first);
                That(collection.Count == 1, "a key is removed");
                collection.RemoveAt(0);
                collection.Add(first);
                collection.Clear();
                That(collection.Count == 0, "the collection clears");
                var section = new Bq.Messaging.Config.RijndaelEncryptionServiceConfig { Key = "abc", KeyIdentifier = "main", KeyFormat = Bq.Messaging.Config.KeyFormat.Base64 };
                section.ExpiredKeys = collection;
                That(section.Key == "abc" && section.KeyIdentifier == "main" && section.KeyFormat == Bq.Messaging.Config.KeyFormat.Base64 && section.ExpiredKeys == collection, "section properties round trip");
            });
        }

        static async Task RunSagaSpiCases()
        {
            Case("saga metadata and collection lookups", () =>
            {
                var metadata = Bq.Messaging.Sagas.SagaMetadata.Create(typeof(ScannedSaga));
                That(metadata.SagaType == typeof(ScannedSaga) && metadata.SagaEntityType == typeof(ScannedSagaState) && metadata.EntityName == typeof(ScannedSagaState).FullName, "type names");
                That(metadata.IsMessageAllowedToStartTheSaga(typeof(SagaStart).FullName) && !metadata.IsMessageAllowedToStartTheSaga(typeof(SagaDone).FullName), "only the starting message may start the saga");
                That(metadata.AssociatedMessages.Count == 2, "both messages are associated");
                That(metadata.TryGetCorrelationProperty(out var correlation) && correlation.Name == "OrderId" && correlation.Type == typeof(string), "the correlation property is found");
                That(!metadata.TryGetFinder(typeof(SagaStart).FullName, out var finder) && finder == null, "Bq has no custom finders");
                That(Bq.Messaging.Sagas.SagaMetadata.IsSagaType(typeof(ScannedSaga)) && !Bq.Messaging.Sagas.SagaMetadata.IsSagaType(typeof(string)), "saga types are recognized");
                Throws<ArgumentException>(() => Bq.Messaging.Sagas.SagaMetadata.Create(typeof(string)), "a non-saga type");
                Throws<ArgumentNullException>(() => Bq.Messaging.Sagas.SagaMetadata.Create(null), "a null type");
                var collection = new Bq.Messaging.Sagas.SagaMetadataCollection();
                collection.Initialize(new[] { typeof(ScannedSaga), typeof(string), typeof(LegacyMappedSaga) });
                That(collection.Count() == 2 && collection.Find(typeof(ScannedSaga)).SagaEntityType == typeof(ScannedSagaState), "the collection holds only sagas");
                That(collection.FindByEntity(typeof(LegacyState)).SagaType == typeof(LegacyMappedSaga), "lookup by entity");
                Throws<System.Collections.Generic.KeyNotFoundException>(() => collection.Find(typeof(string)), "an unknown saga");
                Throws<System.Collections.Generic.KeyNotFoundException>(() => collection.FindByEntity(typeof(string)), "an unknown entity");
                Throws<ArgumentNullException>(() => collection.Initialize(null), "null types");
                That(new Bq.Messaging.Sagas.SagaFinderDefinition(typeof(ScannedSaga), null, null).Properties.Count == 0, "finder properties default to empty");
                Throws<ArgumentNullException>(() => new Bq.Messaging.Sagas.SagaCorrelationProperty(null, 1), "a null property name");
                Throws<ArgumentNullException>(() => new Bq.Messaging.Sagas.SagaCorrelationProperty("n", null), "a null property value");
                Throws<ArgumentNullException>(() => new Bq.Messaging.Sagas.SagaMessage(null, true), "a null message type");
            });
            await CaseAsync("active saga instance and the persister contract", async () =>
            {
                var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                Throws<ArgumentNullException>(() => new Bq.Messaging.Sagas.ActiveSagaInstance(null, null, null), "a missing clock");
                var persister = new Bq.Messaging.Sagas.PersistenceSagaPersister();
                var data = new EdgeSagaData();
                await ThrowsAsync<NotSupportedException>(() => persister.Save(data, null, null, null), "Save");
                await ThrowsAsync<NotSupportedException>(() => persister.Update(data, null, null), "Update");
                await ThrowsAsync<NotSupportedException>(() => persister.Complete(data, null, null), "Complete");
                await ThrowsAsync<NotSupportedException>(() => persister.Get<EdgeSagaData>(Guid.NewGuid(), null, null), "Get by id");
                await ThrowsAsync<ArgumentException>(() => persister.Get<EdgeSagaData>("OrderId", "1", null, null), "Get without a Bq session");
                var memory = new InMemoryPersistenceProvider("edge-spi");
                using (var session = await memory.OpenSession())
                {
                    That(await persister.Get<EdgeSagaData>("OrderId", "missing", (SynchronizedStorageSession)session, null) == null, "a missing saga returns null");
                    var state = new SagaStoredState<EdgeSagaData> { InstanceId = "1", Data = new EdgeSagaData { Originator = "me" } };
                    var options = MessagePack.MessagePackSerializerOptions.Standard.WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
                    await session.SaveSaga(typeof(EdgeSagaData).FullName, "42", MessagePack.MessagePackSerializer.Serialize(state, options), null);
                    var found = await persister.Get<EdgeSagaData>("OrderId", 42, (SynchronizedStorageSession)session, null);
                    That(found != null && found.Originator == "me", "a stored saga is read back by its correlation value");
                }
            });
        }

        static async Task ThrowsAsync<T>(Func<Task> action, string name) where T : Exception
        {
            try { await action(); }
            catch (T) { return; }
            catch (Exception ex) { throw new Exception(name + " threw " + ex.GetType().Name + " instead of " + typeof(T).Name); }
            throw new Exception(name + " did not throw " + typeof(T).Name);
        }

        sealed class EdgeStorageSession : Bq.Messaging.Persistence.CompletableSynchronizedStorageSession
        {
            public bool Completed, Disposed;
            public System.Data.Common.DbConnection Connection => null;
            public System.Data.Common.DbTransaction Transaction => null;
            public Task CompleteAsync() { Completed = true; return Task.CompletedTask; }
            public void Dispose() { Disposed = true; }
        }

        sealed class EdgeStorage : Bq.Messaging.Persistence.ISynchronizedStorage, Bq.Messaging.Persistence.ISynchronizedStorageAdapter
        {
            public readonly EdgeStorageSession Session = new EdgeStorageSession();
            public bool AdaptOutbox, AdaptTransport;
            public Task<Bq.Messaging.Persistence.CompletableSynchronizedStorageSession> OpenSession(ContextBag contextBag) =>
                Task.FromResult<Bq.Messaging.Persistence.CompletableSynchronizedStorageSession>(Session);
            public Task<Bq.Messaging.Persistence.CompletableSynchronizedStorageSession> TryAdapt(Bq.Messaging.Outbox.OutboxTransaction transaction, ContextBag context) =>
                Task.FromResult<Bq.Messaging.Persistence.CompletableSynchronizedStorageSession>(AdaptOutbox ? Session : null);
            public Task<Bq.Messaging.Persistence.CompletableSynchronizedStorageSession> TryAdapt(Bq.Messaging.Transport.TransportTransaction transaction, ContextBag context) =>
                Task.FromResult<Bq.Messaging.Persistence.CompletableSynchronizedStorageSession>(AdaptTransport ? Session : null);
        }

        static async Task RunSpiTypeCases()
        {
            Case("delivery constraints live on the operation's context bag", () =>
            {
                var bag = new ContextBag();
                var delay = new Bq.Messaging.DelayedDelivery.DelayDeliveryWith(TimeSpan.FromSeconds(5));
                var at = new Bq.Messaging.DelayedDelivery.DoNotDeliverBefore(DateTime.UtcNow);
                var ttl = new Bq.Messaging.Performance.TimeToBeReceived.DiscardIfNotReceivedBefore(TimeSpan.FromMinutes(1));
                bag.AddDeliveryConstraint(delay); bag.AddDeliveryConstraint(at); bag.AddDeliveryConstraint(ttl);
                That(bag.GetDeliveryConstraints().Count == 3 && delay.Delay == TimeSpan.FromSeconds(5) && ttl.MaxTime == TimeSpan.FromMinutes(1), "constraints are added");
                That(bag.TryGetDeliveryConstraint<Bq.Messaging.DelayedDelivery.DelayedDeliveryConstraint>(out var first) && first == delay, "the first matching constraint is returned");
                That(!bag.TryGetDeliveryConstraint<EdgeConstraint>(out var none) && none == null, "a missing constraint is absent");
                bag.RemoveDeliveryConstraint(delay);
                bag.RemoveDeliveryConstaint(at);
                That(bag.GetDeliveryConstraints().Count == 1, "constraints are removed");
                That(bag.TryRemoveDeliveryConstraint<Bq.Messaging.Performance.TimeToBeReceived.DiscardIfNotReceivedBefore>(out var removed) && removed == ttl, "TryRemove removes a match");
                That(!bag.TryRemoveDeliveryConstraint<Bq.Messaging.Performance.TimeToBeReceived.DiscardIfNotReceivedBefore>(out _), "TryRemove reports a miss");
                Throws<ArgumentNullException>(() => bag.AddDeliveryConstraint(null), "a null constraint");
                Throws<ArgumentNullException>(() => ((ContextBag)null).GetDeliveryConstraints(), "a null bag");
                Throws<NotSupportedException>(() => new EndpointConfiguration("x", new Uri("amqp://localhost/")).UseExternalTimeoutManager("tm"), "an external timeout manager");
            });
            Case("timeout SPI data types", () =>
            {
                var data = new Bq.Messaging.Timeout.Core.TimeoutData { Id = "t", Time = DateTime.UtcNow, Destination = "d", SagaId = Guid.NewGuid(), State = new byte[1], OwningTimeoutManager = "m", Headers = new Dictionary<string, string>() };
                That(data.ToString().StartsWith("Timeout t due "), "timeout data describes itself");
                var timeout = new Bq.Messaging.Timeout.Core.Timeout("t", data.Time);
                var chunk = new Bq.Messaging.Timeout.Core.TimeoutsChunk(new[] { timeout }, data.Time);
                That(chunk.DueTimeouts[0].Id == "t" && chunk.NextTimeToQuery == data.Time && timeout.DueTime == data.Time, "chunks carry timeouts");
            });
            await CaseAsync("synchronized storage adapter opens, adapts and completes sessions", async () =>
            {
                var storage = new EdgeStorage();
                var adapter = new Bq.Messaging.Persistence.CompletableSynchronizedStorageSessionAdapter(storage, storage);
                That(adapter.Connection == null && adapter.Transaction == null && adapter.AdaptedSession == null, "an unopened adapter has no session");
                await adapter.CompleteAsync();
                await adapter.Open(new ContextBag());
                That(adapter.AdaptedSession == storage.Session, "Open uses the synchronized storage");
                await adapter.CompleteAsync();
                adapter.Dispose();
                That(storage.Session.Completed && storage.Session.Disposed, "complete and dispose reach the session");
                That(!await adapter.TryOpen((Bq.Messaging.Outbox.OutboxTransaction)null, new ContextBag()), "no outbox adaptation");
                storage.AdaptOutbox = true;
                That(await adapter.TryOpen((Bq.Messaging.Outbox.OutboxTransaction)null, new ContextBag()), "outbox adaptation");
                storage.AdaptOutbox = false; storage.AdaptTransport = true;
                That(await adapter.TryOpen(new Bq.Messaging.Transport.TransportTransaction(), new ContextBag()), "transport adaptation");
                await Bq.Messaging.Persistence.CompletableSynchronizedStorageSessionAdapterExtensions.Open(adapter, null, new Bq.Messaging.Transport.TransportTransaction(), new ContextBag());
                That(adapter.AdaptedSession != null, "the combined open falls back to the transport transaction");
                var withoutAdapter = new Bq.Messaging.Persistence.CompletableSynchronizedStorageSessionAdapter(null, storage);
                That(!await withoutAdapter.TryOpen(new Bq.Messaging.Transport.TransportTransaction(), new ContextBag()) &&
                    !await withoutAdapter.TryOpen((Bq.Messaging.Outbox.OutboxTransaction)null, new ContextBag()), "no adapter adapts nothing");
                Throws<ArgumentNullException>(() => new Bq.Messaging.Persistence.PersistenceSynchronizedStorage(null), "a missing provider");
                var memory = new Bq.Messaging.Persistence.PersistenceSynchronizedStorage(new InMemoryPersistenceProvider("edge-sync"));
                That(await memory.TryAdapt((Bq.Messaging.Outbox.OutboxTransaction)null, null) == null, "persistence storage adapts no outbox transaction");
                using (var owned = await memory.OpenSession(new ContextBag())) { await owned.CompleteAsync(); }
            });
            await CaseAsync("pipeline context extension methods send, publish and reply with options", async () =>
            {
                var context = new Bq.Testing.TestableMessageHandlerContext { ReplyToAddress = "back" };
                var message = new EdgeUowMessage();
                await IPipelineContextExtensions.Send(context, "d", message);
                await IPipelineContextExtensions.Send<EdgeUowMessage>(context, "d", m => m.Text = "x");
                await IPipelineContextExtensions.SendLocal(context, message);
                await IPipelineContextExtensions.SendLocal<EdgeUowMessage>(context, m => m.Text = "x");
                await IPipelineContextExtensions.Send(context, message);
                await IPipelineContextExtensions.Publish(context, message);
                await IPipelineContextExtensions.Publish<EdgeUowMessage>(context);
                await IPipelineContextExtensions.Send<EdgeUowMessage>(context, m => m.Text = "x");
                await IPipelineContextExtensions.Publish<EdgeUowMessage>(context, m => m.Text = "x");
                await IMessageProcessingContextExtensions.Reply(context, message);
                await IMessageProcessingContextExtensions.Reply<EdgeUowMessage>(context, m => m.Text = "x");
                await MessageOptionsExtensions.Send(context, "d", message);
                await MessageOptionsExtensions.SendLocal(context, message);
                await MessageOptionsExtensions.Publish<EdgeUowMessage>(context);
                await MessageOptionsExtensions.Reply(context, message);
                var recorded = context.Messages;
                That(recorded.Count == 15, "recorded " + recorded.Count);
                That(recorded[0].Destination == "d" && recorded[2].Operation == Bq.Testing.MessageOperation.SendLocal && recorded[6].Operation == Bq.Testing.MessageOperation.Publish, "operations are recorded");
                That(recorded[9].Operation == Bq.Testing.MessageOperation.Reply && recorded[9].Destination == "back", "replies go to the reply address");
            });
        }

        static EndpointConfiguration Configured(string unicast, string audit = null, string logging = null)
        {
            var sections = new Dictionary<string, string> { ["UnicastBusConfig"] = unicast };
            if (audit != null) sections["AuditConfig"] = audit;
            if (logging != null) sections["Logging"] = logging;
            var configuration = Configuration("edge-config-" + Guid.NewGuid().ToString("N"));
            configuration.CustomConfigurationSource(new XmlConfigurationSource(sections));
            configuration.ApplyConfigurationSections();
            return configuration;
        }

        static void RunConfigurationCases()
        {
            Case("message endpoint mappings by Assembly and Namespace", () =>
            {
                var byNamespace = Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Assembly=""Bq.Messaging.Tests"" Namespace=""Bq.Messaging.Tests"" Endpoint=""ns.endpoint"" /></MessageEndpointMappings></UnicastBusConfig>");
                That(byNamespace.Routes[typeof(SendProbe)] == "ns.endpoint", "a namespace mapping routes message types in it");
                var otherNamespace = Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Assembly=""Bq.Messaging.Tests"" Namespace=""Some.Other.Namespace"" Endpoint=""x"" /></MessageEndpointMappings></UnicastBusConfig>");
                That(otherNamespace.Routes.Count == 0, "a different namespace routes nothing");
                var byType = Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Assembly=""Bq.Messaging.Tests"" Type=""Bq.Messaging.Tests.SendProbe"" Endpoint=""type.endpoint"" /></MessageEndpointMappings></UnicastBusConfig>");
                That(byType.Routes[typeof(SendProbe)] == "type.endpoint" && byType.Routes.Count == 1, "an Assembly and Type pair routes one type");
                var specificWins = Configured(@"<UnicastBusConfig><MessageEndpointMappings>
<add Messages=""Bq.Messaging.Tests"" Endpoint=""assembly.endpoint"" />
<add Messages=""Bq.Messaging.Tests.SendProbe, Bq.Messaging.Tests"" Endpoint=""specific.endpoint"" /></MessageEndpointMappings></UnicastBusConfig>");
                That(specificWins.Routes[typeof(SendProbe)] == "specific.endpoint", "a type mapping beats an assembly mapping");
            });
            Case("invalid message endpoint mappings fail at configuration", () =>
            {
                Throws<System.Configuration.ConfigurationErrorsException>(() => Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Messages=""Bq.Messaging.Tests"" Namespace=""x"" Endpoint=""e"" /></MessageEndpointMappings></UnicastBusConfig>"), "Messages with Namespace");
                Throws<System.Configuration.ConfigurationErrorsException>(() => Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Messages=""Bq.Messaging.Tests"" /></MessageEndpointMappings></UnicastBusConfig>"), "a missing endpoint");
                Throws<System.Configuration.ConfigurationErrorsException>(() => Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Type=""No.Such.Type"" Endpoint=""e"" /></MessageEndpointMappings></UnicastBusConfig>"), "an unknown type");
                Throws<System.Configuration.ConfigurationErrorsException>(() => Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Assembly=""No.Such.Assembly"" Endpoint=""e"" /></MessageEndpointMappings></UnicastBusConfig>"), "an unknown assembly");
                Throws<System.Configuration.ConfigurationErrorsException>(() => Configured(@"<UnicastBusConfig><MessageEndpointMappings><add Namespace=""x"" Endpoint=""e"" /></MessageEndpointMappings></UnicastBusConfig>"), "a mapping naming nothing");
            });
            Case("the Logging section sets the default level and AuditConfig sets an expiry", () =>
            {
                try
                {
                    var configuration = Configured(@"<UnicastBusConfig />", @"<AuditConfig QueueName=""audit"" OverrideTimeToBeReceived=""00:05:00"" />", @"<Logging Threshold=""Error"" />");
                    That(Bq.Messaging.Logging.LogManager.GetLogger("probe").IsErrorEnabled && !Bq.Messaging.Logging.LogManager.GetLogger("probe").IsWarnEnabled, "the threshold applies");
                    That(configuration.AuditQueue == "audit" && configuration.AuditExpiration == "300000", "the audit expiry is in milliseconds: " + configuration.AuditExpiration);
                    var noExpiry = Configured(@"<UnicastBusConfig />", @"<AuditConfig QueueName=""audit"" />");
                    That(noExpiry.AuditQueue == "audit" && noExpiry.AuditExpiration == null, "no override means no expiry");
                    var badLevel = Configured(@"<UnicastBusConfig />", null, @"<Logging Threshold=""Nonsense"" />");
                    That(badLevel != null, "an unknown threshold is ignored");
                }
                finally { Bq.Messaging.Logging.LogManager.Use<Bq.Messaging.Logging.DefaultFactory>(); }
            });
            Case("ForwardReceivedMessagesTo applies unless code set it", () =>
            {
                var fromFile = Configured(@"<UnicastBusConfig ForwardReceivedMessagesTo=""fwd"" />");
                That(fromFile.ForwardReceivedMessagesAddress == "fwd", "the file value applies");
                var coded = Configuration("edge-fwd");
                coded.ForwardReceivedMessagesAddress = "coded";
                coded.CustomConfigurationSource(new XmlConfigurationSource(new Dictionary<string, string> { ["UnicastBusConfig"] = @"<UnicastBusConfig ForwardReceivedMessagesTo=""fwd"" />" }));
                coded.ApplyConfigurationSections();
                That(coded.ForwardReceivedMessagesAddress == "coded", "code wins");
                Throws<ArgumentNullException>(() => Configuration("edge-null").CustomConfigurationSource(null), "a null source");
            });
        }

        static async Task RunContainerAndPersistenceCases()
        {
            Case("the Windsor adapter builds all, registers singletons, scopes and releases", () =>
            {
                using (var windsor = new Castle.Windsor.WindsorContainer())
                {
                    var configuration = Configuration("edge-windsor");
                    configuration.UseContainer<Bq.Extensions.CastleWindsor.WindsorBuilder>(c => c.ExistingContainer(windsor));
                    var singleton = new EdgeServiceOne();
                    configuration.RegisterComponents(components =>
                    {
                        components.ConfigureComponent<EdgeServiceTwo>(DependencyLifecycle.SingleInstance);
                        components.RegisterSingleton<IEdgeService>(singleton);
                        components.ConfigureComponent<EdgeScopedService>(DependencyLifecycle.InstancePerUnitOfWork);
                        components.ConfigureComponent<IEdgeFactoryService>(() => new EdgeFactoryService(), DependencyLifecycle.InstancePerCall);
                        That(components.HasComponent<IEdgeService>() && !components.HasComponent(typeof(string)), "HasComponent reflects registrations");
                    });
                    var root = new Bq.Messaging.Container.ContainerBuilder(configuration.BuildContainer());
                    That(root.BuildAll<IEdgeService>().Count() == 2, "BuildAll returns every registration of a service");
                    That(root.BuildAll(typeof(IEdgeService)).Contains(singleton), "BuildAll includes the registered singleton");
                    That(root.Build<IEdgeFactoryService>() != root.Build<IEdgeFactoryService>(), "an instance-per-call factory component is new each time");
                    var before = EdgeScopedService.Disposed;
                    using (var child = root.CreateChildBuilder())
                    {
                        var scoped = child.Build<EdgeScopedService>();
                        That(scoped == child.Build<EdgeScopedService>(), "a unit-of-work component is shared within a child builder");
                        child.Release(scoped);
                        var seen = new List<object>();
                        child.BuildAndDispatch(typeof(EdgeScopedService), seen.Add);
                        That(seen.Count == 1, "BuildAndDispatch hands the instance to the action");
                    }
                    That(EdgeScopedService.Disposed > before, "ending the child builder releases scoped components");
                    root.Release(root.Build<EdgeServiceTwo>());
                }
            });
            Case("the default container resolves, scopes and reports missing components", () =>
            {
                var configuration = Configuration("edge-default-container");
                configuration.RegisterComponents(components =>
                {
                    components.ConfigureComponent<EdgeServiceOne>(DependencyLifecycle.InstancePerCall);
                    components.ConfigureComponent<EdgeServiceTwo>(DependencyLifecycle.InstancePerCall);
                    components.ConfigureComponent<EdgeScopedService>(DependencyLifecycle.InstancePerUnitOfWork);
                });
                var root = new Bq.Messaging.Container.ContainerBuilder(configuration.BuildContainer());
                That(root.Build<EdgeServiceOne>() != root.Build<EdgeServiceOne>(), "instance per call");
                That(root.BuildAll<EdgeServiceOne>().Count() == 1, "BuildAll finds a concrete registration");
                using (var child = root.CreateChildBuilder())
                    That(child.Build<EdgeScopedService>() == child.Build<EdgeScopedService>(), "scoped within a child");
                Throws<Exception>(() => root.Build(typeof(string)), "an unregistered component");
            });
            Case("UsePersistence selects one definition, applies defaults and runs installers", () =>
            {
                EdgePersistenceA.Initialized = 0;
                var configuration = Configuration("edge-persistence");
                configuration.UsePersistence<EdgePersistenceA>();
                configuration.UsePersistence<EdgePersistenceA, StorageType.Sagas>();
                configuration.UsePersistence(typeof(EdgePersistenceA));
                Throws<InvalidOperationException>(() => configuration.UsePersistence<EdgePersistenceB>(), "a second definition");
                Throws<ArgumentException>(() => configuration.UsePersistence(typeof(string)), "a non-definition type");
                Throws<ArgumentException>(() => configuration.UsePersistence(typeof(PersistenceDefinition)), "the abstract definition");
                configuration.EnableInstallers();
                configuration.ApplyPersistenceSettings();
                That(configuration.Persistence is EdgeInstallableProvider && EdgePersistenceA.Initialized == 1, "the provider is created and its installer runs once");
                That(SettingsOf(configuration).Get<string>("edge-default") == "applied", "definition defaults apply to the settings");
                That(new EdgePersistenceA().HasSupportFor<StorageType.Sagas>() && !new EdgePersistenceA().HasSupportFor(typeof(StorageType.Outbox)), "support is reported per storage type");
                var withoutInstallers = Configuration("edge-no-installers");
                withoutInstallers.UsePersistence<EdgePersistenceA>();
                EdgePersistenceA.Initialized = 0;
                withoutInstallers.ApplyPersistenceSettings();
                That(EdgePersistenceA.Initialized == 0, "installers do not run unless enabled");
                var explicitProvider = Configuration("edge-explicit");
                var memory = new InMemoryPersistenceProvider("edge-explicit");
                explicitProvider.Persistence = memory;
                explicitProvider.UsePersistence<EdgePersistenceB>();
                explicitProvider.ApplyPersistenceSettings();
                That(explicitProvider.Persistence == memory, "a provider set in code is kept");
            });
            await Task.CompletedTask;
        }

        static void RunStartValidationCases()
        {
            // Start validates its configuration before it opens a connection, so none of these need a broker.
            void Invalid<T>(string name, Action<EndpointConfiguration> change) where T : Exception
            {
                Case("Start rejects " + name, () =>
                {
                    var configuration = new EndpointConfiguration("edge-validate", new Uri("amqp://localhost:1/")) { Persistence = new InMemoryPersistenceProvider("edge-validate") };
                    change(configuration);
                    Throws<T>(() => RabbitEndpoint.Start(configuration), name);
                });
            }
            Invalid<ArgumentException>("an empty events exchange", c => c.EventsExchange = " ");
            Invalid<ArgumentException>("an empty error exchange", c => c.ErrorExchange = "");
            Invalid<ArgumentException>("the same events and error exchange", c => { c.EventsExchange = "same"; c.ErrorExchange = "same"; });
            Invalid<ArgumentException>("a dead-letter exchange argument", c => c.InputQueueArguments["x-dead-letter-exchange"] = "x");
            Invalid<ArgumentException>("a dead-letter routing key argument", c => c.InputQueueArguments["x-dead-letter-routing-key"] = "x");
            Invalid<InvalidOperationException>("a send-only endpoint with handlers", c =>
            {
                c.SendOnly();
                c.Handle(() => new Handler<EdgeUowMessage>((m, ctx) => Task.CompletedTask));
            });
            Invalid<InvalidOperationException>("missing persistence", c => c.Persistence = null);
            Invalid<InvalidOperationException>("persistence without recovery support", c => c.Persistence = new EdgePlainPersistence());
            Invalid<ArgumentException>("a negative delayed retry", c => c.Recoverability.DelayedRetries = new[] { TimeSpan.FromSeconds(-1) });
            Invalid<ArgumentException>("a delayed retry beyond 24 days", c => c.Recoverability.DelayedRetries = new[] { TimeSpan.FromDays(25) });
            Invalid<ArgumentException>("missing delayed retries", c => c.Recoverability.DelayedRetries = null);
            Invalid<ArgumentException>("a blank error queue", c => c.Recoverability.ErrorQueue = " ");
            Invalid<ArgumentException>("an error queue equal to the input queue", c => c.Recoverability.ErrorQueue = "edge-validate");
            Invalid<ArgumentOutOfRangeException>("a zero dispatch poll interval", c => c.DispatchPollInterval = TimeSpan.Zero);
            Invalid<ArgumentOutOfRangeException>("a zero publisher confirm timeout", c => c.PublisherConfirmTimeout = TimeSpan.Zero);
            Invalid<ArgumentOutOfRangeException>("a zero network recovery interval", c => c.NetworkRecoveryInterval = TimeSpan.Zero);
            Invalid<ArgumentOutOfRangeException>("a critical error action without a threshold", c =>
            {
                c.OnCriticalError = (error, exception) => Task.CompletedTask;
                c.CriticalErrorThreshold = TimeSpan.Zero;
            });
            Invalid<ArgumentOutOfRangeException>("zero concurrency", c => c.MaxConcurrency = 0);
            Invalid<ArgumentOutOfRangeException>("negative immediate retries", c => c.ImmediateRetries = -1);
        }

        static async Task RunInMemoryCases()
        {
            await CaseAsync("in-memory recovery records, errors and replay", async () =>
            {
                var memory = new InMemoryPersistenceProvider("edge-memory");
                var due = DateTime.UtcNow.AddMinutes(-1);
                using (var session = await memory.OpenSession())
                {
                    var recovery = (IRecoverySession)session;
                    await recovery.StoreRecovery("edge-memory", new RecoveryRecord { RecoveryId = "r-retry", MessageId = "m1", Cycle = 1, DueAtUtc = due, FailedAtUtc = due, Payload = new byte[] { 1 } });
                    await recovery.StoreRecovery("edge-memory", new RecoveryRecord { RecoveryId = "r-retry", MessageId = "m1", Payload = new byte[] { 9 } });
                    await recovery.StoreRecovery("edge-memory", new RecoveryRecord { RecoveryId = "r-err1", MessageId = "m2", IsError = true, DueAtUtc = due, FailedAtUtc = due.AddSeconds(1), Payload = new byte[] { 2 } });
                    await recovery.StoreRecovery("edge-memory", new RecoveryRecord { RecoveryId = "r-err2", MessageId = "m3", IsError = true, DueAtUtc = due, FailedAtUtc = due.AddSeconds(2), Payload = new byte[] { 3 } });
                    That(await recovery.HasRecovery("edge-memory", "r-err1"), "a staged recovery record is visible to its session");
                    That(!await recovery.HasRecovery("edge-memory", "nope"), "an unknown record is absent");
                    session.Commit();
                }
                var errors = await memory.ListErrors("edge-memory", 10);
                That(errors.Count == 2 && errors[0].RecoveryId == "r-err2", "errors list newest first");
                That((await memory.ListErrors("edge-memory", 1)).Count == 1, "the limit applies");
                That((await memory.GetError("edge-memory", "r-err1")).MessageId == "m2", "GetError finds an error record");
                That(await memory.GetError("edge-memory", "r-retry") == null, "GetError ignores retry records");
                That(await memory.GetError("edge-memory", "none") == null, "GetError ignores unknown ids");
                var first = await memory.NextRecovery("edge-memory");
                That(first != null && !first.Dispatched, "a due record is returned");
                await memory.MarkRecoveryDispatched("edge-memory", "r-retry");
                await memory.MarkRecoveryDispatched("edge-memory", "unknown");
                var replay = new RecoveryRecord { RecoveryId = "r-replay", MessageId = "m2", DueAtUtc = due, Payload = new byte[] { 2 } };
                That(await memory.ReplayError("edge-memory", "r-err1", replay), "a recorded error can be replayed");
                That(!await memory.ReplayError("edge-memory", "r-err1", replay), "an error replays once");
                That(!await memory.ReplayError("edge-memory", "r-retry", replay), "a retry record is not an error");
                That(!await memory.ReplayError("edge-memory", "unknown", replay), "an unknown error cannot replay");
                That(await memory.GetError("edge-memory", "r-err1") == null && (await memory.ListErrors("edge-memory", 10)).Count == 1, "a replayed error leaves the error list");
                Throws<ArgumentOutOfRangeException>(() => memory.ListErrors("edge-memory", 0).GetAwaiter().GetResult(), "a zero limit");
                Throws<InvalidOperationException>(() => memory.ListErrors("other", 1).GetAwaiter().GetResult(), "another endpoint name");
                Throws<ArgumentException>(() => new InMemoryPersistenceProvider(" "), "a blank endpoint");
                await memory.Initialize();
            });
            await CaseAsync("in-memory timeouts, outgoing order and claims", async () =>
            {
                var memory = new InMemoryPersistenceProvider("edge-timeouts");
                using (var session = await memory.OpenSession())
                {
                    await session.StoreTimeout(new OutgoingMessage { Id = "t-late", Body = new byte[0] }, DateTime.UtcNow.AddHours(1));
                    await session.StoreTimeout(new OutgoingMessage { Id = "t-due", Body = new byte[0] }, DateTime.UtcNow.AddMinutes(-1));
                    Throws<ArgumentException>(() => session.StoreTimeout(new OutgoingMessage { Id = "x" }, DateTime.Now.AddMinutes(1)).GetAwaiter().GetResult(), "a non-UTC due time");
                    Throws<InvalidOperationException>(() => session.TryClaimMessage("edge-timeouts", "").GetAwaiter().GetResult(), "an empty message id");
                    await session.StoreOutgoing(new OutgoingMessage { Id = "o1", Body = new byte[0] });
                    session.Commit();
                }
                var seen = new HashSet<string>();
                seen.Add((await memory.NextOutgoing("edge-timeouts")).Id);
                seen.Add((await memory.NextOutgoing("edge-timeouts")).Id);
                That(seen.SetEquals(new[] { "o1", "t-due" }), "outgoing alternates between the outbox and due timeouts: " + string.Join(",", seen));
                await memory.MarkDispatched("edge-timeouts", "o1");
                await memory.MarkDispatched("edge-timeouts", "t-due");
                That(await memory.NextOutgoing("edge-timeouts") == null, "a not-yet-due timeout is not returned");
            });
            await CaseAsync("in-memory saga concurrency at commit and delete", async () =>
            {
                var memory = new InMemoryPersistenceProvider("edge-saga");
                using (var seed = await memory.OpenSession())
                {
                    await seed.SaveSaga("type", "key", new byte[] { 1 }, null);
                    Throws<SagaConcurrencyException>(() => seed.SaveSaga("type", "key", new byte[] { 2 }, null).GetAwaiter().GetResult(), "creating over an existing saga in one session");
                    seed.Commit();
                }
                var first = await memory.OpenSession();
                var second = await memory.OpenSession();
                try
                {
                    var loaded = await first.LoadSaga("type", "key");
                    await first.SaveSaga("type", "key", new byte[] { 3 }, loaded.Version);
                    await second.SaveSaga("type", "key", new byte[] { 4 }, loaded.Version);
                    first.Commit();
                    Throws<SagaConcurrencyException>(() => second.Commit(), "the second concurrent commit");
                }
                finally { first.Dispose(); second.Dispose(); }
                using (var check = await memory.OpenSession())
                {
                    var current = await check.LoadSaga("type", "key");
                    That(current.Version == 2 && current.Data[0] == 3, "the first writer wins");
                    Throws<SagaConcurrencyException>(() => check.DeleteSaga("type", "key", 99).GetAwaiter().GetResult(), "delete with a stale version");
                    Throws<SagaConcurrencyException>(() => check.DeleteSaga("type", "absent", 1).GetAwaiter().GetResult(), "delete of an absent saga");
                    await check.DeleteSaga("type", "key", current.Version);
                    That(await check.LoadSaga("type", "key") == null, "a deleted saga is invisible to its own session");
                    await check.SaveSaga("type", "key", new byte[] { 5 }, null);
                    That((await check.LoadSaga("type", "key")).Data[0] == 5, "a deleted saga can be recreated in the same session");
                    check.Commit();
                }
                using (var again = await memory.OpenSession())
                {
                    var current = await again.LoadSaga("type", "key");
                    await again.DeleteSaga("type", "key", current.Version);
                    again.Commit();
                }
                using (var gone = await memory.OpenSession())
                    That(await gone.LoadSaga("type", "key") == null, "a committed delete removes the saga");
            });
        }
    }

    public sealed class EdgeSatelliteArgs : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            Func<Bq.Messaging.RecoverabilityConfig, Bq.Messaging.ErrorContext, Bq.Messaging.RecoverabilityAction> policy =
                (config, error) => Bq.Messaging.RecoverabilityAction.MoveToError(config.Failed.ErrorQueue);
            Func<Bq.Messaging.ObjectBuilder.IBuilder, Bq.Messaging.Transport.MessageContext, Task> handler = (builder, message) => Task.CompletedTask;
            context.AddSatelliteReceiver("one", "edge.satellite", null, policy, handler);
            try { context.AddSatelliteReceiver(" ", "edge.other", null, policy, handler); throw new Exception("a blank name was accepted"); } catch (ArgumentException) { }
            try { context.AddSatelliteReceiver("blank", " ", null, policy, handler); throw new Exception("a blank address was accepted"); } catch (ArgumentException) { }
            try { context.AddSatelliteReceiver("tx", "edge.tx", Bq.Messaging.TransportTransactionMode.SendsAtomicWithReceive, null, policy, handler); throw new Exception("an unsupported mode was accepted"); } catch (NotSupportedException) { }
            try { context.AddSatelliteReceiver("nopolicy", "edge.nopolicy", null, null, handler); throw new Exception("a null policy was accepted"); } catch (ArgumentNullException) { }
            try { context.AddSatelliteReceiver("nohandler", "edge.nohandler", null, policy, null); throw new Exception("a null handler was accepted"); } catch (ArgumentNullException) { }
            context.AddSatelliteReceiver("two", "edge.satellite", null, policy, handler);
        }
    }
}
