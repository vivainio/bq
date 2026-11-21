using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging.Features;

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

    static class EdgeCaseTests
    {
        static readonly List<string> failures = new List<string>();
        static int passed;

        static void Case(string name, Action body)
        {
            try { body(); passed++; }
            catch (Exception ex) { failures.Add(name + ": " + ex.Message); }
        }

        static async Task CaseAsync(string name, Func<Task> body)
        {
            try { await body(); passed++; }
            catch (Exception ex) { failures.Add(name + ": " + ex.Message); }
        }

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

        public static async Task Run()
        {
            RunFeatureCases();
            RunPipelineCases();
            await RunInMemoryCases();
            Console.WriteLine("Edge case tests: " + passed + " passed, " + failures.Count + " failed.");
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
