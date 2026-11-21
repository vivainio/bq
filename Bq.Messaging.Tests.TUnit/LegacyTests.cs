using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MessagePack;
using MessagePack.Resolvers;
using TUnit.Core;

namespace Bq.Messaging.Tests
{
    // The original hand-rolled groups from Bq.Messaging.Tests. Each group is one TUnit test, so a failing group no longer
    // hides the others. The broker outage test stays in the console runner because the test script drives the broker for it.
    partial class Program
    {
        internal static IReadOnlyList<(string Name, Func<Task> Body)> LegacyGroups(Uri uri)
        {
            var groups = new List<(string, Func<Task>)>
            {
                ("MessagePack versioning", MessagePackVersioning),
                ("Transport-free handler and saga tests", TestingSupportTests),
                ("SQLite transaction and saga tests", PersistenceTests),
                ("Oracle persistence (needs BQ_ORACLE_*)", OraclePersistenceTests)
            };
            if (uri != null)
            {
                groups.Add(("Broker integration", () => BrokerTests(uri)));
                groups.Add(("Saga lifecycle and durable timeouts", () => SagaTests(uri)));
                groups.Add(("Durable recoverability and replay", () => RecoverabilityTests(uri)));
                groups.Add(("Stalled dispatcher critical error", () => StalledDispatcherTest(uri)));
                groups.Add(("Migration behaviors and units of work", () => MigrationPipelineTests(uri)));
                groups.Add(("Phased migration routing and wire isolation", () => PhasedMigrationTests(uri)));
                groups.Add(("Assembly scanning", () => ScannerTests(uri)));
                groups.Add(("Instance routing", () => InstanceRoutingTests(uri)));
                groups.Add(("Parity integration", () => ParityIntegrationTests(uri)));
                groups.Add(("Oracle endpoint (needs BQ_ORACLE_*)", () => OracleEndpointTests(uri)));
                groups.Add(("Cross-endpoint order workflow and restart", () => OrderWorkflowEndToEndTest(uri)));
                groups.Add(("Lifecycle and concurrency", () => LifecycleTests.Run(uri)));
                groups.Add(("Uniform session and local send", () => UniformSessionTests.Run(uri, Sqlite)));
            }
            return groups;
        }

        static Task MessagePackVersioning()
        {
            var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
            var bytes = MessagePackSerializer.Serialize(new OldContract { Text = "hello" }, options);
            var newer = MessagePackSerializer.Deserialize<NewContract>(bytes, options);
            Assert(newer.Text == "hello" && newer.Added == 0, "MessagePack versioning");
            var older = MessagePackSerializer.Deserialize<OldContract>(MessagePackSerializer.Serialize(new NewContract { Text = "new", Added = 42 }, options), options);
            Assert(older.Text == "new", "MessagePack unknown property");
            return Task.CompletedTask;
        }
    }
}

namespace Bq.Messaging.Tests.TUnitRunner
{
    [NotInParallel]
    public class LegacyGroups
    {
        static Uri BrokerUri() => SimBroker.Uri();

        public static IEnumerable<Func<string>> Names() =>
            Program.LegacyGroups(BrokerUri()).Select(g => (Func<string>)(() => g.Name));

        [Test, MethodDataSource(nameof(Names))]
        public async Task Group(string name)
        {
            var body = Program.LegacyGroups(BrokerUri()).Single(g => g.Name == name).Body;
            SoftChecks.Begin();
            Exception thrown = null;
            try { await body(); }
            catch (Exception ex) { thrown = ex; }
            SoftChecks.Finish(thrown);
        }
    }
}
