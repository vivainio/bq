using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TUnit.Core;

namespace Bq.Messaging.Tests.TUnitRunner
{
    // Several cases share static probes and the LogManager, so they run one at a time.
    [NotInParallel]
    public class EdgeCases
    {
        public static IEnumerable<Func<string>> Names() =>
            EdgeCaseTests.Collect().GetAwaiter().GetResult().Select(c => (Func<string>)(() => c.Name));

        [Test, MethodDataSource(nameof(Names))]
        public Task Case(string name) => EdgeCaseTests.RunOne(name);
    }

    // These need a broker. They are listed only when BQ_RABBITMQ_URI is set, which scripts/test-rabbit-sim.sh does.
    [NotInParallel]
    public class EdgeBrokerCases
    {
        const string NoBroker = "(no broker configured)";

        static Uri BrokerUri() => SimBroker.Uri();

        public static IEnumerable<Func<string>> Names()
        {
            var uri = BrokerUri();
            if (uri == null) return new Func<string>[] { () => NoBroker };
            return EdgeBrokerTests.Collect(uri).GetAwaiter().GetResult().Select(c => (Func<string>)(() => c.Name));
        }

        [Test, MethodDataSource(nameof(Names))]
        public Task Case(string name)
        {
            if (name == NoBroker) Skip.Test("Set BQ_RABBITMQ_URI to run the broker cases.");
            return EdgeBrokerTests.RunOne(BrokerUri(), name);
        }
    }
}
