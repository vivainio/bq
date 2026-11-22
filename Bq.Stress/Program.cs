using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using Microsoft.Data.Sqlite;

namespace Bq.Stress
{
    // The payload is fuzzed: random text in many scripts, empty and large bodies. The checksum lets the handler detect corruption.
    public class StressWork
    {
        public string Id { get; set; }
        public int Group { get; set; }
        public string Text { get; set; }
        public byte[] Blob { get; set; }
        public string Checksum { get; set; }
        public int Hops { get; set; }
        public bool Poison { get; set; }
    }

    static class Run
    {
        public static int Seed;
        public static double FailRate = 0.05;
        public static long BacklogLimit = 500;
        public static string DbPath;
        public static long Handled, Attempts, Corrupt, ProducerBatches, ProducerFailures, GarbageSent, Restarts;
        static int counter;
        public static Random NewRandom() => new Random(unchecked(Seed * 31 + Interlocked.Increment(ref counter)));

        public static DbConnection Connect()
        {
            var connection = new SqliteConnection("Data Source=" + DbPath + ";Default Timeout=30");
            return connection;
        }

        public static string Checksum(string text, byte[] blob)
        {
            var bytes = Encoding.UTF8.GetBytes(text ?? "").Concat(blob ?? new byte[0]).ToArray();
            return Convert.ToHexString(SHA256.HashData(bytes));
        }
    }

    public sealed class StressHandler : IHandleMessages<StressWork>
    {
        [ThreadStatic] static Random random;

        static void Exec(DbConnection connection, DbTransaction transaction, string sql, params (string, object)[] values)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;
                cmd.CommandText = sql;
                foreach (var (name, value) in values)
                {
                    var p = cmd.CreateParameter(); p.ParameterName = name; p.Value = value ?? DBNull.Value; cmd.Parameters.Add(p);
                }
                cmd.ExecuteNonQuery();
            }
        }

        public async Task Handle(StressWork message, IMessageHandlerContext context)
        {
            random ??= Run.NewRandom();
            Interlocked.Increment(ref Run.Attempts);
            if (Run.Checksum(message.Text, message.Blob) != message.Checksum)
            {
                Interlocked.Increment(ref Run.Corrupt);
                throw new InvalidOperationException("The payload changed on the way: " + message.Id);
            }
            var roll = random.Next(100);
            if (roll < 2) await Task.Delay(random.Next(1000));
            else if (roll < 25) await Task.Delay(random.Next(30));
            if (message.Poison) throw new InvalidOperationException("poison " + message.Id);
            if (random.NextDouble() < Run.FailRate) throw new InvalidOperationException("transient failure before the write");

            var session = context.PersistenceSession;
            Exec(session.Connection, session.Transaction,
                "INSERT INTO ledger(id,grp,effects) VALUES(@id,@g,1) ON CONFLICT(id) DO UPDATE SET effects=effects+1",
                ("@id", message.Id), ("@g", message.Group));
            if (message.Hops > 0)
            {
                var next = Producer.NewWork(random, message.Id + "/" + message.Hops, message.Group, message.Hops - 1, poison: false);
                Exec(session.Connection, session.Transaction, "INSERT INTO sent(id,grp,poison) VALUES(@id,@g,0)", ("@id", next.Id), ("@g", next.Group));
                await context.SendLocal(next);
            }
            // After the write and the outbox row: a failure here must roll both back.
            if (random.NextDouble() < Run.FailRate / 2) throw new InvalidOperationException("transient failure after the write");
            Interlocked.Increment(ref Run.Handled);
        }
    }

    static class Producer
    {
        static readonly string[] Alphabets =
        {
            "abcdefghijklmnopqrstuvwxyz0123456789 ",
            "åäöÅÄÖéèüßñç",
            "日本語中文한국어",
            "אבגדהעربية",
            "<>&\"'\\/{}[]%$#@!\t\r\n\0",
        };

        static string RandomText(Random rnd)
        {
            switch (rnd.Next(8))
            {
                case 0: return "";
                case 1: return new string('x', rnd.Next(1000, 4000));
            }
            var sb = new StringBuilder();
            for (var i = rnd.Next(1, 200); i > 0; i--)
            {
                if (rnd.Next(20) == 0) sb.Append(char.ConvertFromUtf32(rnd.Next(0x1F300, 0x1F64F)));
                else { var a = Alphabets[rnd.Next(Alphabets.Length)]; sb.Append(a[rnd.Next(a.Length)]); }
            }
            return sb.ToString();
        }

        public static StressWork NewWork(Random rnd, string id, int group, int hops, bool poison)
        {
            var blob = new byte[rnd.Next(5) == 0 ? rnd.Next(0, 64 * 1024) : rnd.Next(0, 64)];
            rnd.NextBytes(blob);
            var text = RandomText(rnd);
            return new StressWork { Id = id, Group = group, Text = text, Blob = blob, Checksum = Run.Checksum(text, blob), Hops = hops, Poison = poison };
        }

        public static async Task Loop(Node node, int index, CancellationToken stop)
        {
            var rnd = new Random(Run.Seed + index);
            while (!stop.IsCancellationRequested)
            {
                // Let the consumers catch up instead of building an unbounded backlog.
                if (Interlocked.Read(ref Run.ProducerBatches) > 0 && Backlog() > Run.BacklogLimit) { await Task.Delay(200); continue; }
                var endpoint = node.Current;
                if (endpoint == null) { await Task.Delay(100); continue; }
                try
                {
                    var abort = rnd.Next(40) == 0;
                    await endpoint.ExecuteTransaction(async session =>
                    {
                        for (var i = rnd.Next(1, 20); i > 0; i--)
                        {
                            var poison = rnd.Next(100) == 0;
                            var work = NewWork(rnd, Guid.NewGuid().ToString("N"), rnd.Next(8), poison ? 0 : rnd.Next(4), poison);
                            using (var cmd = session.Connection.CreateCommand())
                            {
                                cmd.Transaction = session.Transaction;
                                cmd.CommandText = "INSERT INTO sent(id,grp,poison) VALUES(@id,@g,@p)";
                                foreach (var (n, v) in new (string, object)[] { ("@id", work.Id), ("@g", work.Group), ("@p", poison ? 1 : 0) })
                                { var p = cmd.CreateParameter(); p.ParameterName = n; p.Value = v; cmd.Parameters.Add(p); }
                                cmd.ExecuteNonQuery();
                            }
                            await session.SendLocal(work);
                        }
                        // A producer that fails after sending must leave neither a sent row nor a message.
                        if (abort) throw new InvalidOperationException("producer abort");
                    });
                    Interlocked.Increment(ref Run.ProducerBatches);
                }
                catch { Interlocked.Increment(ref Run.ProducerFailures); await Task.Delay(100); }
                await Task.Delay(rnd.Next(0, 20));
            }
        }

        static long Backlog()
        {
            try
            {
                using (var c = Run.Connect())
                {
                    c.Open();
                    using (var cmd = c.CreateCommand())
                    {
                        cmd.CommandText = "SELECT (SELECT COUNT(*) FROM sent)-(SELECT COUNT(*) FROM ledger)";
                        return Convert.ToInt64(cmd.ExecuteScalar());
                    }
                }
            }
            catch { return 0; }
        }
    }

    sealed class Node
    {
        readonly string name;
        readonly Uri uri;
        RabbitEndpoint endpoint;
        public Node(string name, Uri uri) { this.name = name; this.uri = uri; }
        public RabbitEndpoint Current => Volatile.Read(ref endpoint);
        public string Name => name;

        public void Start()
        {
            var config = new EndpointConfiguration(name, uri)
            {
                Persistence = new SqlitePersistence(Run.Connect, name),
                MaxConcurrency = (ushort)int.Parse(Environment.GetEnvironmentVariable("BQ_STRESS_CONCURRENCY") ?? "8"),
                OnCriticalError = (message, ex) => { Console.WriteLine("critical error: " + message + " " + ex?.Message); return Task.CompletedTask; },
            };
            config.Recoverability.ImmediateRetries = 2;
            config.Recoverability.DelayedRetries = new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            config.Handle(() => new StressHandler());
            Volatile.Write(ref endpoint, RabbitEndpoint.Start(config));
        }

        // A graceful restart stops the consumer first; the other kind disposes it with work in flight, as a crash would.
        public async Task Restart(bool graceful)
        {
            var old = Interlocked.Exchange(ref endpoint, null);
            if (old == null) return;
            if (graceful) await old.StopAsync();
            old.Dispose();
            Interlocked.Increment(ref Run.Restarts);
            await Task.Delay(500);
            Start();
        }
    }

    static class Program
    {
        static async Task<int> Main()
        {
            var seconds = int.Parse(Environment.GetEnvironmentVariable("BQ_STRESS_SECONDS") ?? "300");
            Run.Seed = int.Parse(Environment.GetEnvironmentVariable("BQ_STRESS_SEED") ?? new Random().Next(1, int.MaxValue).ToString());
            Run.FailRate = double.Parse(Environment.GetEnvironmentVariable("BQ_STRESS_FAIL_RATE") ?? "0.05", System.Globalization.CultureInfo.InvariantCulture);
            Run.BacklogLimit = long.Parse(Environment.GetEnvironmentVariable("BQ_STRESS_BACKLOG") ?? "500");
            var producers = int.Parse(Environment.GetEnvironmentVariable("BQ_STRESS_PRODUCERS") ?? "4");
            var uri = new Uri(Environment.GetEnvironmentVariable("BQ_RABBITMQ_URI") ?? "amqp://guest:guest@127.0.0.1:5672/");
            var admin = Environment.GetEnvironmentVariable("BQ_RABBITSIM_HTTP");
            Run.DbPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "bq-stress-" + Guid.NewGuid().ToString("N") + ".db");
            Console.WriteLine($"Stress run: {seconds}s, seed {Run.Seed}, {producers} producers, failure rate {Run.FailRate}, db {Run.DbPath}");
            Console.WriteLine("Repeat it with BQ_STRESS_SEED=" + Run.Seed + " (thread timing still varies, so a failure may not repeat exactly).");

            var persistence = new SqlitePersistence(Run.Connect, "bq.stress");
            await persistence.Initialize();
            using (var c = Run.Connect())
            {
                c.Open();
                foreach (var sql in new[]
                {
                    "PRAGMA journal_mode=WAL",
                    "CREATE TABLE IF NOT EXISTS sent(id TEXT PRIMARY KEY, grp INTEGER NOT NULL, poison INTEGER NOT NULL)",
                    "CREATE TABLE IF NOT EXISTS ledger(id TEXT PRIMARY KEY, grp INTEGER NOT NULL, effects INTEGER NOT NULL)",
                })
                using (var cmd = c.CreateCommand()) { cmd.CommandText = sql; cmd.ExecuteNonQuery(); }
            }

            var node = new Node("bq.stress", uri);
            node.Start();
            using var cts = new CancellationTokenSource();
            var rnd = new Random(Run.Seed);
            var tasks = Enumerable.Range(0, producers).Select(i => Task.Run(() => Producer.Loop(node, i, cts.Token))).ToList();
            tasks.Add(Task.Run(() => Faults.Chaos(admin, new Random(Run.Seed + 1000), cts.Token)));
            tasks.Add(Task.Run(() => Faults.Garbage(uri, node.Name, new Random(Run.Seed + 2000), cts.Token)));
            tasks.Add(Task.Run(() => Faults.Restarts(node, seconds, new Random(Run.Seed + 3000), cts.Token)));
            tasks.Add(Task.Run(() => Report(seconds, cts.Token)));

            await Task.Delay(TimeSpan.FromSeconds(seconds));
            cts.Cancel();
            try { await Task.WhenAll(tasks); } catch (OperationCanceledException) { }
            await Faults.ClearChaos(admin);
            if (node.Current == null) node.Start();

            return await Verify(node);
        }

        static async Task Report(int seconds, CancellationToken stop)
        {
            var started = DateTime.UtcNow;
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(10_000, stop);
                    Console.WriteLine($"[{(DateTime.UtcNow - started).TotalSeconds,5:0}s] handled {Interlocked.Read(ref Run.Handled)}, attempts {Interlocked.Read(ref Run.Attempts)}, " +
                        $"batches {Interlocked.Read(ref Run.ProducerBatches)}, producer failures {Interlocked.Read(ref Run.ProducerFailures)}, garbage {Interlocked.Read(ref Run.GarbageSent)}, restarts {Interlocked.Read(ref Run.Restarts)}");
                }
            }
            catch (OperationCanceledException) { }
        }

        static long Scalar(string sql)
        {
            using (var c = Run.Connect())
            {
                c.Open();
                using (var cmd = c.CreateCommand()) { cmd.CommandText = sql; return Convert.ToInt64(cmd.ExecuteScalar()); }
            }
        }

        static List<string> Ids(string sql)
        {
            var list = new List<string>();
            using (var c = Run.Connect())
            {
                c.Open();
                using (var cmd = c.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var r = cmd.ExecuteReader()) while (r.Read()) list.Add(r.GetString(0));
                }
            }
            return list;
        }

        // Nothing sent may be lost, applied twice or invented. Every sent message ends in the ledger, or in the error queue.
        static async Task<int> Verify(Node node)
        {
            Console.WriteLine("Faults are off. Waiting for the endpoint to drain.");
            long sent = 0, ledger = 0, errors = 0;
            var stableSince = DateTime.UtcNow;
            var last = (-1L, -1L, -1L);
            for (var i = 0; i < 120; i++)
            {
                await Task.Delay(3000);
                sent = Scalar("SELECT COUNT(*) FROM sent");
                ledger = Scalar("SELECT COUNT(*) FROM ledger");
                var records = await node.Current.ListErrors(1_000_000);
                errors = records.Count(r => r.IsError);
                var accounted = ledger + (errors - Interlocked.Read(ref Run.GarbageSent));
                if (accounted == sent && (sent, ledger, errors).Equals(last) && (DateTime.UtcNow - stableSince).TotalSeconds >= 6) break;
                if (!(sent, ledger, errors).Equals(last)) stableSince = DateTime.UtcNow;
                last = (sent, ledger, errors);
            }

            var failures = new List<string>();
            var duplicated = Ids("SELECT id FROM ledger WHERE effects<>1");
            var invented = Ids("SELECT id FROM ledger WHERE id NOT IN (SELECT id FROM sent)");
            var poisonApplied = Ids("SELECT id FROM ledger WHERE id IN (SELECT id FROM sent WHERE poison=1)");
            var missing = Ids("SELECT id FROM sent WHERE id NOT IN (SELECT id FROM ledger)");
            var garbage = Interlocked.Read(ref Run.GarbageSent);
            if (duplicated.Count > 0) failures.Add($"{duplicated.Count} messages had their effect applied more than once, e.g. {string.Join(", ", duplicated.Take(3))}");
            if (invented.Count > 0) failures.Add($"{invented.Count} ledger rows have no sent message, e.g. {string.Join(", ", invented.Take(3))}");
            if (poisonApplied.Count > 0) failures.Add($"{poisonApplied.Count} poison messages were applied");
            if (Interlocked.Read(ref Run.Corrupt) > 0) failures.Add($"{Run.Corrupt} payloads arrived changed");
            if (errors - garbage != missing.Count)
                failures.Add($"{missing.Count} messages are neither handled nor in the error queue accounting: {errors} error records, {garbage} garbage messages injected, {missing.Count} sent messages without an effect");

            Console.WriteLine();
            Console.WriteLine($"Sent {sent} messages (including follow-ups). Handled {ledger}. Error records {errors} ({garbage} of them garbage injected on purpose).");
            Console.WriteLine($"Handler attempts {Run.Attempts}, restarts {Run.Restarts}, producer failures {Run.ProducerFailures} (aborted batches leave nothing behind).");
            await node.Current.StopAsync();
            node.Current.Dispose();
            if (failures.Count == 0) { Console.WriteLine("PASS: nothing lost, duplicated or invented."); return 0; }
            Console.WriteLine("FAIL:");
            foreach (var f in failures) Console.WriteLine("  - " + f);
            Console.WriteLine("Database kept for inspection: " + Run.DbPath);
            return 1;
        }
    }

    static class Faults
    {
        static readonly HttpClient Http = new HttpClient();

        public static async Task Chaos(string admin, Random rnd, CancellationToken stop)
        {
            if (admin == null) { Console.WriteLine("BQ_RABBITSIM_HTTP is not set: no broker faults will be injected."); return; }
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var settings = rnd.Next(3) == 0 ? "{}" :
                        $"{{\"confirm_delay_ms\":{rnd.Next(0, 200)},\"deliver_delay_ms\":{rnd.Next(0, 100)},\"ack_delay_ms\":{rnd.Next(0, 300)},\"nack_next\":{(rnd.Next(4) == 0 ? rnd.Next(1, 6) : 0)}}}";
                    try { await Http.PostAsync(admin + "/chaos", new StringContent(settings, Encoding.UTF8, "application/json")); } catch { }
                    await Task.Delay(rnd.Next(1000, 8000), stop);
                }
            }
            catch (OperationCanceledException) { }
        }

        public static async Task ClearChaos(string admin)
        {
            if (admin == null) return;
            try { await Http.PostAsync(admin + "/chaos", new StringContent("{}", Encoding.UTF8, "application/json")); } catch { }
        }

        // Random bytes straight into the input queue. They must reach the error queue and not stop the endpoint.
        public static async Task Garbage(Uri uri, string queue, Random rnd, CancellationToken stop)
        {
            try
            {
                var factory = new RabbitMQ.Client.ConnectionFactory { Uri = uri };
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    while (!stop.IsCancellationRequested)
                    {
                        await Task.Delay(rnd.Next(500, 4000), stop);
                        var body = new byte[rnd.Next(0, 2000)];
                        rnd.NextBytes(body);
                        var props = channel.CreateBasicProperties();
                        props.Persistent = true;
                        if (rnd.Next(2) == 0) props.MessageId = Guid.NewGuid().ToString("N");
                        if (rnd.Next(3) == 0) props.ContentType = "application/x-" + rnd.Next(1000);
                        channel.BasicPublish("", queue, false, props, body);
                        Interlocked.Increment(ref Run.GarbageSent);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine("garbage injector stopped: " + ex.Message); }
        }

        public static async Task Restarts(Node node, int seconds, Random rnd, CancellationToken stop)
        {
            try
            {
                var gap = Math.Max(5, Math.Min(45, seconds / 6));
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(rnd.Next(gap / 2 + 1, gap + 1)), stop);
                    await node.Restart(graceful: rnd.Next(3) > 0);
                }
            }
            catch (OperationCanceledException) { }
        }
    }
}
