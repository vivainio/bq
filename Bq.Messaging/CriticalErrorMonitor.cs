using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    sealed class CriticalErrorMonitor
    {
        readonly Func<string, Exception, Task> callback;
        readonly TimeSpan threshold;
        readonly TimeSpan interval;
        readonly CancellationToken cancellation;

        public CriticalErrorMonitor(Func<string, Exception, Task> callback, TimeSpan threshold, CancellationToken cancellation)
        {
            this.callback = callback;
            this.threshold = threshold;
            this.cancellation = cancellation;
            interval = TimeSpan.FromMilliseconds(Math.Max(1, Math.Min(1000, threshold.TotalMilliseconds / 2)));
        }

        public Probe Outbox { get; } = new Probe("Outbox dispatcher");
        public Probe Recovery { get; } = new Probe("Recovery dispatcher");

        public async Task Run(Func<bool> brokerOpen)
        {
            var broker = new Probe("RabbitMQ connection");
            var probes = new[] { broker, Outbox, Recovery };
            while (!cancellation.IsCancellationRequested)
            {
                if (brokerOpen()) broker.Succeeded();
                else broker.Failed(new InvalidOperationException("The RabbitMQ connection is closed."));

                foreach (var probe in probes)
                {
                    var error = probe.Check(threshold);
                    if (error == null || cancellation.IsCancellationRequested) continue;
                    try { await callback(probe.Name, error).ConfigureAwait(false); }
                    catch (Exception ex) { Trace.TraceError("Critical error callback failed: {0}", ex); }
                    return;
                }

                try { await Task.Delay(interval, cancellation).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }

        internal sealed class Probe
        {
            readonly object gate = new object();
            Stopwatch unhealthyFor;
            Exception failure;

            public Probe(string name) { Name = name; }
            public string Name { get; }

            public void Started()
            {
                lock (gate) if (unhealthyFor == null) unhealthyFor = Stopwatch.StartNew();
            }

            public void Succeeded()
            {
                lock (gate)
                {
                    unhealthyFor = null;
                    failure = null;
                }
            }

            public void Failed(Exception error)
            {
                lock (gate)
                {
                    if (unhealthyFor == null) unhealthyFor = Stopwatch.StartNew();
                    failure = error;
                }
            }

            public Exception Check(TimeSpan threshold)
            {
                lock (gate)
                {
                    if (unhealthyFor == null || unhealthyFor.Elapsed < threshold) return null;
                    return failure ?? new TimeoutException(Name + " did not complete within the critical error threshold.");
                }
            }
        }
    }
}
