using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Bq.Messaging.Tests.TUnitRunner
{
    /// <summary>
    /// Supplies the broker for the broker tests. A configured BQ_RABBITMQ_URI is used as is. Otherwise, if rabbit-sim is on
    /// PATH, one is started for this test run and stopped when the process exits. Set BQ_NO_SIM=1 to run without a broker.
    /// </summary>
    static class SimBroker
    {
        static readonly object Gate = new object();
        static bool resolved;
        static Uri uri;

        public static Uri Uri()
        {
            lock (Gate)
            {
                if (resolved) return uri;
                resolved = true;
                var configured = Environment.GetEnvironmentVariable("BQ_RABBITMQ_URI");
                if (!string.IsNullOrEmpty(configured)) return uri = new Uri(configured);
                if (Environment.GetEnvironmentVariable("BQ_NO_SIM") == "1") return null;
                uri = Start();
                return uri;
            }
        }

        static int FreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
            finally { listener.Stop(); }
        }

        static Uri Start()
        {
            var executable = FindOnPath("rabbit-sim");
            if (executable == null) return null;
            var port = FreePort();
            var httpPort = FreePort();
            var data = Path.Combine(Path.GetTempPath(), "bq-sim-" + Guid.NewGuid().ToString("N") + ".db");
            // Emulate RabbitMQ 4's stricter checks when this rabbit-sim can.
            var strict = SupportsRabbitMqOption(executable) ? " --rabbitmq 4" : "";
            var info = new ProcessStartInfo(executable, "--bind 127.0.0.1 --port " + port + " --http-port " + httpPort + strict + " --data \"" + data + "\"")
            {
                UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true
            };
            var process = Process.Start(info);
            // Drain the output so the broker never blocks on a full pipe.
            process.OutputDataReceived += (sender, args) => { };
            process.ErrorDataReceived += (sender, args) => { };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            AppDomain.CurrentDomain.ProcessExit += (sender, args) =>
            {
                try { if (!process.HasExited) process.Kill(); } catch (Exception) { }
                try { File.Delete(data); } catch (Exception) { }
            };
            for (var attempt = 0; attempt < 100; attempt++)
            {
                if (process.HasExited) throw new InvalidOperationException("rabbit-sim exited with code " + process.ExitCode + " during start-up.");
                try
                {
                    using (var client = new TcpClient()) { client.Connect(IPAddress.Loopback, port); }
                    Environment.SetEnvironmentVariable("BQ_RABBITSIM_HTTP", "http://127.0.0.1:" + httpPort);
                    var started = new Uri("amqp://guest:guest@127.0.0.1:" + port + "/");
                    Environment.SetEnvironmentVariable("BQ_RABBITMQ_URI", started.ToString());
                    return started;
                }
                catch (SocketException) { Thread.Sleep(100); }
            }
            throw new InvalidOperationException("rabbit-sim did not listen on port " + port + ".");
        }

        static bool SupportsRabbitMqOption(string executable)
        {
            try
            {
                var info = new ProcessStartInfo(executable, "--help") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true };
                using (var help = Process.Start(info))
                {
                    var text = help.StandardError.ReadToEnd() + help.StandardOutput.ReadToEnd();
                    help.WaitForExit(5000);
                    return text.Contains("--rabbitmq");
                }
            }
            catch (Exception) { return false; }
        }

        static string FindOnPath(string name)
        {
            foreach (var directory in (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator))
            {
                if (string.IsNullOrWhiteSpace(directory)) continue;
                var candidate = Path.Combine(directory, name);
                if (File.Exists(candidate)) return candidate;
            }
            return null;
        }
    }
}
