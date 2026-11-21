using System;
using System.Collections.Generic;
using System.Linq;

namespace Bq.Messaging.Tests
{
    /// <summary>
    /// The shared Assert behind the older test groups. The console runner stops at the first failed check, as it always has.
    /// The TUnit runner records every failed check and reports them together once the group ends, so one failure does not
    /// hide the checks after it.
    /// </summary>
    static class SoftChecks
    {
        static readonly object Gate = new object();
        static readonly List<string> Failures = new List<string>();

        public static void Assert(bool value, string label)
        {
            if (value) return;
#if TUNIT
            lock (Gate) Failures.Add(label);
#else
            throw new Exception("Failed: " + label);
#endif
        }

        public static void Begin() { lock (Gate) Failures.Clear(); }

        public static void Finish(Exception thrown)
        {
            string[] failed;
            lock (Gate) { failed = Failures.ToArray(); Failures.Clear(); }
            if (failed.Length == 0 && thrown == null) return;
            var text = failed.Length == 0 ? "" : failed.Length + " failed check(s):" + Environment.NewLine + string.Join(Environment.NewLine, failed.Select(f => "  - " + f));
            if (thrown != null) text += (text.Length == 0 ? "" : Environment.NewLine) + "Then the group stopped with: " + thrown;
            throw new Exception(text);
        }
    }
}
