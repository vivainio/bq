using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Drawing.Drawing2D;
using System.Reflection;
using System.Threading.Tasks;
using NFluent;
using TrivialTestRunner;

namespace Bq.Tests.Integration
{
    class BqIntegrationTests
    {
        [fCase]
        public static async Task TestRedisScheduler()
        {
            var rs = new BqRedisScheduler("main");
            await rs.ConnectAsync();
            var got = new List<string>();
            rs.StartListening(async id =>
            {
                got.Add(id);
                await Task.CompletedTask;
            });
            rs.Send("11");
            rs.Send("12");
            await Task.Delay(1000);
            Check.That(got).HasSize(2);

        }
    }
}