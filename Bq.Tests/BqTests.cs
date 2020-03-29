using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using TrivialTestRunner;

using Bq.Jobs;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Configuration;

namespace Bq.Tests
{
    public class PingHandler: BqMessageHandler<DemoMessagePing>
    {
        protected override Task HandleMessage(IJobContext context, DemoMessagePing message)
        {
            Console.WriteLine("handling job " + context.Envelope.Token);
            context.Complete();
            return Task.CompletedTask;
        }
    }
    public class BqTests
    {
        class FakeRepository : IBqRepository
        {
            public Task CreateJob(DbJob job)
            {
                throw new NotImplementedException();
            }

            public Task<DbJob> ReadJobs(IReadOnlyList<string> tokens)
            {
                throw new NotImplementedException();
            }
        }
        [Case]
        public static void CreateWorker()
        {
            var repo = new FakeRepository();
            var hub = new BqJobServer(repo);
            var handler = new PingHandler();
            
            hub.AddHandler(handler);
            var pingMessage = new DemoMessagePing
            {
                Message = "hello"
            };

            var env = new Envelope
            {
                Msg = Any.Pack(pingMessage)
            };

            var dbJob = hub.CreateDbJob(env);
            hub.DispatchToHandler(env);

        }

        [fCase]
        public static void TestRedis()
        {
            var config = new ConfigurationBuilder();
            config.AddUserSecrets(Assembly.GetExecutingAssembly());
            var croot = config.Build();
            var sect = croot.GetSection("AppSettings:RedisAuth");
            var rs = new BqRedisScheduler("main");
            var t =  rs.Connect();
            t.Wait();
        }
    }
}