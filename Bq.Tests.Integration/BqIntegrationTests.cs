using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data;
using System.Data.Common;
using System.Drawing.Drawing2D;
using System.Linq;
using System.Reflection;
using System.Security.Policy;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Bq.Jobs;
using Microsoft.Extensions.Configuration;
using NFluent;
using Oracle.ManagedDataAccess.Client;
using Polly;
using TrivialTestRunner;

namespace Bq.Tests.Integration
{
    
    public class PingHandler: BqMessageHandler<DemoMessagePing>
    {
        protected override async Task HandleMessage(IJobContext context, DemoMessagePing message)
        {
            Console.WriteLine($"handling job {context.Envelope.Id} = {message.Message}");
            await Task.Delay(100);
            if (message.Message == "error")
            {
                throw new Exception("Wanted to fail so here we go");
            } 
            await context.CompleteAsync();
        }
    }

    class BqIntegrationTests
    {
        private readonly Fixture _fixture;

        
        public BqIntegrationTests()
        {
            _fixture = new Fixture();
            var config = new ConfigurationBuilder()
                .AddUserSecrets<BqIntegrationTests>();

            Config = config.Build();
        }

        public IConfigurationRoot Config { get; set; }

        [Case]
        public async Task TestRedisScheduler()
        {
            
            var repo = new BqDbRepository(OracleConnectionFactory);
            var rs = new BqRedisScheduler(repo);
            await rs.ConnectAsync();
            var got = new List<string>();
            var server = new DummyJobServer();
            rs.StartListening("main", server);
            await rs.NotifyJobAvailableToListeners("main", "11");
            await rs.NotifyJobAvailableToListeners("main", "12");
            await Task.Delay(1000);
            Check.That(server.Handled).HasSize(2);
        }

        public DbConnection OracleConnectionFactory()
        {
            
            string connstring = Config.GetConnectionString("Oracle");
            var con = new OracleConnection(connstring);
            con.Open();
            return con;
        }
        [Case]
        public async Task TestDb()
        {
            
            var repo = new BqDbRepository(OracleConnectionFactory);
            await repo.ReadJobAsync("12");

        }

        [Case]
        public static void TestCreationSql()
        {
            var sql = DbExtensions.CreationSql<DbJob>();
            Check.That(sql).Contains("ENVELOPE", "BLOB", "EXPIRESAT", "TIMESTAMP");
        }

        [Case]
        public void CreateTable()
        {
            var repo = new BqDbRepository(OracleConnectionFactory);
            repo.DangerouslyDropTable();
            repo.CreateTable();
            
        }
        [Case]
        public void TestInsertionSql()
        {
            var orm = new FastMemberOrm<DbJob>().OmitProperties("Envelope");

            var sql = DbExtensions.InsertionSql("BQ_JOBS", 
                orm.Props.Select(p => p.Name.ToUpperInvariant()).ToArray());
            Check.That(sql).Contains("values");
        }

        [Case]
        public async Task TestSend()
        {
            var repo = new BqDbRepository(OracleConnectionFactory);
            var job = _fixture.Create<DbJob>();
            await repo.CreateJobAsync(job);
        }

        private void Setup()
        {
            var repo = new BqDbRepository(OracleConnectionFactory);
            repo.DangerouslyDropTable();
            repo.CreateTable();
            
        }

        async Task<(BqJobServer worker, BqRedisScheduler scheduler)> CreateWorker(string channelName)
        {
            var repo = new BqDbRepository(OracleConnectionFactory);
            var worker = new BqJobServer(repo);
            worker.ResiliencePolicy = Policy.Handle<Exception>().RetryAsync(2);
            var scheduler = new BqRedisScheduler(repo);
            await scheduler.ConnectAsync();
            scheduler.Clear(channelName);
            scheduler.StartListening(channelName, worker);
            return (worker, scheduler);
        }
        
        [Case]
        public async Task TestSendToRepository()
        {
            Setup();
            var repo = new BqDbRepository(OracleConnectionFactory);
            var worker = new BqJobServer(repo);
            var ping = new DemoMessagePing
            {
                Message = "ping"
            };
            
            var pingHandler = new PingHandler();

            worker.ResiliencePolicy = Policy.Handle<Exception>().RetryAsync(2);
            worker.AddHandler(pingHandler);

            await worker.SendJobAsync("main", ping);
            // crashing message
            await worker.SendJobAsync("main", new DemoMessagePing
            {
                Message = "error"
            });
            var available = await repo.ReadAvailableWork();
            var scheduler = new BqRedisScheduler(repo);
            await scheduler.ConnectAsync();
            scheduler.Clear("main");
            scheduler.StartListening("main", worker);
            foreach (var job in available)
            {
                await scheduler.NotifyJobAvailableToListeners(job.Channel, job.Id);
            }
            await Task.Delay(5000);
        }
        [fCase]
        public async Task TestRoundTrip()
        {
            Setup();
            var (worker, scheduler) = await CreateWorker("main");
            scheduler.Clear("main");
            var pingHandler = new PingHandler();
            worker.AddHandler(pingHandler);
            worker.ResiliencePolicy = Policy.BulkheadAsync(2);
            
            var ping = new DemoMessagePing
            {
                Message = "ping"
            };

            async Task WorkAsLeader()
            {
                while (true)
                {
                    await Task.Delay(10);
                    await scheduler.ReadAndSendWork();
                }
            }


            async Task WorkAsProducer(string channel, string name)
            {
                int counter = 0;
                while (true)
                {
                    
                    await Task.Delay(1);
                    counter++;
                    await worker.SendJobAsync(channel, new DemoMessagePing
                    {
                        Message = $"{name}:{counter}"
                    });
                }
            }
            
            
            var t1 = Task.Run(WorkAsLeader);
            var prod = Task.Run(() => WorkAsProducer("main", "a"));
            await t1;
        }
        
    }
}
