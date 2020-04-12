using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using Bq.Jobs;
using Microsoft.Extensions.Configuration;
using NFluent;
using Oracle.ManagedDataAccess.Client;
using Polly;
using TrivialTestRunner;
using static System.Console;
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


        async Task<BqRedisScheduler> CreateScheduler()
        {
            var repo = new BqDbRepository(OracleConnectionFactory);
            var scheduler = new BqRedisScheduler(repo);
            await scheduler.ConnectAsync();
            return scheduler;
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
            var pingHandler = new PingHandler();
            worker.AddHandler(pingHandler);

            return (worker, scheduler);
        }
        BqJobServer CreateAndConnectWorker(string channelName, BqRedisScheduler scheduler)
        {
            var repo = new BqDbRepository(OracleConnectionFactory);
            var worker = new BqJobServer(repo);
            worker.ResiliencePolicy = Policy.Handle<Exception>().RetryAsync(2);
            var pingHandler = new PingHandler();
            worker.AddHandler(pingHandler);
            scheduler.StartListening(channelName, worker);
            return worker;
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
            var scheduler = await CreateScheduler();
            scheduler.Clear("main");

            var workers = Enumerable.Range(0, 100).Select(i =>
                CreateAndConnectWorker("main", scheduler)).ToArray();

            // use random worker for send?
            var sendWorker = workers[0];
            async Task WorkAsLeader()
            {
                while (true)
                {
                    await Task.Delay(10);
                    await scheduler.ReadAndSendWork();
                }
            }


            // send out tasks
            async Task WorkAsProducer(string channel, string name)
            {
                int counter = 0;
                while (true)
                {
                    
                    await Task.Delay(1);
                    counter++;
                    await sendWorker.SendJobAsync(channel, new DemoMessagePing
                    {
                        Message = $"{name}:{counter}"
                    });
                }
            }

            async Task WorkAsStatsDumper()
            {
                while (true)
                {
                    await Task.Delay(100);
                    var stats = string.Join(", ", workers.Select(w => w.Stats.Handled));
                    
                    WriteLine($"*********** {stats}");
                }
                
            }
            
            var t1 = Task.Run(WorkAsLeader);
            var p1 = Task.Run(() => WorkAsProducer("main", "a"));
            var p2 = Task.Run(() => WorkAsProducer("main", "b"));
            var d = Task.Run(WorkAsStatsDumper);
            await t1;
        }
        
    }
}
