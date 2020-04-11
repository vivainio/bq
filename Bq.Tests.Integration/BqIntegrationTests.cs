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
            Console.WriteLine("handling job " + context.Envelope.Id);
            if (message.Message == "error")
            {
                throw new Exception("Wanted to fail so here we go");
            } 
            await context.CompleteAsync();
        }
    }

    class BqIntegrationTests
    {
        private Fixture _fixture;

        
        public BqIntegrationTests()
        {
            _fixture = new Fixture();
            var config = new ConfigurationBuilder()
                .AddUserSecrets<BqIntegrationTests>();

            Config = config.Build();
        }

        public IConfigurationRoot Config { get; set; }

        [Case]
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
            await rs.SendAsync("main", "11");
            await rs.SendAsync("main", "12");
            await Task.Delay(1000);
            Check.That(got).HasSize(2);
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
        [fCase]
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

            await worker.SendAsync("main", ping);
            // crashing message
            await worker.SendAsync("main", new DemoMessagePing
            {
                Message = "error"
            });
            var available = await repo.ReadAvailableWork();
            var scheduler = new BqRedisScheduler("main");
            await scheduler.ConnectAsync();
            scheduler.Clear("main");
            var received = new List<string>();
            scheduler.StartListening(  (async m =>
            {
                received.Add(m);
                await worker.ReadAndDispatchJob(m);
            }));
            foreach (var job in available)
            {
                await scheduler.SendAsync(job.Channel, job.Id);
            }
            await Task.Delay(10000);
            Check.That(received).HasSize(2);
        }
    }
}