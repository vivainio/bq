using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data;
using System.Data.Common;
using System.Drawing.Drawing2D;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AutoFixture;
using Bq.Jobs;
using Microsoft.Extensions.Configuration;
using NFluent;
using Oracle.ManagedDataAccess.Client;
using TrivialTestRunner;

namespace Bq.Tests.Integration
{
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
            rs.Send("11");
            rs.Send("12");
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
        public async Task TestInsertionSql()
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
    }
    
    
}