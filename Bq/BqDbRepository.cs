using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Bq.Jobs;
using Google.Protobuf;


/* Relevant SQL
 CREATE TABLE BQ_JOBS (
	ID VARCHAR2(40),
	CHANNEL varchar2(40),
	ENVELOPE blob
);
 */


namespace Bq
{
    public class BqDbRepository: IBqRepository
    {
        private const string TABLE_NAME = "BQ_JOBS";
        private readonly Func<DbConnection> ConnectionFactory;

        public BqDbRepository(Func<DbConnection> connectionFactory)
        {
            ConnectionFactory = connectionFactory;
            Mapper = new FastMemberOrm<DbJob>()
                .OmitProperties("Envelope")
                .UseFallbackWhenReading((prop, value, tgt) =>
                {
                    if (prop.ToLowerInvariant() == "envelope")
                    {
                        if (value != null)
                        {
                            tgt.Envelope = ByteString.CopyFrom(value as byte[]);
                        }
                        
                    }
                });
            InsertionSql = DbExtensions.InsertionSql(TABLE_NAME, 
                Mapper.Props.Select(p => p.Name.ToUpperInvariant()).Concat(new[] {"ENVELOPE" }).ToArray());

        }

        private string InsertionSql { get; set; }
        
        private FastMemberOrm<DbJob> Mapper { get; set; }


        public async Task<DbJob> CreateJobAsync(DbJob job)
        {
            using var conn = ConnectionFactory();
            job.Id = Guid.NewGuid().ToString("N");
            var cmd = conn.CreateCommand();
            cmd.CommandText = InsertionSql;
            Mapper.AddParamsToCommand(cmd, job);
            var blobParam = cmd.CreateParameter();
            blobParam.ParameterName = "ENVELOPE";
            blobParam.DbType = DbType.Binary;
            blobParam.Value = job.Envelope.ToByteArray();
            cmd.Parameters.Add(blobParam);
            await cmd.ExecuteNonQueryAsync();
            return job;

        }

        public async Task<DbJob> ReadJobAsync(string id)
        {
            using var conn = ConnectionFactory();
            var query = $"select * from {TABLE_NAME} where id = '{id}'";
            var rd = conn.ExecuteReader(query);
            var got = await rd.ReadAsync();
            if (!got)
            {
                return null;
            }
            var dbJob = new DbJob();
            Mapper.CopyReaderRowToObject(rd, dbJob);
            return dbJob;
        }

        // immediately set job status in own new transaction
        public async Task SetJobStatusAsync(string id, JobStatus status)
        {
            using var conn = ConnectionFactory();
            var query = $"update {TABLE_NAME} set STATE = :state where id = :id";
            var cmd  = conn.SqlCommand(query);
            cmd.AddParameter("state", DbType.Int32, (int) status);
            cmd.AddParameter("id", DbType.String, id);
            using var tx = new TransactionScope(TransactionScopeOption.RequiresNew);
            await cmd.ExecuteNonQueryAsync();
            tx.Complete();
        }

        public async Task DeleteJobAsync(string id)
        {
            var query = $"delete from {TABLE_NAME} where id = :id";
            using var conn = ConnectionFactory();
            var cmd = conn.SqlCommand(query);
            cmd.AddParameter("id", DbType.String, id);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<IReadOnlyList<DbJob>> ReadAvailableWork()
        {
            using var conn = ConnectionFactory();
            const int state = (int) Jobs.JobStatus.Ready;
            var query = $"select ID, CHANNEL from {TABLE_NAME} where STATE = {state}";
            var rd = conn.ExecuteReader(query);
            var res = new List<DbJob>();
            while (await rd.ReadAsync() )
            {
                var j = new DbJob();
                Mapper.CopyReaderRowToObject(rd, j);
                res.Add(j);
            }

            return res;
        }

        public void DangerouslyDropTable()
        {
            using var conn = ConnectionFactory();
            conn.ExecuteSql("drop table BQ_JOBS");

        }
        public void CreateTable()
        {
            var sql = DbExtensions.CreationSql<DbJob>();
            var full = "create table BQ_JOBS " + sql;
            using var conn = ConnectionFactory();
            
            conn.ExecuteSql(full);
        }
    }
}