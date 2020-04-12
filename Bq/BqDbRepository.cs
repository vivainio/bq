using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Security;
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


        public BqDbRepository(DbConnection connection)
        {
            ConnectionFactory = () => connection;
        }

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
            var cmd = conn.CreateCommand();
            var ins = InsertionSql.Replace(":ID", "BQ_SEQ_ID.NEXTVAL");
            cmd.CommandText = ins;
            Mapper.AddParamsToCommand(cmd, job);
            // removing ID because it's coming from sequence now
            cmd.Parameters.RemoveAt("ID");
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
            using var tx = BqUtil.TxNew();
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

        public async Task CompleteToCursorAsync(string id, string cursor, int delaySec)
        {
            var query = $"update {TABLE_NAME} set CUR = :cursor, STATE = {(int)JobStatus.Ready}, " +
                        $"LAUNCHAT = :launchat where ID = :id";
            Console.WriteLine($"Cur {id} = '{cursor}'");
            using var conn = ConnectionFactory();
            var cmd = conn.SqlCommand(query);
            cmd.AddParameter("cursor", DbType.String, cursor);
            cmd.AddParameter("launchat", DbType.DateTime, DateTime.UtcNow.AddSeconds(delaySec));
            cmd.AddParameter("id", DbType.String, id);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<IReadOnlyList<DbJob>> ReadAvailableWork()
        {
            using var conn = ConnectionFactory();
            const int state = (int) Jobs.JobStatus.Ready;
            var query = $"select ID, CHANNEL from {TABLE_NAME} where STATE = {state} " +
                        $"order by LAUNCHAT asc fetch first 200 rows only";
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
            
            conn.ExecuteSqlIgnoreError("drop table BQ_JOBS");
            conn.ExecuteSqlIgnoreError("drop sequence BQ_SEQ_ID");
        }
        public void CreateTables()
        {
            using var conn = ConnectionFactory();
            conn.ExecuteSql("create sequence BQ_SEQ_ID");
            var sql = DbExtensions.CreationSql<DbJob>();
            conn.ExecuteSql("create table BQ_JOBS " + sql);
            conn.ExecuteSql("create index BQ_IDX_STATE_DATA on BQ_JOBS(STATE, LAUNCHAT)" );
            conn.ExecuteSql("create index BQ_IDX_ID on BQ_JOBS(ID)" );

        }
    }
}