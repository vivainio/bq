using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
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
        }

        private FastMemberOrm<DbJob> Mapper { get; set; }


        public Task CreateJobAsync(DbJob job)
        {
            throw new NotImplementedException();
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


    }
}