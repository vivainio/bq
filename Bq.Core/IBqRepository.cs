using System.Collections.Generic;
using System.Threading.Tasks;
using Bq.Jobs;

namespace Bq
{
    public interface IBqRepository
    {
        // writes the job to database
        Task<DbJob> CreateJobAsync(DbJob job);

        // get some number of db jobs, depends on repository how many
        Task<DbJob> ReadJobAsync(string id);
        Task SetJobStatusAsync(string id, JobStatus status);
        Task DeleteJobAsync(string id);
        Task CompleteToCursorAsync(string id, string cursor, int delaySec);
        Task<IReadOnlyList<DbJob>> ReadAvailableWork();

    }
}