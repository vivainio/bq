using System;
using System.Threading.Tasks;
using Bq.Jobs;

namespace Bq
{
    public class DbJobContext : IJobContext
    {
        private readonly IBqRepository _repo;

        public DbJobContext(Envelope envelope, IBqRepository repo)
        {
            _repo = repo;
            Envelope = envelope;
        }

        public Envelope Envelope { get; }

        public async Task CompleteToCursorAsync(string cursor, int delaySec)
        {

            await _repo.CompleteToCursorAsync(Envelope.Id, cursor, delaySec);
        }

        public async Task CompleteAsync()
        {
            await _repo.DeleteJobAsync(Envelope.Id);
        }
    }
}