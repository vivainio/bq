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

        public Task CompleteToCursor(string cursor)
        {
            throw new NotImplementedException();
        }

        public async Task CompleteAsync()
        {
            await _repo.DeleteJobAsync(Envelope.Id);
        }
    }
}