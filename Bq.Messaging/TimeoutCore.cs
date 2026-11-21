using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging.Timeout.Core
{
    public class TimeoutData
    {
        public string Id { get; set; }
        public string Destination { get; set; }
        public Guid SagaId { get; set; }
        public byte[] State { get; set; }
        public DateTime Time { get; set; }
        public string OwningTimeoutManager { get; set; }
        public Dictionary<string, string> Headers { get; set; }

        public override string ToString() => "Timeout " + Id + " due " + Time.ToString("O");
    }

    public struct Timeout
    {
        public Timeout(string id, DateTime dueTime)
        {
            Id = id;
            DueTime = dueTime;
        }

        public string Id { get; }
        public DateTime DueTime { get; }
    }

    public class TimeoutsChunk
    {
        public TimeoutsChunk(Timeout[] dueTimeouts, DateTime nextTimeToQuery)
        {
            DueTimeouts = dueTimeouts;
            NextTimeToQuery = nextTimeToQuery;
        }

        public Timeout[] DueTimeouts { get; }
        public DateTime NextTimeToQuery { get; }
    }

    /// <summary>NServiceBus's timeout store contract. Bq keeps timeouts in its own persistence provider and does not call these.</summary>
    public interface IPersistTimeouts
    {
        Task Add(TimeoutData timeout, ContextBag context);
        Task<bool> TryRemove(string timeoutId, ContextBag context);
        Task<TimeoutData> Peek(string timeoutId, ContextBag context);
        Task RemoveTimeoutBy(Guid sagaId, ContextBag context);
    }

    public interface IQueryTimeouts
    {
        Task<TimeoutsChunk> GetNextChunk(DateTime startSlice);
    }
}
