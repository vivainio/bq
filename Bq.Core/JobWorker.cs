using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Transactions;
using Bq.Jobs;
using Google.Protobuf;


namespace Bq
{
    public interface IJobContext
    {
        Envelope Envelope { get; }

        // these are not async because outer layer will commit the transaction
        Task CompleteToCursorAsync(string cursor, int delaySec);
        Task CompleteAsync();
    }

    public interface IJobHandler
    {
        // context contains the message. IJobHandler CAN handle multiple types of messages
        Task HandleJob(IJobContext context);
    }

    // abstraction for the database

    public interface IBqScheduler
    {
        Task NotifyJobAvailableToListeners(string channel, string id);
        Task NotifyJobAvailableToLeader();
    }

    public class BqError : Exception
    {
        public readonly string Code;

        public BqError(string code, string msg) : base(msg)
        {
            Code = code;
        }
    }

    public abstract class BqMessageHandler<T> : IJobHandler where T : IMessage<T>, new()
    {
        protected abstract Task HandleMessage(IJobContext context, T message);

        public async Task HandleJob(IJobContext context)
        {
            var unpacked = context.Envelope.Msg.Unpack<T>();
            await HandleMessage(context, unpacked);
        }
    }

    // redis scheduler calls this
    public interface IDispatchJobs
    {
        Task ReadAndDispatchJob(string id);
    }

    // use e.g. in tests
    public class DummyJobServer : IDispatchJobs
    {
        public readonly ConcurrentBag<string> Handled = new ConcurrentBag<string>();
        public Task ReadAndDispatchJob(string id)
        {
            Handled.Add(id);
            return Task.CompletedTask;
        }
    }
}