using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Bq.Jobs;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;


namespace Bq
{
    public interface IJobContext
    {
        Envelope Envelope { get; }

        // these are not async because outer layer will commit the transaction
        Task CompleteToCursor(string cursor);
        Task CompleteAsync();
    }

    public interface IJobHandler
    {
        // context contains the message. IJobHandler CAN handle multiple types of messages
        Task HandleJob(IJobContext context);
    }

    // abstraction for the database
    public interface IBqRepository
    {
        // writes the job to database
        Task<DbJob> CreateJobAsync(DbJob job);

        // get some number of db jobs, depends on repository how many
        Task<DbJob> ReadJobAsync(string id);
        Task DeleteJobAsync(string id);
    }

    public interface IBqScheduler
    {
        Task SendAsync(string channel, string id);
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


    public class BqJobServer
    {
        private readonly IBqRepository _repository;
        private readonly ConcurrentDictionary<string, IJobHandler> _handlers = new ConcurrentDictionary<string, IJobHandler>();

        public BqJobServer(IBqRepository repository)
        {
            _repository = repository;
        }

        // yeah I don't know how to get access to static descriptor from T
        public void AddHandler<T>(BqMessageHandler<T> handler) where T : IMessage<T>, new()
        {
            var dummy = new T();
            var name = dummy.Descriptor.FullName;
            _handlers[name] = handler;
        }

        private static string ExtractTypeName(string typeUrl) => typeUrl.Split('/')[1];

        private IJobHandler FindHandler(Envelope envelope)
        {
            var typeUrl = envelope.Msg.TypeUrl;
            var typeName = ExtractTypeName(typeUrl);
            var ok = _handlers.TryGetValue(typeName, out var handler);
            if (!ok)
            {
                throw new BqError("NOHANDLER", $"Bq missing registered handler for {typeName}");
            }

            return handler;
        }

        public async Task DispatchToHandler(Envelope envelope)
        {
            var handler = FindHandler(envelope);
            var ctx = new DbJobContext(envelope, _repository);
            await handler.HandleJob(ctx);
        }

        public DbJob CreateDbJob(Envelope envelope)
        {
            var now = Timestamp.FromDateTime(DateTime.UtcNow);
            var dbJob = new DbJob
            {
                Envelope = envelope.ToByteString(),
                Cursor = "",
                Id = envelope.Id,
                State = JobStatus.Ready,
                LaunchAt = now,
                ExpiresAt = null,
            };
            return dbJob;
        }

        public Envelope CreateEnvelope(IMessage msg)
        {
            var env = new Envelope
            {
                Msg = Any.Pack(msg),
            };
            return env;
        }

        public async Task<DbJob> SendAsync(IMessage msg, string channel)
        {
            var env = CreateEnvelope(msg);
            var dbJob = CreateDbJob(env);
            dbJob.Channel = channel;
            return await _repository.CreateJobAsync(dbJob);
        }

        public static Envelope CreateEnvelopeFromDbJob(DbJob job)
        {
            var env = Envelope.Parser.ParseFrom(job.Envelope);
            env.Id = job.Id;
            env.Cursor = job.Cursor ?? "";
            return env;
        }

        public async Task ReadAndDispatchJob(string id)
        {
            var job = await _repository.ReadJobAsync(id);
            var env = CreateEnvelopeFromDbJob(job);
            await DispatchToHandler(env);
        }
    }
}