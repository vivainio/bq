using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Bq.Jobs;
using Google.Protobuf;
using Google.Protobuf.Reflection;


namespace Bq
{
    public interface IJobContext
    {
        Envelope Envelope { get; }
        // these are not async because outer layer will commit the transaction
        void CompleteToCursor(string cursor);
        void Complete();
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
        Task CreateJob(DbJob job);
        // get some number of db jobs, depends on repository how many
        Task<DbJob> ReadJobs(IReadOnlyList<string> tokens);
    }
    
    public class BqError : Exception
    {
        public readonly string Code;
        public BqError(string code, string msg) : base(msg)
        {
            Code = code;
        }
        
    }
    public abstract class BqMessageHandler<T> : IJobHandler where T: IMessage<T>, new()
    {

        protected abstract Task HandleMessage(IJobContext context, T message);
        
        public async Task HandleJob(IJobContext context)
        {
            var unpacked = context.Envelope.Msg.Unpack<T>();
            await HandleMessage(context, unpacked);
        }
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class BqJobHandlerAttribute: Attribute
    {
        public string Descriptor { get; }

        public BqJobHandlerAttribute(string descriptor)
        {
            Descriptor = descriptor;
        }
    }

    public class DbJobContext : IJobContext
    {
        public DbJobContext(Envelope envelope)
        {
            Envelope = envelope;
        }

        public Envelope Envelope { get; }
        public void CompleteToCursor(string cursor)
        {
            throw new NotImplementedException();
        }

        public void Complete()
        {
            throw new NotImplementedException();
        }
    }
    
    
    public class BqJobServer
    {
        private readonly IBqRepository _repository;
        private ConcurrentDictionary<string, IJobHandler> _handlers = new ConcurrentDictionary<string, IJobHandler>();

        public BqJobServer(IBqRepository repository)
        {
            _repository = repository;
        }
        // yeah I don't know how to get access to static descriptor from T
        public void AddHandler<T>(BqMessageHandler<T> handler) where T: IMessage<T>, new()
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
            var ctx = new DbJobContext(envelope);
            await handler.HandleJob(ctx);
        }

        public DbJob CreateDbJob(Envelope envelope)
        {
            var dbJob = new DbJob
            {
                Envelope = envelope.ToByteString(),
                Cursor = "",
            };
            return dbJob;

        }
        
    }
}