using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Bq.Jobs;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Polly;

namespace Bq
{
    public class BqStats
    {
        public long Handled { get; set; }
    }
    public class BqJobServer : IDispatchJobs
    {
        private readonly IBqRepository _repository;

        public BqStats Stats { get; } = new BqStats();

        private readonly ConcurrentDictionary<string, IJobHandler> _handlers =
            new ConcurrentDictionary<string, IJobHandler>();

        
        public BqJobServer(IBqRepository repository)
        {
            _repository = repository;
        }

        public AsyncPolicy ResiliencePolicy { get; set; } = Policy.NoOpAsync();

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

        public async Task<DbJob> SendJobAsync(string channel, IMessage msg)
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

        private void Assert(bool value, string error)
        {
            if (!value)
            {
                throw new BqError("ASSERT", error);
            }

        }

        public static void LogError(Exception ex)
        {
            Console.WriteLine("BqJobServer crash:");
            Console.WriteLine(ex);

        }

        public static void LogWarning(string warning)
        {
            Console.WriteLine($"BqJobServer WARN: {warning}");
        }


        // this is the main dispatch entry point. Does the transactions etc
        // will NOT raise exceptions but "handle" them according to
        // configured behavior 
        public async Task ReadAndDispatchJob(string id)
        {
            var job = await _repository.ReadJobAsync(id);
            // These two shouldn't happen if redis works correctly
            if (job == null)
            {
                LogWarning($"JOB_GONE Job {id} was completed when Bq tried to load it for running");
                return;
            }
            Assert(job.State == JobStatus.Ready, $"Job {id} should be READY, is {job.State}");
            var env = CreateEnvelopeFromDbJob(job);
            await _repository.SetJobStatusAsync(id, JobStatus.Pending);
            using var tx = BqUtil.Tx();
            try
            {
                
                var t = ResiliencePolicy.ExecuteAsync(() => DispatchToHandler(env));
                await t;
                tx.Complete();
                Stats.Handled++;


            }
            catch (Exception ex)
            {
                await _repository.SetJobStatusAsync(id, JobStatus.Failed);
                LogError(ex);
            }
        }
    }
}