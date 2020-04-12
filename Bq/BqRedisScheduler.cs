using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using static System.Console;
namespace Bq
{

    // cheapo alternative to Streams
    // create singleton instance of this
    public class BqRedisScheduler : IBqScheduler
    {
        private readonly IBqRepository _repository;
        private const string WORKLIST_NAME = "bq_worklist";
        private const string SUB_CHANNEL_NAME = "bq_chan";
        private const string FLOW_CONTROL_PUBSUB = "bq_flow";
        private ConnectionMultiplexer _redis;

        private ConcurrentDictionary<string, object> _recentlySent = new ConcurrentDictionary<string, object>();
        
        public BqRedisScheduler(IBqRepository repository)
        {
            _repository = repository;
        }
        
        private IDatabase Db() => _redis.GetDatabase();

        private string SubChan(string channelName) => $"{SUB_CHANNEL_NAME}/{channelName}";
        private string WorkList(string channelName) => $"{WORKLIST_NAME}/{channelName}";

        public static async Task<ConnectionMultiplexer> DefaultRedis()
        {
            return await ConnectionMultiplexer.ConnectAsync("localhost:17005");
            
        }
        public async Task ConnectAsync()
        {
            _redis = await DefaultRedis();
        }

        private RedisPubSubQueue CreateQueue(string channelName) =>
            new RedisPubSubQueue(_redis, SubChan(channelName), 
                WorkList(channelName));

        // same scheduler can observe many channels
        public void StartListening(string channelName, IDispatchJobs server)
        {
            var q = CreateQueue(channelName);
            q.StartListening(msg => server.ReadAndDispatchJob(msg));
        }

        // it's totally safe to clear redis. stuff will be reissued from db
        public void Clear(string channel)
        {
            var db = Db();
            db.ListTrim(WorkList(channel), 0, 0);
        }

        private static int SubCounter = 0;
        public async Task NotifyJobAvailableToListeners(string channel, string id)
        {
            var db = _redis.GetDatabase();
            await db.ListLeftPushAsync(WorkList(channel), id);
            SubCounter++;
            WriteLine($"Iss {id}");
            await db.PublishAsync(SubChan(channel), $"r{SubCounter}", CommandFlags.FireAndForget);
        }
        
        // should be repeatedly executed as session leader only
        public async Task ReadAndSendWork()
        {
            var jobs = await _repository.ReadAvailableWork();
            foreach (var job in jobs)
            {
                if (_recentlySent.ContainsKey(job.Id))
                {
                    continue;
                }
                await NotifyJobAvailableToListeners(job.Channel, job.Id);
                _recentlySent[job.Id] = true;
            }
        }
    }
}
