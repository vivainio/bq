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
        private const string KEY_WORKLIST_NAME = "bq_worklist";
        private const string KEY_SUB_CHANNEL_NAME = "bq_chan";
        public const string KEY_LEADER_SUB = "bq_leader";

        private ConnectionMultiplexer _redis;

        private ConcurrentDictionary<string, object> _recentlySent = new ConcurrentDictionary<string, object>();
        
        public BqRedisScheduler(IBqRepository repository)
        {
            _repository = repository;
        }
        
        
        private IDatabase Db() => _redis.GetDatabase();

        public ConnectionMultiplexer Mux => _redis;
        private string SubChan(string channelName) => $"{KEY_SUB_CHANNEL_NAME}/{channelName}";
        private string WorkList(string channelName) => $"{KEY_WORKLIST_NAME}/{channelName}";

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
        private RedisSchedulerLeaderWork _leader;

        public async Task NotifyJobAvailableToListeners(string channel, string id)
        {
            var db = _redis.GetDatabase();
            await db.ListLeftPushAsync(WorkList(channel), id);
            SubCounter++;
            WriteLine($"Iss {id}");
            await db.PublishAsync(SubChan(channel), $"r{SubCounter}", CommandFlags.FireAndForget);
        }

        public Task NotifyJobAvailableToLeader(string id)
        {
            var db = _redis.GetDatabase();
            db.Publish(KEY_LEADER_SUB, "n" + id);
            return Task.CompletedTask;
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

        public void StartAsLeader()
        {
            this._leader = new RedisSchedulerLeaderWork(this);
            this._leader.Start();
        }

        public void StopAsLeader()
        {
            this._leader.Stop();
        }
        
    }
}
