using System;
using System.Diagnostics.Tracing;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Bq.Jobs;
using StackExchange.Redis;

namespace Bq
{

    // cheapo alternative to Streams
    public class RedisPubSubQueue
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly string _subName;
        private readonly string _listName;

        public RedisPubSubQueue(ConnectionMultiplexer redis, string subName, string listName)
        {
            _redis = redis;
            _subName = subName;
            _listName = listName;
        }

        // 
        public void StartListening(Func<RedisValue, Task> handler)
        {
            var db = _redis.GetDatabase();
            var sub = _redis.GetSubscriber();

            sub.Subscribe(_subName, async (channel, channelMessage) =>
                {
                    RedisValue work = db.ListRightPop(_listName);
                    if (!work.IsNull)
                    {
                        await handler(work);
                    }
                }
            );
        }

        public void Send(RedisValue message)
        {
            var db = _redis.GetDatabase();
            var chan = _redis.GetSubscriber();
            db.ListLeftPush(_listName, message);
            // short for "read now" eh
            db.Publish(_subName, "r", CommandFlags.FireAndForget);
        }
    }
    // create singleton instance of this
    public class BqRedisScheduler
    {
        private const string WORKLIST_NAME = "bq_worklist";
        private const string SUB_CHANNEL_NAME = "bq_chan";
        private ConnectionMultiplexer _redis;
        private RedisPubSubQueue _redisQueue;

        public BqRedisScheduler(string channelName)
        {
            ChannelName = channelName;
        }

        public string ChannelName { get; set; }
        
        private IDatabase Db() => _redis.GetDatabase();
        public async Task ConnectAsync()
        {
            _redis = await ConnectionMultiplexer.ConnectAsync("localhost:17005");
            _redisQueue = new RedisPubSubQueue(_redis, $"{SUB_CHANNEL_NAME}/{ChannelName}", 
                $"{WORKLIST_NAME}/{ChannelName}");
        }
 
        public void StartListening(Func<string, Task> onReceive)
        {
            
            if (_redisQueue == null)
                throw new InvalidOperationException("Must call Connect() before StartListening()");
            _redisQueue.StartListening(( async msg => { await onReceive(msg); }));
        }

        public void Send(string id)
        {
            _redisQueue.Send(id);
        }
        
        
    }
}
