using System;
using System.Threading.Tasks;
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
    }
    // create singleton instance of this
    public class BqRedisScheduler : IBqScheduler
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


        private string SubChan(string channelName) => $"{SUB_CHANNEL_NAME}/{channelName}";
        private string WorkList(string channelName) => $"{WORKLIST_NAME}/{ChannelName}";
        public async Task ConnectAsync()
        {
            _redis = await ConnectionMultiplexer.ConnectAsync("localhost:17005");
            _redisQueue = new RedisPubSubQueue(_redis, SubChan(ChannelName), 
                WorkList(ChannelName)
                );
        }
 
        public void StartListening(Func<string, Task> onReceive)
        {
            
            if (_redisQueue == null)
                throw new InvalidOperationException("Must call Connect() before StartListening()");
            _redisQueue.StartListening(( async msg => { await onReceive(msg); }));
        }

        public void Clear(string channel)
        {
            var db = Db();
            db.ListTrim(WorkList(channel), 0, 0);
        }

        private static int SubCounter = 0;
        public async Task SendAsync(string channel, string id)
        {
            var db = _redis.GetDatabase();
            await db.ListLeftPushAsync(WorkList(channel), id);
            // short for "read now" eh
            SubCounter++;
            await db.PublishAsync(SubChan(channel), $"r{SubCounter}", CommandFlags.FireAndForget);
        }
    }
}
