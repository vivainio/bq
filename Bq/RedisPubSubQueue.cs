using System;
using System.Threading.Tasks;
using StackExchange.Redis;
using static System.Console;
namespace Bq
{
    public class RedisPubSubQueue
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly string _subName;
        private readonly string _listName;
        private static readonly Random _random = new Random();
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
                    var jitterDelay = _random.Next(0, 20);
                    // we add some jitter to spread the load
                    while (true)
                    {
                        await Task.Delay(jitterDelay);
                        RedisValue work = db.ListRightPop(_listName);
                        if (work.IsNull)
                        {
                            // no more stuff in queue
                            break;
                        }
                        await handler(work);
                    }
                }
            );
        }
    }
}