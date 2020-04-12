using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Bq
{
    public class RedisSchedulerLeaderWork
    {
        private readonly BqRedisScheduler _bqScheduler;
        private ISubscriber subscriber;

        public RedisSchedulerLeaderWork(BqRedisScheduler bqScheduler)
        {
            _bqScheduler = bqScheduler;
        }

        IDatabase Db() => _bqScheduler.Mux.GetDatabase();
        public void Start()
        {
            this.subscriber = _bqScheduler.Mux.GetSubscriber();
            this.subscriber.Subscribe(BqRedisScheduler.KEY_LEADER_SUB, async (channel, msg) =>
            {
                await OnLeaderMessage(msg);
            });
        }

        private async Task OnLeaderMessage(RedisValue msg)
        {
            await this._bqScheduler.ReadAndSendWork();
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }
    }
}