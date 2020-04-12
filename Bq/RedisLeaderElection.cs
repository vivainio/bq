using System;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Bq.Jobs;
using Google.Protobuf.WellKnownTypes;
using StackExchange.Redis;
using Timer = System.Threading.Timer;

namespace Bq
{
    public class RedisLeaderOptions
    {
        public string Id { get; set; } = Guid.NewGuid().ToString("N");
        public string Key { get; set; } = "leader.changeme"; // default leader key
        public TimeSpan Ttl { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan Wait { get; set; } = TimeSpan.FromSeconds(1);
    }

    public enum RedisLeaderEventType
    {
        Elected = 1,
        Revoked = 2
    }
    public class RedisLeaderEventArgs
    {
        public RedisLeaderEventType Type { get; }

        public RedisLeaderEventArgs(RedisLeaderEventType type)
        {
            Type = type;
        }
        
        
    }
    
    public class RedisLeaderElection
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly RedisLeaderOptions _options;
        private Timer renewTimer;
        private Timer electTimer;
        private IDatabase Db() => _redis.GetDatabase();

        public delegate void RedisLeaderEventHandler(object sender, RedisLeaderEventArgs e);
        
        public event RedisLeaderEventHandler RedisLeaderEvent;
        public RedisLeaderElection(ConnectionMultiplexer redis, RedisLeaderOptions options)
        {
            _redis = redis;
            _options = options;
        }

        public bool IsLeader()
        {
            var db = Db();
            var currentLeader = db.StringGet(_options.Key);
            if (currentLeader.IsNull)
            {
                return false;
            }
            return currentLeader == _options.Id;
        }

        public void Emit(RedisLeaderEventType evt)
        {
            RedisLeaderEvent?.Invoke(this, new RedisLeaderEventArgs(evt));
            
        }
        // start trying to get elected or remaining elected
        public void Elect()
        {
            var db = Db();
            var isLeader = db.StringSet(_options.Key, _options.Id, _options.Ttl, When.NotExists);
            if (isLeader)
            {
                Emit(RedisLeaderEventType.Elected);
                var wait = new TimeSpan(_options.Ttl.Ticks / 2);
                this.renewTimer = new Timer((state) => { Renew(); }, this, wait, wait );
            }
            else
            {
                this.renewTimer?.Dispose();
                this.electTimer = new Timer((state) =>
                {
                    Elect();
                }, this, _options.Wait, Timeout.InfiniteTimeSpan);
            }
        }

        public void Stop()
        {
            var db = Db();
            var isLeader = IsLeader();
            if (isLeader)
            {
                db.KeyDelete(_options.Key);
                Emit(RedisLeaderEventType.Revoked);
            }
            this.electTimer?.Dispose();
            this.renewTimer?.Dispose();
        }

        private void Renew()
        {
            var isLeader = IsLeader();
            var db = Db();
            if (isLeader)
            {
                db.KeyExpire(_options.Key, _options.Ttl);
            }
            else
            {
                this.renewTimer.Dispose();
                this.RedisLeaderEvent?.Invoke(this, new RedisLeaderEventArgs(RedisLeaderEventType.Revoked));
                this.electTimer = new Timer((state) =>
                {
                    Elect();
                    
                }, this, _options.Wait.Milliseconds, Timeout.Infinite);
                
                
            }
        }
    }
}