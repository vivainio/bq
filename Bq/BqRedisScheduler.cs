using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;
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

        private ShiftOutDict<string, object> _recentlySent = new ShiftOutDict<string, object>();
        
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
        private long _previousMinute;

        public Task NotifyJobsAvailableToListeners(string channel, IReadOnlyList<string> ids)
        {
            var db = _redis.GetDatabase();
            
            var l = new List<Task<long>>();
            var wl = WorkList(channel);
            foreach (var id in ids)
            {
                db.ListLeftPush(wl, id, When.Always, CommandFlags.FireAndForget);
            }

            SubCounter++;
            //WriteLine($"Iss {id}");
            db.Publish(SubChan(channel), $"r{SubCounter}", CommandFlags.FireAndForget);
            return Task.CompletedTask;
        }

        public async Task NotifyJobAvailableToListeners(string channel, string id)
        {
            var db = _redis.GetDatabase();
            await db.ListLeftPushAsync(WorkList(channel), id);
            SubCounter++;
            //WriteLine($"Iss {id}");
            await db.PublishAsync(SubChan(channel), $"r{SubCounter}", CommandFlags.FireAndForget);
        }

        public Task NotifyJobAvailableToLeader()
        {
            var db = _redis.GetDatabase();
            db.Publish(KEY_LEADER_SUB, "CREATED");
            return Task.CompletedTask;
        }

        public bool RotateNeeded()
        {
            
            var currentMinute = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 10;
            if (currentMinute == _previousMinute)
            {
                return false;
            }
            _previousMinute = currentMinute;
            return true;
        }
        // should be repeatedly executed as session leader only
        public async Task ReadAndSendWork()
        {
            var jobs = await _repository.ReadAvailableWork();
            // capacity based rotation. Time based rotation would work as well?
            if (RotateNeeded())
            {
                _recentlySent.Shift();
            }

            var l = new List<(string Channel, string Id)>();
            foreach (var job in jobs)
            {
                var id = job.Id;
                var (dupe, _) = _recentlySent.TryGet(id);
                if (dupe)
                {
                    continue;
                }

                l.Add((job.Channel, job.Id));

                _recentlySent.New[job.Id] = null;
            }

            var byChan = l.GroupBy(p => p.Channel);
            foreach (var grouping in byChan)
            {
                var ids = grouping.Select(it => it.Id).ToList();
                await NotifyJobsAvailableToListeners(grouping.Key, ids);
                
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
