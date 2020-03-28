using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Bq
{
    // create singleton instance of this
    public class BqRedisScheduler
    {
        public async Task Connect()
        {
            var conn = await ConnectionMultiplexer.ConnectAsync("localhost");
            return;
        }
        
        
        
        
        
    }
}
