using System;
using System.Threading.Tasks;
using TrivialTestRunner;

using Bq.Jobs;
using Google.Protobuf.WellKnownTypes;

namespace Bq.Tests
{
    public class PingHandler: BqMessageHandler<DemoMessagePing>
    {
        protected override Task HandleMessage(IJobContext context, DemoMessagePing message)
        {
            Console.WriteLine("handling job " + context.Envelope.Token);
            context.Complete();
            return Task.CompletedTask;
        }
    }
    public class BqTests
    {
        [Case]
        public static void CreateWorker()
        {
            var hub = new BqJobHub();
            var handler = new PingHandler();
            
            hub.AddHandler(handler);
            var pingMessage = new DemoMessagePing
            {
                Message = "hello"
            };

            var env = new Envelope
            {
                Msg = Any.Pack(pingMessage)
            };

            hub.DispatchToHandler(env);

        }
    }
}