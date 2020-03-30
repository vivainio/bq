# Bq - the Business Queue for dotnet

## Problem statement

Problem: you want to implement batching/messaging patterns (i.e. "run later while I go about other stuff") but with following constraints:

- Messages appear and change state together with business transactions.
- There are no messages anywhere apart from database. There are no queues to
  back up or synchronize
- Messages are small and fast to parse
- Serving the load is not limited to dotnet. Message types are shareable.
- No commercial software, or software that is complex enough to be monetizable

  - Hangfire
  - NServiceBus

  ## Solution

- Messages are stored in database (currently Oracle) using protobuf
- Redis is used to "wake up" the Workers so that only one Worker sees one message

  - LPOP is used to pop message id's from "queue"
  - PUBLISH/SUBSCRIBE is the wake up signal for the servers to do the LPOP
  - Research: Redis Streams viablity?

- Workers are registered to handle protobuf types. The message envelope they see is:

```protobuf
message Envelope {
    google.protobuf.Any msg = 1;
    string id = 2;
    string token = 3;
    string cursor = 4;
}
```

The `Any` type is unpacked to the right message format and dispatched for user
code to handle like so:

```csharp
 public class PingHandler: BqMessageHandler<DemoMessagePing>
    {
        protected override Task HandleMessage(IJobContext context, DemoMessagePing message)
        {
            Console.WriteLine("handling job " + context.Envelope.Token);
            context.Complete();
            return Task.CompletedTask;
        }
    }

// ....

var hub = new BqJobServer();
var handler = new PingHandler();
hub.AddHandler(handler);


```

In addition to calling for Complete for the context, you can partially complete it
by setting a string "cursor" value in the database. If the message has list of 100 documents, you could indicate that you processed 12 of those and want to leave
rest for the later by setting the cursor value to "12". You can also use it for
Saga / state machine like behavior.

Database structure is described by this proto:

```protobuf
// you will only get "READY" jobs when reading
enum JobStatus {
    // not used, required by protobuf
    UNKNOWN = 0;
    // task that can be started by anyone
    READY = 1;
    // task that has been started and someone is working on it
    PENDING = 2;
    // task that has failed
    FAILED = 3;

}
// representation of job in database
message DbJob {
    string id = 1;

    // This can be parsed to Envelope
    bytes envelope = 2;
    string cursor = 4; // this will populate Envelope.cursor & is authority
    google.protobuf.Timestamp launch_at = 5;
    google.protobuf.Timestamp expires_at = 6;
    // descibes "channel" where message is. different workers can be picky about what channels they care about
    // similar to rabbit queue maybe?
    string channel = 7;
    JobStatus state = 8;
}

```
