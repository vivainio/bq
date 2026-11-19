# Bq messaging prototype

RabbitMQ endpoints with contractless MessagePack messages, targeting .NET Standard 2.0 and usable from .NET Framework 4.7.2 / Mono. This is an early Bq API inspired by NServiceBus, not a replacement for its assemblies or its RabbitMQ wire protocol. The older database and Redis projects remain separate.

```csharp
using System;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;

public class PlaceOrder { public string OrderId { get; set; } }
public class OrderPlaced { public string OrderId { get; set; } }
public class Handler : IHandleMessages<PlaceOrder>
{
    public Task Handle(PlaceOrder message, IMessageHandlerContext context) =>
        context.Publish(new OrderPlaced { OrderId = message.OrderId });
}

// Configure before starting. Each registration creates a handler per attempt.
var config = new EndpointConfiguration("orders", new Uri("amqp://guest:guest@localhost:5672/"));
config.Handle(() => new Handler());
var store = new SqlitePersistence(() => CreateSqliteConnection(), "orders");
await store.Initialize();
config.Persistence = store;
using (var endpoint = RabbitEndpoint.Start(config))
{
    // Keep the process running until shutdown.
}
```

Register handlers explicitly before calling `RabbitEndpoint.Start`. Register event handlers with `config.Handle(() => new EventHandler(), subscribe: true)`. The factory creates a handler for each processing attempt. For scoped dependencies, use `config.Handle<PlaceOrder>(context => new Handler(/* construct services using context */))`. Bq does not automatically discover handlers or register an endpoint with a DI container; the host owns those registrations and endpoint lifetime. See the [Castle Windsor reference application](../Bq.Reference.Windsor/README.md) for an end-to-end example.

Configure command destinations with `config.Route<PlaceOrder>("orders")`, then call `await endpoint.Send(command)`; `endpoint.Publish(evt)` broadcasts to registered subscribers. `endpoint.SendLocal(message)` sends to that endpoint's own queue without a route. Use `Send(destination, message)` on an endpoint, handler context, or uniform session to address a queue explicitly and override the configured route. Handlers have `Send`, `SendLocal`, `Publish` and `Reply` on their context. Handler-context operations are recorded in the current transactional outbox; endpoint operations publish directly. Message types use their full .NET type name on the wire; receivers must explicitly register each type. Renaming types or properties changes the contract. Avoid polymorphic `object` properties; test schema changes with both versions.

### Uniform sessions

Bq provides its own `IUniformSession` interface with `Send`, `SendLocal`, and `Publish`. It is a small compatibility-oriented surface, not the NServiceBus interface or an automatic dependency-injection integration. Obtain a fresh session for each scope:

```csharp
// Outside a handler, this publishes directly through the running endpoint.
IUniformSession requestSession = endpoint.CreateUniformSession();
await requestSession.SendLocal(new PlaceOrder { OrderId = "123" });

// Inside a handler, a context-aware factory can inject the session into shared code.
config.Handle<PlaceOrder>(context => new OrderHandler(
    new SharedOrderService(context.UniformSession)));
```

Resolve handler sessions inside the handler factory or from `context.UniformSession`; their sends participate in the handler's database transaction and inherit its correlation ID. An external session does not make external database changes transactional. Do not cache a session across scopes: Bq rejects a handler session after its handler ends, an external session used inside a handler, and any session used after its endpoint stops. Sessions are endpoint-specific; sending through another endpoint does not join the current handler transaction.

Use `context.UniformSession.WithHeader("tenant-id", "north")` to create a session view that attaches application metadata to immediate and scheduled sends and publishes. Headers are stored in the outbox and preserved by retries. Values must use RabbitMQ field-table types. Incoming headers are available through `context.Headers`.

### Concurrency and shutdown

`config.MaxConcurrency` sets the maximum number of deliveries processed simultaneously per endpoint instance (default `1`). RabbitMQ prefetch follows that value by default; set `config.PrefetchCount` to tune how many unacknowledged deliveries RabbitMQ can send to this consumer independently. A value of `0` asks RabbitMQ for unlimited prefetch. Concurrent handlers get separate persistence sessions. Actual throughput depends on the provider; SQLite's single-writer locking can serialize work or cause contention.

Call `await endpoint.StopAsync(cancellationToken)` to stop accepting deliveries and wait for active handlers and the dispatcher before closing the connection. If cancellation is requested before draining completes, the connection is forcibly closed, the handler context's `CancellationToken` is cancelled, and unacknowledged deliveries can be redelivered. `Dispose()` is an immediate forced close; use `StopAsync` for graceful shutdown. Handler code should observe its context cancellation token, and external side effects should be idempotent because an in-flight handler can finish after a forced close.

Every endpoint requires a configured persistence provider implementing both `IPersistenceProvider` and `IRecoveryPersistence`, with each session implementing `IRecoverySession`. Initialize its schema before starting the endpoint. All instances of the same endpoint must share the same database and compatible configuration.

The endpoint declares a durable input queue and a configurable error queue (default `<endpoint>.error`). A send requires an existing destination queue; unroutable commands and failed publisher confirms throw. Published events without subscribers are discarded. Messages are persistent. Handler-generated sends are committed to the outbox with business and saga data. Sending directly through an endpoint is outside the handler transaction.

## Recoverability

```csharp
config.Recoverability.ImmediateRetries = 3;
config.Recoverability.DelayedRetries = new[] {
    TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1)
};
config.Recoverability.ErrorQueue = "orders.errors";
config.Recoverability.IsUnrecoverable = ex => ex is InvalidOperationException;
```

Each delivery gets the configured immediate retries (default two), then each configured delay (default 10, 20 and 30 seconds) starts a new immediate-retry cycle. Handler failures roll back their database transaction. Unknown message types, unsupported content types and deserialization failures skip retries. The optional predicate can mark more exceptions unrecoverable. An empty delay array disables delayed retries.

Before acknowledging a failed delivery, Bq commits its recovery decision and a snapshot of the original AMQP properties, headers and body to the database. The dispatcher publishes due retries to the input queue and errors to the configured error queue using publisher confirms and mandatory routing. A storage failure requeues the original. A crash after the decision commits but before acknowledgment can cause redelivery; the persisted message-ID-and-cycle decision makes that delivery a no-op. Publishing a recovery record can itself occur more than once; retries retain their message ID so the inbox suppresses duplicate successful processing. Error queue copies may be duplicated. The message ID is required for deduplication; a failed message without one is assigned an ID and moved to error, but its original redelivery cannot be deduplicated.

Failures are retained in the database for inspection and contain type, message, stack, timestamps, cycle and encoded original message. Error queue copies also carry `x-bq-failure-*` headers. Inspect and replay using:

```csharp
var errors = await endpoint.ListErrors(limit: 100);
foreach (var error in errors)
{
    Console.WriteLine(error.MessageId + ": " + error.ExceptionMessage);
    // Once the cause is corrected: await endpoint.ReplayError(error.RecoveryId);
}
```

SQLite users can page through errors with `store.ListErrors(endpointName, limit, after)`. Pass the last record from the previous page as `after`. An empty page ends the scan.

Replay is an atomic database operation and can be requested once per error record. It creates a fresh message ID so the old retry decisions and inbox entry do not suppress it; the original ID is retained in `x-bq-original-message-id`. Replay does not remove an already-published copy from the RabbitMQ error queue. Consumption directly from that queue is separate from Bq's inspection and replay API. Retry and error records are retained indefinitely; monitor and back up the database. An unavailable error queue leaves its database record pending, and can block subsequent recovery records until restored. Ordinary outbox messages and saga timeouts use a separate polling loop. Delivery remains at least once; external side effects still need idempotency.

This is an early Bq API inspired by NServiceBus, not a replacement for its assemblies or its RabbitMQ wire protocol.

## Tests

```sh
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj
mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
BQ_RABBITMQ_URI=amqp://guest:guest@localhost:5672/ mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
```

The last command also exercises command delivery, replies, event fan-out, saga timeouts, delayed retry, replay, and failure handling against a local RabbitMQ broker. For example, with Podman:

```sh
podman run -d --name bq-rabbitmq -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 docker.io/library/rabbitmq:3.13-management
```

## SQLite persistence and saga state

`Bq.Persistence.Sqlite` is the first persistence provider. It accepts a factory for an ADO.NET SQLite connection, so the host chooses a compatible driver (the Mono tests use `Mono.Data.Sqlite`). Use a file-backed database shared by instances of the same endpoint, and initialize the schema before starting the endpoint:

```csharp
var store = new SqlitePersistence(() => CreateSqliteConnection(), "orders");
await store.Initialize();
config.Persistence = store;
using (var endpoint = RabbitEndpoint.Start(config)) { /* host the endpoint */ }
```

Within a handler, `context.PersistenceSession.Connection` and `.Transaction` are the same connection and local transaction used by the inbox, saga and outbox stores. Application commands must use both. Bq records the incoming message ID in a unique inbox key, invokes the handler, records context-generated sends/publishes/replies in the outbox, commits, and then acknowledges RabbitMQ. On failure the transaction is disposed without commit. Each immediate retry creates a new session and transaction. A dispatcher polls committed outbox records; it deletes a record only after a broker confirmation. Publisher-confirm uncertainty and multiple dispatchers can produce duplicates, with the same outgoing message ID.

### Sagas

Define a saga with a data class, message handlers, and optional timeout handlers. Register each message's correlation-key selector; `Start` messages create an instance when none exists, while `Handle` messages only apply to an existing instance. This API resembles NServiceBus concepts but is not its configuration API.

```csharp
public class OrderState { public string Status { get; set; } }
public class PlaceOrder { public string OrderId { get; set; } }
public class PaymentReceived { public string OrderId { get; set; } }
public class PaymentDeadline { public string OrderId { get; set; } }

public class OrderSaga : Saga<OrderState>, IAmStartedByMessages<PlaceOrder>,
    IHandleMessages<PaymentReceived>, IHandleTimeouts<PaymentDeadline>
{
    public async Task Handle(PlaceOrder message, IMessageHandlerContext context)
    {
        Data.Status = "waiting";
        await RequestTimeout(TimeSpan.FromMinutes(15), new PaymentDeadline { OrderId = message.OrderId });
    }
    public Task Handle(PaymentReceived message, IMessageHandlerContext context)
    {
        MarkAsComplete();
        return Task.CompletedTask;
    }
    public Task Timeout(PaymentDeadline timeout, IMessageHandlerContext context)
    {
        Data.Status = "expired";
        return Task.CompletedTask;
    }
}

config.RegisterSaga<OrderSaga, OrderState>(() => new OrderSaga(), mappings =>
{
    mappings.Start<PlaceOrder>(m => m.OrderId);
    mappings.Handle<PaymentReceived>(m => m.OrderId);
    mappings.Timeout<PaymentDeadline>();
});
```

Saga registrations require persistence. The selector returns a nonempty, stable string; use the same key format for all message types of the saga. Set any corresponding property on `Data` yourself. A saga instance is created per processing attempt. Its state, version, requested timeouts, incoming-message claim and context-generated outgoing messages share one transaction. Optimistic version checks and the database transaction protect state updates; database contention may cause retries. Other handlers of the same message execute in the same processing transaction.

`MarkAsComplete()` deletes the active state at commit. A later start message may create a new instance with the same key. Timeouts have a unique saga-instance identity, so timeouts requested by the old instance are ignored. A non-starting message with no matching saga is ignored by default; set `config.SagaNotFound` to inspect it or throw to trigger recoverability. Missing or obsolete timeouts are silently ignored. Messages may arrive out of order, so choose the callback behavior accordingly.

Timeouts are stored in SQLite and polled by the endpoint's outbox dispatcher, including after restart. Their actual delivery can be later than requested; clock adjustments and a blocked dispatcher affect timing. Delivery is at least once and duplicate suppression uses the persistent inbox. Completion does not proactively delete outstanding timeout records; they are discarded when delivered. Keep the inbox, timeout and saga data for as long as messages can be redelivered. Saga type names, correlation keys, timeout types and MessagePack property names are persisted contracts. The lower-level `IPersistenceSession` saga methods remain available for manual state management; they use their own type keys and do not automatically manage the saga lifecycle.

`config.DispatchPollInterval` sets the wait after an empty or failed dispatch poll. It defaults to one second, and both the outbox and recovery loops use it. Shorter intervals reduce idle pickup latency and increase database polling. `config.PublisherConfirmTimeout` controls how long direct sends and both background dispatchers wait for publisher confirmation. It defaults to ten seconds. After a failed confirmation, durable records remain pending for another attempt.

Use `await store.GetDispatchBacklog()` to count pending outbox messages, due and scheduled saga timeouts, and due and scheduled recovery records for the SQLite endpoint. Counts include records awaiting publisher confirmation. Use `await store.ListPendingDispatches(limit: 100)` to inspect message IDs, dispatch kinds, exchanges, routing keys, and scheduled UTC times. Pass a `DispatchKind` such as `DispatchKind.Error` as the first argument to filter before applying the limit. The listing omits message bodies and its order is for inspection.

The inbox and recovery ledger currently have no retention cleanup, and the outbox has no dead-letter policy: a persistently unroutable command blocks later ordinary outbox records until fixed. The SQLite dispatcher alternates preference between ordinary outbox messages and due saga timeouts on each poll. The SQLite provider is a first implementation intended for local or low-concurrency use, not a claim that SQLite supports distributed multi-host deployment. Run the provider against the exact filesystem, SQLite driver and Mono version used in production. Handler external effects, other databases, and sends made outside the handler context are not part of the transaction.
