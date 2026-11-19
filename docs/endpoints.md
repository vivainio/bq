# Endpoints and sessions

Register handlers explicitly before calling `RabbitEndpoint.Start`. Register event handlers with `config.Handle(() => new EventHandler(), subscribe: true)`. The factory creates a handler for each processing attempt. For scoped dependencies, use `config.Handle<PlaceOrder>(context => new Handler(/* construct services using context */))`. Bq does not automatically discover handlers or register an endpoint with a DI container; the host owns those registrations and endpoint lifetime. See the [Castle Windsor reference application](windsor.md) for an end-to-end example.

Configure command destinations with `config.Route<PlaceOrder>("orders")`, then call `await endpoint.Send(command)`; `endpoint.Publish(evt)` broadcasts to registered subscribers. `endpoint.SendLocal(message)` sends to that endpoint's own queue without a route. Use `Send(destination, message)` on an endpoint, handler context, or uniform session to address a queue explicitly and override the configured route. Handlers have `Send`, `SendLocal`, `Publish` and `Reply` on their context. Handler-context operations are recorded in the current transactional outbox; endpoint operations publish directly. Message types use their full .NET type name on the wire by default. To give an ordinary class a stable identity, register it before starting the endpoint:

```csharp
config.RegisterMessageContract<OrderPlaced>("orders.placed.v1", "Legacy.Messages.OrderPlaced");
```

The first name is used for outgoing messages and event routing. A registered handler accepts the first name and every listed alias. Event handlers registered with `subscribe: true` bind to each of those names. Deploy receivers with both names first, while producers still emit the old name. Then switch producers to the new name. Keep the old alias while queued messages, schedules, or replays can still use it. When retiring an alias, also remove its old RabbitMQ binding if it was used for events. Bq adds bindings at startup and does not remove existing ones. The MessagePack payload must remain compatible across versions. Saga storage keys and internal timeout identifiers still use CLR names.

## Uniform sessions

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

Create a session view with `WithHeader` to attach application metadata to every message sent through that view, including scheduled sends and publishes:

```csharp
var tenantSession = context.UniformSession.WithHeader("tenant-id", "north");
await tenantSession.SendLocal(new RebuildTenantIndex());
```

Each session view has its own header map. The `x-bq-` prefix is reserved. Headers are stored with outbox and scheduled messages, so they survive restarts and are preserved by retries. Values must use RabbitMQ field-table types: strings, booleans, numeric primitives, byte arrays, nested string-keyed tables, or lists of supported values. Received messages expose these as `context.Headers`.

## Concurrency and shutdown

`config.MaxConcurrency` sets the maximum number of deliveries processed simultaneously per endpoint instance (default `1`). RabbitMQ prefetch follows that value by default; set `config.PrefetchCount` to tune how many unacknowledged deliveries RabbitMQ can send to this consumer independently. A value of `0` asks RabbitMQ for unlimited prefetch. Concurrent handlers get separate persistence sessions. Actual throughput depends on the provider; SQLite's single-writer locking can serialize work or cause contention.

Call `await endpoint.StopAsync(cancellationToken)` to stop accepting deliveries and wait for active handlers and the dispatcher before closing the connection. If cancellation is requested before draining completes, the connection is forcibly closed, the handler context's `CancellationToken` is cancelled, and unacknowledged deliveries can be redelivered. `Dispose()` is an immediate forced close; use `StopAsync` for graceful shutdown. Bq checks for forced cancellation before committing the handler transaction. A commit already in progress may still succeed. Handler code should observe its context cancellation token, and external side effects should be idempotent because an in-flight handler can finish after a forced close.

Every endpoint requires a configured persistence provider implementing both `IPersistenceProvider` and `IRecoveryPersistence`, with each session implementing `IRecoverySession`. Initialize its schema before starting the endpoint. All instances of the same endpoint must share the same database and compatible configuration.

The endpoint declares a durable input queue and a configurable error queue (default `<endpoint>.error`). A send requires an existing destination queue; unroutable commands and failed publisher confirms throw. Published events without subscribers are discarded. Messages are persistent. Handler-generated sends are committed to the outbox with business and saga data. Sending directly through an endpoint is outside the handler transaction.

## Broker reconnects

Set `config.NetworkRecoveryInterval` to tune the delay between RabbitMQ client reconnection attempts after losing an established connection. It defaults to five seconds. The initial call to `RabbitEndpoint.Start` still requires an available broker.

## Schedule a message

Use `await endpoint.SendAt("billing", message, dueAt)` with a `DateTimeOffset` to store a durable send. The message becomes eligible for publication at that instant. Polling and broker availability affect actual delivery time. Use `SendAt(message, dueAt)` to use the configured route or `SendLocalAt(message, dueAt)` to target the endpoint’s own queue. For a send-only producer, provision the named queue separately.

Handlers and application transaction callbacks can call `session.SendAt(destination, message, dueAt)` on their context or transactional session. The schedule commits with their other database work and disappears on rollback. The SQLite backlog totals labeled timeouts also include scheduled sends. Use `DispatchKind.ScheduledSend` to inspect ordinary scheduled messages separately.

Use `PublishAt(event, dueAt)` to schedule an event for all subscribers bound when RabbitMQ routes the publish. Scheduling is also available on a uniform session. A handler-scoped uniform session uses the handler transaction, and a session created outside a handler opens a transaction to store the schedule. Like immediate publish, a scheduled event can have no subscribers.

## Send-only endpoints

Call `config.SendOnly()` before starting a producer that only sends or publishes. Bq starts the publisher and durable outbox dispatcher without declaring an input queue or starting a consumer. Initialize persistence as usual. Register routes for outgoing commands and omit handlers and sagas.

Send-only messages carry no reply address. `SendLocal` targets the configured endpoint name. A receiving endpoint or provisioning step must create that queue before the send. Use an explicit receiving endpoint for scheduled sends.

When resolved inside `ExecuteTransaction`, a uniform session joins that application transaction for immediate and scheduled messaging. A session obtained before the transaction cannot be reused inside it.


## RabbitMQ topology

`config.EventsExchange` and `config.ErrorExchange` select the durable topic and direct exchanges. Their defaults are `bq.events` and `bq.errors`. Publishers and subscribers must agree on the event exchange name. A pending outbox message retains the exchange it was originally queued for.

Set AMQP queue arguments before starting a receiving endpoint, for example:

```csharp
config.EventsExchange = "orders.events";
config.ErrorExchange = "orders.errors";
config.InputQueueArguments["x-message-ttl"] = 120_000;
```

RabbitMQ requires a queue declaration to match existing topology. Coordinate argument changes with queue provisioning. Bq supplies the dead-letter exchange and routing key using `ErrorExchange` and the endpoint name, and binds the error queue configured in `Recoverability.ErrorQueue`.


### Connection settings

Use `config.ConfigureRabbitMqConnection` to customize the RabbitMQ .NET client’s connection factory during endpoint startup. Bq initializes it from the broker URI, invokes the hook, then applies async consumer dispatch, `MaxConcurrency`, and `NetworkRecoveryInterval` from the endpoint configuration.

```csharp
config.ConfigureRabbitMqConnection = factory =>
{
    factory.RequestedHeartbeat = TimeSpan.FromSeconds(30);
    factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(10);
};
```

## Change a subscription at runtime

On a receiving endpoint with a registered handler, use `await endpoint.Subscribe<MyEvent>()` and `await endpoint.Unsubscribe<MyEvent>()`. Bq binds or unbinds the endpoint's RabbitMQ queue for the registered event name and receive aliases. Unsubscribing applies to future publications. Messages already in the queue can still reach the handler. A new endpoint instance applies any subscriptions declared in its startup configuration.
