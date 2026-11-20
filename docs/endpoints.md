# Endpoints and sessions

Register handlers explicitly before calling `RabbitEndpoint.Start`. Register event handlers with `config.Handle(() => new EventHandler(), subscribe: true)`. The factory creates a handler for each processing attempt. For scoped dependencies, use `config.Handle<PlaceOrder>(context => new Handler(/* construct services using context */))`. Bq does not automatically discover handlers or register an endpoint with a DI container; the host owns those registrations and endpoint lifetime. See the [Castle Windsor reference application](windsor.md) for an end-to-end example.

Configure command destinations with `config.Route<PlaceOrder>("orders")`, then call `await endpoint.Send(command)`; `endpoint.Publish(evt)` broadcasts to registered subscribers. `endpoint.SendLocal(message)` sends to that endpoint's own queue without a route.

Use `Send(destination, message)` on an endpoint, handler context, or uniform session to address a queue explicitly and override the configured route. Handlers have `Send`, `SendLocal`, `Publish` and `Reply` on their context. Handler-context operations are recorded in the current transactional outbox; endpoint operations publish directly. Message types use their full .NET type name on the wire by default. To give an ordinary class a stable identity, register it before starting the endpoint:

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

## Endpoint startup and queue configuration

`Endpoint.Create` returns an `IStartableEndpoint` whose `Start` returns an `IEndpointInstance`. `Endpoint.Start` starts directly. Bq takes an explicit RabbitMQ URI and persistence provider. `PurgeOnStartup(true)` deletes ready messages from the input queue before the consumer starts, on every start with that setting. It defaults to false.

Use `DoNotCreateQueues()` with pre-provisioned RabbitMQ queues and exchanges. Bq checks that they exist and leaves their arguments intact. Provision the dead-letter topology beforehand.

`OverrideLocalAddress` changes the input queue, local-send destination, and default reply address. Persistence and the default error queue still use the logical endpoint name. Explicit routes to a renamed queue must use its new address. Event subscriptions still manage their bindings.

## Routing and subscriptions

Register routes with `Route<T>` and handlers with `Handle<T>`. Subscribe through handler registration at startup or use `IMessageSession.Subscribe` and `Unsubscribe` at runtime.

Use `AutoSubscribe().DisableFor<T>()` to exclude an event from convention-based startup subscriptions. `AutoSubscribe().DoNotAutoSubscribeSagas()` suppresses convention-based subscriptions from saga registrations. Explicit subscriptions still work. The `IMessageSessionExtensions` class exposes the send, publish, and optionless subscription helpers. The parameterless generic forms also work as `Func<Task>` method groups. Runtime calls update RabbitMQ bindings for the registered contract and its aliases.

Use `Conventions().DefiningEventsAs(...)` for automatic subscriptions on handlers without an explicit subscription setting. Use `Conventions().Add(IMessageConvention)` to add reusable classifications. An event classification from a registered convention can trigger automatic subscription on its own. Command and message conventions classify types for host routing, and assembly scanning stays in the host.

## Outgoing message options

Typed `Send(message, options)` supports destination selection, `RouteToThisEndpoint`, headers, scheduling, and immediate dispatch. Sends, publications, and replies can request immediate dispatch. Shared helpers can set `RequireImmediateDispatch()` and read `RequiredImmediateDispatch()` through `ExtendableOptions`. Immediate sends reach RabbitMQ outside the transaction and can be repeated by a handler retry.

Replies can also select an explicit destination. Use `SetMessageId` to supply the wire ID and `RouteReplyTo` on send or reply options to set the return address. Use `OverridePublicReturnAddress` to set the endpoint default, including on send-only endpoints. A per-message reply route takes precedence. Both forms accept up to 255 UTF-8 bytes.

Use `AddHeaderToAllOutgoingMessages` to set endpoint-wide defaults for new outgoing messages. Per-message headers take precedence. `DisableDurableMessages()` selects transient broker delivery for outgoing publications and sends, including outbox dispatch. `EnableDurableMessages()` restores the default. Outbox storage, queue durability, and recovery delivery are independent of this setting.

Apply `[TimeToBeReceived("00:10:00")]` to a message contract to set its RabbitMQ TTL. For scheduled and outbox messages, the TTL starts at broker publish. Use `Conventions().DefiningTimeToBeReceivedAs(...)` to replace attribute-based lookup. Return `TimeSpan.MaxValue` for contracts with no TTL.

Use `RoutingOptionExtensions` for explicit destination and reply-route helpers and `DelayedDeliveryOptionExtensions` for scheduling helpers. Use `GetDestination`, `GetReplyToRoute`, `IsRoutingToThisEndpoint`, `GetDeliveryDelay`, `GetDeliveryDate`, `GetMessageId`, and `GetHeaders` to inspect explicitly configured options. Shared helpers can set and read message IDs and text headers through `ExtendableOptions`. `GetHeaders` returns a text snapshot. `GetExtensions()` gives caller code a context bag on each send, publish, reply, subscribe, or unsubscribe options instance. The bag is local to that options object. Use `Send(destination, message)` and `SendLocal(message)` on `IMessageSession` or `IPipelineContext`. Both shortcuts also accept message initializers. Use `IPipelineContextExtensions` and `IMessageProcessingContextExtensions` for explicit calls to the pipeline send, publish, and reply helpers. Use `Publish<T>()` on either API to publish a default-initialized event.

The object-based `IMessageSession` overloads resolve the contract from the concrete message type. You can pass typed message initializers directly to `IEndpointInstance.Send` and `Publish` and to handler-context `Send`, `Publish`, and `Reply`, with or without options. Uniform and transactional sessions also accept optionless `Send` and `Publish` initializers. Message-initializer overloads accept classes with a public parameterless constructor and public interfaces with read/write data properties. Interface contracts use their declared type for routing and wire names. Use `IMessageCreator` with Bq's `MessageCreator` to construct a message separately from sending or publishing it.

## Handler context and message copies

Bq provides the string-valued `MessageHeaders` view and `ReplyToAddress`. Use `Headers` for raw AMQP values and `PersistenceSession` for the shared transaction. Use the typed `Extensions` bag to share state between an incoming behavior, handler, and before-commit callbacks. Each retry gets a fresh bag. `ForwardCurrentMessageTo` queues the original wire body, contract, headers, and message ID in the outbox for another queue.

Use `ForwardReceivedMessagesTo` to forward successfully handled messages through the same outbox. Provision the destination queue for outbox delivery. `AuditProcessedMessagesTo(queue, timeToBeReceived)` writes a separate copy through the outbox. The optional TTL starts when RabbitMQ receives the copy. Both features skip internal saga timeouts. Bq’s `SynchronizedStorageSession` exposes the same connection and transaction as `PersistenceSession`. Use those ADO.NET objects for transaction-scoped application work.

## Message property encryption

Call `EnableMessagePropertyEncryption(encryptionService, encryptedPropertyConvention)` on each participating endpoint. Supply an `IMessagePropertyEncryptionService` that authenticates ciphertext and manages keys, and select public read/write string properties on the message contract. Bq encrypts selected values on a copy before transport dispatch and decrypts them before the incoming logical pipeline. For attribute-based selection, mark the properties with `[EncryptedProperty]` and call `EnableMessagePropertyEncryption(encryptionService)`. A convention can select additional properties. Properties marked with `[EncryptedProperty]` are always selected on an encryption-enabled endpoint. Configure the same selected properties on both endpoints. The service defines the ciphertext format and must support concurrent calls.

If the provider throws, Bq reports which operation failed and omits the provider's exception text. A decryption failure goes to recoverability with the received wire body.

Bq validates encryption selections for registered contracts, handlers, and routes at startup. Other outgoing contracts are validated the first time they are sent or published.

A contract marked with `[EncryptedProperty]` requires an encryption provider. A concrete contract also honors markers on the interfaces it implements. Bq rejects a known marked contract during startup and rejects an unregistered marked contract before sending or publishing it.

## Message body encryption

Call `EnableMessageEncryption(encryptionService)` with an `IMessageBodyEncryptionService` on participating endpoints. Supply a thread-safe provider that authenticates ciphertext and manages keys. Bq encrypts the serialized body after outgoing transport mutation and before persistence or publish. It decrypts incoming bodies before transport mutation and deserialization. Forwarding, auditing, and recovery retain the received ciphertext. Headers and routing metadata remain visible. Every incoming body on an enabled endpoint must be decryptable by the provider.

Call `context.DoNotContinueDispatchingCurrentMessageToHandlers()` to skip subsequent handlers for the current delivery attempt. The current handler can finish, and ordinary transaction and recoverability rules apply.

Use `Pipeline.Register<TRegisterStep>()` or `Pipeline.Register(step)` to register an incoming behavior with ordering constraints. Derive from `RegisterStep` and declare dependencies with `InsertBefore` or `InsertAfter` by step ID. The `IfExists` forms allow optional steps. Bq resolves ordering at startup and rejects missing required steps and cycles.

Pass a behavior factory to the `RegisterStep` constructor when a behavior needs constructor arguments. Bq calls the factory during registration and reuses the returned behavior across deliveries.

The `Pipeline.Register` and `Pipeline.Replace` overloads also accept a `Func<TBehavior>` for behavior construction during configuration. A factory registration without a step ID uses the full name of `TBehavior`.
