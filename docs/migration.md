# Migrate from NServiceBus in phases

This page covers porting and verification. For the strategy of running Bq beside NServiceBus and moving one flow at a time, see [Moving one flow at a time](gradual-migration.md). Never send a Bq message from inside an NServiceBus handler; that page explains why.

Start with a small group of endpoints whose senders and receivers can switch together. A send-only Bq producer and a subscriber without saga state make a useful first exercise. List the commands, events, subscriptions, and queues each endpoint uses before choosing the group.

Bq uses MessagePack. Give migrated consumers their own queues and use a distinct event exchange for the Bq group while legacy XML consumers remain active. Route commands to the new queues only when their receivers are ready. Before redirecting each flow, account for messages already queued, scheduled deliveries, and unpublished outbox entries. For flows with active sagas, decide how to handle existing state and pending timeouts before switching traffic.

## Port the endpoint

Use the `Bq.Messaging` APIs in the selected C# projects. Bq provides optional `IMessage`, `ICommand`, and `IEvent` markers. The host can use them when discovering handlers and configuring routes or subscriptions. Use [stable contract names](endpoints.md) to keep routing independent of CLR namespace changes.

### Behaviors and the pipeline

Register incoming behaviors with `configuration.Incoming.Use`, or register a `Behavior<TContext>` with `configuration.Pipeline.Register`. The stages, from outermost to innermost:

| Stage | Behavior context | What it can do |
| --- | --- | --- |
| Delivery | `IDeliveryContext` | Runs before the persistence session opens. [Multi-tenant endpoints](multi-tenant.md) use it to choose a tenant's database. |
| Transport receive | `ITransportReceiveContext` | Wraps each delivery attempt. `AbortReceiveOperation()` rolls back the attempt and requeues the message without counting a failure. |
| Physical | `IIncomingPhysicalMessageContext` | Runs before transport mutators, type resolution and deserialization. Sees the received `IncomingMessage` with editable text headers, can replace the body with `UpdateMessage`, and can short-circuit by skipping `next()`. |
| Logical | `IIncomingLogicalMessageContext` | Runs around message mutators. `UpdateMessageInstance` passes a replacement to downstream behaviors and the handler. |
| Handler invocation | `IInvokeHandlerContext` | Wraps each non-saga handler invocation. Exposes the handler instance, the message and its metadata. |

Physical behaviors run inside each delivery attempt, wrap the logical stage, and have their own ordering. Logical steps run in registration order. Recovery keeps the original wire body.

Outgoing behaviors for `IOutgoingSendContext`, `IOutgoingPublishContext`, `IOutgoingReplyContext`, `IOutgoingLogicalMessageContext` and `IOutgoingPhysicalMessageContext` register through the same `Pipeline.Register`. The operation stage runs first, then the logical stage around message mutators and serialization, then the physical stage around transport mutators. Header edits carry through each stage, `UpdateMessage` replaces the instance or body, and `RoutingStrategies` is read-only.

Rules for every behavior:

- Await `next()` once to continue processing.
- Type registration requires a public parameterless constructor. Container-based behavior factories and other pipeline stages require adaptation.
- Behaviors can share attempt-local state, short-circuit processing, and wrap handler execution.
- Before starting the endpoint, use `Pipeline.Replace(stepId, ...)` to replace a registered behavior in place, or `Pipeline.Remove(stepId)` to remove it.
- Edit `IIncomingLogicalMessageContext.Headers` to share text header changes with downstream behaviors and the handler's `MessageHeaders`. Raw AMQP headers, forwarding, and recoverability use the transport values.
- Logical behaviors share the attempt-local `MessageHandled` flag. Bq sets it when handler invocation succeeds, and a behavior that short-circuits can set it itself.
- Logical behaviors and handlers share the `IMessageProcessingContext` APIs for sending, publishing, replying, and forwarding.
- Behaviors can read the contract name, retry counts, and deferred or saga-timeout flags from the incoming context. Use `FindHeader` or `GetHeader` for text values such as `MessageHeaders.TimeSent`.
- Outgoing behaviors can read `DoNotDeliverBefore` and `DiscardIfNotReceivedBefore` with `Extensions.TryGetDeliveryConstraint` and `GetDeliveryConstraints`. Bq records every delayed send as `DoNotDeliverBefore`, so `DelayDeliveryWith` is never present.

### Message mutators

| Mutator | Register with | What it can edit |
| --- | --- | --- |
| `IMutateIncomingMessages` | `RegisterMessageMutator` | The deserialized message or its text headers. |
| `IMutateIncomingTransportMessages` | `RegisterMessageMutator` | Bytes and text headers before deserialization. Runs after the native `MutateIncoming` callback. |
| `IMutateOutgoingMessages` | `RegisterMessageMutator` | Text headers, or replace the outgoing object before serialization. The replacement must implement the declared contract, which still governs routing. |
| `IMutateOutgoingTransportMessages` | `RegisterMessageMutator` | Serialized bytes and text headers before Bq persists or publishes a new message. Runs after the native `MutateOutgoing` callback. |

Transport mutators can change headers, the contract name, or the body. The outbox stores the outgoing mutator's final contract name, headers, and body. Bq preserves the received wire payload for [recovery and replay](recoverability.md). A replacement for an incoming message must implement the incoming message contract.

### Units of work and transactions

- Register an `IManageUnitsOfWork` factory with `RegisterUnitOfWork`. Units begin before the physical stage and end in reverse order, with the failure passed to `End`.
- Use `configuration.Incoming.BeforeCommit` for application work that must commit with the inbox claim and outbox entries.
- Configure one database per endpoint for both application data and Bq persistence. Pass the supplied connection and transaction to the application data layer.
- An exception from the handler, a callback, or a surrounding behavior rolls back the transaction. If an outer behavior catches a downstream failure and returns, Bq still sends the failure to recoverability.
- `EnableOutbox().TimeToKeepDeduplicationData` and `UnitOfWork().WrapHandlersInATransactionScope` throw `NotSupportedException`. Bq does not clean inbox rows automatically, and a `TransactionScope` would escalate to MSDTC.
- `EnableOutbox()`, `License`, `UniquelyIdentifyRunningInstance` and `DisableInstallers` are accepted.

### Container, logging and settings

- `RegisterComponents`, `UseContainer` and `GetSettings()` follow the NServiceBus shapes. Bq ships a small constructor-injection container, and `UseContainer(IContainer)` plugs in another one.
- Each message attempt gets a child `IBuilder`, available as `Builder` on behavior contexts and disposed when the attempt ends.
- `Headers` holds the NServiceBus header names.
- `LogManager`, `ILog` and `ILoggerFactory` (in `Bq.Messaging.Logging`) match the NServiceBus logging API. They write to `System.Diagnostics.Trace` by default.

### Features, satellites and scheduling

- `Feature`, `FeatureConfigurationContext`, `FeatureStartupTask`, `EnableFeature` and `DisableFeature` follow NServiceBus. Scanning discovers features, defaults apply first, and a feature sets up once its dependencies are active.
- Startup tasks run synchronously inside `RabbitEndpoint.Start` after the consumer is created, and stop in reverse order during `StopAsync`.
- `FeatureConfigurationContext.AddSatelliteReceiver` reads an extra queue with its own delegate and recoverability policy. Delayed retries hold the delivery for the delay and requeue it. Failed messages move to the error queue with failure headers.
- `ScheduleEvery` schedules in-memory recurring tasks through local delayed messages, as in NServiceBus, so a restarted endpoint must schedule them again.
- `ExecuteTheseHandlersFirst` orders scanned handlers.

### Sagas and persistence types

- `SagaMetadata`, `SagaMetadataCollection`, the synchronized storage contracts (`PersistenceSynchronizedStorage`) and the timeout store types exist for code that inspects or implements them.
- Bq does not call custom `IFinder`, `ISagaPersister`, `IPersistTimeouts` or `IQueryTimeouts` implementations, because its persistence provider owns saga and timeout storage.
- The pre-7 `ConfigureHowToFindSaga(IConfigureHowToFindSagaWithMessage)` hook works beside the `SagaPropertyMapper` hook.
- `ErrorContext` and `Transport.OutgoingMessage` construct from transport data.

## API differences from NServiceBus 7

The comparison uses the [NServiceBus 7.8.6 source tag](https://github.com/Particular/NServiceBus/tree/7.8.6). Use Bq’s namespaces when porting code.

- Provide the RabbitMQ URI and persistence provider explicitly. Register routes, handlers, and sagas in the host. See [endpoint configuration](endpoints.md) and [saga registration](sagas.md).
- Use the application's ADO.NET connection and transaction from `SynchronizedStorageSession` for provider-specific work. See [persistence](persistence.md).
- Adapt custom saga finders and container-backed pipeline factories for Bq. See [saga correlation](sagas.md) and [Castle Windsor integration](windsor.md).

## Verify the migration locally

Run `bash scripts/test-rabbitmq.sh` to exercise the following cases with RabbitMQ and SQLite. The [integration test instructions](testing.md) also show how to run the transport-free cases on their own.

| Migration concern | What the tests verify |
| --- | --- |
| Routing and cutover | A send-only producer uses configured conventions to route a command and subscribe to an event. The migrated subscriber receives MessagePack while the simulated legacy consumer receives XML on a separate exchange. |
| Behaviors and application transactions | Each attempt resolves handlers and runs before-commit work in the same host scope. Short-circuiting skips handler resolution. A failed attempt rolls back the inbox claim, application writes, and outbox entries. |
| Send options and metadata | Relative and absolute delays, send and reply headers, publish headers, stable contract names, and incoming retry and saga-timeout metadata survive the expected transport or persistence path. |
| Transport mutation and replay | The outbox stores the mutated wire payload before publishing. Recovery retains the original received bytes so replay can run the incoming mutator again. |
| Recoverability and outages | Policies select immediate retry, delayed retry, or direct error routing. A retry gets a freshly deserialized message. Failed publications stay pending, and the critical-error hook reports a stalled dispatcher or sustained broker outage. |
| Sagas and test doubles | Saga correlation, completion, and durable timeouts run through RabbitMQ. Transport-free tests capture messages and timeout requests with `Bq.Testing`. |

The local fixtures use representative Bq endpoints. Production cutover still requires verification against the consuming application's contracts, topology, and database. The Oracle adapter accepts a host-supplied Devart or ODP.NET connection. Live Oracle and shared EF6 transaction verification require a database and the selected driver.
