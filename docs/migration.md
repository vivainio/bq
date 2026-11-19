# Migrate from NServiceBus in phases

Start with a small group of endpoints whose senders and receivers can switch together. A send-only Bq producer and a subscriber without saga state make a useful first exercise. List the commands, events, subscriptions, and queues each endpoint uses before choosing the group.

Bq uses MessagePack. Give migrated consumers their own queues and use a distinct event exchange for the Bq group while legacy XML consumers remain active. Route commands to the new queues only when their receivers are ready. Before redirecting each flow, account for messages already queued, scheduled deliveries, and unpublished outbox entries. For flows with active sagas, decide how to handle existing state and pending timeouts before switching traffic.

## Port the endpoint

Use the `Bq.Messaging` APIs in the selected C# projects. Bq provides optional `IMessage`, `ICommand`, and `IEvent` markers. The host can use them when discovering handlers and configuring routes or subscriptions. Use [stable contract names](endpoints.md) to keep routing independent of CLR namespace changes.

Register incoming behaviors with `configuration.Incoming.Use` or register a `Behavior<IIncomingLogicalMessageContext>` with `configuration.Pipeline.Register`. Bq executes these steps in registration order. Type registration requires a public parameterless constructor. Container-based behavior factories and other pipeline stages require adaptation. They can share attempt-local state, short-circuit processing, and wrap handler execution. Await `next()` once to continue processing. Use `configuration.Incoming.BeforeCommit` for application work that must commit with the inbox claim and outbox entries. Configure one database per endpoint for both application data and Bq persistence. Pass the supplied connection and transaction to the application data layer. An exception from the handler, callback, or surrounding behavior rolls back the transaction. If an outer behavior catches a downstream failure and returns, Bq still sends the failure to recoverability.

Transport mutators can change headers, the contract name, or the body. The outbox stores the outgoing mutator’s final contract name, headers, and body. Bq preserves the received wire payload for [recovery and replay](recoverability.md). Incoming behaviors can read the contract name, retry counts, and deferred or saga-timeout flags from the incoming context. Use `FindHeader` or `GetHeader` for text values such as `MessageHeaders.TimeSent`.

## API differences from NServiceBus 7

The comparison uses the [NServiceBus 7.8.6 source tag](https://github.com/Particular/NServiceBus/tree/7.8.6). Use Bq’s namespaces when porting code.

| Area | Bq API and remaining differences |
| --- | --- |
| Startup | `Endpoint.Create` returns an `IStartableEndpoint` whose `Start` returns an `IEndpointInstance`. `Endpoint.Start` starts directly. Bq still takes an explicit RabbitMQ URI and persistence provider. |
| Routing and subscriptions | Register routes with `Route<T>` and handlers with `Handle<T>`. Subscribe through handler registration at startup or use `IMessageSession.Subscribe` and `Unsubscribe` at runtime. Runtime calls update RabbitMQ bindings for the registered contract and its aliases. Use `Conventions().DefiningEventsAs(...)` for automatic subscriptions on handlers without an explicit subscription setting. Command and message conventions classify types for host routing, and assembly scanning stays in the host. |
| Sends | Typed `Send(message, options)` supports destination selection, `RouteToThisEndpoint`, headers, scheduling, and immediate dispatch. Sends, publications, and replies can request immediate dispatch. They reach RabbitMQ outside the transaction and can be repeated by a handler retry. Replies can also select an explicit destination. The object-based `IMessageSession` overloads resolve the contract from the concrete message type. Message-initializer overloads accept classes with a public parameterless constructor and public interfaces with read/write data properties. Interface contracts use their declared type for routing and wire names. |
| Endpoint tuning | Use `LimitMessageProcessingConcurrencyTo`, `SendFailedMessagesTo`, and `Recoverability().Immediate(...).Delayed(...)` for familiar configuration calls. Use `CustomPolicy((config, error) => ...)` for immediate or delayed retry, discard, and per-message error queues. Use `AddUnrecoverableException<T>()` to classify failures in the default policy. A custom policy can delegate to `DefaultRecoverabilityPolicy.Invoke`. Bq also retains its native `Policy` callback. A custom error queue must already exist. Use `DefineCriticalErrorAction` for an infrastructure failure callback that can stop the endpoint through its context. |
| Handler context | Bq provides the string-valued `MessageHeaders` view and `ReplyToAddress`. Use `Headers` for raw AMQP values and `PersistenceSession` for the shared transaction. Use the typed `Extensions` bag to share state between an incoming behavior, handler, and before-commit callbacks. Each retry gets a fresh bag. `ForwardCurrentMessageTo` queues the original wire body, contract, headers, and message ID in the outbox for another queue. Bq’s `SynchronizedStorageSession` exposes the same connection and transaction as `PersistenceSession`. Replace provider-specific NServiceBus storage extensions with those ADO.NET objects. |
| Sagas | Override `ConfigureHowToFindSaga` and use `MapSaga(...).ToMessage<T>(...)` or `ConfigureMapping<T>(...).ToSaga(...)`. Register the saga with `RegisterSaga<TSaga, TData>()` or supply a factory. Bq uses one scalar correlation property. Use `ToMessageHeader<T>` or `ConfigureHeaderMapping<T>` to read the key from a text header. Use `ContainSagaData` or implement `IContainSagaData` to store saga ID and originator metadata for `ReplyToOriginator`. Existing records need their originator metadata populated before using that API. Custom finders require separate adaptation. |
| Castle Windsor | Use the optional `Bq.Extensions.CastleWindsor` package to attach the existing container. Register handlers explicitly. A scope covers handler resolution and before-commit callbacks. |

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
