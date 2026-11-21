# What Bq leaves out, and why

Bq follows the NServiceBus API wherever application code touches it: handlers, sagas, behaviors, options, features, conventions, configuration sections and the testing surface. It leaves some NServiceBus parts out on purpose. A part is left out when it exists to serve infrastructure that Bq does not have, or when Bq's own design makes it unnecessary. This page lists each omission, the reason, and what to use instead. Where a call exists but cannot work, it throws `NotSupportedException` when you configure the endpoint, so you find out at startup and not in production.

## Local development without a learning transport

NServiceBus ships a learning transport and learning persistence so that you can run an endpoint on a laptop with no broker and no database. Bq does not need either, because the two pieces they replace are cheap to run:

- **The broker.** [rabbit-sim](https://github.com/vivainio/rabbit-sim) is a single-binary AMQP 0-9-1 broker for development and tests. Bq talks to it exactly as it talks to RabbitMQ, so the code you run locally is the code you ship, with no second transport to keep in step.
- **The database.** `UsePersistence<InMemoryPersistence>()` keeps the inbox, outbox, timeouts, sagas and recovery records in memory, with the same commit and rollback behavior as the database providers. For something that survives a restart, use the SQLite provider with a file.

```sh
pip install rabbit-sim          # or: pipx install rabbit-sim
rabbit-sim                      # amqp://guest:guest@127.0.0.1:5672/, management UI on :15672
rabbit-sim --data broker.db     # keep durable queues and messages across restarts
```

```csharp
var configuration = new EndpointConfiguration("sales", new Uri("amqp://guest:guest@127.0.0.1:5672/"));
configuration.UsePersistence<InMemoryPersistence>();
```

The full Bq test suite runs on rabbit-sim, including broker outage and restart. rabbit-sim is a simulator, so run a final check against a real RabbitMQ broker before a release. The [integration tests page](testing.md) shows how to do both.

## Omitted by design

| NServiceBus part | Why Bq leaves it out | Use instead |
| --- | --- | --- |
| Learning transport and learning persistence | Replaced by rabbit-sim and the in-memory or SQLite providers, as above | `rabbit-sim`, `InMemoryPersistence`, SQLite |
| MSMQ transport, MSMQ subscription storage and its configuration section | MSMQ is a Windows-only queue. Bq uses RabbitMQ only | RabbitMQ topic exchange, which Bq uses for events |
| Other transports and the transport infrastructure types (`TransportDefinition.Initialize`, `IPushMessages`, `IDispatchMessages`, `TransportInfrastructure`, `QueueBindings` and related types) | These exist so a third party can write a transport. Bq has one transport, so there is nothing to plug in | `UseTransport<RabbitMQTransport>()`, which accepts a connection string, the circuit breaker time and `ReceiveOnly` |
| Gateway and its deduplication persistence | A site-to-site HTTP bridge with its own dedupe store. Bq's inbox already deduplicates every delivery | RabbitMQ federation or shovel between sites, or an HTTP endpoint that sends a Bq message |
| Routing tables and distribution (`EndpointInstances`, `UnicastRoutingTable`, `DistributionStrategy`, `EndpointInstance`) | RabbitMQ's shared queue already gives competing consumers, and static routes cover the rest | `Route<T>`, `MakeInstanceUniquelyAddressable` and the instance routing options |
| Pipeline connectors (`StageConnector`, `ForkConnector`, `PipelineTerminator`) and the dispatch, routing, audit, subscribe and forwarding contexts | Connectors rewire NServiceBus's stage graph. Bq's stages are fixed, so there is no graph to rewire | Behaviors at the stages Bq exposes, listed below |
| The timeout manager, `UseExternalTimeoutManager` and the NServiceBus timeout persister contracts | Bq stores delayed messages in its persistence provider and has no timeout manager endpoint | `DelayDeliveryWith`, `DoNotDeliverBefore`, saga `RequestTimeout` and `ScheduleEvery` |
| Custom saga finders and persisters (`IFinder`, `IFindSagas<T>`, `ISagaPersister`, `IPersistTimeouts`) | Bq finds a saga by one correlation key and its persistence provider owns the storage. The types exist for code that inspects them, and Bq never calls an implementation | Map messages with `ConfigureHowToFindSaga`, or write an `IPersistenceProvider` |
| XML and JSON serialization (`UseSerialization<XmlSerializer>()`) | The wire format is MessagePack, which keeps contracts and the outbox simple | `UseSerialization<MessagePackSerialization>()`, which is the default |
| `TransactionScope` units of work and the transport transaction modes other than `ReceiveOnly` | A transaction scope would enlist a second resource manager and escalate to MSDTC. The outbox gives atomic sends without it | `PersistenceSession`, `ITransactionalSession` and `Incoming.BeforeCommit`, which share one database transaction |
| Performance counters, the SLA and critical time monitors, and the platform retry notifications | They report to ServiceControl and Windows counters, which Bq does not use | `Notifications.Errors` events, `OnCriticalError`, and your own metrics around the pipeline behaviors |
| Configuration sections Bq cannot honor: `MsmqSubscriptionStorageConfig`, `UnicastBusConfig.TimeoutManagerAddress` | They configure the parts above | Configure the endpoint in code |

## What Bq does provide

Some areas that look like omissions are implemented, with the NServiceBus names. `UnicastBusConfig`, `MessageForwardingInCaseOfFaultConfig`, `AuditConfig`, `Logging` and `RijndaelEncryptionServiceConfig` are real configuration sections read from your configuration file. The data bus works through `DataBusProperty<T>` and `FileShareDataBus`. Feature classes, satellite receivers, `INeedInitialization`, externally managed container endpoints and the `IBuilder` container API are all there. The [phased migration page](migration.md) lists them and the small differences.

## Pipeline stages you can extend

You extend Bq at fixed stages, with the NServiceBus context interfaces. Register a `Behavior<TContext>` for any of them with `configuration.Pipeline.Register`.

| Stage | Context interface | Runs |
| --- | --- | --- |
| Transport receive | `ITransportReceiveContext` | Around each delivery attempt, and can abort it |
| Incoming physical | `IIncomingPhysicalMessageContext` | Before transport mutators and deserialization |
| Incoming logical | `IIncomingLogicalMessageContext` | After deserialization, before handlers |
| Handler invocation | `IInvokeHandlerContext` | Around each handler |
| Outgoing send, publish, reply | `IOutgoingSendContext`, `IOutgoingPublishContext`, `IOutgoingReplyContext` | Before a message of that kind is prepared |
| Outgoing logical | `IOutgoingLogicalMessageContext` | Around message mutators and serialization |
| Outgoing physical | `IOutgoingPhysicalMessageContext` | Around transport mutators |

## If you depend on something omitted

Check whether the dependence is on the feature or on the infrastructure. Most applications that use the gateway, a custom transport or distribution tables use them to solve a problem that RabbitMQ solves directly. If you hit a case where that is not true, open an issue with the use case. Bq adds a part when it can do it without importing infrastructure it does not have.
