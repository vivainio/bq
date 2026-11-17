# Bq messaging prototype

RabbitMQ endpoints with contractless MessagePack messages, targeting .NET Standard 2.0 and usable from .NET Framework 4.7.2 / Mono. This is an early Bq API inspired by NServiceBus, not a replacement for its assemblies or its RabbitMQ wire protocol. The older database and Redis projects remain separate.

```csharp
using System;
using System.Threading.Tasks;
using Bq.Messaging;

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
using (var endpoint = RabbitEndpoint.Start(config))
{
    // Keep the process running until shutdown.
}
```

Register event handlers with `config.Handle(() => new EventHandler(), subscribe: true)`. Configure command destinations with `config.Route<PlaceOrder>("orders")`, then call `await endpoint.Send(command)`; `endpoint.Publish(evt)` broadcasts to registered subscribers. Handlers can send, publish and reply through their context. Message types use their full .NET type name on the wire; receivers must explicitly register each type. Renaming types or properties changes the contract. Avoid polymorphic `object` properties; test schema changes with both versions.

The endpoint declares a durable queue named after the endpoint, an error queue named `<endpoint>.error`, and durable `bq.events` / `bq.errors` exchanges. Multiple instances with the same endpoint name compete for messages and should use the same registrations. A send requires an existing destination queue; unroutable commands and failed publisher confirms throw. Published events without subscribers are discarded. Messages are persistent and acknowledged after the handler finishes. Exceptions are retried immediately twice by default, then dead-lettered to the error queue. Retry counts are in-memory per delivery and reset after redelivery. The error queue needs operational monitoring and a replay process.

Delivery is at least once, not exactly once. Without persistence, outgoing messages are published immediately, including during failed attempts, and are not atomic with handler side effects or the incoming acknowledgment. Make handlers idempotent. `Dispose` closes the connection; in-flight messages may be redelivered. This prototype does not yet provide delayed retries, automatic saga correlation and timeouts, graceful draining, or full NServiceBus API/wire compatibility. Broker queues and exchanges persist after shutdown.

## Tests

```sh
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj
mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
BQ_RABBITMQ_URI=amqp://guest:guest@localhost:5672/ mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
```

The last command also exercises command delivery, replies, event fan-out, and error routing against a local RabbitMQ broker. For example, with Podman:

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

A handler can persist saga state explicitly in that transaction:

```csharp
var session = context.PersistenceSession;
var snapshot = await session.LoadSaga<OrderState>(orderId);
if (snapshot == null)
    await session.SaveSaga(orderId, new OrderState { Status = "Started" });
else
{
    snapshot.Data.Status = "Completed";
    await session.SaveSaga(orderId, snapshot.Data, snapshot.Version);
}
// Completion: await session.DeleteSaga<OrderState>(orderId, snapshot.Version);
```

A missing expected version means insert; updates and deletes require the previously loaded version. Conflicts throw `SagaConcurrencyException` and roll back the transaction. Saga type names, IDs, and MessagePack property names are persisted contracts. This is a state-store primitive, not a full NServiceBus saga API: handlers supply correlation and lifecycle, and durable saga timeouts are not implemented.

The inbox currently has no retention cleanup, and the outbox has no operator tools or dead-letter policy: a persistently unroutable command blocks later records for that endpoint until fixed. The SQLite provider is a first implementation intended for local or low-concurrency use, not a claim that SQLite supports distributed multi-host deployment. Run the provider against the exact filesystem, SQLite driver and Mono version used in production. Handler external effects, other databases, and sends made outside the handler context are not part of the transaction. `PersistenceSession` is null when persistence is disabled.
