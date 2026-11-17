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

Delivery is at least once, not exactly once. Outgoing messages are published immediately, including during failed attempts, and are not atomic with handler side effects or the incoming acknowledgment. Make handlers idempotent. `Dispose` closes the connection; in-flight messages may be redelivered. This prototype does not yet provide delayed retries, transactional outbox, sagas, graceful draining, or full NServiceBus API/wire compatibility. Broker queues and exchanges persist after shutdown.

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
