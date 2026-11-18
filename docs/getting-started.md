# Getting started

Run the [Castle Windsor example](windsor.md) for a complete RabbitMQ and SQLite workflow on Mono. The following wiring shows the same endpoint without a dependency injection container.

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
