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

## Virtual hosts

The path of the AMQP URI is the virtual host, as with any RabbitMQ client: `amqp://guest:guest@localhost:5672/` uses the default `/`, and `amqp://guest:guest@localhost:5672/tenant-a` uses `tenant-a`. The `virtualhost=tenant-a` key of the connection-string form means the same. An endpoint's queues and exchanges live in its own virtual host, so two endpoints with the same name on different virtual hosts are separate, and so are their messages. Bq does not create virtual hosts: on RabbitMQ, create the virtual host and give the user permissions on it first. rabbit-sim 0.4.1 and later creates a virtual host when a client first opens it.
