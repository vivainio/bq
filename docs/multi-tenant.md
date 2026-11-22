# Multi-tenant endpoints

Some systems keep each tenant's data in its own database, with its own connection string. Bq supports this directly. One endpoint serves every tenant, and each message is handled, and its saga state, inbox and outbox stored, in the database of the tenant that sent it.

This is what NServiceBus does with `MultiTenantConnectionBuilder`. If you are moving from it, see [Coming from NServiceBus](#coming-from-nservicebus).

## At a glance

- Every message carries its tenant in a header, `tenant-id` by default.
- A delivery behavior makes that tenant the **current tenant**, and Bq opens its persistence session on the current tenant's database.
- Everything a handler sends carries the same tenant, including delayed sends and saga timeouts.
- A background dispatcher publishes the outbox rows of all tenants without polling every database every second.
- Work that has no tenant fails. It never falls back to a default database.

```mermaid
sequenceDiagram
    participant B as RabbitMQ
    participant D as Delivery behavior
    participant S as Session and handler
    participant T as Tenant database
    participant P as Dispatcher
    B->>D: delivery with tenant-id: acme
    D->>D: Tenant.Current = acme
    D->>S: next()
    S->>T: claim, saga, your writes, outbox rows (one transaction)
    S-->>P: acme has new rows
    P->>T: read acme's outbox
    P->>B: publish, tenant-id: acme
```

## Setting it up

```csharp
var config = new EndpointConfiguration("orders", new Uri("amqp://guest:guest@broker:5672/"));

var tenants = config.UseTenants(
    // One provider per tenant: any Bq persistence provider over that tenant's connection string.
    tenant => new OraclePersistence(() => OpenConnection(ConnectionStringFor(tenant)), "orders"),
    options =>
    {
        options.HeaderName = "tenant-id";                  // the default
        options.Tenants = () => LoadTenantIdsAsync();      // every tenant ID, from your tenant catalogue
    });

await tenants.Initialize();   // create or upgrade each tenant's tables

config.Handle(() => new PlaceOrderHandler());
using (var endpoint = RabbitEndpoint.Start(config))
{
    // Run until shutdown. Handlers see Tenant.Current, and context.PersistenceSession is the tenant's.
}
```

`UseTenants` sets the endpoint's persistence and registers two behaviors. The provider factory is called once for each tenant, and its result is kept. Each tenant's provider must support recovery, failure tracking and batches, as the SQLite, Oracle and in-memory providers do.

In a handler, `context.PersistenceSession.Connection` and `.Transaction` belong to the current tenant's database, so your own writes commit with the inbox claim and the outbox rows, as with a single database.

### The tenant catalogue

`options.Tenants` returns every tenant ID. It is optional, but you should supply it. It does four things:

- A delivery for a tenant that is not in it is **rejected as unknown**, instead of opening a database for a name nobody has heard of.
- The dispatcher uses it to **find outbox rows left behind by a crash**, in tenants that have had no activity since.
- **Retention cleanup** of old inbox and recovery rows visits every tenant in it.
- `Initialize()` **creates or upgrades each tenant's tables** from it.

The catalogue is cached for `CatalogRefresh` (a minute by default). When a message names a tenant that is not in the cache, it is read again at once, so a tenant created a moment ago works.

Without a catalogue, only tenants that have been active since the endpoint started are looked after in the background, and unknown tenants are not refused. That suits tests and small systems, not production.

## Sending from outside a handler

Inside a handler, the tenant is already set. Elsewhere, such as in a web request, set it with a scope:

```csharp
using (Tenant.Scope(tenantIdFromTheRequest))
{
    await endpoint.Send(new PlaceOrder { OrderId = id });

    await endpoint.ExecuteTransaction(async session =>
    {
        // Your writes on session.Connection and the message commit together, in this tenant's database.
        await session.Send(new OrderPlaced { OrderId = id });
    });
}
```

The scope sets the tenant for the code inside it, including tasks it starts. Keep it as narrow as the work. Do not start long-lived background loops inside one.

To send on behalf of a named tenant without a scope, set the header yourself:

```csharp
var options = new SendOptions();
options.SetHeader("tenant-id", "acme");
await endpoint.Send(new PlaceOrder(), options);
```

A message with no tenant, and no tenant set, is refused when you try to send it, with `TenantNotSetException`. It is never sent for the receiver to reject.

## What happens when something is wrong

| Situation | Result |
| --- | --- |
| A delivery has no tenant header | Rejected: sent to the error queue without opening a database, with `x-bq-failure-message` naming the problem |
| A delivery names a tenant that is not in the catalogue | Rejected the same way |
| Code needs a tenant and none is set | `TenantNotSetException`. It never falls back to a default database |
| A tenant's database is down | That tenant waits and its messages are retried or redelivered. Other tenants are not held up. The dispatcher leaves it alone for `FailureBackoff` (30 seconds) before it looks again |
| Every tenant a poll looked at is down | The failure is raised, so the endpoint's critical-error monitoring sees it |
| A handler fails | As in a single-database endpoint: immediate and delayed retries, then the error queue, recorded in that tenant's database |

A rejected delivery is acknowledged, so it is not redelivered in a loop. It cannot be replayed from a recovery record, because there was no database to hold one. Fix the cause, then move the message out of the error queue by hand. Its headers say which endpoint rejected it and why.

## How the dispatcher keeps up with hundreds of tenants

The dispatcher calls the provider about once a second, as it does for a single database. The tenant provider decides which databases to look at in each call:

- A tenant that has just written outbox rows or recovery records is **hot**, and is looked at first.
- Every other tenant is looked at after a wait that starts at one second and doubles each time it finds nothing, up to `MaxIdleInterval` (30 seconds by default).
- No more than `ProbesPerPoll` tenants (8 by default) are looked at in one call. With hundreds of idle tenants that is a few queries a second per loop, not hundreds.

So in the normal case, a handler that sends a message publishes within about one poll interval of its commit. A row left by a crash waits at most about `MaxIdleInterval`, plus its turn when many tenants are idle at once.

| Option | Default | |
| --- | --- | --- |
| `HeaderName` | `tenant-id` | The header that carries the tenant |
| `Tenants` | none | The catalogue of tenant IDs |
| `CatalogRefresh` | 1 minute | How long the catalogue is kept |
| `MaxIdleInterval` | 30 seconds | The longest an idle tenant waits between looks |
| `ProbesPerPoll` | 8 | How many tenants one poll may look at |
| `FailureBackoff` | 30 seconds | How long a tenant whose database failed is left alone |
| `MaintenanceConcurrency` | 4 | How many tenants `Initialize()` works on at once |

Several processes can run the same endpoint. Each looks after tenants on its own, using the row locking its provider offers, as described in [many processes on one endpoint](persistence.md#many-processes-on-one-endpoint). Extra processes add failover, not publishing speed.

## Inspecting and replaying

Inspection and replay work on one tenant, the current one:

```csharp
using (Tenant.Scope("acme"))
{
    var errors = await endpoint.ListErrors();
    await endpoint.ReplayError(errors[0].RecoveryId);
}
```

Without a tenant they throw `TenantNotSetException`.

## Adding a tenant

1. Create the tenant's database.
2. Call `await tenants.InitializeTenant("acme")` to create its tables.
3. Add it to the catalogue, so it is accepted and looked after.

Bq creates the tables but not the databases, and it does not manage connection strings.

## Connections

Each tenant has its own connection string and so its own connection pool. With hundreds of tenants, set a small maximum pool size and a short idle time in the connection string. Size the database server for the number of tenants that are active at once, not for the total.

## Testing

Use one SQLite file per tenant, and the provider factory picks the file:

```csharp
config.UseTenants(tenant => new SqlitePersistence(() => Connect("tenant-" + tenant + ".db"), "orders"),
    options => options.Tenants = () => Task.FromResult<IReadOnlyList<string>>(new[] { "acme", "globex" }));
```

Then send a message for each tenant and check that each database holds only its own rows. [rabbit-sim](https://vivainio.github.io/rabbit-sim/) runs the broker side, and moves its clock so that delayed messages and timeouts need no waiting. Bq's own tests cover isolation, missing and unknown tenants, a tenant whose database is down, and rows left behind by a crash.

## Limits

- **Every message needs a tenant.** There are no tenant-less messages in a multi-tenant endpoint. A system-wide message needs an endpoint that is not multi-tenant.
- **No ordering across tenants.** Within one tenant, rows are published in the order they were stored.
- **No replay of rejected deliveries**, as described above.
- **Delayed retries after a failed handler** are `bq_recovery` rows that the dispatcher polls per tenant, in the same way as the outbox. Delayed sends and saga timeouts are not: the broker holds them.
- **Without a catalogue**, rows left in a tenant that has had no activity since a restart are not found until the tenant is active again.

## Your own delivery behaviors

The tenant is set by an ordinary behavior on a stage that runs before the persistence session opens, `IDeliveryContext`. You can use the same stage for other ambient state, or to take the tenant from somewhere else, for example a different header:

```csharp
public sealed class TenantFromCustomerHeader : Behavior<IDeliveryContext>
{
    public override async Task Invoke(IDeliveryContext context, Func<Task> next)
    {
        var tenant = context.Headers["x-customer"];
        using (Tenant.Scope(tenant)) await next();
    }
}

config.Pipeline.Register("tenant", new TenantFromCustomerHeader(), "Take the tenant from x-customer");
```

The delivery stage wraps everything Bq does with a delivery: opening the session, claiming the message, the handler, and recording a failure. It has no session of its own, so it cannot write to a database. A behavior that throws before it calls `next` rejects the delivery, and one that returns without calling `next` discards it.

## Coming from NServiceBus

| NServiceBus | Bq |
| --- | --- |
| `MultiTenantConnectionBuilder(tenantIdHeaderName: "tenant_id", ...)` | `UseTenants(providerFor, o => o.HeaderName = "tenant_id")` |
| A pipeline behavior you write to copy the tenant header onto outgoing messages | Done by `UseTenants` |
| Sends outside a handler: not described | `Tenant.Scope(tenant)` |
| Missing tenant: an exception, and the message goes to the error queue | The same, without needing a database |
| Outbox cleanup: impossible, so you disable it and run a job | Retention visits every tenant in the catalogue |
| Installers: not supported, deploy the scripts to each tenant | `Initialize()` and `InitializeTenant(tenant)` |
| The outbox dispatches inline after the commit | A tenant's outbox is looked at straight after its commit, with a slow sweep as a safety net |
| Timeouts in the broker | The same: held by RabbitMQ, see [delayed delivery](delayed-delivery.md) |
