# Design: a database per tenant

!!! warning "Draft for review"
    This page describes a proposal. Bq does not do this today. Nothing here is built, and it is not linked from the book's navigation yet.

## The requirement

- The system serves **hundreds of tenants**, each with **its own database** and connection string.
- The tenant of a message is found in a **message header**.
- The RabbitMQ cluster has **one virtual host**. All tenants share the same queues.
- Sagas, and everything else Bq stores, must live in the tenant's database.

## What Bq does today

An endpoint has one persistence provider, built from one connection factory when the endpoint is configured. The provider's `OpenSession()` takes no arguments, so it never sees the message. The dispatcher polls that one database for outbox rows and recovery records, and nothing wakes it after a commit. See [databases in a migration](databases-in-migration.md).

So today every tenant's data would end up in one database, or you would run one endpoint instance per tenant. With hundreds of tenants the second option means hundreds of consumers and polling loops per process.

## How NServiceBus does it

NServiceBus selects the connection for each message from a header (`MultiTenantConnectionBuilder`). Saga state, outbox rows and business data go to the tenant database in one transaction. A message with no tenant fails to the error queue.

It avoids the scanning problem because it has nothing to scan. The outbox has no poller: outgoing messages are dispatched by the flow that handled the message, after the commit and before the acknowledgment, and the table is only for deduplication and for resending when a failed message is retried. On RabbitMQ, timeouts are held in the broker's delay queues, which are shared by every endpoint and tenant on the virtual host. Bq now does the same with its own `bq.v1` queues. What it cannot do is clean up or install schema for tenants it cannot list, so cleanup is an external job and schema comes from deployment.

## Proposal

### Tenant per message

- A **tenant resolver** reads the tenant from the delivery's headers. A message without a tenant, or with one the system does not know, goes to the error queue with a clear reason. It is never handled against a default database.
- A **connection factory** maps a tenant ID to a database connection.
- Each delivery opens its session on that tenant's database. The inbox claim, saga state, outbox rows and the application's writes commit together there, so the existing atomicity holds for each tenant.
- Sends made from a handler carry the handler's tenant header automatically. Sends from code outside a handler name the tenant explicitly, for example `endpoint.ForTenant(id).Send(...)`.

### Timeouts and delayed messages: already solved

Delayed sends and saga timeouts no longer need a database poll. They are ordinary outbox rows that the dispatcher publishes at once, and the broker holds them until they are due, either in the `bq.v1` delay levels or, on RabbitMQ 4.4 and later, natively. See [delayed delivery](delayed-delivery.md). So the timeout side of the scanning problem is gone: there are no due-time rows in any tenant database to look for. The delay queues are shared on the one virtual host, with nothing per tenant.

What remains to poll is the outbox and the recovery records.

### Publishing the outbox without scanning every tenant

- **Fast path.** After a handler commits, the process publishes that tenant's new outbox rows at once. No polling is involved in the normal case.
- **Slow sweep.** A background sweep covers crashes and anything the fast path missed. It visits tenants in turn with a long interval, a limit on how many it visits at once, and an idle backoff for quiet tenants. One unreachable tenant database must not delay the others.

### Recovery, retention and schema

- Recovery records (delayed retries and the error list) live in the tenant database, and replay takes a tenant. Delayed retries still use `bq_recovery` rows, so they need the slow sweep. Moving them onto the same broker-held delay would remove the last due-time poll.
- Retention cleanup and schema creation or upgrade need the list of tenants. Bq can do them itself if the endpoint can ask the host system for it.

## Costs and risks

- **Polling load.** Today every idle loop polls once a second. Sweeping hundreds of tenants at that rate would be hundreds of queries a second for each process, plus a connection pool for each connection string. The fast path and the idle backoff exist to avoid that, and the sweep must have its own measured budget.
- **A tenant database that is down** must not stall the others, and its backlog must resume when it returns.
- **Many processes.** Several processes of one endpoint would share the sweep. As described in [many processes on one endpoint](persistence.md#many-processes-on-one-endpoint), extra processes add failover, not publishing speed, so tenants need to be spread across them or the sweep claimed per tenant.
- **Ordering** within one tenant is as today. Across tenants nothing is promised.

## Testing

Use several SQLite files, one per tenant, with the simulator. Cover a message for each tenant landing in its own database, a missing and an unknown tenant, one tenant's database failing while the others continue, and a crash between commit and publish. rabbit-sim already holds delayed messages for both delay modes and moves its clock, so timeouts in several tenants can be tested without waiting.

## Open questions

1. **Can the endpoint list every tenant ID,** from a catalogue or a callback? Without it, Bq has the same cleanup and sweep problem as NServiceBus.
2. **Which RabbitMQ version runs in production?** Both delay modes work with tenants. 4.4 or later also allows delayed publishes.
3. **How are edge sends identified?** Code outside a handler needs a way to say which tenant it is sending for.

## Phases

1. Tenant resolution, per-tenant sessions, and handler work in the tenant database.
2. Fast-path dispatch and the slow sweep for the outbox and recovery records.
3. Delayed retries on the broker-held delay, so no due-time polling is left.
4. Per-tenant cleanup and schema setup, and the documentation for NServiceBus migrators.
