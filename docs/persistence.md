# Persistence and outbox

The tables and columns are described in [database schema](schema.md).

`Bq.Persistence.Sqlite` is the first persistence provider. It accepts a factory for an ADO.NET SQLite connection, so the host chooses a compatible driver (the Mono tests use `Mono.Data.Sqlite`). Use a file-backed database shared by instances of the same endpoint, and initialize the schema before starting the endpoint:

```csharp
var store = new SqlitePersistence(() => CreateSqliteConnection(), "orders");
await store.Initialize();
config.Persistence = store;
using (var endpoint = RabbitEndpoint.Start(config)) { /* host the endpoint */ }
```

Within a handler, `context.PersistenceSession.Connection` and `.Transaction` are the same connection and local transaction used by the inbox, saga and outbox stores. Application commands must use both. Bq records the incoming message ID in a unique inbox key, invokes the handler, records context-generated sends/publishes/replies in the outbox, commits, and then acknowledges RabbitMQ. On failure the transaction is disposed without commit. Each immediate retry creates a new session and transaction. A dispatcher polls committed outbox records; it deletes a record only after a broker confirmation. Publisher-confirm uncertainty and multiple dispatchers can produce duplicates, with the same outgoing message ID.

## Transactions outside handlers

Call `endpoint.ExecuteTransaction` from an application request or background job to commit SQL changes and outgoing messages together.

```csharp
await endpoint.ExecuteTransaction(async session =>
{
    await orders.SaveAsync(order, session.Connection, session.Transaction);
    await session.Send("billing", new OrderPlaced { OrderId = order.Id });
});
```

Bq commits when the callback returns successfully and rolls back if it throws. Use the supplied connection and transaction on every participating command. Await operations before leaving the callback and use its session for messaging. Calls on the endpoint itself publish directly. A successful return means Bq committed the database transaction. Return a value from the callback to use the generic overload and receive that value after commit. The dispatcher publishes the message separately.

Use the handler context for work that already runs inside a handler. A transactional session cannot be nested. Graceful shutdown waits for active application transactions. Pass a cancellation token to `ExecuteTransaction` to roll back when request cancellation is observed before commit. Use the session’s `CancellationToken` for cancellable work inside the callback. It also signals forced endpoint shutdown. A commit already in progress may still succeed.

## Message bodies and payload references

The outbox stores serialized message bodies and AMQP headers so handlers can commit their outgoing messages with their business changes. The dispatcher publishes the body, headers, routing and correlation metadata to RabbitMQ. After publisher confirmation, SQLite can delete the staging row. A receiver can then consume the body even when the sender or its database is offline.

For a large payload, your application can store the payload separately and send a small message containing a stable reference. Make the payload durable before the reference can be dispatched. Give consumers access to the referenced version and keep it available for the full lifetime of queued deliveries, retries, and operator replays. A missing payload becomes a handler failure. With event fan-out, one subscriber finishing does not imply that all subscribers are done.

Bq serializes the reference like any other message property. Your application owns payload storage, access, and cleanup. Pointing a message at its outbox row would only work if you changed the row's lifetime and gave receivers access to the sender's database.

### When both endpoints share a database

A shared database can hold an immutable message body and one delivery row per receiving endpoint. The sender inserts both alongside its business writes in the same transaction. Each receiver claims its own delivery and commits completion with its handler writes. If a receiver crashes before committing, the delivery remains available for another attempt.

In that design, RabbitMQ can send an ID as a wake-up signal. Periodic database polling still discovers committed deliveries when a notification is missed. If multiple endpoints consume a message, track their delivery states separately and keep the body until all deliveries finish and any replay window ends.

This is an alternative transport design. Sharing a database in the current RabbitMQ transport still uses the same outbox and publish flow at the top of this section.

## Many processes on one endpoint

!!! note "In short"
    You can run many processes of the same endpoint against one database, and nothing is lost or handled twice. But **extra processes do not publish faster**. They add failover and **extra polling load on the database**. To publish faster, raise `OutboxBatchSize`; do not add processes.

Every process that runs an endpoint polls the same rows. The rows belong to the endpoint's name, so processes of one endpoint share them, and endpoints with different names never touch each other's rows.

### What each process does

- It runs an **outbox loop** that publishes ready outbox rows and due saga timeouts. A send-only endpoint runs only this loop.
- Unless it is send-only, it also runs a **recovery loop** for delayed retries and failed messages.
- When a loop finds a row it publishes it and looks again at once. When it finds nothing, or fails, it waits `DispatchPollInterval` (one second by default).

### How a row is claimed

| | SQLite | Oracle |
| --- | --- | --- |
| Claiming a row | None. A process reads the first ready row and marks it dispatched afterwards | The row is locked with `FOR UPDATE SKIP LOCKED` while it is published, and removed after the broker confirms |
| Two processes, one row | Both may publish it. The message has the same ID both times, and the inbox keeps handling to once | The second process does not take a row that is locked. It gets nothing and waits for the next poll |
| A process dies while publishing | The row is still pending, so the next poll publishes it | The lock is released with the transaction, so another process publishes the row |
| A process dies after the confirm, before the row is removed | The row is published again | The row is published again |
| Meant for | One process, or a few on a developer machine | Production, with several processes |

In every case the worst outcome is a duplicate publish with the original message ID, never a lost message. Receivers deduplicate through the inbox, so handling still happens once.

### What more processes cost

- **Polling.** Every idle loop queries the database once per `DispatchPollInterval`. On Oracle a poll can run two queries, one for outbox rows and one for scheduled rows. They use indexes (`bq_outbox_ready_ix` on `(endpoint, enqueue_order)` and `bq_saga_timeout_due_ix` on `(endpoint, due_ticks, id)`), so each is cheap. But the load is linear in the number of processes and has no backoff. Twenty processes with the defaults run forty loops, which is roughly forty to sixty small queries a second while idle.
- **No extra throughput.** A dispatcher takes the first ready row, or the first `OutboxBatchSize` rows, in order. If another process holds those rows, the second process finds them locked, gets nothing, and waits for the next poll. So for one endpoint, publishing is in effect done by one process at a time. This follows from how the query is written. It has not been load-tested.
- **Ordering.** Rows are published in the order they were stored by one dispatcher. With several dispatchers Bq does not promise that order across processes, so do not depend on it. Receivers that need order should carry their own sequence.
- **Timeouts.** A due timeout is published by whichever process polls it first, up to one poll interval after it falls due.

### What to do about it

- To publish a large backlog faster, raise `OutboxBatchSize` (100 by default). One process confirms a whole batch with a single broker round trip.
- To cut polling, raise `DispatchPollInterval`. A longer interval delays pickup of new rows and timeouts by up to that long.
- Use as few processes per endpoint as you need for failover and for running handlers. Handler throughput is separate: RabbitMQ shares one queue between all the processes consuming it, so more processes do process more messages at a time. Only the publishing of the outbox is single-file.
- Do not point many processes at a SQLite file. SQLite allows one writer at a time, so they would compete for it.

## Dispatch inspection

`config.DispatchPollInterval` sets the wait after an empty or failed dispatch poll. It defaults to one second, and both the outbox and recovery loops use it. Shorter intervals reduce idle pickup latency and increase database polling. `config.PublisherConfirmTimeout` controls how long direct sends and both background dispatchers wait for publisher confirmation. It defaults to ten seconds. After a failed confirmation, durable records remain pending for another attempt.

Use `await store.GetDispatchBacklog()` to count pending outbox messages, due and scheduled saga timeouts, and due and scheduled recovery records for the SQLite endpoint. Counts include records awaiting publisher confirmation. Use `await store.ListPendingDispatches(limit: 100)` to inspect message IDs, dispatch kinds, exchanges, routing keys, and scheduled UTC times. Pass a `DispatchKind` such as `DispatchKind.Error` as the first argument to filter before applying the limit. The listing omits message bodies and its order is for inspection.

## Retention

An endpoint removes the records that only make delivery safe once they are old, so the tables do not grow without end. `config.Retention` sets how long each kind is kept:

```csharp
config.Retention.InboxRetention = TimeSpan.FromDays(14);    // default 30 days, null keeps every row
config.Retention.RecoveryRetention = TimeSpan.FromDays(7);  // default 30 days, null keeps every row
config.Retention.CleanupInterval = TimeSpan.FromHours(1);   // the default
```

- **Inbox rows** are removed when they are older than `InboxRetention`. The inbox is what stops a redelivered message from running twice, so keep it longer than any redelivery can take. A message that is redelivered after its inbox row is gone runs again.
- **Recovery records** are removed when they are older than `RecoveryRetention` and finished: a retry that was sent, or a failed message that was replayed. A failed message nobody has replayed is never removed.
- The cleanup runs in the background, in batches of 5,000 rows, first about a minute after start and then every `CleanupInterval`. A failure is logged and tried again on the next round. Outbox rows and scheduled rows leave the tables when they are published, so they need no cleanup.

Delivery behavior of the outbox, each of which a test checks:

- Rows are published in the order they were stored. A row that cannot be published because of the message itself, such as a mandatory message no queue accepts, is deferred: it stays pending with its attempt count and reason, and the rows behind it are sent. It is retried after a delay that starts at 100 milliseconds and doubles up to 30 seconds, and it is never dropped. A queue that appears a moment later is found quickly, and a row that never succeeds is tried every 30 seconds. A broker outage is not blamed on any row, so rows keep their place while the broker is away.
- Delivery is at least once. If the endpoint stops after publishing and before it records the dispatch, the row is published again with the same message ID, and the receiver's inbox keeps handling to once.
- Several endpoint instances can dispatch from one database. With SQLite two dispatchers may each publish the same row, and the inbox keeps each message to one handling. The Oracle provider locks the row while it publishes, so two dispatchers do not publish it at the same time. A crash after the broker confirms but before the row is removed can still publish it again, which the inbox absorbs. See [many processes on one endpoint](#many-processes-on-one-endpoint) for what extra processes do and do not give you.
- The dispatcher publishes up to `config.OutboxBatchSize` ready rows (100 by default) and waits for one broker confirm for the whole batch, then removes the delivered rows together. A real broker confirms durable messages one batch at a time, so a backlog of 1,500 rows drains in a few seconds, where one confirm per row took over a minute. The rows keep their order. If the broker does not confirm a batch, every row of it stays pending and may be published again, which the inbox absorbs. Set `OutboxBatchSize` to 1 to confirm each row on its own. A provider that does not support batches always works that way.
- A backlog of thousands of rows drains completely, and delivered rows leave the outbox.

The outbox has no dead-letter policy: a persistently unroutable command is retried every 30 seconds for as long as it exists, and `ListPendingDispatches` shows its `Attempts`, `LastError` and `RetryAfterUtc`. The SQLite dispatcher alternates preference between ordinary outbox messages and due saga timeouts on each poll. The SQLite provider is a first implementation intended for local or low-concurrency use, not a claim that SQLite supports distributed multi-host deployment. Run the provider against the exact filesystem, SQLite driver and Mono version used in production. Handler external effects, other databases, and sends made outside the handler context are not part of the transaction.

Use `await store.ListReadyDispatches()` to inspect rows eligible for dispatch now. It accepts the same kind filter and limit. A row stays visible until Bq records confirmation, so the list can include a publish in flight.

## NServiceBus-style configuration

`config.UsePersistence<SqlitePersistenceDefinition>().ConnectionFactory(() => connection)` creates the provider when the endpoint starts, unless `config.Persistence` is already set. `UsePersistence<T, S>()` with a `StorageType` is accepted, but Bq keeps the inbox, outbox, sagas, timeouts and recovery records in one provider, so selecting a different definition twice throws. `EnableInstallers()` runs the schema creation of providers implementing `IInstallablePersistence` (SQLite does) at start. Oracle schemas stay under the host's migration scripts.

## In-memory persistence

`config.UsePersistence<InMemoryPersistence>()` keeps the inbox, outbox, timeouts, sagas and recovery records in memory, with the same commit and rollback behavior as the database providers: writes are staged in the session, claims deduplicate across sessions, and saga versions are checked. Nothing survives a restart, and sessions have no database `Connection` or `Transaction`, so use it for tests and local runs only.
