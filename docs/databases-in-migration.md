# Databases in a migration

Both stacks keep state in a database, and they keep it differently. This page lays the two side by side, shows where NServiceBus stores each thing, and says what to check before you move a flow. The Bq tables are described in full in the [database schema](schema.md).

NServiceBus has several persisters, and the details below come from its documentation, linked at each point. They change between versions, so check the pages for the version you run.

## Side by side

| What | NServiceBus | Bq |
| --- | --- | --- |
| Saga state | One table per saga in SQL Persistence, with JSON data. Other persisters use their own stores | One table, `bq_saga`, with MessagePack data |
| Saga timeouts | With the RabbitMQ transport, in the broker's delayed-delivery queues. SQL Persistence 7.x also has a legacy `TimeoutData` table | `bq_saga_timeout`, polled by the endpoint's dispatcher |
| Outbox | An `OutboxData` table with the endpoint name as a prefix, when the outbox is enabled | `bq_outbox`, always on |
| Duplicate detection | The outbox table, when the outbox is enabled | `bq_inbox`, always on |
| Subscriptions | Stored only when the transport has no native publish-subscribe. RabbitMQ has it, so nothing is stored | Bindings on the event exchange, `bq.events` by default. No table |
| Failed messages | The error queue in the broker | `bq_recovery`, plus the error queue |

## Where NServiceBus keeps things

### Sagas

NServiceBus lists these persisters: SQL (SQL Server, MySQL, PostgreSQL, Oracle), Cosmos DB, DynamoDB, MongoDB, Azure Table, NHibernate, RavenDB and the Learning persister for development. See its [persistence overview](https://docs.particular.net/persistence/). Which one you use decides where to look. The rest of this section is SQL Persistence, the most common relational choice.

SQL Persistence gives each saga its own table. The name is the endpoint's table prefix plus the saga's class name, or a suffix set with `[SqlSaga(tableSuffix: ...)]`. The columns, from the [saga persister page](https://docs.particular.net/persistence/sql/saga):

| Column | Holds |
| --- | --- |
| `ID` | The saga ID, the primary key |
| `Metadata` | A JSON dictionary of information NServiceBus manages about the saga |
| `Data` | The saga data as JSON, written with Newtonsoft.Json |
| `PersistenceVersion` | The version of the SQL persister |
| `SagaTypeVersion` | The version of the assembly the saga is in |
| `Correlation_<Property>` | Zero to two columns, one for each mapped correlation property, each with a unique index |

The [SQL Server scripts](https://docs.particular.net/persistence/sql/sqlserver-scripts) also show a `Concurrency` column that gives optimistic concurrency. Check the scripts for your version.

Two differences matter for a migration. NServiceBus stores the saga ID as its key and the correlation value in its own column, while Bq's key is the correlation value as a string. NServiceBus writes JSON and Bq writes MessagePack, so the data cannot be copied as it is.

### Timeouts and delayed messages

With the RabbitMQ transport, delayed delivery lives in the broker, not in a database. The transport declares a set of exchanges and queues that give 28 delay levels, and a delayed message waits in them until it is due, then goes to the `nsb.v2.delay-delivery` exchange and on to its endpoint. See the [delayed delivery page](https://docs.particular.net/transports/rabbitmq/delayed-delivery). Saga timeouts and delayed retries use it.

So an NServiceBus saga's pending timeouts are messages sitting in broker queues. You cannot select them with SQL, and Bq cannot take them over. Older setups used the SQL persister's `TimeoutData` table instead; the docs call it legacy and describe [moving to native delivery](https://docs.particular.net/nservicebus/tools/migrate-to-native-delivery).

### Outbox and subscriptions

When the outbox is enabled, SQL Persistence keeps an `OutboxData` table with the endpoint name as a prefix. It holds the message ID, a dispatched flag and time, and the serialized outgoing operations. See [outbox with SQL Persistence](https://docs.particular.net/persistence/sql/outbox). The docs say SQL Persistence cannot clean outbox entries automatically in a multi-tenant setup, so an old outbox there can grow.

NServiceBus needs subscription storage only for transports without native publish-subscribe. RabbitMQ has it, so there is no subscription table to migrate.

### How Bq's timeouts differ

Bq has nothing like the delay queues. It does not declare a delay exchange or delay queues, and it does not read or honour NServiceBus's. A delayed send, a saga timeout and a delayed retry are each a row with a due time: in `bq_saga_timeout` for sends and timeouts, and in `bq_recovery` for retries. The endpoint's dispatcher polls for rows that are due and publishes them to the real queue. The broker never holds a delayed message.

That changes how timeouts behave:

| | NServiceBus on RabbitMQ | Bq |
| --- | --- | --- |
| Where a pending timeout waits | In the broker's delay queues | In the endpoint's database |
| Needs the database to fire | No. The broker releases it | Yes. The dispatcher reads the row |
| Needs the endpoint running to fire | No. The broker releases it to the queue, and a consumer picks it up when one is running | Yes. Only a running dispatcher publishes due rows |
| Created with the handler's work | Sent by the saga, like any message | Written in the handler's own transaction, so it exists only if the work committed |
| After an outage | Overdue delayed messages are released as the broker catches up | Overdue rows are published when the dispatcher is back, so they arrive late, not lost |
| Where to see them | The delay queues in the management UI | `SELECT ... FROM bq_saga_timeout` |
| Timing | Set by its delay levels. See its docs for how exact that is | Polling, so it fires after the due time, by up to the dispatch poll interval (one second by default), and later when the dispatcher is blocked |

Two things follow for a migration. A saga that moves to Bq depends on its database and its dispatcher for every timeout, so a stopped endpoint stops its timeouts, where NServiceBus's would still be in the broker. And a timeout that exists only in the NServiceBus delay queues is invisible to Bq. See [sagas and timeouts](sagas.md) for how Bq delivers and discards them.

## Where Bq keeps things

Everything is in the endpoint's database, in tables whose names start with `bq_`: `bq_inbox`, `bq_outbox`, `bq_recovery`, `bq_saga` and `bq_saga_timeout`. The [schema page](schema.md) has the columns, indexes and the differences between SQLite and Oracle. The names do not collide with NServiceBus's tables, so the two can share a database, though a separate schema or database for the Bq group keeps the blast radius smaller.

Bq's outbox, inbox, saga state and your application's writes share one database transaction. That is why a handler's sends must come from the same stack as its data. See [never send a Bq message from an NServiceBus handler](gradual-migration.md#never-send-a-bq-message-from-an-nservicebus-handler).

## Before you move a flow

Count what is in flight on the NServiceBus side. The table names depend on your endpoint's prefix, and the column names follow the 7.x scripts, so treat these as examples and check them against your own schema:

```sql
-- Saga instances still running (one table per saga)
SELECT COUNT(*) FROM [dbo].[OrdersOrderSaga];

-- Outbox rows not yet dispatched
SELECT COUNT(*) FROM [dbo].[OrdersOutboxData] WHERE [Dispatched] = 0;
```

For pending timeouts and delayed retries, look at the delay queues in the RabbitMQ management UI, since that is where they are. An empty set of delay queues for the endpoint means no timeouts are waiting.

After the cutover, watch the Bq side the same way:

```sql
SELECT COUNT(*) FROM bq_outbox   WHERE endpoint = 'orders-bq';              -- unpublished
SELECT COUNT(*) FROM bq_saga     WHERE endpoint = 'orders-bq';              -- running sagas
SELECT COUNT(*) FROM bq_recovery WHERE endpoint = 'orders-bq' AND is_error = 1 AND replayed = 0;  -- failures
```

## Copying saga state

[Moving a saga by copying its state](gradual-migration.md#sagas) means reading the NServiceBus rows and writing `bq_saga` rows through Bq, not by raw SQL. The persistence session has `SaveSaga`, which writes the row with the right version. This is a sketch, not a tested tool:

```csharp
// One-off. Reads the NServiceBus saga table and writes Bq saga rows.
var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
using (var session = await bqStore.OpenSession())
{
    foreach (var (id, json) in ReadLegacyRows())   // ID and Data columns of the NServiceBus table
    {
        var legacy = JsonConvert.DeserializeObject<OrderState>(json);
        var key = legacy.OrderId;                  // the correlation value, as Bq's selector returns it
        var bytes = MessagePackSerializer.Serialize(legacy, options);
        await session.SaveSaga(typeof(OrderSaga).FullName, key, bytes, expectedVersion: null);
    }
    session.Commit();
}
```

Check three things before you trust it:

- **The saga type.** Bq stores the saga class's full name in `saga_type`. Let a Bq saga create one instance in a test, and compare its `saga_type` with what the script writes.
- **The key.** It must be exactly the string the saga's correlation selector returns for that instance, in the same format. A different format starts a second instance instead of continuing the first.
- **The data.** Bq reads MessagePack property names, which are a persisted contract. Rehearse on a copy and load a few copied instances through the real saga before cutting over.

This copies saga rows only. It does not move the timeouts, which are broker messages. Let them fire on the NServiceBus side, or request them again from the Bq saga, and make sure a timeout that fires after the copy cannot act on the same instance twice.
