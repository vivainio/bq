# Databases in a migration

Both stacks keep state in a database, and they keep it differently. This page lays the two side by side, shows where NServiceBus stores each thing, and says what to check before you move a flow. The Bq tables are described in full in the [database schema](schema.md).

NServiceBus has several persisters, and the details below come from its documentation, linked at each point. They change between versions, so check the pages for the version you run.

## Side by side

| What | NServiceBus | Bq |
| --- | --- | --- |
| Saga state | One table per saga in SQL Persistence, with JSON data. Other persisters use their own stores | One table, `bq_saga`, with MessagePack data |
| Saga timeouts | With the RabbitMQ transport, in the broker's delayed-delivery queues. SQL Persistence 7.x also has a legacy `TimeoutData` table | An outbox row that the broker holds until it is due, in delay levels or natively. See [delayed delivery](delayed-delivery.md) |
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

Bq also lets the broker hold a delayed message, but with its own topology. A delayed send, a saga timeout and a delayed publish are published at once and wait in RabbitMQ until they are due. Bq declares its own delay queues, named `bq.v1.delay-level-NN` with a final exchange `bq.v1.delay-delivery`, or on RabbitMQ 4.4 and later uses the native `x-opt-delivery-time` header. It does not read or honour NServiceBus's `nsb.v2` queues, so the two do not share delay infrastructure. See [delayed delivery](delayed-delivery.md).

| | NServiceBus on RabbitMQ | Bq |
| --- | --- | --- |
| Where a pending timeout waits | In the broker's `nsb.v2` delay queues | In the broker: `bq.v1` delay queues, or natively on a quorum queue |
| Needs the database to fire | No | No, once published |
| Needs the endpoint running to fire | No | No |
| Created with the handler's work | Only with the outbox | Yes, as an outbox row in the handler's own transaction |
| Where to see them | The delay queues in the management UI | The delay queues, or the destination queue's delayed messages |
| Delayed publish | Not possible | Native delay only, RabbitMQ 4.4 or later |
| Delayed retries | Delayed delivery | Still `bq_recovery` rows polled by a loop |

A timeout the NServiceBus side requested stays in its own delay queues and fires there. Bq cannot take it over.

## Where Bq keeps things

Everything is in the endpoint's database, in tables whose names start with `bq_`: `bq_inbox`, `bq_outbox`, `bq_recovery` and `bq_saga`. The [schema page](schema.md) has the columns, indexes and the differences between SQLite and Oracle. The names do not collide with NServiceBus's tables, so the two can share a database, though a separate schema or database for the Bq group keeps the blast radius smaller.

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
