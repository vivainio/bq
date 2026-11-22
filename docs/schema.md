# Database schema

Every Bq persistence provider keeps the same five tables. They sit in the same database as your application data, so a handler's business writes, its inbox claim and the messages it sends commit in one transaction. Several endpoints can share a database because every row carries the `endpoint` name it belongs to.

| Table | Holds | Rows are written | Rows are removed |
|---|---|---|---|
| `bq_inbox` | The ID of every message an endpoint has handled | When an attempt claims a message, in the handler's transaction | Never. There is no retention cleanup yet |
| `bq_outbox` | Messages waiting to be published | With the handler's business writes | After RabbitMQ confirms the publish |
| `bq_saga_timeout` | Saga timeouts and scheduled sends, with the time they fall due | With the handler's business writes | After RabbitMQ confirms the publish |
| `bq_saga` | The state of each running saga | When a saga handles a message | When the saga completes |
| `bq_recovery` | Retries waiting to be sent and messages that failed for good | When an attempt fails | Never. A sent row is marked `dispatched` and a replayed error is marked `replayed`. There is no retention cleanup yet |

SQLite creates the tables in `SqlitePersistence.Initialize`, which `EnableInstallers()` calls at start. Oracle uses the script `Bq.Persistence.Oracle/migrations/001_create_bq_tables.sql`, which you apply through your own schema process. There is no upgrade path between versions of the schema yet.

Treat the tables as Bq's. Read them to see what is pending, and use `ListPendingDispatches` and `GetDispatchBacklog` on the SQLite provider where they fit. Do not write to them, apart from adding your own indexes. Times are .NET ticks in UTC (`DateTime.UtcNow.Ticks`: 100 nanosecond units since 0001-01-01). Booleans are `0` or `1`.

## `bq_inbox`

| Column | Meaning |
|---|---|
| `endpoint`, `message_id` | Primary key. A message ID that is already here is a duplicate and is skipped |
| `claimed_ticks` | Oracle only. When the message was claimed. Useful when you add retention |

## `bq_outbox` and `bq_saga_timeout`

Both tables describe a message to publish, with the same columns. `bq_saga_timeout` adds `due_ticks`. The dispatcher sends a timeout row only once it is due.

| Column | Meaning |
|---|---|
| `endpoint`, `id` | Primary key. `id` becomes the AMQP message ID, so a row published twice is recognized by the receiver's inbox |
| `correlation_id`, `reply_to` | AMQP properties of the message |
| `type` (Oracle: `type_name`) | The message's contract name, which receivers use to find the handler. For a saga timeout it is the contract name of the timeout envelope |
| `exchange_name`, `routing_key` | Where to publish. Commands use the default exchange with the queue name as the routing key. Events use the events exchange |
| `mandatory` | `1` for commands, so an unroutable message is returned and counted as a failed publish |
| `body` | The serialized message in the endpoint's wire format (MessagePack or JSON) |
| `headers` | The AMQP headers. They include `x-bq-outbox-content-type`, the wire format of `body`, so a row keeps its format if the endpoint changes format before it is sent |
| `due_ticks` | `bq_saga_timeout` only. When the row may be sent |
| `attempts` | How many times publishing was rejected because of this message, for example no queue accepted it. `0` for a row that has never failed |
| `retry_after_ticks` | While in the future, the dispatcher skips the row so the rows behind it are sent. The delay starts at 100 milliseconds and doubles up to 30 seconds. `0` if the row never failed |
| `last_error` | The reason for the last rejection, up to 500 characters |
| `enqueue_order` | Oracle only. An identity column that orders `bq_outbox`. SQLite orders by its row ID |

Indexes: Oracle has `bq_outbox_ready_ix (endpoint, enqueue_order)` and `bq_saga_timeout_due_ix (endpoint, due_ticks, id)`. SQLite has `bq_saga_timeout_due (endpoint, due_ticks)`.

## `bq_saga`

| Column | Meaning |
|---|---|
| `endpoint`, `saga_type`, `saga_id` | Primary key. `saga_type` is the saga class's full name, so renaming the class starts a new set of sagas. `saga_id` is the correlation key as a string |
| `version` (Oracle: `saga_version`) | Increases with each save. A save with a stale version fails, which is how two handlers touching one saga are kept apart |
| `data` (Oracle: `saga_data`) | The saga's data, serialized |

## `bq_recovery`

One row per failed attempt cycle. A row is either a retry waiting for its due time, or a message that exhausted its retries and sits in the error list.

| Column | Meaning |
|---|---|
| `endpoint`, `recovery_id` | Primary key. The ID derives from the message ID and the cycle, so a failure is recorded once |
| `message_id` | The ID of the message that failed |
| `cycle` (Oracle: `retry_cycle`) | Which delayed-retry cycle this is |
| `is_error` | `1` for a message that moves to the error queue, `0` for a retry |
| `error_queue` | The queue a failed message is sent to |
| `exception_type`, `exception_message`, `exception_detail` | What failed. The detail is the full exception text and can be large (Oracle stores the last two as `NCLOB`) |
| `failed_ticks`, `due_ticks` | When the attempt failed, and when the retry is due |
| `payload` | A snapshot of the original delivery, so a retry or a replay sends the original bytes |
| `dispatched` | `1` once the retry or error copy was published |
| `replayed` | `1` once an error was replayed, so it is replayed only once |

Indexes: `(endpoint, dispatched, due_ticks)` finds due retries, and `(endpoint, is_error, replayed, failed_ticks)` lists errors. Oracle's versions end with `recovery_id`.

## Differences between providers

- **Column types.** SQLite uses `TEXT`, `INTEGER` and `BLOB`. Oracle uses `VARCHAR2`, `NUMBER` and `BLOB`, with check constraints on the `0` or `1` columns.
- **Names.** Oracle avoids reserved words: `type_name`, `retry_cycle`, `saga_version` and `saga_data`.
- **Dispatch locking.** Oracle selects the next row `FOR UPDATE SKIP LOCKED` and holds the lock through the broker confirm, so two dispatchers never publish the same row. SQLite has no row lock, so two dispatchers on one file may each publish a row. The receiver's inbox keeps it to one handling.
- **Retention.** Neither provider removes inbox rows or recovery rows, sent or not. Add your own cleanup if the tables grow, for example deleting `bq_recovery` rows that are `dispatched` and, if errors, `replayed`, once they are old enough.

See [persistence and outbox](persistence.md) for the delivery behavior and [retries and replay](recoverability.md) for the recovery rows.
