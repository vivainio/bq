# Delayed delivery and timeouts

A delayed send, `SendAt`, `DelayDeliveryWith` and a saga timeout all work the same way. The message is stored as an ordinary row in the outbox, in the same transaction as the handler's work. The dispatcher publishes it at once, and **RabbitMQ holds it until it is due**. Nothing polls the database for due rows.

That makes a pending timeout a message in the broker, not a row to query. It fires even when the endpoint that requested it is stopped: the broker releases it to the destination queue, and a consumer handles it when one runs.

## Choosing how the broker holds it

Set `config.DelayedDelivery` to one of three values.

| Mode | How it holds the message | Needs |
| --- | --- | --- |
| `Auto` (default) | Native when it can, otherwise levels | |
| `Native` | The `x-opt-delivery-time` header of RabbitMQ 4.4 and later | RabbitMQ 4.4 or later, and quorum queues for every destination |
| `Levels` | A chain of delay queues, declared by Bq | Quorum queue support (RabbitMQ 3.8 or later) |

`Auto` picks native delay when the broker reports 4.4 or later **and** the endpoint's input queue is a quorum queue (`config.InputQueueArguments["x-queue-type"] = "quorum"`). Otherwise it uses levels. A send-only endpoint always uses levels under `Auto`, because it cannot know what type the destination queues are.

### Native delay

The dispatcher adds an `x-opt-delivery-time` header, in milliseconds since the Unix epoch. A **quorum** queue routes and confirms the message at once and releases it to consumers at that time. A **classic queue ignores the header**, and the message is delivered immediately with no error. So native delay needs quorum queues everywhere a delayed message can land.

Bq checks what it can at start-up. With `Native`, it throws `InvalidOperationException` when the broker is older than 4.4 or when the endpoint's input queue is not a quorum queue.

### Delay levels

Without native delay, Bq builds the delay from ordinary exchanges and queues. There are 28 levels, one for each bit of a delay in whole seconds. Level `n` is a quorum queue whose messages all expire after 2^n seconds, and an expired message is dead-lettered to the next level down. A delay of ten seconds (binary `1010`) waits 8 seconds in level 3, then 2 seconds in level 1. Every message in a level has the same TTL, so messages expire in order, which a single queue with per-message TTLs cannot do.

The names all start with `bq.v1`: the exchanges and queues `bq.v1.delay-level-27` to `bq.v1.delay-level-00`, and the final exchange `bq.v1.delay-delivery`. Every endpoint declares them at start-up, so they are shared on the virtual host. There are no per-endpoint or per-tenant copies.

- A delay is rounded **up** to a whole second, so a message is never early. The longest delay is 2^28 − 1 seconds, about 8.5 years.
- **A queue must be bound to the delivery exchange to receive delayed messages.** Every endpoint with an input queue binds it at start. A delayed message for a queue that is not bound is dropped silently after the delay, because the broker has nowhere to route it. This applies to a queue you create yourself, and to an endpoint that has never started. Call `DelayTopology.BindDestination(channel, queue)` for such a queue.
- With `UsePreProvisionedTopology`, Bq does not declare anything. The topology and the bindings must already exist.

## Delayed publishes need native delay

`PublishAt` sends an event to every subscriber. A chain of delay queues delivers to one destination, so levels cannot hold it. NServiceBus cannot delay a publish either. With levels, `PublishAt` throws `NotSupportedException`. Under native delay each subscriber's queue holds its own copy until the time, so it works if every subscriber's queue is a quorum queue.

## What changed from a database timeout table

Earlier versions kept timeouts and scheduled sends in a `bq_saga_timeout` table, which the dispatcher polled. That table is gone, along with the `SagaTimeout` and `ScheduledSend` dispatch kinds and the due-time counts in `GetDispatchBacklog`. A delayed send is now a `bq_outbox` row until the dispatcher has published it. After that it is in the broker.

For an Oracle database created before this change, run `migrations/002_drop_bq_saga_timeout.sql` once no endpoint has rows left in the old table. SQLite databases keep the unused table, which does no harm.

Delayed retries after a failed handler are a separate mechanism. They still use `bq_recovery` rows.

## Trade-offs

| | |
| --- | --- |
| Atomic with the handler's work | Yes. The row is committed with the saga state, so the timeout exists if and only if the work committed |
| Fires while the endpoint is stopped | Yes. The broker holds it |
| Fires while the database is down | Yes, once published |
| Can be seen with SQL | No. Look at the broker, or at `messages_delayed` in rabbit-sim |
| Can be cancelled | No. Bq ignores a timeout for a saga that has completed or never existed, as before |
| Depends on RabbitMQ | Yes, on quorum queues and, for native delay, version 4.4 |

## Testing

[rabbit-sim](https://vivainio.github.io/rabbit-sim/) holds delayed messages for both modes and moves its clock on request, so a test does not wait out a delay. See its chapter on [testing delayed messages](https://vivainio.github.io/rabbit-sim/testing-delayed-messages/).
