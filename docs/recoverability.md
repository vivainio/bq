# Retries and replay

```csharp
config.Recoverability.ImmediateRetries = 3;
config.Recoverability.DelayedRetries = new[] {
    TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1)
};
config.Recoverability.ErrorQueue = "orders.errors";
config.Recoverability.IsUnrecoverable = ex => ex is InvalidOperationException;
```

Each delivery gets the configured immediate retries (default two), then each configured delay (default 10, 20 and 30 seconds) starts a new immediate-retry cycle. Handler failures roll back their database transaction. Unknown message types, unsupported content types and deserialization failures skip retries. The optional predicate can mark more exceptions unrecoverable. An empty delay array disables delayed retries.

Before acknowledging a failed delivery, Bq commits its recovery decision and a snapshot of the original AMQP properties, headers and body to the database. The dispatcher publishes due retries to the input queue and errors to the configured error queue using publisher confirms and mandatory routing. A storage failure requeues the original. A crash after the decision commits but before acknowledgment can cause redelivery; the persisted message-ID-and-cycle decision makes that delivery a no-op. Publishing a recovery record can itself occur more than once; retries retain their message ID so the inbox suppresses duplicate successful processing. Error queue copies may be duplicated. The message ID is required for deduplication; a failed message without one is assigned an ID and moved to error, but its original redelivery cannot be deduplicated.

Failures are retained in the database for inspection and contain type, message, stack, timestamps, cycle and encoded original message. Error queue copies also carry `x-bq-failure-*` headers. Inspect and replay using:

```csharp
var errors = await endpoint.ListErrors(limit: 100);
foreach (var error in errors)
{
    Console.WriteLine(error.MessageId + ": " + error.ExceptionMessage);
    // Once the cause is corrected: await endpoint.ReplayError(error.RecoveryId);
}
```

SQLite users can page through errors with `store.ListErrors(endpointName, limit, after)`. Pass the last record from the previous page as `after`. An empty page ends the scan.

Replay is an atomic database operation and can be requested once per error record. It creates a fresh message ID so the old retry decisions and inbox entry do not suppress it; the original ID is retained in `x-bq-original-message-id`. Replay does not remove an already-published copy from the RabbitMQ error queue. Consumption directly from that queue is separate from Bq's inspection and replay API. Retry and error records are retained indefinitely; monitor and back up the database. An unavailable error queue leaves its database record pending, and can block subsequent recovery records until restored. Ordinary outbox messages and saga timeouts use a separate polling loop. Delivery remains at least once; external side effects still need idempotency.

This is an early Bq API inspired by NServiceBus, not a replacement for its assemblies or its RabbitMQ wire protocol.
