# Troubleshooting

Start with the endpoint's logs and its SQLite dispatch backlog. Use the same endpoint name and database file as the service. The backlog describes work Bq still holds in SQL. Check RabbitMQ queue depth and consumers separately for messages the broker has already accepted.

```csharp
var backlog = await store.GetDispatchBacklog();
var ready = await store.ListReadyDispatches(limit: 25);
foreach (var item in ready)
{
    Console.WriteLine($"{item.Kind} {item.MessageId} {item.Exchange} {item.RoutingKey}");
}
```

| Symptom | What to check |
| --- | --- |
| An ordinary outbox message remains pending | Match its exchange and routing key to the broker topology. For a direct send, check that the destination queue exists. Check logs for publisher confirmation failures. A stuck ordinary message can hold up later ordinary messages. |
| A message went to the error queue with a `tenant` failure message | In a [multi-tenant endpoint](multi-tenant.md) it has no tenant header, or its tenant is not in the catalogue. The error copy's `x-bq-failure-message` header says which. Fix the sender or the catalogue, then move the message back by hand, since it was rejected before any database was opened. |
| `TenantNotSetException` | Code ran with no tenant. Inside a handler the tenant is set. Elsewhere, wrap the work in `using (Tenant.Scope(id))`, or set the `tenant-id` header on the send. See [multi-tenant endpoints](multi-tenant.md#sending-from-outside-a-handler). |
| One tenant's messages are not being published | Its database may be down: the dispatcher leaves a failed tenant alone for 30 seconds at a time and the others continue. Check the trace for `Tenant '...' could not be dispatched`. |
| A retry has yet to fire | Check its UTC due time using `ListPendingDispatches()`. When it is due, check the dispatch logs and the destination queue. A broker may have accepted the message even though the receiver has yet to process it. |
| A saga timeout or delayed send has yet to arrive | It is held by the broker once published, not in the database. Check that `ListPendingDispatches()` no longer lists it, then look at the delay queues (`bq.v1.delay-level-NN`), or for native delay at the destination queue's delayed messages. A delayed message for a queue that is not bound to `bq.v1.delay-delivery` is dropped after the delay. See [delayed delivery](delayed-delivery.md). |
| An error is visible in SQL but missing from RabbitMQ | Check the error record’s `Dispatched` flag and `ListReadyDispatches(DispatchKind.Error)`, then verify the configured error queue exists. An unavailable error queue can delay later recovery records. Ordinary messages and saga timeouts continue polling separately. |
| A message appears more than once | Compare message IDs. A lost or unrecorded publisher confirmation can trigger another publish with the same ID. A manual replay uses a fresh ID. |

## Replay a failed message

Inspect the failure details with `endpoint.ListErrors()`. After fixing the cause, pass the chosen row's `RecoveryId` to `endpoint.ReplayError(recoveryId)`. A successful result means Bq recorded the replay for dispatch. Check the destination endpoint or its application state to verify that processing completed.

A broker error queue can still contain an older copy after a replay. Use the database record to tell whether Bq accepted the replay request. See [Retries and replay](recoverability.md) for the retention and deduplication behavior.
