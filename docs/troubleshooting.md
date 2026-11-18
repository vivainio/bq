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
| A retry or saga timeout has yet to fire | Check its UTC due time using `ListPendingDispatches()`. When it is due, check the dispatch logs and the destination queue. A broker may have accepted the message even though the receiver has yet to process it. |
| An error is visible in SQL but missing from RabbitMQ | Check `ListReadyDispatches(DispatchKind.Error)` and verify the configured error queue exists. An unavailable error queue can delay later recovery records. Ordinary messages and saga timeouts continue polling separately. |
| A message appears more than once | Compare message IDs. A lost or unrecorded publisher confirmation can trigger another publish with the same ID. A manual replay uses a fresh ID. |

## Replay a failed message

Inspect the failure details with `endpoint.ListErrors()`. After fixing the cause, pass the chosen row's `RecoveryId` to `endpoint.ReplayError(recoveryId)`. A successful result means Bq recorded the replay for dispatch. Check the destination endpoint or its application state to verify that processing completed.

A broker error queue can still contain an older copy after a replay. Use the database record to tell whether Bq accepted the replay request. See [Retries and replay](recoverability.md) for the retention and deduplication behavior.
