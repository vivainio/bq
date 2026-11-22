# Moving one flow at a time

You do not have to switch every NServiceBus endpoint at once. Bq and NServiceBus can run side by side on the same RabbitMQ broker, so you can move one flow to Bq, watch it in production, and leave everything else alone. If the new flow misbehaves, the blast radius is that flow.

This page is the strategy. [Migrate from NServiceBus in phases](migration.md) covers the porting details and the tests that back them.

## What is shared and what is not

| | Shared with the NServiceBus endpoints? |
| --- | --- |
| The RabbitMQ broker | Yes. Both run on the same cluster. |
| The virtual host | Optional. Sharing one works if names do not clash. A separate virtual host removes the question. |
| Queues and exchanges | No. Bq uses its own queue names and its own event exchange, `bq.events` by default, and an error exchange, `bq.errors`. |
| Messages | No. Bq does not read NServiceBus messages or write messages NServiceBus can read. Its default wire format is MessagePack, with its own headers and topology. See [what Bq leaves out](omissions.md). |
| The database | No. Bq keeps its own `bq_inbox`, `bq_outbox`, `bq_recovery` and saga tables, in the endpoint's database. The NServiceBus tables are not touched. |

The message boundary decides how you migrate. **A flow is the unit of migration**: the producer and every consumer of a message type move together, because a Bq producer and an NServiceBus consumer cannot talk to each other. Pick flows whose senders and receivers you can switch in one step.

## Never send a Bq message from an NServiceBus handler

!!! danger "The rule"
    Do not send or publish a Bq message from inside an NServiceBus handler. This includes calling a Bq endpoint or session from handler code, and any helper that routes some messages to Bq from there.

The reason is atomicity. A handler that sends through its own stack is protected by that stack's transaction: NServiceBus records the sends with its outbox, and Bq records them in the `bq_outbox` table in the same database transaction as the handler's work. Those are two separate transactions. A handler that does its work on NServiceBus and sends to Bq has no shared transaction between the two, so a failure at the wrong moment loses the Bq message or sends it twice:

- The Bq send succeeds, then the handler fails. NServiceBus retries the handler, and the Bq message is sent again for work that was rolled back.
- The handler commits, then the process stops before the Bq send. The work is done and the Bq message never goes out, and nothing retries it.

Neither shows up in a test with a healthy broker, which is why it is worth a rule rather than a code review comment.

What to do instead:

- **Move the sending handler to Bq.** The handler that produces a Bq-bound message belongs to the same flow as that message. Port it, so its sends are recorded in the Bq outbox with the handler's own transaction.
- **Send from the edge only.** Code that is not a handler, such as a web request or a send-only producer, may use a Bq endpoint directly. It has no handler transaction to share, so nothing can disagree.
- **Keep the legacy side talking to legacy queues.** An NServiceBus handler keeps sending NServiceBus messages until it is ported.

This is also why a flow is the unit of migration: the producer and its consumers move together, including every handler in between.

## Pick the first flow

Good first candidates are small and easy to undo:

- a send-only producer, or a subscriber that has no saga state
- a producer that is not itself an NServiceBus handler, because a handler must [never send a Bq message](#never-send-a-bq-message-from-an-nservicebus-handler)
- a flow with a low business cost when a message is delayed
- handlers that are already safe to run twice, since Bq delivery is at least once

Leave sagas for later. A saga needs a decision about its existing state and pending timeouts, which is a bigger cutover than a stateless handler.

## Stand the Bq side up next to the old one

Give the Bq endpoint its own queue name, so it cannot collide with the NServiceBus queue it will replace, and keep it on its own event exchange. This sketch puts the Bq group on a separate virtual host as well:

```csharp
// The legacy endpoints stay on amqp://.../ . The Bq group lives on its own virtual host.
var config = new EndpointConfiguration("orders-bq", new Uri("amqp://svc:secret@broker:5672/bq"));
config.EventsExchange = "bq.events";      // the default, shown for clarity
config.Persistence = store;               // its own database, see Persistence
config.Handle(() => new PlaceOrderHandler());

using (var endpoint = RabbitEndpoint.Start(config))
{
    // Idle until a producer routes to "orders-bq".
}
```

Bq does not create virtual hosts. On RabbitMQ, create `bq` and give the user permissions on it first. See [virtual hosts](getting-started.md#virtual-hosts).

At this point nothing sends to the new queue, so it changes nothing in production. Run the [integration tests](testing.md) and your own checks against it.

## Cut the flow over

Switch the producer, not the consumer. The producer is the only thing that decides which stack receives new messages.

```csharp
// Before: the producer is an NServiceBus endpoint sending PlaceOrder to "orders".
// After: the same flow, ported to Bq, routes the command to the new queue.
var producer = new EndpointConfiguration("shop-bq", new Uri("amqp://svc:secret@broker:5672/bq"));
producer.SendOnly();
producer.Route<PlaceOrder>("orders-bq");
producer.Persistence = producerStore;

using (var endpoint = RabbitEndpoint.Start(producer))
{
    await endpoint.Send(new PlaceOrder { OrderId = orderId });
}
```

Before you flip it, account for what is already in flight:

1. **Queued messages.** Leave the legacy consumer running until its queue is empty. Messages already there are still its to process.
2. **Scheduled deliveries.** Delayed messages and timeouts set by the legacy side fire later, on the legacy side. Wait for them or move them by hand.
3. **Unpublished work.** An outbox on the legacy producer may still hold messages. Let it drain before you stop that producer.

When the old queue stays empty and the Bq side has run cleanly for as long as you need, remove the legacy endpoint for that flow.

## Rolling back

While the legacy endpoint and its queue still exist, rolling back means pointing the producer at the old queue again. Keep the Bq consumer running afterwards until its queue is empty, so messages that already reached it are still handled. Because both sides are at least once, make sure a message that is retried or replayed on either side is safe to handle twice.

## Sagas

A flow with a saga is the hardest to move, because a saga has state in the middle of its life. Do these flows last, and plan each one separately. Bq has no tool that imports NServiceBus saga data, so every option below is something you do yourself.

What makes it hard:

- **State lives in the old store.** An NServiceBus saga's data sits in its own persister. Bq keeps saga state in `bq_saga`, keyed by endpoint, saga type name and the correlation key as a string, with the data serialized as MessagePack. The two do not read each other's data.
- **Follow-up messages need the right stack.** A message for an instance that started on NServiceBus must reach NServiceBus, and a message for one that started on Bq must reach Bq. Whoever sends the follow-up has to know which. When other services send them, that is often impossible to know.
- **Timeouts work differently.** NServiceBus on RabbitMQ keeps them in the broker. Bq keeps them as database rows that its dispatcher publishes when they fall due, so a Bq saga's timeouts depend on its database and a running endpoint. See [how Bq's timeouts differ](databases-in-migration.md#how-bqs-timeouts-differ).
- **Pending timeouts stay behind.** A timeout the old saga requested fires on the old side. With the RabbitMQ transport it is a message in the broker's delay queues, not a database row, so it does not move by itself. See [databases in a migration](databases-in-migration.md).
- **The saga sends messages.** A saga is a handler, so the [rule above](#never-send-a-bq-message-from-an-nservicebus-handler) applies. A saga that sends commands to a handler you have already moved to Bq must move too, or its commands will not be atomic.

Three ways to do it, from safest to riskiest:

1. **Wait for a quiet moment.** Stop the producers that start the saga, let the running instances complete and their timeouts fire, and confirm the old saga store is empty. Then switch the flow. This is the best choice for short-lived sagas, such as an order that completes within minutes.
2. **Start new instances on Bq and finish old ones on NServiceBus.** Route only the start message to Bq, and keep the old saga endpoint running until its last instance completes. This works only when the follow-up messages can be sent to the right stack, for example because they carry something that says which one started the instance. If they cannot, do not use this option.
3. **Copy the state across.** Write a one-off script that reads the NServiceBus saga rows and writes matching `bq_saga` rows through Bq's persistence session. Timeouts are broker messages and do not copy; let them fire on the old side or request them again from the Bq saga. See [databases in a migration](databases-in-migration.md#copying-saga-state). This keeps long-running instances alive, but the data is a persisted contract: the saga type name, the correlation key format and the MessagePack property names must all match what the Bq saga expects. Rehearse it on a copy of the data, and compare the Bq saga's behavior with the old one before the real cutover.

Whichever you choose, use the same correlation key format for every message of the saga, as [the saga chapter](sagas.md) requires. Keep the old saga data and endpoint until you have seen a full lifetime of instances finish on Bq.

## Repeat

Move the next flow the same way, one at a time. Do the flows with sagas last, as described above.

## When both sides must talk to each other

If a flow cannot move as a unit, something has to translate at the boundary. Bq gives you hooks to do it on the receiving side: an incoming transport mutator runs before the message is deserialized and can rewrite the body and headers.

```csharp
class LegacyOrderTranslator : IMutateIncomingTransportMessages
{
    public Task MutateIncoming(MutateIncomingTransportMessageContext context)
    {
        // context.Body holds the bytes received from the legacy sender and
        // context.Headers its text headers. Rewrite them into the Bq form here.
        return Task.CompletedTask;
    }
}

config.RegisterMessageMutator(new LegacyOrderTranslator());
```

This is a sketch of the extension point. Bq does not ship a converter for NServiceBus payloads, and the topology still has to line up, so test it hard before relying on it. Prefer moving a whole flow at once.
