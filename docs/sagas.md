# Sagas and timeouts

Define a saga with a data class, message handlers, and optional timeout handlers. Register each message's correlation-key selector; `Start` messages create an instance when none exists, while `Handle` messages only apply to an existing instance. This API resembles NServiceBus concepts but is not its configuration API.

```csharp
public class OrderState { public string Status { get; set; } }
public class PlaceOrder { public string OrderId { get; set; } }
public class PaymentReceived { public string OrderId { get; set; } }
public class PaymentDeadline { public string OrderId { get; set; } }

public class OrderSaga : Saga<OrderState>, IAmStartedByMessages<PlaceOrder>,
    IHandleMessages<PaymentReceived>, IHandleTimeouts<PaymentDeadline>
{
    public async Task Handle(PlaceOrder message, IMessageHandlerContext context)
    {
        Data.Status = "waiting";
        await RequestTimeout(TimeSpan.FromMinutes(15), new PaymentDeadline { OrderId = message.OrderId });
    }
    public Task Handle(PaymentReceived message, IMessageHandlerContext context)
    {
        MarkAsComplete();
        return Task.CompletedTask;
    }
    public Task Timeout(PaymentDeadline timeout, IMessageHandlerContext context)
    {
        Data.Status = "expired";
        return Task.CompletedTask;
    }
}

config.RegisterSaga<OrderSaga, OrderState>(() => new OrderSaga(), mappings =>
{
    mappings.Start<PlaceOrder>(m => m.OrderId);
    mappings.Handle<PaymentReceived>(m => m.OrderId);
    mappings.Timeout<PaymentDeadline>();
});
```

Saga registrations require persistence. The selector returns a nonempty, stable string; use the same key format for all message types of the saga. Set any corresponding property on `Data` yourself. A saga instance is created per processing attempt. Its state, version, requested timeouts, incoming-message claim and context-generated outgoing messages share one transaction. Optimistic version checks and the database transaction protect state updates; database contention may cause retries. Other handlers of the same message execute in the same processing transaction.

`MarkAsComplete()` deletes the active state at commit. A later start message may create a new instance with the same key. Timeouts have a unique saga-instance identity, so timeouts requested by the old instance are ignored. A non-starting message with no matching saga is ignored by default; set `config.SagaNotFound` to inspect it or throw to trigger recoverability. Missing or obsolete timeouts are silently ignored. Messages may arrive out of order, so choose the callback behavior accordingly.

Timeouts are stored in SQLite and polled by the endpoint's outbox dispatcher, including after restart. Their actual delivery can be later than requested; clock adjustments and a blocked dispatcher affect timing. Delivery is at least once and duplicate suppression uses the persistent inbox. Completion does not proactively delete outstanding timeout records; they are discarded when delivered. Keep the inbox, timeout and saga data for as long as messages can be redelivered. Saga type names, correlation keys, timeout types and MessagePack property names are persisted contracts. The lower-level `IPersistenceSession` saga methods remain available for manual state management; they use their own type keys and do not automatically manage the saga lifecycle.
