# Migrate from NServiceBus in phases

Start with a small group of endpoints whose senders and receivers can switch together. A send-only Bq producer and a subscriber without saga state make a useful first exercise. List the commands, events, subscriptions, and queues each endpoint uses before choosing the group.

Bq uses MessagePack. Give migrated consumers their own queues and use a distinct event exchange for the Bq group while legacy XML consumers remain active. Route commands to the new queues only when their receivers are ready. Before redirecting each flow, account for messages already queued, scheduled deliveries, and unpublished outbox entries. For flows with active sagas, decide how to handle existing state and pending timeouts before switching traffic.

## Port the endpoint

Use the `Bq.Messaging` APIs in the selected C# projects. Bq provides optional `IMessage`, `ICommand`, and `IEvent` markers. The host can use them when discovering handlers and configuring routes or subscriptions. Use [stable contract names](endpoints.md) to keep routing independent of CLR namespace changes.

Register incoming behaviors with `configuration.Incoming.Use`. They can share attempt-local state, short-circuit processing, and wrap handler execution. Use `configuration.Incoming.BeforeCommit` for application work that must commit with the inbox claim and outbox entries. Pass the supplied connection and transaction to the application data layer. An exception from the handler, callback, or surrounding behavior rolls back the transaction.

Transport mutators can change headers, the contract name, or the body. Bq runs the outgoing mutator before storing an outbox message and preserves the received wire payload for [recovery and replay](recoverability.md). Code that used NServiceBus headers can read Bq's delivery metadata and use `FindHeader` or `GetHeader` for text values.

## Verify the migration locally

Use `Bq.Testing` to record sends, publications, replies, and saga timeouts without RabbitMQ. Run `bash scripts/test-rabbitmq.sh` for the [integration suite](testing.md). Its migration cases exercise per-attempt handler scopes, transaction rollback, policy-selected retries, provider-controlled dispatch after broker confirmation, critical-error notification for a stalled persistence poll, and an isolated MessagePack flow alongside a simulated legacy XML flow. The phased fixture uses host-defined marker conventions to set up both a command route and an event subscription.

The local fixtures use SQLite and representative Bq endpoints. Production cutover still requires verification against the consuming application's contracts, topology, and database. The Oracle provider and shared EF6 transaction verification are deferred.
