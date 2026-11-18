# Bq

Bq is an experimental .NET messaging framework built on RabbitMQ, with an API inspired by NServiceBus. It uses contractless MessagePack for ordinary .NET message classes and requires a database-backed persistence provider. The messaging library targets .NET Standard 2.0; its integration executable targets .NET Framework 4.7.2 and is exercised on Mono.

The current implementation supports:

- Explicit endpoint and handler registration, command routing, SendLocal, event subscriptions, replies, and multiple handlers per message.
- Scoped uniform sessions, configurable concurrency, and graceful endpoint shutdown.
- SQLite-backed inbox deduplication, a transactional outbox, and explicit ADO.NET transactions shared with application data.
- SQLite dispatch backlog counts, with separate polling for recovery and ordinary outbox messages.
- Saga message correlation, persisted state, completion, and durable timeouts.
- Configurable immediate and durable delayed retries, an error queue, retained failure details, and an API to inspect and replay failures.

Delivery is at least once. Database changes only participate in the Bq transaction when they use the supplied connection and transaction; external effects still need idempotency. SQLite is the only persistence provider and is intended for local or low-concurrency use. The API and wire format are not drop-in compatible with NServiceBus. Recovery and inbox records currently have no automatic retention cleanup.

See [Bq.Messaging/README.md](Bq.Messaging/README.md) for setup, examples, guarantees, limitations, and test commands. The [Castle Windsor reference application](Bq.Reference.Windsor/README.md) demonstrates net472/Mono endpoint registration, scoped handlers, and uniform sessions.

The older `Bq` and `Bq.Core` projects contain the previous Oracle/Redis/protobuf queue prototype; they are separate from the RabbitMQ implementation.
