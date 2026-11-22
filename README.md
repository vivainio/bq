# Bq

Bq is an experimental, open source .NET messaging framework built on RabbitMQ, with an API that follows NServiceBus. The implementation is written from scratch and is not based on NServiceBus code. It exists to give you an open layer that coordinates your messaging backbone, and the freedom to extend its use without licensing concerns. Bq is MIT licensed (see [LICENSE](LICENSE)), and the plan is to never monetize it in any way. Everyone is responsible for testing Bq in their own systems, and no commercial-level guarantees are given for anything. It uses contractless MessagePack for ordinary .NET message classes and requires a database-backed persistence provider. The messaging library targets .NET Standard 2.0; its integration executable targets .NET Framework 4.7.2 and is exercised on Mono.

The current implementation supports:

- Explicit endpoint and handler registration, command routing, SendLocal, event subscriptions, replies, and multiple handlers per message.
- Scoped uniform sessions, configurable concurrency, and graceful endpoint shutdown.
- SQLite-backed inbox deduplication, a transactional outbox, and explicit ADO.NET transactions shared with application data.
- SQLite dispatch backlog counts, with separate polling for recovery and ordinary outbox messages.
- Saga message correlation, persisted state, completion, and durable timeouts.
- Configurable immediate and durable delayed retries, an error queue, retained failure details, and an API to inspect and replay failures.

Delivery is at least once. Database changes only participate in the Bq transaction when they use the supplied connection and transaction; external effects still need idempotency. SQLite is the only persistence provider and is intended for local or low-concurrency use. The API follows NServiceBus at the source level, but the wire format is Bq's own, so the two do not exchange messages directly. Recovery and inbox records currently have no automatic retention cleanup.

Start with the [Bq book](docs/index.md). See [Bq.Messaging/README.md](Bq.Messaging/README.md) for setup, examples, guarantees, limitations, and test commands. The [Castle Windsor reference application](Bq.Reference.Windsor/README.md) demonstrates net472/Mono endpoint registration, scoped handlers, and uniform sessions.

The older `Bq` and `Bq.Core` projects contain the previous Oracle/Redis/protobuf queue prototype; they are separate from the RabbitMQ implementation.
