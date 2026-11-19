# The Bq book

Bq runs message handlers and sagas on RabbitMQ with a transactional database inbox and outbox. Messages are ordinary .NET classes serialized with MessagePack. The libraries target .NET Standard 2.0 and the integration suite runs under Mono on .NET Framework 4.7.2.

- [Getting started](getting-started.md) walks through endpoint registration and SQLite setup.
- [Phased NServiceBus migration](migration.md) covers routing isolation, transaction hooks, and local verification.
- [Endpoints and sessions](endpoints.md) covers routing, local sends, uniform sessions, concurrency, and shutdown.
- [Persistence and outbox](persistence.md) explains transaction boundaries and dispatch inspection.
- [Sagas and timeouts](sagas.md) covers correlation, state, completion, and durable timeouts.
- [Retries and replay](recoverability.md) shows how to configure retries and recover failed messages.
- [Castle Windsor example](windsor.md) shows a working order flow with scoped handlers.

Bq delivers messages at least once. SQLite is the current persistence provider and is suitable for local or low-concurrency use. The API draws on NServiceBus concepts and uses Bq’s own API and wire format.
