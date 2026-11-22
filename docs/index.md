# The Bq book

Bq runs message handlers and sagas on RabbitMQ with a transactional database inbox and outbox. Messages are ordinary .NET classes serialized with MessagePack by default, or JSON if you choose it. The libraries target .NET Standard 2.0 and the integration suite runs under Mono on .NET Framework 4.7.2.

## Why Bq exists

Messaging is the backbone that coordinates an application's services, so the layer that runs it should be one you can read, change and extend. Bq is an open source layer for that job. It gives you the freedom to use it across more endpoints, services and environments without licensing concerns.

Bq is MIT licensed, and the plan is to never monetize it in any way: no paid tier, no commercial license, no license change later. Other open source messaging libraries, MassTransit among them, have moved to a commercial model. Bq is meant to be something you can build on without that risk.

In return, you are responsible for testing Bq in your own systems. The MIT license provides the software as is, and nothing here is a commercial-level guarantee: no support commitment, no service level, and no promise of fitness for any purpose. The tests in this repository show what Bq is expected to do, but they don't replace your own testing against your workload, your broker and your database.

Its API follows NServiceBus at the source level, but the implementation is not based on NServiceBus: Bq is written from scratch and shares no NServiceBus code. Because the API matches, an application built on NServiceBus can move over by changing namespaces and a few registrations. See [Phased NServiceBus migration](migration.md).

## Where to start

- [An order workflow with Bq](order-workflow.md) introduces messaging through a three-endpoint example.
- [Getting started](getting-started.md) walks through endpoint registration and SQLite setup.
- [Multi-tenant endpoints](multi-tenant.md) covers a database per tenant, with the tenant taken from a message header.
- [Moving one flow at a time](gradual-migration.md) shows how to prove Bq on one flow while the rest stays on NServiceBus.
- [Databases in a migration](databases-in-migration.md) compares where NServiceBus and Bq keep sagas, timeouts and the outbox, with checks to run before a cutover.
- [Phased NServiceBus migration](migration.md) covers routing isolation, transaction hooks, and local verification.
- [Endpoints and sessions](endpoints.md) covers routing, local sends, uniform sessions, concurrency, and shutdown.
- [Persistence and outbox](persistence.md) explains transaction boundaries and dispatch inspection.
- [Sagas and timeouts](sagas.md) covers correlation, state, completion, and durable timeouts.
- [Retries and replay](recoverability.md) shows how to configure retries and recover failed messages.
- [Castle Windsor example](windsor.md) shows a working order flow with scoped handlers.

Bq delivers messages at least once. SQLite is the current persistence provider and is suitable for local or low-concurrency use. The API matches NServiceBus at the source level. The wire format is Bq’s own, so Bq and NServiceBus endpoints do not exchange messages directly.
