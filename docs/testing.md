# Integration tests

```sh
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj
mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
BQ_RABBITMQ_URI=amqp://guest:guest@localhost:5672/ mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
```

The last command also exercises command delivery, replies, event fan-out, saga timeouts, delayed retry, replay, and failure handling against a local RabbitMQ broker. For example, with Podman:

```sh
podman run -d --name bq-rabbitmq -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 docker.io/library/rabbitmq:3.13-management
```

## Run against rabbitsim

If you have `rabbitsim` available locally, start it on a loopback port and point the messaging test executable at it.

```sh
~/r/rabbitsim/target/debug/rabbitsim --port 5679
```

In another terminal, build and run the tests.

```sh
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj
BQ_RABBITMQ_URI=amqp://guest:guest@127.0.0.1:5679/ mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
```

The simulator keeps broker state in memory. Use the RabbitMQ runner below for the broker restart case.

## Cross-endpoint workflow

The [order workflow tutorial](order-workflow.md) explains the scenario. The test uses three endpoints with separate SQLite databases and one RabbitMQ broker. It checks that intake commits an order with its command, fulfillment runs a saga and durable timeout, and a downstream handler rolls back and retries after an endpoint restart. It also redelivers both the original event and its successful retry to verify that the workflow completes once.

Use `Bq.Testing` for fast handler and saga logic tests. The cross-endpoint test uses RabbitMQ to cover queue routing, confirms, and redelivery.

## Broker restart test

Run the complete suite with `./scripts/test-rabbitmq.sh`. It requires the .NET SDK, Mono, and a running Podman machine or native Podman. The runner starts its own RabbitMQ container on an available localhost port, stops it during a scheduled retry, waits for the endpoint to report a critical error, restarts it, then runs the Castle Windsor reference application against the restarted broker using a temporary SQLite database. On failure it prints the last 200 RabbitMQ log lines before removing the container.

The Messaging integration workflow runs this script on Ubuntu against RabbitMQ 3.13 and the [RabbitMQ 4.x management image](https://www.rabbitmq.com/docs/4.2/download) for changes to messaging, the testing library, SQLite persistence, the Windsor sample, or the test runner. It can also be started manually from the GitHub Actions tab.

To check a broker upgrade, set `BQ_RABBITMQ_IMAGE` to the RabbitMQ container image used by your deployment. The runner defaults to `docker.io/library/rabbitmq:3.13-management`. The selected image must provide `rabbitmq-server` and `rabbitmq-diagnostics`.
