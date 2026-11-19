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

## Run against rabbit-sim

The messaging CI job installs the [published rabbit-sim wheel](https://pypi.org/project/rabbit-sim/) and runs the full messaging suite, including the broker outage and restart test. Run the same checks locally with the .NET SDK, Mono, and Python.

```sh
python3 -m venv .venv-rabbit-sim
source .venv-rabbit-sim/bin/activate
python -m pip install rabbit-sim==0.1.0
bash scripts/test-rabbit-sim.sh
```

The runner uses `--data` to persist broker state to a temporary SQLite file. It stops the simulator, waits for the endpoint to observe the outage, and restarts it with that same file before running the Windsor sample.

## Cross-endpoint workflow

The [order workflow tutorial](order-workflow.md) explains the scenario. The test uses three endpoints with separate SQLite databases and one RabbitMQ broker. It checks that intake commits an order with its command, fulfillment runs a saga and durable timeout, and a downstream handler rolls back and retries after an endpoint restart. It also redelivers both the original event and its successful retry to verify that the workflow completes once.

Use `Bq.Testing` for fast handler and saga logic tests. The cross-endpoint test covers queue routing, confirms, and redelivery against an AMQP broker.

## Broker restart test

Run the complete suite with `./scripts/test-rabbitmq.sh`. It requires the .NET SDK, Mono, and a running Podman machine or native Podman. The runner starts its own RabbitMQ container on an available localhost port, stops it during a scheduled retry, waits for the endpoint to report a critical error, restarts it, then runs the Castle Windsor reference application against the restarted broker using a temporary SQLite database. On failure it prints the last 200 RabbitMQ log lines before removing the container.

Use the Messaging integration workflow’s manual trigger to also run the two RabbitMQ image jobs (3.13 and 4.x).

To check a broker upgrade, set `BQ_RABBITMQ_IMAGE` to the RabbitMQ container image used by your deployment. The runner defaults to `docker.io/library/rabbitmq:3.13-management`. The selected image must provide `rabbitmq-server` and `rabbitmq-diagnostics`.
