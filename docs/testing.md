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

## TUnit runner

`Bq.Messaging.Tests.TUnit` runs the same tests under [TUnit](https://tunit.dev) on .NET Framework (net472, on Mono), so each test reports on its own and one failure does not hide the rest. The older test groups each appear as one test, and the edge-case tests appear one per case.

```sh
dotnet build Bq.Messaging.Tests.TUnit/Bq.Messaging.Tests.TUnit.csproj
mono Bq.Messaging.Tests.TUnit/bin/Debug/net472/Bq.Messaging.Tests.exe
```

With `BQ_RABBITMQ_URI` unset and `rabbit-sim` on `PATH`, the runner starts its own simulator on free ports and stops it when it exits, so the broker tests run without setup. Set `BQ_NO_SIM=1` to run only the tests that need no broker. `scripts/test-rabbit-sim.sh` runs this project after the broker outage test, which the script drives by stopping and restarting the broker.

New tests go into the registered-case files (`EdgeCaseTests.cs` for tests without a broker, `EdgeBrokerTests.cs` for tests with one). The console runner and TUnit both pick them up.

## Coverage

`scripts/coverage.sh` runs the suite under Mono's coverage profiler and lists the files with the most uncovered lines. Pass a row count and a file name to list that file's missed lines, for example `bash scripts/coverage.sh 30 RabbitEndpoint.cs`.

## Oracle

When `BQ_ORACLE_CONNECTION_STRING`, `BQ_ORACLE_DRIVER_ASSEMBLY` and `BQ_ORACLE_CONNECTION_TYPE` are set, the same run also checks the Oracle provider and a whole endpoint on Oracle. The `oracle` job in the messaging workflow does this against an Oracle Database Free container. `Bq.Persistence.Oracle/README.md` in the repository describes the local setup, including the Oracle Database Free container.

## Two runtimes

`Bq.Reference.Net10` builds the Castle Windsor reference application for net10.0 from the same source as the net472 sample, and the test script runs both against the simulator. The libraries target .NET Standard 2.0, so one build serves both runtimes.

## Cross-endpoint workflow

The [order workflow tutorial](order-workflow.md) explains the scenario. The test uses three endpoints with separate SQLite databases and one RabbitMQ broker. It checks that intake commits an order with its command, fulfillment runs a saga and durable timeout, and a downstream handler rolls back and retries after an endpoint restart. It also redelivers both the original event and its successful retry to verify that the workflow completes once.

Use `Bq.Testing` for fast handler and saga logic tests. The cross-endpoint test covers queue routing, confirms, and redelivery against an AMQP broker.

## Broker restart test

Run the complete suite with `./scripts/test-rabbitmq.sh`. It requires the .NET SDK, Mono, and a running Podman machine or native Podman. The runner starts its own RabbitMQ container on an available localhost port, stops it during a scheduled retry, waits for the endpoint to report a critical error, restarts it, then runs the Castle Windsor reference application against the restarted broker using a temporary SQLite database. On failure it prints the last 200 RabbitMQ log lines before removing the container.

Use the Messaging integration workflow’s manual trigger to also run the two RabbitMQ image jobs (3.13 and 4.x).

To check a broker upgrade, set `BQ_RABBITMQ_IMAGE` to the RabbitMQ container image used by your deployment. The runner defaults to `docker.io/library/rabbitmq:3.13-management`. The selected image must provide `rabbitmq-server` and `rabbitmq-diagnostics`.
