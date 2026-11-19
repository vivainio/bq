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

## Broker restart test

Run the complete suite with `./scripts/test-rabbitmq.sh`. It requires the .NET SDK, Mono, and a running Podman machine or native Podman. The runner starts its own RabbitMQ container on an available localhost port, restarts it during a scheduled retry, then runs the Castle Windsor reference application against the restarted broker using a temporary SQLite database. On failure it prints the last 200 RabbitMQ log lines before removing the container.

The Messaging integration workflow runs this script on Ubuntu against RabbitMQ 3.13 and the [RabbitMQ 4.x management image](https://www.rabbitmq.com/docs/4.2/download) for changes to messaging, the testing library, SQLite persistence, the Windsor sample, or the test runner. It can also be started manually from the GitHub Actions tab.

To check a broker upgrade, set `BQ_RABBITMQ_IMAGE` to the RabbitMQ container image used by your deployment. The runner defaults to `docker.io/library/rabbitmq:3.13-management`. The selected image must provide `rabbitmq-server` and `rabbitmq-diagnostics`.
