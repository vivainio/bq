#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj
dotnet build Bq.Reference.Windsor/Bq.Reference.Windsor.csproj

test_dir=$(mktemp -d)
container="bq-integration-${test_dir##*/}"
watcher=""
created=false
cleanup() {
    local status=$?
    if [[ -n "$watcher" ]]; then
        kill "$watcher" 2>/dev/null || true
        wait "$watcher" 2>/dev/null || true
    fi
    if "$created"; then
        if ((status != 0)); then
            printf '\nRabbitMQ container logs (%s)\n' "$container" >&2
            podman logs --tail 200 "$container" >&2 || true
        fi
        podman rm -f -v "$container" >/dev/null || true
    fi
    rm -rf "$test_dir"
    exit "$status"
}
trap cleanup EXIT

podman run -d --name "$container" --user 0 --entrypoint rabbitmq-server \
    -e RABBITMQ_ERLANG_COOKIE=bq-local-integration \
    -p 127.0.0.1::5672 docker.io/library/rabbitmq:3.13-management >/dev/null
created=true
address=$(podman port "$container" 5672/tcp)

wait_for_broker() {
    local i
    for ((i=0; i<30; i++)); do
        if podman exec "$container" rabbitmq-diagnostics -q check_running >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    echo "RabbitMQ did not become ready." >&2
    return 1
}

wait_for_broker
(
    while [[ ! -f "$test_dir/ready" ]]; do sleep 0.2; done
    podman restart "$container" >/dev/null
    touch "$test_dir/restarted"
) &
watcher=$!

export BQ_RABBITMQ_URI="amqp://guest:guest@$address/"
BQ_BROKER_OUTAGE_DIR="$test_dir" \
    mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
wait "$watcher"
watcher=""

BQ_SAMPLE_DB="$test_dir/windsor.db" \
    mono Bq.Reference.Windsor/bin/Debug/net472/Bq.Reference.Windsor.exe
