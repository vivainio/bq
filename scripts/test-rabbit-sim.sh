#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj
dotnet build Bq.Reference.Windsor/Bq.Reference.Windsor.csproj
dotnet build Bq.Messaging.Tests.TUnit/Bq.Messaging.Tests.TUnit.csproj
dotnet build Bq.Reference.Net10/Bq.Reference.Net10.csproj

test_dir=$(mktemp -d)
broker_pid=""
test_pid=""

cleanup() {
    local status=$?
    if [[ -n "$test_pid" ]]; then
        kill "$test_pid" 2>/dev/null || true
        wait "$test_pid" 2>/dev/null || true
    fi
    if [[ -n "$broker_pid" ]]; then
        kill "$broker_pid" 2>/dev/null || true
        wait "$broker_pid" 2>/dev/null || true
    fi
    if ((status != 0)) && [[ -f "$test_dir/broker.log" ]]; then
        tail -n 200 "$test_dir/broker.log" >&2
    fi
    rm -rf "$test_dir"
    exit "$status"
}
trap cleanup EXIT

port=$(python3 - <<'PYTHON'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PYTHON
)

http_port=$(python3 - <<'PYTHON'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PYTHON
)

start_broker() {
    rabbit-sim --bind 127.0.0.1 --port "$port" --http-port "$http_port" --data "$test_dir/broker.db" \
        >>"$test_dir/broker.log" 2>&1 &
    broker_pid=$!
    local attempt
    for ((attempt = 0; attempt < 100; attempt++)); do
        if (: >/dev/tcp/127.0.0.1/"$port") 2>/dev/null && kill -0 "$broker_pid" 2>/dev/null; then
            return
        fi
        if ! kill -0 "$broker_pid" 2>/dev/null; then break; fi
        sleep 0.1
    done
    echo "rabbit-sim did not listen on port $port." >&2
    return 1
}

wait_for_marker() {
    while [[ ! -f "$1" ]]; do
        if ! kill -0 "$test_pid" 2>/dev/null; then
            echo "Messaging tests exited before signaling ${1##*/}." >&2
            return 1
        fi
        sleep 0.1
    done
}

start_broker
export BQ_RABBITMQ_URI="amqp://guest:guest@127.0.0.1:$port/"
export BQ_RABBITSIM_HTTP="http://127.0.0.1:$http_port"
BQ_OUTAGE_ONLY=1 BQ_BROKER_OUTAGE_DIR="$test_dir" \
    mono Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe &
test_pid=$!

wait_for_marker "$test_dir/ready"
kill -TERM "$broker_pid"
wait "$broker_pid"
broker_pid=""
wait_for_marker "$test_dir/critical-error"
start_broker
touch "$test_dir/restarted"

wait "$test_pid"
test_pid=""
# The TUnit runner reports each registered case on its own. It reuses the running broker.
TESTINGPLATFORM_TELEMETRY_OPTOUT=1 mono Bq.Messaging.Tests.TUnit/bin/Debug/net472/Bq.Messaging.Tests.exe
BQ_SAMPLE_DB="$test_dir/windsor.db" \
    mono Bq.Reference.Windsor/bin/Debug/net472/Bq.Reference.Windsor.exe
BQ_SAMPLE_DB="$test_dir/net10.db" \
    dotnet Bq.Reference.Net10/bin/Debug/net10.0/Bq.Reference.Net10.dll
