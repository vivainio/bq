#!/usr/bin/env bash
# A long stress and fuzz run against rabbit-sim, for a laptop. It is not part of CI.
# Usage: bash scripts/stress.sh [seconds]   (default 300)
# Environment: BQ_STRESS_SEED, BQ_STRESS_PRODUCERS, BQ_STRESS_CONCURRENCY, BQ_STRESS_FAIL_RATE, BQ_STRESS_BACKLOG.
set -euo pipefail

cd "$(dirname "$0")/.."
dotnet build Bq.Stress/Bq.Stress.csproj -c Release --nologo -v quiet

work=$(mktemp -d)
broker_pid=""
trap 'if [[ -n "$broker_pid" ]]; then kill "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; fi; rm -rf "$work"' EXIT

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
port=$(free_port)
http_port=$(free_port)
rabbitmq_args=()
if rabbit-sim --help 2>&1 | grep -q -- '--rabbitmq'; then rabbitmq_args=(--rabbitmq 4); fi
rabbit-sim --bind 127.0.0.1 --port "$port" --http-port "$http_port" "${rabbitmq_args[@]}" --data "$work/broker.db" >"$work/broker.log" 2>&1 &
broker_pid=$!
for _ in $(seq 100); do (: >/dev/tcp/127.0.0.1/"$port") 2>/dev/null && break; sleep 0.1; done

export BQ_RABBITMQ_URI="amqp://guest:guest@127.0.0.1:$port/"
export BQ_RABBITSIM_HTTP="http://127.0.0.1:$http_port"
export BQ_STRESS_SECONDS="${1:-300}"
dotnet Bq.Stress/bin/Release/net10.0/Bq.Stress.dll
