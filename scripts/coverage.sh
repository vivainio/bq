#!/usr/bin/env bash
# Measures line coverage of the Bq assemblies with Mono's coverage profiler.
# Needs rabbit-sim on PATH. Usage: bash scripts/coverage.sh [rows] [file.cs to list missed lines for]
set -euo pipefail
cd "$(dirname "$0")/.."
dotnet build Bq.Messaging.Tests/Bq.Messaging.Tests.csproj >/dev/null
dir=$(mktemp -d)
trap 'kill $broker 2>/dev/null || true; rm -rf "$dir"' EXIT
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
printf '+Bq.Messaging\n+Bq.Persistence.Sqlite\n+Bq.Persistence.Oracle\n+Bq.Testing\n+Bq.Extensions.CastleWindsor\n' >"$dir/filter.txt"
rabbit-sim --bind 127.0.0.1 --port "$port" --http-port 0 --data "$dir/broker.db" >"$dir/broker.log" 2>&1 &
broker=$!
sleep 1
BQ_RABBITMQ_URI="amqp://guest:guest@127.0.0.1:$port/" \
    mono --debug "--profile=coverage:covfilter-file=$dir/filter.txt,output=$dir/coverage.xml" \
    Bq.Messaging.Tests/bin/Debug/net472/Bq.Messaging.Tests.exe
kill $broker; wait $broker 2>/dev/null || true; sleep 2
python3 scripts/coverage-report.py "$dir/coverage.xml" "${1:-40}" ${2:+"$2"}
