#!/usr/bin/env bash
# Benchmark pg_kafka, the real broker (ceiling) and raw INSERT (floor), then assemble → bench.json.
# Each run is best-effort; a missing input degrades to zeros in assemble. Assumes:
#   pg_kafka on $PG_KAFKA_BROKER (:9092), real broker on $REAL_BROKER (:9093), libpq env for psql.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${1:-$PWD}"
WORK="$(mktemp -d)"
mkdir -p "$OUT_DIR"

( cd "$HERE" && npm install --no-audit --no-fund --loglevel=error ) || echo "::warning::bench npm install failed"

# Each bench is wrapped in `timeout` (kafkajs keeps the event loop alive — a lingering client hung
# the first CI run for 6h; the harness now exits explicitly, this is the belt-and-suspenders).
echo "== bench: pg_kafka =="
( cd "$HERE" && BROKER="${PG_KAFKA_BROKER:-localhost:9092}" LABEL=pg_kafka \
  OUT="$WORK/bench-pg_kafka.json" timeout -k 10 240 node bench.mjs ) || echo "::warning::pg_kafka bench failed"

echo "== bench: real broker =="
( cd "$HERE" && BROKER="${REAL_BROKER:-localhost:9093}" LABEL=realbroker \
  OUT="$WORK/bench-realbroker.json" timeout -k 10 240 node bench.mjs ) || echo "::warning::real-broker bench failed"

echo "== bench: raw INSERT =="
( cd "$HERE" && OUT="$WORK/bench-rawinsert.json" timeout -k 10 180 node raw_insert.mjs ) || echo "::warning::raw-insert bench failed"

node "$HERE/assemble.mjs" "$WORK" "$OUT_DIR/bench.json"
