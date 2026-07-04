#!/usr/bin/env bash
# Run the shadow-forwarding compliance harness, then assemble → shadow.json. Best-effort: a failed
# harness leaves no raw file and assemble emits a "pending" shadow.json rather than breaking the
# artifact. Assumes (the evidence workflow sets these up):
#   pg_kafka on $PG_KAFKA_BROKER (:9092), real broker on $REAL_BROKER (:9093), libpq env for pg.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${1:-$PWD}"
WORK="$(mktemp -d)"
mkdir -p "$OUT_DIR"

( cd "$HERE" && npm install --no-audit --no-fund --loglevel=error ) || echo "::warning::shadow npm install failed"

# Wrapped in `timeout` (belt-and-suspenders — kafkajs keeps the event loop alive; the harness exits
# explicitly). Six scenarios each reload the bgworker config + drive produce/consume, so allow headroom.
echo "== shadow: forwarding compliance =="
( cd "$HERE" && PG_KAFKA_BROKER="${PG_KAFKA_BROKER:-localhost:9092}" REAL_BROKER="${REAL_BROKER:-localhost:9093}" \
  OUT="$WORK/shadow-raw.json" timeout -k 10 420 node shadow.mjs ) || echo "::warning::shadow harness failed"

node "$HERE/assemble.mjs" "$WORK" "$OUT_DIR/shadow.json"
