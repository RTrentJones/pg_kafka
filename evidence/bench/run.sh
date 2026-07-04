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

# Shadow-forwarding throughput: produce to a topic whose kafka.shadow_config forwards 100% to the real
# broker, once async (forward off the ack path ≈ plain produce) and once sync (bounded wait for the
# external ack = the true forwarding cost). Each: ensure the topic row exists, apply the shadow config +
# SIGHUP, let the bgworker reload, then run only the batched-produce scenario. Best-effort; a failure
# just omits that row. Delivery itself is proven by the A1 compliance harness on the same config.
shadow_bench() {
  local topic="$1" tag="$2" sync="$3"
  local tid
  tid="$(psql -tAX -c "INSERT INTO kafka.topics (name, partitions) VALUES ('$topic', 1) \
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id;" 2>/dev/null | tr -d '[:space:]')"
  if [ -z "$tid" ]; then echo "::warning::$tag: could not resolve topic id — skipping"; return; fi
  psql -qX >/dev/null 2>&1 <<SQL || { echo "::warning::$tag: shadow config apply failed — skipping"; return; }
ALTER SYSTEM SET pg_kafka.shadow_mode_enabled = true;
ALTER SYSTEM SET pg_kafka.shadow_bootstrap_servers = '${REAL_BROKER:-localhost:9093}';
ALTER SYSTEM SET pg_kafka.shadow_security_protocol = 'PLAINTEXT';
ALTER SYSTEM SET pg_kafka.config_reload_interval_ms = 2000;
INSERT INTO kafka.shadow_config
  (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode, updated_at)
VALUES ($tid, 'shadow', 100, NULL, '$sync', 'dual_write', NOW())
ON CONFLICT (topic_id) DO UPDATE SET mode = EXCLUDED.mode,
  forward_percentage = EXCLUDED.forward_percentage, external_topic_name = EXCLUDED.external_topic_name,
  sync_mode = EXCLUDED.sync_mode, write_mode = EXCLUDED.write_mode, updated_at = NOW();
SELECT pg_reload_conf();
SQL
  sleep 3 # > config_reload_interval_ms — let the bgworker pick up the new shadow config before producing
  echo "== bench: $tag =="
  ( cd "$HERE" && BROKER="${PG_KAFKA_BROKER:-localhost:9092}" LABEL=pg_kafka TOPIC="$topic" TAG="$tag" ONLY=produce-batched \
    OUT="$WORK/bench-$tag.json" timeout -k 10 240 node bench.mjs ) || echo "::warning::$tag bench failed"
}
shadow_bench bench-shadow-async shadow-async async
shadow_bench bench-shadow-sync  shadow-sync  sync

node "$HERE/assemble.mjs" "$WORK" "$OUT_DIR/bench.json"
