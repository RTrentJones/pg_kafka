#!/usr/bin/env bash
# pg_kafka soak test (wave 3, stream `worker`).
#
# Runs kafka_test's `soak` driver (kafka_test/src/bin/soak.rs) against a live broker for
# DURATION seconds (default 1800), with:
#   * an idempotent and a transactional producer, two consumer groups;
#   * time partitions 5 minutes wide and per-topic retention (3 minutes), so the
#     retention sweep trims rows and drops whole leaves while traffic runs;
#   * pg_terminate_backend() of the listener every 2 minutes;
#   * DROP EXTENSION + CREATE EXTENSION between epochs (every 10 minutes);
# and fails unless:
#   * no acknowledged record is lost and no idempotent record is stored twice;
#   * committed transactions are visible, aborted ones never are (also as seen by a
#     read_committed group), no row is left `pending`;
#   * the listener's resident memory does not grow by more than 128 MiB within one
#     incarnation;
#   * the server log of the run holds no unexpected ERROR/FATAL/PANIC/WARNING (expected
#     ones, e.g. the FATAL of a terminated worker or client-caused request errors, are
#     counted and listed) and no "deadlock detected".
#
# DESTRUCTIVE: it drops and re-creates the extension (all Kafka data) several times.
# Use a scratch cluster. The listener's GUCs it changes are reset at the end.
#
# Usage:
#   PGHOST=127.0.0.1 PGPORT=28814 PGUSER=postgres SERVER_LOG=/path/to/server.log \
#   KAFKA_BOOTSTRAP=127.0.0.1:9092 scripts/soak.sh [DURATION_SECS]
# Extra driver flags: SOAK_ARGS="--epoch-secs 300 --terminate-secs 60".
# A prebuilt driver: SOAK_BIN=/path/to/soak (else it is built with cargo).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DURATION="${1:-${DURATION:-1800}}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-28814}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export PGHOST PGPORT PGUSER PGDATABASE
PSQL=(psql -X -q -At -v ON_ERROR_STOP=1)

if [ -z "${SOAK_BIN:-}" ]; then
  (cd "$ROOT/kafka_test" && cargo build --release --bin soak)
  SOAK_BIN="${CARGO_TARGET_DIR:-$ROOT/kafka_test/target}/release/soak"
fi

if [ -z "${SERVER_LOG:-}" ]; then
  # The listener's stderr is the server log when Postgres logs to a file (pg_ctl -l).
  pid="$("${PSQL[@]}" -c "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'")"
  SERVER_LOG="$(readlink "/proc/$pid/fd/2" 2>/dev/null || true)"
fi
if [ -n "$SERVER_LOG" ] && [ -f "$SERVER_LOG" ]; then
  LOG_FROM=$(stat -c %s "$SERVER_LOG")
else
  echo "WARNING: server log not found (set SERVER_LOG); the log check is skipped" >&2
  SERVER_LOG=""
fi

reset_gucs() {
  "${PSQL[@]}" -c "ALTER SYSTEM RESET pg_kafka.message_partition_minutes" \
               -c "SELECT pg_reload_conf()" >/dev/null || true
}
trap reset_gucs EXIT
"${PSQL[@]}" -c "ALTER SYSTEM SET pg_kafka.message_partition_minutes = 5" \
             -c "SELECT pg_reload_conf()" >/dev/null

echo "soak: ${DURATION}s against $KAFKA_BOOTSTRAP (db $PGHOST:$PGPORT)"
status=0
"$SOAK_BIN" --duration-secs "$DURATION" --bootstrap "$KAFKA_BOOTSTRAP" \
  --db "host=$PGHOST port=$PGPORT user=$PGUSER dbname=$PGDATABASE" ${SOAK_ARGS:-} || status=$?

if [ -n "$SERVER_LOG" ]; then
  segment="$(mktemp)"
  tail -c +"$((LOG_FROM + 1))" "$SERVER_LOG" > "$segment"
  # Expected during a soak: the FATAL of a worker terminated in a transaction, client
  # request errors the handlers report (fencing, rebalances, unknown members), the
  # batch errors and lock waits caused by DROP EXTENSION, DROP's own NOTICEs.
  expected='terminating background worker "pg_kafka_listener" due to administrator command|Failed to handle [A-Za-z]+ request|Client disconnected before response flush|Request batch failed and was rolled back.*(lock timeout|extension)|canceling statement due to lock timeout|could not (create|drop) time partition .*lock timeout|partition maintenance skipped'
  echo "server log: $(wc -l < "$segment") lines in the run"
  grep -E 'deadlock detected' "$segment" && { echo "FAIL: deadlock detected"; status=1; } || true
  grep -E '(ERROR|FATAL|PANIC|WARNING|panicked)' "$segment" \
    | sed -E 's/^[0-9-]+ [0-9:.]+ [A-Z]+ \[[0-9]+\] //; s/[0-9]+/N/g' | cut -c1-160 \
    | sort | uniq -c | sort -rn > "$segment.kinds" || true
  echo "log message kinds (count, message):"
  sed 's/^/  /' "$segment.kinds"
  unexpected="$(grep -vE "$expected" "$segment.kinds" || true)"
  if [ -n "$unexpected" ]; then
    echo "FAIL: unexpected log messages:"
    echo "$unexpected" | sed 's/^/  /'
    status=1
  fi
  rm -f "$segment" "$segment.kinds"
fi

if [ "$status" -eq 0 ]; then echo "SOAK PASSED"; else echo "SOAK FAILED"; fi
exit "$status"
