#!/usr/bin/env bash
# Quick rebuild-and-restart loop for pg_kafka development against the pgrx-managed PostgreSQL.
#
#   scripts/restart.sh            rebuild, install (.so + generated SQL + control file),
#                                 restart PostgreSQL; the data is kept, and the worker comes
#                                 back up with the new binary
#   scripts/restart.sh --fresh    also DROP EXTENSION pg_kafka CASCADE + CREATE EXTENSION:
#                                 this DELETES EVERY TOPIC, MESSAGE, OFFSET AND SASL USER
#
# PG_MAJOR selects the pgrx-managed major (default 14). The server must have pg_kafka in
# shared_preload_libraries (see README.md, "Configuration").
set -euo pipefail
cd "$(dirname "$0")/.."

PG_MAJOR=${PG_MAJOR:-14}
FRESH=false
case "${1:-}" in
  --fresh) FRESH=true ;;
  "") ;;
  *) echo "usage: $0 [--fresh]" >&2; exit 2 ;;
esac

PG_CONFIG="$(cargo pgrx info pg-config "pg$PG_MAJOR")"
PSQL="$("$PG_CONFIG" --bindir)/psql"
# pgrx runs each major on port 28800 + major
PORT=$((28800 + PG_MAJOR))

echo "=== Stopping PostgreSQL $PG_MAJOR ==="
cargo pgrx stop "pg$PG_MAJOR" || true

echo "=== Building and installing the extension (release) ==="
# cargo pgrx install writes the shared library AND the generated pg_kafka--<version>.sql,
# the upgrade script and pg_kafka.control, so schema changes are picked up too.
cargo pgrx install --release --no-default-features --features "pg$PG_MAJOR" --pg-config "$PG_CONFIG"

echo "=== Starting PostgreSQL $PG_MAJOR ==="
cargo pgrx start "pg$PG_MAJOR"

if $FRESH; then
  echo "=== Recreating the extension: all pg_kafka data is dropped ==="
  # Stop the listener first so DROP EXTENSION does not wait on (or deadlock with) the worker.
  "$PSQL" -h localhost -p "$PORT" -d postgres -v ON_ERROR_STOP=1 <<'SQL'
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener';
DROP EXTENSION IF EXISTS pg_kafka CASCADE;
CREATE EXTENSION pg_kafka;
SQL
else
  "$PSQL" -h localhost -p "$PORT" -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS pg_kafka" \
    -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_kafka'"
  echo "(after a schema change: ALTER EXTENSION pg_kafka UPDATE, see docs/UPGRADING.md, or --fresh)"
fi

echo "=== Waiting for the Kafka listener ==="
timeout 30 bash -c 'until (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null; do sleep 1; done'

echo "=== Building the E2E test client ==="
(cd kafka_test && cargo build --release)

echo ""
echo "Done. Run the E2E suite with:"
echo "  cd kafka_test && DATABASE_URL=postgresql://localhost:$PORT/postgres ./target/release/kafka_test --exclude performance --exclude shadow"
