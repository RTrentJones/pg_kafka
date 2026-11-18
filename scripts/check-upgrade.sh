#!/usr/bin/env bash
# Check the 0.0.0 -> 0.1.0 upgrade path of the pg_kafka extension.
#
# Against a running Postgres that has the CURRENT pg_kafka build installed
# (`cargo pgrx install`), this:
#   1. installs the frozen 0.0.0 install script (sql/upgrade-test/pg_kafka--0.0.0.sql)
#      next to the extension, so `CREATE EXTENSION pg_kafka VERSION '0.0.0'` works;
#   2. creates database A with a fresh `CREATE EXTENSION pg_kafka` (current version);
#   3. creates database B at 0.0.0, writes some data, runs
#      `ALTER EXTENSION pg_kafka UPDATE`;
#   4. fails unless B reports the current version, kept its data, and its kafka
#      schema (columns, indexes, table/column/schema/function ACLs, roles) is
#      identical to A's.
#
# Both scratch databases are dropped afterwards. The background worker only serves
# the database named by pg_kafka.database, so these databases do not disturb it.
#
# Usage: PG_CONFIG=/path/to/pg_config PGHOST=... PGPORT=... PGUSER=... scripts/check-upgrade.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-pg_config}"
EXT_DIR="$("$PG_CONFIG" --sharedir)/extension"
OLD_SCRIPT="$ROOT/sql/upgrade-test/pg_kafka--0.0.0.sql"
FRESH_DB="pgkafka_upgrade_fresh_$$"
OLD_DB="pgkafka_upgrade_old_$$"
PSQL=(psql -X -q -v ON_ERROR_STOP=1 -At)

NEW_VERSION="$(sed -n 's/^version *= *"\(.*\)"/\1/p' "$ROOT/Cargo.toml" | head -1)"
echo "Checking pg_kafka 0.0.0 -> $NEW_VERSION upgrade (extension dir: $EXT_DIR)"

for f in "pg_kafka--$NEW_VERSION.sql" "pg_kafka--0.0.0--$NEW_VERSION.sql"; do
  if [ ! -f "$EXT_DIR/$f" ]; then
    echo "ERROR: $EXT_DIR/$f is missing; run cargo pgrx install first" >&2
    exit 1
  fi
done
cp "$OLD_SCRIPT" "$EXT_DIR/pg_kafka--0.0.0.sql"

cleanup() {
  "${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS $FRESH_DB" || true
  "${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS $OLD_DB" || true
}
trap cleanup EXIT

"${PSQL[@]}" -d postgres -c "CREATE DATABASE $FRESH_DB"
"${PSQL[@]}" -d postgres -c "CREATE DATABASE $OLD_DB"

"${PSQL[@]}" -d "$FRESH_DB" -c "CREATE EXTENSION pg_kafka"

"${PSQL[@]}" -d "$OLD_DB" <<'SQL'
CREATE EXTENSION pg_kafka VERSION '0.0.0';
INSERT INTO kafka.topics (name, partitions) VALUES ('upgrade-check', 1);
INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
SELECT id, 0, g, 'v'::bytea FROM kafka.topics, generate_series(0, 99) AS g
WHERE name = 'upgrade-check';
INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
SELECT id, 0, 100 FROM kafka.topics WHERE name = 'upgrade-check';
SQL
"${PSQL[@]}" -d "$OLD_DB" -c "ALTER EXTENSION pg_kafka UPDATE"

# A normalized description of everything the upgrade must reproduce.
describe() {
  "${PSQL[@]}" -d "$1" <<'SQL'
SELECT 'extension ' || extversion FROM pg_extension WHERE extname = 'pg_kafka';
SELECT 'column ' || table_name || '.' || column_name || ' ' || data_type || ' '
       || is_nullable || ' ' || coalesce(column_default, '')
FROM information_schema.columns WHERE table_schema = 'kafka'
ORDER BY 1;
SELECT 'index ' || indexname || ' ' || indexdef FROM pg_indexes
WHERE schemaname = 'kafka' ORDER BY 1;
SELECT 'acl ' || c.relname || ' ' || a.acl
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (SELECT unnest(coalesce(c.relacl, '{}'))::text AS acl) a
WHERE n.nspname = 'kafka' ORDER BY 1;
SELECT 'column-acl ' || c.relname || '.' || at.attname || ' ' || a.acl
FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (SELECT unnest(at.attacl)::text AS acl) a
WHERE n.nspname = 'kafka' AND at.attacl IS NOT NULL ORDER BY 1;
SELECT 'schema-acl ' || a.acl FROM pg_namespace n
CROSS JOIN LATERAL (SELECT unnest(coalesce(n.nspacl, '{}'))::text AS acl) a
WHERE n.nspname = 'kafka' ORDER BY 1;
SELECT 'function ' || p.oid::regprocedure::text || ' ' || coalesce(p.proacl::text, 'default')
FROM pg_proc p JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'pg_kafka'
ORDER BY 1;
SELECT 'role ' || rolname || ' login=' || rolcanlogin FROM pg_roles
WHERE rolname IN ('pgkafka_reader', 'pgkafka_worker') ORDER BY 1;
SQL
}

FRESH_OUT="$(mktemp)"
OLD_OUT="$(mktemp)"
describe "$FRESH_DB" > "$FRESH_OUT"
describe "$OLD_DB" > "$OLD_OUT"

status=0
if ! diff -u --label "fresh $NEW_VERSION install" --label "0.0.0 upgraded to $NEW_VERSION" \
    "$FRESH_OUT" "$OLD_OUT"; then
  echo "ERROR: the upgraded schema differs from a fresh install" >&2
  status=1
fi
if ! grep -qx "extension $NEW_VERSION" "$OLD_OUT"; then
  echo "ERROR: upgraded database does not report version $NEW_VERSION" >&2
  status=1
fi
kept="$("${PSQL[@]}" -d "$OLD_DB" -c "SELECT count(*) FROM kafka.messages")"
if [ "$kept" != "100" ]; then
  echo "ERROR: expected 100 messages to survive the upgrade, found $kept" >&2
  status=1
fi
rm -f "$FRESH_OUT" "$OLD_OUT"

if [ "$status" -eq 0 ]; then
  echo "OK: 0.0.0 -> $NEW_VERSION upgrade matches a fresh install and kept its data"
fi
exit "$status"
