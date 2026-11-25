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
#      schema (columns, indexes, CHECK/foreign-key constraints, table/column/schema/function
#      ACLs, function bodies, views, comments, roles, the partitioning of kafka.messages) is
#      identical to A's (function arguments with their defaults, strictness and owner;
#      relation storage parameters, persistence and owner; sequence parameters);
#   5. checks the time partitions work on both (ADR-001): maintenance creates leaves,
#      the converted 0.0.0 leaf drops whole, and pg_dump emits no kafka table (pg_dump
#      must be present: PG_DUMP names it when it is not in `pg_config --bindir`).
#
# Both scratch databases are dropped afterwards. The background worker only serves
# the database named by pg_kafka.database, so these databases do not disturb it.
#
# Usage: PG_CONFIG=/path/to/pg_config PGHOST=... PGPORT=... PGUSER=... scripts/check-upgrade.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-pg_config}"
EXT_DIR="$("$PG_CONFIG" --sharedir)/extension"
OLD_SCRIPT="${OLD_SCRIPT:-$ROOT/sql/upgrade-test/pg_kafka--0.0.0.sql}"
FRESH_DB="pgkafka_upgrade_fresh_$$"
OLD_DB="pgkafka_upgrade_old_$$"
ZONE_DB="pgkafka_upgrade_zone_$$"
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
  "${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS $ZONE_DB" 2>/dev/null || true
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
#
# kafka.messages is time-partitioned (ADR-001). Its leaves are data-dependent: a fresh
# install has only kafka.messages_default, an upgraded one also the converted 0.0.0
# table (kafka.messages_legacy), and the worker adds time leaves as time passes. So
# leaves are not listed one by one; instead every leaf's index set and grants are
# listed with the leaf name normalized away (DISTINCT): each leaf must look alike, and
# the same in both databases.
describe() {
  "${PSQL[@]}" -d "$1" <<'SQL'
CREATE TEMP TABLE leaves AS
SELECT c.oid, c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'kafka.messages'::regclass;
SELECT 'extension ' || extversion FROM pg_extension WHERE extname = 'pg_kafka';
SELECT 'partitioned kafka.messages ' || pg_get_partkeydef('kafka.messages'::regclass);
SELECT 'default-leaf ' || c.relname FROM pg_class c
WHERE c.oid IN (SELECT oid FROM leaves) AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT';
SELECT 'column ' || table_name || '.' || column_name || ' ' || data_type || ' '
       || is_nullable || ' ' || coalesce(column_default, '')
FROM information_schema.columns WHERE table_schema = 'kafka'
  AND table_name NOT IN (SELECT relname FROM leaves)
ORDER BY 1;
SELECT 'index ' || indexname || ' ' || indexdef FROM pg_indexes
WHERE schemaname = 'kafka' AND tablename NOT IN (SELECT relname FROM leaves) ORDER BY 1;
SELECT DISTINCT 'leaf-index ' || regexp_replace(regexp_replace(indexdef,
           '^CREATE (UNIQUE )?INDEX \S+ ON kafka\.\S+', 'CREATE \1INDEX <index> ON kafka.<leaf>'),
           '\s+', ' ', 'g')
FROM pg_indexes WHERE schemaname = 'kafka' AND tablename IN (SELECT relname FROM leaves)
ORDER BY 1;
-- Storage parameters, persistence (UNLOGGED/TEMP) and owner of every relation (tables,
-- indexes, sequences, views) outside the leaves, and of the leaves normalized.
SELECT 'relation ' || c.relname || ' kind=' || c.relkind::text
       || ' persistence=' || c.relpersistence::text || ' owner=' || pg_get_userbyid(c.relowner)
       || ' options=' || coalesce(array_to_string(c.reloptions, ','), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'kafka' AND c.oid NOT IN (SELECT oid FROM leaves)
  AND NOT EXISTS (SELECT 1 FROM pg_index x WHERE x.indexrelid = c.oid
                  AND x.indrelid IN (SELECT oid FROM leaves))
ORDER BY 1;
SELECT DISTINCT 'leaf-relation kind=' || c.relkind::text
       || ' persistence=' || c.relpersistence::text || ' owner=' || pg_get_userbyid(c.relowner)
       || ' options=' || coalesce(array_to_string(c.reloptions, ','), '')
FROM pg_class c
WHERE c.oid IN (SELECT oid FROM leaves)
   OR c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM leaves))
ORDER BY 1;
-- Sequence parameters (the producer-id sequence's start is time-based, checked below).
SELECT 'sequence ' || c.relname || ' ' || format_type(s.seqtypid, NULL)
       || ' start=' || CASE WHEN c.relname = 'producer_ids_producer_id_seq' THEN '<time>'
                            ELSE s.seqstart::text END
       || ' increment=' || s.seqincrement || ' min=' || s.seqmin || ' max=' || s.seqmax
       || ' cache=' || s.seqcache || ' cycle=' || s.seqcycle
FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'kafka' ORDER BY 1;
SELECT 'acl ' || c.relname || ' ' || a.acl
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (SELECT unnest(coalesce(c.relacl, '{}'))::text AS acl) a
WHERE n.nspname = 'kafka' AND c.oid NOT IN (SELECT oid FROM leaves) ORDER BY 1;
-- Leaves are only reached through kafka.messages: no grant to anyone but the owner,
-- except the worker's documented access to the DEFAULT leaf.
SELECT DISTINCT 'leaf-acl ' || CASE WHEN pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
                                    THEN 'default ' ELSE 'time ' END || a.acl
FROM pg_class c
CROSS JOIN LATERAL (SELECT unnest(coalesce(c.relacl, '{}'))::text AS acl) a
WHERE c.oid IN (SELECT oid FROM leaves)
  AND a.acl NOT LIKE pg_get_userbyid(c.relowner) || '=%'
ORDER BY 1;
SELECT 'column-acl ' || c.relname || '.' || at.attname || ' ' || a.acl
FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (SELECT unnest(at.attacl)::text AS acl) a
WHERE n.nspname = 'kafka' AND at.attacl IS NOT NULL ORDER BY 1;
SELECT 'schema-acl ' || a.acl FROM pg_namespace n
CROSS JOIN LATERAL (SELECT unnest(coalesce(n.nspacl, '{}'))::text AS acl) a
WHERE n.nspname = 'kafka' ORDER BY 1;
-- md5(prosrc): a function the upgrade script redefines must have the body of the fresh
-- install's, not only its signature and ACL (for a C function it is the symbol name).
SELECT 'function ' || p.oid::regprocedure::text || ' ' || coalesce(p.proacl::text, 'default')
       || ' secdef=' || p.prosecdef || ' ' || coalesce(array_to_string(p.proconfig, ','), '')
       || ' returns ' || pg_get_function_result(p.oid) || ' ' || p.provolatile::text
       || ' args=(' || pg_get_function_arguments(p.oid) || ')'
       || ' strict=' || p.proisstrict || ' parallel=' || p.proparallel::text
       || ' owner=' || pg_get_userbyid(p.proowner)
       || ' src=' || md5(p.prosrc)
FROM pg_proc p JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'pg_kafka'
ORDER BY 1;
-- CHECK and foreign-key constraints by definition (primary keys and unique constraints are
-- covered by their indexes above). On leaves the name is normalized away like the index
-- names: the converted 0.0.0 leaf keeps its own name for the foreign key it shares with the
-- parent (ATTACH adopts an equal constraint under the leaf's name).
SELECT 'constraint ' || c.relname || ' ' || co.conname || ' ' || pg_get_constraintdef(co.oid)
FROM pg_constraint co JOIN pg_class c ON c.oid = co.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'kafka' AND co.contype IN ('c', 'f') AND c.oid NOT IN (SELECT oid FROM leaves)
ORDER BY 1;
SELECT DISTINCT 'leaf-constraint ' || pg_get_constraintdef(co.oid)
FROM pg_constraint co
WHERE co.conrelid IN (SELECT oid FROM leaves) AND co.contype IN ('c', 'f')
ORDER BY 1;
SELECT 'view ' || c.relname || ' ' || md5(pg_get_viewdef(c.oid))
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'kafka' AND c.relkind = 'v' ORDER BY 1;
SELECT 'sequence-owner ' || s.relname || ' ' || t.relname || '.' || a.attname
FROM pg_depend d JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
JOIN pg_class t ON t.oid = d.refobjid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
JOIN pg_namespace n ON n.oid = s.relnamespace
WHERE n.nspname = 'kafka' AND d.deptype = 'a' ORDER BY 1;
-- Comments (COMMENT ON) of the schema, its relations, their columns and the extension's
-- functions: an upgrade that forgets to (re)write one leaves the 0.0.0 text or none.
-- Leaves are left out: the converted 0.0.0 leaf keeps the comments the 0.0.0
-- kafka.messages had, which no leaf of a fresh install has.
SELECT 'comment schema ' || md5(d.description) FROM pg_description d
WHERE d.classoid = 'pg_namespace'::regclass AND d.objoid = 'kafka'::regnamespace;
SELECT 'comment ' || c.relname || coalesce('.' || a.attname, '') || ' ' || md5(d.description)
FROM pg_description d JOIN pg_class c ON c.oid = d.objoid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid AND d.objsubid > 0
WHERE d.classoid = 'pg_class'::regclass AND n.nspname = 'kafka'
  AND c.oid NOT IN (SELECT oid FROM leaves) ORDER BY 1;
SELECT 'comment function ' || p.oid::regprocedure::text || ' ' || md5(d.description)
FROM pg_description d JOIN pg_proc p ON p.oid = d.objoid
JOIN pg_depend dep ON dep.objid = p.oid AND dep.deptype = 'e'
JOIN pg_extension e ON e.oid = dep.refobjid AND e.extname = 'pg_kafka'
WHERE d.classoid = 'pg_proc'::regclass ORDER BY 1;
-- Producer ids start from the extension's creation time (milliseconds since the epoch),
-- in a fresh install and after the upgrade alike, so a recreated extension never hands
-- out an id an earlier one used. Only whether the start is time-based is comparable.
SELECT 'producer-id-seq-time-based ' || (last_value > 1700000000000)
FROM kafka.producer_ids_producer_id_seq;
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

# The converted 0.0.0 table is one leaf covering [MINVALUE, upgrade time).
legacy="$("${PSQL[@]}" -d "$OLD_DB" -c "SELECT count(*) || ' ' || (lower_bound IS NULL) FROM kafka.messages_legacy, kafka.message_partitions() p WHERE p.partition_name = 'messages_legacy' GROUP BY lower_bound")"
if [ "$legacy" != "100 true" ]; then
  echo "ERROR: expected the 0.0.0 rows in leaf kafka.messages_legacy [MINVALUE, ...), got '$legacy'" >&2
  status=1
fi

# Partition maintenance and retention work on both: time leaves get created, a new
# row lands in one (not in the DEFAULT leaf), and in the upgraded database the legacy
# leaf can be dropped whole without losing the global_offset sequence.
for db in "$FRESH_DB" "$OLD_DB"; do
  out="$("${PSQL[@]}" -d "$db" <<'SQL'
SELECT kafka.ensure_message_partitions(1440) >= 3;
INSERT INTO kafka.topics (name, partitions) VALUES ('upgrade-check-new', 1);
INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
SELECT id, 0, 0, 'v'::bytea FROM kafka.topics WHERE name = 'upgrade-check-new';
SELECT count(*) = 0 FROM kafka.messages_default;
SQL
)"
  if [ "${out//$'\n'/ }" != "t t" ]; then
    echo "ERROR: partition maintenance check failed on $db: $out" >&2
    status=1
  fi
done
out="$("${PSQL[@]}" -d "$OLD_DB" <<'SQL'
SELECT dropped FROM kafka.drop_message_partition('kafka.messages_legacy'::regclass,
    ARRAY(SELECT id FROM kafka.topics WHERE name = 'upgrade-check'));
SELECT to_regclass('kafka.messages_legacy') IS NULL;
INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
SELECT id, 0, 1, 'v'::bytea FROM kafka.topics WHERE name = 'upgrade-check-new';
SELECT count(*) = 2 AND count(DISTINCT global_offset) = 2 FROM kafka.messages;
SQL
)"
if [ "${out//$'\n'/ }" != "t t t" ]; then
  echo "ERROR: dropping the legacy leaf failed: $out" >&2
  status=1
fi

# The legacy leaf's upper bound survives the session's DateStyle and TimeZone. Under a
# non-ISO DateStyle a timestamptz prints with a zone abbreviation that is read back
# through timezone_abbreviations, not TimeZone: Europe/Dublin's summer IST as Israel's
# +02 (the bound lands an hour before the newest row: the ATTACH fails), Asia/Kolkata's
# IST likewise (3 h 30 off). Each upgrade must succeed and the bound must be exactly
# one microsecond past the newest row (here written in the future, so it is the max).
for setting in "Postgres, MDY|Europe/Dublin" "SQL, DMY|Asia/Kolkata"; do
  datestyle="${setting%%|*}"
  timezone="${setting##*|}"
  "${PSQL[@]}" -d postgres -c "CREATE DATABASE $ZONE_DB"
  if ! out="$("${PSQL[@]}" -d "$ZONE_DB" 2>&1 <<SQL
CREATE EXTENSION pg_kafka VERSION '0.0.0';
INSERT INTO kafka.topics (name, partitions) VALUES ('upgrade-zone', 1);
INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
SELECT id, 0, 0, 'v'::bytea, timestamptz '2099-07-01 12:00:00+00' FROM kafka.topics;
SET datestyle = '$datestyle';
SET timezone = '$timezone';
ALTER EXTENSION pg_kafka UPDATE;
RESET datestyle;
RESET timezone;
SELECT p.upper_bound = (SELECT max(created_at) FROM kafka.messages) + INTERVAL '1 microsecond'
FROM kafka.message_partitions() p WHERE p.partition_name = 'messages_legacy';
SQL
)"; then
    echo "ERROR: the upgrade under DateStyle '$datestyle', TimeZone $timezone failed: $out" >&2
    status=1
  elif [ "$out" != "t" ]; then
    echo "ERROR: under DateStyle '$datestyle', TimeZone $timezone the legacy leaf's upper bound is not the newest row + 1 us: $out" >&2
    status=1
  fi
  "${PSQL[@]}" -d postgres -c "DROP DATABASE $ZONE_DB"
done

# Leaves are extension members: pg_dump leaves them out exactly like kafka.messages
# itself (a dump holds CREATE EXTENSION only), so a restore never meets an orphan leaf.
PG_DUMP="${PG_DUMP:-$("$PG_CONFIG" --bindir)/pg_dump}"
if [ ! -x "$PG_DUMP" ]; then
  echo "ERROR: pg_dump not found at $PG_DUMP (set PG_DUMP); the extension-member check needs it" >&2
  status=1
elif ! dump="$("$PG_DUMP" -d "$OLD_DB")"; then
  echo "ERROR: pg_dump of $OLD_DB failed" >&2
  status=1
else
  dumped="$(grep -c 'CREATE TABLE kafka\.' <<<"$dump" || true)"
  if [ "$dumped" != "0" ]; then
    echo "ERROR: pg_dump emitted $dumped kafka table(s); leaves must be extension members" >&2
    status=1
  fi
fi

if [ "$status" -eq 0 ]; then
  echo "OK: 0.0.0 -> $NEW_VERSION upgrade matches a fresh install and kept its data"
fi
exit "$status"
