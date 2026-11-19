# ADR-001: Partitioning and Retention Strategy

**Status:** Accepted and implemented in 0.1.0. The sections from "Summary" on are the
original 2026-01 proposal, kept for the record; where they differ, "As built" below wins.
**Date:** 2026-01-02
**Last Reviewed:** 2026-11-18
**Context:** Based on PostgreSQL queue/pubsub benchmark findings

## As built (0.1.0)

`kafka.messages` is `PARTITION BY RANGE (created_at)`. Retention drops a whole leaf
("time partition", the analogue of a Kafka log segment) instead of deleting it row by
row, wherever every row in the leaf is expired under its own topic's rule.

### Layout

- **Leaves**: width `pg_kafka.message_partition_minutes`
  (default 1440 = one day, 5 minutes to 30 days, SIGHUP), aligned to UTC multiples of the
  width, named `kafka.messages_pYYYYMMDD_HHMMSS` after their (UTC) lower bound.
- **`kafka.messages_default`**, the DEFAULT leaf, catches any row whose leaf does not exist
  (worker late, or a row written out of band with an old/future `created_at`). A Produce
  therefore never fails because maintenance was late. It is normally empty.
- **Maintenance** (`kafka.ensure_message_partitions(width, ahead)`; the worker calls it at
  start and every 60 s sweep interval, operators via `SELECT pg_kafka_run_partition_maintenance()`)
  creates the leaf for now and the next two widths, and gives every row in the DEFAULT leaf a
  proper leaf (create the table standalone, move the rows with one `DELETE ... RETURNING` /
  `INSERT`, attach it: Postgres refuses `CREATE ... PARTITION OF` while the default holds rows
  for the range). Only gaps are filled, so changing the width never produces overlapping leaves;
  existing leaves keep their bounds.
- **Catalog listing:** `kafka.message_partitions()` returns every leaf with its bounds
  (parsed from `pg_get_expr(relpartbound)` with DateStyle pinned so they round-trip).
- **Monitoring:** `kafka.metrics` sums `reltuples` / `pg_table_size` over the leaves (the
  partitioned parent stores nothing) and reports `messages.time_partitions`.

### Uniqueness

Postgres only accepts a unique constraint on a partitioned table if it includes the partition
key, so the parent has no primary key and no `UNIQUE (global_offset)`. What actually makes
offsets unique has always been the allocator: `partition_offset` comes from
`kafka.partition_offsets.next_offset`, read and advanced under the per-partition advisory lock
and the counter row's lock in the same transaction, and never decremented (BUG-3);
`global_offset` comes from one sequence. As a backstop every leaf carries
`UNIQUE (topic_id, partition_id, partition_offset)` and `UNIQUE (global_offset)` btrees
(created by `kafka.create_message_partition` for every leaf, by bootstrap for the DEFAULT leaf,
and inherited from the old primary key / unique constraint by the upgraded leaf). A duplicate
inside one leaf is still rejected; a duplicate across leaves would need the counter to go
backwards, which nothing does.

Fetch, ListOffsets and the HWM/LSO probes do not name `created_at`, so they cannot prune: the
planner runs a MergeAppend (or one LIMIT 1 probe per leaf for MIN/MAX) over every leaf's unique
btree. The cost is one btree descent per leaf, which stays cheap because the leaf count is
bounded (retention window / width + 3 + DEFAULT, e.g. 11 leaves for 7-day retention with daily
leaves). Very small widths with long retention multiply the leaf count and should be avoided.

Planning is the larger cost: planning a statement over a partitioned table expands every leaf,
so with 11 leaves a one-shot `MAX(partition_offset)` probe cost 0.75 ms against 0.10 ms on the
unpartitioned table (pgbench, simple protocol), almost all of it planning. The hot read
statements in `storage/postgres.rs` (produce's base-offset probe, Fetch, the partition-bounds,
HWM and earliest-offset probes) therefore run through prepared plans kept for the life of the
backend (`select_cached`, `SPI_keepplan`; the plan cache re-plans them when a leaf is created
or dropped). With cached plans, at 11 leaves: Fetch of 100 rows 0.20 ms vs 0.135 ms, MAX probe
0.19 ms vs 0.05 ms. The plans execute with `read_only = false` so each takes a fresh READ
COMMITTED snapshot like the one-shot statements: executed read-only, a cached plan reused a
snapshot older than the group-commit batch's latest requests and Fetch reported a stale
high watermark (caught by `test_fetch_bounds_from_single_statement`).

### Kafka-visible behaviour

- **Unchanged:** offsets, HWM, log start, Fetch, ListOffsets (earliest/latest/timestamp),
  DeleteRecords, transactions, shadow outbox/replay. No UPDATE the extension issues changes
  `created_at` (EndTxn only sets `txn_state`), so no row ever moves between leaves.
- **Retention granularity (the one deliberate change):** a topic whose window is at least one
  width is expired a leaf at a time, like Kafka segment retention (a segment goes once its
  newest record is past `retention.ms`): its rows are removed once the leaf's upper bound is past
  the window, so a row lives at most `window + width`. A topic whose window is shorter than one
  width keeps exact row-by-row expiry (as before 0.1.0), so a short `retention.ms` is not
  stretched to a day. Rows still in the DEFAULT leaf are always expired row by row.

### The sweep (`PostgresStore::run_retention_sweep`)

1. For every past leaf (upper bound <= now) it asks `kafka.message_partition_contents(leaf)` for
   the (topic, partition) pairs present (a loose index scan, one descent per pair) and whether a
   pending transactional row is there, then `retention::classify_leaf` decides:
   **Drop** when every topic present is wholly expired (`upper + window <= now`) and nothing is
   pending; **Trim** when some topics are wholly expired but the leaf must stay (a longer or
   infinite window, or a pending row): those topics' rows in that leaf get the existing bounded
   row delete; **Keep** otherwise. Empty past leaves are dropped.
2. Row-level expiry for short windows (and for the DEFAULT leaf), per (topic, leaf), with the
   same 10,000-row batches and 1.5 s pass budget as before. Each statement is confined to one
   leaf (the leaf's bounds make the planner prune to it, and `tableoid` keeps `ctid`
   unambiguous; the DEFAULT leaf is addressed directly).
3. Drops last (at most 64 per pass), through `kafka.drop_message_partition(leaf, expired_topics)`,
   which re-checks under `ACCESS EXCLUSIVE` on `kafka.messages` that the leaf holds only listed
   topics and no pending row, so a concurrent writer cannot lose a row.
4. `log_start_offset` of every touched (topic, partition) advances to the oldest surviving offset
   or the HWM, only forward (GREATEST), exactly as for row deletes. Offsets are assigned in
   `created_at` order, so a dropped leaf is an offset prefix of each partition it held; the only
   transient exception is a topic whose older rows sit in a non-droppable leaf awaiting the Trim
   delete of the same pass, and those rows are expired too.

A pending transactional row blocks its leaf's drop until the transaction ends (the transaction
timeout bounds that). `pg_kafka_run_retention_sweep()` reports the drops as `dropped_partitions`.

### Concurrency with the worker and with manual maintenance

Maintenance can run in the worker and, at the same time, in any session
(`pg_kafka_run_partition_maintenance()`, `pg_kafka_run_retention_sweep()`). The rules:

* **Moving rows out of the DEFAULT leaf never overlaps a worker transaction.** The move
  (`DELETE` from `kafka.messages_default` + `INSERT` into the new leaf, in
  `create_message_partition`) would otherwise race the worker's UPDATEs of `kafka.messages`
  (EndTxn's `txn_state` change, the timeout/fencing aborts, DeleteRecords): an UPDATE that
  meets a row the move deleted skips it once the move commits and never sees its copy,
  leaving a committed transaction `pending` forever. So every worker transaction
  starts by taking the pg_kafka schema advisory lock (key 8099547369649367906) in SHARE
  mode (`worker::lock_schema`), and the move takes it EXCLUSIVE, bounded by the function's
  `lock_timeout`; on a timeout the leaf is not created and the next pass retries. The
  upgrade scripts take the same lock first.
* **Every lock wait in maintenance is bounded and handled.** `ensure_message_partitions`
  takes its serializing advisory lock (8099547369649367905) inside a `lock_not_available`
  handler under `lock_timeout = 2s`: a session that ran maintenance and stayed open makes
  the next pass skip with a WARNING instead of stalling it (and, for the worker, the
  whole broker). `drop_message_partition` takes the same lock and `ACCESS EXCLUSIVE` on
  `kafka.messages` inside its handler, so a timeout skips that leaf instead of rolling
  back the caller's whole sweep.
* **A leaf dropped concurrently is "not dropped", not an error.** When two sweeps pick
  the same leaf, the second waits for the first's locks; afterwards
  `drop_message_partition` (and `message_partition_contents`) find the leaf gone and
  return `dropped = false` (NULL contents) instead of erroring on the vanished OID.
* The worker's own lock waits are bounded by `pg_kafka.worker_lock_timeout_ms` (default
  5 s), which applies to these locks too.

### Least privilege

The worker runs as `pgkafka_worker`, which has no DDL rights. All partition DDL runs inside
SECURITY DEFINER functions owned by the extension owner, each with `SET search_path = pg_catalog,
pg_temp`, schema-qualified names and `lock_timeout = 2s` around the DDL (a lock that cannot be had
in time is a WARNING and a retry next pass, never a stalled worker). EXECUTE is revoked from PUBLIC
and granted to `pgkafka_worker` only for `ensure_message_partitions`, `message_partition_contents`
and `drop_message_partition`; `create_message_partition` is internal. Each function refuses any
relation that is not a leaf of `kafka.messages` (and `drop` refuses the DEFAULT leaf), so the
grant is no wider than the worker's existing INSERT/DELETE on `kafka.messages`: it can create
leaves of that table and drop leaves whose rows it may delete anyway. The worker reads and writes
leaves only through `kafka.messages`, plus `SELECT, DELETE` on `kafka.messages_default` for the
row delete there. `storage/privileges.rs` lists the function grants (`WORKER_FUNCTION_GRANTS`) and
the startup role check verifies them with `has_function_privilege`.

### Upgrade from 0.0.0

`ALTER EXTENSION pg_kafka UPDATE` renames the populated table to `kafka.messages_legacy`
(renaming its primary key / unique index / constraints to the per-leaf names), creates the
partitioned parent (same `global_offset` sequence, now owned by the parent) with the DEFAULT leaf,
and attaches the old table as one leaf `[MINVALUE, upgrade time)`; the rows are not copied. It
reads the old table three times (MAX(created_at) for the bound, the attach's bound check, the BRIN
build) under ACCESS EXCLUSIVE. The legacy leaf is then retained and dropped like any past leaf.
The views that read the table are recreated on the parent. A new binary
running before the update (docs/UPGRADING.md step 2) detects the missing partitioning and
skips partition maintenance; its retention sweep expires rows row by row on the
unpartitioned table. `scripts/check-upgrade.sh` compares
the upgraded and fresh schemas with leaves normalized (each leaf's index set and grants must be
identical; leaf names and counts are data-dependent), then checks that maintenance works on both,
that the legacy leaf drops whole without losing the sequence, and that `pg_dump` emits no kafka
table.

### pg_dump / restore

Every leaf is made a member of the extension (`ALTER EXTENSION pg_kafka ADD TABLE`, and `DROP`
before dropping it), like the tables `CREATE EXTENSION` made. pg_dump therefore treats leaves
exactly as it always treated `kafka.messages`: it dumps `CREATE EXTENSION pg_kafka` and no kafka
table or data (pg_kafka never marked its tables with `pg_extension_config_dump`). A restore gets a
fresh, empty schema with the DEFAULT leaf, and the worker creates the time leaves. If leaves were
not members, pg_dump would emit them, their `ATTACH` and their rows, and the restore would fail on
the foreign key to the (not dumped) `kafka.topics`. Back up pg_kafka data physically (base
backup + WAL), as before. `pg_upgrade` (binary-upgrade dump of extension members) was not tested.

### PostgreSQL versions

Only features present since PG11-12 are used (declarative RANGE partitioning with DEFAULT,
partitioned BRIN/partial indexes, FKs from partitioned tables, `pg_partition_tree`,
`ATTACH PARTITION`, run-time partition pruning); no catalog column or syntax newer than PG14.
Measured on PG15; CI runs the E2E suite (time partitions included) on PG14-18.

### Measured (PG15 sandbox, 1,000,000 expired + 100,000 retained rows, one topic, retention 1 day)

| | before (row DELETE sweep) | after (partition drop) |
|---|---|---|
| sweep passes until caught up | 3 | 1 (+1 no-op) |
| wall time of the sweep calls | 2.94 s | 0.35 s |
| rows deleted / leaves dropped | 1,000,000 / - | 0 / 3 |
| WAL written by the sweep | 240.5 MB | 0.16 MB |
| dead tuples left | 1,002,360 | 0 |
| following VACUUM | 0.37 s, 59 MB WAL | 0.04 s, 8 kB WAL |
| `kafka.messages` size afterwards | 248 MB (space kept) | 17 MB |

`performance` E2E category (rdkafka clients, msgs/s; three runs each, noisy shared 4-core box):

| | produce | consume | produce P50 |
|---|---|---|---|
| before (unpartitioned) | 120-140 | 433-435 | 6.7-8.0 ms |
| after, 4 leaves | 142-145 | 417-434 | 6.8-6.9 ms |
| after, 11 leaves | 138-142 | 416-420 | 7.0-7.1 ms |
| after, 11 leaves, before plan caching | 123-127 | 416-418 | 7.5-7.7 ms |


## Summary

This document outlines the architectural decision for implementing table partitioning and retention policies in `pg_kafka`. It addresses the critical performance issue of unbounded table growth while acknowledging the tradeoffs involved.

## Problem Statement

The current Phase 2 implementation stores all messages in a single `kafka.messages` table with no retention mechanism. This leads to:

- **Unbounded Growth:** Table grows indefinitely (1M → 10M → 100M+ rows)
- **Performance Degradation:** Query latency increases linearly with table size
- **Index Bloat:** Indexes become multi-GB and slow down even indexed queries
- **Expensive Maintenance:** `VACUUM` operations take hours on large tables
- **Eventual Failure:** Disk exhaustion causes database crash

**Benchmark Evidence:**
The [pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark) demonstrates that PostgreSQL can handle 1M+ reads/sec, but performance degrades significantly beyond 50M rows even with proper indexing.

## Decision

Implement **time-based table partitioning** with **configurable retention policies** in Phase 3.

### Architecture

```sql
-- Partitioned parent table
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,
    partition_offset BIGINT NOT NULL,
    key BYTEA,
    value BYTEA,
    headers JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Child partitions (one per day)
CREATE TABLE kafka.messages_2026_01_01 PARTITION OF kafka.messages
    FOR VALUES FROM ('2026-01-01') TO ('2026-01-02');

CREATE TABLE kafka.messages_2026_01_02 PARTITION OF kafka.messages
    FOR VALUES FROM ('2026-01-02') TO ('2026-01-03');
-- ... etc
```

### Retention Policy

```sql
-- Configurable via GUC
pg_kafka.retention_days = 7  -- Default: 7 days

-- Automatic partition management
SELECT kafka.ensure_next_partition();  -- Creates tomorrow's partition
SELECT kafka.drop_old_partitions();     -- Drops partitions older than retention_days
```

### Background Worker Integration

A background worker task runs daily to:
1. Create the next day's partition (proactive)
2. Drop partitions older than `retention_days`

## Tradeoffs

### ✅ Benefits

1. **Constant Query Performance**
   - Each partition stays small (e.g., 1 day of data)
   - Query latency remains <10ms regardless of total historical volume
   - Partition pruning skips irrelevant partitions automatically

2. **O(1) Data Deletion**
   - `DROP TABLE` is instant (vs hours for `DELETE FROM ... WHERE`)
   - No `VACUUM` bloat from deletions
   - Disk space immediately reclaimed

3. **Predictable Resource Usage**
   - Disk usage bounded by retention window
   - Each partition's maintenance cost is constant
   - No runaway table growth

4. **Better Maintenance**
   - `VACUUM` per partition completes in seconds (vs hours)
   - Smaller indexes fit in memory
   - Easier to backup/restore individual time ranges

### ❌ Drawbacks

1. **Loss of Long-Term Storage**
   - **Critical:** Old messages are permanently deleted after retention period
   - Cannot replay historical data beyond retention window
   - No way to recover dropped partitions (they're gone forever)

2. **Operational Complexity**
   - Requires partition management background worker
   - More moving parts (partition creation/dropping logic)
   - Migration from non-partitioned to partitioned schema needed

3. **Schema Inflexibility**
   - Partition key (`created_at`) cannot be changed without full rebuild
   - Changing partition interval (daily → hourly) requires migration
   - All queries must be aware of partition key for optimal performance

4. **Not True Kafka Semantics**
   - Real Kafka: Retention by log segments (can be very long, e.g., months/years)
   - `pg_kafka`: Aggressive retention required for performance (typically days)
   - Users migrating from real Kafka may expect longer retention

5. **Global Offset Gaps**
   - `global_offset` (BIGSERIAL) doesn't reset per partition
   - Dropped partitions leave permanent gaps in the sequence
   - Acceptable for Kafka clients (they tolerate gaps), but may confuse users

## Alternatives Considered

### Alternative 1: Keep Unbounded Table (Status Quo)

**Rejected:** Benchmark data proves this hits performance cliff at ~50M rows. Not viable for production.

### Alternative 2: External Archival (Messages Table + Archive Table)

```sql
-- Active messages (last N days)
CREATE TABLE kafka.messages_active (...);

-- Archive table (compressed, read-only)
CREATE TABLE kafka.messages_archive (...) WITH (fillfactor = 100);

-- Background worker moves old data to archive
INSERT INTO kafka.messages_archive SELECT * FROM kafka.messages_active WHERE ...;
DELETE FROM kafka.messages_active WHERE ...;
```

**Pros:**
- Preserves long-term data in archive
- Active table stays small

**Cons:**
- `DELETE` from active table causes massive bloat (same problem as unbounded growth)
- Complex query logic (must `UNION` active + archive for historical queries)
- Archive table still grows unbounded (just slower)

**Decision:** Rejected due to `DELETE` bloat issue. Partitioning is cleaner.

### Alternative 3: Columnar Storage for Old Partitions

Use PostgreSQL's `cstore_fdw` or similar for archival partitions:

```sql
-- Recent partition: Standard heap storage
CREATE TABLE kafka.messages_2026_01_01 PARTITION OF kafka.messages ...;

-- Old partition: Columnar storage (10x compression, read-only)
CREATE FOREIGN TABLE kafka.messages_2025_12_01
    SERVER cstore_server
    OPTIONS (compression 'pglz');
```

**Pros:**
- Preserves historical data with 90% less disk
- Transparent to queries (still looks like one table)

**Cons:**
- Requires external extension (`cstore_fdw`)
- Adds operational complexity
- Read-only (no updates/deletes on archived partitions)

**Decision:** Defer to Phase 4+. Consider as future enhancement, but implement basic partitioning first.

### Alternative 4: Configurable Retention Per Topic

```sql
-- Different retention per topic
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT DEFAULT 1,
    retention_days INT DEFAULT 7  -- Per-topic override
);
```

**Pros:**
- Flexibility: Critical topics get longer retention
- Granular control

**Cons:**
- Complex partition management (need per-topic partition sets)
- Queries across topics become harder to optimize
- Much more complex background worker logic

**Decision:** Defer to Phase 3.5. Start with global retention, add per-topic later if needed.

## Implementation Recommendations

### Phase 3.1: Core Partitioning

1. Create migration script: `sql/migrate_to_partitioned.sql`
2. Implement partition management functions:
   - `kafka.ensure_next_partition()` - Creates tomorrow's partition
   - `kafka.drop_old_partitions()` - Drops old partitions
3. Add GUC parameter: `pg_kafka.retention_days`
4. Update background worker to run partition management daily

### Phase 3.2: Monitoring & Safety

1. Add partition count metrics:
   ```sql
   SELECT COUNT(*) AS partition_count
   FROM pg_tables
   WHERE schemaname = 'kafka' AND tablename LIKE 'messages_%';
   ```

2. Log partition operations:
   ```
   INFO: Created partition messages_2026_01_03
   INFO: Dropped partition messages_2025_12_26 (1,234,567 rows)
   ```

3. Add safety checks:
   - Warn if partition count exceeds expected (indicates stalled cleanup)
   - Prevent dropping partitions if retention_days = 0 (infinite retention)

### Phase 3.3: Documentation

1. Clearly document the **data loss** aspect:
   > ⚠️ **WARNING:** pg_kafka automatically deletes messages older than `pg_kafka.retention_days`.
   > This is PERMANENT. If you need long-term storage, use Shadow Mode to replicate to real Kafka.

2. Provide guidance on choosing retention period:
   - Dev/test: 1-3 days sufficient
   - Production: 7-30 days typical
   - Warning: >30 days may degrade performance (large partitions)

3. Document recovery options:
   - Use WAL archiving for disaster recovery
   - Use Shadow Mode replication for long-term storage
   - Regular backups via `pg_dump` (per partition)

## Success Metrics

### Performance Targets

| Metric | Target (Phase 3) |
|--------|-----------------|
| Query latency (p50) | <5ms regardless of total historical rows |
| Query latency (p99) | <20ms regardless of total historical rows |
| VACUUM time per partition | <30 seconds |
| Disk usage | Bounded by `retention_days * daily_write_rate` |

### Operational Targets

| Metric | Target |
|--------|--------|
| Partition creation success rate | 100% (no missed days) |
| Partition dropping success rate | 100% (cleanup always runs) |
| Partition count stability | Constant (equals `retention_days + 1`) |

## User Migration Guide

### For Existing Deployments

**If you have <1M messages (small dataset):**
1. Run migration script during maintenance window
2. Verify data integrity
3. Resume operations
4. **Downtime:** ~5 minutes

**If you have >1M messages (large dataset):**
1. Enable Shadow Mode replication to external Kafka (preserves long-term data)
2. Create partitioned table alongside existing table
3. Backfill recent data (last `retention_days`) to new table
4. Switch writes to partitioned table
5. Verify dual-write working
6. Drop old table after retention period
7. **Downtime:** Zero (dual-write strategy)

### Configuration Examples

```sql
-- Development (short retention, saves disk)
pg_kafka.retention_days = 1

-- Production (balanced)
pg_kafka.retention_days = 7

-- Long-term storage (WARNING: may impact performance)
pg_kafka.retention_days = 30

-- Infinite retention (NOT RECOMMENDED: performance cliff inevitable)
pg_kafka.retention_days = 0  -- Disables auto-dropping
```

## When NOT to Use pg_kafka

Based on this retention limitation:

### ❌ Use Real Kafka Instead If:

1. **Long-term Event Sourcing**
   - Need to replay events from months/years ago
   - Event log is the source of truth (cannot delete)

2. **Compliance Requirements**
   - Legal requirement to retain messages >30 days
   - Audit logs that must be immutable

3. **Massive Scale**
   - >1M messages/sec sustained
   - Multi-datacenter replication
   - Need Kafka's distributed architecture

### ✅ pg_kafka is Good For:

1. **Recent Event Processing**
   - Real-time dashboards (last 7 days)
   - Alert systems (recent data only)
   - Session tracking (short-lived data)

2. **Development/Testing**
   - Local Kafka replacement for dev environments
   - CI/CD pipelines (ephemeral data)

3. **Migration Path**
   - Start with pg_kafka, grow into real Kafka
   - Use Shadow Mode for zero-downtime cutover

## Open Questions

1. **Should we support partition-by-size instead of partition-by-time?**
   - Pro: More predictable partition sizes
   - Con: Harder to implement, non-standard

2. **Should retention be configurable at topic level?**
   - Pro: Flexibility (critical topics get longer retention)
   - Con: Complexity (per-topic partition management)

3. **Should we compress old partitions instead of dropping them?**
   - Pro: Preserves data, saves disk
   - Con: Requires extensions like `cstore_fdw`

**Decision:** Revisit in Phase 3.5 after core partitioning is stable.

## References

- [pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark) - Performance validation
- [PostgreSQL Partitioning Docs](https://www.postgresql.org/docs/current/ddl-partitioning.html)
- [Kafka Log Retention](https://kafka.apache.org/documentation/#retention) - For comparison

---

**Next Steps:**
- [x] Implement partition management functions (0.1.0, see "As built")
- [ ] Test the 0.0.0 upgrade with 10M+ rows (checked with small data in `scripts/check-upgrade.sh`)
- [x] Document data loss behavior in user-facing docs ([PERFORMANCE.md](../PERFORMANCE.md#retention--lifecycle-gucs-shipped))
- [x] Add monitoring for partition operations (`kafka.metrics` `messages.time_partitions`, worker log lines)
