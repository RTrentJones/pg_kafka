# Performance Considerations for pg_kafka

This document outlines performance benchmarks, tuning recommendations, and architectural decisions based on real-world PostgreSQL queue/pub-sub benchmarking.

## Measured pg_kafka Performance (the numbers that matter)

> pg_kafka's own measured numbers live in the evidence
> pipeline — `evidence/bench/` regenerates `bench.json` in the `evidence` workflow
> (`.github/workflows/evidence.yml`: weekly, on `v*` tags, or on demand) and the results
> render at [rtrentjones.dev/pg_kafka](https://rtrentjones.dev/pg_kafka).
> **Do not quote the external PostgreSQL benchmark figures as pg_kafka
> throughput**: pg_kafka serializes ALL SPI work through a single background
> worker thread (a deliberate correctness trade — see "Architecture ceiling"
> below), so raw multi-core PostgreSQL numbers are not reachable through the
> Kafka wire path.

For current figures, see `bench.json` on the `evidence` branch (produce/consume
throughput, latency percentiles, raw-INSERT floor, shadow-forwarding rates).
The E2E performance suite (`kafka_test --category performance`) enforces
regression bounds in CI, every test of it, each 5-100x from dev-machine measurements so that
only order-of-magnitude regressions fail: produce and consume throughput floors
(`throughput.rs`: awaited single sends, consume; `regression.rs`: 1000 concurrent sends and
their consume), concurrent sends no slower than half the sequential ones, P50/P99 ceilings of
an awaited produce, a time ceiling for ten producers' first sends, and a ceiling on the
listener's CPU time while a consumer long-polls an empty topic.

### Architecture ceiling

Every Produce/Fetch/Offset/Txn request executes on **one** DB thread, one at a
time. This makes the in-memory coordinator and check-then-act SQL race-free
without locks, at the cost of a hard single-core ceiling for SPI work.
Throughput levers, in order: batching (one Produce carrying N records costs
~one insert), fetch byte budgets (max_bytes is filled in bounded query
loops), connection pipelining (safe — responses stay ordered per connection),
and vertical DB tuning. Heartbeats travel on a dedicated liveness lane that the
worker drains before the main request queue, so group liveness does not
degrade with produce load.

## Background: PostgreSQL as a message store (external benchmark)

**Source:** [stanislavkozlovski/pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark)

These figures characterize the *storage engine*, not pg_kafka (measured with
many parallel backends on a 96-vCPU node — a topology pg_kafka's single-writer
design deliberately does not use):
- 1,000,000+ reads/sec / 200,000+ writes/sec on that hardware
- Monotonic offset pattern (identical to Kafka) performs well
- ⚠️ Infinite table growth causes a performance cliff
- ⚠️ Default autovacuum settings too slow for high-churn workloads

**Takeaway:** the storage engine is not the limiting factor at pg_kafka's
scale; schema design, retention, and the single-writer wire path are.

## Critical Performance Pitfalls

### 1. The "Infinite Growth" Anti-Pattern

**Problem:**
```sql
-- ❌ BAD: Unbounded table growth
CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT,
    is_read BOOLEAN DEFAULT FALSE  -- Anti-pattern!
);

-- Consumers mark as read
UPDATE messages SET is_read = TRUE WHERE id = $1;
```

**Why This Fails:**
- Table grows indefinitely (never deletes old rows)
- Index bloat increases over time
- `VACUUM` cannot reclaim space (rows still referenced)
- Query performance degrades linearly with table size
- Eventual database crash when disk fills

**pg_kafka Status:**
- ✅ We do NOT use `is_read` columns
- ✅ We use offset-based reads (Kafka pattern)
- ✅ `kafka.messages` is time-partitioned on `created_at` (ADR-001, 0.1.0): retention drops whole
  leaves (`DROP TABLE`, no dead tuples, no VACUUM debt) wherever every row in a leaf is expired, and
  falls back to the bounded `DELETE` sweep only for leaves shared with a longer-lived topic and for
  topics whose window is shorter than one leaf (see "Retention & lifecycle GUCs" below)

### 2. SKIP LOCKED Destroys Ordering

**Finding:** `FOR UPDATE SKIP LOCKED` is excellent for work queues but breaks Kafka semantics.

**Work Queue Pattern (Competing Consumers):**
```sql
-- ✅ GOOD for job queues
SELECT * FROM jobs WHERE status = 'pending'
ORDER BY priority DESC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

**Kafka Pattern (Ordered Consumption):**
```sql
-- ✅ GOOD for Kafka (pg_kafka uses this)
SELECT * FROM kafka.messages
WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
ORDER BY partition_offset
LIMIT 1000;
```

**pg_kafka Status:**
- ✅ We correctly use offset-based reads (no locks)
- ✅ No `SKIP LOCKED` in fetch path
- ✅ Ordering preserved within partitions

## Recommended PostgreSQL Configuration

### For High-Throughput Message Workloads

These settings are optimized for workloads with >10,000 inserts/sec and high read concurrency.

#### 1. Aggressive Autovacuum

**Problem:** Default settings trigger vacuum too infrequently for high-churn tables.

```sql
-- Default (too slow)
autovacuum_vacuum_scale_factor = 0.2   -- Vacuum after 20% of rows change
autovacuum_analyze_scale_factor = 0.1  -- Analyze after 10% of rows change

-- Recommended for pg_kafka
autovacuum_vacuum_scale_factor = 0.05  -- Vacuum after 5% of rows change
autovacuum_analyze_scale_factor = 0.05 -- Analyze after 5% of rows change
```

**Per-Table Override (Best Practice):**
```sql
ALTER TABLE kafka.messages SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_vacuum_cost_delay = 2  -- Lower delay = more aggressive
);
```

**Why This Matters:**
- Keeps table statistics fresh for query planner
- Prevents index bloat from accumulating
- Enables faster `VACUUM` runs (less work per run)
- Critical for tables with millions of rows/day

#### 2. Memory and Buffer Settings

```sql
-- For systems with 64GB+ RAM
shared_buffers = 16GB           -- 25% of system RAM
effective_cache_size = 48GB     -- 75% of system RAM
work_mem = 64MB                 -- Per-operation memory (adjust based on concurrent queries)
maintenance_work_mem = 2GB      -- For CREATE INDEX, VACUUM
```

#### 3. Write-Ahead Log (WAL) Tuning

```sql
-- For high write throughput
wal_buffers = 16MB              -- WAL write buffer (default is often too small)
checkpoint_timeout = 15min      -- Time between checkpoints
max_wal_size = 4GB              -- Allow more WAL before forcing checkpoint
min_wal_size = 1GB              -- Reserve WAL space

-- Synchronous commit (based on acks requirement)
synchronous_commit = on         -- For acks=-1 (wait for WAL flush)
# synchronous_commit = off      -- For acks=1 (async commit, ~2x faster writes)
```

**pg_kafka Recommendation:**
- Use `synchronous_commit = on` by default (data safety)
- pg_kafka has no GUC of its own for this. The worker's transactions follow the server's
  `synchronous_commit` setting, so `off` trades durability of the latest commits for throughput
  for every acks level.
- Group commit: the worker runs every request already queued (up to 64, or about 50 ms of work,
  and never past a waiting Heartbeat) in ONE top-level transaction, each request in its own
  subtransaction, and releases their responses only after that single commit. Concurrent and
  pipelined producers therefore share one WAL flush instead of paying one each, which lifts the
  ~220 requests/s fsync ceiling of the old one-transaction-per-request design. A lone request
  still commits on its own, with no added wait.
- There is deliberately no "wait for more requests" delay (like Postgres's `commit_delay`). One
  was built and measured (2026-11, PG14, fsync on, eight Java producers with
  `max.in.flight.requests.per.connection=1`, `batch.size=0`, server-side rate from
  `kafka.messages.created_at`). While one batch flushes, the other producers' requests queue, so
  the next batch already carries about 5 of the 8 possible requests (one record each on a
  one-partition topic) or about 35 records (eight partitions). A wait cannot add requests that
  the clients have not sent, so it only lengthened every round: one partition fell from 1,165 to
  1,014 records/s at 0.5 ms and 630 at 5 ms; eight partitions went from 2,600-2,730 to
  2,450-2,520 records/s at 1 ms (three runs each). With few in-flight requests, throughput is
  bounded by the fsync rate times the number of requests the clients keep in flight; raise
  client concurrency (`max.in.flight.requests.per.connection`, `linger.ms`, more producers) or,
  if losing the latest commits on a crash is acceptable, `synchronous_commit = off`.

#### 4. Huge Pages (Large Instances)

```sql
-- For systems with 64GB+ RAM
huge_pages = on                 -- Reduces CPU overhead for memory management
```

**Impact:** On large instances, managing millions of 4KB pages consumes significant CPU. Huge pages (2MB each) reduce this overhead by ~5-10% CPU usage.

#### 5. Connection Pooling

**Problem:** Postgres processes are heavyweight (~10MB each). Too many connections = memory exhaustion.

**Solution:** Use PgBouncer or connection pooling in front of Postgres.

```bash
# PgBouncer configuration
[databases]
postgres = host=localhost dbname=postgres

[pgbouncer]
pool_mode = transaction        # Best for pg_kafka (short transactions)
max_client_conn = 10000        # High client connection limit
default_pool_size = 50         # Actual Postgres connections
reserve_pool_size = 10         # Emergency reserve
```

**pg_kafka Impact:**
- The TCP listener on port 9092 multiplexes thousands of client connections
- But only uses ONE background worker process for SPI calls
- So PgBouncer-style pooling is NOT APPLICABLE to the Kafka wire path — there
  is exactly one SPI connection by design. (That is a correctness trade with a
  hard throughput ceiling, not an optimization — see "Architecture ceiling"
  at the top of this document. Pooling advice here applies only to regular
  SQL clients sharing the same database.)

## Performance Monitoring

### Key Metrics to Track

#### 1. Table Bloat
```sql
-- Check table and index bloat
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS index_size
FROM pg_tables
WHERE schemaname = 'kafka'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

#### 2. Autovacuum Activity
```sql
-- Check last vacuum/analyze times
SELECT
    relname,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    n_tup_ins AS inserts,
    n_tup_upd AS updates,
    n_tup_del AS deletes,
    n_dead_tup AS dead_tuples
FROM pg_stat_user_tables
WHERE schemaname = 'kafka';
```

#### 3. Query Performance
```sql
-- Top queries by total time
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
WHERE query LIKE '%kafka.messages%'
ORDER BY total_exec_time DESC
LIMIT 10;
```

#### 4. WAL Generation Rate
```sql
-- WAL write rate (MB/sec)
SELECT
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) AS total_wal,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), pg_current_wal_lsn()) /
                   EXTRACT(EPOCH FROM NOW() - pg_postmaster_start_time())) AS wal_per_sec;
```

#### 5. Built-in Metrics View
```sql
-- O(1) catalog estimates for message counts/bytes, plus groups, producers,
-- transactions, and shadow-mode counters (category, metric, value, unit)
SELECT * FROM kafka.metrics;
SELECT * FROM kafka.metrics WHERE category = 'messages';
```

## Architectural Decisions for pg_kafka

### 1. ✅ Offset-Based Reads (Not Mark-as-Read)

**Decision:** Use Kafka's offset-based consumption pattern.

**Implementation:**
```sql
-- Correct pattern (what we do)
SELECT * FROM kafka.messages
WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
ORDER BY partition_offset
LIMIT $4;
```

**Benefits:**
- No row updates during consumption (read-only queries)
- No index bloat from UPDATE churn
- Naturally supports replayability (restart from any offset)
- Multiple consumers can read same data

### 2. ✅ Partitioning Strategy (0.1.0)

`kafka.messages` is `PARTITION BY RANGE (created_at)` with leaves of
`pg_kafka.message_partition_minutes` (default one day) plus a DEFAULT leaf. See
[ADR-001 "As built"](architecture/ADR-001-partitioning-and-retention.md#as-built-010) for the
layout, uniqueness, least-privilege DDL and upgrade. Costs to keep in mind:

- Fetch/ListOffsets/HWM lookups do not name `created_at`, so they probe every leaf's
  `(topic_id, partition_id, partition_offset)` btree: one descent per leaf. Keep the leaf count
  modest (retention / width + 4); do not combine a tiny width with a long retention. Only a
  retention window bounds it: with the defaults (`pg_kafka.message_retention_hours = 0`, no
  `retention.ms`) a leaf with rows is never dropped and the count grows by one per width, so
  set `pg_kafka.message_retention_hours` (or `retention.ms` on every topic) on a long-lived
  broker. Planning such a
  statement expands every leaf, so the hot read statements run through kept prepared plans
  (`select_cached` in `storage/postgres.rs`).
- Retention of a leaf is a `DROP TABLE`. On 1M expired rows (PG15 sandbox): 0.35 s and 0.16 MB of WAL
  with 0 dead tuples, versus 2.9 s, 240 MB of WAL and 1M dead tuples (plus a 0.37 s VACUUM) for the
  row-delete sweep, and the table shrinks to the retained data instead of keeping its size.

### 3. ✅ Separate Consumer Offsets Table (Phase 3)

**Decision:** Do NOT store consumer offsets in the same table as messages.

**Implementation:**
```sql
-- Phase 3: Consumer group offsets
CREATE TABLE kafka.consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,  -- Optional client metadata
    commit_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK (partition_id >= 0),
    PRIMARY KEY (group_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- This table stays small (one row per consumer-group-partition combo)
-- Frequent UPDATEs are fine because table size is bounded
```

### 4. ✅ Dual-Offset Design

**Decision:** Use both `partition_offset` (Kafka compatibility) and `global_offset` (temporal ordering).

**Current Implementation:**
- `global_offset BIGSERIAL` - Monotonic across ALL partitions
- `partition_offset BIGINT` - Assigned per partition from the `kafka.partition_offsets.next_offset` counter,
  which is never decremented, so offsets are not reused after aborted rows are cleaned up

**Performance Impact:**
- Assigning `partition_offset` takes a per-partition transaction-scoped advisory lock (`pg_advisory_xact_lock`)
  and updates the `kafka.partition_offsets` row
- Lock is per-partition, not global (concurrent writes to different partitions don't block)
- Adds ~1ms per INSERT batch (acceptable for Phase 2 throughput targets)

## Retention & lifecycle GUCs (shipped)

```sql
-- Time-based retention for kafka.messages, enforced by the worker's periodic
-- retention sweep (60s cadence, at most 10,000 rows per DELETE). 0 (default)
-- keeps forever (a topic's retention.ms still applies). The sweep runs either way:
-- it also cleans up aborted rows and the shadow outbox (see "The sweep's cost"
-- below). Range 0-87600; reloadable with SIGHUP (pg_reload_conf()).
pg_kafka.message_retention_hours = 168   -- e.g. 7 days

-- Width of one time partition of kafka.messages (minutes, default 1440 = one day,
-- 5-43200, SIGHUP). Windows >= one width are enforced a leaf at a time (a row lives
-- at most window + width, like Kafka segment retention); shorter windows row by row.
pg_kafka.message_partition_minutes = 1440

-- Both functions below are for superusers (EXECUTE is revoked from PUBLIC; ADR-001).
-- On-demand partition maintenance: create the leaves for now and the next two
-- widths and move rows the DEFAULT leaf caught. Returns the number created.
SELECT pg_kafka_run_partition_maintenance();

-- On-demand sweep + report (also reclaims aborted transactional rows and prunes
-- stale producers, terminal transactions, delivered shadow-outbox rows and
-- outbox rows whose message retention or DeleteRecords removed).
-- Arguments: message_retention_hours (NULL = use the GUC), aborted_grace_seconds
-- (default 60). Returns one (category, deleted) row per category, including
-- dropped_partitions (whole leaves dropped).
SELECT * FROM pg_kafka_run_retention_sweep();
```

### The sweep's cost

The sweep runs on the worker's single DB thread every 60 s, so while it runs no request is
served (Produce, Fetch, Heartbeats). Its cost therefore follows the work it does, not the
data stored: the aborted-row cleanup goes through the partial index
`idx_messages_txn_aborted` leaf by leaf, delivered outbox rows are walked per partition through
`idx_shadow_tracking_delivered` (only rows a day old are visited), orphaned outbox rows through
the primary key below each partition's log start, and a past leaf gets the full contents
probe (one descent per (topic, partition)) only where some topic's window can have expired it
(at most 64 per pass); other past leaves get one emptiness check. Every step runs bounded
statements (10,000 rows, one leaf) and stops once the pass has used 1.5 s, after at least one
statement (the aborted-row cleanup and the contents probes, which run before row expiry, at a
third of that each), so a backlog drains over several passes, each step resuming where the last
pass left it. The producer and transaction prunes are indexed too (idle producer ids through
`(last_active_at, producer_id)`, terminal transactions through a partial index on
`last_updated_at`, and the foreign key of `kafka.transactions` on each deleted producer id
through `producer_id`): 40,000 idle producer ids with their terminal transactions took one
18.6 s statement before (each deleted id's foreign-key check read the whole transactions table),
and 2,000,000 now drain 10,000 producer ids and up to 190,000 transactions per 1.6-1.8 s pass;
with 2,000,000 producer ids none of which is idle, a pass takes 2.5 ms (PG17). Measured on PG16 (3,000,000 committed rows
in one 521 MB leaf, 500,000 recent delivered outbox rows, retention off):

| | before | after |
|---|---|---|
| aborted-row cleanup (EXPLAIN ANALYZE) | Seq Scan, 66,667 blocks, 231 ms | partial index, 1 block, 0.15 ms |
| delivered-outbox reaping | Seq Scan, 4,673 blocks, 58 ms | index walk, 11 blocks, 0.2 ms |
| whole sweep (`pg_kafka_run_retention_sweep(0, 60)`) | 262 ms | 2.1 ms |
| worst Metadata round trip, 20 ms probe | 217-308 ms each minute | 24 ms |
| sweep with 20 past daily leaves of 2,000 partitions each, too | 539 ms | 32-52 ms |

The delivered-row index costs the outbox one btree entry per ack and nothing per claim: its
key columns never change and its predicate column (`external_offset`) already keys the pending
indexes, so a claim stays a HOT update. Bulk UPDATEs of 200,000 rows as the outbox issues them
(PG16, three runs): claims 273-332 ms before, 296-339 ms after, with the same HOT count; acks
710-779 ms before, 853-989 ms after (about 0.5-1 us per acked row).

Per-topic overrides use Kafka's own config key. `retention.ms` set through
IncrementalAlterConfigs (API 44) is stored in `kafka.topics.retention_ms` and
enforced by the same sweep: a value `>= 0` applies even when the global GUC is
0, and a negative value pins that topic to infinite retention. DescribeConfigs
(API 32) reports the effective value. DeleteRecords (API 21) truncates a
partition up to an offset on demand.

Aggressive autovacuum for the high-churn counter tables is applied by
`sql/tune_autovacuum.sql`. Time partitioning and partition-drop retention:
ADR-001 "As built".

### Suggested System Configuration

For production deployments, recommend users set:

```sql
-- In postgresql.conf (requires restart)
shared_buffers = 4GB                           -- 25% of RAM (16GB system)
effective_cache_size = 12GB                    -- 75% of RAM
work_mem = 32MB                                -- Adjust based on max_connections
maintenance_work_mem = 1GB
wal_buffers = 16MB
checkpoint_timeout = 15min
max_wal_size = 4GB
autovacuum_vacuum_scale_factor = 0.05
autovacuum_analyze_scale_factor = 0.05
huge_pages = try                               -- Enable if supported
```

## Benchmarking Targets

### Phase 2 (Producer Support)

**Target Metrics:**
- **Write Throughput:** 50,000 messages/sec (single partition)
- **Write Latency (p50):** <5ms
- **Write Latency (p99):** <20ms
- **Concurrent Producers:** 100+ clients

**Test Plan:**
```bash
# Benchmark with kcat
seq 1 100000 | kcat -P -b localhost:9092 -t benchmark -K: -T

# Measure with pgbench
pgbench -c 10 -j 4 -T 60 -f produce_benchmark.sql
```

### Phase 3 (Consumer Support)

**Target Metrics:**
- **Read Throughput:** 200,000 messages/sec (single consumer)
- **Read Latency (p50):** <2ms
- **Read Latency (p99):** <10ms
- **Concurrent Consumers:** 1000+ clients (read-only, no locks)

### Phase 4 (Shadow Mode Replication)

These targets predate the shadow-mode implementation. Shadow mode shipped (Phase 11)
as a durable outbox (`kafka.shadow_tracking`, written in the producing transaction)
drained by an rdkafka producer, with at-least-once delivery to the external broker.
It does not use logical replication or LSNs. Measured shadow-forwarding rates are
the `shadow-async` / `shadow-sync` rows in `bench.json`.

**Target Metrics:**
- **Replication Lag:** <100ms (p99)
- **Replication Throughput:** Match producer throughput (50k msg/sec)
- **Zero Data Loss:** All messages replicated before LSN advance

## Phase 8: Compression Performance

### Overview

pg_kafka supports all standard Kafka compression codecs: gzip, snappy, lz4, and zstd. Compression is handled transparently - inbound messages are automatically decompressed, outbound compression is configurable.

### Compression Trade-offs

| Codec | CPU Overhead | Size Reduction | Best For |
|-------|--------------|----------------|----------|
| **none** | 0% | 0% | Low-latency, already compressed data |
| **snappy** | ~2-5% | 30-40% | High throughput, low latency |
| **lz4** | ~3-5% | 40-50% | Balanced (recommended default) |
| **gzip** | ~10-15% | 60-70% | Storage-constrained environments |
| **zstd** | ~5-10% | 50-60% | Best compression/speed ratio |

### Configuration

```sql
-- Outbound compression. This is a PGC_POSTMASTER GUC: set it in postgresql.conf
-- and restart PostgreSQL (SET at runtime is rejected).
pg_kafka.compression_type = 'lz4'  -- none (default), gzip, snappy, lz4, zstd

-- Check current setting
SHOW pg_kafka.compression_type;
```

### Recommendations

**High Throughput Workloads:**
- Use `lz4` or `snappy` for minimal CPU overhead
- Target: 50,000+ msg/sec with <5% CPU increase

**Storage-Constrained Environments:**
- Use `zstd` or `gzip` for maximum compression
- Trade-off: ~10-15% higher CPU usage

**Network-Bound Clients:**
- Enable compression to reduce bandwidth
- Particularly beneficial for large message payloads (>1KB)

**Default Recommendation:**
- `none` for backward compatibility
- `lz4` for new deployments (best balance)

### Inbound Decompression

Producer-compressed messages are automatically decompressed by the `kafka-protocol` crate. This is transparent and requires no configuration:

```
Producer (gzip) → TCP → pg_kafka (auto-decompress) → PostgreSQL (uncompressed)
```

**Note:** Messages are stored uncompressed in PostgreSQL. This enables SQL queries on message content but increases storage requirements. Future phases may add optional storage compression.

---

## Future Optimizations

### Phase 3+

1. **Partition Pruning:** Leverage PostgreSQL's native partition pruning for time-range queries
2. **Parallel Query:** Use `max_parallel_workers_per_gather` for large scans
3. **Columnar Storage:** Consider `cstore_fdw` for archival partitions
4. **Compression:** Use `pg_lz4` or `pg_zstd` for cold storage partitions

### Phase 4+ (Shadow Mode)

These ideas predate the outbox-based implementation described above.

1. **Logical Replication Slots:** Fine-tune `wal_sender_timeout` and `max_slot_wal_keep_size`
2. **External Kafka Batching:** Batch multiple Postgres rows into single Kafka ProduceRequest
3. **Parallel Replication:** Multiple replication workers for different topics/partitions

## References

- [pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark) - Performance validation
- [PostgreSQL Autovacuum Tuning](https://www.postgresql.org/docs/current/runtime-config-autovacuum.html)
- [Kafka Protocol Specification](https://kafka.apache.org/protocol.html)
- [pgrx Documentation](https://docs.rs/pgrx/latest/pgrx/)

---

**Status:** Phase 11 Complete. Retention (global GUC plus per-topic `retention.ms`) drops whole time partitions of `kafka.messages` where it can and deletes row by row in bounded batches elsewhere (ADR-001).

**Last Updated:** 2026-11-19
