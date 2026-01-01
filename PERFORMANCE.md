# Performance Considerations for pg_kafka

This document outlines performance benchmarks, tuning recommendations, and architectural decisions based on real-world PostgreSQL queue/pub-sub benchmarking.

## Benchmark Validation

### PostgreSQL as High-Throughput Message Store

**Source:** [stanislavkozlovski/pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark)

**Key Findings:**
- ✅ **1,000,000+ reads/sec** on single node (96 vCPU)
- ✅ **200,000+ writes/sec** sustained throughput
- ✅ **Monotonic offset pattern** (identical to Kafka) performs well
- ⚠️ **Infinite table growth** causes performance cliff
- ⚠️ **Default autovacuum settings** too slow for high-churn workloads

**Conclusion:** PostgreSQL can absolutely serve as a Kafka-compatible message broker for small-to-medium scale workloads. The bottleneck is NOT the database engine itself, but rather schema design and configuration.

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
- ⚠️ **We currently have no retention policy**

**Required Fix (Phase 3+):**
Implement table partitioning with automatic partition dropping:

```sql
-- Partition by time range (e.g., daily)
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

-- Create partitions automatically
CREATE TABLE kafka.messages_2026_01_01 PARTITION OF kafka.messages
    FOR VALUES FROM ('2026-01-01') TO ('2026-01-02');

-- Drop old partitions (retention policy)
DROP TABLE kafka.messages_2025_12_01;  -- O(1) operation, not O(n)
```

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
- Allow users to set `synchronous_commit = off` via GUC for higher throughput (Phase 2.1)

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
- This is already optimal! No additional pooling needed.

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

### 2. ⚠️ Partitioning Strategy (TODO: Phase 3+)

**Decision:** Implement time-based partitioning with automatic retention.

**Why:**
- Prevents infinite table growth
- Enables O(1) deletion of old data (drop partition vs DELETE)
- Keeps active partition small for index efficiency
- Aligns with Kafka's segment-based storage

**Implementation Plan:**
```sql
-- Phase 3: Partition by created_at (daily partitions)
CREATE TABLE kafka.messages (
    ...
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Automatic partition management (cron job or background worker)
CREATE OR REPLACE FUNCTION kafka.create_next_partition()
RETURNS void AS $$
DECLARE
    partition_date DATE := CURRENT_DATE + INTERVAL '1 day';
    partition_name TEXT := 'kafka.messages_' || TO_CHAR(partition_date, 'YYYY_MM_DD');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF kafka.messages
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        partition_date,
        partition_date + INTERVAL '1 day'
    );
END;
$$ LANGUAGE plpgsql;

-- Retention policy (drop partitions older than 7 days)
CREATE OR REPLACE FUNCTION kafka.drop_old_partitions()
RETURNS void AS $$
DECLARE
    retention_days INT := 7;
    cutoff_date DATE := CURRENT_DATE - retention_days;
    partition_name TEXT;
BEGIN
    FOR partition_name IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'kafka'
        AND tablename LIKE 'messages_%'
        AND tablename < 'messages_' || TO_CHAR(cutoff_date, 'YYYY_MM_DD')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS kafka.%I', partition_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
```

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
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, partition_id)
);

-- This table stays small (one row per consumer-group-partition combo)
-- Frequent UPDATEs are fine because table size is bounded
```

### 4. ✅ Dual-Offset Design

**Decision:** Use both `partition_offset` (Kafka compatibility) and `global_offset` (temporal ordering).

**Current Implementation:**
- `global_offset BIGSERIAL` - Monotonic across ALL partitions
- `partition_offset BIGINT` - Computed per partition

**Performance Impact:**
- Computing `partition_offset` requires `FOR UPDATE` lock on partition
- Lock is per-partition, not global (concurrent writes to different partitions don't block)
- Adds ~1ms per INSERT batch (acceptable for Phase 2 throughput targets)

## Recommended GUC Parameters for pg_kafka

### Phase 2 (Current)

Add these to our extension's GUC configuration:

```rust
// src/config.rs additions for Phase 3
static PG_KAFKA_RETENTION_DAYS: GucSetting<i32> = GucSetting::new(7);  // Default: 7 days
static PG_KAFKA_PARTITION_INTERVAL: GucSetting<&str> = GucSetting::new("daily");  // "daily" or "hourly"
static PG_KAFKA_AUTOVACUUM_AGGRESSIVE: GucSetting<bool> = GucSetting::new(true);  // Enable aggressive autovacuum
```

```sql
-- postgresql.conf recommendations
pg_kafka.retention_days = 7                    -- Drop partitions older than 7 days
pg_kafka.partition_interval = 'daily'          -- 'daily' or 'hourly'
pg_kafka.autovacuum_aggressive = true          -- Apply aggressive autovacuum settings
```

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

**Target Metrics:**
- **Replication Lag:** <100ms (p99)
- **Replication Throughput:** Match producer throughput (50k msg/sec)
- **Zero Data Loss:** All messages replicated before LSN advance

## Future Optimizations

### Phase 3+

1. **Partition Pruning:** Leverage PostgreSQL's native partition pruning for time-range queries
2. **Parallel Query:** Use `max_parallel_workers_per_gather` for large scans
3. **Columnar Storage:** Consider `cstore_fdw` for archival partitions
4. **Compression:** Use `pg_lz4` or `pg_zstd` for cold storage partitions

### Phase 4+ (Shadow Mode)

1. **Logical Replication Slots:** Fine-tune `wal_sender_timeout` and `max_slot_wal_keep_size`
2. **External Kafka Batching:** Batch multiple Postgres rows into single Kafka ProduceRequest
3. **Parallel Replication:** Multiple replication workers for different topics/partitions

## References

- [pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark) - Performance validation
- [PostgreSQL Autovacuum Tuning](https://www.postgresql.org/docs/current/runtime-config-autovacuum.html)
- [Kafka Protocol Specification](https://kafka.apache.org/protocol.html)
- [pgrx Documentation](https://docs.rs/pgrx/latest/pgrx/)

---

**Status:** Phase 2 implementation does NOT yet include partitioning or retention policies. These are critical for production deployments and must be implemented in Phase 3.
