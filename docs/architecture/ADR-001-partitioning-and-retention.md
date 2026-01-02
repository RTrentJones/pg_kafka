# ADR-001: Partitioning and Retention Strategy

**Status:** Proposed (Phase 3)
**Date:** 2026-01-02
**Context:** Based on PostgreSQL queue/pubsub benchmark findings

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
- [ ] Implement partition management functions (Phase 3.1)
- [ ] Test migration script with 10M+ rows
- [ ] Document data loss behavior in user-facing docs
- [ ] Add monitoring for partition operations
