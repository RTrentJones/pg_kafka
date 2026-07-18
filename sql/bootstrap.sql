-- pg_kafka extension schema
--
-- This schema creates tables for Kafka-compatible message storage with a unique
-- dual-offset architecture that provides both:
-- 1. Kafka protocol compatibility (partition_offset)
-- 2. Cross-partition temporal ordering (global_offset)
--
-- Phase 3 additions:
-- - Consumer offset tracking (consumer_offsets table)
-- (Consumer-group membership is coordinated in-memory by the GroupCoordinator;
--  only committed offsets are persisted. See DR-6, DEEP-REVIEW-2026-07.)

-- Create kafka schema for all extension objects
CREATE SCHEMA IF NOT EXISTS kafka;

-- Topics table: Metadata about topics
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT NOT NULL DEFAULT 1,
    -- Per-topic retention.ms override (IncrementalAlterConfigs, API 44).
    -- NULL = inherit pg_kafka.message_retention_hours; >= 0 = enforce this
    -- window (even when the global sweep is off); < 0 = infinite retention
    -- for this topic (even when a global window is set).
    retention_ms BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Messages table: The actual message log with dual-offset design
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,           -- Monotonic across ALL partitions (temporal ordering)
    partition_offset BIGINT NOT NULL,  -- Per-partition offset (Kafka protocol compatibility)
    key BYTEA,
    value BYTEA,
    headers JSONB,
    -- BUG-7 (AUDIT-2026-06): the producer's record timestamp (epoch ms), stored verbatim so consumers
    -- read the producer's time, not the broker's insert time. -1 means the record carried no
    -- timestamp; the fetch path then falls back to created_at.
    timestamp_ms BIGINT NOT NULL DEFAULT -1,
    -- created_at is the broker insert time, kept for retention/cleanup. TIMESTAMPTZ (not naive
    -- TIMESTAMP) so EXTRACT(EPOCH ...) is unambiguous and doesn't skew by the session time zone.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Phase 10: Transaction tracking columns
    producer_id BIGINT,                -- Producer ID for transactional messages (NULL for non-transactional)
    producer_epoch SMALLINT,           -- Producer epoch for fencing
    txn_state TEXT,                    -- NULL (committed/non-txn), 'pending' (uncommitted), 'aborted'
    -- DR-7 (DEEP-REVIEW-2026-07): reject negative partition ids at the storage layer too
    -- (handlers validate range against topics.partitions; this is the schema backstop).
    CHECK (partition_id >= 0),
    PRIMARY KEY (topic_id, partition_id, partition_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE,
    -- Ensures global ordering is strictly monotonic. The unique constraint's backing
    -- btree also serves temporal-range queries (global_offset > ? ORDER BY global_offset),
    -- so no separate index on global_offset is needed (DR-3, DEEP-REVIEW-2026-07: a
    -- second identical index doubled write amplification on the hottest column).
    UNIQUE (global_offset)
);

-- Index for Kafka fetch queries (by topic/partition/offset range)
-- Supports: SELECT * FROM kafka.messages WHERE topic_id = ? AND partition_id = ? AND partition_offset >= ? LIMIT ?
CREATE INDEX idx_messages_topic_partition_offset
ON kafka.messages(topic_id, partition_id, partition_offset);

-- Index for read_committed filtering (Phase 10: exclude pending transactional messages)
-- Supports: SELECT * FROM kafka.messages WHERE ... AND (txn_state IS NULL)
CREATE INDEX idx_messages_txn_pending
ON kafka.messages(topic_id, partition_id, partition_offset)
WHERE txn_state = 'pending';

-- Per-partition monotonic offset counter (BUG-3, AUDIT-2026-06): the next partition_offset to
-- assign for each (topic, partition). Producers advance it under the per-partition advisory lock
-- and it is NEVER decremented, so deleting the highest (aborted) rows in cleanup_aborted_messages
-- cannot lower the next offset and reuse offsets — they stay strictly increasing for the life of
-- the partition. A missing row is seeded from MAX(partition_offset)+1 on first use, so existing
-- data is respected.
CREATE TABLE IF NOT EXISTS kafka.partition_offsets (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL DEFAULT 0,
    CHECK (partition_id >= 0),
    PRIMARY KEY (topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Consumer offsets table: Track committed offsets per consumer group
-- Phase 3: Consumer support
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

-- Index for consumer group queries
CREATE INDEX idx_consumer_offsets_group ON kafka.consumer_offsets(group_id);
CREATE INDEX idx_consumer_offsets_timestamp ON kafka.consumer_offsets(commit_timestamp);

-- NOTE (DR-6, DEEP-REVIEW-2026-07): the former kafka.consumer_groups table was removed.
-- It was dead schema — group membership, generations, and assignments live in the
-- in-memory GroupCoordinator (src/kafka/coordinator.rs) with full Range/RoundRobin/Sticky
-- rebalancing; nothing ever read or wrote the table, and its comment claimed
-- "static assignment (no rebalancing)", contradicting the implementation.

-- Grant permissions
-- Background worker and extension functions run as superuser by default,
-- but we set up permissions for future user-facing functions
GRANT USAGE ON SCHEMA kafka TO PUBLIC;
GRANT SELECT ON kafka.topics TO PUBLIC;
GRANT SELECT ON kafka.messages TO PUBLIC;
GRANT SELECT ON kafka.consumer_offsets TO PUBLIC;

-- Comments for documentation
COMMENT ON SCHEMA kafka IS 'pg_kafka extension schema for Kafka-compatible message storage';

COMMENT ON TABLE kafka.topics IS 'Kafka topics metadata - each topic has 1+ partitions';

COMMENT ON TABLE kafka.messages IS 'Kafka message log with dual-offset design:
- global_offset: BIGSERIAL auto-incrementing across all partitions for temporal ordering
- partition_offset: Computed per-partition for Kafka protocol compatibility';

COMMENT ON COLUMN kafka.messages.global_offset IS 'Monotonically increasing across ALL messages (all topics, all partitions). Enables cross-partition temporal queries. May have gaps due to rollbacks.';

COMMENT ON COLUMN kafka.messages.partition_offset IS 'Per-partition offset starting at 0. Kafka clients use this for offset management. Computed atomically on INSERT, no gaps within a partition.';

COMMENT ON COLUMN kafka.messages.key IS 'Optional message key (nullable). Used for partition routing in real Kafka.';

COMMENT ON COLUMN kafka.messages.value IS 'Message payload (nullable for tombstone messages in log compaction)';

COMMENT ON COLUMN kafka.messages.headers IS 'Optional message headers as JSONB. Kafka headers are key-value pairs (binary values base64-encoded).';

COMMENT ON COLUMN kafka.messages.producer_id IS 'Producer ID for transactional messages. NULL for non-transactional messages. (Phase 10)';

COMMENT ON COLUMN kafka.messages.producer_epoch IS 'Producer epoch for fencing. Used with producer_id to identify unique producer instance. (Phase 10)';

COMMENT ON COLUMN kafka.messages.txn_state IS 'Transaction state: NULL (committed or non-transactional), ''pending'' (uncommitted), ''aborted'' (rolled back). (Phase 10)';

COMMENT ON TABLE kafka.consumer_offsets IS 'Consumer offset tracking per group/topic/partition. Stores committed offsets for Kafka consumers (Phase 3).';

COMMENT ON COLUMN kafka.consumer_offsets.group_id IS 'Consumer group identifier (e.g., "my-consumer-group")';

COMMENT ON COLUMN kafka.consumer_offsets.committed_offset IS 'Last committed partition offset for this consumer group. Consumer will fetch from committed_offset + 1.';

-- =============================================================================
-- Phase 9: Idempotent Producer Support
-- =============================================================================

-- Producer IDs table: Allocates producer IDs for idempotent/transactional producers
CREATE TABLE kafka.producer_ids (
    producer_id BIGSERIAL PRIMARY KEY,
    epoch SMALLINT NOT NULL DEFAULT 0,
    client_id TEXT,
    transactional_id TEXT,  -- NULL for non-transactional idempotent producers
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_active_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for transactional ID lookup (InitProducerId with existing transactional_id)
CREATE INDEX idx_producer_ids_txn_id
ON kafka.producer_ids(transactional_id) WHERE transactional_id IS NOT NULL;

-- Producer sequences table: Tracks sequence numbers for idempotent deduplication
CREATE TABLE kafka.producer_sequences (
    producer_id BIGINT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    last_sequence INT NOT NULL DEFAULT -1,  -- Last successfully written sequence (-1 = none)
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK (partition_id >= 0),
    PRIMARY KEY (producer_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Grant permissions for Phase 9 tables
GRANT SELECT ON kafka.producer_ids TO PUBLIC;
GRANT SELECT ON kafka.producer_sequences TO PUBLIC;

-- Comments for Phase 9
COMMENT ON TABLE kafka.producer_ids IS 'Producer ID allocation for idempotent producers. Each idempotent producer gets a unique ID and epoch.';

COMMENT ON COLUMN kafka.producer_ids.producer_id IS 'Unique producer ID allocated by InitProducerId API';

COMMENT ON COLUMN kafka.producer_ids.epoch IS 'Producer epoch, incremented on each InitProducerId call for same producer. Used for fencing.';

COMMENT ON COLUMN kafka.producer_ids.transactional_id IS 'Optional transactional ID for transactional producers (Phase 10)';

COMMENT ON TABLE kafka.producer_sequences IS 'Sequence number tracking for idempotent producer deduplication';

COMMENT ON COLUMN kafka.producer_sequences.last_sequence IS 'Last successfully written sequence number. Next expected = last_sequence + 1';

-- =============================================================================
-- Phase 10: Transaction Support
-- =============================================================================

-- Transactions table: Tracks active transaction state per transactional_id
-- Each transactional producer has one row tracking its current transaction state
CREATE TABLE kafka.transactions (
    transactional_id TEXT PRIMARY KEY,
    -- ON UPDATE CASCADE: RV-13 reallocates producer_ids.producer_id on epoch
    -- exhaustion; the transaction row's producer_id must follow the new id.
    producer_id BIGINT NOT NULL REFERENCES kafka.producer_ids(producer_id) ON UPDATE CASCADE,
    producer_epoch SMALLINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'Empty',
    -- States: 'Empty', 'Ongoing', 'PrepareCommit', 'PrepareAbort', 'CompleteCommit', 'CompleteAbort'
    timeout_ms INT NOT NULL DEFAULT 60000,
    -- RV-10/BUG-7: TIMESTAMPTZ (not naive TIMESTAMP). started_at is compared to
    -- NOW() in the timeout sweep (`started_at < NOW() - (timeout_ms||'ms')::interval`),
    -- so a naive column skews the abort deadline by the session/DST offset.
    started_at TIMESTAMPTZ,
    last_updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for timeout scanning (find stale transactions)
CREATE INDEX idx_transactions_timeout
ON kafka.transactions(state, started_at)
WHERE state = 'Ongoing';

-- Pending transactional offsets table: Stores offset commits within an active transaction
-- These offsets are moved to consumer_offsets on commit, deleted on abort
CREATE TABLE kafka.txn_pending_offsets (
    transactional_id TEXT NOT NULL REFERENCES kafka.transactions(transactional_id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL REFERENCES kafka.topics(id) ON DELETE CASCADE,
    partition_id INT NOT NULL,
    pending_offset BIGINT NOT NULL,
    metadata TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK (partition_id >= 0),
    PRIMARY KEY (transactional_id, group_id, topic_id, partition_id)
);

-- Unique index for transactional_id lookup (fencing - one producer per transactional_id)
CREATE UNIQUE INDEX idx_producer_ids_txn_id_unique
ON kafka.producer_ids(transactional_id)
WHERE transactional_id IS NOT NULL;

-- Grant permissions for Phase 10 tables
GRANT SELECT ON kafka.transactions TO PUBLIC;
GRANT SELECT ON kafka.txn_pending_offsets TO PUBLIC;

-- Comments for Phase 10
COMMENT ON TABLE kafka.transactions IS 'Transaction state tracking for transactional producers. One row per transactional_id.';

COMMENT ON COLUMN kafka.transactions.transactional_id IS 'Unique identifier for the transactional producer (e.g., "order-processor-1")';

COMMENT ON COLUMN kafka.transactions.state IS 'Transaction state: Empty (no active txn), Ongoing (active), PrepareCommit/PrepareAbort (ending), CompleteCommit/CompleteAbort (finished)';

COMMENT ON COLUMN kafka.transactions.timeout_ms IS 'Transaction timeout in milliseconds. Transactions exceeding this are auto-aborted.';

COMMENT ON COLUMN kafka.transactions.started_at IS 'When the current transaction started (NULL if state is Empty)';

COMMENT ON TABLE kafka.txn_pending_offsets IS 'Pending offset commits within an active transaction. Moved to consumer_offsets on commit.';

COMMENT ON COLUMN kafka.txn_pending_offsets.pending_offset IS 'Offset to be committed when transaction completes successfully';

-- =============================================================================
-- Phase 11: Shadow Mode
-- =============================================================================
-- NOTE: Shadow mode tables are defined inline below rather than via \i
-- to avoid SQL file include issues with pgrx extension packaging.

-- Shadow mode configuration per topic
CREATE TABLE kafka.shadow_config (
    topic_id INT PRIMARY KEY REFERENCES kafka.topics(id) ON DELETE CASCADE,
    mode TEXT NOT NULL DEFAULT 'local_only',
    forward_percentage INT NOT NULL DEFAULT 0,
    external_topic_name TEXT,
    sync_mode TEXT NOT NULL DEFAULT 'sync',  -- Now always sync (async deprecated)
    write_mode TEXT NOT NULL DEFAULT 'dual_write',  -- dual_write or external_only
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_mode CHECK (mode IN ('local_only', 'shadow')),
    CONSTRAINT valid_percentage CHECK (forward_percentage >= 0 AND forward_percentage <= 100),
    CONSTRAINT valid_sync_mode CHECK (sync_mode IN ('async', 'sync')),
    CONSTRAINT valid_write_mode CHECK (write_mode IN ('dual_write', 'external_only'))
);

-- Tracking table for forwarding state
CREATE TABLE kafka.shadow_tracking (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    local_offset BIGINT NOT NULL,
    external_offset BIGINT,
    -- RV-10/BUG-7: TIMESTAMPTZ (not naive TIMESTAMP). forwarded_at is compared to
    -- NOW() in the outbox retry-backoff filter (`forwarded_at < NOW() - (backoff||'ms')::interval`),
    -- so a naive column skews the re-forward cadence by the session/DST offset.
    forwarded_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    CHECK (partition_id >= 0),
    PRIMARY KEY (topic_id, partition_id, local_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

CREATE INDEX idx_shadow_tracking_pending
ON kafka.shadow_tracking(topic_id, partition_id, local_offset)
WHERE external_offset IS NULL;

CREATE INDEX idx_shadow_tracking_errors
ON kafka.shadow_tracking(topic_id, partition_id)
WHERE error_message IS NOT NULL;

-- Per-partition metrics
CREATE TABLE kafka.shadow_metrics (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    messages_forwarded BIGINT NOT NULL DEFAULT 0,
    messages_skipped BIGINT NOT NULL DEFAULT 0,
    messages_failed BIGINT NOT NULL DEFAULT 0,
    last_forwarded_offset BIGINT NOT NULL DEFAULT -1,
    last_forwarded_at TIMESTAMP,
    PRIMARY KEY (topic_id, partition_id)
);

-- Monitoring view
CREATE VIEW kafka.shadow_status AS
SELECT
    t.name AS topic_name,
    COALESCE(sc.mode, 'local_only') AS mode,
    COALESCE(sc.forward_percentage, 0) AS forward_percentage,
    COALESCE(sc.sync_mode, 'async') AS sync_mode,
    sc.external_topic_name,
    COALESCE(SUM(sm.messages_forwarded), 0)::BIGINT AS total_forwarded,
    COALESCE(SUM(sm.messages_skipped), 0)::BIGINT AS total_skipped,
    COALESCE(SUM(sm.messages_failed), 0)::BIGINT AS total_failed,
    MAX(sm.last_forwarded_offset) AS last_forwarded_offset,
    MAX(sm.last_forwarded_at) AS last_forwarded_at,
    (SELECT MAX(partition_offset) FROM kafka.messages WHERE topic_id = t.id) -
        COALESCE(MAX(sm.last_forwarded_offset), 0) AS lag
FROM kafka.topics t
LEFT JOIN kafka.shadow_config sc ON t.id = sc.topic_id
LEFT JOIN kafka.shadow_metrics sm ON t.id = sm.topic_id
GROUP BY t.id, t.name, sc.mode, sc.forward_percentage, sc.sync_mode, sc.external_topic_name;

-- Grant permissions
GRANT SELECT ON kafka.shadow_config TO PUBLIC;
-- RV-10 (SEC-8 class): error_message can hold raw external-broker error strings
-- (authz failures, host/topic names, payload-size limits), so it is withheld from
-- PUBLIC. Grant only the non-sensitive columns column-by-column instead of the
-- whole row; the bgworker (table owner) keeps full access to error_message.
GRANT SELECT (topic_id, partition_id, local_offset, external_offset, forwarded_at, retry_count)
    ON kafka.shadow_tracking TO PUBLIC;
GRANT SELECT ON kafka.shadow_metrics TO PUBLIC;
GRANT SELECT ON kafka.shadow_status TO PUBLIC;

-- Comments
COMMENT ON TABLE kafka.shadow_config IS 'Per-topic shadow mode configuration for forwarding to external Kafka.';
COMMENT ON TABLE kafka.shadow_tracking IS 'Tracks which messages have been forwarded and their external offsets.';
COMMENT ON TABLE kafka.shadow_metrics IS 'Aggregated shadow mode metrics per topic/partition.';
COMMENT ON VIEW kafka.shadow_status IS 'Unified view of shadow mode health, lag, and error counts.';

-- Replay (SH-10/SH-11): re-mark a range of locally-stored messages as pending in
-- the durable outbox so the background poller re-forwards them to external Kafka.
-- This is how an operator runs an initial migration or recovers after an external
-- outage: replay reuses the same at-least-once outbox path as a live produce (no
-- separate, drift-prone forwarding engine). For each kafka.messages row in
-- [from_offset, to_offset) it inserts a pending kafka.shadow_tracking row, or
-- resets an existing one back to pending (external_offset = NULL). The poller then
-- forwards via the idempotent producer. Returns the number of rows (re)queued.
-- to_offset NULL means "to the current end".
CREATE OR REPLACE FUNCTION kafka.replay_shadow_messages(
    p_topic_id INT,
    p_from_offset BIGINT DEFAULT 0,
    p_to_offset BIGINT DEFAULT NULL
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_count BIGINT;
BEGIN
    INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset)
    SELECT m.topic_id, m.partition_id, m.partition_offset
    FROM kafka.messages m
    WHERE m.topic_id = p_topic_id
      AND m.partition_offset >= p_from_offset
      AND (p_to_offset IS NULL OR m.partition_offset < p_to_offset)
      -- RA-6: only replay committed/non-transactional records. txn_state NULL =
      -- committed or non-txn; 'pending' (uncommitted) and 'aborted' must never be
      -- forwarded to the external broker.
      AND m.txn_state IS NULL
    ON CONFLICT (topic_id, partition_id, local_offset) DO UPDATE
        SET external_offset = NULL,
            forwarded_at    = NULL,
            error_message   = NULL,
            retry_count     = 0;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION kafka.replay_shadow_messages(INT, BIGINT, BIGINT) TO PUBLIC;
COMMENT ON FUNCTION kafka.replay_shadow_messages(INT, BIGINT, BIGINT) IS
    'Re-queue a range of stored messages into the shadow outbox for re-forwarding (replay/migration/recovery).';

-- =============================================================================
-- Operational Metrics View (ADR-002: Staff-Level Architectural Improvements)
-- =============================================================================
-- Provides a unified view of system health metrics for monitoring.
-- This view aggregates key operational metrics in a single query.

-- DR-4 (DEEP-REVIEW-2026-07): the messages-table metrics use O(1) catalog estimates
-- (pg_class.reltuples / pg_table_size) instead of COUNT(*)/SUM(pg_column_size(...))
-- full scans — kafka.messages is the one unbounded table, so a scan-per-scrape view
-- degrades monitoring exactly when it matters. reltuples is refreshed by
-- (auto)VACUUM/ANALYZE; -1 (never analyzed) is clamped to 0. The shadow 'lag' metric
-- is now the pending-outbox depth (rows not yet forwarded, served by the partial
-- index idx_shadow_tracking_pending) instead of a cross-topic MAX(partition_offset)
-- diff, which both full-scanned and mixed all topics into one number.
CREATE VIEW kafka.metrics AS
-- Topic metrics
SELECT
    'topics' AS category,
    'count' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM kafka.topics
UNION ALL
-- Message metrics (estimates; see view comment)
SELECT
    'messages' AS category,
    'count_estimate' AS metric,
    GREATEST(reltuples, 0)::BIGINT AS value,
    'total' AS unit
FROM pg_class
WHERE oid = 'kafka.messages'::regclass
UNION ALL
SELECT
    'messages' AS category,
    'stored_bytes' AS metric,
    pg_table_size('kafka.messages'::regclass)::BIGINT AS value,
    'bytes' AS unit
UNION ALL
-- Consumer group metrics (groups with at least one committed offset; live
-- membership is in-memory in the GroupCoordinator and not visible to SQL)
SELECT
    'consumer_groups' AS category,
    'groups_with_commits' AS metric,
    COUNT(DISTINCT group_id)::BIGINT AS value,
    'total' AS unit
FROM kafka.consumer_offsets
UNION ALL
SELECT
    'consumer_offsets' AS category,
    'committed' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM kafka.consumer_offsets
UNION ALL
-- Producer metrics (Phase 9)
SELECT
    'producers' AS category,
    'active' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM kafka.producer_ids
WHERE last_active_at > NOW() - INTERVAL '5 minutes'
UNION ALL
-- Transaction metrics (Phase 10)
SELECT
    'transactions' AS category,
    'ongoing' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM kafka.transactions
WHERE state = 'Ongoing'
UNION ALL
SELECT
    'transactions' AS category,
    'pending_messages' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM kafka.messages
WHERE txn_state = 'pending'
UNION ALL
-- Shadow mode metrics (Phase 11)
SELECT
    'shadow' AS category,
    'forwarded' AS metric,
    COALESCE(SUM(messages_forwarded), 0)::BIGINT AS value,
    'total' AS unit
FROM kafka.shadow_metrics
UNION ALL
SELECT
    'shadow' AS category,
    'failed' AS metric,
    COALESCE(SUM(messages_failed), 0)::BIGINT AS value,
    'total' AS unit
FROM kafka.shadow_metrics
UNION ALL
SELECT
    'shadow' AS category,
    'lag' AS metric,
    COUNT(*)::BIGINT AS value,
    'messages' AS unit
FROM kafka.shadow_tracking
WHERE external_offset IS NULL;

GRANT SELECT ON kafka.metrics TO PUBLIC;

COMMENT ON VIEW kafka.metrics IS 'Unified operational metrics view for monitoring pg_kafka health.
Categories: topics, messages, consumer_groups, consumer_offsets, producers, transactions, shadow.
messages.count_estimate and messages.stored_bytes are O(1) catalog estimates (refreshed by
(auto)VACUUM/ANALYZE), not exact scans; shadow.lag is the pending-outbox depth.
Use with: SELECT * FROM kafka.metrics WHERE category = ''messages'';';
