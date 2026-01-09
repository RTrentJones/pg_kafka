-- pg_kafka extension schema
--
-- This schema creates tables for Kafka-compatible message storage with a unique
-- dual-offset architecture that provides both:
-- 1. Kafka protocol compatibility (partition_offset)
-- 2. Cross-partition temporal ordering (global_offset)
--
-- Phase 3 additions:
-- - Consumer offset tracking (consumer_offsets table)
-- - Consumer group coordination (consumer_groups table)

-- Create kafka schema for all extension objects
CREATE SCHEMA IF NOT EXISTS kafka;

-- Topics table: Metadata about topics
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT NOT NULL DEFAULT 1,
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
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic_id, partition_id, partition_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE,
    UNIQUE (global_offset)  -- Ensures global ordering is strictly monotonic
);

-- Index for Kafka fetch queries (by topic/partition/offset range)
-- Supports: SELECT * FROM kafka.messages WHERE topic_id = ? AND partition_id = ? AND partition_offset >= ? LIMIT ?
CREATE INDEX idx_messages_topic_partition_offset
ON kafka.messages(topic_id, partition_id, partition_offset);

-- Index for temporal queries (Phase 3: Hippocampus feature)
-- Supports: SELECT * FROM kafka.messages WHERE global_offset > ? ORDER BY global_offset
CREATE INDEX idx_messages_global_offset
ON kafka.messages(global_offset);

-- Consumer offsets table: Track committed offsets per consumer group
-- Phase 3: Consumer support
CREATE TABLE kafka.consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,  -- Optional client metadata
    commit_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Index for consumer group queries
CREATE INDEX idx_consumer_offsets_group ON kafka.consumer_offsets(group_id);
CREATE INDEX idx_consumer_offsets_timestamp ON kafka.consumer_offsets(commit_timestamp);

-- Consumer groups table: Track active consumer group members
-- Phase 3: Simplified consumer group coordination (static assignment only)
CREATE TABLE kafka.consumer_groups (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_ids INT[] NOT NULL,  -- Static partition assignment
    heartbeat_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, member_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Index for heartbeat checks
CREATE INDEX idx_consumer_groups_heartbeat ON kafka.consumer_groups(heartbeat_timestamp);

-- Grant permissions
-- Background worker and extension functions run as superuser by default,
-- but we set up permissions for future user-facing functions
GRANT USAGE ON SCHEMA kafka TO PUBLIC;
GRANT SELECT ON kafka.topics TO PUBLIC;
GRANT SELECT ON kafka.messages TO PUBLIC;
GRANT SELECT ON kafka.consumer_offsets TO PUBLIC;
GRANT SELECT ON kafka.consumer_groups TO PUBLIC;

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

COMMENT ON TABLE kafka.consumer_offsets IS 'Consumer offset tracking per group/topic/partition. Stores committed offsets for Kafka consumers (Phase 3).';

COMMENT ON COLUMN kafka.consumer_offsets.group_id IS 'Consumer group identifier (e.g., "my-consumer-group")';

COMMENT ON COLUMN kafka.consumer_offsets.committed_offset IS 'Last committed partition offset for this consumer group. Consumer will fetch from committed_offset + 1.';

COMMENT ON TABLE kafka.consumer_groups IS 'Consumer group membership tracking. pg_kafka uses simplified static assignment (no rebalancing).';

COMMENT ON COLUMN kafka.consumer_groups.partition_ids IS 'Array of partition IDs statically assigned to this member. No automatic rebalancing.';

COMMENT ON COLUMN kafka.consumer_groups.heartbeat_timestamp IS 'Last heartbeat from this consumer. Stale members (no heartbeat > 5min) may be cleaned up.';

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
