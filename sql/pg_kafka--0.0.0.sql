-- pg_kafka extension schema
-- Phase 2: Producer support with dual-offset design
--
-- This schema creates tables for Kafka-compatible message storage with a unique
-- dual-offset architecture that provides both:
-- 1. Kafka protocol compatibility (partition_offset)
-- 2. Cross-partition temporal ordering (global_offset)

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

-- Grant permissions
-- Background worker and extension functions run as superuser by default,
-- but we set up permissions for future user-facing functions
GRANT USAGE ON SCHEMA kafka TO PUBLIC;
GRANT SELECT ON kafka.topics TO PUBLIC;
GRANT SELECT ON kafka.messages TO PUBLIC;

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
