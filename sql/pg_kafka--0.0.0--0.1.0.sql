-- pg_kafka upgrade 0.0.0 -> 0.1.0
--
-- Run with:  ALTER EXTENSION pg_kafka UPDATE TO '0.1.0';
-- (installed next to the extension by `cargo pgrx install` / `cargo pgrx package`).
-- See docs/UPGRADING.md. The result must match a fresh 0.1.0 install; CI checks it
-- with scripts/check-upgrade.sh.
--
-- Takes an ACCESS EXCLUSIVE lock on kafka.messages for the index changes (the BRIN
-- build reads the table once); run it in a quiet period on a large table.

\echo Use "ALTER EXTENSION pg_kafka UPDATE TO '0.1.0'" to load this file. \quit

-- 0.0.0 installs that predate these additive columns (see the notes in bootstrap.sql).
ALTER TABLE kafka.topics ADD COLUMN IF NOT EXISTS retention_ms BIGINT;
ALTER TABLE kafka.partition_offsets
    ADD COLUMN IF NOT EXISTS log_start_offset BIGINT NOT NULL DEFAULT 0;

-- 1. The duplicate of the kafka.messages primary key.
DROP INDEX IF EXISTS kafka.idx_messages_topic_partition_offset;

-- 2. Retention expiry scans.
CREATE INDEX IF NOT EXISTS idx_messages_created_at_brin
ON kafka.messages USING brin (created_at) WITH (autosummarize = on);

-- 3. Least-privilege roles (same definitions as bootstrap.sql).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pgkafka_reader') THEN
        CREATE ROLE pgkafka_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pgkafka_worker') THEN
        CREATE ROLE pgkafka_worker NOLOGIN;
    END IF;
END
$$;

-- Withdraw everything 0.0.0 granted to PUBLIC.
REVOKE ALL ON SCHEMA kafka FROM PUBLIC;
REVOKE ALL ON kafka.topics, kafka.messages, kafka.consumer_offsets,
              kafka.producer_ids, kafka.producer_sequences,
              kafka.transactions, kafka.txn_pending_offsets,
              kafka.shadow_config, kafka.shadow_tracking, kafka.shadow_metrics,
              kafka.shadow_status, kafka.metrics
    FROM PUBLIC;
REVOKE SELECT (topic_id, partition_id, local_offset, external_offset, forwarded_at, retry_count)
    ON kafka.shadow_tracking FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.replay_shadow_messages(INT, BIGINT, BIGINT) FROM PUBLIC;

-- Reader role.
GRANT USAGE ON SCHEMA kafka TO pgkafka_reader;
GRANT SELECT ON kafka.topics TO pgkafka_reader;
GRANT SELECT ON kafka.messages TO pgkafka_reader;
GRANT SELECT ON kafka.consumer_offsets TO pgkafka_reader;
GRANT SELECT ON kafka.partition_offsets TO pgkafka_reader;
GRANT SELECT ON kafka.producer_ids TO pgkafka_reader;
GRANT SELECT ON kafka.producer_sequences TO pgkafka_reader;
GRANT SELECT ON kafka.transactions TO pgkafka_reader;
GRANT SELECT ON kafka.txn_pending_offsets TO pgkafka_reader;
GRANT SELECT ON kafka.shadow_config TO pgkafka_reader;
GRANT SELECT (topic_id, partition_id, local_offset, external_offset, forwarded_at, retry_count) ON kafka.shadow_tracking TO pgkafka_reader;
GRANT SELECT ON kafka.shadow_metrics TO pgkafka_reader;
GRANT SELECT ON kafka.shadow_status TO pgkafka_reader;
GRANT SELECT ON kafka.metrics TO pgkafka_reader;

-- Worker role (exactly storage::privileges::WORKER_TABLE_GRANTS / WORKER_SEQUENCE_GRANTS).
GRANT USAGE ON SCHEMA kafka TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.topics TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.topics_id_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.messages TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.messages_global_offset_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.partition_offsets TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.consumer_offsets TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_ids TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.producer_ids_producer_id_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_sequences TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.transactions TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.txn_pending_offsets TO pgkafka_worker;
GRANT SELECT ON kafka.shadow_config TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.shadow_tracking TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.shadow_metrics TO pgkafka_worker;

-- BEGIN broker request metrics (metrics stream)
-- =============================================================================
-- The background worker keeps per-API request/error/latency counters and broker
-- gauges (open connections, request-queue depth) in memory (src/kafka/metrics.rs)
-- and publishes them here at most once per second, only when they changed, with a
-- single upsert statement. Values are cumulative since the worker started; the
-- first flush after a worker (re)start rewrites every API row. UNLOGGED: these are
-- monitoring data, cheap to write and acceptable to lose on a crash.
CREATE UNLOGGED TABLE kafka.broker_api_stats (
    api_key SMALLINT PRIMARY KEY,
    api_name TEXT NOT NULL,
    requests BIGINT NOT NULL DEFAULT 0,       -- requests executed by the worker
    errors BIGINT NOT NULL DEFAULT 0,         -- requests whose response carried any non-zero error code, or that failed to commit
    latency_us_sum BIGINT NOT NULL DEFAULT 0, -- total DB-thread service time (handler + commit), microseconds
    latency_us_max BIGINT NOT NULL DEFAULT 0, -- largest single service time since worker start, microseconds
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNLOGGED TABLE kafka.broker_gauges (
    name TEXT PRIMARY KEY,                    -- connections_open, connections_accepted_total, connections_rejected_total, request_queue_depth, liveness_queue_depth
    value BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE VIEW kafka.api_metrics AS
SELECT
    api_key,
    api_name,
    requests,
    errors,
    latency_us_sum,
    latency_us_max,
    CASE WHEN requests > 0 THEN latency_us_sum::DOUBLE PRECISION / requests END AS latency_us_avg,
    updated_at
FROM kafka.broker_api_stats;

CREATE VIEW kafka.broker_metrics AS
SELECT name, value, updated_at FROM kafka.broker_gauges;

-- Consumer lag per committed (group, topic, partition): the log-end offset
-- (kafka.partition_offsets.next_offset, the never-decreasing counter Produce
-- assigns from) minus the committed offset. A negative committed offset (no
-- position) yields NULL lag. Groups with no commits are not visible to SQL
-- (membership lives in the in-memory coordinator).
CREATE VIEW kafka.consumer_lag AS
SELECT
    co.group_id,
    t.name AS topic,
    co.partition_id,
    co.committed_offset,
    COALESCE(po.next_offset, 0) AS high_watermark,
    CASE WHEN co.committed_offset < 0 THEN NULL
         ELSE GREATEST(COALESCE(po.next_offset, 0) - co.committed_offset, 0)
    END AS lag,
    co.commit_timestamp
FROM kafka.consumer_offsets co
JOIN kafka.topics t ON t.id = co.topic_id
LEFT JOIN kafka.partition_offsets po
       ON po.topic_id = co.topic_id AND po.partition_id = co.partition_id;

GRANT SELECT ON kafka.broker_api_stats TO pgkafka_reader;
GRANT SELECT ON kafka.broker_gauges TO pgkafka_reader;
GRANT SELECT ON kafka.api_metrics TO pgkafka_reader;
GRANT SELECT ON kafka.broker_metrics TO pgkafka_reader;
GRANT SELECT ON kafka.consumer_lag TO pgkafka_reader;

COMMENT ON TABLE kafka.broker_api_stats IS 'Per-API request counters published by the pg_kafka worker (cumulative since worker start).';
COMMENT ON TABLE kafka.broker_gauges IS 'Broker gauges published by the pg_kafka worker (connections, request-queue depth).';
COMMENT ON VIEW kafka.api_metrics IS 'Per-API requests, errors and DB-thread latency (count/sum/max/avg, microseconds).';
COMMENT ON VIEW kafka.broker_metrics IS 'Broker gauges: open/accepted/rejected connections and request-lane depths.';
COMMENT ON VIEW kafka.consumer_lag IS 'Committed offset vs log-end offset per consumer group/topic/partition.';
-- =============================================================================
-- The worker flushes these two tables (storage::privileges::WORKER_TABLE_GRANTS).
GRANT SELECT, INSERT, UPDATE ON kafka.broker_api_stats TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.broker_gauges TO pgkafka_worker;

-- END broker request metrics (metrics stream)
