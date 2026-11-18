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
