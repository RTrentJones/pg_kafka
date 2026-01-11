-- =============================================================================
-- Phase 11: Shadow Mode
-- Enables forwarding messages to an external Kafka cluster for gradual migration
-- =============================================================================

-- Shadow mode configuration per topic
-- Controls which topics are forwarded and with what settings
CREATE TABLE kafka.shadow_config (
    topic_id INT PRIMARY KEY REFERENCES kafka.topics(id) ON DELETE CASCADE,
    mode TEXT NOT NULL DEFAULT 'local_only',      -- local_only, shadow
    forward_percentage INT NOT NULL DEFAULT 0,    -- 0-100, percentage to forward
    external_topic_name TEXT,                     -- NULL = same name as local topic
    sync_mode TEXT NOT NULL DEFAULT 'sync',       -- async (deprecated), sync
    write_mode TEXT NOT NULL DEFAULT 'dual_write',  -- dual_write, external_only
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_mode CHECK (mode IN ('local_only', 'shadow')),
    CONSTRAINT valid_percentage CHECK (forward_percentage >= 0 AND forward_percentage <= 100),
    CONSTRAINT valid_sync_mode CHECK (sync_mode IN ('async', 'sync')),
    CONSTRAINT valid_write_mode CHECK (write_mode IN ('dual_write', 'external_only'))
);

-- Tracking table for forwarding state
-- Records which messages have been forwarded and their external offsets
CREATE TABLE kafka.shadow_tracking (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    local_offset BIGINT NOT NULL,
    external_offset BIGINT,                       -- NULL = not yet forwarded
    forwarded_at TIMESTAMP,
    error_message TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id, partition_id, local_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Index for replay queries (find unforwarded messages)
CREATE INDEX idx_shadow_tracking_pending
ON kafka.shadow_tracking(topic_id, partition_id, local_offset)
WHERE external_offset IS NULL;

-- Index for error analysis
CREATE INDEX idx_shadow_tracking_errors
ON kafka.shadow_tracking(topic_id, partition_id)
WHERE error_message IS NOT NULL;

-- Per-partition metrics for shadow mode
-- Tracks forwarding statistics for monitoring and alerting
CREATE TABLE kafka.shadow_metrics (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    messages_forwarded BIGINT NOT NULL DEFAULT 0,
    messages_skipped BIGINT NOT NULL DEFAULT 0,   -- Skipped due to percentage dial
    messages_failed BIGINT NOT NULL DEFAULT 0,
    last_forwarded_offset BIGINT NOT NULL DEFAULT -1,
    last_forwarded_at TIMESTAMP,
    PRIMARY KEY (topic_id, partition_id)
);

-- Monitoring view: Shadow mode status by topic
-- Provides a unified view of shadow mode health and lag
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

-- Function to enable shadow mode for a topic
CREATE OR REPLACE FUNCTION kafka.enable_shadow_mode(
    p_topic_name TEXT,
    p_forward_percentage INT DEFAULT 100,
    p_external_topic_name TEXT DEFAULT NULL,
    p_sync_mode TEXT DEFAULT 'async'
) RETURNS VOID AS $$
DECLARE
    v_topic_id INT;
BEGIN
    -- Get topic ID
    SELECT id INTO v_topic_id FROM kafka.topics WHERE name = p_topic_name;
    IF v_topic_id IS NULL THEN
        RAISE EXCEPTION 'Topic % does not exist', p_topic_name;
    END IF;

    -- Upsert shadow config
    INSERT INTO kafka.shadow_config (topic_id, mode, forward_percentage, external_topic_name, sync_mode, updated_at)
    VALUES (v_topic_id, 'shadow', p_forward_percentage, p_external_topic_name, p_sync_mode, NOW())
    ON CONFLICT (topic_id) DO UPDATE SET
        mode = 'shadow',
        forward_percentage = EXCLUDED.forward_percentage,
        external_topic_name = EXCLUDED.external_topic_name,
        sync_mode = EXCLUDED.sync_mode,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to disable shadow mode for a topic
CREATE OR REPLACE FUNCTION kafka.disable_shadow_mode(p_topic_name TEXT) RETURNS VOID AS $$
DECLARE
    v_topic_id INT;
BEGIN
    SELECT id INTO v_topic_id FROM kafka.topics WHERE name = p_topic_name;
    IF v_topic_id IS NULL THEN
        RAISE EXCEPTION 'Topic % does not exist', p_topic_name;
    END IF;

    UPDATE kafka.shadow_config
    SET mode = 'local_only', updated_at = NOW()
    WHERE topic_id = v_topic_id;
END;
$$ LANGUAGE plpgsql;

-- Function to replay unforwarded messages for a topic
-- Returns the number of messages queued for replay
CREATE OR REPLACE FUNCTION kafka.shadow_replay(
    p_topic_name TEXT,
    p_from_offset BIGINT DEFAULT 0,
    p_to_offset BIGINT DEFAULT NULL
) RETURNS TABLE (
    partition_id INT,
    messages_queued BIGINT
) AS $$
DECLARE
    v_topic_id INT;
BEGIN
    SELECT id INTO v_topic_id FROM kafka.topics WHERE name = p_topic_name;
    IF v_topic_id IS NULL THEN
        RAISE EXCEPTION 'Topic % does not exist', p_topic_name;
    END IF;

    -- Reset retry count for unforwarded messages in range to trigger retry
    RETURN QUERY
    WITH reset_messages AS (
        UPDATE kafka.shadow_tracking
        SET retry_count = 0, error_message = NULL
        WHERE topic_id = v_topic_id
          AND external_offset IS NULL
          AND local_offset >= p_from_offset
          AND (p_to_offset IS NULL OR local_offset <= p_to_offset)
        RETURNING shadow_tracking.partition_id, local_offset
    )
    SELECT reset_messages.partition_id, COUNT(*)::BIGINT AS messages_queued
    FROM reset_messages
    GROUP BY reset_messages.partition_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get shadow mode lag per topic
CREATE OR REPLACE FUNCTION kafka.shadow_lag(p_topic_name TEXT DEFAULT NULL)
RETURNS TABLE (
    topic_name TEXT,
    partition_id INT,
    current_offset BIGINT,
    last_forwarded_offset BIGINT,
    lag BIGINT,
    pending_count BIGINT,
    failed_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.name,
        m.partition_id,
        MAX(m.partition_offset),
        COALESCE(sm.last_forwarded_offset, -1),
        MAX(m.partition_offset) - COALESCE(sm.last_forwarded_offset, -1),
        COUNT(st.local_offset) FILTER (WHERE st.external_offset IS NULL AND st.error_message IS NULL),
        COUNT(st.local_offset) FILTER (WHERE st.error_message IS NOT NULL)
    FROM kafka.topics t
    JOIN kafka.messages m ON t.id = m.topic_id
    LEFT JOIN kafka.shadow_metrics sm ON t.id = sm.topic_id AND m.partition_id = sm.partition_id
    LEFT JOIN kafka.shadow_tracking st ON t.id = st.topic_id AND m.partition_id = st.partition_id
    WHERE p_topic_name IS NULL OR t.name = p_topic_name
    GROUP BY t.name, m.partition_id, sm.last_forwarded_offset;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions for shadow mode tables
GRANT SELECT ON kafka.shadow_config TO PUBLIC;
GRANT SELECT ON kafka.shadow_tracking TO PUBLIC;
GRANT SELECT ON kafka.shadow_metrics TO PUBLIC;
GRANT SELECT ON kafka.shadow_status TO PUBLIC;

-- Comments for documentation
COMMENT ON TABLE kafka.shadow_config IS 'Per-topic shadow mode configuration. Controls forwarding to external Kafka.';

COMMENT ON COLUMN kafka.shadow_config.mode IS 'Shadow mode: local_only (no forwarding), shadow (forward to both local and external)';

COMMENT ON COLUMN kafka.shadow_config.forward_percentage IS 'Percentage of messages to forward (0-100). Deterministic hash-based routing for consistency.';

COMMENT ON COLUMN kafka.shadow_config.external_topic_name IS 'Topic name in external Kafka. NULL means use same name as local topic.';

COMMENT ON COLUMN kafka.shadow_config.sync_mode IS 'Forwarding mode: async (non-blocking), sync (wait for external ack)';

COMMENT ON TABLE kafka.shadow_tracking IS 'Tracks which messages have been forwarded to external Kafka and their status.';

COMMENT ON COLUMN kafka.shadow_tracking.external_offset IS 'Offset assigned by external Kafka. NULL if not yet forwarded.';

COMMENT ON COLUMN kafka.shadow_tracking.retry_count IS 'Number of forward attempts. Used for exponential backoff.';

COMMENT ON TABLE kafka.shadow_metrics IS 'Aggregated metrics per topic/partition for shadow mode monitoring.';

COMMENT ON VIEW kafka.shadow_status IS 'Unified view of shadow mode health including lag and error counts by topic.';

COMMENT ON FUNCTION kafka.enable_shadow_mode IS 'Enable shadow mode for a topic with optional percentage, external name, and sync mode.';

COMMENT ON FUNCTION kafka.disable_shadow_mode IS 'Disable shadow mode for a topic (sets mode to local_only).';

COMMENT ON FUNCTION kafka.shadow_replay IS 'Queue unforwarded messages for replay. Returns count per partition.';

COMMENT ON FUNCTION kafka.shadow_lag IS 'Get shadow mode lag and pending/failed counts per topic/partition.';
