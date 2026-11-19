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

-- Least-privilege roles (0.1.0). Roles are cluster-wide and are not extension
-- members, so they are created only if missing (a second database's CREATE EXTENSION,
-- or a reinstall, reuses them) and DROP EXTENSION leaves them behind. Names cannot
-- start with the reserved "pg_" prefix.
--   pgkafka_reader: read-only access; replaces the former GRANT SELECT ... TO PUBLIC,
--                   which exposed every stored message to every role. Grant it to
--                   the roles that should read pg_kafka data.
--   pgkafka_worker: exactly what the background worker's SQL needs (derived from the
--                   statements in storage/postgres.rs, shadow/store.rs and the
--                   retention sweep; see storage/privileges.rs, whose unit tests
--                   check these GRANTs). The worker switches to it via
--                   pg_kafka.run_as_role once this schema exists.
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
GRANT USAGE ON SCHEMA kafka TO pgkafka_reader;
GRANT USAGE ON SCHEMA kafka TO pgkafka_worker;

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
-- Idempotent guard for the additive column above: re-applying this file is a
-- no-op. Existing installs get schema changes from the versioned upgrade scripts
-- (sql/pg_kafka--<from>--<to>.sql, run by ALTER EXTENSION pg_kafka UPDATE; see
-- docs/UPGRADING.md), which repeat this statement for 0.0.0 installs that predate it.
ALTER TABLE kafka.topics ADD COLUMN IF NOT EXISTS retention_ms BIGINT;

-- Messages table: The actual message log with dual-offset design.
--
-- Time-partitioned (0.1.0, ADR-001): RANGE on created_at, the broker insert time.
-- Each leaf ("time partition", like a Kafka log segment) covers
-- pg_kafka.message_partition_minutes (default one day, aligned to UTC). The worker
-- creates the current and the next two leaves ahead of time through
-- kafka.ensure_message_partitions(); kafka.messages_default catches any row whose
-- leaf does not exist yet, so a Produce never fails because maintenance was late,
-- and the next maintenance pass moves such rows into their proper leaf. Retention
-- drops a whole leaf once every row in it is expired under its own topic's rule
-- (kafka.drop_message_partition) instead of deleting it row by row.
--
-- Uniqueness: Postgres only allows a unique constraint on a partitioned table if it
-- contains the partition key, so the parent has no primary key. What makes offsets
-- unique is how they are allocated: every partition_offset comes from
-- kafka.partition_offsets.next_offset, read and advanced under the per-partition
-- advisory lock and the counter row's lock, and never decremented (BUG-3); every
-- global_offset comes from one sequence. As a backstop, each leaf carries UNIQUE
-- (topic_id, partition_id, partition_offset) and UNIQUE (global_offset) indexes
-- (kafka.create_message_partition adds them to every leaf it creates), which also
-- serve the fetch/offset lookups: those queries do not name created_at, so they
-- probe every leaf's btree, one cheap descent per leaf.
--
-- No UPDATE the extension issues changes created_at (EndTxn only sets txn_state),
-- so a row never has to move between leaves.
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
    -- created_at is the broker insert time: the partition key, and what retention is measured
    -- against. TIMESTAMPTZ (not naive TIMESTAMP) so EXTRACT(EPOCH ...) is unambiguous and doesn't
    -- skew by the session time zone.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Phase 10: Transaction tracking columns
    producer_id BIGINT,                -- Producer ID for transactional messages (NULL for non-transactional)
    producer_epoch SMALLINT,           -- Producer epoch for fencing
    txn_state TEXT,                    -- NULL (committed/non-txn), 'pending' (uncommitted), 'aborted'
    -- DR-7 (DEEP-REVIEW-2026-07): reject negative partition ids at the storage layer too
    -- (handlers validate range against topics.partitions; this is the schema backstop).
    CHECK (partition_id >= 0),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Kafka fetch queries (WHERE topic_id = ? AND partition_id = ? AND partition_offset >= ?)
-- are served by each leaf's UNIQUE (topic_id, partition_id, partition_offset) btree.
-- (The former idx_messages_topic_partition_offset duplicated the old primary key
-- exactly and only doubled write amplification; dropped in 0.1.0.)

-- Retention expiry scans inside a leaf that cannot be dropped whole (WHERE topic_id = ?
-- AND created_at < cutoff). BRIN, not btree: created_at grows with insertion order, so
-- a BRIN summary per block range lets the sweep skip every block of still-retained
-- messages at a tiny fraction of a btree's size and insert cost. autosummarize asks
-- autovacuum to summarize each block range as it fills. Partitioned index: every leaf
-- gets one. Added in 0.1.0.
CREATE INDEX idx_messages_created_at_brin
ON kafka.messages USING brin (created_at) WITH (autosummarize = on);

-- Index for read_committed filtering (Phase 10: exclude pending transactional messages)
-- Supports: SELECT * FROM kafka.messages WHERE ... AND (txn_state IS NULL)
CREATE INDEX idx_messages_txn_pending
ON kafka.messages(topic_id, partition_id, partition_offset)
WHERE txn_state = 'pending';

-- The catch-all leaf. Normally empty: it only holds rows that arrived before their
-- time partition existed (or rows written out of band with a created_at outside every
-- leaf). kafka.ensure_message_partitions() moves them into a proper leaf.
CREATE TABLE kafka.messages_default PARTITION OF kafka.messages DEFAULT;
CREATE UNIQUE INDEX messages_default_offset_key
ON kafka.messages_default (topic_id, partition_id, partition_offset);
CREATE UNIQUE INDEX messages_default_global_offset_key
ON kafka.messages_default (global_offset);

-- Every leaf of kafka.messages with its bounds (NULL = MINVALUE/MAXVALUE) and whether
-- it is the DEFAULT leaf. Read-only catalog query. The bounds are parsed from
-- pg_get_expr(relpartbound), which prints timestamptz constants in the session's
-- DateStyle: the function pins DateStyle so the text round-trips exactly.
CREATE FUNCTION kafka.message_partitions()
RETURNS TABLE (partition_oid OID, partition_name TEXT, lower_bound TIMESTAMPTZ,
               upper_bound TIMESTAMPTZ, is_default BOOLEAN)
LANGUAGE sql STABLE
SET search_path = pg_catalog, pg_temp
SET datestyle = 'ISO, YMD'
AS $$
    SELECT c.oid,
           c.relname::text,
           CASE WHEN m.b IS NULL OR m.b[1] = 'MINVALUE' THEN NULL
                ELSE btrim(m.b[1], '''')::timestamptz END,
           CASE WHEN m.b IS NULL OR m.b[2] = 'MAXVALUE' THEN NULL
                ELSE btrim(m.b[2], '''')::timestamptz END,
           m.b IS NULL
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    CROSS JOIN LATERAL (
        SELECT regexp_match(pg_get_expr(c.relpartbound, c.oid),
                            '^FOR VALUES FROM \((.*)\) TO \((.*)\)$') AS b
    ) m
    WHERE i.inhparent = 'kafka.messages'::regclass
$$;

-- What a leaf holds, for the retention sweep's drop decision: every distinct
-- (topic_id, partition_id) pair (parallel arrays, found with a loose index scan over
-- the leaf's unique offset index, one btree descent per pair) and whether any row is a
-- pending transactional row. SECURITY DEFINER because the worker has no privileges on
-- the leaves themselves (it reads and writes through kafka.messages); it refuses any
-- relation that is not a leaf of kafka.messages, so it cannot read other tables.
CREATE FUNCTION kafka.message_partition_contents(
    p_partition REGCLASS,
    OUT topic_ids INT[],
    OUT partition_ids INT[],
    OUT has_pending BOOLEAN
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_inherits
                   WHERE inhrelid = p_partition AND inhparent = 'kafka.messages'::regclass) THEN
        RAISE EXCEPTION '% is not a partition of kafka.messages', p_partition;
    END IF;
    EXECUTE format(
        'WITH RECURSIVE k AS (
             (SELECT topic_id, partition_id FROM %1$s
              ORDER BY topic_id, partition_id LIMIT 1)
             UNION ALL
             SELECT n.topic_id, n.partition_id
             FROM k CROSS JOIN LATERAL (
                 SELECT l.topic_id, l.partition_id FROM %1$s l
                 WHERE (l.topic_id, l.partition_id) > (k.topic_id, k.partition_id)
                 ORDER BY l.topic_id, l.partition_id LIMIT 1) n
         )
         SELECT COALESCE(array_agg(topic_id ORDER BY topic_id, partition_id), ''{}''),
                COALESCE(array_agg(partition_id ORDER BY topic_id, partition_id), ''{}''),
                EXISTS (SELECT 1 FROM %1$s WHERE txn_state = ''pending'')
         FROM k', p_partition)
    INTO topic_ids, partition_ids, has_pending;
END;
$$;

-- Create the leaf [p_lower, p_upper) (internal; called by ensure_message_partitions).
-- Rows already in the DEFAULT leaf for that range are moved into the new leaf first
-- (Postgres refuses CREATE ... PARTITION OF while the default holds such rows). The
-- leaf gets the per-leaf unique indexes and is made a member of the extension, like
-- the tables CREATE EXTENSION made: pg_dump then leaves it out exactly as it leaves
-- out kafka.messages itself, DROP EXTENSION drops it, and no dump can restore a leaf
-- without its parent. Returns 1 if created, 0 if a lock could not be taken within
-- lock_timeout (the next maintenance pass retries).
CREATE FUNCTION kafka.create_message_partition(p_lower TIMESTAMPTZ, p_upper TIMESTAMPTZ)
RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET datestyle = 'ISO, YMD'
SET lock_timeout = '2s'
AS $$
DECLARE
    -- messages_pYYYYMMDD_HHMMSS (UTC lower bound), plus _microseconds for a bound
    -- that is not a whole second (only the gap after the upgrade's legacy leaf).
    v_name TEXT := 'messages_p' || to_char(p_lower AT TIME ZONE 'UTC', 'YYYYMMDD"_"HH24MISS')
                   || CASE WHEN date_trunc('second', p_lower) = p_lower THEN ''
                           ELSE to_char(p_lower AT TIME ZONE 'UTC', '"_"US') END;
    v_moved BIGINT := 0;
BEGIN
    IF p_lower IS NULL OR p_upper IS NULL OR p_lower >= p_upper THEN
        RAISE EXCEPTION 'invalid partition range [%, %)', p_lower, p_upper;
    END IF;
    BEGIN
        IF EXISTS (SELECT 1 FROM kafka.messages_default
                   WHERE created_at >= p_lower AND created_at < p_upper) THEN
            EXECUTE format('CREATE TABLE kafka.%I (LIKE kafka.messages INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_name);
            EXECUTE format(
                'WITH moved AS (DELETE FROM kafka.messages_default
                                WHERE created_at >= $1 AND created_at < $2 RETURNING *)
                 INSERT INTO kafka.%I SELECT * FROM moved', v_name)
            USING p_lower, p_upper;
            GET DIAGNOSTICS v_moved = ROW_COUNT;
            EXECUTE format('CREATE UNIQUE INDEX %I ON kafka.%I (topic_id, partition_id, partition_offset)',
                           v_name || '_offset_key', v_name);
            EXECUTE format('CREATE UNIQUE INDEX %I ON kafka.%I (global_offset)',
                           v_name || '_global_offset_key', v_name);
            EXECUTE format('ALTER TABLE kafka.messages ATTACH PARTITION kafka.%I FOR VALUES FROM (%L) TO (%L)',
                           v_name, p_lower, p_upper);
        ELSE
            EXECUTE format('CREATE TABLE kafka.%I PARTITION OF kafka.messages FOR VALUES FROM (%L) TO (%L)',
                           v_name, p_lower, p_upper);
            EXECUTE format('CREATE UNIQUE INDEX %I ON kafka.%I (topic_id, partition_id, partition_offset)',
                           v_name || '_offset_key', v_name);
            EXECUTE format('CREATE UNIQUE INDEX %I ON kafka.%I (global_offset)',
                           v_name || '_global_offset_key', v_name);
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_depend
                       WHERE classid = 'pg_class'::regclass
                         AND objid = format('kafka.%I', v_name)::regclass
                         AND deptype = 'e') THEN
            EXECUTE format('ALTER EXTENSION pg_kafka ADD TABLE kafka.%I', v_name);
        END IF;
    EXCEPTION WHEN lock_not_available THEN
        RAISE WARNING 'pg_kafka: could not create time partition kafka.% (lock timeout); will retry', v_name;
        RETURN 0;
    END;
    IF v_moved > 0 THEN
        RAISE LOG 'pg_kafka: created time partition kafka.% and moved % row(s) into it from kafka.messages_default',
            v_name, v_moved;
    END IF;
    RETURN 1;
END;
$$;

-- Partition maintenance (worker: at start and every sweep interval; operators: any
-- time). Makes sure leaves of p_width_minutes (aligned to UTC multiples of the width)
-- exist for now and the next p_ahead widths, and gives every row in the DEFAULT leaf a
-- proper leaf. Ranges that already have a leaf (e.g. made under an earlier width) are
-- left alone: only the gaps are filled, so changing the width never overlaps. Runs as
-- the extension owner (DDL) but only ever creates leaves of kafka.messages. Returns the
-- number of leaves created.
CREATE FUNCTION kafka.ensure_message_partitions(p_width_minutes INT, p_ahead INT DEFAULT 2)
RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_width_s BIGINT;
    v_width INTERVAL;
    v_bucket TIMESTAMPTZ;
    v_cur TIMESTAMPTZ;
    v_hi TIMESTAMPTZ;
    v_ov RECORD;
    v_created INT := 0;
BEGIN
    IF p_width_minutes IS NULL OR p_width_minutes < 1 THEN
        RAISE EXCEPTION 'partition width must be at least 1 minute, got %', p_width_minutes;
    END IF;
    v_width_s := p_width_minutes::BIGINT * 60;
    v_width := make_interval(secs => v_width_s);
    -- Serializes concurrent maintenance (worker and a manual call). Key: 'pg_kafka'.
    PERFORM pg_advisory_xact_lock(8099547369649367905);
    FOR v_bucket IN
        SELECT DISTINCT b FROM (
            SELECT to_timestamp((floor(extract(epoch FROM now()) / v_width_s) + g) * v_width_s) AS b
            FROM generate_series(0, GREATEST(COALESCE(p_ahead, 2), 0)) AS g
            UNION
            SELECT to_timestamp(floor(extract(epoch FROM d.created_at) / v_width_s) * v_width_s)
            FROM kafka.messages_default d
        ) buckets
        ORDER BY b
    LOOP
        -- Fill the gaps of [v_bucket, v_bucket + width) not covered by an existing leaf.
        v_cur := v_bucket;
        v_hi := v_bucket + v_width;
        WHILE v_cur < v_hi LOOP
            SELECT p.lower_bound, p.upper_bound INTO v_ov
            FROM kafka.message_partitions() p
            WHERE NOT p.is_default
              AND COALESCE(p.upper_bound, 'infinity') > v_cur
              AND COALESCE(p.lower_bound, '-infinity') < v_hi
            ORDER BY p.lower_bound NULLS FIRST
            LIMIT 1;
            IF NOT FOUND THEN
                v_created := v_created + kafka.create_message_partition(v_cur, v_hi);
                EXIT;
            END IF;
            IF COALESCE(v_ov.lower_bound, '-infinity') > v_cur THEN
                v_created := v_created + kafka.create_message_partition(v_cur, v_ov.lower_bound);
            END IF;
            v_cur := COALESCE(v_ov.upper_bound, 'infinity');
        END LOOP;
    END LOOP;
    RETURN v_created;
END;
$$;

-- Drop one time leaf for retention, if (re-checked here, under the locks) it holds no
-- pending transactional row and every topic with rows in it is listed in
-- p_expired_topics (the topics the caller found wholly expired in this leaf: its upper
-- bound is older than the topic's retention window). Never drops the DEFAULT leaf.
-- Returns whether it dropped the leaf and the (topic_id, partition_id) pairs the leaf
-- held, so the caller advances their log_start_offset. Dropping a leaf is equivalent
-- to deleting its rows, which the worker may already do; the function grants no more.
CREATE FUNCTION kafka.drop_message_partition(
    p_partition REGCLASS,
    p_expired_topics INT[],
    OUT dropped BOOLEAN,
    OUT topic_ids INT[],
    OUT partition_ids INT[]
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '2s'
AS $$
DECLARE
    v_contents RECORD;
BEGIN
    dropped := false;
    IF NOT EXISTS (SELECT 1 FROM kafka.message_partitions() p
                   WHERE p.partition_oid = p_partition AND NOT p.is_default) THEN
        RAISE EXCEPTION '% is not a time partition of kafka.messages', p_partition;
    END IF;
    PERFORM pg_advisory_xact_lock(8099547369649367905);
    BEGIN
        -- DROP takes ACCESS EXCLUSIVE on the parent; take it first (parent, then leaf,
        -- the order inserts use) so the contents check below cannot race a writer.
        LOCK TABLE kafka.messages IN ACCESS EXCLUSIVE MODE;
        SELECT * INTO v_contents FROM kafka.message_partition_contents(p_partition);
        topic_ids := v_contents.topic_ids;
        partition_ids := v_contents.partition_ids;
        IF v_contents.has_pending
           OR NOT (v_contents.topic_ids <@ COALESCE(p_expired_topics, '{}'::INT[])) THEN
            RETURN;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_depend
                   WHERE classid = 'pg_class'::regclass AND objid = p_partition AND deptype = 'e') THEN
            EXECUTE format('ALTER EXTENSION pg_kafka DROP TABLE %s', p_partition);
        END IF;
        EXECUTE format('DROP TABLE %s', p_partition);
        dropped := true;
    EXCEPTION WHEN lock_not_available THEN
        RAISE WARNING 'pg_kafka: could not drop time partition % (lock timeout); will retry', p_partition;
        dropped := false;
        topic_ids := NULL;
        partition_ids := NULL;
    END;
END;
$$;

-- Functions are executable by PUBLIC by default: these are for the worker role only.
REVOKE EXECUTE ON FUNCTION kafka.message_partition_contents(REGCLASS) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.create_message_partition(TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.ensure_message_partitions(INT, INT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.drop_message_partition(REGCLASS, INT[]) FROM PUBLIC;

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
    -- Durable log start offset. DeleteRecords (API 21) advances this to the
    -- truncation point so a fully-emptied partition still reports the correct
    -- earliest offset (ListOffsets EARLIEST / Fetch log_start_offset) instead of
    -- regressing to 0 — matching Kafka, which never lets the log start move
    -- backwards. 0 for partitions that have never been truncated.
    log_start_offset BIGINT NOT NULL DEFAULT 0,
    CHECK (partition_id >= 0),
    PRIMARY KEY (topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);
-- Idempotent guard for the additive log_start_offset column (see the note on
-- kafka.topics above). Manual in-place upgrade path for existing installs.
ALTER TABLE kafka.partition_offsets
    ADD COLUMN IF NOT EXISTS log_start_offset BIGINT NOT NULL DEFAULT 0;

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

-- Grant permissions (roles: see the top of this file)
GRANT SELECT ON kafka.topics TO pgkafka_reader;
GRANT SELECT ON kafka.messages TO pgkafka_reader;
GRANT SELECT ON kafka.consumer_offsets TO pgkafka_reader;
GRANT SELECT ON kafka.partition_offsets TO pgkafka_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.topics TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.topics_id_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.messages TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.messages_global_offset_seq TO pgkafka_worker;
-- Time partitions (ADR-001): the worker reads and writes the leaves only through
-- kafka.messages, except the retention sweep's row delete in the DEFAULT leaf; all
-- partition DDL goes through these SECURITY DEFINER functions.
GRANT SELECT, DELETE ON kafka.messages_default TO pgkafka_worker;
GRANT EXECUTE ON FUNCTION kafka.message_partition_contents(regclass) TO pgkafka_worker;
GRANT EXECUTE ON FUNCTION kafka.ensure_message_partitions(int, int) TO pgkafka_worker;
GRANT EXECUTE ON FUNCTION kafka.drop_message_partition(regclass, int[]) TO pgkafka_worker;
-- partition_offsets is only upserted; rows go with their topic (ON DELETE CASCADE).
GRANT SELECT, INSERT, UPDATE ON kafka.partition_offsets TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.consumer_offsets TO pgkafka_worker;

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
GRANT SELECT ON kafka.producer_ids TO pgkafka_reader;
GRANT SELECT ON kafka.producer_sequences TO pgkafka_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_ids TO pgkafka_worker;
-- nextval() for new ids and the RV-13 epoch-exhaustion reallocation.
GRANT USAGE ON SEQUENCE kafka.producer_ids_producer_id_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_sequences TO pgkafka_worker;

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
GRANT SELECT ON kafka.transactions TO pgkafka_reader;
GRANT SELECT ON kafka.txn_pending_offsets TO pgkafka_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.transactions TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.txn_pending_offsets TO pgkafka_worker;

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
GRANT SELECT ON kafka.shadow_config TO pgkafka_reader;
-- RV-10 (SEC-8 class): error_message can hold raw external-broker error strings
-- (authz failures, host/topic names, payload-size limits), so it is withheld from
-- the reader role. Grant only the non-sensitive columns column-by-column instead of
-- the whole row; the worker role keeps full access to error_message.
GRANT SELECT (topic_id, partition_id, local_offset, external_offset, forwarded_at, retry_count) ON kafka.shadow_tracking TO pgkafka_reader;
GRANT SELECT ON kafka.shadow_metrics TO pgkafka_reader;
GRANT SELECT ON kafka.shadow_status TO pgkafka_reader;
-- The worker only reads shadow_config (operators manage it).
GRANT SELECT ON kafka.shadow_config TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.shadow_tracking TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.shadow_metrics TO pgkafka_worker;

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

-- It writes the outbox and runs with the caller's rights: no longer executable by
-- PUBLIC (functions are by default). Superusers run it, or GRANT EXECUTE explicitly
-- to a role that also has INSERT/UPDATE on kafka.shadow_tracking.
REVOKE EXECUTE ON FUNCTION kafka.replay_shadow_messages(INT, BIGINT, BIGINT) FROM PUBLIC;
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
-- Message metrics (estimates; see view comment), summed over the leaves of the
-- time-partitioned kafka.messages (the partitioned parent itself stores nothing).
SELECT
    'messages' AS category,
    'count_estimate' AS metric,
    COALESCE(SUM(GREATEST(c.reltuples, 0)), 0)::BIGINT AS value,
    'total' AS unit
FROM pg_partition_tree('kafka.messages'::regclass) pt
JOIN pg_class c ON c.oid = pt.relid
WHERE pt.isleaf
UNION ALL
SELECT
    'messages' AS category,
    'stored_bytes' AS metric,
    COALESCE(SUM(pg_table_size(pt.relid)), 0)::BIGINT AS value,
    'bytes' AS unit
FROM pg_partition_tree('kafka.messages'::regclass) pt
WHERE pt.isleaf
UNION ALL
SELECT
    'messages' AS category,
    'time_partitions' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM pg_partition_tree('kafka.messages'::regclass) pt
WHERE pt.isleaf
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

GRANT SELECT ON kafka.metrics TO pgkafka_reader;

COMMENT ON VIEW kafka.metrics IS 'Unified operational metrics view for monitoring pg_kafka health.
Categories: topics, messages, consumer_groups, consumer_offsets, producers, transactions, shadow.
messages.count_estimate and messages.stored_bytes are catalog estimates summed over the
time partitions (refreshed by (auto)VACUUM/ANALYZE), not exact scans; messages.time_partitions
counts the leaves of kafka.messages (DEFAULT included); shadow.lag is the pending-outbox depth.
Use with: SELECT * FROM kafka.metrics WHERE category = ''messages'';';

-- =============================================================================
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
COMMENT ON VIEW kafka.api_metrics IS 'Per-API requests, errors and latency (count/sum/max/avg, microseconds): handler plus batch commit for worker APIs, decode to response on the network thread for SaslHandshake/SaslAuthenticate.';
COMMENT ON VIEW kafka.broker_metrics IS 'Broker gauges: open/accepted/rejected connections and request-lane depths.';
COMMENT ON VIEW kafka.consumer_lag IS 'Committed offset vs log-end offset per consumer group/topic/partition.';
-- =============================================================================
-- The worker flushes these two tables (storage::privileges::WORKER_TABLE_GRANTS).
GRANT SELECT, INSERT, UPDATE ON kafka.broker_api_stats TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.broker_gauges TO pgkafka_worker;

-- END broker request metrics (metrics stream)
-- =============================================================================

-- =============================================================================
-- SASL credentials (auth stream)
-- =============================================================================
-- One SCRAM credential per Kafka user and mechanism, stored the way Kafka stores
-- them: salt, iteration count, StoredKey = H(HMAC(SaltedPassword, 'Client Key')),
-- ServerKey = HMAC(SaltedPassword, 'Server Key'). No password is ever stored. SASL/PLAIN
-- is checked against the SCRAM-SHA-256 credential (or SCRAM-SHA-512 if that is the
-- only one). Manage users with pg_kafka_create_user / pg_kafka_alter_user /
-- pg_kafka_set_user_verifier / pg_kafka_drop_user (src/kafka/auth/users.rs).
--
-- Not readable by pgkafka_reader (it gets the kafka.sasl_users view, without the
-- salts and keys); pgkafka_worker may only SELECT it.
CREATE TABLE kafka.sasl_credentials (
    username TEXT NOT NULL CHECK (octet_length(username) BETWEEN 1 AND 255),
    mechanism TEXT NOT NULL CHECK (mechanism IN ('SCRAM-SHA-256', 'SCRAM-SHA-512')),
    salt BYTEA NOT NULL CHECK (octet_length(salt) > 0),
    iterations INT NOT NULL CHECK (iterations BETWEEN 4096 AND 16384),
    stored_key BYTEA NOT NULL,
    server_key BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, mechanism)
);

-- Include the credentials in pg_dump output (extension tables are skipped otherwise),
-- so a dump/restore keeps the Kafka users. A dump therefore contains the verifiers.
SELECT pg_catalog.pg_extension_config_dump('kafka.sasl_credentials', '');

CREATE VIEW kafka.sasl_users AS
SELECT username, mechanism, iterations, created_at, updated_at
FROM kafka.sasl_credentials;

GRANT SELECT ON kafka.sasl_users TO pgkafka_reader;
GRANT SELECT ON kafka.sasl_credentials TO pgkafka_worker;

COMMENT ON TABLE kafka.sasl_credentials IS 'SCRAM credentials of Kafka SASL users (salt, iterations, StoredKey, ServerKey; never a password).';
COMMENT ON VIEW kafka.sasl_users IS 'Kafka SASL users and their mechanisms, without secrets.';

-- END SASL credentials (auth stream)
-- =============================================================================

-- =============================================================================
-- wave3 groups: bound kafka.topics.partitions
-- =============================================================================
-- Metadata and the assignors allocate one entry per partition, so an unbounded stored count
-- (CreateTopics used to accept 2000000000) aborted the worker on every Metadata request and
-- sent the server into crash recovery. The handlers cap counts at
-- pg_kafka.max_partitions_per_topic (at most 100000); this is the schema backstop.
ALTER TABLE kafka.topics ADD CONSTRAINT topics_partitions_range
    CHECK (partitions >= 1 AND partitions <= 100000);
-- END wave3 groups
-- wave3 txn: idempotent producer batch cache + transaction partition registry
-- =============================================================================
-- The last 5 batches (Kafka's max.in.flight bound for idempotence) of every producer
-- on every partition: their sequence range and base offset, so a retried batch is
-- answered with its original offset and anything else out of sequence is refused.
-- Rows belong to the current epoch only: every epoch change deletes the producer's
-- kafka.producer_sequences rows, and this table follows them (ON DELETE CASCADE).
CREATE TABLE kafka.producer_batches (
    producer_id BIGINT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    first_sequence INT NOT NULL CHECK (first_sequence >= 0),
    last_sequence INT NOT NULL CHECK (last_sequence >= 0),
    base_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (producer_id, topic_id, partition_id, base_offset),
    FOREIGN KEY (producer_id, topic_id, partition_id)
        REFERENCES kafka.producer_sequences (producer_id, topic_id, partition_id)
        ON DELETE CASCADE
);

-- Partitions registered in the open transaction of a transactional id
-- (AddPartitionsToTxn). A transactional Produce to any other partition is refused with
-- INVALID_TXN_STATE, as in Kafka. Cleared when the next transaction begins and on
-- InitProducerId.
CREATE TABLE kafka.txn_partitions (
    transactional_id TEXT NOT NULL
        REFERENCES kafka.transactions (transactional_id) ON DELETE CASCADE,
    topic_id INT NOT NULL REFERENCES kafka.topics (id) ON DELETE CASCADE,
    partition_id INT NOT NULL CHECK (partition_id >= 0),
    PRIMARY KEY (transactional_id, topic_id, partition_id)
);

GRANT SELECT ON kafka.producer_batches TO pgkafka_reader;
GRANT SELECT ON kafka.txn_partitions TO pgkafka_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_batches TO pgkafka_worker;
GRANT SELECT, INSERT, DELETE ON kafka.txn_partitions TO pgkafka_worker;

COMMENT ON TABLE kafka.producer_batches IS 'Last 5 batches per idempotent producer and partition (sequence range, base offset) for duplicate detection.';
COMMENT ON TABLE kafka.txn_partitions IS 'Partitions added (AddPartitionsToTxn) to the open transaction of each transactional id.';

-- END wave3 txn
-- =============================================================================
