-- pg_kafka upgrade 0.0.0 -> 0.1.0
--
-- Run with:  ALTER EXTENSION pg_kafka UPDATE TO '0.1.0';
-- (installed next to the extension by `cargo pgrx install` / `cargo pgrx package`).
-- See docs/UPGRADING.md. The result must match a fresh 0.1.0 install; CI checks it
-- with scripts/check-upgrade.sh.
--
-- Converts kafka.messages to a time-partitioned table (the existing rows become one
-- leaf) under an ACCESS EXCLUSIVE lock, rewrites it once to fill record_bytes (reading it,
-- writing a copy and reading that copy once per index rebuilt, three) and reads it six more
-- times: ten full reads (see step 1; an install whose created_at is a naive TIMESTAMP gets
-- one more rewrite with its three index rebuilds); run it in a quiet period on a large table.

\echo Use "ALTER EXTENSION pg_kafka UPDATE TO '0.1.0'" to load this file. \quit

-- Take the pg_kafka schema lock first. Every transaction of a running
-- background worker holds it in SHARE mode (src/worker/txn.rs, SCHEMA_LOCK_KEY), so this
-- waits for the worker's current transaction and keeps new ones out until the upgrade
-- commits: the script's DDL never meets a worker transaction holding some of the same
-- tables in another order (lock-order deadlocks). Every upgrade script must start so.
SELECT pg_advisory_xact_lock(8099547369649367906);

-- 0.0.0 installs that predate these additive columns (see the notes in bootstrap.sql).
ALTER TABLE kafka.topics ADD COLUMN IF NOT EXISTS retention_ms BIGINT;
ALTER TABLE kafka.partition_offsets
    ADD COLUMN IF NOT EXISTS log_start_offset BIGINT NOT NULL DEFAULT 0;

-- InitProducerId with a presented producer id and epoch (KIP-360): the epoch a retry of
-- the producer's own last bump presents.
ALTER TABLE kafka.producer_ids ADD COLUMN last_epoch SMALLINT;
COMMENT ON COLUMN kafka.producer_ids.last_epoch IS 'Epoch before the last bump the producer requested itself (a retry of that InitProducerId gets the current epoch back); NULL after a fencing bump.';

-- Producer ids continue from the upgrade time in milliseconds since the Unix epoch, as
-- a fresh install starts from its creation time (see bootstrap.sql), and never below an
-- id already issued.
SELECT pg_catalog.setval('kafka.producer_ids_producer_id_seq',
                         GREATEST((extract(epoch FROM clock_timestamp()) * 1000)::bigint,
                                  (SELECT last_value FROM kafka.producer_ids_producer_id_seq),
                                  (SELECT COALESCE(MAX(producer_id), 0) FROM kafka.producer_ids)));

-- Producer ids by idleness, for the retention sweep's prune (see bootstrap.sql).
CREATE INDEX idx_producer_ids_last_active
ON kafka.producer_ids(last_active_at, producer_id);

-- Offset commits maintain only the primary key and idx_consumer_offsets_topic (see
-- bootstrap.sql): the primary key leads with group_id, and nothing reads
-- commit_timestamp through an index.
DROP INDEX IF EXISTS kafka.idx_consumer_offsets_group;
DROP INDEX IF EXISTS kafka.idx_consumer_offsets_timestamp;

-- Rows by topic, for the foreign keys on kafka.topics, whose ON DELETE CASCADE
-- (DeleteTopics) would otherwise read each table whole once per deleted topic (see
-- bootstrap.sql). kafka.txn_partitions gets its index where it is created.
CREATE INDEX idx_consumer_offsets_topic ON kafka.consumer_offsets(topic_id);
CREATE INDEX idx_producer_sequences_topic ON kafka.producer_sequences(topic_id);
CREATE INDEX idx_txn_pending_offsets_topic ON kafka.txn_pending_offsets(topic_id);

-- 1. Time-partition kafka.messages (ADR-001).
--
-- The populated 0.0.0 table is not copied into a new one: it is renamed to
-- kafka.messages_legacy and attached to the new partitioned kafka.messages as one leaf covering
-- [MINVALUE, the upgrade time). It keeps its primary key and unique index (renamed to
-- the per-leaf names), its rows, offsets and global_offset sequence. The retention
-- sweep treats it like any other past leaf: it is dropped whole once every row in it
-- is expired under its topic's retention (row deletes until then only for topics with
-- a window shorter than one partition width). New rows go to the new time leaves.
--
-- Cost: ACCESS EXCLUSIVE on the old table for the whole script; one rewrite of it (the
-- record_bytes fill: a full read and write, then the rebuild of its three indexes, the two
-- unique ones and the 0.0.0 pending-row index renamed messages_legacy_txn_pending_idx below,
-- which ATTACH adopts as idx_messages_txn_pending's leaf, each rebuild a full read of the new
-- copy) and six sequential reads (MAX(created_at) for the leaf bound, the attach's bound check, and the BRIN,
-- idx_messages_txn_aborted, idx_messages_txn_pending_producer and idx_messages_timestamp
-- builds).
ALTER TABLE kafka.messages RENAME TO messages_legacy;
ALTER INDEX kafka.messages_pkey RENAME TO messages_legacy_offset_key;
ALTER INDEX kafka.messages_global_offset_key RENAME TO messages_legacy_global_offset_key;
-- Constraint names are chosen unique per schema: free the 0.0.0 names so the new
-- parent gets the same names as a fresh install (the CHECK is renamed back below,
-- ATTACH matches CHECK constraints by name).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
               WHERE conrelid = 'kafka.messages_legacy'::regclass AND conname = 'messages_partition_id_check') THEN
        ALTER TABLE kafka.messages_legacy RENAME CONSTRAINT messages_partition_id_check TO messages_legacy_partition_id_check;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
               WHERE conrelid = 'kafka.messages_legacy'::regclass AND conname = 'messages_topic_id_fkey') THEN
        ALTER TABLE kafka.messages_legacy RENAME CONSTRAINT messages_topic_id_fkey TO messages_legacy_topic_id_fkey;
    END IF;
END
$$;
-- The duplicate of the old primary key (dropped in 0.1.0).
DROP INDEX IF EXISTS kafka.idx_messages_topic_partition_offset;
ALTER INDEX IF EXISTS kafka.idx_messages_txn_pending RENAME TO messages_legacy_txn_pending_idx;
-- Leaves are reached only through kafka.messages (0.0.0 granted the table to PUBLIC).
REVOKE ALL ON kafka.messages_legacy FROM PUBLIC;
-- These views follow the renamed table; they are recreated on the new parent below.
DROP VIEW kafka.shadow_status;
DROP VIEW kafka.metrics;

-- Time-partitioned (ADR-001): RANGE on created_at, the broker insert time.
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
    global_offset BIGINT NOT NULL DEFAULT nextval('kafka.messages_global_offset_seq'::regclass), -- the 0.0.0 BIGSERIAL's sequence, kept
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
    -- Transaction tracking (NULL for non-transactional records)
    producer_id BIGINT,                -- Producer ID for transactional messages (NULL for non-transactional)
    producer_epoch SMALLINT,           -- Producer epoch for fencing
    txn_state TEXT,                    -- NULL (committed/non-txn), 'pending' (uncommitted), 'aborted'
    -- What the record adds to a Fetch response (uncompressed key, value and headers plus
    -- their framing), stored by Produce so a fetch sums the sizes of the rows it scans
    -- without reading their keys, values or headers (from TOAST, when stored there). NULL
    -- for a row written by SQL outside the extension: fetches then compute it.
    record_bytes INT,
    -- DR-7 (DEEP-REVIEW-2026-07): reject negative partition ids at the storage layer too
    -- (handlers validate range against topics.partitions; this is the schema backstop).
    CHECK (partition_id >= 0),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Kafka fetch queries (WHERE topic_id = ? AND partition_id = ? AND partition_offset >= ?)
-- are served by each leaf's UNIQUE (topic_id, partition_id, partition_offset) btree.

-- Retention expiry scans inside a leaf that cannot be dropped whole (WHERE topic_id = ?
-- AND created_at < cutoff). BRIN, not btree: created_at grows with insertion order, so
-- a BRIN summary per block range lets the sweep skip every block of still-retained
-- messages at a tiny fraction of a btree's size and insert cost. autosummarize asks
-- autovacuum to summarize each block range as it fills. Partitioned index: every leaf
-- gets one.
CREATE INDEX idx_messages_created_at_brin
ON kafka.messages USING brin (created_at) WITH (autosummarize = on);

-- The lowest pending transactional offset of a partition (the last stable offset of
-- read_committed fetches).
CREATE INDEX idx_messages_txn_pending
ON kafka.messages(topic_id, partition_id, partition_offset)
WHERE txn_state = 'pending';

-- A transactional producer's pending rows, for the statements that end its
-- transaction (EndTxn's commit and abort, the abort of a fenced epoch or a timed-out
-- transaction, the shadow outbox's commit enqueue). idx_messages_txn_pending leads
-- with the partition, so those statements read every pending row of every producer;
-- this one reads only the producer's own. Partitioned index: every leaf gets one.
CREATE INDEX idx_messages_txn_pending_producer
ON kafka.messages(producer_id, producer_epoch)
WHERE txn_state = 'pending';

-- Aborted transactional rows, for the retention sweep's aborted-row cleanup (every
-- pass, whatever the retention settings): without it that DELETE read every block of
-- every leaf (the BRIN on created_at matches nearly all of them), on the single worker
-- DB thread. Partial, so only aborted rows are indexed: a non-transactional or
-- committed write never touches it. Partitioned index: every leaf gets one.
CREATE INDEX idx_messages_txn_aborted
ON kafka.messages(created_at)
WHERE txn_state = 'aborted';

-- Records by producer timestamp, for ListOffsets by timestamp and MAX_TIMESTAMP (-3):
-- the record with the largest timestamp is one descent, and the earliest offset whose
-- timestamp is at or after T is found among the rows that match from either side, the
-- rows below the first match in timestamp order (through the offset key) or the rows at
-- or after T (index only, partition_offset is a key column), whichever the planner
-- estimates smaller; without it either lookup read every row of the partition.
-- Partial on what a lookup can match (rows with a timestamp, not aborted). It costs
-- every Produce one more btree entry per record (about 40 bytes, and its WAL) and a
-- transaction commit one more per record (its UPDATE of txn_state is not HOT anyway:
-- txn_state is in the partial indexes' predicates). Partitioned index: every leaf gets one.
CREATE INDEX idx_messages_timestamp
ON kafka.messages(topic_id, partition_id, timestamp_ms, partition_offset)
WHERE timestamp_ms >= 0 AND txn_state IS DISTINCT FROM 'aborted';

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
    -- A leaf dropped since the caller listed it (a concurrent sweep): no contents
    -- (all NULL), not an error, so the caller's pass goes on.
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = p_partition) THEN
        RETURN;
    END IF;
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

-- Take the partition-maintenance advisory lock (key 8099547369649367905, 'pg_kafka')
-- for this transaction, polling for it until the caller's lock_timeout has passed:
-- returns whether it is held (internal). Polling instead of queueing for it, so a
-- worker transaction (which holds the schema lock in SHARE mode) never sits in a lock
-- wait for it while its holder waits for the schema lock: no lock cycle through it.
-- Already held by this transaction (a retention sweep takes it first): true at once.
CREATE FUNCTION kafka.try_maintenance_lock()
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_deadline TIMESTAMPTZ := clock_timestamp()
        + COALESCE(NULLIF(current_setting('lock_timeout'), '0')::INTERVAL, INTERVAL '2s');
BEGIN
    LOOP
        IF pg_try_advisory_xact_lock(8099547369649367905) THEN
            RETURN true;
        END IF;
        IF clock_timestamp() >= v_deadline THEN
            RETURN false;
        END IF;
        PERFORM pg_sleep(0.01 + random() * 0.04);
    END LOOP;
END;
$$;

-- Lock kafka.topics in SHARE ROW EXCLUSIVE mode and then kafka.messages in ACCESS
-- EXCLUSIVE mode, what partition DDL needs (internal). Worker transactions lock these
-- tables in either order (a batch can fetch, then auto-create a topic), so a DDL that
-- waited for one while holding the other could deadlock with them. Each attempt
-- therefore waits at most 50 ms (well below deadlock_timeout), in a subtransaction
-- whose failure releases both locks, and the attempts repeat with a random pause
-- until the caller's lock_timeout has passed; then lock_not_available is raised.
CREATE FUNCTION kafka.lock_message_tables()
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_saved TEXT := current_setting('lock_timeout');
    v_deadline TIMESTAMPTZ := clock_timestamp()
        + COALESCE(NULLIF(current_setting('lock_timeout'), '0')::INTERVAL, INTERVAL '2s');
BEGIN
    LOOP
        BEGIN
            PERFORM set_config('lock_timeout', '50ms', true);
            LOCK TABLE kafka.topics IN SHARE ROW EXCLUSIVE MODE;
            LOCK TABLE kafka.messages IN ACCESS EXCLUSIVE MODE;
            PERFORM set_config('lock_timeout', v_saved, true);
            RETURN;
        EXCEPTION WHEN lock_not_available THEN
            -- The subtransaction is rolled back: neither lock is held, and lock_timeout
            -- is back to v_saved.
            IF clock_timestamp() >= v_deadline THEN
                RAISE;
            END IF;
        END;
        PERFORM pg_sleep(0.01 + random() * 0.04);
    END LOOP;
END;
$$;

-- Create the leaf [p_lower, p_upper) (internal; called by ensure_message_partitions).
-- Rows already in the DEFAULT leaf for that range are moved into the new leaf first
-- (Postgres refuses CREATE ... PARTITION OF while the default holds such rows). The
-- leaf gets the per-leaf unique indexes and is made a member of the extension, like
-- the tables CREATE EXTENSION made: pg_dump then leaves it out exactly as it leaves
-- out kafka.messages itself, DROP EXTENSION drops it, and no dump can restore a leaf
-- without its parent. Returns 1 if created, 0 if a lock could not be taken within
-- lock_timeout, a move found the schema lock taken, or the DEFAULT leaf holds rows of
-- the range the caller's snapshot does not see (the next maintenance pass retries).
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
    v_move BOOLEAN;
BEGIN
    IF p_lower IS NULL OR p_upper IS NULL OR p_lower >= p_upper THEN
        RAISE EXCEPTION 'invalid partition range [%, %)', p_lower, p_upper;
    END IF;
    BEGIN
        v_move := EXISTS (SELECT 1 FROM kafka.messages_default
                          WHERE created_at >= p_lower AND created_at < p_upper);
        -- Moving rows (DELETE from the DEFAULT leaf + INSERT into the new leaf) must not
        -- overlap any worker transaction: an UPDATE of the worker (EndTxn's txn_state
        -- change, DeleteRecords) that met a moved row would skip it and never see its
        -- copy. Every worker transaction holds the pg_kafka schema lock in SHARE mode
        -- from its start, so a move needs it EXCLUSIVE. ensure_message_partitions
        -- queues for it before its first leaf, while it holds no table lock; here it is
        -- only tried, never waited for: an earlier leaf of the same pass holds
        -- kafka.messages ACCESS EXCLUSIVE to commit, and a worker transaction waiting
        -- for that table while holding the schema lock would close a lock cycle.
        IF v_move AND NOT pg_try_advisory_xact_lock(8099547369649367906) THEN
            RAISE WARNING 'pg_kafka: could not create time partition kafka.% (rows of kafka.messages_default to move, schema lock taken); will retry', v_name;
            RETURN 0;
        END IF;
        -- CREATE ... PARTITION OF / ATTACH PARTITION lock kafka.messages and, to clone
        -- the topic_id foreign key, kafka.topics: take both first, without ever
        -- waiting long enough to deadlock with a worker transaction.
        PERFORM kafka.lock_message_tables();
        -- Rows can reach the DEFAULT leaf while lock_message_tables waits, and CREATE
        -- ... PARTITION OF fails (check_violation) on any of the new leaf's range. With
        -- kafka.messages held ACCESS EXCLUSIVE none can arrive now: check again and
        -- move them. The check reads the transaction's snapshot, so under REPEATABLE
        -- READ or SERIALIZABLE (a manual pass) it misses rows committed during the
        -- wait; the check_violation handler below skips the leaf then.
        IF NOT v_move THEN
            v_move := EXISTS (SELECT 1 FROM kafka.messages_default
                              WHERE created_at >= p_lower AND created_at < p_upper);
            IF v_move AND NOT pg_try_advisory_xact_lock(8099547369649367906) THEN
                RAISE WARNING 'pg_kafka: could not create time partition kafka.% (rows of kafka.messages_default to move, schema lock taken); will retry', v_name;
                RETURN 0;
            END IF;
        END IF;
        IF v_move THEN
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
    EXCEPTION
        WHEN lock_not_available THEN
            RAISE WARNING 'pg_kafka: could not create time partition kafka.% (lock timeout); will retry', v_name;
            RETURN 0;
        -- The DEFAULT leaf holds rows of the range this transaction's snapshot does not
        -- see (committed after a REPEATABLE READ or SERIALIZABLE snapshot was taken):
        -- the leaf is skipped and a later pass, with a newer snapshot, moves them.
        WHEN check_violation THEN
            RAISE WARNING 'pg_kafka: could not create time partition kafka.% (kafka.messages_default holds rows of its range this snapshot does not see); will retry', v_name;
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
SET lock_timeout = '2s'
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
    -- Serializes concurrent maintenance (worker and manual calls, retention sweeps
    -- included). Key: 'pg_kafka'. Bounded: a session that ran maintenance and stayed
    -- open must not stall the next pass (the worker's included) indefinitely; this pass
    -- is skipped instead.
    IF NOT kafka.try_maintenance_lock() THEN
        RAISE WARNING 'pg_kafka: partition maintenance skipped: another session holds the maintenance lock; will retry';
        RETURN 0;
    END IF;
    -- kafka.messages before any of its leaves, the order every statement through the
    -- parent takes them in: the reads of kafka.messages_default below lock only that
    -- leaf, and a pass holding it without the parent then waits (in lock_message_tables)
    -- for kafka.messages behind a session that already holds it ACCESS EXCLUSIVE and
    -- waits for the leaf (LOCK TABLE kafka.messages, ALTER TABLE, partition DDL): a
    -- lock cycle, which deadlock detection breaks by failing that session, or which
    -- holds it until this pass's lock_timeout gives up the leaf. ACCESS SHARE on the
    -- parent alone (ONLY) conflicts with no worker statement.
    BEGIN
        LOCK TABLE ONLY kafka.messages IN ACCESS SHARE MODE;
    EXCEPTION WHEN lock_not_available THEN
        RAISE WARNING 'pg_kafka: partition maintenance skipped: kafka.messages is locked (lock timeout); will retry';
        RETURN 0;
    END;
    -- Rows in the DEFAULT leaf are moved by some leaf of this pass, and a move needs the
    -- schema lock EXCLUSIVE (see create_message_partition). Queue for it here, bounded
    -- by lock_timeout, before any leaf locks kafka.messages: this transaction holds no
    -- lock a worker transaction waits for, so the wait cannot close a cycle. On a
    -- timeout the leaves that move rows are left to the next pass; the others are made.
    IF EXISTS (SELECT 1 FROM kafka.messages_default) THEN
        BEGIN
            PERFORM pg_advisory_xact_lock(8099547369649367906);
        EXCEPTION WHEN lock_not_available THEN
            RAISE WARNING 'pg_kafka: rows of kafka.messages_default not moved (schema lock wait timed out); will retry';
        END;
    END IF;
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
-- Returns whether it dropped the leaf, the (topic_id, partition_id) pairs the leaf
-- held and, for each, the offset after the last one it held (MAX + 1, one btree descent
-- per pair), so the caller advances their log_start_offset and keeps their offset
-- counter past the dropped offsets even where no counter row existed. lock_timed_out
-- says the leaf was kept because a lock wait ran out (the maintenance lock, or the
-- locks the DROP needs, within the 2 s lock_timeout): a caller dropping several leaves
-- stops there, since the session holding the lock blocks the next drop just as long.
-- Only under READ COMMITTED: the re-check reads the snapshot, and a REPEATABLE READ or
-- SERIALIZABLE one predates the lock wait, so rows committed during the wait would be
-- invisible to it and dropped with the leaf. Under those levels the leaf is kept with a
-- WARNING (dropped and lock_timed_out false) and a READ COMMITTED pass drops it.
-- Dropping a leaf is equivalent to deleting its rows, which the worker may already do;
-- the function grants no more.
CREATE FUNCTION kafka.drop_message_partition(
    p_partition REGCLASS,
    p_expired_topics INT[],
    OUT dropped BOOLEAN,
    OUT topic_ids INT[],
    OUT partition_ids INT[],
    OUT next_offsets BIGINT[],
    OUT lock_timed_out BOOLEAN
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '2s'
AS $$
DECLARE
    v_contents RECORD;
BEGIN
    dropped := false;
    lock_timed_out := false;
    -- Already gone (a concurrent sweep dropped it): nothing to do, not an error.
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = p_partition) THEN
        RETURN;
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE WARNING 'pg_kafka: did not drop time partition % (a % snapshot cannot see rows committed while the drop waits for its locks); a READ COMMITTED sweep drops it',
            p_partition, upper(current_setting('transaction_isolation'));
        RETURN;
    END IF;
    -- The maintenance lock first, before this call locks any table (the leaf check
    -- below opens every leaf). A retention sweep already holds it.
    IF NOT kafka.try_maintenance_lock() THEN
        RAISE WARNING 'pg_kafka: could not drop time partition % (another session holds the maintenance lock); will retry', p_partition;
        lock_timed_out := true;
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM kafka.message_partitions() p
                   WHERE p.partition_oid = p_partition AND NOT p.is_default) THEN
        -- message_partitions() opens each leaf, so this check can wait behind a
        -- concurrent drop of p_partition and then miss it: gone is not an error.
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = p_partition) THEN
            RETURN;
        END IF;
        RAISE EXCEPTION '% is not a time partition of kafka.messages', p_partition;
    END IF;
    BEGIN
        -- Every lock wait is inside this block, so a lock timeout skips this leaf
        -- (the next sweep retries) instead of failing the caller's whole sweep.
        -- DROP takes ACCESS EXCLUSIVE on the parent and, to drop the leaf's foreign
        -- key, SHARE ROW EXCLUSIVE on kafka.topics; take both first (parent, then leaf,
        -- the order inserts use) so the contents check below cannot race a writer.
        PERFORM kafka.lock_message_tables();
        -- Re-check under the locks: a concurrent drop of the same leaf may have
        -- committed while this call waited for them. Each statement here takes a new
        -- snapshot (READ COMMITTED, checked above), so the contents check sees every
        -- row committed during the wait.
        IF NOT EXISTS (SELECT 1 FROM pg_inherits
                       WHERE inhrelid = p_partition AND inhparent = 'kafka.messages'::regclass) THEN
            RETURN;
        END IF;
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
        EXECUTE format(
            'SELECT COALESCE(array_agg((SELECT MAX(l.partition_offset) + 1 FROM %s l
                                        WHERE l.topic_id = u.t AND l.partition_id = u.p)
                                       ORDER BY u.n), ''{}'')
             FROM unnest($1, $2) WITH ORDINALITY AS u(t, p, n)', p_partition)
        INTO next_offsets USING topic_ids, partition_ids;
        EXECUTE format('DROP TABLE %s', p_partition);
        dropped := true;
    EXCEPTION WHEN lock_not_available THEN
        RAISE WARNING 'pg_kafka: could not drop time partition % (lock timeout); will retry', p_partition;
        dropped := false;
        lock_timed_out := true;
        topic_ids := NULL;
        partition_ids := NULL;
        next_offsets := NULL;
    END;
END;
$$;

-- Functions are executable by PUBLIC by default: these are for the worker role only.
REVOKE EXECUTE ON FUNCTION kafka.message_partition_contents(REGCLASS) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.try_maintenance_lock() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.lock_message_tables() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.create_message_partition(TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.ensure_message_partitions(INT, INT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION kafka.drop_message_partition(REGCLASS, INT[]) FROM PUBLIC;

-- The sequence now belongs to the new parent, so dropping the legacy leaf keeps it.
ALTER SEQUENCE kafka.messages_global_offset_seq OWNED BY kafka.messages.global_offset;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
               WHERE conrelid = 'kafka.messages_legacy'::regclass AND conname = 'messages_legacy_partition_id_check') THEN
        ALTER TABLE kafka.messages_legacy RENAME CONSTRAINT messages_legacy_partition_id_check TO messages_partition_id_check;
    ELSE
        -- An install that predates DR-7: ATTACH needs the parent's CHECK on the leaf.
        ALTER TABLE kafka.messages_legacy ADD CONSTRAINT messages_partition_id_check CHECK (partition_id >= 0);
    END IF;
END
$$;
-- 0.0.0 installs from before BUG-7 (no kafka.messages.timestamp_ms) and RV-10 (naive
-- TIMESTAMP columns compared with NOW()): ATTACH needs the leaf's columns to be the
-- parent's, and the other two columns get the type a fresh install has. Converting
-- created_at rewrites the legacy table (one more full read and write, and the rebuild of
-- its three indexes, a full read each); the naive
-- values are taken in the session's TimeZone, the zone NOW() was stored in.
ALTER TABLE kafka.messages_legacy ADD COLUMN IF NOT EXISTS timestamp_ms BIGINT NOT NULL DEFAULT -1;
DO $$
DECLARE
    v_col RECORD;
BEGIN
    FOR v_col IN
        SELECT c.relname, a.attname FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'kafka' AND NOT a.attisdropped
          AND a.atttypid = 'timestamp'::regtype
          AND (c.relname, a.attname) IN (('messages_legacy', 'created_at'),
                                         ('transactions', 'started_at'),
                                         ('shadow_tracking', 'forwarded_at'))
    LOOP
        EXECUTE format('ALTER TABLE kafka.%I ALTER COLUMN %I TYPE TIMESTAMPTZ',
                       v_col.relname, v_col.attname);
    END LOOP;
END
$$;
-- Fetches sum the record_bytes of the rows they scan (bootstrap.sql): the 0.0.0 rows get
-- theirs here, computed as fetches compute it for a row without one (record_bytes_sql! in
-- src/kafka/storage/postgres/log.rs). ALTER COLUMN TYPE ... USING rewrites the legacy
-- table (one more full read and write, the headers of every row detoasted, and the
-- rebuild of its three indexes, a full read each); its USING
-- expression cannot hold the subqueries itself, so a function computes it for the rewrite
-- only.
ALTER TABLE kafka.messages_legacy ADD COLUMN record_bytes INT;
CREATE FUNCTION kafka.upgrade_record_bytes(key BYTEA, value BYTEA, headers JSONB)
RETURNS INT
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT COALESCE(octet_length(key), 0) + COALESCE(octet_length(value), 0) + 84
            + CASE WHEN headers IS NULL OR headers = '{}'::jsonb THEN 0
                   WHEN jsonb_typeof(headers) = 'array' THEN
                     (SELECT COALESCE(SUM(COALESCE(octet_length(h ->> 0), 0)
                                          + COALESCE(octet_length(h ->> 1), 0) / 2 + 10), 0)
                      FROM jsonb_array_elements(headers) h)
                   ELSE
                     (SELECT COALESCE(SUM(octet_length(k) + COALESCE(octet_length(v), 0) / 2 + 10), 0)
                      FROM jsonb_each_text(headers) e(k, v))
              END
$$;
ALTER TABLE kafka.messages_legacy
    ALTER COLUMN record_bytes TYPE INT USING kafka.upgrade_record_bytes(key, value, headers);
DROP FUNCTION kafka.upgrade_record_bytes(BYTEA, BYTEA, JSONB);
DO $$
DECLARE
    v_upper TIMESTAMPTZ;
BEGIN
    -- Every existing row sorts below the bound, including any written with a future
    -- created_at; rows written after this transaction go to the new time leaves.
    SELECT GREATEST(now(), MAX(created_at)) + INTERVAL '1 microsecond'
    INTO v_upper FROM kafka.messages_legacy;
    -- The bound as ISO 8601 text in UTC with a numeric offset: the session's DateStyle
    -- would print a zone abbreviation (Europe/Dublin's IST), which is read back through
    -- timezone_abbreviations (as Israel's) and moves the bound.
    EXECUTE format('ALTER TABLE kafka.messages ATTACH PARTITION kafka.messages_legacy FOR VALUES FROM (MINVALUE) TO (%L)',
                   to_char(v_upper AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00');
END
$$;

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
COMMENT ON COLUMN kafka.messages.record_bytes IS 'Bytes the record adds to a Fetch response: uncompressed key, value and header lengths, 84 per record and 10 per header. NULL for rows written outside the extension (fetches compute it).';

-- 2. The views over kafka.messages, recreated on the partitioned parent (same
--    definitions as bootstrap.sql).
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
COMMENT ON VIEW kafka.shadow_status IS 'Unified view of shadow mode health, lag, and error counts.';

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
-- Producer metrics
SELECT
    'producers' AS category,
    'active' AS metric,
    COUNT(*)::BIGINT AS value,
    'total' AS unit
FROM kafka.producer_ids
WHERE last_active_at > NOW() - INTERVAL '5 minutes'
UNION ALL
-- Transaction metrics
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
-- Shadow mode metrics
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

COMMENT ON VIEW kafka.metrics IS 'Unified operational metrics view for monitoring pg_kafka health.
Categories: topics, messages, consumer_groups, consumer_offsets, producers, transactions, shadow.
messages.count_estimate and messages.stored_bytes are catalog estimates summed over the
time partitions (refreshed by (auto)VACUUM/ANALYZE), not exact scans; messages.time_partitions
counts the leaves of kafka.messages (DEFAULT included); shadow.lag is the pending-outbox depth.
Use with: SELECT * FROM kafka.metrics WHERE category = ''messages'';';

-- New SQL entry point of the Rust library (pgrx generates it for fresh installs).
CREATE FUNCTION pg_kafka_run_partition_maintenance(
	"partition_minutes" INT DEFAULT NULL
) RETURNS bigint
VOLATILE
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_kafka_run_partition_maintenance_wrapper';

-- The SQL entry points of the Rust library are for superusers only (as in
-- bootstrap.sql): both take the extension lock first and then wait for the maintenance
-- advisory lock, which any role can hold.
REVOKE EXECUTE ON FUNCTION pg_kafka_run_retention_sweep(INT, BIGINT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_kafka_run_partition_maintenance(INT) FROM PUBLIC;

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

-- Worker role (exactly storage::privileges::WORKER_TABLE_GRANTS / WORKER_SEQUENCE_GRANTS /
-- WORKER_FUNCTION_GRANTS).
GRANT USAGE ON SCHEMA kafka TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.topics TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.topics_id_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.messages TO pgkafka_worker;
GRANT USAGE ON SEQUENCE kafka.messages_global_offset_seq TO pgkafka_worker;
GRANT SELECT, DELETE ON kafka.messages_default TO pgkafka_worker;
GRANT EXECUTE ON FUNCTION kafka.message_partition_contents(regclass) TO pgkafka_worker;
GRANT EXECUTE ON FUNCTION kafka.ensure_message_partitions(int, int) TO pgkafka_worker;
GRANT EXECUTE ON FUNCTION kafka.drop_message_partition(regclass, int[]) TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.partition_offsets TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.consumer_offsets TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_ids TO pgkafka_worker;
GRANT USAGE, UPDATE ON SEQUENCE kafka.producer_ids_producer_id_seq TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_sequences TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.transactions TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.txn_pending_offsets TO pgkafka_worker;
GRANT SELECT ON kafka.shadow_config TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.shadow_tracking TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.shadow_metrics TO pgkafka_worker;

-- =============================================================================
-- Broker request metrics
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
-- The worker flushes these two tables (storage::privileges::WORKER_TABLE_GRANTS).
GRANT SELECT, INSERT, UPDATE ON kafka.broker_api_stats TO pgkafka_worker;
GRANT SELECT, INSERT, UPDATE ON kafka.broker_gauges TO pgkafka_worker;

-- =============================================================================
-- SASL credentials
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

-- Kafka user management functions (in a fresh install pgrx generates these from
-- src/kafka/auth/users.rs; keep the signatures identical).
CREATE FUNCTION "pg_kafka_create_user"(
	"username" TEXT,
	"password" TEXT,
	"iterations" INT DEFAULT 4096
) RETURNS void
STRICT VOLATILE
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_kafka_create_user_wrapper';

CREATE FUNCTION "pg_kafka_alter_user"(
	"username" TEXT,
	"password" TEXT,
	"iterations" INT DEFAULT 4096
) RETURNS void
STRICT VOLATILE
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_kafka_alter_user_wrapper';

CREATE FUNCTION "pg_kafka_set_user_verifier"(
	"username" TEXT,
	"verifier" TEXT
) RETURNS void
STRICT VOLATILE
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_kafka_set_user_verifier_wrapper';

CREATE FUNCTION "pg_kafka_drop_user"(
	"username" TEXT
) RETURNS bool
STRICT VOLATILE
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_kafka_drop_user_wrapper';

-- =============================================================================
-- Bound kafka.topics.partitions (see bootstrap.sql)
-- =============================================================================
-- A 0.0.0 install may hold an out-of-range count written before the cap existed (such a topic
-- made every Metadata request crash the server). Clamp it into range, loudly, so the
-- constraint can be added; no partition at or above 100000 can hold data in practice.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT name, partitions FROM kafka.topics
             WHERE partitions < 1 OR partitions > 100000 LOOP
        RAISE WARNING 'pg_kafka: topic % had % partitions; clamping to %',
            r.name, r.partitions, LEAST(GREATEST(r.partitions, 1), 100000);
    END LOOP;
    UPDATE kafka.topics SET partitions = LEAST(GREATEST(partitions, 1), 100000)
    WHERE partitions < 1 OR partitions > 100000;
END
$$;
ALTER TABLE kafka.topics ADD CONSTRAINT topics_partitions_range
    CHECK (partitions >= 1 AND partitions <= 100000);

-- =============================================================================
-- Idempotent producer batch cache and transaction partition registry
-- =============================================================================
-- The last 5 batches (Kafka's max.in.flight bound for idempotence) of every producer
-- on every partition: their sequence range and base offset, so a retried batch is
-- answered with its original offset and anything else out of sequence is refused.
-- Rows belong to the current epoch only: every epoch change deletes the producer's
-- batches (its kafka.producer_sequences rows stay, at last_sequence -1: the new epoch
-- starts at 0 there), and they go with their producer_sequences row (ON DELETE CASCADE).
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

-- Rows by topic, for the foreign key on kafka.topics (see bootstrap.sql).
CREATE INDEX idx_txn_partitions_topic ON kafka.txn_partitions(topic_id);

-- Ongoing transactions by producer id: every non-transactional idempotent batch asks
-- whether its producer has an open transaction (open_txn_epoch_on_partition), and a plain
-- idempotent producer has none, so the lookup must not scan every open transaction.
CREATE INDEX idx_transactions_ongoing_producer
ON kafka.transactions(producer_id)
WHERE state = 'Ongoing';

-- Every transaction row by producer id, for the foreign key on producer_ids: the
-- retention sweep deletes idle producer ids, and each delete checks that no transaction
-- row references the id. Without an index on the referencing column that check reads
-- the whole table once per deleted id.
CREATE INDEX idx_transactions_producer
ON kafka.transactions(producer_id);

-- Terminal transactions oldest first: the retention sweep deletes the ones idle past
-- the transactional-id expiration through it, reading only those (see bootstrap.sql:
-- last_updated_at changes only with the state, so Ongoing updates stay HOT).
CREATE INDEX idx_transactions_terminal
ON kafka.transactions(last_updated_at)
WHERE state IN ('CompleteCommit', 'CompleteAbort', 'Empty');

-- Pending offsets by group: OffsetFetch with require_stable (KIP-447, the Java consumer
-- under read_committed) asks for the group's pending partitions, and the primary key leads
-- with the transactional id, so without this every group's pending rows would be scanned.
CREATE INDEX idx_txn_pending_offsets_group
ON kafka.txn_pending_offsets(group_id, topic_id, partition_id);

GRANT SELECT ON kafka.producer_batches TO pgkafka_reader;
GRANT SELECT ON kafka.txn_partitions TO pgkafka_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.producer_batches TO pgkafka_worker;
GRANT SELECT, INSERT, DELETE ON kafka.txn_partitions TO pgkafka_worker;

COMMENT ON TABLE kafka.producer_batches IS 'Last 5 batches per idempotent producer and partition (sequence range, base offset) for duplicate detection.';
COMMENT ON TABLE kafka.txn_partitions IS 'Partitions added (AddPartitionsToTxn) to the open transaction of each transactional id.';

-- Consumer groups added (AddOffsetsToTxn) to the open transaction of a transactional id.
-- A TxnOffsetCommit for any other group is refused with INVALID_TXN_STATE, as in Kafka.
-- Cleared with kafka.txn_partitions.
CREATE TABLE kafka.txn_groups (
    transactional_id TEXT NOT NULL
        REFERENCES kafka.transactions (transactional_id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    PRIMARY KEY (transactional_id, group_id)
);

GRANT SELECT ON kafka.txn_groups TO pgkafka_reader;
GRANT SELECT, INSERT, DELETE ON kafka.txn_groups TO pgkafka_worker;
COMMENT ON TABLE kafka.txn_groups IS 'Consumer groups added (AddOffsetsToTxn) to the open transaction of each transactional id.';

-- =============================================================================
-- Durable secret for the fake credentials of unknown SASL users
-- =============================================================================
-- A SASL exchange for a user that does not exist runs to the end against a fake
-- credential whose salt is HMAC(secret, mechanism:username), so probing a username
-- twice, even across worker restarts, shows the same salt as a real user would. The
-- secret is generated once, at CREATE EXTENSION (gen_random_uuid() draws from the
-- server's strong random source). Readable only by superusers and pgkafka_worker.
-- Not dumped by pg_dump: a restore into a new installation gets a new secret (fake
-- salts change once, nothing else does).
CREATE TABLE kafka.sasl_server_secret (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    secret BYTEA NOT NULL CHECK (octet_length(secret) >= 32)
);
INSERT INTO kafka.sasl_server_secret (secret)
SELECT decode(
    replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', '')
        || replace(gen_random_uuid()::text, '-', ''),
    'hex');
GRANT SELECT ON kafka.sasl_server_secret TO pgkafka_worker;
COMMENT ON TABLE kafka.sasl_server_secret IS 'Secret keying the fake SCRAM salts of unknown SASL users; never shown to clients.';

-- =============================================================================
-- Shadow outbox attempts and per-partition lag
-- =============================================================================
-- attempt_open: a claim opens an attempt; its ack closes it. retry_count counts
-- failed delivery attempts only (an ack of a failure, or an attempt still open when
-- its worker no longer tracks it, i.e. a lost ack), not claims.
ALTER TABLE kafka.shadow_tracking
    ADD COLUMN attempt_open BOOLEAN NOT NULL DEFAULT false;
GRANT SELECT (attempt_open) ON kafka.shadow_tracking TO pgkafka_reader;
-- dead: retry_count >= pg_kafka.shadow_max_retries on pending rows, kept by the
-- worker (failure acks, lost attempts, replay, and a full sync at worker start, on a
-- schema found current again and when the cap changes, so the first claim after the
-- upgrade marks the existing dead letters). It keys both pending indexes, so a claim
-- skips the dead letters by an index condition instead of reading them.
ALTER TABLE kafka.shadow_tracking
    ADD COLUMN dead BOOLEAN NOT NULL DEFAULT false;
DROP INDEX kafka.idx_shadow_tracking_pending;
CREATE INDEX idx_shadow_tracking_pending
ON kafka.shadow_tracking(topic_id, partition_id, dead, local_offset)
WHERE external_offset IS NULL;
-- Failed (or dead), still pending rows: the outbox claim finds each partition's
-- first backing-off row here without scanning the whole backlog, and the rows
-- whose dead flag the retry cap changed by two index ranges per partition.
CREATE INDEX idx_shadow_tracking_retrying
ON kafka.shadow_tracking(topic_id, partition_id, dead, retry_count, local_offset)
WHERE external_offset IS NULL AND (retry_count > 0 OR dead);

-- Delivered rows, for the retention sweep's reaping of rows delivered more than a day
-- ago (walked per partition in offset order instead of scanning the outbox). Its key
-- columns never change and external_offset already keys the pending indexes, so the
-- outbox's claims stay HOT; an ack adds one entry.
CREATE INDEX idx_shadow_tracking_delivered
ON kafka.shadow_tracking(topic_id, partition_id, local_offset)
WHERE external_offset IS NOT NULL;
COMMENT ON COLUMN kafka.shadow_tracking.retry_count IS 'Failed delivery attempts. At pg_kafka.shadow_max_retries the row is a dead letter (pending, never retried) until replayed or the cap is raised.';
COMMENT ON COLUMN kafka.shadow_tracking.attempt_open IS 'True while a forward of the row is in flight (claimed, not yet acked).';
COMMENT ON COLUMN kafka.shadow_tracking.dead IS 'retry_count >= pg_kafka.shadow_max_retries on pending rows, kept by the worker (failure acks, lost attempts, replay, and a full sync at worker start and when the cap changes); indexed so claims skip dead letters.';

-- Replay also closes any open attempt and clears the dead flag.
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
      -- RA-6: only replay committed/non-transactional records.
      AND m.txn_state IS NULL
    ON CONFLICT (topic_id, partition_id, local_offset) DO UPDATE
        SET external_offset = NULL,
            forwarded_at    = NULL,
            error_message   = NULL,
            retry_count     = 0,
            attempt_open    = false,
            dead            = false;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;
REVOKE EXECUTE ON FUNCTION kafka.replay_shadow_messages(INT, BIGINT, BIGINT) FROM PUBLIC;

-- Per-partition forwarding state. lag = records of the partition still to be
-- forwarded (pending outbox rows under the retry cap): exact under sampling
-- (sampled-out records are never in the outbox) and 0 when caught up.
-- dead_lettered = pending rows at the cap (never retried until replayed).
CREATE VIEW kafka.shadow_partition_status AS
WITH cap AS (
    SELECT COALESCE(NULLIF(current_setting('pg_kafka.shadow_max_retries', true), '')::int, 10) AS n
),
parts AS (
    SELECT topic_id, partition_id FROM kafka.shadow_metrics
    UNION
    SELECT DISTINCT topic_id, partition_id FROM kafka.shadow_tracking
)
SELECT
    t.name AS topic_name,
    p.topic_id,
    p.partition_id,
    COALESCE(q.lag, 0)::BIGINT AS lag,
    COALESCE(q.dead_lettered, 0)::BIGINT AS dead_lettered,
    COALESCE(q.in_flight, 0)::BIGINT AS in_flight,
    q.oldest_pending_offset,
    sm.last_forwarded_offset,
    sm.last_forwarded_at,
    COALESCE(sm.messages_forwarded, 0)::BIGINT AS messages_forwarded,
    COALESCE(sm.messages_skipped, 0)::BIGINT AS messages_skipped,
    COALESCE(sm.messages_failed, 0)::BIGINT AS messages_failed
FROM parts p
JOIN kafka.topics t ON t.id = p.topic_id
LEFT JOIN kafka.shadow_metrics sm
       ON sm.topic_id = p.topic_id AND sm.partition_id = p.partition_id
LEFT JOIN LATERAL (
    SELECT COUNT(*) FILTER (WHERE st.retry_count < cap.n) AS lag,
           COUNT(*) FILTER (WHERE st.retry_count >= cap.n) AS dead_lettered,
           COUNT(*) FILTER (WHERE st.attempt_open) AS in_flight,
           MIN(st.local_offset) FILTER (WHERE st.retry_count < cap.n) AS oldest_pending_offset
    FROM kafka.shadow_tracking st, cap
    WHERE st.topic_id = p.topic_id AND st.partition_id = p.partition_id
      AND st.external_offset IS NULL
) q ON true;
GRANT SELECT ON kafka.shadow_partition_status TO pgkafka_reader;
COMMENT ON VIEW kafka.shadow_partition_status IS 'Per-partition shadow forwarding state: lag (records still to forward), dead letters, in-flight forwards, metrics.';

-- shadow_status: lag is the sum of the per-partition lags (the 0.0.0 view's
-- MAX(offset) - MAX(last_forwarded_offset) across partitions hid lagging partitions,
-- was off by one and never 0 under sampling); totals come from kafka.shadow_metrics
-- without a join fan-out. Trailing columns added: dead_lettered, max_partition_lag.
CREATE OR REPLACE VIEW kafka.shadow_status AS
SELECT
    t.name AS topic_name,
    COALESCE(sc.mode, 'local_only') AS mode,
    COALESCE(sc.forward_percentage, 0) AS forward_percentage,
    COALESCE(sc.sync_mode, 'async') AS sync_mode,
    sc.external_topic_name,
    COALESCE(ps.total_forwarded, 0)::BIGINT AS total_forwarded,
    COALESCE(ps.total_skipped, 0)::BIGINT AS total_skipped,
    COALESCE(ps.total_failed, 0)::BIGINT AS total_failed,
    ps.last_forwarded_offset,
    ps.last_forwarded_at::timestamp AS last_forwarded_at,
    COALESCE(ps.lag, 0)::BIGINT AS lag,
    COALESCE(ps.dead_lettered, 0)::BIGINT AS dead_lettered,
    COALESCE(ps.max_partition_lag, 0)::BIGINT AS max_partition_lag
FROM kafka.topics t
LEFT JOIN kafka.shadow_config sc ON t.id = sc.topic_id
LEFT JOIN (
    SELECT topic_id,
           SUM(messages_forwarded) AS total_forwarded,
           SUM(messages_skipped) AS total_skipped,
           SUM(messages_failed) AS total_failed,
           MAX(last_forwarded_offset) AS last_forwarded_offset,
           MAX(last_forwarded_at) AS last_forwarded_at,
           SUM(lag) AS lag,
           SUM(dead_lettered) AS dead_lettered,
           MAX(lag) AS max_partition_lag
    FROM kafka.shadow_partition_status
    GROUP BY topic_id
) ps ON ps.topic_id = t.id;
