// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! The database side of shadow mode, behind a small trait.
//!
//! `ShadowStore` keeps the outbox *logic* (what to enqueue, which claimed rows
//! to dispatch, how an ack finalizes a row, the sync wait, the config-cache
//! swap). Every SQL statement and GUC read it needs goes through [`ShadowDb`]:
//!
//! - [`SpiShadowDb`] is the production implementation. Its statements are the
//!   ones `ShadowStore` used to run inline, unchanged and in the same order.
//! - `InMemoryShadowDb` (test builds only) models the `kafka.shadow_tracking`
//!   outbox, the `kafka.messages` rows it points at, and `kafka.shadow_metrics`
//!   in memory, so the claim/ack/retry/dead-letter behaviour is unit-tested.

use super::config::TopicShadowConfig;
use super::{ForwardRecord, RowKey};
use crate::kafka::error::Result;

/// A transactional record's outbox pointer: the row key plus the record key
/// (used only for percentage routing at enqueue time). The payload is re-read
/// from `kafka.messages` by the forwarder.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingForwardRecord {
    /// Topic ID (outbox row key + per-topic config lookup)
    pub topic_id: i32,
    /// Partition ID (outbox row key)
    pub partition_id: i32,
    /// Local offset the record was committed at (outbox row key)
    pub offset: i64,
    /// Record key — used only for percentage routing at enqueue time
    pub key: Option<Vec<u8>>,
}

/// The shadow GUCs the outbox reads on the DB thread (Sighup: read per use, so
/// a reload takes effect at once and a session-level SET cannot fake one).
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxSettings {
    /// `pg_kafka.shadow_max_retries`: failed delivery attempts after which a row
    /// is a dead letter.
    pub max_retries: i64,
    /// `pg_kafka.shadow_retry_backoff_ms`: base of the exponential re-attempt
    /// backoff after a failed attempt.
    pub retry_backoff_ms: i64,
    /// `pg_kafka.shadow_batch_size`: rows claimed per outbox poll.
    pub batch_size: i64,
    /// `pg_kafka.shadow_metrics_enabled`: write `kafka.shadow_metrics`.
    pub metrics_enabled: bool,
    /// `pg_kafka.shadow_sync_timeout_ms`: how long a sync-mode produce response is
    /// held for the external ack (0 = not held).
    pub sync_timeout_ms: u64,
}

impl Default for OutboxSettings {
    fn default() -> Self {
        use crate::kafka::constants::*;
        Self {
            max_retries: DEFAULT_SHADOW_MAX_RETRIES as i64,
            retry_backoff_ms: DEFAULT_SHADOW_RETRY_BACKOFF_MS as i64,
            batch_size: DEFAULT_SHADOW_BATCH_SIZE as i64,
            metrics_enabled: DEFAULT_SHADOW_METRICS_ENABLED,
            sync_timeout_ms: DEFAULT_SHADOW_SYNC_TIMEOUT_MS as u64,
        }
    }
}

/// Upper bound of the outbox re-attempt backoff.
pub const OUTBOX_MAX_BACKOFF_MS: i64 = 30_000;

/// Backoff before re-attempting a row that failed `retry_count` times:
/// `base * 2^(retry_count-1)`, capped at `max`. Zero for a row that never failed.
pub fn retry_backoff_ms(retry_count: i64, base: i64, max: i64) -> i64 {
    if retry_count <= 0 {
        return 0;
    }
    let shift = (retry_count - 1).min(30) as u32;
    base.saturating_mul(1i64 << shift).min(max)
}

/// Parameters of one outbox claim (see [`ShadowDb::claim_due_outbox_rows`]).
#[derive(Clone, Debug, Default)]
pub struct OutboxClaim {
    /// Max rows claimed per call.
    pub batch_limit: i64,
    /// Rows with `retry_count >= max_retries` are dead letters and never claimed.
    pub max_retries: i64,
    /// Exponential re-attempt backoff (see [`retry_backoff_ms`]).
    pub backoff_base_ms: i64,
    pub backoff_max_ms: i64,
    /// In-flight row keys (topic ids, partition ids, offsets; parallel arrays),
    /// excluded from the claim.
    pub inflight_topics: Vec<i32>,
    pub inflight_partitions: Vec<i32>,
    pub inflight_offsets: Vec<i64>,
}

/// What one batch of failure acks did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FailedApplied {
    /// Rows that recorded the failure.
    pub transitioned: usize,
    /// Rows that reached the dead-letter cap with this failure, with the error.
    pub dead_lettered: Vec<(RowKey, String)>,
}

/// Every database/GUC access shadow mode makes.
pub trait ShadowDb {
    /// `pg_kafka.shadow_mode_enabled`.
    fn shadow_mode_enabled(&self) -> bool;

    /// `pg_kafka.shadow_external_primary_strict`.
    fn external_primary_strict(&self) -> bool;

    /// The outbox GUCs, read now.
    fn settings(&self) -> OutboxSettings;

    /// Whether the topic has a dead-lettered forward (`external_offset` NULL,
    /// `retry_count >= max_retries`).
    fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool>;

    /// Insert pending outbox rows `(topic_id, partition_id, local_offset)` in the
    /// current transaction, each only if its `kafka.messages` row exists;
    /// existing rows are left alone.
    fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()>;

    /// Count `n` records of a partition that percentage routing sampled out
    /// (`kafka.shadow_metrics.messages_skipped`), in the current transaction.
    fn record_skipped(&self, topic_id: i32, partition_id: i32, n: i64) -> Result<()>;

    /// The pending (uncommitted) records of a producer's transaction, read from
    /// `kafka.messages`, ordered by row key. `None` if the lookup failed.
    fn pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Option<Vec<PendingForwardRecord>>;

    /// Claim due outbox rows and return a forward record (payload, headers,
    /// timestamp, external topic name) for each, ordered by row key.
    ///
    /// The candidates of a partition are its pending rows under the retry cap
    /// whose message still exists. A candidate is *backing off* when it failed
    /// (`retry_count > 0`), has no open attempt, and its last attempt is more
    /// recent than [`retry_backoff_ms`]. A candidate is claimed when it is not in
    /// flight, not backing off, and no backing-off candidate of its partition has
    /// a lower offset (so a retry is never overtaken by a later record). A row
    /// whose attempt is still open in the table but not in flight in this worker
    /// lost its ack (worker restart, eviction): it is claimed and that lost
    /// attempt counts as one failure. Claiming opens an attempt and stamps
    /// `forwarded_at`. `restart` is set on the lowest candidate of each partition.
    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRecord>>;

    /// Mark pending rows delivered at their external offsets (and count them in
    /// `kafka.shadow_metrics` when `metrics`). Returns how many transitioned (a
    /// duplicate ack or an unknown row does not).
    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<usize>;

    /// Record failed attempts: `retry_count + 1`, the error, attempt closed,
    /// `forwarded_at` = now (start of the backoff); counted in
    /// `kafka.shadow_metrics.messages_failed` when `metrics`.
    fn apply_failed(
        &self,
        rows: &[(RowKey, String)],
        max_retries: i64,
        metrics: bool,
    ) -> Result<FailedApplied>;

    /// Close attempts that never reached the broker (no retry counted).
    fn apply_not_attempted(&self, rows: &[RowKey]) -> Result<()>;

    /// Every row of `kafka.shadow_config`, parsed. `None` if the query failed.
    fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>>;

    /// The xid of the current (sub)transaction, or 0 if it has none. Taken right
    /// after a sync-mode produce writes its outbox rows, so it identifies the
    /// innermost subtransaction those writes belong to.
    fn current_transaction_id(&self) -> u32;

    /// Whether `xid` committed — for a subtransaction xid, whether it and every
    /// enclosing transaction committed. False for 0, an aborted xid, or one
    /// still in progress. Called after the worker's commit.
    fn transaction_committed(&self, xid: u32) -> bool;
}

/// Production [`ShadowDb`]: SPI and the real GUCs.
#[derive(Debug, Default, Clone, Copy)]
pub struct SpiShadowDb;

/// Split row keys into the parallel arrays the SQL unnests.
fn key_arrays<'a>(keys: impl Iterator<Item = &'a RowKey>) -> (Vec<i32>, Vec<i32>, Vec<i64>) {
    let mut t = Vec::new();
    let mut p = Vec::new();
    let mut o = Vec::new();
    for k in keys {
        t.push(k.0);
        p.push(k.1);
        o.push(k.2);
    }
    (t, p, o)
}

impl ShadowDb for SpiShadowDb {
    fn shadow_mode_enabled(&self) -> bool {
        crate::config::SHADOW_MODE_ENABLED.get()
    }

    fn external_primary_strict(&self) -> bool {
        crate::config::SHADOW_EXTERNAL_PRIMARY_STRICT.get()
    }

    fn settings(&self) -> OutboxSettings {
        crate::config::shadow_outbox_settings()
    }

    fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool> {
        use pgrx::prelude::*;
        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.shadow_tracking \
                 WHERE topic_id = $1 AND external_offset IS NULL AND retry_count >= $2 \
                 LIMIT 1",
                Some(1),
                &[topic_id.into(), max_retries.into()],
            )?;
            Ok(!table.is_empty())
        })
    }

    fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(());
        }
        let (t, p, o) = key_arrays(rows.iter());
        Spi::connect_mut(|client| {
            // B1: only enqueue an outbox pointer when the backing
            // kafka.messages row actually exists. The live-produce enqueue
            // runs in the same subtransaction as the message insert, so the
            // row is always visible and this is a no-op there. On the txn
            // commit path it drops orphan pointers left by a transactional
            // produce that buffered offsets in RAM but whose message rows were
            // rolled back (a panic mid-txn) — which would otherwise be
            // never-claimed, never-cleaned rows. One statement per batch; the
            // LATERAL + LIMIT 1 keeps the check an index probe per row (an EXISTS
            // was planned as a hash join over every message).
            client.update(
                "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset) \
                 SELECT r.t, r.p, r.o \
                 FROM unnest($1::int4[], $2::int4[], $3::int8[]) AS r(t, p, o) \
                 CROSS JOIN LATERAL ( \
                 SELECT 1 FROM kafka.messages m \
                 WHERE m.topic_id = r.t AND m.partition_id = r.p \
                 AND m.partition_offset = r.o \
                 LIMIT 1 \
                 ) has_message \
                 ON CONFLICT (topic_id, partition_id, local_offset) DO NOTHING",
                None,
                &[t.into(), p.into(), o.into()],
            )?;
            Ok(())
        })
    }

    fn record_skipped(&self, topic_id: i32, partition_id: i32, n: i64) -> Result<()> {
        use pgrx::prelude::*;
        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.shadow_metrics (topic_id, partition_id, messages_skipped) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                    messages_skipped = kafka.shadow_metrics.messages_skipped + EXCLUDED.messages_skipped",
                None,
                &[topic_id.into(), partition_id.into(), n.into()],
            )?;
            Ok(())
        })
    }

    fn pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Option<Vec<PendingForwardRecord>> {
        use pgrx::prelude::*;
        let result: std::result::Result<Vec<PendingForwardRecord>, pgrx::spi::SpiError> =
            Spi::connect(|client| {
                let table = client.select(
                    "SELECT topic_id, partition_id, partition_offset, key
                     FROM kafka.messages
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'
                     ORDER BY topic_id, partition_id, partition_offset",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
                let mut records = Vec::new();
                for row in table {
                    records.push(PendingForwardRecord {
                        topic_id: row.get_by_name("topic_id")?.unwrap_or(0),
                        partition_id: row.get_by_name("partition_id")?.unwrap_or(0),
                        offset: row.get_by_name("partition_offset")?.unwrap_or(0),
                        key: row.get_by_name::<Vec<u8>, _>("key")?,
                    });
                }
                Ok(records)
            });
        match result {
            Ok(records) => Some(records),
            Err(e) => {
                tracing::warn!("Shadow: failed to read pending txn records: {:?}", e);
                None
            }
        }
    }

    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRecord>> {
        use pgrx::prelude::*;
        Spi::connect_mut(|client| {
            let table = client.update(
                r#"
                WITH inf AS (
                    SELECT * FROM unnest($3::int4[], $4::int4[], $5::int8[]) AS i(t, p, o)
                ),
                -- The first backing-off row of each partition: it failed, its
                -- attempt is closed and its backoff has not passed. Only failed
                -- rows are looked at (idx_shadow_tracking_retrying), so this does
                -- not grow with the backlog.
                blocked AS MATERIALIZED (
                    SELECT st.topic_id, st.partition_id, MIN(st.local_offset) AS first_blocked
                    FROM kafka.shadow_tracking st
                    WHERE st.external_offset IS NULL
                      AND st.retry_count > 0
                      AND st.retry_count < $2
                      AND NOT st.attempt_open
                      AND st.forwarded_at > NOW() - make_interval(secs =>
                          LEAST($6::float8 * power(2::float8, LEAST(st.retry_count - 1, 30)),
                                $7::float8) / 1000.0)
                      AND NOT EXISTS (
                          SELECT 1 FROM inf
                          WHERE inf.t = st.topic_id AND inf.p = st.partition_id
                            AND inf.o = st.local_offset)
                    GROUP BY st.topic_id, st.partition_id
                ),
                -- Due rows in row-key order, read from the pending index and cut
                -- at the limit: the cost is O(limit), not O(backlog).
                due AS (
                    SELECT st.topic_id, st.partition_id, st.local_offset
                    FROM kafka.shadow_tracking st
                    LEFT JOIN blocked b
                      ON b.topic_id = st.topic_id AND b.partition_id = st.partition_id
                    CROSS JOIN LATERAL (
                        SELECT 1 FROM kafka.messages m
                        WHERE m.topic_id = st.topic_id
                          AND m.partition_id = st.partition_id
                          AND m.partition_offset = st.local_offset
                        LIMIT 1
                    ) has_message
                    WHERE st.external_offset IS NULL
                      AND st.retry_count < $2
                      AND (b.first_blocked IS NULL OR st.local_offset < b.first_blocked)
                      AND NOT (st.retry_count > 0 AND NOT st.attempt_open
                               AND st.forwarded_at > NOW() - make_interval(secs =>
                                   LEAST($6::float8 * power(2::float8, LEAST(st.retry_count - 1, 30)),
                                         $7::float8) / 1000.0))
                      AND NOT EXISTS (
                          SELECT 1 FROM inf
                          WHERE inf.t = st.topic_id AND inf.p = st.partition_id
                            AND inf.o = st.local_offset)
                    ORDER BY st.topic_id, st.partition_id, st.local_offset
                    LIMIT $1
                ),
                claimed AS (
                    -- An attempt that is still open here lost its ack (the worker
                    -- restarted, or the in-flight entry was evicted): count it as
                    -- one failure so a forward that never answers still reaches
                    -- the dead-letter cap (RV-10). A fresh claim counts nothing;
                    -- real failures are counted by their ack (wave 3).
                    UPDATE kafka.shadow_tracking st
                    SET forwarded_at = NOW(),
                        attempt_open = true,
                        retry_count = st.retry_count + CASE WHEN st.attempt_open THEN 1 ELSE 0 END
                    FROM due
                    WHERE st.topic_id = due.topic_id
                      AND st.partition_id = due.partition_id
                      AND st.local_offset = due.local_offset
                    RETURNING st.topic_id, st.partition_id, st.local_offset,
                              -- restart: no lower pending, non-dead row of the
                              -- partition whose message exists (the partition head).
                              NOT EXISTS (
                                  SELECT 1 FROM kafka.shadow_tracking lo
                                  CROSS JOIN LATERAL (
                                      SELECT 1 FROM kafka.messages m
                                      WHERE m.topic_id = lo.topic_id
                                        AND m.partition_id = lo.partition_id
                                        AND m.partition_offset = lo.local_offset
                                      LIMIT 1
                                  ) lo_message
                                  WHERE lo.topic_id = st.topic_id
                                    AND lo.partition_id = st.partition_id
                                    AND lo.local_offset < st.local_offset
                                    AND lo.external_offset IS NULL
                                    AND lo.retry_count < $2
                              ) AS restart
                )
                SELECT c.topic_id    AS topic_id,
                       c.partition_id AS partition_id,
                       c.local_offset AS local_offset,
                       c.restart      AS restart,
                       m.key          AS msg_key,
                       m.value        AS msg_value,
                       m.headers::text AS headers_json,
                       CASE WHEN m.timestamp_ms >= 0 THEN m.timestamp_ms
                            ELSE (EXTRACT(EPOCH FROM m.created_at) * 1000)::bigint END AS ts,
                       COALESCE(sc.external_topic_name, t.name) AS external_topic
                FROM claimed c
                JOIN kafka.topics t ON t.id = c.topic_id
                LEFT JOIN kafka.shadow_config sc ON sc.topic_id = c.topic_id
                -- LATERAL + LIMIT 1: one index probe per claimed row. A plain join
                -- is planned from the CTE's 1-row estimate and matched every
                -- claimed row against all of the topic's messages (5 s for 1000
                -- rows of a 20 000-row backlog).
                JOIN LATERAL (
                    SELECT m.key, m.value, m.headers, m.timestamp_ms, m.created_at
                    FROM kafka.messages m
                    WHERE m.topic_id = c.topic_id
                      AND m.partition_id = c.partition_id
                      AND m.partition_offset = c.local_offset
                    LIMIT 1
                ) m ON true
                ORDER BY c.topic_id, c.partition_id, c.local_offset
                "#,
                None,
                &[
                    claim.batch_limit.into(),
                    claim.max_retries.into(),
                    claim.inflight_topics.clone().into(),
                    claim.inflight_partitions.clone().into(),
                    claim.inflight_offsets.clone().into(),
                    (claim.backoff_base_ms as f64).into(),
                    (claim.backoff_max_ms as f64).into(),
                ],
            )?;

            let mut records: Vec<ForwardRecord> = Vec::new();
            for row in table {
                let headers_json: Option<String> = row.get_by_name("headers_json")?;
                records.push(ForwardRecord {
                    topic_id: row.get_by_name("topic_id")?.unwrap_or(0),
                    topic_name: row.get_by_name("external_topic")?.unwrap_or_default(),
                    partition_id: row.get_by_name("partition_id")?.unwrap_or(0),
                    key: row.get_by_name("msg_key")?,
                    value: row.get_by_name("msg_value")?,
                    headers: crate::kafka::storage::decode_headers_json(headers_json.as_deref()),
                    timestamp_ms: row.get_by_name("ts")?,
                    local_offset: row.get_by_name("local_offset")?.unwrap_or(0),
                    restart: row.get_by_name("restart")?.unwrap_or(false),
                });
            }
            Ok(records)
        })
    }

    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<usize> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(0);
        }
        let (t, p, o) = key_arrays(rows.iter().map(|(k, _)| k));
        let x: Vec<i64> = rows.iter().map(|&(_, x)| x).collect();
        // SH-7: the metric is bumped in the SAME statement that flips the rows,
        // guarded by `external_offset IS NULL`, so a duplicate ack updates
        // nothing and counts nothing — exactly-once per row. One statement for
        // the whole batch of acks.
        Spi::connect_mut(|client| {
            let table = client.update(
                "WITH a AS ( \
                     SELECT * FROM unnest($1::int4[], $2::int4[], $3::int8[], $4::int8[]) AS a(t, p, o, x) \
                 ), \
                 done AS ( \
                     UPDATE kafka.shadow_tracking st \
                     SET external_offset = a.x, forwarded_at = NOW(), error_message = NULL, \
                         attempt_open = false \
                     FROM a \
                     WHERE st.topic_id = a.t AND st.partition_id = a.p AND st.local_offset = a.o \
                       AND st.external_offset IS NULL \
                     RETURNING st.topic_id, st.partition_id, st.local_offset \
                 ), \
                 bumped AS ( \
                     INSERT INTO kafka.shadow_metrics \
                         (topic_id, partition_id, messages_forwarded, last_forwarded_offset, last_forwarded_at) \
                     SELECT topic_id, partition_id, COUNT(*), MAX(local_offset), NOW() FROM done \
                     WHERE $5 \
                     GROUP BY topic_id, partition_id \
                     ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                        messages_forwarded = kafka.shadow_metrics.messages_forwarded + EXCLUDED.messages_forwarded, \
                        last_forwarded_offset = GREATEST(kafka.shadow_metrics.last_forwarded_offset, EXCLUDED.last_forwarded_offset), \
                        last_forwarded_at = NOW() \
                 ) \
                 SELECT COUNT(*)::int8 AS n FROM done",
                None,
                &[t.into(), p.into(), o.into(), x.into(), metrics.into()],
            )?;
            let mut n = 0i64;
            for row in table {
                n = row.get_by_name::<i64, _>("n")?.unwrap_or(0);
            }
            Ok(n as usize)
        })
    }

    fn apply_failed(
        &self,
        rows: &[(RowKey, String)],
        max_retries: i64,
        metrics: bool,
    ) -> Result<FailedApplied> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(FailedApplied::default());
        }
        let (t, p, o) = key_arrays(rows.iter().map(|(k, _)| k));
        let e: Vec<String> = rows.iter().map(|(_, e)| e.clone()).collect();
        Spi::connect_mut(|client| {
            let table = client.update(
                "WITH a AS ( \
                     SELECT * FROM unnest($1::int4[], $2::int4[], $3::int8[], $4::text[]) AS a(t, p, o, e) \
                 ), \
                 failed AS ( \
                     UPDATE kafka.shadow_tracking st \
                     SET retry_count = st.retry_count + 1, error_message = a.e, \
                         forwarded_at = NOW(), attempt_open = false \
                     FROM a \
                     WHERE st.topic_id = a.t AND st.partition_id = a.p AND st.local_offset = a.o \
                       AND st.external_offset IS NULL \
                     RETURNING st.topic_id, st.partition_id, st.local_offset, st.retry_count, \
                               st.error_message \
                 ), \
                 bumped AS ( \
                     INSERT INTO kafka.shadow_metrics (topic_id, partition_id, messages_failed) \
                     SELECT topic_id, partition_id, COUNT(*) FROM failed \
                     WHERE $5 \
                     GROUP BY topic_id, partition_id \
                     ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                        messages_failed = kafka.shadow_metrics.messages_failed + EXCLUDED.messages_failed \
                 ) \
                 SELECT topic_id, partition_id, local_offset, retry_count, error_message FROM failed",
                None,
                &[t.into(), p.into(), o.into(), e.into(), metrics.into()],
            )?;
            let mut out = FailedApplied::default();
            for row in table {
                out.transitioned += 1;
                let retry_count: i32 = row.get_by_name("retry_count")?.unwrap_or(0);
                if retry_count as i64 >= max_retries {
                    out.dead_lettered.push((
                        (
                            row.get_by_name("topic_id")?.unwrap_or(0),
                            row.get_by_name("partition_id")?.unwrap_or(0),
                            row.get_by_name("local_offset")?.unwrap_or(0),
                        ),
                        row.get_by_name("error_message")?.unwrap_or_default(),
                    ));
                }
            }
            Ok(out)
        })
    }

    fn apply_not_attempted(&self, rows: &[RowKey]) -> Result<()> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(());
        }
        let (t, p, o) = key_arrays(rows.iter());
        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.shadow_tracking st SET attempt_open = false \
                 FROM unnest($1::int4[], $2::int4[], $3::int8[]) AS a(t, p, o) \
                 WHERE st.topic_id = a.t AND st.partition_id = a.p AND st.local_offset = a.o \
                   AND st.external_offset IS NULL",
                None,
                &[t.into(), p.into(), o.into()],
            )?;
            Ok(())
        })
    }

    fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>> {
        use super::config::{ShadowMode, SyncMode, WriteMode};
        use pgrx::prelude::*;

        let query = r#"
            SELECT sc.topic_id, t.name, sc.mode, sc.forward_percentage,
                   sc.external_topic_name, sc.sync_mode,
                   COALESCE(sc.write_mode, 'dual_write') as write_mode
            FROM kafka.shadow_config sc
            JOIN kafka.topics t ON sc.topic_id = t.id
        "#;

        let mut configs: Vec<TopicShadowConfig> = Vec::new();
        let mut query_ok = false;

        Spi::connect(|client| {
            let result = client.select(query, None, &[]);

            if let Ok(table) = result {
                query_ok = true;
                for row in table {
                    // Use named columns for clarity and safety
                    let topic_id: i32 = row.get_by_name("topic_id").unwrap_or(Some(0)).unwrap_or(0);
                    let topic_name: String = row
                        .get_by_name("name")
                        .unwrap_or(Some(String::new()))
                        .unwrap_or_default();
                    let mode_str: String = row
                        .get_by_name("mode")
                        .unwrap_or(Some("local_only".to_string()))
                        .unwrap_or_default();
                    let forward_percentage: i32 = row
                        .get_by_name("forward_percentage")
                        .unwrap_or(Some(0))
                        .unwrap_or(0);
                    let external_topic_name: Option<String> =
                        row.get_by_name("external_topic_name").unwrap_or(None);
                    let sync_mode_str: String = row
                        .get_by_name("sync_mode")
                        .unwrap_or(Some("sync".to_string()))
                        .unwrap_or_default();
                    let write_mode_str: String = row
                        .get_by_name("write_mode")
                        .unwrap_or(Some("dual_write".to_string()))
                        .unwrap_or_default();

                    configs.push(TopicShadowConfig {
                        topic_id,
                        topic_name,
                        mode: ShadowMode::parse(&mode_str),
                        forward_percentage: forward_percentage.clamp(0, 100) as u8,
                        external_topic_name,
                        sync_mode: SyncMode::parse(&sync_mode_str),
                        write_mode: WriteMode::parse(&write_mode_str),
                    });
                }
            }
        });

        query_ok.then_some(configs)
    }

    fn current_transaction_id(&self) -> u32 {
        // SAFETY: reads the backend's own transaction state; no allocation.
        unsafe { pgrx::pg_sys::GetCurrentTransactionIdIfAny() }.into_inner()
    }

    fn transaction_committed(&self, xid: u32) -> bool {
        if xid == 0 {
            return false;
        }
        // SAFETY: a clog lookup; valid outside a transaction. The worker calls
        // this after its own commit, so the xid is never still in progress here.
        unsafe {
            pgrx::pg_sys::TransactionIdDidCommit(pgrx::pg_sys::TransactionId::from_inner(xid))
        }
    }
}

#[cfg(test)]
pub(crate) mod fake {
    //! In-memory [`ShadowDb`] for unit tests. It models the semantics the SPI
    //! statements implement (see each method), with a manual clock in place of
    //! `NOW()`.

    use super::*;
    use crate::kafka::error::KafkaError;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    /// A `kafka.messages` row as far as shadow mode cares.
    #[derive(Clone, Debug, Default)]
    pub struct FakeMessage {
        pub key: Option<Vec<u8>>,
        pub value: Option<Vec<u8>>,
        pub headers: Vec<(String, Vec<u8>)>,
        pub timestamp_ms: Option<i64>,
        /// `(producer_id, producer_epoch)` and whether the row is still pending.
        pub txn: Option<(i64, i16)>,
        pub txn_pending: bool,
    }

    /// A `kafka.shadow_tracking` row.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct FakeTrackingRow {
        pub external_offset: Option<i64>,
        pub retry_count: i32,
        /// Last attempt, in fake-clock ms.
        pub forwarded_at: Option<i64>,
        pub error_message: Option<String>,
        pub attempt_open: bool,
    }

    /// A `kafka.shadow_metrics` row.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct FakeMetrics {
        pub messages_forwarded: i64,
        pub messages_failed: i64,
        pub messages_skipped: i64,
        pub last_forwarded_offset: Option<i64>,
    }

    #[derive(Default)]
    pub struct State {
        pub enabled: bool,
        pub strict: bool,
        pub settings: OutboxSettings,
        pub now_ms: i64,
        /// topic_id -> local topic name
        pub topics: HashMap<i32, String>,
        /// topic_id -> shadow_config.external_topic_name
        pub external_names: HashMap<i32, Option<String>>,
        pub messages: BTreeMap<RowKey, FakeMessage>,
        pub tracking: BTreeMap<RowKey, FakeTrackingRow>,
        pub metrics: HashMap<(i32, i32), FakeMetrics>,
        /// `None` makes `load_topic_configs` fail.
        pub configs: Option<Vec<TopicShadowConfig>>,
        /// Make `pending_txn_records` fail (None).
        pub fail_pending_txn_lookup: bool,
        /// Make `claim_due_outbox_rows` fail.
        pub fail_claim: bool,
        /// What `current_transaction_id` returns.
        pub current_xid: u32,
        /// Xids `transaction_committed` reports as committed.
        pub committed_xids: std::collections::HashSet<u32>,
    }

    /// In-memory [`ShadowDb`].
    #[derive(Default)]
    pub struct InMemoryShadowDb {
        pub state: Mutex<State>,
    }

    impl InMemoryShadowDb {
        /// Shadow mode enabled, no strict, empty tables.
        pub fn enabled() -> Self {
            let db = Self::default();
            db.state().enabled = true;
            db.state().configs = Some(Vec::new());
            db
        }

        pub fn state(&self) -> std::sync::MutexGuard<'_, State> {
            self.state.lock().unwrap()
        }

        pub fn add_topic(&self, topic_id: i32, name: &str, external_name: Option<&str>) {
            let mut s = self.state();
            s.topics.insert(topic_id, name.to_string());
            s.external_names
                .insert(topic_id, external_name.map(str::to_string));
        }

        pub fn add_message(&self, key: RowKey, record_key: &[u8], value: &[u8]) {
            self.state().messages.insert(
                key,
                FakeMessage {
                    key: Some(record_key.to_vec()),
                    value: Some(value.to_vec()),
                    ..Default::default()
                },
            );
        }

        pub fn row(&self, key: RowKey) -> Option<FakeTrackingRow> {
            self.state().tracking.get(&key).cloned()
        }

        pub fn advance_ms(&self, ms: i64) {
            self.state().now_ms += ms;
        }
    }

    impl ShadowDb for InMemoryShadowDb {
        fn shadow_mode_enabled(&self) -> bool {
            self.state().enabled
        }

        fn external_primary_strict(&self) -> bool {
            self.state().strict
        }

        fn settings(&self) -> OutboxSettings {
            self.state().settings.clone()
        }

        fn topic_has_dead_letters(&self, topic_id: i32, max_retries: i64) -> Result<bool> {
            Ok(self.state().tracking.iter().any(|(k, r)| {
                k.0 == topic_id
                    && r.external_offset.is_none()
                    && r.retry_count as i64 >= max_retries
            }))
        }

        fn insert_outbox_rows(&self, rows: &[(i32, i32, i64)]) -> Result<()> {
            let mut s = self.state();
            for &key in rows {
                if s.messages.contains_key(&key) {
                    s.tracking.entry(key).or_default();
                }
            }
            Ok(())
        }

        fn record_skipped(&self, topic_id: i32, partition_id: i32, n: i64) -> Result<()> {
            self.state()
                .metrics
                .entry((topic_id, partition_id))
                .or_default()
                .messages_skipped += n;
            Ok(())
        }

        fn pending_txn_records(
            &self,
            producer_id: i64,
            producer_epoch: i16,
        ) -> Option<Vec<PendingForwardRecord>> {
            let s = self.state();
            if s.fail_pending_txn_lookup {
                return None;
            }
            Some(
                s.messages
                    .iter()
                    .filter(|(_, m)| m.txn == Some((producer_id, producer_epoch)) && m.txn_pending)
                    .map(|(&(t, p, o), m)| PendingForwardRecord {
                        topic_id: t,
                        partition_id: p,
                        offset: o,
                        key: m.key.clone(),
                    })
                    .collect(),
            )
        }

        fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<Vec<ForwardRecord>> {
            let mut s = self.state();
            if s.fail_claim {
                return Err(KafkaError::Internal("claim failed".to_string()));
            }
            let now = s.now_ms;
            let inflight: std::collections::HashSet<RowKey> = claim
                .inflight_topics
                .iter()
                .zip(&claim.inflight_partitions)
                .zip(&claim.inflight_offsets)
                .map(|((&t, &p), &o)| (t, p, o))
                .collect();
            // cand: pending, under the cap, message exists (BTreeMap = key order).
            struct Cand {
                key: RowKey,
                in_flight: bool,
                backing_off: bool,
            }
            let cands: Vec<Cand> = s
                .tracking
                .iter()
                .filter(|(k, r)| {
                    r.external_offset.is_none()
                        && (r.retry_count as i64) < claim.max_retries
                        && s.messages.contains_key(k)
                })
                .map(|(k, r)| Cand {
                    key: *k,
                    in_flight: inflight.contains(k),
                    backing_off: !r.attempt_open
                        && r.retry_count > 0
                        && r.forwarded_at.is_some_and(|at| {
                            at > now
                                - retry_backoff_ms(
                                    r.retry_count as i64,
                                    claim.backoff_base_ms,
                                    claim.backoff_max_ms,
                                )
                        }),
                })
                .collect();
            let mut head: HashMap<(i32, i32), i64> = HashMap::new();
            let mut blocked: HashMap<(i32, i32), i64> = HashMap::new();
            for c in &cands {
                let part = (c.key.0, c.key.1);
                head.entry(part).or_insert(c.key.2);
                if c.backing_off && !c.in_flight {
                    blocked.entry(part).or_insert(c.key.2);
                }
            }
            let due: Vec<(RowKey, bool)> = cands
                .iter()
                .filter(|c| {
                    !c.in_flight
                        && !c.backing_off
                        && blocked
                            .get(&(c.key.0, c.key.1))
                            .is_none_or(|&b| c.key.2 < b)
                })
                .map(|c| (c.key, head[&(c.key.0, c.key.1)] == c.key.2))
                .take(claim.batch_limit.max(0) as usize)
                .collect();
            let mut out = Vec::new();
            for (key, restart) in due {
                let row = s.tracking.get_mut(&key).unwrap();
                if row.attempt_open {
                    row.retry_count += 1;
                }
                row.attempt_open = true;
                row.forwarded_at = Some(now);
                let Some(topic_name) = s.topics.get(&key.0).cloned() else {
                    continue; // JOIN kafka.topics drops it
                };
                let external = s
                    .external_names
                    .get(&key.0)
                    .cloned()
                    .flatten()
                    .unwrap_or(topic_name);
                let m = &s.messages[&key];
                out.push(ForwardRecord {
                    topic_id: key.0,
                    topic_name: external,
                    partition_id: key.1,
                    key: m.key.clone(),
                    value: m.value.clone(),
                    headers: m.headers.clone(),
                    timestamp_ms: m.timestamp_ms,
                    local_offset: key.2,
                    restart,
                });
            }
            Ok(out)
        }

        fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<usize> {
            let mut s = self.state();
            let now = s.now_ms;
            let mut n = 0;
            for &(key, external_offset) in rows {
                let Some(row) = s.tracking.get_mut(&key) else {
                    continue;
                };
                if row.external_offset.is_some() {
                    continue;
                }
                row.external_offset = Some(external_offset);
                row.forwarded_at = Some(now);
                row.error_message = None;
                row.attempt_open = false;
                n += 1;
                if metrics {
                    let m = s.metrics.entry((key.0, key.1)).or_default();
                    m.messages_forwarded += 1;
                    m.last_forwarded_offset =
                        Some(m.last_forwarded_offset.unwrap_or(i64::MIN).max(key.2));
                }
            }
            Ok(n)
        }

        fn apply_failed(
            &self,
            rows: &[(RowKey, String)],
            max_retries: i64,
            metrics: bool,
        ) -> Result<FailedApplied> {
            let mut s = self.state();
            let now = s.now_ms;
            let mut out = FailedApplied::default();
            for (key, error) in rows {
                let Some(row) = s.tracking.get_mut(key) else {
                    continue;
                };
                if row.external_offset.is_some() {
                    continue;
                }
                row.retry_count += 1;
                row.error_message = Some(error.clone());
                row.forwarded_at = Some(now);
                row.attempt_open = false;
                out.transitioned += 1;
                if row.retry_count as i64 >= max_retries {
                    out.dead_lettered.push((*key, error.clone()));
                }
                if metrics {
                    s.metrics.entry((key.0, key.1)).or_default().messages_failed += 1;
                }
            }
            Ok(out)
        }

        fn apply_not_attempted(&self, rows: &[RowKey]) -> Result<()> {
            let mut s = self.state();
            for key in rows {
                if let Some(row) = s.tracking.get_mut(key) {
                    if row.external_offset.is_none() {
                        row.attempt_open = false;
                    }
                }
            }
            Ok(())
        }

        fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>> {
            self.state().configs.clone()
        }

        fn current_transaction_id(&self) -> u32 {
            self.state().current_xid
        }

        fn transaction_committed(&self, xid: u32) -> bool {
            xid != 0 && self.state().committed_xids.contains(&xid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_the_base_and_is_capped() {
        assert_eq!(retry_backoff_ms(0, 100, 30_000), 0);
        assert_eq!(retry_backoff_ms(1, 100, 30_000), 100);
        assert_eq!(retry_backoff_ms(2, 100, 30_000), 200);
        assert_eq!(retry_backoff_ms(4, 100, 30_000), 800);
        assert_eq!(retry_backoff_ms(20, 100, 30_000), 30_000);
        assert_eq!(retry_backoff_ms(1000, 60_000, 30_000), 30_000);
    }
}
