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
use super::{ForwardRecord, KeyColumns, RowKey};
use crate::kafka::error::Result;

/// A transactional record's outbox pointer: the row key plus the record key
/// (used only for percentage routing at enqueue time). The payload is re-read
/// from `kafka.messages` by the forwarder.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingForwardRecord {
    /// The record's row (and future outbox row).
    pub row: RowKey,
    /// Record key, used only for percentage routing at enqueue time.
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
        use crate::kafka::constants::{
            DEFAULT_SHADOW_BATCH_SIZE, DEFAULT_SHADOW_MAX_RETRIES, DEFAULT_SHADOW_METRICS_ENABLED,
            DEFAULT_SHADOW_RETRY_BACKOFF_MS, DEFAULT_SHADOW_SYNC_TIMEOUT_MS,
        };
        Self {
            max_retries: i64::from(DEFAULT_SHADOW_MAX_RETRIES),
            retry_backoff_ms: i64::from(DEFAULT_SHADOW_RETRY_BACKOFF_MS),
            batch_size: i64::from(DEFAULT_SHADOW_BATCH_SIZE),
            metrics_enabled: DEFAULT_SHADOW_METRICS_ENABLED,
            sync_timeout_ms: u64::try_from(DEFAULT_SHADOW_SYNC_TIMEOUT_MS).unwrap_or(0),
        }
    }
}

/// Upper bound of the outbox re-attempt backoff.
pub const OUTBOX_MAX_BACKOFF_MS: i64 = 30_000;

/// Parameters of one outbox claim (see [`ShadowDb::claim_due_outbox_rows`]).
#[derive(Clone, Debug, Default)]
pub struct OutboxClaim {
    /// Max rows claimed per call.
    pub batch_limit: i64,
    /// Rows with `retry_count >= max_retries` are dead letters and never claimed.
    pub max_retries: i64,
    /// Re-attempt backoff after a row's `n`-th failure: `base * 2^(n-1)`, at
    /// most `max` (milliseconds).
    pub backoff_base_ms: i64,
    pub backoff_max_ms: i64,
    /// In-flight row keys, excluded from the claim.
    pub inflight: KeyColumns,
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

    /// Insert pending outbox rows in the current transaction, each only if its `kafka.messages` row exists;
    /// existing rows are left alone.
    fn insert_outbox_rows(&self, rows: &[RowKey]) -> Result<()>;

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
    /// recent than its backoff (see [`OutboxClaim`]). A candidate is claimed
    /// when it is not in flight, not backing off, and no backing-off candidate
    /// of its partition has a lower offset (so a retry is never overtaken by a later record). A row
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

    fn insert_outbox_rows(&self, rows: &[RowKey]) -> Result<()> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(());
        }
        let keys: KeyColumns = rows.iter().collect();
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
                &[
                    keys.topic_ids.into(),
                    keys.partition_ids.into(),
                    keys.local_offsets.into(),
                ],
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
        let result: std::result::Result<Vec<PendingForwardRecord>, spi::SpiError> =
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
                        row: RowKey::new(
                            row.get_by_name("topic_id")?.unwrap_or(0),
                            row.get_by_name("partition_id")?.unwrap_or(0),
                            row.get_by_name("partition_offset")?.unwrap_or(0),
                        ),
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
        #[expect(
            clippy::cast_precision_loss,
            reason = "backoffs are GUC-bounded milliseconds (at most a minute), exact in f64"
        )]
        let (backoff_base_ms, backoff_max_ms) =
            (claim.backoff_base_ms as f64, claim.backoff_max_ms as f64);
        Spi::connect_mut(|client| {
            let table = client.update(
                r"
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
                ",
                None,
                &[
                    claim.batch_limit.into(),
                    claim.max_retries.into(),
                    claim.inflight.topic_ids.clone().into(),
                    claim.inflight.partition_ids.clone().into(),
                    claim.inflight.local_offsets.clone().into(),
                    backoff_base_ms.into(),
                    backoff_max_ms.into(),
                ],
            )?;

            let mut records: Vec<ForwardRecord> = Vec::new();
            for row in table {
                let headers_json: Option<String> = row.get_by_name("headers_json")?;
                records.push(ForwardRecord {
                    row: RowKey::new(
                        row.get_by_name("topic_id")?.unwrap_or(0),
                        row.get_by_name("partition_id")?.unwrap_or(0),
                        row.get_by_name("local_offset")?.unwrap_or(0),
                    ),
                    topic_name: row.get_by_name("external_topic")?.unwrap_or_default(),
                    key: row.get_by_name("msg_key")?,
                    value: row.get_by_name("msg_value")?,
                    headers: crate::kafka::storage::decode_headers_json(headers_json.as_deref()),
                    timestamp_ms: row.get_by_name("ts")?,
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
        let keys: KeyColumns = rows.iter().map(|(k, _)| k).collect();
        let external_offsets: Vec<i64> = rows.iter().map(|&(_, x)| x).collect();
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
                &[
                    keys.topic_ids.into(),
                    keys.partition_ids.into(),
                    keys.local_offsets.into(),
                    external_offsets.into(),
                    metrics.into(),
                ],
            )?;
            let mut n = 0i64;
            for row in table {
                n = row.get_by_name::<i64, _>("n")?.unwrap_or(0);
            }
            Ok(usize::try_from(n).unwrap_or(0))
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
        let keys: KeyColumns = rows.iter().map(|(k, _)| k).collect();
        let errors: Vec<&str> = rows.iter().map(|(_, e)| e.as_str()).collect();
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
                &[
                    keys.topic_ids.into(),
                    keys.partition_ids.into(),
                    keys.local_offsets.into(),
                    errors.into(),
                    metrics.into(),
                ],
            )?;
            let mut out = FailedApplied::default();
            for row in table {
                out.transitioned += 1;
                let retry_count: i32 = row.get_by_name("retry_count")?.unwrap_or(0);
                if i64::from(retry_count) >= max_retries {
                    out.dead_lettered.push((
                        RowKey::new(
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
        let keys: KeyColumns = rows.iter().collect();
        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.shadow_tracking st SET attempt_open = false \
                 FROM unnest($1::int4[], $2::int4[], $3::int8[]) AS a(t, p, o) \
                 WHERE st.topic_id = a.t AND st.partition_id = a.p AND st.local_offset = a.o \
                   AND st.external_offset IS NULL",
                None,
                &[
                    keys.topic_ids.into(),
                    keys.partition_ids.into(),
                    keys.local_offsets.into(),
                ],
            )?;
            Ok(())
        })
    }

    fn load_topic_configs(&self) -> Option<Vec<TopicShadowConfig>> {
        use super::config::{ShadowMode, SyncMode, WriteMode};
        use pgrx::prelude::*;

        let result: std::result::Result<Vec<TopicShadowConfig>, spi::SpiError> =
            Spi::connect(|client| {
                let table = client.select(
                    "SELECT sc.topic_id, t.name, sc.mode, sc.forward_percentage, \
                            sc.external_topic_name, sc.sync_mode, \
                            COALESCE(sc.write_mode, 'dual_write') AS write_mode \
                     FROM kafka.shadow_config sc \
                     JOIN kafka.topics t ON sc.topic_id = t.id",
                    None,
                    &[],
                )?;
                let mut configs = Vec::new();
                for row in table {
                    // A NULL text column parses as its mode's default; a column
                    // that cannot be read at all falls back to `on_error`.
                    let text = |column: &str, on_error: &str| -> String {
                        row.get_by_name::<String, _>(column)
                            .unwrap_or_else(|_| Some(on_error.to_string()))
                            .unwrap_or_default()
                    };
                    let int = |column: &str| -> i32 {
                        row.get_by_name::<i32, _>(column)
                            .ok()
                            .flatten()
                            .unwrap_or(0)
                    };
                    configs.push(TopicShadowConfig {
                        topic_id: int("topic_id"),
                        topic_name: text("name", ""),
                        mode: ShadowMode::parse(&text("mode", "local_only")),
                        forward_percentage: u8::try_from(int("forward_percentage").clamp(0, 100))
                            .expect("a value in 0..=100 fits in u8"),
                        external_topic_name: row.get_by_name("external_topic_name").ok().flatten(),
                        sync_mode: SyncMode::parse(&text("sync_mode", "sync")),
                        write_mode: WriteMode::parse(&text("write_mode", "dual_write")),
                    });
                }
                Ok(configs)
            });
        match result {
            Ok(configs) => Some(configs),
            Err(e) => {
                tracing::warn!("Shadow: failed to read kafka.shadow_config: {e:?}");
                None
            }
        }
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
pub(crate) mod fake;
