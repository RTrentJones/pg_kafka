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
//! - [`SpiShadowDb`] is the production implementation (SPI and the real GUCs).
//! - `InMemoryShadowDb` (test builds only, in [`fake`]) models the `kafka.shadow_tracking`
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

/// How many due rows of one partition a claim of `batch_limit` rows takes
/// when `partitions` partitions have a row due: an equal share, at least 1.
/// A partition with a backlog then cannot fill every claim while the others
/// (another topic's live traffic, a sync-mode produce waiting for its ack)
/// wait behind it, and each partition still gets a contiguous run of its
/// lowest due offsets, so its order holds. Partitions whose pending rows are
/// not due (dead letters, orphans, rows in flight or backing off) take no
/// share, so they cannot slow the others down.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the claim SQL computes the same share (its `share` CTE); the fake ShadowDb calls this"
    )
)]
pub fn per_partition_claim_cap(batch_limit: i64, partitions: usize) -> i64 {
    let partitions = i64::try_from(partitions.max(1)).unwrap_or(i64::MAX);
    let limit = batch_limit.max(0);
    (limit / partitions + i64::from(limit % partitions != 0)).max(1)
}

/// The result of one outbox claim.
#[derive(Clone, Debug, Default)]
pub struct ClaimedRows {
    /// The claimed records, in row-key order.
    pub records: Vec<ForwardRecord>,
    /// The claim's [`per_partition_claim_cap`] (0 when nothing was claimed): a
    /// partition that got this many rows may have more due.
    pub partition_share: i64,
}

impl ClaimedRows {
    /// Whether more rows are probably due right away: the claim was full, or a
    /// partition used its whole share.
    pub fn more_due(&self, batch_limit: usize) -> bool {
        if self.records.is_empty() {
            return false;
        }
        if self.records.len() >= batch_limit {
            return true;
        }
        let share = usize::try_from(self.partition_share).unwrap_or(usize::MAX);
        let mut run = 0usize;
        let mut last = None;
        for record in &self.records {
            let part = record.row.partition();
            run = if last == Some(part) { run + 1 } else { 1 };
            last = Some(part);
            if run >= share {
                return true;
            }
        }
        false
    }
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

    /// Whether `pg_kafka.shadow_bootstrap_servers` names a broker. The forwarder
    /// answers every record `NotAttempted` while it is empty.
    fn forwarding_configured(&self) -> bool;

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

    /// Which of these `(producer_id, producer_epoch)` pairs have a transaction
    /// still Ongoing in `kafka.transactions`. `None` if the lookup failed.
    fn ongoing_transactions(&self, keys: &[(i64, i16)]) -> Option<Vec<(i64, i16)>>;

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
    ///
    /// The batch is shared between the partitions that have a due row: at most
    /// [`per_partition_claim_cap`] due rows of each (its lowest ones), taken
    /// round-robin across partitions up to `batch_limit`.
    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<ClaimedRows>;

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

/// Typed reads of one `kafka.shadow_config` row, by column name.
pub trait ConfigColumns {
    /// Why a column could not be read.
    type Error;
    /// An integer column (NULL is `None`).
    fn int(&self, column: &str) -> std::result::Result<Option<i32>, Self::Error>;
    /// A text column (NULL is `None`).
    fn text(&self, column: &str) -> std::result::Result<Option<String>, Self::Error>;
}

impl ConfigColumns for pgrx::spi::SpiHeapTupleData<'_> {
    type Error = pgrx::spi::SpiError;

    fn int(&self, column: &str) -> std::result::Result<Option<i32>, Self::Error> {
        self.get_by_name(column)
    }

    fn text(&self, column: &str) -> std::result::Result<Option<String>, Self::Error> {
        self.get_by_name(column)
    }
}

/// One topic's configuration from its `kafka.shadow_config` row.
///
/// A NULL column takes its default (`mode`, `sync_mode` and `write_mode`
/// through their `parse`). A column that cannot be read at all fails the
/// row, so `load_topic_configs` fails and the reload keeps the previous
/// cache instead of loading a config whose unreadable fields were guessed.
pub fn topic_config_from_row<R: ConfigColumns>(
    row: &R,
) -> std::result::Result<TopicShadowConfig, R::Error> {
    use super::config::{ShadowMode, SyncMode, WriteMode};
    let text = |column: &str| -> std::result::Result<String, R::Error> {
        Ok(row.text(column)?.unwrap_or_default())
    };
    let percentage = row.int("forward_percentage")?.unwrap_or(0);
    Ok(TopicShadowConfig {
        topic_id: row.int("topic_id")?.unwrap_or(0),
        topic_name: text("name")?,
        mode: ShadowMode::parse(&text("mode")?),
        forward_percentage: u8::try_from(percentage.clamp(0, 100))
            .expect("a value in 0..=100 fits in u8"),
        external_topic_name: row.text("external_topic_name")?,
        sync_mode: SyncMode::parse(&text("sync_mode")?),
        write_mode: WriteMode::parse(&text("write_mode")?),
    })
}

/// The claim's due rows of the partition `p` (with its `blocked` row `b`): the
/// `FROM ... WHERE` of a select over `kafka.shadow_tracking st`, shared by the
/// probe that finds the partitions with a due row and the select that takes
/// them, so the two cannot disagree. `$2` is the retry cap, `$6`/`$7` the
/// backoff base and max, `inf` the in-flight rows.
///
/// The in-flight rows are excluded with `NOT IN (SELECT ... FROM inf)`, not a
/// correlated `NOT EXISTS`: Postgres runs the uncorrelated `NOT IN` as a hashed
/// subplan built once per claim, while the `NOT EXISTS` was planned as a nested
/// loop anti join that scanned all of `inf` for every pending row it passed. With
/// thousands of rows in flight (up to `MAX_INFLIGHT`) at the head of a backlog, that
/// made each claim O(in flight x scanned): a claim behind 8 000 in-flight rows of
/// a 20 000-row backlog took 5.3 s that way, 0.26 s this way.
/// Every column on both sides is NOT NULL, so `NOT IN` means exactly `NOT EXISTS`.
macro_rules! due_rows_of_partition {
    () => {
        r"
                        FROM kafka.shadow_tracking st
                        CROSS JOIN LATERAL (
                            SELECT 1 FROM kafka.messages m
                            WHERE m.topic_id = st.topic_id
                              AND m.partition_id = st.partition_id
                              AND m.partition_offset = st.local_offset
                            LIMIT 1
                        ) has_message
                        WHERE st.topic_id = p.topic_id
                          AND st.partition_id = p.partition_id
                          AND st.external_offset IS NULL
                          AND st.retry_count < $2
                          AND (b.first_blocked IS NULL OR st.local_offset < b.first_blocked)
                          AND NOT (st.retry_count > 0 AND NOT st.attempt_open
                                   AND st.forwarded_at > NOW() - make_interval(secs =>
                                       LEAST($6::float8 * power(2::float8, LEAST(st.retry_count - 1, 30)),
                                             $7::float8) / 1000.0))
                          AND (st.topic_id, st.partition_id, st.local_offset)
                              NOT IN (SELECT inf.t, inf.p, inf.o FROM inf)
                        "
    };
}

/// The outbox claim (`SpiShadowDb::claim_due_outbox_rows`); its parameters are listed there.
const CLAIM_DUE_OUTBOX_ROWS_SQL: &str = concat!(
    r"
        WITH RECURSIVE inf AS (
            SELECT * FROM unnest($3::int4[], $4::int4[], $5::int8[]) AS i(t, p, o)
        ),
        -- The partitions with pending rows, found by a skip scan of the
        -- pending index: one probe per partition, not one per row.
        parts AS (
            (SELECT st.topic_id, st.partition_id
             FROM kafka.shadow_tracking st
             WHERE st.external_offset IS NULL
             ORDER BY st.topic_id, st.partition_id
             LIMIT 1)
            UNION ALL
            SELECT nxt.topic_id, nxt.partition_id
            FROM parts
            CROSS JOIN LATERAL (
                SELECT st.topic_id, st.partition_id
                FROM kafka.shadow_tracking st
                WHERE st.external_offset IS NULL
                  AND (st.topic_id, st.partition_id) > (parts.topic_id, parts.partition_id)
                ORDER BY st.topic_id, st.partition_id
                LIMIT 1
            ) nxt
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
              AND (st.topic_id, st.partition_id, st.local_offset)
                  NOT IN (SELECT inf.t, inf.p, inf.o FROM inf)
            GROUP BY st.topic_id, st.partition_id
        ),
        -- The partitions with a row due now (one probe each). Only they
        -- share the batch: a partition holding only dead letters, orphan
        -- pointers, or rows in flight or backing off would otherwise take
        -- a share it cannot use, and with many of them a backlog elsewhere
        -- would get a few rows per claim. The probe is a LATERAL subquery cut
        -- at the partition's first due row in offset order, not WHERE EXISTS:
        -- the planner may flatten an EXISTS into a semi join, and with a
        -- 100 000-200 000-row backlog in one partition it chose a hash semi
        -- join that read every pending row (and probed messages for each) on
        -- every claim. The ORDER BY keeps the probe on the pending index:
        -- without it the planner may take a seq scan expected to stop early,
        -- which first reads every delivered row kept for diagnostics.
        due_parts AS MATERIALIZED (
            SELECT p.topic_id, p.partition_id
            FROM parts p
            LEFT JOIN blocked b
              ON b.topic_id = p.topic_id AND b.partition_id = p.partition_id
            CROSS JOIN LATERAL (
                SELECT 1 ",
    due_rows_of_partition!(),
    r"
                ORDER BY st.local_offset
                LIMIT 1
            ) any_due
        ),
        -- Each partition's share of the batch (see per_partition_claim_cap).
        share AS (
            SELECT GREATEST(1, CEIL($1::numeric / GREATEST(COUNT(*), 1)))::int8 AS k
            FROM due_parts
        ),
        -- Due rows: the first k of each partition in offset order (read
        -- from the pending index and cut at k, so the cost is
        -- O(partitions + limit), not O(backlog)), then taken round-robin
        -- across partitions up to the limit, so one partition's backlog
        -- cannot fill every claim while the others wait.
        due AS (
            SELECT d.topic_id, d.partition_id, d.local_offset
            FROM due_parts p
            CROSS JOIN share
            LEFT JOIN blocked b
              ON b.topic_id = p.topic_id AND b.partition_id = p.partition_id
            CROSS JOIN LATERAL (
                SELECT st.topic_id, st.partition_id, st.local_offset,
                       row_number() OVER (ORDER BY st.local_offset) AS rn
                ",
    due_rows_of_partition!(),
    r"
                ORDER BY st.local_offset
                LIMIT share.k
            ) d
            ORDER BY d.rn, d.topic_id, d.partition_id
            LIMIT $1
        ),
        -- Each due partition's head: its lowest pending, non-dead row whose
        -- message exists. A claimed row restarts its partition when nothing
        -- lies below it, i.e. when it is at or below the head. Found once per
        -- partition, not once per claimed row: every probe walks the pending
        -- index from the partition's start, past the entries of rows forwarded
        -- since the last vacuum, so a per-row probe made a claim cost
        -- O(claimed x forwarded). MATERIALIZED: inlined, the planner put it
        -- inside the update's nested loop and probed it once per row again.
        heads AS MATERIALIZED (
            SELECT p.topic_id, p.partition_id, h.local_offset AS head
            FROM due_parts p
            CROSS JOIN LATERAL (
                SELECT lo.local_offset
                FROM kafka.shadow_tracking lo
                CROSS JOIN LATERAL (
                    SELECT 1 FROM kafka.messages m
                    WHERE m.topic_id = lo.topic_id
                      AND m.partition_id = lo.partition_id
                      AND m.partition_offset = lo.local_offset
                    LIMIT 1
                ) lo_message
                WHERE lo.topic_id = p.topic_id
                  AND lo.partition_id = p.partition_id
                  AND lo.external_offset IS NULL
                  AND lo.retry_count < $2
                ORDER BY lo.local_offset
                LIMIT 1
            ) h
        ),
        claimed AS (
            -- An attempt that is still open here lost its ack (the worker
            -- restarted, or the in-flight entry was evicted): count it as
            -- one failure so a forward that never answers still reaches
            -- the dead-letter cap. A fresh claim counts nothing; real
            -- failures are counted by their ack.
            UPDATE kafka.shadow_tracking st
            SET forwarded_at = NOW(),
                attempt_open = true,
                retry_count = st.retry_count + CASE WHEN st.attempt_open THEN 1 ELSE 0 END
            FROM due
            LEFT JOIN heads h
              ON h.topic_id = due.topic_id AND h.partition_id = due.partition_id
            WHERE st.topic_id = due.topic_id
              AND st.partition_id = due.partition_id
              AND st.local_offset = due.local_offset
            RETURNING st.topic_id, st.partition_id, st.local_offset,
                      -- restart: no lower pending, non-dead row of the
                      -- partition whose message exists (the partition head).
                      (h.head IS NULL OR h.head >= st.local_offset) AS restart
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
               COALESCE(sc.external_topic_name, t.name) AS external_topic,
               share.k        AS share
        FROM claimed c
        CROSS JOIN share
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
        "
);

/// Production [`ShadowDb`]: SPI and the real GUCs.
#[derive(Debug, Default, Clone, Copy)]
pub struct SpiShadowDb;

/// The claim's floor for `work_mem`. Its three `NOT IN (SELECT ... FROM inf)` exclusions are
/// hashed subplans only when the in-flight list (up to `MAX_INFLIGHT`, 8 192 rows, a few
/// hundred kB hashed) fits in the hash memory limit; below it Postgres rescans the list for
/// every pending row it passes (10 s per claim with 8 000 rows in flight at `work_mem = 64kB`).
const CLAIM_WORK_MEM: &str = "4MB";

/// Read the current `work_mem` and `jit`, then set them for the claim, transaction-locally:
/// `work_mem` at least [`CLAIM_WORK_MEM`] (never lowered), and JIT off. The claim's cost
/// estimate grows with the backlog and crosses `jit_above_cost` (and the inline/optimize
/// thresholds), and compiling it costs more than running it (57 ms to 0.9 s per claim
/// where the server has llvmjit). `$1` is [`CLAIM_WORK_MEM`].
const CLAIM_SETTINGS_SQL: &str = "
    SELECT o.work_mem, o.jit,
           set_config('work_mem',
                      CASE WHEN pg_size_bytes(o.work_mem) < pg_size_bytes($1)
                           THEN $1 ELSE o.work_mem END,
                      true) AS claim_work_mem,
           set_config('jit', 'off', true) AS claim_jit
    FROM (SELECT current_setting('work_mem') AS work_mem,
                 current_setting('jit') AS jit
          OFFSET 0) o";

/// Put back what [`CLAIM_SETTINGS_SQL`] read (`$1` work_mem, `$2` jit), so the rest of the
/// worker's transaction runs with its own settings.
const RESTORE_CLAIM_SETTINGS_SQL: &str =
    "SELECT set_config('work_mem', $1, true), set_config('jit', $2, true)";

impl SpiShadowDb {
    /// Run [`CLAIM_DUE_OUTBOX_ROWS_SQL`] (parameters in [`ShadowDb::claim_due_outbox_rows`]).
    fn run_claim(
        client: &mut pgrx::spi::SpiClient<'_>,
        claim: &OutboxClaim,
        backoff_base_ms: f64,
        backoff_max_ms: f64,
    ) -> Result<ClaimedRows> {
        let table = client.update(
            CLAIM_DUE_OUTBOX_ROWS_SQL,
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

        let mut claimed = ClaimedRows::default();
        for row in table {
            claimed.partition_share = row.get_by_name("share")?.unwrap_or(0);
            let headers_json: Option<String> = row.get_by_name("headers_json")?;
            claimed.records.push(ForwardRecord {
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
        Ok(claimed)
    }
}

impl ShadowDb for SpiShadowDb {
    fn shadow_mode_enabled(&self) -> bool {
        crate::config::SHADOW_MODE_ENABLED.get()
    }

    fn external_primary_strict(&self) -> bool {
        crate::config::SHADOW_EXTERNAL_PRIMARY_STRICT.get()
    }

    fn forwarding_configured(&self) -> bool {
        // The forwarder's own test (`forward_one`): an empty value is unset.
        crate::config::SHADOW_BOOTSTRAP_SERVERS
            .get()
            .is_some_and(|servers| !servers.is_empty())
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
            // Only enqueue an outbox pointer when the backing kafka.messages
            // row exists. The live-produce enqueue runs in the same
            // subtransaction as the message insert, so the row is always
            // visible there. On the txn commit path it drops orphan pointers
            // left by a transactional produce that buffered offsets in RAM but
            // whose message rows were rolled back (a panic mid-txn), which
            // would otherwise be never-claimed, never-cleaned rows. One
            // statement per batch; the LATERAL + LIMIT 1 keeps the check an
            // index probe per row (an EXISTS is planned as a hash join over
            // every message).
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

    fn ongoing_transactions(&self, keys: &[(i64, i16)]) -> Option<Vec<(i64, i16)>> {
        use pgrx::prelude::*;
        if keys.is_empty() {
            return Some(Vec::new());
        }
        let producer_ids: Vec<i64> = keys.iter().map(|&(id, _)| id).collect();
        let epochs: Vec<i16> = keys.iter().map(|&(_, epoch)| epoch).collect();
        let result: std::result::Result<Vec<(i64, i16)>, spi::SpiError> = Spi::connect(|client| {
            let table = client.select(
                "SELECT t.producer_id, t.producer_epoch \
                 FROM kafka.transactions t \
                 JOIN unnest($1::int8[], $2::int2[]) AS k(id, epoch) \
                   ON t.producer_id = k.id AND t.producer_epoch = k.epoch \
                 WHERE t.state = 'Ongoing'",
                None,
                &[producer_ids.into(), epochs.into()],
            )?;
            let mut ongoing = Vec::new();
            for row in table {
                ongoing.push((
                    row.get_by_name("producer_id")?.unwrap_or(0),
                    row.get_by_name("producer_epoch")?.unwrap_or(0),
                ));
            }
            Ok(ongoing)
        });
        match result {
            Ok(ongoing) => Some(ongoing),
            Err(e) => {
                tracing::warn!("Shadow: failed to read the open transactions: {e:?}");
                None
            }
        }
    }

    fn claim_due_outbox_rows(&self, claim: &OutboxClaim) -> Result<ClaimedRows> {
        use pgrx::prelude::*;
        #[expect(
            clippy::cast_precision_loss,
            reason = "backoffs are GUC-bounded milliseconds (at most a minute), exact in f64"
        )]
        let (backoff_base_ms, backoff_max_ms) =
            (claim.backoff_base_ms as f64, claim.backoff_max_ms as f64);
        Spi::connect_mut(|client| {
            // The claim's planner settings, for this statement alone (see
            // CLAIM_SETTINGS_SQL); restored below whatever the claim returns. A
            // Postgres ERROR aborts the subtransaction, which undoes them anyway.
            let saved = client.update(CLAIM_SETTINGS_SQL, None, &[CLAIM_WORK_MEM.into()])?;
            let saved = saved.first();
            let work_mem: Option<String> = saved.get_by_name("work_mem")?;
            let jit: Option<String> = saved.get_by_name("jit")?;
            let claimed = Self::run_claim(client, claim, backoff_base_ms, backoff_max_ms);
            client.update(
                RESTORE_CLAIM_SETTINGS_SQL,
                None,
                &[work_mem.into(), jit.into()],
            )?;
            claimed
        })
    }

    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<usize> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(0);
        }
        let keys: KeyColumns = rows.iter().map(|(k, _)| k).collect();
        let external_offsets: Vec<i64> = rows.iter().map(|&(_, x)| x).collect();
        // The metric is bumped in the SAME statement that flips the rows,
        // guarded by `external_offset IS NULL`, so a duplicate ack updates
        // nothing and counts nothing: exactly once per row. One statement for
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
                table.map(|row| topic_config_from_row(&row)).collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::shadow::config::{ShadowMode, SyncMode, WriteMode};
    use std::collections::HashMap;

    /// The claim's cost must not grow with the rows in flight or with the claim's
    /// size beyond linear (the E2E `test_shadow_claims_stay_fast_with_a_full_in_flight_window`
    /// times it; the plan itself needs SPI): the in-flight rows are excluded by an
    /// uncorrelated NOT IN, which Postgres hashes once per claim, never by a
    /// correlated scan of `inf` per pending row; and the restart flag reads each
    /// partition's head once (a materialized CTE), not a probe per claimed row.
    #[test]
    fn the_claim_has_no_per_row_scan_of_the_in_flight_rows() {
        let sql: String = CLAIM_DUE_OUTBOX_ROWS_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join(" ");
        let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            !sql.contains("SELECT 1 FROM inf"),
            "correlated in-flight scan: {sql}"
        );
        assert_eq!(
            sql.matches("FROM inf").count(),
            sql.matches("NOT IN (SELECT inf.t, inf.p, inf.o FROM inf)")
                .count(),
            "every read of inf is an uncorrelated NOT IN"
        );
        assert_eq!(
            sql.matches("NOT IN (SELECT inf.t, inf.p, inf.o FROM inf)")
                .count(),
            3
        );
        assert!(sql.contains("heads AS MATERIALIZED ("));
        assert!(sql.contains("(h.head IS NULL OR h.head >= st.local_offset) AS restart"));
        let returning = &sql[sql.find("RETURNING").expect("the claim returns its rows")..];
        let returning = &returning[..returning.find(") SELECT").expect("the claimed CTE ends")];
        assert!(
            !returning.contains("SELECT"),
            "no subquery per claimed row in RETURNING: {returning}"
        );
    }

    /// `due_parts` finds a partition's first due row with an ordered LATERAL ... LIMIT 1
    /// (an index probe that stops there), never an EXISTS the planner may turn into a
    /// semi join over every pending row (the E2E
    /// `test_shadow_claims_stay_fast_behind_a_large_backlog` times it).
    #[test]
    fn the_due_partition_probe_stops_at_the_first_due_row() {
        let sql: String = CLAIM_DUE_OUTBOX_ROWS_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join(" ");
        let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(!sql.contains("EXISTS"), "no (NOT) EXISTS: {sql}");
        let start = sql.find("due_parts AS MATERIALIZED (").unwrap();
        let due_parts = &sql[start..sql[start..].find("), share AS").unwrap() + start];
        assert!(due_parts.contains("CROSS JOIN LATERAL ( SELECT 1 FROM kafka.shadow_tracking st"));
        assert!(
            due_parts
                .trim_end()
                .ends_with("ORDER BY st.local_offset LIMIT 1 ) any_due"),
            "{due_parts}"
        );
    }

    /// The claim runs with a `work_mem` floor and without JIT, set transaction-locally
    /// (`set_config(.., true)`, undone by a rollback of the poll's subtransaction) and put
    /// back after it, so no other statement of the worker's transaction sees them. The
    /// floor is only ever raised to, never lowered.
    #[test]
    fn the_claim_runs_with_a_work_mem_floor_and_without_jit() {
        let squash = |sql: &str| sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let settings = squash(CLAIM_SETTINGS_SQL);
        assert!(settings.contains("set_config('jit', 'off', true)"));
        assert!(settings.contains(
            "set_config('work_mem', CASE WHEN pg_size_bytes(o.work_mem) < pg_size_bytes($1) \
             THEN $1 ELSE o.work_mem END, true)"
        ));
        // The old values are read before they are changed.
        assert!(settings.contains("current_setting('work_mem') AS work_mem"));
        assert!(settings.contains("OFFSET 0) o"));
        let restore = squash(RESTORE_CLAIM_SETTINGS_SQL);
        assert_eq!(
            restore,
            "SELECT set_config('work_mem', $1, true), set_config('jit', $2, true)"
        );
        assert_eq!(
            settings.matches(", true)").count(),
            2,
            "both settings are local"
        );
        // 8 192 in-flight keys hashed take a few hundred kB; the floor leaves room.
        let mb: u64 = CLAIM_WORK_MEM.trim_end_matches("MB").parse().unwrap();
        assert!(mb * 1024 * 1024 >= 8 * 8_192 * 64);
    }

    /// A config row: a column is a value, NULL, or unreadable (`Err`).
    #[derive(Default)]
    struct Row(HashMap<&'static str, std::result::Result<Option<String>, ()>>);

    impl Row {
        fn full() -> Self {
            let mut row = Self::default();
            for (column, value) in [
                ("topic_id", "7"),
                ("name", "orders"),
                ("mode", "shadow"),
                ("forward_percentage", "40"),
                ("external_topic_name", "prod-orders"),
                ("sync_mode", "sync"),
                ("write_mode", "external_only"),
            ] {
                row.0.insert(column, Ok(Some(value.to_string())));
            }
            row
        }

        fn with(
            mut self,
            column: &'static str,
            value: std::result::Result<Option<&str>, ()>,
        ) -> Self {
            self.0.insert(column, value.map(|v| v.map(str::to_string)));
            self
        }
    }

    impl ConfigColumns for Row {
        type Error = String;

        fn int(&self, column: &str) -> std::result::Result<Option<i32>, String> {
            self.text(column)?
                .map(|v| v.parse().map_err(|_| format!("{column}: not an int")))
                .transpose()
        }

        fn text(&self, column: &str) -> std::result::Result<Option<String>, String> {
            match self.0.get(column) {
                Some(Ok(value)) => Ok(value.clone()),
                Some(Err(())) | None => Err(format!("{column}: unreadable")),
            }
        }
    }

    #[test]
    fn test_topic_config_from_a_full_row() {
        let config = topic_config_from_row(&Row::full()).unwrap();
        assert_eq!(config.topic_id, 7);
        assert_eq!(config.topic_name, "orders");
        assert_eq!(config.mode, ShadowMode::Shadow);
        assert_eq!(config.forward_percentage, 40);
        assert_eq!(config.external_topic_name.as_deref(), Some("prod-orders"));
        assert_eq!(config.sync_mode, SyncMode::Sync);
        assert_eq!(config.write_mode, WriteMode::ExternalOnly);
    }

    #[test]
    fn test_topic_config_null_columns_take_their_defaults() {
        let mut row = Row::default();
        for column in [
            "topic_id",
            "name",
            "mode",
            "forward_percentage",
            "external_topic_name",
            "sync_mode",
            "write_mode",
        ] {
            row = row.with(column, Ok(None));
        }
        let config = topic_config_from_row(&row).unwrap();
        assert_eq!(config.topic_id, 0);
        assert_eq!(config.topic_name, "");
        assert_eq!(config.mode, ShadowMode::LocalOnly);
        assert_eq!(config.forward_percentage, 0);
        assert_eq!(config.external_topic_name, None);
        assert_eq!(config.sync_mode, SyncMode::Async);
        assert_eq!(config.write_mode, WriteMode::DualWrite);
    }

    #[test]
    fn test_topic_config_percentage_is_clamped() {
        for (stored, expected) in [("-5", 0), ("0", 0), ("100", 100), ("250", 100)] {
            let row = Row::full().with("forward_percentage", Ok(Some(stored)));
            assert_eq!(
                topic_config_from_row(&row).unwrap().forward_percentage,
                expected,
                "{stored}"
            );
        }
    }

    #[test]
    fn test_topic_config_unreadable_column_fails_the_row() {
        // Each column, unreadable, fails the whole row rather than defaulting:
        // load_topic_configs then fails and the previous cache stays.
        for column in [
            "topic_id",
            "name",
            "mode",
            "forward_percentage",
            "external_topic_name",
            "sync_mode",
            "write_mode",
        ] {
            let row = Row::full().with(column, Err(()));
            assert!(topic_config_from_row(&row).is_err(), "{column}");
        }
    }
}
