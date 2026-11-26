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
use std::collections::HashMap;

/// Rows of a transaction read per fetch when its sampled topics' records are routed
/// at commit ([`ShadowDb::enqueue_pending_txn_records`]): what the worker holds at once,
/// however large the transaction.
pub const TXN_ENQUEUE_CHUNK: i64 = 1000;

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
    /// Run the full dead-flag sync ([`DEAD_LETTER_SYNC_SQL`]) with `max_retries`
    /// before the claim: the flags may not agree with this cap (the worker's first
    /// claim, a changed cap, an earlier sync that did not commit). See
    /// [`ClaimedRows::dead_flags_synced`].
    pub sync_dead_flags: bool,
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
    /// Whether the claim ran the dead-flag sync ([`OutboxClaim::sync_dead_flags`]).
    /// The flags agree with the cap once the claim's transaction committed.
    pub dead_flags_synced: bool,
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
    /// Pending rows another transaction holds: their acks were not applied (see
    /// [`AcksApplied::held`]).
    pub held: Vec<RowKey>,
}

/// What one batch of delivery or not-attempted acks did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AcksApplied {
    /// Rows the acks finalized.
    pub transitioned: usize,
    /// Pending rows another transaction holds (`kafka.replay_shadow_messages` until its
    /// caller commits, an operator's UPDATE): the ack statements skip them instead of
    /// waiting, so these acks were not applied and are applied again by a later tick.
    pub held: Vec<RowKey>,
}

/// Lock the outbox rows of a batch of acks (`$1`-`$3`, their keys) and report what it
/// found: the ctids and keys of the pending rows it locked (one primary-key probe per
/// ack), and the keys of the pending rows another transaction holds (`held_*`: a
/// primary-key probe for each ack it did not lock, usually none). It never waits for a
/// row (`SKIP LOCKED`): the tick waiting for one stopped the DB thread, and a held row
/// failed the whole batch of acks, every topic's. The probes are LATERAL subqueries
/// (cut at one row for `held`), not joins or `EXISTS` the planner could turn into a
/// scan of the outbox, and they name the full primary key and nothing else: with
/// `external_offset IS NULL` in their WHERE the planner took the partial pending index
/// (`topic_id, partition_id, dead, local_offset`), whose key cannot be matched without
/// `dead`, so every probe walked its partition's whole backlog (8 192 acks behind a
/// 250 000-row partition: 30 s, the DB thread stopped meanwhile). A row found delivered
/// is locked but left alone; the `pending` test runs outside the materialized `probed`,
/// where the planner cannot push it back into the probe. The rows are then changed by
/// ctid in a statement of their own ([`ack_update_by_tid`]).
const LOCK_ACK_ROWS_SQL: &str = "
    WITH a AS (SELECT * FROM unnest($1::int4[], $2::int4[], $3::int8[]) AS a(t, p, o)),
    probed AS MATERIALIZED (
        SELECT k.* FROM a CROSS JOIN LATERAL (
            SELECT st.topic_id AS t, st.partition_id AS p, st.local_offset AS o,
                   st.ctid AS row_ctid, st.external_offset IS NULL AS pending
            FROM kafka.shadow_tracking st
            WHERE st.topic_id = a.t AND st.partition_id = a.p AND st.local_offset = a.o
            FOR UPDATE OF st SKIP LOCKED) k
    ),
    lk AS (SELECT t, p, o, row_ctid FROM probed WHERE pending),
    held AS (
        SELECT u.t, u.p, u.o
        FROM (SELECT * FROM a
              WHERE (a.t, a.p, a.o) NOT IN (SELECT lk.t, lk.p, lk.o FROM lk)) u
        CROSS JOIN LATERAL (
            SELECT st.external_offset IS NULL AS pending FROM kafka.shadow_tracking st
            WHERE st.topic_id = u.t AND st.partition_id = u.p AND st.local_offset = u.o
            LIMIT 1) r
        WHERE r.pending
    )
    SELECT ARRAY(SELECT lk.row_ctid::text FROM lk) AS locked_ctid,
           ARRAY(SELECT lk.t FROM lk) AS locked_t,
           ARRAY(SELECT lk.p FROM lk) AS locked_p,
           ARRAY(SELECT lk.o FROM lk) AS locked_o,
           ARRAY(SELECT t FROM held) AS held_t,
           ARRAY(SELECT p FROM held) AS held_p,
           ARRAY(SELECT o FROM held) AS held_o";

/// The planner settings under which an ack update by ctid can only read the outbox by
/// TID (every other scan disabled, TID scans enabled whatever the session says), set
/// transaction-locally; returns the five settings as they were (`saved`), in the order
/// [`RESTORE_ACK_SCAN_SETTINGS_SQL`] puts them back. With the rows named by ctid a TID
/// scan reads exactly them; otherwise the planner, from statistics that lag a large
/// enqueue, chose a hash join over a scan of every outbox row (16 s per ack batch
/// behind a few hundred thousand rows, the DB thread stopped meanwhile). Nested loops are
/// off too: the update joins the TID scan with the acks' values by ctid, and from an
/// estimate of one pending row the planner rescanned the values for every row it
/// updated (8 192 x 8 192 comparisons, 9 s per ack batch); a hash or merge join on the
/// ctid is linear (n log n). Only the
/// UPDATE of the outbox runs under them: a statement that fires a foreign-key check (the
/// `kafka.shadow_metrics` upsert checks `kafka.topics`) would plan that check under them,
/// and Postgres keeps such a plan for the session, so it would scan `kafka.topics` on
/// every later check.
const ACK_SCAN_SETTINGS_SQL: &str = "
    SELECT ARRAY[o.tid, o.seq, o.bitmap, o.idx, o.ios, o.nl] AS saved,
           set_config('enable_tidscan', 'on', true),
           set_config('enable_seqscan', 'off', true),
           set_config('enable_bitmapscan', 'off', true),
           set_config('enable_indexscan', 'off', true),
           set_config('enable_indexonlyscan', 'off', true),
           set_config('enable_nestloop', 'off', true)
    FROM (SELECT current_setting('enable_tidscan') AS tid,
                 current_setting('enable_seqscan') AS seq,
                 current_setting('enable_bitmapscan') AS bitmap,
                 current_setting('enable_indexscan') AS idx,
                 current_setting('enable_indexonlyscan') AS ios,
                 current_setting('enable_nestloop') AS nl
          OFFSET 0) o";

/// Put back what [`ACK_SCAN_SETTINGS_SQL`] read (`$1`, in its order).
const RESTORE_ACK_SCAN_SETTINGS_SQL: &str = "
    SELECT set_config('enable_tidscan', $1[1], true),
           set_config('enable_seqscan', $1[2], true),
           set_config('enable_bitmapscan', $1[3], true),
           set_config('enable_indexscan', $1[4], true),
           set_config('enable_indexonlyscan', $1[5], true),
           set_config('enable_nestloop', $1[6], true)";

/// The rows [`LOCK_ACK_ROWS_SQL`] locked (ctid as text, key) and the keys it found held.
struct LockedAckRows {
    locked: Vec<(String, RowKey)>,
    held: Vec<RowKey>,
}

/// Run [`LOCK_ACK_ROWS_SQL`] for `keys`.
fn lock_ack_rows(client: &mut pgrx::spi::SpiClient<'_>, keys: KeyColumns) -> Result<LockedAckRows> {
    let table = client.update(
        LOCK_ACK_ROWS_SQL,
        Some(1),
        &[
            keys.topic_ids.into(),
            keys.partition_ids.into(),
            keys.local_offsets.into(),
        ],
    )?;
    let row = table.first();
    let ctids: Vec<Option<String>> = row.get_by_name("locked_ctid")?.unwrap_or_default();
    let keys = key_columns(&row, "locked")?;
    Ok(LockedAckRows {
        locked: ctids
            .into_iter()
            .zip(keys)
            .filter_map(|(c, k)| c.map(|c| (c, k)))
            .collect(),
        held: key_columns(&row, "held")?,
    })
}

/// Run `update` (an ack statement whose rows are named by ctid) under
/// [`ACK_SCAN_SETTINGS_SQL`], restored afterwards.
fn ack_update_by_tid<T>(
    client: &mut pgrx::spi::SpiClient<'_>,
    update: impl FnOnce(&mut pgrx::spi::SpiClient<'_>) -> Result<T>,
) -> Result<T> {
    let saved: Vec<Option<String>> = client
        .update(ACK_SCAN_SETTINGS_SQL, Some(1), &[])?
        .first()
        .get_by_name("saved")?
        .unwrap_or_default();
    let out = update(client)?;
    client.update(RESTORE_ACK_SCAN_SETTINGS_SQL, None, &[saved.into()])?;
    Ok(out)
}

/// For each locked row (in order), the payload of the first ack of its key.
fn payloads_of<'a, P>(locked: &[(String, RowKey)], acks: &'a [(RowKey, P)]) -> Vec<&'a P> {
    let by_key: HashMap<RowKey, &P> = acks.iter().rev().map(|(k, p)| (*k, p)).collect();
    locked
        .iter()
        .filter_map(|(_, k)| by_key.get(k).copied())
        .collect()
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

    /// Insert, in the current transaction, a pending outbox row for each pending
    /// (uncommitted) record of the producer's transaction that routing selects: every
    /// record on a topic of `forward_all`, and on a topic of `sampled`
    /// (`(topic_id, forward_percentage)`, below 100) each one [`make_forward_decision`]
    /// forwards. Existing rows are left alone. Returns how many records of each
    /// `(topic_id, partition_id)` were sampled out. Memory stays bounded whatever the
    /// size of the transaction: the `forward_all` records are enqueued by one
    /// `INSERT ... SELECT`, the sampled ones read [`TXN_ENQUEUE_CHUNK`] rows at a time.
    ///
    /// [`make_forward_decision`]: super::routing::make_forward_decision
    fn enqueue_pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        forward_all: &[i32],
        sampled: &[(i32, u8)],
    ) -> Result<HashMap<(i32, i32), u64>>;

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
    /// duplicate ack or an unknown row does not) and the rows another transaction
    /// holds, whose acks were not applied ([`AcksApplied::held`]).
    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<AcksApplied>;

    /// Record failed attempts: `retry_count + 1`, the error, attempt closed,
    /// `forwarded_at` = now (start of the backoff), `dead` = the new `retry_count`
    /// at `max_retries` or above (see `DEAD_LETTER_SYNC_SQL`); counted in
    /// `kafka.shadow_metrics.messages_failed` when `metrics`. Rows another transaction
    /// holds are skipped and returned ([`FailedApplied::held`]).
    fn apply_failed(
        &self,
        rows: &[(RowKey, String)],
        max_retries: i64,
        metrics: bool,
    ) -> Result<FailedApplied>;

    /// Close attempts that never reached the broker (no retry counted). Returns the
    /// rows another transaction holds, whose acks were not applied.
    fn apply_not_attempted(&self, rows: &[RowKey]) -> Result<AcksApplied>;

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

/// The partitions with pending rows (CTE `parts`), found by a skip scan of the
/// pending index: one probe per partition, not one per row. Shared by the claim
/// and [`DEAD_LETTER_SYNC_SQL`]. Ends with the CTE's closing parenthesis.
macro_rules! pending_partitions {
    () => {
        r"
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
        )"
    };
}

/// Make `kafka.shadow_tracking.dead` equal `retry_count >= $1` (the retry cap) on
/// every pending row. `dead` keys both pending indexes, so the claim skips a
/// partition's dead letters by an index condition instead of reading them on every
/// claim.
///
/// The invariant: before every claim, every pending row has `dead = (retry_count >=
/// cap)` for the cap that claim uses. It is kept where `retry_count` or the cap
/// changes, not by a statement per claim (this one costs a skip scan of the
/// partitions with pending rows, 8-12 ms per claim with 2 000 of them):
/// - a failure ack (`apply_failed`) and a lost attempt counted by the claim set
///   `dead` in the UPDATE that raises `retry_count`, with the cap of that tick;
/// - `kafka.replay_shadow_messages` sets `retry_count = 0, dead = false`; new rows
///   start at 0 and false;
/// - this statement runs once when the cap may disagree with the flags: the
///   worker's first claim (an upgraded schema, rows changed while no worker ran),
///   the first claim on a schema found current again (`forget_dead_flags_sync`),
///   a claim whose cap differs from the cap of the last sync (a reload changed
///   `pg_kafka.shadow_max_retries`) or of a failure ack applied since, and a claim
///   after a sync whose tick did not commit (a sync counts only once its tick's
///   transaction committed: `ShadowStore::commit_applied_acks`).
///
/// A hand edit of `retry_count` outside these paths is picked up at the next worker
/// start or cap change: until then a dead letter lowered below the cap by hand (not
/// by replay) is not claimed, and a row raised to the cap by hand is not claimed
/// either (the claim keeps its `retry_count < cap` filter) but is still read.
///
/// It reads only the rows it changes, found by two index ranges per partition of
/// idx_shadow_tracking_retrying (`topic_id, partition_id, dead, retry_count, ...`),
/// whatever the number of dead letters. The rows are locked and updated by ctid (TID
/// scans), never by a join the planner could turn into a scan of the table.
///
/// It skips the stale rows another transaction holds and returns how many it skipped
/// (`skipped`); a sync that skipped any does not count as done, so the next claim runs
/// it again. Until then such a row is only delayed: a stale `dead = true` keeps a live
/// row out of the claim, and a stale `dead = false` row at the cap is still filtered by
/// the claim's `retry_count` test.
const DEAD_LETTER_SYNC_SQL: &str = concat!(
    r"
        WITH RECURSIVE",
    pending_partitions!(),
    r",
        stale AS MATERIALIZED (
            SELECT s.ctid
            FROM parts p
            CROSS JOIN LATERAL (
                SELECT st.ctid
                FROM kafka.shadow_tracking st
                WHERE st.topic_id = p.topic_id
                  AND st.partition_id = p.partition_id
                  AND st.external_offset IS NULL
                  AND st.dead
                  AND st.retry_count < $1
                UNION ALL
                SELECT st.ctid
                FROM kafka.shadow_tracking st
                WHERE st.topic_id = p.topic_id
                  AND st.partition_id = p.partition_id
                  AND st.external_offset IS NULL
                  AND NOT st.dead
                  AND st.retry_count > 0
                  AND st.retry_count >= $1
            ) s
        ),
        -- The stale rows no other transaction holds (a replay re-queuing them, an
        -- operator's UPDATE): the sync never waits for a row; one it skips keeps its
        -- flag, which only delays the row (see below), and the sync runs again.
        locked AS MATERIALIZED (
            SELECT st.ctid
            FROM kafka.shadow_tracking st
            WHERE st.ctid = ANY (ARRAY(SELECT stale.ctid FROM stale))
            FOR UPDATE OF st SKIP LOCKED
        ),
        synced AS (
            UPDATE kafka.shadow_tracking st
            SET dead = st.retry_count >= $1
            WHERE st.ctid = ANY (ARRAY(SELECT locked.ctid FROM locked))
              AND st.external_offset IS NULL
              AND st.dead IS DISTINCT FROM (st.retry_count >= $1)
            RETURNING 1
        )
        SELECT (SELECT COUNT(*) FROM stale) - (SELECT COUNT(*) FROM locked) AS skipped
        "
);

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
///
/// Every condition that bounds the scan is an index condition of
/// `idx_shadow_tracking_pending` (`topic_id, partition_id, dead, local_offset`), so
/// the scan starts at the partition's first live row and stops at its first blocked
/// one: `NOT st.dead` skips the dead letters (`dead` equals `retry_count >= $2` before
/// every claim, see [`DEAD_LETTER_SYNC_SQL`]; the `retry_count` test stays as a filter), and the
/// blocked bound is a `COALESCE` with the largest `int8` (`local_offset` is NOT NULL,
/// so it means `b.first_blocked IS NULL OR st.local_offset < b.first_blocked`). Written
/// as that `OR` it was a filter: behind a backing-off head the probe walked the
/// partition's whole backlog on every claim and found nothing (a 100-row claim on PG17
/// took 20-28 ms beside 150 000 rows behind a backing-off head, 1.5-2 ms with the index
/// condition, linear in the backlog); dead letters were walked the same way (52-78 ms
/// behind 100 000 dead letters at a partition's head, 2-6 ms skipping them).
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
                          AND NOT st.dead
                          AND st.retry_count < $2
                          AND st.local_offset < COALESCE(b.first_blocked, 9223372036854775807)
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
        -- The partitions with pending rows.",
    pending_partitions!(),
    r",
        -- The first backing-off row of each partition: it failed, its
        -- attempt is closed and its backoff has not passed. Only the
        -- partition's failed live rows are looked at (the index range
        -- (topic_id, partition_id, NOT dead, retry_count 1 .. $2 - 1) of
        -- idx_shadow_tracking_retrying), so this grows neither with the
        -- backlog nor with the dead letters.
        blocked AS MATERIALIZED (
            SELECT p.topic_id, p.partition_id, fb.first_blocked
            FROM parts p
            CROSS JOIN LATERAL (
                SELECT MIN(st.local_offset) AS first_blocked
                FROM kafka.shadow_tracking st
                WHERE st.topic_id = p.topic_id
                  AND st.partition_id = p.partition_id
                  AND st.external_offset IS NULL
                  AND NOT st.dead
                  AND st.retry_count > 0
                  AND st.retry_count < $2
                  AND NOT st.attempt_open
                  AND st.forwarded_at > NOW() - make_interval(secs =>
                      LEAST($6::float8 * power(2::float8, LEAST(st.retry_count - 1, 30)),
                            $7::float8) / 1000.0)
                  AND (st.topic_id, st.partition_id, st.local_offset)
                      NOT IN (SELECT inf.t, inf.p, inf.o FROM inf)
            ) fb
            WHERE fb.first_blocked IS NOT NULL
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
        cand AS MATERIALIZED (
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
        -- The candidates no other transaction holds, locked now: one primary-key
        -- probe each, skipping a held row instead of waiting for it. Another
        -- session holds outbox rows across statements (kafka.replay_shadow_messages
        -- until its caller commits, an operator's UPDATE), and the claim waiting
        -- for one stopped forwarding for every partition, the DB thread with it.
        locked AS MATERIALIZED (
            SELECT k.topic_id, k.partition_id, k.local_offset
            FROM cand c
            CROSS JOIN LATERAL (
                SELECT st.topic_id, st.partition_id, st.local_offset
                FROM kafka.shadow_tracking st
                WHERE st.topic_id = c.topic_id
                  AND st.partition_id = c.partition_id
                  AND st.local_offset = c.local_offset
                FOR UPDATE OF st SKIP LOCKED
            ) k
        ),
        -- Each partition's first held candidate: the claim takes the partition's
        -- candidates below it only, so its records still go out in offset order;
        -- the held row and the rows after it wait for a later claim. The other
        -- partitions are claimed as if nothing were held.
        held AS MATERIALIZED (
            SELECT c.topic_id, c.partition_id, MIN(c.local_offset) AS first_held
            FROM cand c
            WHERE (c.topic_id, c.partition_id, c.local_offset)
                  NOT IN (SELECT l.topic_id, l.partition_id, l.local_offset FROM locked l)
            GROUP BY c.topic_id, c.partition_id
        ),
        due AS (
            SELECT c.topic_id, c.partition_id, c.local_offset
            FROM cand c
            LEFT JOIN held h
              ON h.topic_id = c.topic_id AND h.partition_id = c.partition_id
            WHERE c.local_offset < COALESCE(h.first_held, 9223372036854775807)
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
                  AND NOT lo.dead
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
                retry_count = st.retry_count + CASE WHEN st.attempt_open THEN 1 ELSE 0 END,
                -- Keeps dead = (retry_count >= cap) (see DEAD_LETTER_SYNC_SQL).
                dead = st.retry_count + CASE WHEN st.attempt_open THEN 1 ELSE 0 END >= $2
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
    /// Run [`DEAD_LETTER_SYNC_SQL`] if the claim asks for it, then
    /// [`CLAIM_DUE_OUTBOX_ROWS_SQL`] (parameters in [`ShadowDb::claim_due_outbox_rows`]).
    fn run_claim(
        client: &mut pgrx::spi::SpiClient<'_>,
        claim: &OutboxClaim,
        backoff_base_ms: f64,
        backoff_max_ms: f64,
    ) -> Result<ClaimedRows> {
        let mut synced = false;
        if claim.sync_dead_flags {
            // A statement of its own: the claim's CTEs would read `dead` as it was
            // before an UPDATE in the same statement.
            let skipped: i64 = client
                .update(DEAD_LETTER_SYNC_SQL, Some(1), &[claim.max_retries.into()])?
                .first()
                .get_by_name("skipped")?
                .unwrap_or(0);
            synced = skipped == 0;
        }
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

        let mut claimed = ClaimedRows {
            dead_flags_synced: synced,
            ..ClaimedRows::default()
        };
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
            // row exists, so a pointer can never outlive a write that was not
            // made: the live-produce enqueue runs in the same subtransaction as
            // the message insert, so the row is always visible there. One
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

    fn enqueue_pending_txn_records(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        forward_all: &[i32],
        sampled: &[(i32, u8)],
    ) -> Result<HashMap<(i32, i32), u64>> {
        use super::routing::{make_forward_decision, ForwardDecision};
        use pgrx::prelude::*;
        // Both statements read the producer's pending rows through
        // idx_messages_txn_pending_producer, then keep the listed topics': no other
        // producer's rows and no committed row are read. No ORDER BY: the outbox claim
        // orders the rows.
        if !forward_all.is_empty() {
            Spi::connect_mut(|client| {
                client.update(
                    "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset) \
                     SELECT m.topic_id, m.partition_id, m.partition_offset \
                     FROM kafka.messages m \
                     WHERE m.topic_id = ANY($3::int4[]) AND m.txn_state = 'pending' \
                       AND m.producer_id = $1 AND m.producer_epoch = $2 \
                     ON CONFLICT (topic_id, partition_id, local_offset) DO NOTHING",
                    None,
                    &[
                        producer_id.into(),
                        producer_epoch.into(),
                        forward_all.to_vec().into(),
                    ],
                )?;
                Ok::<(), spi::SpiError>(())
            })?;
        }
        let mut skipped: HashMap<(i32, i32), u64> = HashMap::new();
        if sampled.is_empty() {
            return Ok(skipped);
        }
        // A sampled topic's decision needs the record key's murmur2 hash, so its
        // records are routed here, a chunk at a time through a cursor. Each chunk is
        // fetched and inserted in its own SPI connection, whose end frees that
        // chunk's tuple table.
        let percentage: HashMap<i32, u8> = sampled.iter().copied().collect();
        let topic_ids: Vec<i32> = sampled.iter().map(|&(topic_id, _)| topic_id).collect();
        let cursor = Spi::connect_mut(|client| {
            let cursor = client.try_open_cursor_mut(
                "SELECT m.topic_id, m.partition_id, m.partition_offset, m.key \
                 FROM kafka.messages m \
                 WHERE m.topic_id = ANY($3::int4[]) AND m.txn_state = 'pending' \
                   AND m.producer_id = $1 AND m.producer_epoch = $2",
                &[producer_id.into(), producer_epoch.into(), topic_ids.into()],
            )?;
            Ok::<String, spi::SpiError>(cursor.detach_into_name())
        })?;
        loop {
            let fetched = Spi::connect_mut(|client| {
                let mut cursor = client.find_cursor(&cursor)?;
                let mut rows: Vec<RowKey> = Vec::new();
                let mut fetched = 0usize;
                for row in cursor.fetch(TXN_ENQUEUE_CHUNK)? {
                    fetched += 1;
                    let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                    let key = RowKey::new(
                        topic_id,
                        row.get_by_name("partition_id")?.unwrap_or(0),
                        row.get_by_name("partition_offset")?.unwrap_or(0),
                    );
                    let record_key: Option<Vec<u8>> = row.get_by_name("key")?;
                    let pct = percentage.get(&topic_id).copied().unwrap_or(0);
                    match make_forward_decision(record_key.as_deref(), key.local_offset, pct) {
                        ForwardDecision::Forward => rows.push(key),
                        ForwardDecision::Skip => {
                            *skipped.entry(key.partition()).or_default() += 1;
                        }
                    }
                }
                // Keep the cursor open for the next chunk (dropping it closes it).
                cursor.detach_into_name();
                if !rows.is_empty() {
                    let keys: KeyColumns = rows.iter().collect();
                    client.update(
                        "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset) \
                         SELECT * FROM unnest($1::int4[], $2::int4[], $3::int8[]) \
                         ON CONFLICT (topic_id, partition_id, local_offset) DO NOTHING",
                        None,
                        &[
                            keys.topic_ids.into(),
                            keys.partition_ids.into(),
                            keys.local_offsets.into(),
                        ],
                    )?;
                }
                Ok::<usize, spi::SpiError>(fetched)
            })?;
            if fetched == 0 {
                break;
            }
        }
        Spi::connect(|client| {
            drop(client.find_cursor(&cursor)?);
            Ok::<(), spi::SpiError>(())
        })?;
        Ok(skipped)
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

    fn apply_delivered(&self, rows: &[(RowKey, i64)], metrics: bool) -> Result<AcksApplied> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(AcksApplied::default());
        }
        // The update is guarded by `external_offset IS NULL`, so a duplicate ack updates
        // nothing and counts nothing: the metric counts exactly the rows it returns.
        Spi::connect_mut(|client| {
            let locked = lock_ack_rows(client, rows.iter().map(|(k, _)| k).collect())?;
            if locked.locked.is_empty() {
                return Ok(AcksApplied {
                    transitioned: 0,
                    held: locked.held,
                });
            }
            let ctids: Vec<&str> = locked.locked.iter().map(|(c, _)| c.as_str()).collect();
            let external: Vec<i64> = payloads_of(&locked.locked, rows)
                .into_iter()
                .copied()
                .collect();
            let done = ack_update_by_tid(client, |client| {
                returned_keys(client.update(
                    "UPDATE kafka.shadow_tracking st \
                     SET external_offset = u.x, forwarded_at = NOW(), error_message = NULL, \
                         attempt_open = false \
                     FROM unnest($1::text[]::tid[], $2::int8[]) AS u(c, x) \
                     WHERE st.ctid = ANY ($1::text[]::tid[]) AND st.ctid = u.c \
                       AND st.external_offset IS NULL \
                     RETURNING st.topic_id, st.partition_id, st.local_offset",
                    None,
                    &[ctids.into(), external.into()],
                )?)
            })?;
            if metrics && !done.is_empty() {
                let keys: KeyColumns = done.iter().collect();
                client.update(
                    "INSERT INTO kafka.shadow_metrics \
                         (topic_id, partition_id, messages_forwarded, last_forwarded_offset, last_forwarded_at) \
                     SELECT t, p, COUNT(*), MAX(o), NOW() \
                     FROM unnest($1::int4[], $2::int4[], $3::int8[]) AS d(t, p, o) \
                     GROUP BY t, p \
                     ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                        messages_forwarded = kafka.shadow_metrics.messages_forwarded + EXCLUDED.messages_forwarded, \
                        last_forwarded_offset = GREATEST(kafka.shadow_metrics.last_forwarded_offset, EXCLUDED.last_forwarded_offset), \
                        last_forwarded_at = NOW()",
                    None,
                    &[
                        keys.topic_ids.into(),
                        keys.partition_ids.into(),
                        keys.local_offsets.into(),
                    ],
                )?;
            }
            Ok(AcksApplied {
                transitioned: done.len(),
                held: locked.held,
            })
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
        Spi::connect_mut(|client| {
            let locked = lock_ack_rows(client, rows.iter().map(|(k, _)| k).collect())?;
            let mut out = FailedApplied {
                held: locked.held,
                ..FailedApplied::default()
            };
            if locked.locked.is_empty() {
                return Ok(out);
            }
            let ctids: Vec<&str> = locked.locked.iter().map(|(c, _)| c.as_str()).collect();
            let errors: Vec<&str> = payloads_of(&locked.locked, rows)
                .into_iter()
                .map(String::as_str)
                .collect();
            let mut failed_keys = Vec::new();
            ack_update_by_tid(client, |client| {
                let table = client.update(
                    "UPDATE kafka.shadow_tracking st \
                     SET retry_count = st.retry_count + 1, error_message = u.e, \
                         forwarded_at = NOW(), attempt_open = false, \
                         dead = st.retry_count + 1 >= $3 \
                     FROM unnest($1::text[]::tid[], $2::text[]) AS u(c, e) \
                     WHERE st.ctid = ANY ($1::text[]::tid[]) AND st.ctid = u.c \
                       AND st.external_offset IS NULL \
                     RETURNING st.topic_id, st.partition_id, st.local_offset, st.retry_count, \
                               st.error_message",
                    None,
                    &[ctids.into(), errors.into(), max_retries.into()],
                )?;
                for row in table {
                    let key = RowKey::new(
                        row.get_by_name("topic_id")?.unwrap_or(0),
                        row.get_by_name("partition_id")?.unwrap_or(0),
                        row.get_by_name("local_offset")?.unwrap_or(0),
                    );
                    failed_keys.push(key);
                    let retry_count: i32 = row.get_by_name("retry_count")?.unwrap_or(0);
                    if i64::from(retry_count) >= max_retries {
                        out.dead_lettered
                            .push((key, row.get_by_name("error_message")?.unwrap_or_default()));
                    }
                }
                Ok(())
            })?;
            out.transitioned = failed_keys.len();
            if metrics && !failed_keys.is_empty() {
                let keys: KeyColumns = failed_keys.iter().collect();
                client.update(
                    "INSERT INTO kafka.shadow_metrics (topic_id, partition_id, messages_failed) \
                     SELECT t, p, COUNT(*) FROM unnest($1::int4[], $2::int4[]) AS d(t, p) \
                     GROUP BY t, p \
                     ON CONFLICT (topic_id, partition_id) DO UPDATE SET \
                        messages_failed = kafka.shadow_metrics.messages_failed + EXCLUDED.messages_failed",
                    None,
                    &[keys.topic_ids.into(), keys.partition_ids.into()],
                )?;
            }
            Ok(out)
        })
    }

    fn apply_not_attempted(&self, rows: &[RowKey]) -> Result<AcksApplied> {
        use pgrx::prelude::*;
        if rows.is_empty() {
            return Ok(AcksApplied::default());
        }
        Spi::connect_mut(|client| {
            let locked = lock_ack_rows(client, rows.iter().collect())?;
            if locked.locked.is_empty() {
                return Ok(AcksApplied {
                    transitioned: 0,
                    held: locked.held,
                });
            }
            let ctids: Vec<&str> = locked.locked.iter().map(|(c, _)| c.as_str()).collect();
            let n = ack_update_by_tid(client, |client| {
                let table = client.update(
                    "WITH closed AS ( \
                         UPDATE kafka.shadow_tracking st SET attempt_open = false \
                         WHERE st.ctid = ANY ($1::text[]::tid[]) \
                           AND st.external_offset IS NULL \
                         RETURNING 1 \
                     ) \
                     SELECT COUNT(*)::int8 AS n FROM closed",
                    Some(1),
                    &[ctids.into()],
                )?;
                Ok(table.first().get_by_name::<i64, _>("n")?.unwrap_or(0))
            })?;
            Ok(AcksApplied {
                transitioned: usize::try_from(n).unwrap_or(0),
                held: locked.held,
            })
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

/// The `(topic_id, partition_id, local_offset)` of every row a statement returned.
fn returned_keys(table: pgrx::spi::SpiTupleTable<'_>) -> Result<Vec<RowKey>> {
    let mut keys = Vec::with_capacity(table.len());
    for row in table {
        keys.push(RowKey::new(
            row.get_by_name("topic_id")?.unwrap_or(0),
            row.get_by_name("partition_id")?.unwrap_or(0),
            row.get_by_name("local_offset")?.unwrap_or(0),
        ));
    }
    Ok(keys)
}

/// The `<prefix>_t`, `<prefix>_p`, `<prefix>_o` key columns of a result row.
fn key_columns(row: &pgrx::spi::SpiTupleTable<'_>, prefix: &str) -> Result<Vec<RowKey>> {
    let t: Vec<Option<i32>> = row.get_by_name(format!("{prefix}_t"))?.unwrap_or_default();
    let p: Vec<Option<i32>> = row.get_by_name(format!("{prefix}_p"))?.unwrap_or_default();
    let o: Vec<Option<i64>> = row.get_by_name(format!("{prefix}_o"))?.unwrap_or_default();
    Ok(t.into_iter()
        .zip(p)
        .zip(o)
        .map(|((t, p), o)| RowKey::new(t.unwrap_or(0), p.unwrap_or(0), o.unwrap_or(0)))
        .collect())
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

    /// What bounds a probe of a partition's pending rows is an index condition of
    /// `(topic_id, partition_id, dead, local_offset)`, never a filter: written as
    /// `b.first_blocked IS NULL OR st.local_offset < b.first_blocked`, the blocked bound
    /// was a filter and a backing-off head made every claim read the partition's whole
    /// backlog; without `NOT dead` every claim read the dead letters (the E2E
    /// `test_shadow_worker_stays_idle_beside_a_blocked_partition` and
    /// `..._beside_dead_letters` measure both).
    #[test]
    fn the_claim_skips_blocked_rows_and_dead_letters_by_index_conditions() {
        let squash = |sql: &str| {
            sql.lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join(" ")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        };
        let due = squash(due_rows_of_partition!());
        assert!(
            due.contains("AND NOT st.dead AND st.retry_count < $2"),
            "{due}"
        );
        assert_eq!(
            "9223372036854775807",
            i64::MAX.to_string(),
            "the COALESCE bound is the largest local_offset"
        );
        assert!(
            due.contains("AND st.local_offset < COALESCE(b.first_blocked, 9223372036854775807)"),
            "{due}"
        );
        assert!(!due.contains(" OR "), "no OR in the probe's bounds: {due}");
        let claim = squash(CLAIM_DUE_OUTBOX_ROWS_SQL);
        let section = |from: &str, to: &str| {
            let start = claim.find(from).unwrap();
            claim[start..start + claim[start..].find(to).unwrap()].to_string()
        };
        let blocked = section("blocked AS MATERIALIZED (", "due_parts AS");
        assert!(
            blocked.contains("FROM parts p CROSS JOIN LATERAL"),
            "{blocked}"
        );
        assert!(
            blocked.contains("AND NOT st.dead AND st.retry_count > 0 AND st.retry_count < $2"),
            "{blocked}"
        );
        let heads = section("heads AS MATERIALIZED (", "claimed AS");
        assert!(
            heads.contains("AND NOT lo.dead AND lo.retry_count < $2"),
            "{heads}"
        );
        // The claim and the dead-flag sync walk the same partitions.
        let parts = squash(pending_partitions!());
        assert!(claim.contains(&parts));
        let sync = squash(DEAD_LETTER_SYNC_SQL);
        assert!(sync.contains(&parts));
    }

    /// The full sync makes `dead` equal `retry_count >= cap` on every pending row
    /// whose flag is wrong, reading only those rows (an index range each way per
    /// partition) and updating them by ctid.
    #[test]
    fn the_dead_flag_sync_reads_only_the_rows_it_changes() {
        let sync = DEAD_LETTER_SYNC_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        // Dead letters a raised cap revives (retry_count 0 too: a hand edit or replay).
        assert!(sync.contains("AND st.dead AND st.retry_count < $1 UNION ALL"));
        // Rows at a (lowered) cap; retry_count > 0 matches the retrying index predicate.
        assert!(sync.contains("AND NOT st.dead AND st.retry_count > 0 AND st.retry_count >= $1 )"));
        assert!(sync.contains("SET dead = st.retry_count >= $1"));
        assert!(sync.contains("WHERE st.ctid = ANY (ARRAY(SELECT stale.ctid FROM stale))"));
        assert!(sync.contains("AND st.dead IS DISTINCT FROM (st.retry_count >= $1)"));
        assert_eq!(sync.matches("st.external_offset IS NULL").count(), 5);
    }

    /// Between full syncs, the claim keeps `dead` itself where it raises
    /// `retry_count` (a lost attempt), with the claim's cap.
    #[test]
    fn the_claim_keeps_the_dead_flag_of_a_lost_attempt() {
        let claim = CLAIM_DUE_OUTBOX_ROWS_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(claim.contains(
            "retry_count = st.retry_count + CASE WHEN st.attempt_open THEN 1 ELSE 0 END, \
             -- Keeps dead = (retry_count >= cap) (see DEAD_LETTER_SYNC_SQL). \
             dead = st.retry_count + CASE WHEN st.attempt_open THEN 1 ELSE 0 END >= $2"
        ));
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
