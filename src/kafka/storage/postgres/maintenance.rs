//! Background maintenance SQL: the retention sweep, time-partition maintenance,
//! timed-out transactions and aborted-row cleanup.
//!
//! The sweep's decisions (which window applies, what to do with a leaf, when to stop)
//! are pure functions in [`retention`]; this module runs the statements they call for.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::producers::{bump_producer_epoch, reset_producer_sequences};
use super::{internal, PostgresStore};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::retention::{
    self, LeafAction, MaintenanceLockWait, TimePartition, MAINTENANCE_LOCK_KEY,
    MAX_CONTENTS_PROBES_PER_PASS, MAX_RETENTION_WINDOW_MS, RETENTION_DELETE_BATCH,
};
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

// Only the messages-table retention (the policy an operator tunes) is a GUC,
// `pg_kafka.message_retention_hours`; the auxiliary windows below are constants that
// match Kafka's own defaults where one exists (DR-1/DR-2, DEEP-REVIEW-2026-07).

/// Grace period before physically deleting `txn_state='aborted'` rows. Aborted rows
/// are already invisible to consumers; the grace only avoids churning rows a
/// concurrent diagnostic query might be looking at.
pub const ABORTED_MESSAGE_GRACE: Duration = Duration::from_secs(60);
/// Idle window after which a producer id (and its sequences) is reclaimed.
/// Kafka: `transactional.id.expiration.ms` defaults to 7 days.
const PRODUCER_ID_RETENTION: Duration = Duration::from_hours(168);
/// Age after which terminal (`CompleteCommit`/`CompleteAbort`) transaction rows are
/// deleted. Matches [`PRODUCER_ID_RETENTION`] so a transaction row never outlives its
/// producer.
const TERMINAL_TXN_RETENTION: Duration = PRODUCER_ID_RETENTION;
/// Age after which successfully forwarded shadow-outbox rows are deleted.
const SHADOW_DELIVERED_RETENTION: Duration = Duration::from_hours(24);

/// The sweep's pass budget ([`retention::RETENTION_PASS_BUDGET`], or the test-only
/// `pg_kafka.test_retention_pass_budget_ms`).
fn pass_budget() -> Duration {
    retention::pass_budget(crate::config::TEST_RETENTION_PASS_BUDGET_MS.get())
}

/// Row counts deleted by one retention sweep (see [`PostgresStore::run_retention_sweep`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSweepStats {
    pub aborted_messages: u64,
    pub expired_messages: u64,
    /// Whole time partitions (leaves of kafka.messages) dropped by retention.
    pub dropped_partitions: u64,
    pub stale_producers: u64,
    pub terminal_transactions: u64,
    pub shadow_delivered_rows: u64,
    /// Shadow-outbox rows below their partition's log start: their message is gone
    /// (retention, DeleteRecords), so they can never be forwarded.
    pub shadow_orphaned_rows: u64,
}

impl RetentionSweepStats {
    /// Every row (and leaf) the sweep removed.
    pub fn total(&self) -> u64 {
        self.aborted_messages
            + self.expired_messages
            + self.dropped_partitions
            + self.stale_producers
            + self.terminal_transactions
            + self.shadow_delivered_rows
            + self.shadow_orphaned_rows
    }
}

/// A leaf of kafka.messages as the sweep sees it: the pure-planning view plus its
/// bounds as SQL text. The bounds travel back into SQL as text: same session, same
/// DateStyle, so the timestamptz round-trips exactly (to the microsecond).
struct Leaf {
    partition: TimePartition,
    lower_txt: Option<String>,
    upper_txt: Option<String>,
}

/// What one batch of row deletes removes from its leaf.
#[derive(Debug, Clone, Copy)]
enum BatchFilter {
    /// `topic_id`'s rows older than `window_ms`, except pending transactional rows.
    Expired { topic_id: i32, window_ms: i64 },
    /// Aborted transactional rows older than `grace_ms`, of every topic.
    Aborted { grace_ms: i64 },
}

impl BatchFilter {
    /// The batch's row predicate on the alias `m`; `$1` is the topic, `$2` the age in
    /// milliseconds (`$1` is unused by [`BatchFilter::Aborted`]). The aborted predicate
    /// matches the partial index idx_messages_txn_aborted (created_at) WHERE txn_state =
    /// 'aborted', so a leaf with no aborted rows costs one probe of an empty index.
    fn predicate(self) -> &'static str {
        match self {
            BatchFilter::Expired { .. } => {
                "m.topic_id = $1
                 AND m.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                 AND (m.txn_state IS NULL OR m.txn_state <> 'pending')"
            }
            BatchFilter::Aborted { .. } => {
                "m.txn_state = 'aborted'
                 AND m.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'"
            }
        }
    }

    /// `($1, $2)` for [`BatchFilter::predicate`].
    fn args(self) -> (i32, i64) {
        match self {
            BatchFilter::Expired {
                topic_id,
                window_ms,
            } => (topic_id, window_ms),
            BatchFilter::Aborted { grace_ms } => (0, grace_ms),
        }
    }
}

/// One unit of row-level expiry: delete `topic_id`'s rows older than `window_ms` in
/// the leaf `leaf_oid`, whose [`retention::leaf_key`] is `leaf_key`.
#[derive(Debug, Clone, Copy)]
struct ExpiryWork {
    topic_id: i32,
    window_ms: i64,
    leaf_oid: u32,
    leaf_key: i64,
}

impl ExpiryWork {
    /// The item's identity for [`retention::EXPIRY_ROTATION`].
    fn key(&self) -> (i32, i64) {
        (self.topic_id, self.leaf_key)
    }
}

/// What one sweep pass does with the messages: whole-leaf drops and row deletes.
#[derive(Default)]
struct ExpiryPlan {
    /// Leaves to drop, with the topics found wholly expired in each (the drop function
    /// re-checks them under its lock).
    drops: Vec<(u32, Vec<i32>)>,
    /// Row deletes, sorted by [`ExpiryWork::key`] (one item per key).
    work: Vec<ExpiryWork>,
}

/// Every topic's effective retention window.
struct TopicWindows {
    /// `None`: kept forever.
    by_topic: HashMap<i32, Option<i64>>,
    /// The topics with a window.
    finite: Vec<(i32, i64)>,
}

impl TopicWindows {
    fn window(&self, topic_id: i32) -> Option<i64> {
        self.by_topic.get(&topic_id).copied().flatten()
    }
}

/// The partitions a sweep removed rows from, each with the offset after the last one
/// it removed: their log start must advance, and their offset counter must stay past
/// the removed offsets (a partition written without a `kafka.partition_offsets` row
/// would otherwise restart at 0 once retention empties it).
#[derive(Debug, Default)]
struct AffectedPartitions(HashMap<(i32, i32), i64>);

impl AffectedPartitions {
    /// Record that offsets before `next_offset` were removed from the partition.
    fn removed(&mut self, topic_id: i32, partition_id: i32, next_offset: i64) {
        let entry = self
            .0
            .entry((topic_id, partition_id))
            .or_insert(next_offset);
        *entry = (*entry).max(next_offset);
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Parallel `(topic_ids, partition_ids, next_offsets)` columns for unnest().
    fn into_columns(self) -> (Vec<i32>, Vec<i32>, Vec<i64>) {
        let mut topic_ids = Vec::with_capacity(self.0.len());
        let mut partition_ids = Vec::with_capacity(self.0.len());
        let mut next_offsets = Vec::with_capacity(self.0.len());
        for ((topic_id, partition_id), next_offset) in self.0 {
            topic_ids.push(topic_id);
            partition_ids.push(partition_id);
            next_offsets.push(next_offset);
        }
        (topic_ids, partition_ids, next_offsets)
    }
}

/// A whole number of seconds for SQL interval arithmetic.
fn whole_seconds(d: Duration) -> i64 {
    i64::try_from(d.as_secs()).unwrap_or(i64::MAX)
}

/// Row count of a `DELETE ... RETURNING` result.
fn row_count(rows: usize) -> u64 {
    u64::try_from(rows).unwrap_or(u64::MAX)
}

impl PostgresStore {
    /// One pass of the storage-lifecycle sweep (DR-1/DR-2, DEEP-REVIEW-2026-07): the
    /// worker calls it every retention interval, and `pg_kafka_run_retention_sweep()`
    /// runs the same pass on demand. It deletes aborted rows past `aborted_grace`,
    /// expired messages, stale producer ids and sequences, terminal transaction rows,
    /// shadow-outbox rows whose message is gone, and delivered shadow-outbox rows.
    ///
    /// Cost: the pass runs on the single worker DB thread, whatever the retention
    /// settings (with `message_retention_hours = 0` too), so its cost must not grow
    /// with the stored data when there is little to do. Every step is index-driven
    /// (the aborted rows through the partial index idx_messages_txn_aborted, the
    /// delivered and orphaned outbox rows through idx_shadow_tracking_delivered and the
    /// primary key), runs bounded
    /// statements ([`RETENTION_DELETE_BATCH`] rows, one leaf), and stops between
    /// statements once the pass has spent [`retention::RETENTION_PASS_BUDGET`] (after at
    /// least one statement per step, so no step starves; the steps before row expiry
    /// get at most a third of it each, so their backlogs cannot crowd expiry out), each
    /// step resuming at the next pass where this one left it. The full contents probe of a leaf runs only where some topic can be
    /// wholly expired, at most [`MAX_CONTENTS_PROBES_PER_PASS`] per pass; the other
    /// past leaves get one emptiness check (a single statement for all of them).
    ///
    /// Expired messages (ADR-001: kafka.messages is time-partitioned):
    /// * a past time leaf whose every topic is wholly expired (the leaf's upper bound
    ///   is older than the topic's window) and that holds no pending transactional row
    ///   is DROPPED whole (`kafka.drop_message_partition`, which re-checks under its
    ///   lock);
    /// * a past leaf that must stay (a topic with a longer or infinite window, or a
    ///   pending row) gets the bounded row delete for its wholly expired topics;
    /// * a topic whose window is shorter than one leaf (`partition_minutes`) is
    ///   expired row by row everywhere; longer windows are enforced a leaf at a time
    ///   (Kafka segment semantics, see [`retention::row_level_expiry`]), and rows still
    ///   in the DEFAULT leaf are always expired row by row.
    ///
    /// `message_retention_hours` <= 0 disables the global window (per-topic
    /// `retention.ms` still applies); pending transactional rows are never deleted
    /// regardless of age.
    ///
    /// Offset monotonicity: neither deletes nor drops touch
    /// `kafka.partition_offsets.next_offset`, and every offset producer/read path
    /// takes `GREATEST(next_offset, MAX+1)` (BUG-3), so removing rows cannot cause
    /// offset reuse or a high-watermark regression. Both advance `log_start_offset`
    /// (forward only) for every partition they touched, so the reported earliest
    /// offset tracks retention. Consumers positioned before a retention cutoff get a
    /// standard Kafka out-of-range reset, as with a real broker's retention.
    ///
    /// Concurrency: the whole pass runs under the partition-maintenance lock
    /// ([`MAINTENANCE_LOCK_KEY`]), taken before its first statement, so sweeps (the
    /// worker's and on-demand ones) and partition maintenance never overlap. Without it
    /// one sweep, holding ROW EXCLUSIVE on kafka.messages and the row locks of its
    /// deletes, waits for ACCESS EXCLUSIVE to drop a leaf, while another, holding ROW
    /// EXCLUSIVE too, waits for those rows or for the lock: a deadlock. `lock` says
    /// whether to queue for the lock or, if another session holds it, skip the pass
    /// (`Ok(None)`).
    pub fn run_retention_sweep(
        &self,
        message_retention_hours: i32,
        aborted_grace: Duration,
        partition_minutes: i32,
        lock: MaintenanceLockWait,
    ) -> Result<Option<RetentionSweepStats>> {
        if !Self::take_maintenance_lock(lock)? {
            return Ok(None);
        }
        let pass_start = Instant::now();
        let width_ms = retention::partition_width_ms(partition_minutes);

        Spi::connect_mut(|client| {
            let windows = load_topic_windows(client, message_retention_hours)?;
            let now_ms = database_now_ms(client)?;
            let partitioned = has_time_partitions(client)?;
            let leaves = list_leaves(client, partitioned)?;
            let default_has_rows = default_leaf_has_rows(client, partitioned)?;
            let mut stats = RetentionSweepStats {
                aborted_messages: delete_aborted_rows(
                    client,
                    &leaves,
                    partitioned,
                    now_ms,
                    aborted_grace,
                    pass_start,
                )?,
                ..Default::default()
            };
            let plan = plan_expiry(
                client,
                &leaves,
                &windows,
                ExpiryClock {
                    now_ms,
                    width_ms,
                    pass_start,
                },
                default_has_rows,
            )?;

            let mut affected = AffectedPartitions::default();
            stats.expired_messages = delete_expired_rows(
                client,
                &plan.work,
                &leaves,
                partitioned,
                &mut affected,
                pass_start,
            )?;
            // Whole-leaf drops last, so the ACCESS EXCLUSIVE lock a drop takes on
            // kafka.messages is held only until this transaction commits.
            stats.dropped_partitions = drop_leaves(client, plan.drops, &mut affected, pass_start)?;
            advance_log_start(client, affected)?;

            stats.terminal_transactions = delete_terminal_transactions(client)?;
            stats.stale_producers = delete_stale_producers(client)?;
            // After advance_log_start, so the outbox rows of the messages this pass
            // removed go in the same pass.
            stats.shadow_orphaned_rows = delete_orphaned_shadow_rows(client, pass_start)?;
            stats.shadow_delivered_rows = delete_delivered_shadow_rows(client, pass_start)?;
            Ok(Some(stats))
        })
        .map_err(internal("run_retention_sweep"))
    }

    /// Take the partition-maintenance lock for this transaction ([`MaintenanceLockWait`]);
    /// returns whether it is held.
    fn take_maintenance_lock(lock: MaintenanceLockWait) -> Result<bool> {
        Spi::connect(|client| {
            let row = client.select(lock.lock_sql(), Some(1), &[MAINTENANCE_LOCK_KEY.into()])?;
            Ok(row
                .first()
                .get_by_name::<bool, _>("locked")?
                .unwrap_or(false))
        })
        .map_err(internal("take_maintenance_lock"))
    }

    /// ADR-001 partition maintenance: make sure time partitions of `partition_minutes`
    /// exist for now and the next [`retention::PARTITIONS_AHEAD`] widths, and move any
    /// row the DEFAULT leaf caught into its proper leaf. The DDL runs inside the
    /// SECURITY DEFINER function `kafka.ensure_message_partitions`, so the worker role
    /// needs only EXECUTE on it. Returns the number of leaves created. The worker
    /// calls this at start and every retention interval; the SQL function
    /// `pg_kafka_run_partition_maintenance()` runs it on demand.
    pub fn ensure_time_partitions(&self, partition_minutes: i32) -> Result<i64> {
        let minutes = partition_minutes.clamp(
            retention::MIN_PARTITION_MINUTES,
            retention::MAX_PARTITION_MINUTES,
        );
        Spi::connect_mut(|client| {
            let table = client.update(
                "SELECT kafka.ensure_message_partitions($1, $2)::bigint AS created",
                None,
                &[minutes.into(), retention::PARTITIONS_AHEAD.into()],
            )?;
            Ok(table.first().get_by_name::<i64, _>("created")?.unwrap_or(0))
        })
        .map_err(internal("ensure_time_partitions"))
    }

    pub(super) fn abort_timed_out_transactions(timeout: Duration) -> Result<Vec<String>> {
        // RV-7: each transaction's own timeout_ms (the client's transaction.timeout.ms
        // from InitProducerId) applies; `timeout` is only the fallback for a NULL one.
        let default_timeout_ms = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        crate::pg_debug!(
            "PostgresStore::abort_timed_out_transactions: default_timeout={default_timeout_ms}ms"
        );

        Spi::connect_mut(|client| {
            let table = client.select(
                "SELECT transactional_id, producer_id, producer_epoch
                 FROM kafka.transactions
                 WHERE state = 'Ongoing'
                   AND started_at < NOW() - (COALESCE(timeout_ms, $1) || ' milliseconds')::interval",
                None,
                &[default_timeout_ms.into()],
            )?;

            let mut aborted = Vec::new();
            for row in table {
                // NOT NULL columns of a row that exists: the fallbacks are unreachable.
                let txn_id: String = row.get_by_name("transactional_id")?.unwrap_or_default();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let producer_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);

                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;
                client.update(
                    "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                    None,
                    &[txn_id.as_str().into()],
                )?;

                // Fence the timed-out producer, as Kafka does, by moving it to its next
                // epoch: otherwise the stalled producer could resume with the same epoch
                // (AddPartitionsToTxn accepts CompleteAbort -> Ongoing), add more
                // partitions and commit only the part written after the abort. An
                // exhausted epoch gets a fresh producer id instead (bump_producer_epoch,
                // as for InitProducerId); the transaction row follows it through its ON
                // UPDATE CASCADE foreign key. An epoch that already moved is left alone.
                let current = client.select(
                    "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                    Some(1),
                    &[producer_id.into()],
                )?;
                let still_current = !current.is_empty()
                    && current.first().get_by_name::<i16, _>("epoch")? == Some(producer_epoch);
                let new_epoch = if still_current {
                    bump_producer_epoch(client, producer_id, producer_epoch)?.1
                } else {
                    producer_epoch
                };
                // The new epoch (or id) restarts sequence numbers.
                reset_producer_sequences(client, producer_id)?;
                client.update(
                    "UPDATE kafka.transactions
                     SET state = 'CompleteAbort', producer_epoch = $2, last_updated_at = NOW()
                     WHERE transactional_id = $1",
                    None,
                    &[txn_id.as_str().into(), new_epoch.into()],
                )?;

                crate::pg_debug!("Aborted timed-out transaction: {txn_id}");
                aborted.push(txn_id);
            }
            Ok(aborted)
        })
        .map_err(internal("abort_timed_out_transactions"))
    }

    /// The aborted-row part of the sweep on its own (`MaintenanceStore`), with the
    /// same per-leaf, index-driven, budgeted batches as in [`Self::run_retention_sweep`].
    pub(super) fn cleanup_aborted_messages(older_than: Duration) -> Result<u64> {
        crate::pg_debug!(
            "PostgresStore::cleanup_aborted_messages: older_than={}s",
            older_than.as_secs()
        );
        let pass_start = Instant::now();
        Spi::connect_mut(|client| {
            let now_ms = database_now_ms(client)?;
            let partitioned = has_time_partitions(client)?;
            let leaves = list_leaves(client, partitioned)?;
            delete_aborted_rows(client, &leaves, partitioned, now_ms, older_than, pass_start)
        })
        .map_err(internal("cleanup_aborted_messages"))
    }
}

/// Each topic's effective window ([`retention::effective_retention_window_ms`]): the
/// per-topic `retention.ms` (settable with IncrementalAlterConfigs) overrides the
/// global GUC; >= 0 enforces that window even when the global sweep is off, < 0 pins
/// the topic to infinite retention, NULL falls back to the GUC.
fn load_topic_windows(
    client: &SpiClient<'_>,
    message_retention_hours: i32,
) -> Result<TopicWindows> {
    let topics = client.select(
        "SELECT id, retention_ms FROM kafka.topics ORDER BY id",
        None,
        &[],
    )?;
    let mut by_topic = HashMap::new();
    let mut finite = Vec::new();
    for row in topics {
        let id: i32 = row.get_by_name("id")?.unwrap_or(0);
        let retention_ms: Option<i64> = row.get_by_name("retention_ms")?;
        let window =
            retention::effective_retention_window_ms(retention_ms, message_retention_hours);
        by_topic.insert(id, window);
        if let Some(window_ms) = window {
            finite.push((id, window_ms));
        }
    }
    Ok(TopicWindows { by_topic, finite })
}

/// The database clock, in epoch milliseconds.
fn database_now_ms(client: &SpiClient<'_>) -> Result<i64> {
    Ok(client
        .select(
            "SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint AS now_ms",
            None,
            &[],
        )?
        .first()
        .get_by_name("now_ms")?
        .unwrap_or(0))
}

/// Whether the schema has time partitions. A schema older than 0.1.0 (a new binary
/// before ALTER EXTENSION UPDATE) has none and no kafka.message_partitions(): the
/// sweep then treats the whole table as one DEFAULT-like leaf (expiry row by row,
/// nothing to drop).
fn has_time_partitions(client: &SpiClient<'_>) -> Result<bool> {
    Ok(client
        .select(
            "SELECT to_regprocedure('kafka.message_partitions()') IS NOT NULL AS p",
            None,
            &[],
        )?
        .first()
        .get_by_name("p")?
        .unwrap_or(false))
}

/// Whether the DEFAULT leaf holds any row (one probe; an unpartitioned table counts as
/// non-empty). Row expiry has a work item in the DEFAULT leaf for every topic with a
/// window, so when it is empty (the usual case: partition maintenance moves its rows
/// out) those items are left out instead of each costing a statement.
fn default_leaf_has_rows(client: &SpiClient<'_>, partitioned: bool) -> Result<bool> {
    if !partitioned {
        return Ok(true);
    }
    Ok(client
        .select(
            "SELECT EXISTS (SELECT 1 FROM kafka.messages_default) AS e",
            None,
            &[],
        )?
        .first()
        .get_by_name("e")?
        .unwrap_or(true))
}

/// The leaves, oldest first (DEFAULT and the upgrade's MINVALUE leaf first).
fn list_leaves(client: &SpiClient<'_>, partitioned: bool) -> Result<Vec<Leaf>> {
    let sql = if partitioned {
        "SELECT partition_oid::bigint AS oid, partition_name, is_default,
                lower_bound::text AS lower_txt, upper_bound::text AS upper_txt,
                (EXTRACT(EPOCH FROM lower_bound) * 1000)::bigint AS lower_ms,
                (EXTRACT(EPOCH FROM upper_bound) * 1000)::bigint AS upper_ms
         FROM kafka.message_partitions()
         ORDER BY lower_bound NULLS FIRST, upper_bound"
    } else {
        "SELECT 0::bigint AS oid, 'messages'::text AS partition_name, true AS is_default,
                NULL::text AS lower_txt, NULL::text AS upper_txt,
                NULL::bigint AS lower_ms, NULL::bigint AS upper_ms"
    };
    let mut leaves = Vec::new();
    for row in client.select(sql, None, &[])? {
        let oid: i64 = row.get_by_name("oid")?.unwrap_or(0);
        let oid = u32::try_from(oid)
            .map_err(|_| KafkaError::Internal(format!("partition oid {oid} out of range")))?;
        leaves.push(Leaf {
            partition: TimePartition {
                oid,
                name: row.get_by_name("partition_name")?.unwrap_or_default(),
                lower_ms: row.get_by_name("lower_ms")?,
                upper_ms: row.get_by_name("upper_ms")?,
                is_default: row.get_by_name("is_default")?.unwrap_or(false),
            },
            lower_txt: row.get_by_name("lower_txt")?,
            upper_txt: row.get_by_name("upper_txt")?,
        });
    }
    Ok(leaves)
}

/// Where one batch of row deletes in `leaf` runs.
fn delete_target(leaf: &Leaf, partitioned: bool) -> DeleteTarget<'_> {
    if !partitioned {
        DeleteTarget::Unpartitioned
    } else if leaf.partition.is_default {
        DeleteTarget::DefaultLeaf
    } else {
        DeleteTarget::Leaf(Some(leaf))
    }
}

/// Delete aborted transactional rows older than `grace`, leaf by leaf (leaves that
/// start after the cutoff cannot hold any), in [`RETENTION_DELETE_BATCH`] batches
/// within the pass budget. Each batch is confined to one leaf and found through that
/// leaf's partial index idx_messages_txn_aborted, so the cost follows the aborted rows,
/// not the leaf's size: a leaf without any costs one probe of an empty index. The
/// step stops at its share of the pass ([`retention::share_may_continue`]): aborted
/// rows are client-driven, and the row expiry after it must keep part of every pass.
/// A pass that stops early leaves the rest for the next ones, which start after the
/// last leaf it worked on ([`retention::ABORTED_ROTATION`]): starting from the oldest
/// every time, passes on many retained leaves (or with the budget spent by earlier
/// steps) would never reach the newest. Aborted rows are invisible to every reader, so
/// their removal moves no log start.
fn delete_aborted_rows(
    client: &mut SpiClient<'_>,
    leaves: &[Leaf],
    partitioned: bool,
    now_ms: i64,
    grace: Duration,
    pass_start: Instant,
) -> Result<u64> {
    let grace_ms = i64::try_from(grace.as_millis())
        .unwrap_or(i64::MAX)
        .min(MAX_RETENTION_WINDOW_MS);
    let filter = BatchFilter::Aborted { grace_ms };
    let step_start = Instant::now();
    let mut eligible: Vec<&Leaf> = leaves
        .iter()
        .filter(|l| retention::may_hold_expired(&l.partition, now_ms, grace_ms))
        .collect();
    eligible.sort_by_key(|l| retention::leaf_key(&l.partition));
    let keys: Vec<i64> = eligible
        .iter()
        .map(|l| retention::leaf_key(&l.partition))
        .collect();
    let start = retention::ABORTED_ROTATION.start(&keys);
    let mut statements = 0usize;
    let mut last_served = None;
    let mut deleted = 0u64;
    'leaves: for leaf in retention::rotated(&eligible, start) {
        loop {
            if !retention::share_may_continue(
                statements,
                step_start.elapsed(),
                pass_start.elapsed(),
                pass_budget(),
            ) {
                break 'leaves;
            }
            let n = delete_batch(client, filter, delete_target(leaf, partitioned), None)?;
            statements += 1;
            last_served = Some(retention::leaf_key(&leaf.partition));
            deleted += n;
            // A short batch: this leaf is caught up.
            if !retention::should_run_another_batch(
                n,
                RETENTION_DELETE_BATCH,
                Duration::ZERO,
                Duration::MAX,
            ) {
                break;
            }
        }
    }
    retention::ABORTED_ROTATION.record(last_served);
    crate::pg_debug!("Deleted {deleted} aborted messages");
    Ok(deleted)
}

/// The past leaves among `candidates` that hold no row: one statement for all of
/// them, each probe pruned to its own leaf by the bounds (run-time pruning) and
/// stopping at the first live row.
fn empty_leaves(client: &SpiClient<'_>, candidates: &[&Leaf]) -> Result<Vec<u32>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let oids: Vec<i64> = candidates
        .iter()
        .map(|l| i64::from(l.partition.oid))
        .collect();
    let lowers: Vec<Option<String>> = candidates.iter().map(|l| l.lower_txt.clone()).collect();
    let uppers: Vec<Option<String>> = candidates.iter().map(|l| l.upper_txt.clone()).collect();
    let mut empty = Vec::new();
    for row in client.select(
        // LATERAL ... LIMIT 1, not NOT EXISTS: the planner turns NOT EXISTS into a
        // hash anti join that reads every leaf whole; the LIMIT keeps a nested loop
        // whose rescans prune the Append to the one leaf (run-time pruning) and stop
        // at its first row.
        "SELECT l.oid
         FROM unnest($1::int8[], $2::text[], $3::text[]) AS l(oid, lo, up)
         LEFT JOIN LATERAL (
             SELECT true AS found FROM kafka.messages m
             WHERE m.created_at >= COALESCE(l.lo::timestamptz, '-infinity')
               AND m.created_at < COALESCE(l.up::timestamptz, 'infinity')
               AND m.tableoid = l.oid::oid
             LIMIT 1) e ON true
         WHERE e.found IS NULL",
        None,
        &[oids.into(), lowers.into(), uppers.into()],
    )? {
        let oid: i64 = row.get_by_name("oid")?.unwrap_or(0);
        if let Ok(oid) = u32::try_from(oid) {
            empty.push(oid);
        }
    }
    Ok(empty)
}

/// Decide the pass's work: inspect the past time leaves (drop one whole, trim it, or
/// keep it), then add the row-level expiry: short windows everywhere they may have
/// expired rows, longer windows only in the DEFAULT leaf (the rest is leaf-granular).
///
/// Only a leaf where some topic can be wholly expired
/// ([`retention::needs_contents_probe`]) gets the full contents probe (one btree
/// descent per (topic, partition) it holds), at most [`MAX_CONTENTS_PROBES_PER_PASS`]
/// per pass and within the step's share of the pass budget, rotating over the
/// candidates ([`retention::PROBE_ROTATION`]). Once the pass has planned
/// [`retention::MAX_PARTITION_DROPS_PER_PASS`] drops the probes stop at the first leaf
/// that would be one more, and the next pass resumes at that leaf. Every other past
/// leaf can only be dropped if it is empty: one statement checks them all. With no
/// finite window anywhere (the default), no leaf is probed. The DEFAULT leaf gets no
/// row-expiry items when it is empty (`default_has_rows`).
fn plan_expiry(
    client: &SpiClient<'_>,
    leaves: &[Leaf],
    windows: &TopicWindows,
    clock: ExpiryClock,
    default_has_rows: bool,
) -> Result<ExpiryPlan> {
    let ExpiryClock {
        now_ms,
        width_ms,
        pass_start,
    } = clock;
    let partitions: Vec<TimePartition> = leaves.iter().map(|l| l.partition.clone()).collect();
    let mut plan = ExpiryPlan::default();
    let shortest = windows.finite.iter().map(|&(_, w)| w).min();
    let (probe, check_empty): (Vec<&TimePartition>, Vec<&TimePartition>) =
        retention::past_time_partitions(&partitions, now_ms)
            .into_iter()
            .partition(|l| {
                retention::needs_contents_probe(l.upper_ms.unwrap_or(i64::MAX), now_ms, shortest)
            });
    let check_empty: Vec<&Leaf> = check_empty
        .iter()
        .filter_map(|p| leaves.iter().find(|l| l.partition.oid == p.oid))
        .collect();
    for oid in empty_leaves(client, &check_empty)? {
        if plan.drops.len() < retention::MAX_PARTITION_DROPS_PER_PASS {
            plan.drops.push((oid, Vec::new()));
        }
    }
    let keys: Vec<i64> = probe.iter().map(|l| retention::leaf_key(l)).collect();
    let probe_start = retention::PROBE_ROTATION.start(&keys);
    let step_start = Instant::now();
    let mut last_probed = None;
    for (probes, leaf) in retention::rotated(&probe, probe_start)
        .into_iter()
        .enumerate()
    {
        if probes >= MAX_CONTENTS_PROBES_PER_PASS
            || !retention::share_may_continue(
                probes,
                step_start.elapsed(),
                pass_start.elapsed(),
                pass_budget(),
            )
        {
            break;
        }
        let row = client.select(
            "SELECT topic_ids, has_pending
             FROM kafka.message_partition_contents($1::bigint::oid::regclass)",
            None,
            &[i64::from(leaf.oid).into()],
        )?;
        let row = row.first();
        let present: Vec<Option<i32>> = row.get_by_name("topic_ids")?.unwrap_or_default();
        let present: Vec<i32> = present.into_iter().flatten().collect();
        let has_pending: bool = row.get_by_name("has_pending")?.unwrap_or(true);
        let upper_ms = leaf.upper_ms.unwrap_or(i64::MAX);
        match retention::classify_leaf(upper_ms, now_ms, &present, has_pending, |t| {
            windows.window(t)
        }) {
            LeafAction::Drop { expired_topics } => {
                if plan.drops.len() >= retention::MAX_PARTITION_DROPS_PER_PASS {
                    // No room for the drop this pass: the next pass resumes at this leaf.
                    break;
                }
                plan.drops.push((leaf.oid, expired_topics));
            }
            LeafAction::Trim { topics } => {
                plan.work.extend(topics.into_iter().filter_map(|topic_id| {
                    windows.window(topic_id).map(|window_ms| ExpiryWork {
                        topic_id,
                        window_ms,
                        leaf_oid: leaf.oid,
                        leaf_key: retention::leaf_key(leaf),
                    })
                }));
            }
            LeafAction::Keep => {}
        }
        last_probed = Some(retention::leaf_key(leaf));
    }
    retention::PROBE_ROTATION.record(last_probed);
    for &(topic_id, window_ms) in &windows.finite {
        for leaf in &partitions {
            if plan.drops.iter().any(|(oid, _)| *oid == leaf.oid)
                || (leaf.is_default && !default_has_rows)
            {
                continue;
            }
            let in_scope = leaf.is_default || retention::row_level_expiry(window_ms, width_ms);
            if in_scope && retention::may_hold_expired(leaf, now_ms, window_ms) {
                plan.work.push(ExpiryWork {
                    topic_id,
                    window_ms,
                    leaf_oid: leaf.oid,
                    leaf_key: retention::leaf_key(leaf),
                });
            }
        }
    }
    // One stable key space for the trims and the row-level items, so the expiry's
    // cursor means the same item from pass to pass; a trim and a row-level item of the
    // same (topic, leaf) are the same delete.
    plan.work.sort_by_key(ExpiryWork::key);
    plan.work.dedup_by_key(|w| w.key());
    Ok(plan)
}

/// The clocks [`plan_expiry`] decides by: the database time and the partition width
/// (epoch ms, ms) and when the pass started.
#[derive(Clone, Copy)]
struct ExpiryClock {
    now_ms: i64,
    width_ms: i64,
    pass_start: Instant,
}

/// Run the row deletes in [`RETENTION_DELETE_BATCH`] batches per work item, until
/// caught up (a short batch) or the pass has spent [`retention::RETENTION_PASS_BUDGET`]
/// (after at least one statement). Each pass starts after the last item the previous
/// one worked on ([`retention::EXPIRY_ROTATION`], keyed by (topic, leaf)), so a pass
/// with little budget left still moves on through the items, and an item with a large
/// backlog takes a turn per rotation instead of every pass. Returns the number of rows
/// deleted and records the partitions they came from.
///
/// Each statement is confined to one leaf: through the parent with the leaf's bounds
/// (so the planner prunes to it) and its tableoid (ctid is only unique within one
/// table), or directly on kafka.messages_default. The per-topic cutoff is one constant
/// per statement, so the planner can use the leaf's idx_messages_created_at_brin or
/// its topic_id-prefixed unique index. Pending transactional rows are never deleted.
/// The window is built by multiplication: PG14 rejects a text 'N milliseconds' literal
/// above the int32 range.
fn delete_expired_rows(
    client: &mut SpiClient<'_>,
    work: &[ExpiryWork],
    leaves: &[Leaf],
    partitioned: bool,
    affected: &mut AffectedPartitions,
    pass_start: Instant,
) -> Result<u64> {
    let default_oid = leaves
        .iter()
        .find(|l| l.partition.is_default)
        .map(|l| l.partition.oid);
    let mut expired = 0u64;
    let mut statements = 0usize;
    let keys: Vec<(i32, i64)> = work.iter().map(ExpiryWork::key).collect();
    let start = retention::EXPIRY_ROTATION.start(&keys);
    let mut last_served = None;
    'work: for item in retention::rotated(work, start) {
        loop {
            if !retention::step_may_continue(statements, pass_start.elapsed(), pass_budget()) {
                break 'work;
            }
            let target = if !partitioned {
                DeleteTarget::Unpartitioned
            } else if Some(item.leaf_oid) == default_oid {
                DeleteTarget::DefaultLeaf
            } else {
                DeleteTarget::Leaf(leaves.iter().find(|l| l.partition.oid == item.leaf_oid))
            };
            let filter = BatchFilter::Expired {
                topic_id: item.topic_id,
                window_ms: item.window_ms,
            };
            let deleted = delete_batch(client, filter, target, Some(affected))?;
            statements += 1;
            last_served = Some(item.key());
            expired += deleted;
            if !retention::should_run_another_batch(
                deleted,
                RETENTION_DELETE_BATCH,
                pass_start.elapsed(),
                pass_budget(),
            ) {
                break;
            }
        }
    }
    retention::EXPIRY_ROTATION.record(last_served);
    Ok(expired)
}

/// Where one batch of row deletes runs.
#[derive(Clone, Copy)]
enum DeleteTarget<'a> {
    /// `kafka.messages` is a plain table.
    Unpartitioned,
    /// `kafka.messages_default`, directly.
    DefaultLeaf,
    /// One time leaf, through the parent (its bounds and tableoid; `None` when the
    /// leaf is not in the listing, which leaves the bounds open).
    Leaf(Option<&'a Leaf>),
}

/// Delete one batch of at most [`RETENTION_DELETE_BATCH`] rows matching `filter`
/// from `target`, record the partitions they came from in `affected` (when given), and
/// return how many were deleted.
fn delete_batch(
    client: &mut SpiClient<'_>,
    filter: BatchFilter,
    target: DeleteTarget<'_>,
    affected: Option<&mut AffectedPartitions>,
) -> Result<u64> {
    let predicate = filter.predicate();
    let (topic_id, age_ms) = filter.args();
    let table = match target {
        DeleteTarget::Unpartitioned | DeleteTarget::DefaultLeaf => {
            let table = if matches!(target, DeleteTarget::Unpartitioned) {
                "kafka.messages"
            } else {
                "kafka.messages_default"
            };
            client.update(
                &format!(
                    "DELETE FROM {table}
                     WHERE ctid IN (SELECT m.ctid FROM {table} m WHERE {predicate} LIMIT $3)
                     RETURNING topic_id, partition_id, partition_offset"
                ),
                None,
                &[
                    topic_id.into(),
                    age_ms.into(),
                    RETENTION_DELETE_BATCH.into(),
                ],
            )?
        }
        DeleteTarget::Leaf(leaf) => {
            let lower = leaf.and_then(|l| l.lower_txt.clone());
            let upper = leaf.and_then(|l| l.upper_txt.clone());
            let oid = leaf.map_or(0, |l| i64::from(l.partition.oid));
            client.update(
                &format!(
                    "DELETE FROM kafka.messages
                     WHERE created_at >= COALESCE($4::text::timestamptz, '-infinity')
                       AND created_at < COALESCE($5::text::timestamptz, 'infinity')
                       AND tableoid = $6::bigint::oid
                       AND ctid = ANY (ARRAY(
                           SELECT m.ctid FROM kafka.messages m
                           WHERE m.created_at >= COALESCE($4::text::timestamptz, '-infinity')
                             AND m.created_at < COALESCE($5::text::timestamptz, 'infinity')
                             AND m.tableoid = $6::bigint::oid
                             AND {predicate}
                           LIMIT $3))
                     RETURNING topic_id, partition_id, partition_offset"
                ),
                None,
                &[
                    topic_id.into(),
                    age_ms.into(),
                    RETENTION_DELETE_BATCH.into(),
                    lower.into(),
                    upper.into(),
                    oid.into(),
                ],
            )?
        }
    };
    let mut deleted = 0u64;
    let mut affected = affected;
    for row in table {
        deleted += 1;
        if let Some(affected) = affected.as_deref_mut() {
            let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
            let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
            let offset: i64 = row.get_by_name("partition_offset")?.unwrap_or(-1);
            affected.removed(topic_id, partition_id, offset.saturating_add(1));
        }
    }
    Ok(deleted)
}

/// Drop the planned leaves (each re-checked under its lock by
/// `kafka.drop_message_partition`), within the pass budget (at least one per pass),
/// stopping after a drop that ran out of a lock wait: each call can wait its 2 s
/// lock_timeout for a session that holds a lock on kafka.messages or a leaf (a reader
/// idle in transaction, a long query, autovacuum), and that session blocks every
/// further drop of the pass just as long, so continuing would only stall the DB thread.
/// The leaves not dropped stay candidates for the next pass. Returns how many were
/// dropped and records the partitions they held.
fn drop_leaves(
    client: &mut SpiClient<'_>,
    drops: Vec<(u32, Vec<i32>)>,
    affected: &mut AffectedPartitions,
    pass_start: Instant,
) -> Result<u64> {
    let mut dropped = 0;
    let mut lock_timed_out = false;
    for (attempts, (leaf_oid, expired_topics)) in drops.into_iter().enumerate() {
        if !retention::drop_may_continue(
            attempts,
            lock_timed_out,
            pass_start.elapsed(),
            pass_budget(),
        ) {
            break;
        }
        let row = client.update(
            "SELECT dropped, topic_ids, partition_ids, next_offsets, lock_timed_out
             FROM kafka.drop_message_partition($1::bigint::oid::regclass, $2::int4[])",
            None,
            &[i64::from(leaf_oid).into(), expired_topics.into()],
        )?;
        let row = row.first();
        lock_timed_out = row
            .get_by_name::<bool, _>("lock_timed_out")?
            .unwrap_or(false);
        if row.get_by_name::<bool, _>("dropped")?.unwrap_or(false) {
            dropped += 1;
            let topic_ids: Vec<Option<i32>> = row.get_by_name("topic_ids")?.unwrap_or_default();
            let partition_ids: Vec<Option<i32>> =
                row.get_by_name("partition_ids")?.unwrap_or_default();
            let next_offsets: Vec<Option<i64>> =
                row.get_by_name("next_offsets")?.unwrap_or_default();
            for ((t, p), next) in topic_ids.into_iter().zip(partition_ids).zip(next_offsets) {
                if let (Some(t), Some(p), Some(next)) = (t, p, next) {
                    affected.removed(t, p, next);
                }
            }
        }
    }
    Ok(dropped)
}

/// Advance the durable log start of every partition the deletes or drops touched:
/// like DeleteRecords, retention must move `partition_offsets.log_start_offset`, or a
/// partition emptied by retention reports EARLIEST = 0 and a consumer resets to offsets
/// retention already removed. Offsets grow with created_at, so retention removes an
/// offset-contiguous prefix; the new log start is the oldest surviving offset, or
/// next_offset (the high watermark) when the partition is now empty.
fn advance_log_start(client: &mut SpiClient<'_>, affected: AffectedPartitions) -> Result<()> {
    if affected.is_empty() {
        return Ok(());
    }
    let (topic_ids, partition_ids, next_offsets) = affected.into_columns();
    // A partition written without a counter row (direct SQL, rows older than the
    // counter) gets one now: once its rows are gone nothing else remembers its log end.
    client.update(
        "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
         SELECT a.topic_id, a.partition_id, a.next_offset
         FROM unnest($1::int4[], $2::int4[], $3::int8[]) AS a(topic_id, partition_id, next_offset)
         ON CONFLICT (topic_id, partition_id) DO UPDATE
             SET next_offset = GREATEST(kafka.partition_offsets.next_offset,
                                        EXCLUDED.next_offset)",
        None,
        &[
            topic_ids.clone().into(),
            partition_ids.clone().into(),
            next_offsets.into(),
        ],
    )?;
    client.update(
        "UPDATE kafka.partition_offsets po
         SET log_start_offset = GREATEST(
                 po.log_start_offset,
                 COALESCE((SELECT MIN(m.partition_offset) FROM kafka.messages m
                           WHERE m.topic_id = po.topic_id AND m.partition_id = po.partition_id),
                          po.next_offset))
         FROM unnest($1::int4[], $2::int4[]) AS a(topic_id, partition_id)
         WHERE po.topic_id = a.topic_id AND po.partition_id = a.partition_id",
        None,
        &[topic_ids.into(), partition_ids.into()],
    )?;
    Ok(())
}

/// Terminal transaction rows idle past [`TERMINAL_TXN_RETENTION`]. They go before the
/// producer prune, which their foreign key on producer_ids would otherwise block. A
/// producer that resumes a terminal transactional id within the window keeps its row;
/// one that resumes after the prune re-creates it with InitProducerId, matching Kafka's
/// transactional.id expiration.
fn delete_terminal_transactions(client: &mut SpiClient<'_>) -> Result<u64> {
    let table = client.update(
        "DELETE FROM kafka.transactions
         WHERE state IN ('CompleteCommit', 'CompleteAbort', 'Empty')
           AND last_updated_at < NOW() - ($1 || ' seconds')::interval
         RETURNING 1",
        None,
        &[whole_seconds(TERMINAL_TXN_RETENTION).into()],
    )?;
    Ok(row_count(table.len()))
}

/// Producers idle past [`PRODUCER_ID_RETENTION`] and not referenced by any remaining
/// transaction row. Their sequences go with them (no foreign key between the two
/// tables, so one statement keeps it atomic).
fn delete_stale_producers(client: &mut SpiClient<'_>) -> Result<u64> {
    let table = client.update(
        "WITH doomed AS (
             DELETE FROM kafka.producer_ids p
             WHERE p.last_active_at < NOW() - ($1 || ' seconds')::interval
               AND NOT EXISTS (
                   SELECT 1 FROM kafka.transactions t
                   WHERE t.producer_id = p.producer_id
               )
               -- last_active_at only moves on InitProducerId; a long-running
               -- producer shows its activity through its sequence rows.
               AND NOT EXISTS (
                   SELECT 1 FROM kafka.producer_sequences s
                   WHERE s.producer_id = p.producer_id
                     AND s.updated_at >= NOW() - ($1 || ' seconds')::interval
               )
             RETURNING p.producer_id
         ),
         seqs AS (
             DELETE FROM kafka.producer_sequences s
             USING doomed d
             WHERE s.producer_id = d.producer_id
         )
         SELECT COUNT(*)::BIGINT AS n FROM doomed",
        None,
        &[whole_seconds(PRODUCER_ID_RETENTION).into()],
    )?;
    let deleted = table.first().get_by_name::<i64, _>("n")?.unwrap_or(0);
    Ok(u64::try_from(deleted).unwrap_or(0))
}

/// Shadow-outbox rows forwarded (external_offset set) more than
/// [`SHADOW_DELIVERED_RETENTION`] ago: a day of history for diagnostics, then reaped.
///
/// Index-driven and bounded, never a scan of the outbox: the partitions with delivered
/// rows are found by a loose scan of idx_shadow_tracking_delivered (topic_id,
/// partition_id, local_offset) WHERE external_offset IS NOT NULL, and each partition's
/// delivered rows are walked in offset order while they are old enough, one index
/// step per row, so a partition with nothing due costs one descent. The forwarder
/// delivers a partition in offset order, so delivery time grows with the offset; a
/// row delivered again later (a replay) holds back the rows after it until it is due
/// too (at most one more [`SHADOW_DELIVERED_RETENTION`]). The index keys columns no
/// UPDATE changes and its predicate column (external_offset) already keys the pending
/// indexes, so it costs the outbox's hot UPDATEs nothing but one index entry per ack:
/// claims stay HOT. Batches of [`RETENTION_DELETE_BATCH`] rows within the pass budget.
fn delete_delivered_shadow_rows(client: &mut SpiClient<'_>, pass_start: Instant) -> Result<u64> {
    let mut deleted = 0u64;
    let mut statements = 0usize;
    while retention::step_may_continue(statements, pass_start.elapsed(), pass_budget()) {
        let table = client.update(
            "WITH RECURSIVE parts AS (
                 (SELECT s.topic_id, s.partition_id FROM kafka.shadow_tracking s
                  WHERE s.external_offset IS NOT NULL
                  ORDER BY s.topic_id, s.partition_id LIMIT 1)
                 UNION ALL
                 SELECT n.topic_id, n.partition_id
                 FROM parts p CROSS JOIN LATERAL (
                     SELECT s.topic_id, s.partition_id FROM kafka.shadow_tracking s
                     WHERE s.external_offset IS NOT NULL
                       AND (s.topic_id, s.partition_id) > (p.topic_id, p.partition_id)
                     ORDER BY s.topic_id, s.partition_id LIMIT 1) n
             ),
             walk AS (
                 SELECT f.topic_id, f.partition_id, f.local_offset, f.row_id, 1 AS depth
                 FROM parts p CROSS JOIN LATERAL (
                     SELECT s.topic_id, s.partition_id, s.local_offset, s.ctid AS row_id,
                            s.forwarded_at
                     FROM kafka.shadow_tracking s
                     WHERE s.topic_id = p.topic_id AND s.partition_id = p.partition_id
                       AND s.external_offset IS NOT NULL
                     ORDER BY s.local_offset LIMIT 1) f
                 WHERE f.forwarded_at < NOW() - ($1 || ' seconds')::interval
                 UNION ALL
                 SELECT f.topic_id, f.partition_id, f.local_offset, f.row_id, w.depth + 1
                 FROM walk w CROSS JOIN LATERAL (
                     SELECT s.topic_id, s.partition_id, s.local_offset, s.ctid AS row_id,
                            s.forwarded_at
                     FROM kafka.shadow_tracking s
                     WHERE s.topic_id = w.topic_id AND s.partition_id = w.partition_id
                       AND s.external_offset IS NOT NULL
                       AND s.local_offset > w.local_offset
                     ORDER BY s.local_offset LIMIT 1) f
                 WHERE f.forwarded_at < NOW() - ($1 || ' seconds')::interval
                   AND w.depth < $2
             )
             DELETE FROM kafka.shadow_tracking
             WHERE ctid = ANY (ARRAY(SELECT row_id FROM walk LIMIT $2))
               AND external_offset IS NOT NULL
             RETURNING 1",
            None,
            &[
                whole_seconds(SHADOW_DELIVERED_RETENTION).into(),
                RETENTION_DELETE_BATCH.into(),
            ],
        )?;
        statements += 1;
        let n = row_count(table.len());
        deleted += n;
        if !retention::should_run_another_batch(
            n,
            RETENTION_DELETE_BATCH,
            Duration::ZERO,
            pass_budget(),
        ) {
            break;
        }
    }
    Ok(deleted)
}

/// Shadow-outbox rows below their partition's log start (`kafka.partition_offsets`):
/// retention or DeleteRecords removed their message, so they can never be forwarded
/// (the claim skips them), and left behind they would keep a dead letter (an
/// `external_only` topic served locally for good, a replay unable to clear it) or a
/// pending row (lag that never reaches 0) forever. One primary-key range per
/// partition, so a partition with none costs one descent; batches of
/// [`RETENTION_DELETE_BATCH`] rows within the pass budget. Runs after
/// [`advance_log_start`], so the rows of messages this pass removed go in the same
/// pass, and DeleteRecords' (which advances the log start itself) at the next pass.
fn delete_orphaned_shadow_rows(client: &mut SpiClient<'_>, pass_start: Instant) -> Result<u64> {
    let mut deleted = 0u64;
    let mut statements = 0usize;
    while retention::step_may_continue(statements, pass_start.elapsed(), pass_budget()) {
        let table = client.update(
            "DELETE FROM kafka.shadow_tracking
             WHERE ctid = ANY (ARRAY(
                 SELECT o.row_id FROM kafka.partition_offsets po
                 CROSS JOIN LATERAL (
                     SELECT s.ctid AS row_id FROM kafka.shadow_tracking s
                     WHERE s.topic_id = po.topic_id AND s.partition_id = po.partition_id
                       AND s.local_offset < po.log_start_offset
                     ORDER BY s.local_offset LIMIT $1) o
                 WHERE po.log_start_offset > 0
                 LIMIT $1))
             RETURNING 1",
            None,
            &[RETENTION_DELETE_BATCH.into()],
        )?;
        statements += 1;
        let n = row_count(table.len());
        deleted += n;
        if !retention::should_run_another_batch(
            n,
            RETENTION_DELETE_BATCH,
            Duration::ZERO,
            pass_budget(),
        ) {
            break;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::AffectedPartitions;

    #[test]
    fn affected_partitions_keep_the_highest_removed_offset() {
        let mut affected = AffectedPartitions::default();
        assert!(affected.is_empty());
        affected.removed(1, 0, 5);
        affected.removed(1, 0, 3);
        affected.removed(1, 0, 9);
        affected.removed(2, 1, 1);
        let (topics, partitions, next) = affected.into_columns();
        let mut rows: Vec<(i32, i32, i64)> = topics
            .into_iter()
            .zip(partitions)
            .zip(next)
            .map(|((t, p), n)| (t, p, n))
            .collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![(1, 0, 9), (2, 1, 1)]);
    }
}
