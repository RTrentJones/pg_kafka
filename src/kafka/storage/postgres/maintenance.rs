//! Background maintenance SQL: the retention sweep, time-partition maintenance,
//! timed-out transactions and aborted-row cleanup.
//!
//! The sweep's decisions (which window applies, what to do with a leaf, when to stop)
//! are pure functions in [`retention`]; this module runs the statements they call for.

use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

use super::producers::{bump_producer_epoch, reset_producer_sequences};
use super::{internal, PostgresStore};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::retention::{
    self, ExpiryItem, FoundWork, Lap, LeafAction, MaintenanceLockWait, PassClock, PassStep,
    SweepClock, TimePartition, MAINTENANCE_LOCK_KEY, MAX_CONTENTS_PROBES_PER_PASS,
    MAX_PARTITION_DROPS_PER_PASS, MAX_RETENTION_WINDOW_MS, RETENTION_DELETE_BATCH, SWEEP_STATE,
};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

// Only the messages-table retention (the policy an operator tunes) is a GUC,
// `pg_kafka.message_retention_hours`; the auxiliary windows below are constants that
// match Kafka's own defaults where one exists (DR-1/DR-2, DEEP-REVIEW-2026-07).

/// Grace period before physically deleting `txn_state='aborted'` rows. Aborted rows
/// are already invisible to consumers; the grace only avoids churning rows a
/// concurrent diagnostic query might be looking at.
pub const ABORTED_MESSAGE_GRACE: Duration = Duration::from_secs(60);
/// Idle window after which a producer id (and its sequences) is reclaimed.
/// Kafka: `transactional.id.expiration.ms` defaults to 7 days.
pub(super) const PRODUCER_ID_RETENTION: Duration = Duration::from_hours(168);
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
/// bounds as timestamptz datums, which go back into SQL unchanged. Never as text: a
/// timestamptz printed under DateStyle SQL, Postgres or German carries a zone
/// abbreviation that reads back through `timezone_abbreviations`, not TimeZone, so it
/// can come back hours away from the bound (IST as +02:00 instead of +05:30).
struct Leaf {
    partition: TimePartition,
    lower: Option<TimestampWithTimeZone>,
    upper: Option<TimestampWithTimeZone>,
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
    /// primary key, the producer and transaction prunes through indexes on
    /// last_active_at, last_updated_at and transactions.producer_id) and works in turns
    /// of one bounded statement ([`RETENTION_DELETE_BATCH`] rows, one leaf, one probe,
    /// one drop), which [`retention::run_pass`] schedules: one turn per step with work,
    /// then the rest of [`retention::RETENTION_PASS_BUDGET`] to the steps that still
    /// have work, least-served first, each step resuming at the next pass where this one
    /// left it. The module docs of [`retention`] state and prove the guarantees: every
    /// item of every step within a bounded number of passes at any budget, a pass at
    /// most the budget plus one turn per step, no budget left idle while a step has
    /// work, and found drops and trims kept until done. The full contents probe of a leaf
    /// runs only where some topic can be wholly expired, at most
    /// [`MAX_CONTENTS_PROBES_PER_PASS`] per pass; the other past leaves get one
    /// emptiness check (a single statement for all of them).
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
    /// (`Ok(None)`). A drop's ACCESS EXCLUSIVE lock on kafka.messages is held until the
    /// pass commits, at most the rest of the pass.
    ///
    /// Lock waits: the pass is one transaction, which holds every row it deleted or
    /// locked until it commits, and `pg_kafka_run_retention_sweep()` runs it in a session
    /// of its own beside the worker, whose group commit holds the rows of every request
    /// of a batch until one commit (and beside the shadow forwarder's acks, operators'
    /// SQL). So once a pass holds a row, none of its statements waits in the lock manager
    /// for a row another transaction holds: waiting for one while holding a row the other
    /// transaction then writes (a Produce that upserts a counter the pass needs and then
    /// refreshes a producer the pass pruned; an ack batch that updates an outbox row the
    /// reaper waits for and then one it deleted) closed a cycle, and the deadlock detector
    /// failed a client's request or the sweep. Every row delete and lock of the pass
    /// skips the rows other transactions hold (`FOR UPDATE SKIP LOCKED`), except the
    /// offset counters whose log start it moves ([`advance_log_start`]), which it cannot
    /// skip and polls for instead, never queueing; a drop, which queues for its table
    /// locks, runs only while the pass holds no row ([`DropStep`]). A turn that finds a
    /// counter still held, or a session waiting for this transaction, rolls back its own
    /// statements and ends the pass ([`PassClock::ended`]), whose commit then frees the
    /// rows that session waits for; what it left goes to the next pass. A worker
    /// statement can still wait for a row the pass holds, until the pass commits (at
    /// most the budget plus one turn per step, and the poll of one counter), as for any
    /// transaction; it can no longer be the end of a cycle through the pass.
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
            let partitions: Vec<TimePartition> =
                leaves.iter().map(|l| l.partition.clone()).collect();
            let by_oid: HashMap<u32, &Leaf> = leaves.iter().map(|l| (l.partition.oid, l)).collect();
            let state = &SWEEP_STATE;
            let mut found = state.found();
            found.retain_current(&partitions, now_ms, |t| windows.window(t));

            let shortest = windows.finite.iter().map(|&(_, w)| w).min();
            let (probe, check_empty) = retention::split_past_leaves(&partitions, now_ms, shortest);
            let check_empty: Vec<&Leaf> = check_empty
                .iter()
                .filter_map(|p| by_oid.get(&p.oid).copied())
                .collect();
            for oid in empty_leaves(client, &check_empty)? {
                if let Some(leaf) = by_oid.get(&oid) {
                    found.record_empty(&leaf.partition);
                }
            }

            let mut aborted =
                AbortedStep::new(&leaves, partitioned, now_ms, aborted_grace, &state.aborted);
            let mut probes = ProbeStep {
                lap: Lap::new(
                    probe.iter().map(|l| (retention::leaf_key(l), *l)).collect(),
                    &state.probe,
                    MAX_CONTENTS_PROBES_PER_PASS,
                ),
                windows: &windows,
                now_ms,
            };
            let items = retention::expiry_items(
                &partitions,
                &windows.finite,
                SweepClock { now_ms, width_ms },
                default_has_rows,
                &found,
            );
            let mut expiry = ExpiryStep {
                lap: Lap::new(
                    items.into_iter().map(|i| (i.key(), i)).collect(),
                    &state.expiry,
                    usize::MAX,
                ),
                leaves: &by_oid,
                partitioned,
            };
            let drops_allowed = read_committed(client)?;
            if !drops_allowed {
                crate::pg_warning!(
                    "pg_kafka: this retention sweep drops no time partition: it runs outside \
                     READ COMMITTED, and its snapshot cannot see rows committed while a drop \
                     waits for its locks; a READ COMMITTED sweep drops them"
                );
            }
            let mut drops = DropStep {
                lap: Lap::new(found.drops(), &state.drop, MAX_PARTITION_DROPS_PER_PASS),
                blocked: !drops_allowed,
            };
            let mut terminal = BatchStep::new(Batch::TerminalTransactions);
            let mut producers = BatchStep::new(Batch::StaleProducers);
            let mut delivered = BatchStep::new(Batch::DeliveredShadowRows);
            let mut orphaned = BatchStep::new(Batch::OrphanedShadowRows);

            let mut pass = SweepPass::new(client, pass_start, &mut found);
            // Phase 1 order: the probes (reads only) and the drops first, as a drop runs
            // only before the pass holds a row ([`DropStep`]); terminal transactions
            // before the producer prune (their foreign key on producer_ids keeps those
            // producers); the orphaned outbox rows last (after the log start this pass's
            // deletes and drops moved).
            retention::run_pass(
                &mut [
                    &mut probes,
                    &mut drops,
                    &mut aborted,
                    &mut expiry,
                    &mut terminal,
                    &mut producers,
                    &mut delivered,
                    &mut orphaned,
                ],
                &mut pass,
                pass_budget(),
            )?;
            Ok(Some(pass.stats))
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
    /// same per-leaf, index-driven, budgeted turns as in [`Self::run_retention_sweep`].
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
            let mut aborted = AbortedStep::new(
                &leaves,
                partitioned,
                now_ms,
                older_than,
                &SWEEP_STATE.aborted,
            );
            let mut found = FoundWork::new();
            let mut pass = SweepPass::new(client, pass_start, &mut found);
            retention::run_pass(&mut [&mut aborted], &mut pass, pass_budget())?;
            Ok(pass.stats.aborted_messages)
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
                lower_bound, upper_bound,
                (EXTRACT(EPOCH FROM lower_bound) * 1000)::bigint AS lower_ms,
                (EXTRACT(EPOCH FROM upper_bound) * 1000)::bigint AS upper_ms
         FROM kafka.message_partitions()
         ORDER BY lower_bound NULLS FIRST, upper_bound"
    } else {
        "SELECT 0::bigint AS oid, 'messages'::text AS partition_name, true AS is_default,
                NULL::timestamptz AS lower_bound, NULL::timestamptz AS upper_bound,
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
            lower: row.get_by_name("lower_bound")?,
            upper: row.get_by_name("upper_bound")?,
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

/// The state the steps of one pass share ([`retention::run_pass`]'s context).
struct SweepPass<'p, 'conn> {
    client: &'p mut SpiClient<'conn>,
    pass_start: Instant,
    /// What earlier passes and this one found to drop or trim.
    found: &'p mut FoundWork,
    /// Trims and drops this pass's probes found, for the expiry and drop steps to take
    /// in the same pass.
    new_trims: Vec<ExpiryItem>,
    new_drops: Vec<(i64, retention::FoundDrop)>,
    /// A turn deleted or locked a row: another transaction may now wait for this one,
    /// so no drop (which queues for its table locks) runs in the rest of the pass.
    holds_rows: bool,
    /// A log start moved since the orphaned-outbox reaper's last turn.
    log_start_moved: bool,
    /// A turn gave up on a row another transaction holds: the pass ends.
    contended: bool,
    stats: RetentionSweepStats,
}

impl<'p, 'conn> SweepPass<'p, 'conn> {
    fn new(
        client: &'p mut SpiClient<'conn>,
        pass_start: Instant,
        found: &'p mut FoundWork,
    ) -> Self {
        SweepPass {
            client,
            pass_start,
            found,
            new_trims: Vec::new(),
            new_drops: Vec::new(),
            holds_rows: false,
            log_start_moved: false,
            contended: false,
            stats: RetentionSweepStats::default(),
        }
    }

    /// Run a turn's removal `remove` (which records the partitions it removed from in
    /// the [`AffectedPartitions`] it is given) and the log-start update of those
    /// partitions ([`advance_log_start`]) in one subtransaction: if the update cannot
    /// have a counter row, the removal is rolled back with it, the pass ends, and
    /// `Ok(None)` is returned; nothing was removed whose log start did not move.
    fn remove_and_advance<T>(
        &mut self,
        remove: impl FnOnce(&mut SpiClient<'conn>, &mut AffectedPartitions) -> Result<T>,
    ) -> Result<Option<T>> {
        let client = &mut *self.client;
        let done = in_subtransaction(|| {
            let mut affected = AffectedPartitions::default();
            let removed = remove(client, &mut affected)?;
            let moved = !affected.is_empty();
            if !advance_log_start(client, affected)? {
                return Ok(None);
            }
            Ok(Some((removed, moved)))
        })?;
        if let Some((removed, moved)) = done {
            self.log_start_moved |= moved;
            Ok(Some(removed))
        } else {
            self.contended = true;
            Ok(None)
        }
    }
}

impl PassClock for SweepPass<'_, '_> {
    fn elapsed(&self) -> Duration {
        self.pass_start.elapsed()
    }

    fn ended(&self) -> bool {
        self.contended
    }
}

/// Run `f` in an internal subtransaction (the sequence PL/pgSQL's exception blocks
/// use): `Ok(Some(_))` releases it into the pass, `Ok(None)` and `Err(_)` roll it back.
/// A lock-not-available ERROR inside it (`FOR UPDATE NOWAIT`, a `lock_timeout`) is
/// caught and rolled back as `Ok(None)`; any other ERROR is rolled back and raised
/// again.
fn in_subtransaction<T>(f: impl FnOnce() -> Result<Option<T>>) -> Result<Option<T>> {
    // SAFETY: called inside a transaction on the backend's main thread; every path
    // releases or rolls back the subtransaction begun here and restores the memory
    // context and resource owner saved first, as PL/pgSQL's exception blocks do.
    let outcome = unsafe {
        let old_context = pg_sys::CurrentMemoryContext;
        let old_owner = pg_sys::CurrentResourceOwner;
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pg_sys::MemoryContextSwitchTo(old_context);
        let outcome = match catch_unwind(AssertUnwindSafe(f)) {
            Ok(Ok(Some(value))) => {
                pg_sys::ReleaseCurrentSubTransaction();
                Ok(Ok(Some(value)))
            }
            Ok(other) => {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
                Ok(other)
            }
            Err(payload) => {
                pg_sys::FlushErrorState();
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
                Err(payload)
            }
        };
        pg_sys::MemoryContextSwitchTo(old_context);
        pg_sys::CurrentResourceOwner = old_owner;
        outcome
    };
    match outcome {
        Ok(result) => result,
        Err(payload) if is_lock_not_available(payload.as_ref()) => Ok(None),
        Err(payload) => resume_unwind(payload),
    }
}

/// Whether a caught panic payload is a Postgres lock-not-available ERROR (55P03).
fn is_lock_not_available(payload: &(dyn std::any::Any + Send)) -> bool {
    match payload.downcast_ref::<CaughtError>() {
        Some(
            CaughtError::PostgresError(r)
            | CaughtError::ErrorReport(r)
            | CaughtError::RustPanic { ereport: r, .. },
        ) => r.sql_error_code() == PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
        None => false,
    }
}

/// Delete aborted transactional rows older than the grace, leaf by leaf (leaves that
/// start after the cutoff cannot hold any), one [`RETENTION_DELETE_BATCH`] batch a
/// turn, a leaf until caught up. Each batch is confined to one leaf and found through
/// that leaf's partial index idx_messages_txn_aborted, so the cost follows the aborted
/// rows, not the leaf's size: a leaf without any costs one probe of an empty index. The
/// leaves are a [`Lap`] ([`retention::SweepState::aborted`]): starting from the oldest
/// every time, passes on many retained leaves (or with the budget spent elsewhere)
/// would never reach the newest. Aborted rows are invisible to every reader, so their
/// removal moves no log start.
struct AbortedStep<'l> {
    lap: Lap<'static, i64, &'l Leaf>,
    filter: BatchFilter,
    partitioned: bool,
}

impl<'l> AbortedStep<'l> {
    fn new(
        leaves: &'l [Leaf],
        partitioned: bool,
        now_ms: i64,
        grace: Duration,
        cursor: &'static retention::KeyCursor<i64>,
    ) -> Self {
        let grace_ms = i64::try_from(grace.as_millis())
            .unwrap_or(i64::MAX)
            .min(MAX_RETENTION_WINDOW_MS);
        let eligible = leaves
            .iter()
            .filter(|l| retention::may_hold_expired(&l.partition, now_ms, grace_ms))
            .map(|l| (retention::leaf_key(&l.partition), l))
            .collect();
        AbortedStep {
            lap: Lap::new(eligible, cursor, usize::MAX),
            filter: BatchFilter::Aborted { grace_ms },
            partitioned,
        }
    }
}

impl PassStep<SweepPass<'_, '_>, KafkaError> for AbortedStep<'_> {
    fn has_work(&self, _: &SweepPass<'_, '_>) -> bool {
        !self.lap.is_done()
    }

    fn turn(&mut self, ctx: &mut SweepPass<'_, '_>) -> Result<()> {
        let Some(&(_, leaf)) = self.lap.current() else {
            return Ok(());
        };
        let n = delete_batch(
            ctx.client,
            self.filter,
            delete_target(leaf, self.partitioned),
            None,
        )?;
        ctx.holds_rows |= n > 0;
        ctx.stats.aborted_messages += n;
        self.lap
            .served(!retention::batch_was_full(n, RETENTION_DELETE_BATCH));
        Ok(())
    }
}

/// Whether this transaction runs at READ COMMITTED. Only there does
/// `kafka.drop_message_partition` re-check a leaf with a snapshot taken after its lock
/// wait; at REPEATABLE READ or SERIALIZABLE it refuses every drop, so the sweep skips
/// its drop step and keeps the found leaves for a later pass.
fn read_committed(client: &SpiClient<'_>) -> Result<bool> {
    Ok(client
        .select(
            "SELECT current_setting('transaction_isolation') = 'read committed' AS rc",
            Some(1),
            &[],
        )?
        .first()
        .get_by_name::<bool, _>("rc")?
        .unwrap_or(false))
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
    let lowers: Vec<Option<TimestampWithTimeZone>> = candidates.iter().map(|l| l.lower).collect();
    let uppers: Vec<Option<TimestampWithTimeZone>> = candidates.iter().map(|l| l.upper).collect();
    let mut empty = Vec::new();
    for row in client.select(
        // LATERAL ... LIMIT 1, not NOT EXISTS: the planner turns NOT EXISTS into a
        // hash anti join that reads every leaf whole; the LIMIT keeps a nested loop
        // whose rescans prune the Append to the one leaf (run-time pruning) and stop
        // at its first row.
        "SELECT l.oid
         FROM unnest($1::int8[], $2::timestamptz[], $3::timestamptz[]) AS l(oid, lo, up)
         LEFT JOIN LATERAL (
             SELECT true AS found FROM kafka.messages m
             WHERE m.created_at >= COALESCE(l.lo, '-infinity')
               AND m.created_at < COALESCE(l.up, 'infinity')
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

/// The contents probes: for a past leaf where some topic can be wholly expired
/// ([`retention::needs_contents_probe`]), `kafka.message_partition_contents` (one btree
/// descent per (topic, partition) it holds) and [`retention::classify_leaf`], one leaf a
/// turn, at most [`MAX_CONTENTS_PROBES_PER_PASS`] per pass, rotating over the
/// candidates ([`retention::SweepState::probe`]). What a probe finds goes to
/// [`FoundWork`], where the expiry and drop steps take it from, in this pass or a later
/// one. Every other past leaf can only be dropped if it is empty: one statement before
/// the pass checks them all. With no finite window anywhere (the default), no leaf is
/// probed.
struct ProbeStep<'l> {
    lap: Lap<'static, i64, &'l TimePartition>,
    windows: &'l TopicWindows,
    now_ms: i64,
}

impl PassStep<SweepPass<'_, '_>, KafkaError> for ProbeStep<'_> {
    fn has_work(&self, _: &SweepPass<'_, '_>) -> bool {
        !self.lap.is_done()
    }

    fn turn(&mut self, ctx: &mut SweepPass<'_, '_>) -> Result<()> {
        let Some(&(key, leaf)) = self.lap.current() else {
            return Ok(());
        };
        let row = ctx.client.select(
            "SELECT topic_ids, has_pending
             FROM kafka.message_partition_contents($1::bigint::oid::regclass)",
            None,
            &[i64::from(leaf.oid).into()],
        )?;
        let row = row.first();
        let present: Vec<Option<i32>> = row.get_by_name("topic_ids")?.unwrap_or_default();
        let present: Vec<i32> = present.into_iter().flatten().collect();
        let has_pending: bool = row.get_by_name("has_pending")?.unwrap_or(true);
        let action = retention::classify_leaf(key, self.now_ms, &present, has_pending, |t| {
            self.windows.window(t)
        });
        ctx.found.record_probe(leaf, &action);
        match action {
            LeafAction::Drop { expired_topics } => ctx.new_drops.push((
                key,
                retention::FoundDrop {
                    oid: leaf.oid,
                    expired_topics,
                },
            )),
            LeafAction::Trim { topics } => {
                ctx.new_trims
                    .extend(topics.into_iter().filter_map(|topic_id| {
                        self.windows.window(topic_id).map(|window_ms| ExpiryItem {
                            topic_id,
                            window_ms,
                            leaf_oid: leaf.oid,
                            leaf_key: key,
                        })
                    }));
            }
            LeafAction::Keep => {}
        }
        self.lap.served(true);
        Ok(())
    }
}

/// Row expiry: the row deletes of [`retention::expiry_items`] (the found trims and the
/// row-level items) plus the trims this pass's probes found, one
/// [`RETENTION_DELETE_BATCH`] batch a turn, an item until caught up (a short batch: a
/// trim is then done). The items are a [`Lap`] keyed by (topic, leaf)
/// ([`retention::SweepState::expiry`]), so a pass with little budget left still moves on
/// through them, and an item with a large backlog takes a turn per rotation instead of
/// every pass.
///
/// Each statement is confined to one leaf: through the parent with the leaf's bounds
/// (so the planner prunes to it) and its tableoid (ctid is only unique within one
/// table), or directly on kafka.messages_default. The per-topic cutoff is one constant
/// per statement, so the planner can use the leaf's idx_messages_created_at_brin or
/// its topic_id-prefixed unique index. Pending transactional rows are never deleted.
/// The window is built by multiplication: PG14 rejects a text 'N milliseconds' literal
/// above the int32 range.
///
/// Cost per turn: the statement, a hash lookup of its leaf, and pushing the trims this
/// pass's probes found since the last turn (O(log n) each, [`Lap::push`]).
struct ExpiryStep<'l> {
    lap: Lap<'static, (i32, i64), ExpiryItem>,
    /// The leaves by oid.
    leaves: &'l HashMap<u32, &'l Leaf>,
    partitioned: bool,
}

impl PassStep<SweepPass<'_, '_>, KafkaError> for ExpiryStep<'_> {
    fn has_work(&self, ctx: &SweepPass<'_, '_>) -> bool {
        !self.lap.is_done() || !ctx.new_trims.is_empty()
    }

    fn turn(&mut self, ctx: &mut SweepPass<'_, '_>) -> Result<()> {
        for item in ctx.new_trims.drain(..) {
            self.lap.push(item.key(), item);
        }
        let Some(&(key, item)) = self.lap.current() else {
            return Ok(());
        };
        let leaf = self.leaves.get(&item.leaf_oid).copied();
        let target = match leaf {
            Some(leaf) => delete_target(leaf, self.partitioned),
            None if !self.partitioned => DeleteTarget::Unpartitioned,
            None => DeleteTarget::Leaf(None),
        };
        let filter = BatchFilter::Expired {
            topic_id: item.topic_id,
            window_ms: item.window_ms,
        };
        let Some(n) = ctx.remove_and_advance(|client, affected| {
            delete_batch(client, filter, target, Some(affected))
        })?
        else {
            // A counter stayed held: the batch is rolled back and the pass ends; this
            // item stays current for the next pass.
            return Ok(());
        };
        ctx.holds_rows |= n > 0;
        ctx.stats.expired_messages += n;
        let caught_up = !retention::batch_was_full(n, RETENTION_DELETE_BATCH);
        if caught_up {
            ctx.found.trim_done(key);
        }
        self.lap.served(caught_up);
        Ok(())
    }
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
///
/// The batch skips the rows another transaction holds (a DeleteRecords or an EndTxn of
/// the worker's open batch, a session's own DML): it never waits for them (see "Lock
/// waits" on [`PostgresStore::run_retention_sweep`]). A skipped row stays, so the log
/// start stops at it, and a later batch or pass takes it. The lock costs one tuple lock
/// per row the batch deletes and no read beyond the ones the delete makes.
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
                     WHERE ctid IN (SELECT m.ctid FROM {table} m WHERE {predicate} LIMIT $3
                                    FOR UPDATE OF m SKIP LOCKED)
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
            let lower = leaf.and_then(|l| l.lower);
            let upper = leaf.and_then(|l| l.upper);
            let oid = leaf.map_or(0, |l| i64::from(l.partition.oid));
            client.update(
                &format!(
                    "DELETE FROM kafka.messages
                     WHERE created_at >= COALESCE($4::timestamptz, '-infinity')
                       AND created_at < COALESCE($5::timestamptz, 'infinity')
                       AND tableoid = $6::bigint::oid
                       AND ctid = ANY (ARRAY(
                           SELECT m.ctid FROM kafka.messages m
                           WHERE m.created_at >= COALESCE($4::timestamptz, '-infinity')
                             AND m.created_at < COALESCE($5::timestamptz, 'infinity')
                             AND m.tableoid = $6::bigint::oid
                             AND {predicate}
                           LIMIT $3
                           FOR UPDATE OF m SKIP LOCKED))
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

/// Drop the found leaves (each re-checked under its lock by
/// `kafka.drop_message_partition`), one a turn, at most
/// [`MAX_PARTITION_DROPS_PER_PASS`] per pass, rotating over them
/// ([`retention::SweepState::drop`]). The step stops for the pass after a drop that ran
/// out of a lock wait: each call can wait its 2 s lock_timeout for a session that holds
/// a lock on kafka.messages or a leaf (a reader idle in transaction, a long query,
/// autovacuum), and that session blocks every further drop of the pass just as long, so
/// continuing would only stall the DB thread. That leaf stays found, and the next pass
/// starts after it; a leaf the re-check refuses is forgotten until a probe finds it
/// droppable again.
///
/// A drop queues for its table locks (in attempts of 50 ms, `kafka.lock_message_tables`),
/// so it runs only while the pass holds no row another transaction could be waiting for
/// ([`SweepPass::holds_rows`]): a transaction that holds ROW EXCLUSIVE on kafka.messages
/// and waits for a row the pass deleted would otherwise close a cycle with it. The drops
/// therefore come first in a pass; once a turn has deleted a row, the rest wait for the
/// next pass. After the first drop the pass holds kafka.messages itself, so later drops
/// of the pass do not wait for other sessions' locks on it. The log start of the dropped
/// leaf's partitions moves in the drop's own subtransaction
/// ([`SweepPass::remove_and_advance`]): if a counter stays held, the drop is rolled back
/// and the pass ends.
struct DropStep {
    lap: Lap<'static, i64, retention::FoundDrop>,
    blocked: bool,
}

impl PassStep<SweepPass<'_, '_>, KafkaError> for DropStep {
    fn has_work(&self, ctx: &SweepPass<'_, '_>) -> bool {
        !self.blocked && !ctx.holds_rows && (!self.lap.is_done() || !ctx.new_drops.is_empty())
    }

    fn turn(&mut self, ctx: &mut SweepPass<'_, '_>) -> Result<()> {
        for (key, found) in ctx.new_drops.drain(..) {
            self.lap.push(key, found);
        }
        let Some((key, found)) = self.lap.current().cloned() else {
            return Ok(());
        };
        let expired_topics = found.expired_topics;
        let Some((dropped, lock_timed_out)) = ctx.remove_and_advance(|client, affected| {
            let row = client.update(
                "SELECT dropped, topic_ids, partition_ids, next_offsets, lock_timed_out
                 FROM kafka.drop_message_partition($1::bigint::oid::regclass, $2::int4[])",
                None,
                &[i64::from(found.oid).into(), expired_topics.into()],
            )?;
            let row = row.first();
            let lock_timed_out = row
                .get_by_name::<bool, _>("lock_timed_out")?
                .unwrap_or(false);
            let dropped = row.get_by_name::<bool, _>("dropped")?.unwrap_or(false);
            if dropped {
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
            Ok((dropped, lock_timed_out))
        })?
        else {
            // A counter stayed held: the drop is rolled back and the pass ends; the leaf
            // stays found.
            return Ok(());
        };
        if dropped {
            ctx.stats.dropped_partitions += 1;
        }
        if lock_timed_out {
            self.lap.served(false);
            self.lap.stop();
            self.blocked = true;
        } else {
            ctx.found.drop_done(key);
            self.lap.served(true);
        }
        Ok(())
    }
}

/// How long [`advance_log_start`] polls for a counter row another transaction holds
/// before it gives up and the pass ends. A worker batch holds one for its group commit
/// (tens of milliseconds); the poll outlasts that, but not a session that holds the row
/// on purpose.
const HELD_COUNTER_POLL: Duration = Duration::from_secs(1);

/// The pause between two attempts of the poll, in seconds (`pg_sleep`).
const HELD_COUNTER_RETRY_PAUSE_S: f64 = 0.02;

/// Advance the durable log start of every partition a delete or drop touched: like
/// DeleteRecords, retention must move `partition_offsets.log_start_offset`, or a
/// partition emptied by retention reports EARLIEST = 0 and a consumer resets to offsets
/// retention already removed. Offsets grow with created_at, so retention removes an
/// offset-contiguous prefix; the new log start is the oldest surviving offset, or
/// next_offset (the high watermark) when the partition is now empty. Returns false when
/// a counter row stays held by another transaction (or a session waits for this one):
/// the caller rolls back its removal ([`SweepPass::remove_and_advance`]).
///
/// Concurrency: `pg_kafka_run_retention_sweep()` runs this in its own session while the
/// worker produces. A Produce inserts its records and then upserts the counter row,
/// holding both until it commits. So every affected counter row is locked first, and
/// the log-start UPDATE runs after, with a snapshot taken once all are held (READ
/// COMMITTED: a fresh snapshot per statement): its MIN sees the committed records whose
/// offsets its next_offset counts, and it updates rows this transaction already holds,
/// so nothing re-evaluates it against a newer counter. A Produce that reaches a counter
/// after it is locked waits for the sweep's commit, and its records are at or past the
/// next_offset the sweep read. Keep the lock before the UPDATE: an UPDATE that waited
/// for the Produce itself would re-read next_offset from the committed counter while its
/// MIN kept the old snapshot, and move the log start past the new record. Under
/// REPEATABLE READ or SERIALIZABLE a counter updated since the snapshot fails the lock
/// with a serialization error instead.
///
/// The locks never wait in the lock manager (see "Lock waits" on
/// [`PostgresStore::run_retention_sweep`]): the free counters are locked `SKIP LOCKED`
/// in one statement; each held one is polled with `FOR UPDATE NOWAIT`, one attempt every
/// [`HELD_COUNTER_RETRY_PAUSE_S`] for at most [`HELD_COUNTER_POLL`], each attempt after
/// checking that no session waits for this transaction ([`someone_waits_for_us`]): a
/// session that does may hold the counter until it gets a row of ours, so the poll stops
/// and the pass commits. A partition without a counter row gets one from the upsert;
/// that INSERT can only meet another transaction inserting the same row (a Produce of
/// a partition first written by direct SQL), so it runs under a 1 ms lock_timeout, once
/// no session waits for this one: a transaction that starts waiting for it afterwards
/// has the sweep's wait ended long before its deadlock check (deadlock_timeout, 1 s by
/// default), so the wait cannot close a cycle that check would see.
///
/// Cost: two statements for the affected partitions (at most the rows the turn
/// removed), the upsert and the UPDATE; a held counter adds at most
/// `HELD_COUNTER_POLL / HELD_COUNTER_RETRY_PAUSE_S` attempts of two small statements
/// each (the waiter check reads pg_locks), and there are no more held counters than
/// transactions holding them.
fn advance_log_start(client: &mut SpiClient<'_>, affected: AffectedPartitions) -> Result<bool> {
    if affected.is_empty() {
        return Ok(true);
    }
    let (topic_ids, partition_ids, next_offsets) = affected.into_columns();
    let free: HashSet<(i32, i32)> = keys_of(client.update(
        "SELECT po.topic_id, po.partition_id
         FROM kafka.partition_offsets po
         JOIN unnest($1::int4[], $2::int4[]) AS a(topic_id, partition_id)
           ON po.topic_id = a.topic_id AND po.partition_id = a.partition_id
         FOR UPDATE OF po SKIP LOCKED",
        None,
        &[topic_ids.clone().into(), partition_ids.clone().into()],
    )?)?;
    let present: HashSet<(i32, i32)> = keys_of(client.select(
        "SELECT po.topic_id, po.partition_id
         FROM kafka.partition_offsets po
         JOIN unnest($1::int4[], $2::int4[]) AS a(topic_id, partition_id)
           ON po.topic_id = a.topic_id AND po.partition_id = a.partition_id",
        None,
        &[topic_ids.clone().into(), partition_ids.clone().into()],
    )?)?;
    let deadline = Instant::now() + HELD_COUNTER_POLL;
    for &(topic_id, partition_id) in present.difference(&free) {
        let locked = poll_without_waiting(client, deadline, |client| {
            client.update(
                "SELECT 1 FROM kafka.partition_offsets
                 WHERE topic_id = $1 AND partition_id = $2
                 FOR UPDATE NOWAIT",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;
            Ok(())
        })?;
        if !locked {
            return Ok(false);
        }
    }
    // A partition written without a counter row (direct SQL, rows older than the
    // counter) gets one now: once its rows are gone nothing else remembers its log end.
    let upsert = |client: &mut SpiClient<'_>| -> Result<()> {
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
                next_offsets.clone().into(),
            ],
        )?;
        Ok(())
    };
    if present.len() == topic_ids.len() {
        // Every row is held by this transaction: the upsert updates them only.
        upsert(client)?;
    } else {
        let inserted = poll_without_waiting(client, deadline, |client| {
            // Set back below on success; a failed attempt's rollback restores it.
            let saved: String = client
                .update(
                    "SELECT current_setting('lock_timeout') AS saved,
                            set_config('lock_timeout', '1ms', true)",
                    Some(1),
                    &[],
                )?
                .first()
                .get_by_name("saved")?
                .unwrap_or_default();
            upsert(client)?;
            client.update(
                "SELECT set_config('lock_timeout', $1, true)",
                None,
                &[saved.into()],
            )?;
            Ok(())
        })?;
        if !inserted {
            return Ok(false);
        }
    }
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
    Ok(true)
}

/// The `(topic_id, partition_id)` pairs of a result.
fn keys_of(table: SpiTupleTable<'_>) -> Result<HashSet<(i32, i32)>> {
    let mut keys = HashSet::new();
    for row in table {
        let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
        let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
        keys.insert((topic_id, partition_id));
    }
    Ok(keys)
}

/// Run `attempt` (which must not wait in the lock manager beyond a lock_timeout of a
/// few milliseconds) in a subtransaction until it takes its lock, pausing
/// [`HELD_COUNTER_RETRY_PAUSE_S`] between attempts; gives up (false) at `deadline` or
/// as soon as a session waits for this transaction. Every attempt is made after that
/// check, so an attempt's own wait, if any, never closes a cycle another session's
/// deadlock check could see (see [`advance_log_start`]).
fn poll_without_waiting(
    client: &mut SpiClient<'_>,
    deadline: Instant,
    mut attempt: impl FnMut(&mut SpiClient<'_>) -> Result<()>,
) -> Result<bool> {
    loop {
        if someone_waits_for_us(client)? {
            return Ok(false);
        }
        if in_subtransaction(|| attempt(client).map(Some))?.is_some() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        client.update(
            "SELECT pg_sleep($1)",
            None,
            &[HELD_COUNTER_RETRY_PAUSE_S.into()],
        )?;
    }
}

/// Whether another session waits for a lock this transaction holds. One read of
/// pg_locks, and `pg_blocking_pids` for each waiting session only.
fn someone_waits_for_us(client: &SpiClient<'_>) -> Result<bool> {
    Ok(client
        .select(
            "SELECT EXISTS (
                 SELECT 1 FROM (SELECT DISTINCT l.pid FROM pg_locks l
                                WHERE NOT l.granted AND l.pid IS NOT NULL
                                  AND l.pid <> pg_backend_pid()) w
                 WHERE pg_backend_pid() = ANY (pg_blocking_pids(w.pid))) AS waits",
            Some(1),
            &[],
        )?
        .first()
        .get_by_name::<bool, _>("waits")?
        .unwrap_or(false))
}

/// The steps that delete one batch of an auxiliary table's due rows a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Batch {
    /// [`terminal_transactions_batch`].
    TerminalTransactions,
    /// [`stale_producers_batch`].
    StaleProducers,
    /// [`delivered_shadow_rows_batch`].
    DeliveredShadowRows,
    /// [`orphaned_shadow_rows_batch`], after the log-start update it reads.
    OrphanedShadowRows,
}

/// A [`Batch`] step: it has work until a batch comes back short (the producer prune:
/// until its cursor reaches the end of the candidates); the orphaned-row reaper also
/// whenever this pass has moved a log start since its last turn, so the outbox rows of
/// the messages this pass removed go in the same pass when the budget allows. Every
/// batch skips the rows other transactions hold, so a turn never waits.
struct BatchStep {
    batch: Batch,
    caught_up: bool,
}

impl BatchStep {
    fn new(batch: Batch) -> Self {
        BatchStep {
            batch,
            caught_up: false,
        }
    }
}

impl PassStep<SweepPass<'_, '_>, KafkaError> for BatchStep {
    fn has_work(&self, ctx: &SweepPass<'_, '_>) -> bool {
        !self.caught_up || (self.batch == Batch::OrphanedShadowRows && ctx.log_start_moved)
    }

    fn turn(&mut self, ctx: &mut SweepPass<'_, '_>) -> Result<()> {
        let full = |n| retention::batch_was_full(n, RETENTION_DELETE_BATCH);
        let (n, caught_up) = match self.batch {
            Batch::TerminalTransactions => {
                let n = terminal_transactions_batch(ctx.client)?;
                ctx.stats.terminal_transactions += n;
                (n, !full(n))
            }
            Batch::StaleProducers => {
                let (n, next) =
                    stale_producers_batch(ctx.client, SWEEP_STATE.producer_prune.get())?;
                SWEEP_STATE.producer_prune.set(next);
                ctx.stats.stale_producers += n;
                // A short batch reached the end of the candidates.
                (n, next.is_none())
            }
            Batch::DeliveredShadowRows => {
                let n = delivered_shadow_rows_batch(ctx.client)?;
                ctx.stats.shadow_delivered_rows += n;
                (n, !full(n))
            }
            Batch::OrphanedShadowRows => {
                // It reads the log starts the turns before it moved.
                ctx.log_start_moved = false;
                let n = orphaned_shadow_rows_batch(ctx.client)?;
                ctx.stats.shadow_orphaned_rows += n;
                (n, !full(n))
            }
        };
        ctx.holds_rows |= n > 0;
        self.caught_up = caught_up;
        Ok(())
    }
}

/// Terminal transaction rows idle past [`TERMINAL_TXN_RETENTION`], one batch. They go
/// before the producer prune in a pass's first turns, since their foreign key on
/// producer_ids keeps those producers (the prune skips a producer still referenced and
/// examines it again on its next lap). A producer that resumes a terminal transactional
/// id within the window keeps its row; one that resumes after the prune re-creates it
/// with InitProducerId, matching Kafka's transactional.id expiration.
///
/// Index-driven and bounded: the partial index idx_transactions_terminal
/// (last_updated_at) WHERE the state is terminal holds exactly the rows this deletes,
/// oldest first, so a pass with none due costs one descent; batches of
/// [`RETENTION_DELETE_BATCH`] rows (the transactional ids are client-chosen, so their
/// number is too).
///
/// Concurrency: rows another transaction holds (InitProducerId, EndTxn and
/// AddPartitionsToTxn update or reference them) are skipped, not waited for, and taken
/// by a later batch: the worker's group commit holds the rows of every request in its
/// batch until one commit, so an on-demand sweep that waited for one of them while
/// holding another the batch then needs was a deadlock. Skipping keeps only this
/// statement out of such a cycle: the rows it deletes stay held to the end of the pass,
/// which is why no later statement of the pass waits for a row either ("Lock waits" on
/// [`PostgresStore::run_retention_sweep`]).
fn terminal_transactions_batch(client: &mut SpiClient<'_>) -> Result<u64> {
    let table = client.update(
        "DELETE FROM kafka.transactions
         WHERE ctid = ANY (ARRAY(
             SELECT t.ctid FROM kafka.transactions t
             WHERE t.state IN ('CompleteCommit', 'CompleteAbort', 'Empty')
               AND t.last_updated_at < NOW() - ($1 || ' seconds')::interval
             LIMIT $2
             FOR UPDATE SKIP LOCKED))
         RETURNING 1",
        None,
        &[
            whole_seconds(TERMINAL_TXN_RETENTION).into(),
            RETENTION_DELETE_BATCH.into(),
        ],
    )?;
    Ok(row_count(table.len()))
}

/// The stale-producer prune's statement: examine the next [`RETENTION_DELETE_BATCH`]
/// producer ids idle past the window in (last_active_at, producer_id) order through
/// idx_producer_ids_last_active, after the cursor (`$3`, `$4`: last_active_at in epoch
/// microseconds and the id) when there is one, and delete those not referenced by a
/// transaction row and without recent sequence activity, with their sequences. Returns
/// the rows deleted, the candidates examined and the key of the last one.
///
/// The deletes name their rows by `= ANY (ARRAY(...))`, not a join with the CTE: a join
/// lets the planner hash the candidates and read kafka.producer_ids (and, for the
/// anti-join, kafka.transactions) whole, which with many producer ids made every
/// statement cost the whole table; an array is probed through the primary keys, and the
/// anti-joins of a few rows through idx_transactions_producer and the sequences' primary
/// key.
///
/// Concurrency: `pg_kafka_run_retention_sweep()` runs this in its own session, beside
/// the worker, whose group commit holds the rows of every request in a batch until one
/// commit (a batch of Produce or InitProducerId requests updates several producer rows,
/// in request order). So the statement never waits for a producer row: it locks the
/// candidates it would delete with `FOR UPDATE SKIP LOCKED` and leaves a row another
/// transaction holds to the next lap. Waiting for one row while holding another, in
/// whatever order its index scan met them, closed a cycle with such a batch (or with
/// any session writing producer rows in another order): a deadlock. (The producers it
/// deletes stay held to the end of the pass, so no later statement of the pass may wait
/// for a row either: "Lock waits" on [`PostgresStore::run_retention_sweep`].) Locking
/// re-checks the lock query's own WHERE on the newest version of a row updated since the
/// statement's snapshot (the CTEs and subqueries keep the snapshot), so the idle
/// condition on producer_ids itself is repeated there: InitProducerId and an epoch bump
/// (KIP-360, a transactional re-init) set last_active_at, and the producer they revived
/// stays. The re-check filters the row the primary-key probe already fetched: no extra
/// read per candidate. An idempotent Produce writes the sequence rows, which the
/// subquery reads from the snapshot, so it also sets last_active_at on a producer idle
/// for more than half the window, the only ones this can take
/// (`producers::read_producer_state`), before it writes them: a Produce open while this
/// runs holds the producer row, which this skips; one that comes after this deleted the
/// row waits for this commit and starts the producer afresh. The sequence rows of a
/// locked producer are written only by a transaction that holds the producer row
/// first, so their DELETE does not wait either.
///
/// Cost: one tuple lock per producer it deletes, no more than the deletes themselves
/// (at most [`RETENTION_DELETE_BATCH`] a statement); kept candidates are not locked.
fn producer_prune_sql(with_cursor: bool) -> String {
    let after = if with_cursor {
        "AND (p.last_active_at, p.producer_id)
             > (TIMESTAMP 'epoch' + $3::bigint * INTERVAL '1 microsecond', $4::bigint)"
    } else {
        ""
    };
    format!(
        "WITH candidates AS (
             SELECT p.producer_id, p.last_active_at FROM kafka.producer_ids p
             WHERE p.last_active_at < NOW() - ($1 || ' seconds')::interval
               {after}
             ORDER BY p.last_active_at, p.producer_id
             LIMIT $2
         ),
         -- The candidates to delete, row-locked; a row another transaction holds is
         -- skipped (examined again on the next lap), so this statement never waits
         -- for a producer row and cannot close a lock cycle with a transaction that
         -- writes several of them.
         victims AS (
             SELECT p.producer_id FROM kafka.producer_ids p
             WHERE p.producer_id = ANY (ARRAY(SELECT c.producer_id FROM candidates c))
               -- Repeated from candidates: the lock re-checks this condition on the
               -- newest version of a row updated since the statement's snapshot, so
               -- an InitProducerId or epoch bump that revived an idle producer and
               -- committed meanwhile keeps it.
               AND p.last_active_at < NOW() - ($1 || ' seconds')::interval
               AND NOT EXISTS (
                   SELECT 1 FROM kafka.transactions t
                   WHERE t.producer_id = p.producer_id
               )
               -- last_active_at moves on InitProducerId and, at most every half
               -- window, on a Produce; a producer's appends since then show in its
               -- sequence rows.
               AND NOT EXISTS (
                   SELECT 1 FROM kafka.producer_sequences s
                   WHERE s.producer_id = p.producer_id
                     AND s.updated_at >= NOW() - ($1 || ' seconds')::interval
               )
             FOR UPDATE OF p SKIP LOCKED
         ),
         doomed AS (
             DELETE FROM kafka.producer_ids p
             WHERE p.producer_id = ANY (ARRAY(SELECT v.producer_id FROM victims v))
               AND p.last_active_at < NOW() - ($1 || ' seconds')::interval
             RETURNING p.producer_id
         ),
         seqs AS (
             DELETE FROM kafka.producer_sequences s
             WHERE s.producer_id = ANY (ARRAY(SELECT d.producer_id FROM doomed d))
         )
         SELECT (SELECT count(*) FROM doomed)::bigint AS deleted,
                (SELECT count(*) FROM candidates)::bigint AS examined,
                last.at_us, last.producer_id
         FROM (SELECT 1) AS one
         LEFT JOIN LATERAL (
             SELECT (EXTRACT(EPOCH FROM c.last_active_at) * 1000000)::bigint AS at_us,
                    c.producer_id
             FROM candidates c
             ORDER BY c.last_active_at DESC, c.producer_id DESC
             LIMIT 1) AS last ON true"
    )
}

/// Producers idle past [`PRODUCER_ID_RETENTION`] and not referenced by any remaining
/// transaction row, one batch from `cursor`; returns the ids deleted and the cursor for
/// the next batch ([`retention::next_prune_cursor`]: `None` once it reached the end).
/// Their sequences go with them (no foreign key between the two tables, so one
/// statement keeps it atomic).
///
/// Index-driven and bounded: producer ids are client-driven (every InitProducerId
/// without a transactional id makes one), so each statement examines at most
/// [`RETENTION_DELETE_BATCH`] candidates through idx_producer_ids_last_active (a pass
/// with none idle costs one descent), and every candidate costs index probes only
/// ([`producer_prune_sql`]), including the foreign key of kafka.transactions on each
/// deleted id, checked through idx_transactions_producer. Candidates it keeps (still
/// referenced, or active through their sequences) stay idle by last_active_at, so the
/// step resumes after the last one it examined
/// ([`retention::SweepState::producer_prune`]) and starts from the oldest again once it
/// reaches the end.
fn stale_producers_batch(
    client: &mut SpiClient<'_>,
    cursor: Option<(i64, i64)>,
) -> Result<(u64, Option<(i64, i64)>)> {
    let window = whole_seconds(PRODUCER_ID_RETENTION);
    let table = match cursor {
        None => client.update(
            &producer_prune_sql(false),
            None,
            &[window.into(), RETENTION_DELETE_BATCH.into()],
        )?,
        Some((at_us, producer_id)) => client.update(
            &producer_prune_sql(true),
            None,
            &[
                window.into(),
                RETENTION_DELETE_BATCH.into(),
                at_us.into(),
                producer_id.into(),
            ],
        )?,
    };
    let row = table.first();
    let n: i64 = row.get_by_name("deleted")?.unwrap_or(0);
    let examined: i64 = row.get_by_name("examined")?.unwrap_or(0);
    let last = match (
        row.get_by_name::<i64, _>("at_us")?,
        row.get_by_name::<i64, _>("producer_id")?,
    ) {
        (Some(at_us), Some(producer_id)) => Some((at_us, producer_id)),
        _ => None,
    };
    let examined = u64::try_from(examined).unwrap_or(0);
    Ok((
        u64::try_from(n).unwrap_or(0),
        retention::next_prune_cursor(examined, RETENTION_DELETE_BATCH, last),
    ))
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
/// claims stay HOT. One batch of [`RETENTION_DELETE_BATCH`] rows.
///
/// Rows another transaction holds (a `kafka.replay_shadow_messages` re-queueing them)
/// are skipped, not waited for: one TID-scan lock per row the batch deletes.
fn delivered_shadow_rows_batch(client: &mut SpiClient<'_>) -> Result<u64> {
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
         WHERE ctid = ANY (ARRAY(
             SELECT s.ctid FROM kafka.shadow_tracking s
             WHERE s.ctid = ANY (ARRAY(SELECT row_id FROM walk LIMIT $2))
               AND s.external_offset IS NOT NULL
             FOR UPDATE OF s SKIP LOCKED))
         RETURNING 1",
        None,
        &[
            whole_seconds(SHADOW_DELIVERED_RETENTION).into(),
            RETENTION_DELETE_BATCH.into(),
        ],
    )?;
    Ok(row_count(table.len()))
}

/// Shadow-outbox rows below their partition's log start (`kafka.partition_offsets`):
/// retention or DeleteRecords removed their message, so they can never be forwarded
/// (the claim skips them), and left behind they would keep a dead letter (an
/// `external_only` topic served locally for good, a replay unable to clear it) or a
/// pending row (lag that never reaches 0) forever. One primary-key range per
/// partition, so a partition with none costs one descent; one batch of
/// [`RETENTION_DELETE_BATCH`] rows. Its turns run after [`advance_log_start`] (see
/// [`BatchStep`]), so the rows of messages this pass removed go in the same pass when
/// the budget allows, else at the next, and DeleteRecords' (which advances the log
/// start itself) at the next pass.
///
/// Rows another transaction holds (the forwarder's ack batch of the worker's outbox
/// tick, which updates its rows in arrival order, or a replay) are skipped, not waited
/// for: one TID-scan lock per row the batch deletes. Waiting for one while holding a row
/// an earlier turn deleted closed a cycle with an ack batch that then updated that row.
fn orphaned_shadow_rows_batch(client: &mut SpiClient<'_>) -> Result<u64> {
    let table = client.update(
        "DELETE FROM kafka.shadow_tracking
         WHERE ctid = ANY (ARRAY(
             SELECT s.ctid FROM kafka.shadow_tracking s
             WHERE s.ctid = ANY (ARRAY(
                 SELECT o.row_id FROM kafka.partition_offsets po
                 CROSS JOIN LATERAL (
                     SELECT s.ctid AS row_id FROM kafka.shadow_tracking s
                     WHERE s.topic_id = po.topic_id AND s.partition_id = po.partition_id
                       AND s.local_offset < po.log_start_offset
                     ORDER BY s.local_offset LIMIT $1) o
                 WHERE po.log_start_offset > 0
                 LIMIT $1))
             FOR UPDATE OF s SKIP LOCKED))
         RETURNING 1",
        None,
        &[RETENTION_DELETE_BATCH.into()],
    )?;
    Ok(row_count(table.len()))
}

#[cfg(test)]
mod tests {
    use super::{producer_prune_sql, AffectedPartitions};

    /// The prune locks its victims without waiting and repeats the idle condition on
    /// the row itself: it is all the lock re-checks on a concurrently revived producer.
    #[test]
    fn producer_prune_locks_without_waiting_and_rechecks_the_idle_window() {
        for with_cursor in [false, true] {
            let sql = producer_prune_sql(with_cursor);
            let victims = sql
                .split("victims AS (")
                .nth(1)
                .and_then(|rest| rest.split("doomed AS (").next())
                .unwrap_or_default();
            assert!(
                victims.contains("AND p.last_active_at < NOW() - ($1 || ' seconds')::interval"),
                "the lock must re-check last_active_at: {victims}"
            );
            assert!(
                victims.contains("FOR UPDATE OF p SKIP LOCKED"),
                "the prune must skip rows others hold: {victims}"
            );
            let delete = sql
                .split("DELETE FROM kafka.producer_ids p")
                .nth(1)
                .unwrap_or_default();
            assert!(
                delete.contains("FROM victims v"),
                "the DELETE must take only locked rows: {delete}"
            );
        }
    }

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
