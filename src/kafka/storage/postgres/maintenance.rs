//! Background maintenance SQL: the retention sweep, time-partition maintenance,
//! timed-out transactions and aborted-row cleanup.
//!
//! The sweep's decisions (which window applies, what to do with a leaf, when to stop)
//! are pure functions in [`retention`]; this module runs the statements they call for.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::{internal, PostgresStore};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::retention::{
    self, LeafAction, TimePartition, RETENTION_DELETE_BATCH, RETENTION_PASS_BUDGET,
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

/// One unit of row-level expiry: delete `topic_id`'s rows older than `window_ms` in
/// the leaf `leaf_oid`.
#[derive(Debug, Clone, Copy)]
struct ExpiryWork {
    topic_id: i32,
    window_ms: i64,
    leaf_oid: u32,
}

/// What one sweep pass does with the messages: whole-leaf drops and row deletes.
#[derive(Default)]
struct ExpiryPlan {
    /// Leaves to drop, with the topics found wholly expired in each (the drop function
    /// re-checks them under its lock).
    drops: Vec<(u32, Vec<i32>)>,
    /// Row deletes, in the order they run.
    work: Vec<ExpiryWork>,
}

/// Every topic's effective retention window.
struct TopicWindows {
    /// `None`: kept forever.
    by_topic: HashMap<i32, Option<i64>>,
    /// The topics with a window, rotated so a pass that runs out of budget does not
    /// starve the topics after the one it stopped at.
    finite: Vec<(i32, i64)>,
}

impl TopicWindows {
    fn window(&self, topic_id: i32) -> Option<i64> {
        self.by_topic.get(&topic_id).copied().flatten()
    }
}

/// `(topic_id, partition_id)` pairs whose log start a sweep must advance.
type AffectedPartitions = HashSet<(i32, i32)>;

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
    /// and delivered shadow-outbox rows.
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
    pub fn run_retention_sweep(
        &self,
        message_retention_hours: i32,
        aborted_grace: Duration,
        partition_minutes: i32,
    ) -> Result<RetentionSweepStats> {
        let mut stats = RetentionSweepStats {
            aborted_messages: Self::cleanup_aborted_messages(aborted_grace)?,
            ..Default::default()
        };
        let width_ms = retention::partition_width_ms(partition_minutes);

        Spi::connect_mut(|client| {
            let windows = load_topic_windows(client, message_retention_hours)?;
            let now_ms = database_now_ms(client)?;
            let partitioned = has_time_partitions(client)?;
            let leaves = list_leaves(client, partitioned)?;
            let plan = plan_expiry(client, &leaves, &windows, now_ms, width_ms)?;

            let mut affected = AffectedPartitions::new();
            stats.expired_messages =
                delete_expired_rows(client, &plan.work, &leaves, partitioned, &mut affected)?;
            // Whole-leaf drops last, so the ACCESS EXCLUSIVE lock a drop takes on
            // kafka.messages is held only until this transaction commits.
            stats.dropped_partitions = drop_leaves(client, plan.drops, &mut affected)?;
            advance_log_start(client, affected)?;

            stats.terminal_transactions = delete_terminal_transactions(client)?;
            stats.stale_producers = delete_stale_producers(client)?;
            stats.shadow_delivered_rows = delete_delivered_shadow_rows(client)?;
            Ok(stats)
        })
        .map_err(internal("run_retention_sweep"))
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

                // Fence the timed-out producer, as Kafka does, by bumping its epoch:
                // otherwise the stalled producer could resume with the same epoch
                // (AddPartitionsToTxn accepts CompleteAbort -> Ongoing), add more
                // partitions and commit only the part written after the abort. The
                // guard leaves the epoch alone if it already moved or is exhausted
                // (the next InitProducerId then allocates a fresh producer id).
                let bumped = client.update(
                    "UPDATE kafka.producer_ids SET epoch = epoch + 1
                     WHERE producer_id = $1 AND epoch = $2 AND epoch < $3
                     RETURNING epoch",
                    None,
                    &[producer_id.into(), producer_epoch.into(), i16::MAX.into()],
                )?;
                let new_epoch: i16 = if bumped.is_empty() {
                    producer_epoch
                } else {
                    bumped.first().get_by_name("epoch")?.unwrap_or(producer_epoch)
                };
                // The new epoch restarts sequence numbers.
                client.update(
                    "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
                    None,
                    &[producer_id.into()],
                )?;
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

    pub(super) fn cleanup_aborted_messages(older_than: Duration) -> Result<u64> {
        let older_than_secs = whole_seconds(older_than);
        crate::pg_debug!("PostgresStore::cleanup_aborted_messages: older_than={older_than_secs}s");

        Spi::connect_mut(|client| {
            // client.update, not client.select: select runs SPI read-only, and Postgres
            // rejects DML there ("DELETE is not allowed in a non-volatile function").
            let table = client.update(
                "DELETE FROM kafka.messages
                 WHERE txn_state = 'aborted'
                   AND created_at < NOW() - ($1 || ' seconds')::interval
                 RETURNING 1",
                None,
                &[older_than_secs.into()],
            )?;
            let deleted = row_count(table.len());
            crate::pg_debug!("Deleted {deleted} aborted messages");
            Ok(deleted)
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
    let finite = retention::rotated(&finite, retention::next_start_index(finite.len()));
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

/// Decide the pass's work: inspect every past time leaf (drop it whole, trim it, or
/// keep it), then add the row-level expiry: short windows everywhere they may have
/// expired rows, longer windows only in the DEFAULT leaf (the rest is leaf-granular).
fn plan_expiry(
    client: &SpiClient<'_>,
    leaves: &[Leaf],
    windows: &TopicWindows,
    now_ms: i64,
    width_ms: i64,
) -> Result<ExpiryPlan> {
    let partitions: Vec<TimePartition> = leaves.iter().map(|l| l.partition.clone()).collect();
    let mut plan = ExpiryPlan::default();
    for leaf in retention::past_time_partitions(&partitions, now_ms) {
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
                if plan.drops.len() < retention::MAX_PARTITION_DROPS_PER_PASS {
                    plan.drops.push((leaf.oid, expired_topics));
                }
            }
            LeafAction::Trim { topics } => {
                plan.work.extend(topics.into_iter().filter_map(|topic_id| {
                    windows.window(topic_id).map(|window_ms| ExpiryWork {
                        topic_id,
                        window_ms,
                        leaf_oid: leaf.oid,
                    })
                }));
            }
            LeafAction::Keep => {}
        }
    }
    for &(topic_id, window_ms) in &windows.finite {
        for leaf in &partitions {
            if plan.drops.iter().any(|(oid, _)| *oid == leaf.oid) {
                continue;
            }
            let in_scope = leaf.is_default || retention::row_level_expiry(window_ms, width_ms);
            if in_scope && retention::may_hold_expired(leaf, now_ms, window_ms) {
                plan.work.push(ExpiryWork {
                    topic_id,
                    window_ms,
                    leaf_oid: leaf.oid,
                });
            }
        }
    }
    Ok(plan)
}

/// Run the row deletes in [`RETENTION_DELETE_BATCH`] batches per work item, until
/// caught up (a short batch) or the pass has spent [`RETENTION_PASS_BUDGET`]. Returns
/// the number of rows deleted and records the partitions they came from.
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
) -> Result<u64> {
    let default_oid = leaves
        .iter()
        .find(|l| l.partition.is_default)
        .map(|l| l.partition.oid);
    let pass_start = Instant::now();
    let mut expired = 0u64;
    'work: for item in work {
        loop {
            if pass_start.elapsed() >= RETENTION_PASS_BUDGET {
                break 'work;
            }
            let target = if !partitioned {
                DeleteTarget::Unpartitioned
            } else if Some(item.leaf_oid) == default_oid {
                DeleteTarget::DefaultLeaf
            } else {
                DeleteTarget::Leaf(leaves.iter().find(|l| l.partition.oid == item.leaf_oid))
            };
            let deleted = delete_expired_batch(client, item, target, affected)?;
            expired += deleted;
            if !retention::should_run_another_batch(
                deleted,
                RETENTION_DELETE_BATCH,
                pass_start.elapsed(),
                RETENTION_PASS_BUDGET,
            ) {
                break;
            }
        }
    }
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

/// Delete one batch of at most [`RETENTION_DELETE_BATCH`] of `item`'s expired rows
/// from `target`, record the partitions they came from, and return how many were
/// deleted.
fn delete_expired_batch(
    client: &mut SpiClient<'_>,
    item: &ExpiryWork,
    target: DeleteTarget<'_>,
    affected: &mut AffectedPartitions,
) -> Result<u64> {
    let table = match target {
        DeleteTarget::Unpartitioned => client.update(
            "DELETE FROM kafka.messages
             WHERE ctid IN (
                 SELECT d.ctid FROM kafka.messages d
                 WHERE d.topic_id = $1
                   AND d.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                   AND (d.txn_state IS NULL OR d.txn_state <> 'pending')
                 LIMIT $3
             )
             RETURNING topic_id, partition_id",
            None,
            &[
                item.topic_id.into(),
                item.window_ms.into(),
                RETENTION_DELETE_BATCH.into(),
            ],
        )?,
        DeleteTarget::DefaultLeaf => client.update(
            "DELETE FROM kafka.messages_default
             WHERE ctid IN (
                 SELECT d.ctid FROM kafka.messages_default d
                 WHERE d.topic_id = $1
                   AND d.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                   AND (d.txn_state IS NULL OR d.txn_state <> 'pending')
                 LIMIT $3
             )
             RETURNING topic_id, partition_id",
            None,
            &[
                item.topic_id.into(),
                item.window_ms.into(),
                RETENTION_DELETE_BATCH.into(),
            ],
        )?,
        DeleteTarget::Leaf(leaf) => {
            let lower = leaf.and_then(|l| l.lower_txt.clone());
            let upper = leaf.and_then(|l| l.upper_txt.clone());
            client.update(
                "DELETE FROM kafka.messages
                 WHERE created_at >= COALESCE($4::text::timestamptz, '-infinity')
                   AND created_at < COALESCE($5::text::timestamptz, 'infinity')
                   AND tableoid = $6::bigint::oid
                   AND ctid = ANY (ARRAY(
                       SELECT m.ctid FROM kafka.messages m
                       WHERE m.created_at >= COALESCE($4::text::timestamptz, '-infinity')
                         AND m.created_at < COALESCE($5::text::timestamptz, 'infinity')
                         AND m.tableoid = $6::bigint::oid
                         AND m.topic_id = $1
                         AND m.created_at < NOW() - $2::bigint * INTERVAL '1 millisecond'
                         AND (m.txn_state IS NULL OR m.txn_state <> 'pending')
                       LIMIT $3))
                 RETURNING topic_id, partition_id",
                None,
                &[
                    item.topic_id.into(),
                    item.window_ms.into(),
                    RETENTION_DELETE_BATCH.into(),
                    lower.into(),
                    upper.into(),
                    i64::from(item.leaf_oid).into(),
                ],
            )?
        }
    };
    let mut deleted = 0u64;
    for row in table {
        deleted += 1;
        let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
        let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
        affected.insert((topic_id, partition_id));
    }
    Ok(deleted)
}

/// Drop the planned leaves (each re-checked under its lock by
/// `kafka.drop_message_partition`). Returns how many were dropped and records the
/// partitions they held.
fn drop_leaves(
    client: &mut SpiClient<'_>,
    drops: Vec<(u32, Vec<i32>)>,
    affected: &mut AffectedPartitions,
) -> Result<u64> {
    let mut dropped = 0;
    for (leaf_oid, expired_topics) in drops {
        let row = client.update(
            "SELECT dropped, topic_ids, partition_ids
             FROM kafka.drop_message_partition($1::bigint::oid::regclass, $2::int4[])",
            None,
            &[i64::from(leaf_oid).into(), expired_topics.into()],
        )?;
        let row = row.first();
        if row.get_by_name::<bool, _>("dropped")?.unwrap_or(false) {
            dropped += 1;
            let topic_ids: Vec<Option<i32>> = row.get_by_name("topic_ids")?.unwrap_or_default();
            let partition_ids: Vec<Option<i32>> =
                row.get_by_name("partition_ids")?.unwrap_or_default();
            affected.extend(topic_ids.into_iter().zip(partition_ids).filter_map(
                |pair| match pair {
                    (Some(t), Some(p)) => Some((t, p)),
                    _ => None,
                },
            ));
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
    let (topic_ids, partition_ids): (Vec<i32>, Vec<i32>) = affected.into_iter().unzip();
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
fn delete_delivered_shadow_rows(client: &mut SpiClient<'_>) -> Result<u64> {
    let table = client.update(
        "DELETE FROM kafka.shadow_tracking
         WHERE external_offset IS NOT NULL
           AND forwarded_at < NOW() - ($1 || ' seconds')::interval
         RETURNING 1",
        None,
        &[whole_seconds(SHADOW_DELIVERED_RETENTION).into()],
    )?;
    Ok(row_count(table.len()))
}
