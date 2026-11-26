//! The DB thread's periodic maintenance tasks and their schedule: time partitions,
//! shadow configuration and outbox, group member and transaction timeouts, the
//! retention sweep and the broker-metrics flush.
//!
//! Each task runs in its own worker transaction, guarded by a subtransaction
//! ([`run_maintenance_task`]), so a Postgres ERROR in one is logged and rolled back
//! instead of ending the worker.

use crossbeam_channel::Sender;
use pgrx::prelude::*;
use std::time::{Duration, Instant};

use super::batch::notify_transaction_ended;
use super::network::Inbox;
use super::txn::{
    log_txn_error, run_in_subtransaction, run_maintenance_task, run_request_in_subtransaction,
    worker_transaction, TaskNames, TxnError,
};
use crate::kafka::metrics::MetricsFlusher;
use crate::kafka::shadow::store::TickOutcome;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::storage::postgres::ABORTED_MESSAGE_GRACE;
use crate::kafka::storage::retention::MaintenanceLockWait;
use crate::kafka::storage::MaintenanceStore;
use crate::kafka::{GroupCoordinator, InternalNotification, PostgresStore};
use crate::{pg_log, pg_warning};

/// How often to check for timed-out group members.
pub(super) const MEMBER_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often to check for timed-out transactions (less often than member timeouts).
pub(super) const TXN_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// DR-1/DR-2: how often the storage-lifecycle retention sweep runs (reclaim aborted
/// rows, enforce message retention, prune stale producers, terminal transactions and
/// delivered shadow-outbox rows). Each pass is bounded, so a large backlog drains
/// across sweeps.
pub(super) const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// The timeout a transaction gets when it has none of its own.
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

/// A task that runs every `every`, measured from the end of its last run.
#[derive(Debug)]
pub(super) struct Periodic {
    every: Duration,
    last: Instant,
}

impl Periodic {
    /// A task first due `every` from now.
    pub(super) fn new(every: Duration) -> Self {
        Periodic {
            every,
            last: Instant::now(),
        }
    }

    /// Whether the task is due.
    pub(super) fn is_due(&self) -> bool {
        self.last.elapsed() >= self.every
    }

    /// The task ran now; the next run is due `every` from now.
    pub(super) fn ran(&mut self) {
        self.last = Instant::now();
    }

    /// Change the interval (a reloaded GUC).
    pub(super) fn set_interval(&mut self, every: Duration) {
        self.every = every;
    }
}

/// ADR-001: one time-partition maintenance pass (`PostgresStore::ensure_time_partitions`):
/// leaves for now and the next widths, and rows caught by kafka.messages_default moved
/// into their leaf.
/// The worker runs no maintenance on a schema older than the library (see
/// `schema::wait_for_schema_update`), so every object used here exists.
pub(super) fn run_partition_maintenance() {
    let names = TaskNames {
        task: "Partition maintenance",
        panic: "Partition maintenance",
    };
    run_maintenance_task(names, || {
        let store = PostgresStore::new();
        match store.ensure_time_partitions(crate::config::MESSAGE_PARTITION_MINUTES.get()) {
            Ok(created) if created > 0 => {
                pg_log!("Partition maintenance: created {created} time partition(s)");
            }
            Ok(_) => {}
            Err(e) => pg_warning!("Partition maintenance failed: {e}"),
        }
    });
}

/// (Re)load the per-topic shadow configuration into the ShadowStore cache.
pub(super) fn load_shadow_config(shadow_store: &ShadowStore<PostgresStore>) {
    let outcome = worker_transaction(|| match shadow_store.load_topic_config_from_db() {
        Ok(count) if count > 0 => log!("Loaded {count} shadow topic configurations"),
        Ok(_) => {}
        Err(e) => pg_warning!("Failed to load shadow config: {e:?}"),
    });
    if let Err(e) = outcome {
        log_txn_error("Shadow config load", &e);
    }
}

/// The shadow part of a configuration reload: the per-topic configuration, the
/// primary/standby status, and the metrics flush.
pub(super) fn reload_shadow_config(shadow_store: &ShadowStore<PostgresStore>) {
    let names = TaskNames {
        task: "Shadow config reload",
        panic: "Shadow config-reload/metrics maintenance",
    };
    run_maintenance_task(names, || {
        match shadow_store.load_topic_config_from_db() {
            Ok(count) => pg_log!("Reloaded {count} shadow topic configurations"),
            Err(e) => pg_warning!("Failed to reload shadow config: {e:?}"),
        }
        // SH-2: is_primary() caches its result for the life of the worker; refreshing
        // it on the reload cycle makes a standby promoted to primary start forwarding
        // within one interval.
        let is_primary = shadow_store.refresh_primary_status();
        tracing::debug!("Shadow: refreshed primary status (is_primary={is_primary})");
        if let Err(e) = shadow_store.flush_metrics_to_db() {
            pg_warning!("Failed to flush shadow metrics: {e:?}");
        }
    });
}

/// The `lock_timeout` of an outbox tick's statements, far below
/// `pg_kafka.worker_lock_timeout_ms` (5 s by default).
const OUTBOX_TICK_LOCK_TIMEOUT_MS: i32 = 50;

/// SH-9: one tick of the durable shadow outbox: apply the forward acks the network
/// thread returned, then dispatch the next due batch, in one transaction; the applied
/// acks are made final after its commit. Failures (for example a dropped schema) pause
/// the tick exponentially and are logged sparsely (`note_outbox_tick`).
///
/// Lock waits: the tick's statements update outbox rows, which other sessions hold
/// across statements: `kafka.replay_shadow_messages` holds every row it re-queues until
/// its caller commits (seconds, or as long as an operator keeps the transaction open),
/// and an on-demand retention sweep holds the rows it reaped until its pass commits. The
/// claim, the ack statements and the dead-flag sync never wait for such a row: they lock
/// theirs `FOR UPDATE SKIP LOCKED`, the claim cuts each partition at its first held row
/// (so a held row stops only its own partition, in order) and a skipped ack stays queued
/// for a later tick (see `SpiShadowDb` in `shadow/outbox.rs`). What can still wait (a
/// relation lock held by DDL, a `kafka.shadow_metrics` row an operator holds) runs with a
/// transaction-local `lock_timeout` of [`OUTBOX_TICK_LOCK_TIMEOUT_MS`] (or the worker's,
/// if lower): a wait that runs out fails the ack subtransaction or the poll like any
/// ERROR, and the next tick comes after the usual interval
/// ([`TickOutcome::LockTimedOut`]: no exponential backoff, which kept forwarding stopped
/// up to 30 s after the holder was gone). A failed ack application keeps its acks (they
/// stay in `unapplied_acks` until a tick that applied them commits) and their rows in
/// flight, so nothing is lost or sent twice. A cycle cannot outlast the timeout: the
/// tick's wait ends within it, long before the other session's deadlock check
/// (deadlock_timeout, 1 s by default) for a wait it began after the tick took its rows.
pub(super) fn tick_shadow_outbox(shadow_store: &ShadowStore<PostgresStore>) {
    let outcome = worker_transaction(|| {
        bound_lock_waits(OUTBOX_TICK_LOCK_TIMEOUT_MS, "Shadow outbox");
        // The acks are applied first (marking delivered rows done so the poll does not
        // re-dispatch them), in a subtransaction of their own, so a Postgres ERROR in
        // the poll does not roll their application back with it.
        let mut applied = 0;
        let mut failure: Option<String> = None;
        let mut lock_timeouts_only = true;
        if let Err(panic) = run_in_subtransaction(|| {
            applied = shadow_store.drain_forward_acks();
        }) {
            lock_timeouts_only &= panic.lock_timeout;
            failure = Some(format!(
                "ack application failed, rolled back: {}",
                panic.message
            ));
        }
        let mut poll_error: Option<String> = None;
        let result = run_in_subtransaction(|| match shadow_store.poll_and_forward_outbox() {
            Ok(dispatched) => {
                if applied > 0 || dispatched > 0 {
                    tracing::debug!(
                        "Shadow outbox: {applied} ack(s) applied, {dispatched} row(s) dispatched"
                    );
                }
            }
            Err(e) => {
                lock_timeouts_only = false;
                poll_error = Some(format!("poll failed: {e:?}"));
            }
        });
        if let Err(panic) = result {
            lock_timeouts_only &= panic.lock_timeout;
            poll_error = Some(format!("poll failed, rolled back: {}", panic.message));
        }
        if poll_error.is_some() {
            shadow_store.discard_staged_poll();
        }
        match failure.or(poll_error) {
            Some(msg) if lock_timeouts_only => {
                if shadow_store.note_outbox_tick(TickOutcome::LockTimedOut) {
                    pg_warning!(
                        "Shadow outbox {msg} (another session holds a lock the tick needs; \
                         retrying every tick)"
                    );
                }
            }
            Some(msg) => {
                if shadow_store.note_outbox_tick(TickOutcome::Failed) {
                    pg_warning!("Shadow outbox {msg} (worker survived; backing off)");
                }
            }
            None => {
                shadow_store.note_outbox_tick(TickOutcome::Ok);
            }
        }
    });
    match outcome {
        // The acks applied in it are final only now (a failed COMMIT rolls them
        // back, and the next tick applies them again).
        Ok(()) => shadow_store.commit_applied_acks(),
        Err(e) => log_txn_error("Shadow outbox poll", &e),
    }
}

/// Set this transaction's `lock_timeout` to `bound_ms`, or to
/// `pg_kafka.worker_lock_timeout_ms` when that is lower (0 there means no timeout).
/// `task` names the caller in the WARNING if it cannot be set.
fn bound_lock_waits(bound_ms: i32, task: &str) {
    let ms = bounded_lock_timeout_ms(crate::config::WORKER_LOCK_TIMEOUT_MS.get(), bound_ms);
    if let Err(e) = Spi::run(&format!(
        "SELECT set_config('lock_timeout', '{ms}ms', true)"
    )) {
        pg_warning!("{task}: could not bound its lock waits: {e}");
    }
}

/// The lock_timeout of a background task bounded by `bound_ms`, for a worker
/// lock_timeout of `worker_ms` (0: none).
fn bounded_lock_timeout_ms(worker_ms: i32, bound_ms: i32) -> i32 {
    if worker_ms > 0 {
        worker_ms.min(bound_ms)
    } else {
        bound_ms
    }
}

/// Remove group members whose session timed out (each removal starts a rebalance).
pub(super) fn check_member_timeouts(coordinator: &GroupCoordinator) {
    for (group_id, member_id) in coordinator.check_and_remove_timed_out_members() {
        pg_log!("Timeout: Removed member {member_id} from group {group_id}, triggering rebalance");
    }
}

/// Abort transactions that outlived their timeout. An abort ends transactions, so
/// parked read_committed fetches are woken now (their LSO moved) rather than on the
/// long-poll safety net. The shadow store keeps nothing for an open transaction (a
/// commit enqueues its records into the outbox from kafka.messages), so an abort
/// leaves it nothing to undo.
pub(super) fn check_transaction_timeouts(
    shadow_store: &ShadowStore<PostgresStore>,
    notify_tx: &Sender<InternalNotification>,
) {
    let names = TaskNames {
        task: "Transaction-timeout check",
        panic: "Transaction-timeout maintenance",
    };
    let mut aborted_any = false;
    let ran = run_maintenance_task(names, || {
        match shadow_store.abort_timed_out_transactions(DEFAULT_TRANSACTION_TIMEOUT) {
            Ok(aborted) => {
                aborted_any = !aborted.is_empty();
                for txn_id in aborted {
                    pg_log!("Transaction timeout: Aborted timed-out transaction '{txn_id}'");
                }
            }
            Err(e) => pg_warning!("Failed to check transaction timeouts: {e}"),
        }
    });
    if ran && aborted_any {
        notify_transaction_ended(notify_tx);
    }
}

/// DR-1/DR-2: one bounded storage-lifecycle retention sweep, honouring
/// `pg_kafka.message_retention_hours` (0 = messages kept forever). The time partitions
/// are brought up to date first, so the sweep decides which leaves to drop on the
/// current set.
///
/// The worker never waits for the partition-maintenance lock (its transaction holds the
/// schema lock, which a manual maintenance pass holding that lock may be waiting for):
/// while another session holds it, this pass is skipped. Any ERROR in the pass (a lock
/// timeout, a deadlock) rolls back its subtransaction only; the next interval retries.
pub(super) fn run_retention_sweep() {
    run_partition_maintenance();
    let names = TaskNames {
        task: "Retention sweep",
        panic: "Retention-sweep maintenance",
    };
    run_maintenance_task(names, || {
        let store = PostgresStore::new();
        match store.run_retention_sweep(
            crate::config::MESSAGE_RETENTION_HOURS.get(),
            ABORTED_MESSAGE_GRACE,
            crate::config::MESSAGE_PARTITION_MINUTES.get(),
            MaintenanceLockWait::Skip,
        ) {
            Ok(None) => pg_warning!(
                "Retention sweep skipped: another session holds the maintenance lock; will retry"
            ),
            Ok(Some(stats)) if stats.total() > 0 => pg_log!(
                "Retention sweep: {} aborted msg(s), {} expired msg(s), \
                 {} time partition(s) dropped, \
                 {} stale producer(s), {} terminal txn(s), \
                 {} delivered and {} orphaned shadow row(s) reclaimed",
                stats.aborted_messages,
                stats.expired_messages,
                stats.dropped_partitions,
                stats.stale_producers,
                stats.terminal_transactions,
                stats.shadow_delivered_rows,
                stats.shadow_orphaned_rows
            ),
            Ok(Some(_)) => {}
            Err(e) => pg_warning!("Retention sweep failed: {e}"),
        }
    });
}

/// Publish the broker request metrics to kafka.broker_api_stats / kafka.broker_gauges:
/// at most once per second and only when they changed (one upsert statement, no SQL at
/// all otherwise).
pub(super) fn flush_broker_metrics(flusher: &mut MetricsFlusher, inbox: &Inbox) {
    let Some(batch) = flusher.poll(inbox.requests.len(), inbox.liveness.len()) else {
        return;
    };
    let written = worker_transaction(|| {
        let mut written = Ok(());
        let guard = run_request_in_subtransaction(|| {
            written = crate::kafka::metrics::write_batch(&batch);
        });
        guard.and(written)
    });
    let written = match written {
        Ok(w) => w,
        // The schema wait reports a dropped extension; nothing to log here.
        Err(TxnError::SchemaGone) => return,
        Err(e) => Err(format!("{e:?}")),
    };
    if let Some(msg) = flusher.finish(batch, written) {
        pg_warning!("{}", msg);
    }
}

#[cfg(test)]
mod tests {
    use super::{bounded_lock_timeout_ms, OUTBOX_TICK_LOCK_TIMEOUT_MS};

    #[test]
    fn background_lock_timeouts_are_the_lower_bound_and_never_unbounded() {
        for bound in [OUTBOX_TICK_LOCK_TIMEOUT_MS] {
            assert_eq!(bounded_lock_timeout_ms(5000, bound), bound);
            assert_eq!(bounded_lock_timeout_ms(0, bound), bound);
            assert_eq!(bounded_lock_timeout_ms(-1, bound), bound);
            assert_eq!(bounded_lock_timeout_ms(1, bound), 1);
        }
    }
}
