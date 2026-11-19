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
    log_txn_error, run_maintenance_task, run_request_in_subtransaction, worker_transaction,
    TaskNames, TxnError,
};
use crate::kafka::metrics::MetricsFlusher;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::storage::postgres::ABORTED_MESSAGE_GRACE;
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

/// SH-9: one tick of the durable shadow outbox: apply the forward acks the network
/// thread returned, then dispatch the next due batch. Failures (for example a dropped
/// schema) pause the tick exponentially and are logged sparsely (`note_outbox_tick`).
pub(super) fn tick_shadow_outbox(shadow_store: &ShadowStore<PostgresStore>) {
    let outcome = worker_transaction(|| {
        // The acks are applied first (marking delivered rows done so the poll does not
        // re-dispatch them), in a subtransaction of their own: they are consumed from
        // the channel as they are read, so if they shared the poll's subtransaction a
        // Postgres ERROR in the poll would roll back the applied acks and lose them
        // (delivered rows left pending, re-sent, and eventually dead-lettered).
        let mut applied = 0;
        let mut failure: Option<String> = None;
        if let Err(panic_msg) = run_request_in_subtransaction(|| {
            applied = shadow_store.drain_forward_acks();
        }) {
            failure = Some(format!("ack application failed, rolled back: {panic_msg}"));
        }
        let mut poll_error: Option<String> = None;
        let result = run_request_in_subtransaction(|| {
            match shadow_store.poll_and_forward_outbox() {
                Ok(dispatched) => {
                    if applied > 0 || dispatched > 0 {
                        tracing::debug!(
                            "Shadow outbox: {applied} ack(s) applied, {dispatched} row(s) dispatched"
                        );
                    }
                }
                Err(e) => poll_error = Some(format!("poll failed: {e:?}")),
            }
        });
        if let Err(panic_msg) = result {
            poll_error = Some(format!("poll failed, rolled back: {panic_msg}"));
        }
        match failure.or(poll_error) {
            Some(msg) => {
                if shadow_store.note_outbox_tick(false) {
                    pg_warning!("Shadow outbox {msg} (worker survived; backing off)");
                }
            }
            None => {
                shadow_store.note_outbox_tick(true);
            }
        }
    });
    if let Err(e) = outcome {
        log_txn_error("Shadow outbox poll", &e);
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
/// long-poll safety net.
pub(super) fn check_transaction_timeouts(notify_tx: &Sender<InternalNotification>) {
    let names = TaskNames {
        task: "Transaction-timeout check",
        panic: "Transaction-timeout maintenance",
    };
    let mut aborted_any = false;
    let ran = run_maintenance_task(names, || {
        let store = PostgresStore::new();
        match store.abort_timed_out_transactions(DEFAULT_TRANSACTION_TIMEOUT) {
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
        ) {
            Ok(stats) if stats.total() > 0 => pg_log!(
                "Retention sweep: {} aborted msg(s), {} expired msg(s), \
                 {} time partition(s) dropped, \
                 {} stale producer(s), {} terminal txn(s), \
                 {} delivered shadow row(s) reclaimed",
                stats.aborted_messages,
                stats.expired_messages,
                stats.dropped_partitions,
                stats.stale_producers,
                stats.terminal_transactions,
                stats.shadow_delivered_rows
            ),
            Ok(_) => {}
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
