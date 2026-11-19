//! Periodic maintenance on the DB thread: time partitions, shadow configuration.

use std::time::Duration;

use super::schema::SchemaFeatures;
use super::txn::{log_txn_error, run_request_in_subtransaction, worker_transaction};
use crate::kafka::shadow::ShadowStore;
use crate::kafka::PostgresStore;
use crate::{pg_log, pg_warning};
use pgrx::prelude::*;

/// How often to check for member timeouts
pub(super) const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often to check for timed-out transactions (less frequent than member timeouts)
pub(super) const TXN_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// DR-1/DR-2 (DEEP-REVIEW-2026-07): how often the storage-lifecycle retention sweep
/// runs (reclaim aborted rows, enforce message retention, prune stale
/// producers/terminal transactions/delivered shadow-outbox rows). Each pass is
/// bounded (RETENTION_DELETE_BATCH), so a large backlog drains across sweeps.
pub(super) const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

// Configuration reload interval is now controlled by pg_kafka.config_reload_interval_ms GUC
// (default 30s, tests can set to 1-2s for fast iteration)

/// ADR-001 (0.1.0): one time-partition maintenance pass in its own transaction
/// (`PostgresStore::ensure_time_partitions`): leaves for now and the next widths, and
/// rows caught by kafka.messages_default moved into their leaf. Guarded like the other
/// maintenance blocks, so an ERROR (for example a schema that predates 0.1.0, before
/// `ALTER EXTENSION pg_kafka UPDATE`) is logged and the worker carries on.
///
/// Skipped on a schema without time partitions (an older extension version, see
/// [`SchemaFeatures`]): the old-schema WARNING already said so, once.
pub(super) fn run_partition_maintenance(features: &SchemaFeatures) {
    if !features.time_partitions {
        return;
    }
    let outcome = worker_transaction(|| {
        let result = run_request_in_subtransaction(|| {
            let store = PostgresStore::new();
            match store.ensure_time_partitions(crate::config::MESSAGE_PARTITION_MINUTES.get()) {
                Ok(created) if created > 0 => {
                    pg_log!(
                        "Partition maintenance: created {} time partition(s)",
                        created
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    pg_warning!("Partition maintenance failed: {}", e);
                }
            }
        });
        if let Err(panic_msg) = result {
            pg_warning!(
                "Partition maintenance panicked, rolled back (worker survived): {}",
                panic_msg
            );
        }
    });
    if let Err(e) = outcome {
        log_txn_error("Partition maintenance", &e);
    }
}

/// (Re)load the per-topic shadow configuration into the ShadowStore cache.
pub(super) fn load_shadow_config(shadow_store: &std::sync::Arc<ShadowStore<PostgresStore>>) {
    let outcome = worker_transaction(|| match shadow_store.load_topic_config_from_db() {
        Ok(count) => {
            if count > 0 {
                log!("Loaded {} shadow topic configurations", count);
            }
        }
        Err(e) => {
            pg_warning!("Failed to load shadow config: {:?}", e);
        }
    });
    if let Err(e) = outcome {
        log_txn_error("Shadow config load", &e);
    }
}
