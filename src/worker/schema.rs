//! The schema the worker runs against: waiting for it at start and after DROP EXTENSION,
//! re-deciding the worker's role when it changes, and detecting what an older extension
//! version lacks.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use pgrx::{spi, Spi};
use std::time::{Duration, Instant};

use super::batch::reject_request;
use super::lifecycle::{postmaster_alive, sigterm_received};
use super::network::Inbox;
use super::txn::{schema_found_again, worker_transaction};
use super::RECV_TIMEOUT;
use crate::kafka::ERROR_KAFKA_STORAGE_ERROR;
use crate::pg_warning;

/// How often the schema waits re-check for the kafka schema.
const SCHEMA_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// How often [`wait_for_schema`] repeats its "waiting for schema" LOG line.
const SCHEMA_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Every table the runtime depends on, so a partial or stale schema is caught at worker
/// start rather than as a late SPI failure on the first produce, transaction or shadow
/// operation. `CREATE EXTENSION pg_kafka` creates all of them.
const REQUIRED_TABLES: [&str; 11] = [
    "topics",
    "messages",
    "consumer_offsets",
    "partition_offsets",
    "producer_ids",
    "producer_sequences",
    "transactions",
    "txn_pending_offsets",
    "shadow_config",
    "shadow_metrics",
    "shadow_tracking",
];

/// Which optional parts of the schema this database has. A new binary can run against
/// the schema of an older extension version until `ALTER EXTENSION pg_kafka UPDATE`
/// (docs/UPGRADING.md step 2): it then skips what that schema lacks instead of failing
/// every cycle.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SchemaFeatures {
    /// `extversion` of the installed extension.
    pub(super) version: String,
    /// kafka.messages is time-partitioned (0.1.0, ADR-001): partition maintenance and
    /// the partition-aware retention sweep.
    pub(super) time_partitions: bool,
    /// kafka.broker_api_stats / kafka.broker_gauges exist (broker metrics flush).
    pub(super) broker_metrics: bool,
    /// kafka.sasl_credentials exists (SASL users).
    pub(super) sasl_credentials: bool,
}

impl SchemaFeatures {
    /// Human-readable list of what this schema lacks (empty: nothing).
    fn missing(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if !self.time_partitions {
            out.push("time partitions of kafka.messages (partition maintenance; retention runs row by row)");
        }
        if !self.broker_metrics {
            out.push("broker metrics (kafka.broker_api_stats / kafka.broker_gauges)");
        }
        if !self.sasl_credentials {
            out.push("SASL users (kafka.sasl_credentials)");
        }
        out
    }
}

/// Read the [`SchemaFeatures`] of the current database (inside a transaction).
fn detect_schema_features() -> SchemaFeatures {
    Spi::connect(|client| {
        let row = client
            .select(
                "SELECT (SELECT extversion FROM pg_extension WHERE extname = 'pg_kafka') AS v,
                        to_regprocedure('kafka.message_partitions()') IS NOT NULL AS tp,
                        to_regclass('kafka.broker_api_stats') IS NOT NULL
                          AND to_regclass('kafka.broker_gauges') IS NOT NULL AS bm,
                        to_regclass('kafka.sasl_credentials') IS NOT NULL AS sc",
                None,
                &[],
            )
            .ok()?;
        let row = row.first();
        Some(SchemaFeatures {
            version: row.get_by_name::<String, _>("v").ok()??,
            time_partitions: row.get_by_name("tp").ok()?.unwrap_or(false),
            broker_metrics: row.get_by_name("bm").ok()?.unwrap_or(false),
            sasl_credentials: row.get_by_name("sc").ok()?.unwrap_or(false),
        })
    })
    .unwrap_or_default()
}

/// Re-detect the schema features; log once whenever they change (a WARNING listing
/// what is unavailable, or a LOG line when everything is there again). Returns true
/// if they changed.
pub(super) fn refresh_schema_features(features: &mut SchemaFeatures) -> bool {
    let Ok(now) = worker_transaction(detect_schema_features) else {
        return false;
    };
    if now == *features {
        return false;
    }
    let missing = now.missing();
    if missing.is_empty() {
        if !features.version.is_empty() {
            log!(
                "pg_kafka schema is now version {}: all features enabled",
                now.version
            );
        }
    } else {
        pg_warning!(
            "pg_kafka: running against an older pg_kafka schema (version {}, library {}); \
             unavailable until ALTER EXTENSION pg_kafka UPDATE: {}",
            now.version,
            env!("CARGO_PKG_VERSION"),
            missing.join("; ")
        );
    }
    *features = now;
    true
}

/// Go back to the bootstrap superuser (`SET ROLE NONE`), in a transaction of its own.
fn reset_worker_role() -> Result<(), spi::Error> {
    BackgroundWorker::transaction(|| {
        Spi::run("SELECT pg_catalog.set_config('role', 'none', false)")
    })
}

/// Re-decide the worker's role after the schema changed under it (DROP + CREATE
/// EXTENSION, ALTER EXTENSION UPDATE): go back to the bootstrap superuser, then apply
/// `pg_kafka.run_as_role` again if the new schema grants it what the worker needs.
/// Otherwise a worker that had switched to `pgkafka_worker` would be denied everything
/// on a re-created 0.0.0 schema (which grants nothing to that role), and one that kept
/// the superuser on 0.0.0 would never switch after the update.
pub(super) fn reapply_worker_role() {
    if let Err(e) = reset_worker_role() {
        pg_warning!("pg_kafka: could not reset the worker role: {e}");
        return;
    }
    crate::kafka::storage::postgres::apply_worker_role();
}

/// Verify that the kafka schema and every [`REQUIRED_TABLES`] entry exist; the error
/// names the first one missing.
fn verify_schema(database: &str) -> Result<(), String> {
    let missing = Spi::connect(|client| {
        REQUIRED_TABLES.iter().find_map(|table| {
            // pg_catalog, not information_schema: information_schema.tables lists only
            // tables the current role may use, so a worker running as pgkafka_worker
            // would never see a schema that does not grant to it (e.g. a re-created
            // 0.0.0 schema).
            let query =
                format!("SELECT 1 WHERE pg_catalog.to_regclass('kafka.{table}') IS NOT NULL");
            match client.select(&query, None, &[]) {
                Ok(rows) if rows.is_empty() => Some(format!(
                    "Required table kafka.{table} does not exist. \
                     Run 'CREATE EXTENSION pg_kafka' in database '{database}'"
                )),
                Ok(_) => None,
                Err(e) => Some(format!("Failed to check for table kafka.{table}: {e}")),
            }
        })
    });
    missing.map_or(Ok(()), Err)
}

/// Whether the schema wait should log on this check: on the first failed check,
/// then once per [`SCHEMA_WAIT_LOG_INTERVAL`], so an instance that never runs
/// `CREATE EXTENSION` does not fill the log.
fn schema_wait_should_log(waited: Duration, last_logged: Option<Duration>) -> bool {
    match last_logged {
        None => true,
        Some(last) => waited.saturating_sub(last) >= SCHEMA_WAIT_LOG_INTERVAL,
    }
}

/// Block until the kafka schema exists in `database`. Returns `false` if SIGTERM
/// arrived first.
///
/// The worker starts when Postgres finishes recovery, but `CREATE EXTENSION` may
/// not have run yet (or may run much later), so the wait is unbounded: giving up would
/// leave the Kafka port dead until Postgres restarted, even after the extension was
/// created. Postgres has no cross-process event for "schema created", so this
/// polls; `wait_latch` sleeps on the process latch, which SIGTERM sets, so
/// shutdown is not delayed by the poll interval.
pub(super) fn wait_for_schema(database: &str) -> bool {
    let mut waited = Duration::ZERO;
    let mut last_logged: Option<Duration> = None;
    loop {
        // SPI calls require a transaction context
        match BackgroundWorker::transaction(|| verify_schema(database)) {
            Ok(()) => {
                if last_logged.is_some() {
                    log!(
                        "pg_kafka schema found after waiting {} ms",
                        waited.as_millis()
                    );
                }
                return true;
            }
            Err(e) => {
                if schema_wait_should_log(waited, last_logged) {
                    log!(
                        "Waiting for pg_kafka schema (run 'CREATE EXTENSION pg_kafka' in database '{database}'): {e}"
                    );
                    last_logged = Some(waited);
                }
            }
        }
        // wait_latch returns false once SIGTERM has been received (it consumes the
        // flag) or the postmaster has died. Our own SIGTERM handler replaces pgrx's
        // flag, so check ours too.
        if !BackgroundWorker::wait_latch(Some(SCHEMA_CHECK_INTERVAL)) || sigterm_received() {
            return false;
        }
        waited = waited.saturating_add(SCHEMA_CHECK_INTERVAL);
    }
}

/// The extension was dropped while the worker ran. Keep the listener up and answer
/// every request with a retriable KAFKA_STORAGE_ERROR (credential lookups fail) until
/// the schema exists again, re-checking every [`SCHEMA_CHECK_INTERVAL`]. Returns true
/// once it exists, false on SIGTERM or postmaster death (the caller's loop handles
/// those).
pub(super) fn wait_for_schema_while_serving(database: &str, inbox: &Inbox) -> bool {
    log!(
        "pg_kafka extension is gone from database '{database}' (DROP EXTENSION?); answering requests \
         with KAFKA_STORAGE_ERROR until CREATE EXTENSION pg_kafka"
    );
    // Back to the bootstrap superuser while waiting: whatever schema appears next is
    // checked (and its grants re-evaluated by reapply_worker_role) from there. A failure
    // here only means the next check runs under the current role.
    let _ = reset_worker_role();
    let started = Instant::now();
    let mut next_check = Instant::now();
    loop {
        if sigterm_received() || !postmaster_alive() {
            return false;
        }
        if Instant::now() >= next_check {
            if BackgroundWorker::transaction(|| verify_schema(database)).is_ok() {
                schema_found_again();
                log!(
                    "pg_kafka schema found again after {} ms; resuming",
                    started.elapsed().as_millis()
                );
                return true;
            }
            next_check = Instant::now() + SCHEMA_CHECK_INTERVAL;
        }
        while let Ok(r) = inbox.liveness.try_recv() {
            reject_request(r, ERROR_KAFKA_STORAGE_ERROR);
        }
        while let Ok(c) = inbox.credentials.try_recv() {
            let _ = c.reply.send(Err("pg_kafka schema is missing".to_string()));
        }
        crossbeam_channel::select! {
            recv(inbox.requests) -> r => {
                match r {
                    Ok(r) => reject_request(r, ERROR_KAFKA_STORAGE_ERROR),
                    Err(_) => std::thread::sleep(RECV_TIMEOUT), // network thread gone
                }
            },
            default(RECV_TIMEOUT) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_features_missing_lists_what_an_old_schema_lacks() {
        let full = SchemaFeatures {
            version: "0.1.0".into(),
            time_partitions: true,
            broker_metrics: true,
            sasl_credentials: true,
        };
        assert_eq!(full.missing(), Vec::<&str>::new());
        let old = SchemaFeatures {
            version: "0.0.0".into(),
            ..Default::default()
        };
        let missing = old.missing().join("; ");
        assert!(missing.contains("time partitions"));
        assert!(missing.contains("broker metrics"));
        assert!(missing.contains("SASL users"));
    }

    #[test]
    fn test_schema_wait_logs_first_check_then_throttles() {
        // First failed check always logs.
        assert!(schema_wait_should_log(Duration::ZERO, None));
        // Within the interval after the last log line: stay quiet.
        assert!(!schema_wait_should_log(
            Duration::from_secs(30),
            Some(Duration::ZERO)
        ));
        // Once the interval has passed: log again.
        assert!(schema_wait_should_log(
            SCHEMA_WAIT_LOG_INTERVAL,
            Some(Duration::ZERO)
        ));
        assert!(schema_wait_should_log(
            Duration::from_secs(185),
            Some(Duration::from_secs(120))
        ));
        assert!(!schema_wait_should_log(
            Duration::from_secs(150),
            Some(Duration::from_secs(120))
        ));
    }
}
