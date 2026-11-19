//! The schema the worker runs against: waiting for it at start and after DROP EXTENSION,
//! and detecting what an older extension version lacks.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use pgrx::Spi;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::lifecycle::{postmaster_alive, sigterm_received};
use super::txn::{worker_transaction, ERROR_KAFKA_STORAGE_ERROR, KNOWN_EXTENSION_OID, SCHEMA_LOST};
use super::RECV_TIMEOUT;
use crate::pg_warning;

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
pub(super) fn detect_schema_features() -> SchemaFeatures {
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
    let now = match worker_transaction(detect_schema_features) {
        Ok(f) => f,
        Err(_) => return false,
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

/// Re-decide the worker's role after the schema changed under it (DROP + CREATE
/// EXTENSION, ALTER EXTENSION UPDATE): go back to the bootstrap superuser (`SET ROLE
/// NONE`), then apply `pg_kafka.run_as_role` again if the new schema grants it what the
/// worker needs. Otherwise a worker that had switched to `pgkafka_worker` would be
/// denied everything on a re-created 0.0.0 schema (which grants nothing to that role),
/// and one that kept the superuser on 0.0.0 would never switch after the update.
pub(super) fn reapply_worker_role() {
    let reset: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
        Spi::run("SELECT pg_catalog.set_config('role', 'none', false)")
    });
    if let Err(e) = reset {
        pg_warning!("pg_kafka: could not reset the worker role: {}", e);
        return;
    }
    crate::kafka::storage::postgres::apply_worker_role();
}

/// Answer a request that will not run (schema missing, worker stopping) with its API's
/// error response, on its own sender. An acks=0 produce gets nothing.
pub(super) fn reject_request(request: crate::kafka::KafkaRequest, error_code: i16) {
    if !request.body.expects_response() {
        return;
    }
    let response = crate::kafka::KafkaResponse::new(
        request.header.correlation_id,
        request.header.api_version,
        request
            .body
            .echo()
            .error_response(request.header.api_version, error_code),
    );
    let _ = request.response_tx.send(response);
}

/// Verify that the kafka schema and required tables exist.
/// Returns Ok(()) if all tables exist, Err with description if any are missing.
pub(super) fn verify_schema(database: &str) -> Result<(), String> {
    // QA-7: verify every table the runtime depends on, not just the core three, so a partial or
    // stale schema (e.g. missing the phase-9/10 idempotent/transaction tables, the BUG-3 offset
    // counter, or the shadow tables) is caught at worker start rather than as a late SPI failure on
    // the first produce/txn/shadow operation. `CREATE EXTENSION pg_kafka` creates all of these.
    // DR-6 (DEEP-REVIEW-2026-07): "consumer_groups" is intentionally absent — the table was
    // dead schema (membership lives in the in-memory GroupCoordinator) and was dropped.
    let required_tables = [
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

    let result: Result<Option<String>, pgrx::spi::SpiError> = Spi::connect(|client| {
        for table in required_tables {
            // pg_catalog, not information_schema: information_schema.tables lists only
            // tables the current role may use, so a worker running as pgkafka_worker
            // would never see a schema that does not grant to it (e.g. a re-created
            // 0.0.0 schema).
            let query = format!(
                "SELECT 1 WHERE pg_catalog.to_regclass('kafka.{}') IS NOT NULL",
                table
            );
            match client.select(&query, None, &[]) {
                Ok(tup_table) => {
                    if tup_table.is_empty() {
                        return Ok(Some(format!(
                            "Required table kafka.{} does not exist. \
                             Run 'CREATE EXTENSION pg_kafka' in database '{}'",
                            table, database
                        )));
                    }
                }
                Err(e) => {
                    return Ok(Some(format!(
                        "Failed to check for table kafka.{}: {}",
                        table, e
                    )));
                }
            }
        }
        Ok(None) // All tables exist
    });

    match result {
        Ok(None) => Ok(()),
        Ok(Some(e)) => Err(e),
        Err(e) => Err(format!("Schema verification failed: {}", e)),
    }
}

/// How often `wait_for_schema` re-checks for the kafka schema.
pub(super) const SCHEMA_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// How often `wait_for_schema` repeats its "waiting for schema" LOG line.
pub(super) const SCHEMA_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Whether the schema wait should log on this check: on the first failed check,
/// then once per `SCHEMA_WAIT_LOG_INTERVAL`, so an instance that never runs
/// `CREATE EXTENSION` does not fill the log.
pub(super) fn schema_wait_should_log(waited: Duration, last_logged: Option<Duration>) -> bool {
    match last_logged {
        None => true,
        Some(last) => waited.saturating_sub(last) >= SCHEMA_WAIT_LOG_INTERVAL,
    }
}

/// Block until the kafka schema exists in `database`. Returns `false` if SIGTERM
/// arrived first.
///
/// The worker starts when Postgres finishes recovery, but `CREATE EXTENSION` may
/// not have run yet (or may run much later). The wait is unbounded: it used to
/// give up after 60 s and exit, and since nothing restarted the worker the Kafka
/// port then stayed dead until Postgres was restarted even after the extension
/// was created. Postgres has no cross-process event for "schema created", so this
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
                        "Waiting for pg_kafka schema (run 'CREATE EXTENSION pg_kafka' in database '{}'): {}",
                        database,
                        e
                    );
                    last_logged = Some(waited);
                }
            }
        }
        // wait_latch returns false once SIGTERM has been received (it consumes the
        // flag) or the postmaster has died.
        // (Our own SIGTERM handler replaces pgrx's flag, so check ours too.)
        if !BackgroundWorker::wait_latch(Some(SCHEMA_CHECK_INTERVAL)) || sigterm_received() {
            return false;
        }
        waited = waited.saturating_add(SCHEMA_CHECK_INTERVAL);
    }
}

/// The extension was dropped while the worker ran. Keep the listener up and answer
/// every request with a retriable KAFKA_STORAGE_ERROR (credential lookups fail) until
/// the schema exists again, re-checking every `SCHEMA_CHECK_INTERVAL`. Returns true
/// once it exists, false on SIGTERM or postmaster death (the caller's loop handles
/// those).
pub(super) fn wait_for_schema_while_serving(
    database: &str,
    request_rx: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    liveness_rx: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    credential_rx: &crossbeam_channel::Receiver<crate::kafka::auth::CredentialRequest>,
) -> bool {
    log!(
        "pg_kafka extension is gone from database '{}' (DROP EXTENSION?); answering requests \
         with KAFKA_STORAGE_ERROR until CREATE EXTENSION pg_kafka",
        database
    );
    // Back to the bootstrap superuser while waiting: whatever schema appears next is
    // checked (and its grants re-evaluated by reapply_worker_role) from there.
    let _: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
        Spi::run("SELECT pg_catalog.set_config('role', 'none', false)")
    });
    let started = Instant::now();
    let mut next_check = Instant::now();
    loop {
        if sigterm_received() || !postmaster_alive() {
            return false;
        }
        if Instant::now() >= next_check {
            if BackgroundWorker::transaction(|| verify_schema(database)).is_ok() {
                // The next worker transaction records the (new) extension's OID.
                KNOWN_EXTENSION_OID.store(0, Ordering::SeqCst);
                SCHEMA_LOST.store(false, Ordering::SeqCst);
                log!(
                    "pg_kafka schema found again after {} ms; resuming",
                    started.elapsed().as_millis()
                );
                return true;
            }
            next_check = Instant::now() + SCHEMA_CHECK_INTERVAL;
        }
        while let Ok(r) = liveness_rx.try_recv() {
            reject_request(r, ERROR_KAFKA_STORAGE_ERROR);
        }
        while let Ok(c) = credential_rx.try_recv() {
            let _ = c.reply.send(Err("pg_kafka schema is missing".to_string()));
        }
        crossbeam_channel::select! {
            recv(request_rx) -> r => {
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
        assert!(full.missing().is_empty());
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
