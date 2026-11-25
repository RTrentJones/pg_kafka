//! The schema the worker runs against: waiting for it at start and after DROP EXTENSION,
//! re-deciding the worker's role when it changes, and detecting a schema older than the
//! library (not served until `ALTER EXTENSION pg_kafka UPDATE`).

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use pgrx::{spi, Spi};
use std::time::{Duration, Instant};

use super::batch::reject_request;
use super::lifecycle::{postmaster_alive, sigterm_received};
use super::network::Inbox;
use super::txn::{schema_found_again, schema_lost, worker_transaction};
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

/// The objects of the current schema (0.1.0) that the library's request and maintenance
/// paths use and an older extension version lacks, each with the SQL test for it. The
/// 0.0.0 schema has none of them: every idempotent Produce reads kafka.producer_batches,
/// transactions use kafka.txn_partitions and kafka.txn_groups, SASL logins read kafka.sasl_credentials and
/// kafka.sasl_server_secret, the shadow outbox claims through
/// kafka.shadow_tracking.attempt_open, every Produce writes kafka.messages.record_bytes,
/// and partition maintenance and the metrics flush need the time partitions and the broker
/// metrics tables.
const CURRENT_SCHEMA_OBJECTS: [(&str, &str); 10] = [
    (
        "kafka.producer_batches",
        "to_regclass('kafka.producer_batches') IS NOT NULL",
    ),
    (
        "kafka.txn_partitions",
        "to_regclass('kafka.txn_partitions') IS NOT NULL",
    ),
    (
        "kafka.txn_groups",
        "to_regclass('kafka.txn_groups') IS NOT NULL",
    ),
    (
        "kafka.sasl_credentials",
        "to_regclass('kafka.sasl_credentials') IS NOT NULL",
    ),
    (
        "kafka.sasl_server_secret",
        "to_regclass('kafka.sasl_server_secret') IS NOT NULL",
    ),
    (
        "kafka.shadow_tracking.attempt_open",
        "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = to_regclass('kafka.shadow_tracking')
                   AND attname = 'attempt_open' AND NOT attisdropped)",
    ),
    (
        "kafka.messages.record_bytes",
        "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                 WHERE attrelid = to_regclass('kafka.messages')
                   AND attname = 'record_bytes' AND NOT attisdropped)",
    ),
    (
        "time partitions of kafka.messages",
        "to_regprocedure('kafka.message_partitions()') IS NOT NULL",
    ),
    (
        "kafka.broker_api_stats",
        "to_regclass('kafka.broker_api_stats') IS NOT NULL",
    ),
    (
        "kafka.broker_gauges",
        "to_regclass('kafka.broker_gauges') IS NOT NULL",
    ),
];

/// The installed schema as the worker last saw it. A new binary loaded before `ALTER
/// EXTENSION pg_kafka UPDATE` (docs/UPGRADING.md step 2) finds an older schema: it then
/// serves no client and runs no maintenance (see [`wait_for_schema_update`]) until the
/// schema is current.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct SchemaState {
    /// `extversion` of the installed extension (empty: not detected yet).
    version: String,
    /// The [`CURRENT_SCHEMA_OBJECTS`] this schema lacks.
    missing: Vec<&'static str>,
}

impl SchemaState {
    /// From the result of each [`CURRENT_SCHEMA_OBJECTS`] test, in order.
    fn from_checks(version: String, present: &[bool]) -> Self {
        let missing = CURRENT_SCHEMA_OBJECTS
            .iter()
            .zip(present.iter().copied().chain(std::iter::repeat(false)))
            .filter(|(_, present)| !present)
            .map(|((name, _), _)| *name)
            .collect();
        SchemaState { version, missing }
    }

    /// Whether the schema has everything the library uses (false until detected).
    pub(super) fn is_current(&self) -> bool {
        !self.version.is_empty() && self.missing.is_empty()
    }
}

/// The detection query: the extension version, then one boolean per
/// [`CURRENT_SCHEMA_OBJECTS`] entry.
fn detect_query() -> String {
    let checks: Vec<&str> = CURRENT_SCHEMA_OBJECTS.iter().map(|(_, sql)| *sql).collect();
    format!(
        "SELECT (SELECT extversion FROM pg_extension WHERE extname = 'pg_kafka'), {}",
        checks.join(", ")
    )
}

/// Read the [`SchemaState`] of the current database (inside a transaction); None if
/// the query fails or the extension is not installed.
fn detect_schema_state() -> Option<SchemaState> {
    Spi::connect(|client| {
        let row = client.select(&detect_query(), Some(1), &[]).ok()?;
        let row = row.first();
        let version = row.get::<String>(1).ok()??;
        let present: Vec<bool> = (0..CURRENT_SCHEMA_OBJECTS.len())
            .map(|i| row.get::<bool>(i + 2).ok().flatten().unwrap_or(false))
            .collect();
        Some(SchemaState::from_checks(version, &present))
    })
}

/// Re-detect the schema; log once whenever it changes (a WARNING that an older schema
/// is not served, or a LOG line when it is current again). Returns true if it changed.
pub(super) fn refresh_schema_state(state: &mut SchemaState) -> bool {
    let Ok(Some(now)) = worker_transaction(detect_schema_state) else {
        return false;
    };
    if now == *state {
        return false;
    }
    if now.is_current() {
        if !state.version.is_empty() {
            log!(
                "pg_kafka schema is now version {}: serving Kafka clients",
                now.version
            );
        }
    } else {
        pg_warning!(
            "pg_kafka: running against an older pg_kafka schema (version {}, library {}): \
             answering Kafka requests with KAFKA_STORAGE_ERROR and running no maintenance \
             until ALTER EXTENSION pg_kafka UPDATE (missing: {})",
            now.version,
            env!("CARGO_PKG_VERSION"),
            now.missing.join(", ")
        );
    }
    *state = now;
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

/// Keep the listener up while the worker cannot serve: answer every request with a
/// retriable KAFKA_STORAGE_ERROR and every credential lookup with `refusal`, calling
/// `ready` every [`SCHEMA_CHECK_INTERVAL`] until it returns true. Returns true then,
/// false on SIGTERM or postmaster death (the caller's loop handles those).
fn refuse_requests_until(inbox: &Inbox, refusal: &str, mut ready: impl FnMut() -> bool) -> bool {
    let mut next_check = Instant::now();
    loop {
        if sigterm_received() || !postmaster_alive() {
            return false;
        }
        if Instant::now() >= next_check {
            if ready() {
                return true;
            }
            next_check = Instant::now() + SCHEMA_CHECK_INTERVAL;
        }
        while let Ok(r) = inbox.liveness.try_recv() {
            reject_request(r, ERROR_KAFKA_STORAGE_ERROR);
        }
        while let Ok(c) = inbox.credentials.try_recv() {
            let _ = c.reply.send(Err(refusal.to_string()));
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

/// The extension was dropped while the worker ran. Keep the listener up and answer
/// every request with a retriable KAFKA_STORAGE_ERROR (credential lookups fail) until
/// the schema exists again. Returns true once it exists, false on SIGTERM or
/// postmaster death (the caller's loop handles those).
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
    refuse_requests_until(inbox, "pg_kafka schema is missing", || {
        if BackgroundWorker::transaction(|| verify_schema(database)).is_err() {
            return false;
        }
        schema_found_again();
        log!(
            "pg_kafka schema found again after {} ms; resuming",
            started.elapsed().as_millis()
        );
        true
    })
}

/// The schema is older than the library (`state` is not current): its tables cannot
/// serve the library's requests, so answer every request with the retriable
/// KAFKA_STORAGE_ERROR (credential lookups fail) and re-detect the schema every
/// [`SCHEMA_CHECK_INTERVAL`] until `ALTER EXTENSION pg_kafka UPDATE` has made it
/// current, or the extension is dropped (the caller then sees [`schema_lost`]). Returns
/// false on SIGTERM or postmaster death.
pub(super) fn wait_for_schema_update(state: &mut SchemaState, inbox: &Inbox) -> bool {
    refuse_requests_until(
        inbox,
        "the pg_kafka schema is older than the library (ALTER EXTENSION pg_kafka UPDATE)",
        || {
            refresh_schema_state(state);
            state.is_current() || schema_lost()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_state_is_current_only_with_every_object() {
        let all = [true; CURRENT_SCHEMA_OBJECTS.len()];
        let full = SchemaState::from_checks("0.1.0".into(), &all);
        assert_eq!(full.missing, Vec::<&str>::new());
        assert!(full.is_current());

        // The 0.0.0 schema has none of them.
        let old = SchemaState::from_checks("0.0.0".into(), &[false; CURRENT_SCHEMA_OBJECTS.len()]);
        assert!(!old.is_current());
        for name in [
            "kafka.producer_batches",
            "kafka.txn_partitions",
            "kafka.txn_groups",
            "kafka.sasl_credentials",
            "kafka.sasl_server_secret",
            "kafka.shadow_tracking.attempt_open",
            "kafka.messages.record_bytes",
        ] {
            assert!(
                old.missing.contains(&name),
                "{name} missing from {:?}",
                old.missing
            );
        }

        // One object missing is enough; a short result counts as missing.
        let mut one = all;
        one[0] = false;
        let partial = SchemaState::from_checks("0.1.0".into(), &one);
        assert_eq!(partial.missing, vec!["kafka.producer_batches"]);
        assert!(!SchemaState::from_checks("0.1.0".into(), &all[..2]).is_current());

        // Not detected yet: not current.
        assert!(!SchemaState::default().is_current());
    }

    #[test]
    fn test_detect_query_has_one_column_per_object() {
        let q = detect_query();
        assert!(q.starts_with("SELECT (SELECT extversion"));
        for (_, sql) in CURRENT_SCHEMA_OBJECTS {
            assert!(q.contains(sql));
        }
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
