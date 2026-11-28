//! The `worker` category (worker lifecycle and robustness) and the helpers it shares with
//! the `part`, `concurrent` and `long_poll` tests that drive the worker the same way.
//!
//! Every such test is sequential: they change GUCs, hold locks on broker
//! tables, terminate the listener worker, or DROP/CREATE the extension.
//!
//! Shared requirements (like the `auth` category): the test process runs on the
//! database host as a superuser. Three tests read the server log through the listener
//! worker's stderr (`/proc/<pid>/fd/2`, only when that is a regular file) and two copy
//! the frozen 0.0.0 install script (`sql/upgrade-test/pg_kafka--0.0.0.sql`) into the
//! server's extension directory, exactly as `scripts/check-upgrade.sh` does.

mod background_waits;
mod cancel;
mod debug_log;
mod lifecycle;
mod same_version;

use crate::common::{create_db_client, get_bootstrap_servers, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use std::io::{Read, Seek, SeekFrom};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// A result whose error can cross tasks (these tests spawn work that fails).
pub type SendResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Box any error into the suite's error type.
pub fn boxed<E: std::fmt::Debug>(e: E) -> Box<dyn std::error::Error + Send + Sync> {
    format!("{e:?}").into()
}

/// The pid of the one `pg_kafka_listener` worker, if it runs.
pub async fn listener_pid(db: &Client) -> SendResult<Option<i32>> {
    let rows = db
        .query(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await
        .map_err(boxed)?;
    Ok(rows.first().map(|r| r.get(0)))
}

/// Wait until a listener (other than `not` if given) runs and answers a produce.
pub async fn wait_for_serving_listener(
    db: &Client,
    not: Option<i32>,
    within: Duration,
) -> SendResult<i32> {
    let deadline = Instant::now() + within;
    let producer = quick_producer("5000")?;
    loop {
        if let Some(pid) = listener_pid(db).await? {
            if Some(pid) != not {
                let topic = format!("worker-probe-{}", std::process::id());
                let ok = producer
                    .send(
                        FutureRecord::<(), _>::to(&topic)
                            .payload("probe")
                            .partition(0),
                        Duration::from_secs(5),
                    )
                    .await
                    .is_ok();
                if ok {
                    return Ok(pid);
                }
            }
        }
        if Instant::now() > deadline {
            return Err(format!("no serving pg_kafka_listener within {within:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// A plain producer with the given `message.timeout.ms`.
pub fn quick_producer(timeout_ms: &str) -> SendResult<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", timeout_ms)
        .set("linger.ms", "0")
        .create()
        .map_err(boxed)
}

/// ALTER SYSTEM SET name = value (or RESET when `value` is None), then reload.
/// Returns false (and leaves things alone) if the server does not know the GUC,
/// so a test can run against a build that predates it.
pub async fn set_guc(db: &Client, name: &str, value: Option<&str>) -> SendResult<bool> {
    let sql = match value {
        Some(v) => format!("ALTER SYSTEM SET {} = '{}'", name, v.replace('\'', "''")),
        None => format!("ALTER SYSTEM RESET {name}"),
    };
    if let Err(e) = db.batch_execute(&sql).await {
        println!("  (could not set {name}: {e})");
        return Ok(false);
    }
    db.batch_execute("SELECT pg_reload_conf()")
        .await
        .map_err(boxed)?;
    // The worker reloads on SIGHUP at its next loop iteration (<= 100 ms idle).
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(true)
}

/// The server log, read through the listener worker's stderr, from `from` on.
/// None when that is not a regular file (e.g. a logging-collector pipe).
pub struct ServerLog {
    path: std::path::PathBuf,
}

impl ServerLog {
    pub async fn open(db: &Client) -> SendResult<Option<ServerLog>> {
        let Some(pid) = listener_pid(db).await? else {
            return Ok(None);
        };
        let link = std::path::PathBuf::from(format!("/proc/{pid}/fd/2"));
        let Ok(target) = std::fs::read_link(&link) else {
            return Ok(None);
        };
        match std::fs::metadata(&target) {
            Ok(m) if m.is_file() => Ok(Some(ServerLog { path: target })),
            _ => Ok(None),
        }
    }

    /// The current end of the log.
    pub fn end(&self) -> u64 {
        std::fs::metadata(&self.path).map_or(0, |m| m.len())
    }

    /// Everything written since `from`.
    pub fn since(&self, from: u64) -> String {
        let mut out = String::new();
        if let Ok(mut f) = std::fs::File::open(&self.path) {
            if f.seek(SeekFrom::Start(from)).is_ok() {
                let mut buf = Vec::new();
                let _ = f.read_to_end(&mut buf);
                out = String::from_utf8_lossy(&buf).into_owned();
            }
        }
        out
    }
}

/// Make `CREATE EXTENSION pg_kafka VERSION '0.0.0'` possible: copy the frozen 0.0.0
/// install script next to the extension (what scripts/check-upgrade.sh does).
pub async fn ensure_old_version_available(db: &Client) -> SendResult<()> {
    let available: bool = db
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_available_extension_versions
                            WHERE name = 'pg_kafka' AND version = '0.0.0')",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0);
    if available {
        return Ok(());
    }
    let sharedir: String = db
        .query_one("SELECT setting FROM pg_config WHERE name = 'SHAREDIR'", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sql/upgrade-test/pg_kafka--0.0.0.sql");
    let dst = std::path::Path::new(&sharedir).join("extension/pg_kafka--0.0.0.sql");
    std::fs::copy(&src, &dst)
        .map_err(|e| format!("copy {} -> {}: {}", src.display(), dst.display(), e))?;
    Ok(())
}

/// The cumulative deadlock count of the current database.
pub async fn deadlocks(db: &Client) -> SendResult<i64> {
    Ok(db
        .query_one(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Convert this module's error type into the suite's.
pub fn into_test_result(r: SendResult<()>) -> TestResult {
    r.map_err(|e| e.to_string().into())
}

/// A second database session.
pub async fn session() -> SendResult<Client> {
    create_db_client().await.map_err(boxed)
}

/// The `worker` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["worker";
        lifecycle::test_worker_lock_timeout_bounds_a_stuck_lock: sequential,
        lifecycle::test_sigterm_interrupts_a_worker_lock_wait: sequential,
        lifecycle::test_commit_failure_answered_without_restart: sequential,
        lifecycle::test_old_schema_degrades_gracefully: sequential,
        lifecycle::test_old_schema_refuses_clients_until_update: sequential,
        same_version::test_same_version_schema_warning_names_recreation: sequential,
        lifecycle::test_extension_ddl_loop_under_load: sequential,
        debug_log::test_pg_debug_logs_when_debug1_is_logged: sequential, // changes log_min_messages
        cancel::test_cancel_does_not_restart_the_worker: sequential, // signals the listener
        background_waits::test_metrics_flush_skips_held_rows: sequential,
        background_waits::test_worker_maintenance_waits_briefly_behind_a_reader: sequential,
    ]
}

pub(crate) const WORKER_LOCK_TIMEOUT_GUC: &str = "pg_kafka.worker_lock_timeout_ms";

pub(crate) fn unique(base: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("worker-{}-{}", base, nanos % 1_000_000_000_000)
}

pub(crate) async fn send(producer: &FutureProducer, topic: &str, value: &str) -> SendResult<i64> {
    producer
        .send(
            FutureRecord::to(topic).key("k").payload(value).partition(0),
            Duration::from_secs(30),
        )
        .await
        .map(|(_, offset)| offset)
        .map_err(|(e, _)| boxed(e))
}

pub(crate) fn assign_consumer(
    topic: &str,
    read_committed: bool,
    extra: &[(&str, &str)],
) -> SendResult<StreamConsumer> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", unique("assign"))
        .set("enable.auto.commit", "false")
        .set(
            "isolation.level",
            if read_committed {
                "read_committed"
            } else {
                "read_uncommitted"
            },
        );
    for (k, v) in extra {
        cfg.set(*k, *v);
    }
    let consumer: StreamConsumer = cfg.create().map_err(boxed)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .map_err(boxed)?;
    consumer.assign(&tpl).map_err(boxed)?;
    Ok(consumer)
}

/// Read `n` values from partition 0 of `topic` (read_committed or not), within `within`.
pub(crate) async fn read_values(
    topic: &str,
    read_committed: bool,
    n: usize,
    within: Duration,
) -> SendResult<Vec<String>> {
    let consumer = assign_consumer(topic, read_committed, &[])?;
    let deadline = Instant::now() + within;
    let mut out = Vec::new();
    while out.len() < n && Instant::now() < deadline {
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            out.push(String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned());
        }
    }
    Ok(out)
}

/// Hold the counter row of partition 0 of `topic` (FOR UPDATE) in an open transaction
/// on `holder`: a Produce to that partition then waits on the row lock.
pub(crate) async fn hold_partition_counter(holder: &Client, topic: &str) -> SendResult<()> {
    holder.batch_execute("BEGIN").await.map_err(boxed)?;
    let n = holder
        .query(
            "SELECT 1 FROM kafka.partition_offsets po JOIN kafka.topics t ON t.id = po.topic_id
             WHERE t.name = $1 AND po.partition_id = 0 FOR UPDATE OF po",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .len();
    if n != 1 {
        return Err(format!("expected one partition_offsets row for {topic}, got {n}").into());
    }
    Ok(())
}
