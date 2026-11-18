//! Regression tests for the `lifecycle` workstream: worker restart, the explicit
//! commit/rollback outcome, and group commit (several requests per top-level
//! transaction).
//!
//! The code under test lives in `src/worker.rs` / `src/lib.rs`, which unit
//! coverage cannot see (QA-1), so these drive the running broker.

use crate::common::{create_producer, TestResult};
use crate::error_paths::bug_hunt::roundtrip;
use crate::setup::TestContext;
use kafka_protocol::messages::ApiVersionsRequest;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const BROKER: &str = "localhost:9092";

/// The listener worker's pid, from pg_stat_activity (`backend_type` is the
/// bgworker type, registered as "pg_kafka_listener" in `_PG_init`).
async fn listener_pid(ctx: &TestContext) -> Result<Option<i32>, Box<dyn std::error::Error>> {
    let rows = ctx
        .db()
        .query(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_kafka_listener'",
            &[],
        )
        .await?;
    if rows.len() > 1 {
        return Err(format!(
            "expected at most one pg_kafka_listener, found {}",
            rows.len()
        )
        .into());
    }
    Ok(rows.first().map(|r| r.get(0)))
}

/// One ApiVersions round trip on a fresh connection, with a short timeout.
async fn broker_answers_api_versions() -> bool {
    let attempt = async {
        let mut stream = TcpStream::connect(BROKER).await?;
        let response = roundtrip(&mut stream, 0, 1, &ApiVersionsRequest::default()).await?;
        Ok::<i16, Box<dyn std::error::Error>>(response.error_code)
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), attempt).await,
        Ok(Ok(0))
    )
}

/// Item 1: terminating the listener worker must not leave the Kafka port dead.
///
/// Before the fix the worker was registered with no restart interval and returned
/// (exit status 0) on SIGTERM, so `pg_terminate_backend` on it left port 9092
/// closed until Postgres was restarted. Now it exits with status 1 and the
/// postmaster restarts it after 5 s; the new worker re-binds the port.
///
/// Asserts: (1) exactly one pg_kafka_listener exists and the broker answers;
/// (2) after pg_terminate_backend, the old pid leaves pg_stat_activity; (3)
/// within 20 s a NEW listener pid appears and the port answers ApiVersions
/// (error_code 0); (4) a produce through rdkafka is then acked and stored.
///
/// Not parallel-safe: it takes the broker down for several seconds.
pub async fn test_worker_restarts_after_terminate() -> TestResult {
    println!("=== Test: Listener Worker Restarts After pg_terminate_backend ===\n");
    let ctx = TestContext::new().await?;

    let old_pid = listener_pid(&ctx)
        .await?
        .ok_or("no pg_kafka_listener in pg_stat_activity")?;
    assert!(
        broker_answers_api_versions().await,
        "broker must answer before the test terminates it"
    );
    println!("Step 1: listener pid {} is serving", old_pid);

    let terminated: bool = ctx
        .db()
        .query_one("SELECT pg_terminate_backend($1)", &[&old_pid])
        .await?
        .get(0);
    assert!(
        terminated,
        "pg_terminate_backend({}) returned false",
        old_pid
    );
    println!("Step 2: terminated pid {}", old_pid);

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut new_pid = None;
    let mut serving = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let current = listener_pid(&ctx).await?;
        match current {
            Some(pid) if pid != old_pid => {
                new_pid = Some(pid);
                if broker_answers_api_versions().await {
                    serving = true;
                    break;
                }
            }
            _ => {}
        }
    }
    let new_pid = new_pid.ok_or("no new pg_kafka_listener appeared within 20 s")?;
    assert!(
        serving,
        "new listener pid {} did not serve ApiVersions on {} within 20 s",
        new_pid, BROKER
    );
    println!("Step 3: new listener pid {} serves the port", new_pid);

    // The restarted worker handles real traffic end to end.
    let topic = ctx.unique_topic("lifecycle-restart").await;
    let producer = create_producer()?;
    producer
        .send(
            rdkafka::producer::FutureRecord::<(), _>::to(&topic)
                .payload("after-restart")
                .partition(0),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    let count: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(count, 1, "the produce after restart must be stored");
    println!("Step 4: produce after restart acked and stored");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
