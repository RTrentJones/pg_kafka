//! RB-1 E2E: response-after-commit barrier.
//!
//! An acked produce (acks >= 1) must imply that the write is committed and
//! visible to any other PostgreSQL connection. Before the barrier, the worker
//! sent the Kafka response from *inside* `BackgroundWorker::transaction`, so a
//! client could receive the ack microseconds before the commit became visible
//! — a durability gap (a crash in the window lost an acked write) and the
//! source of flaky immediate-read database verifications in this suite.
//!
//! Determinism: the window is normally microseconds, far too narrow to assert
//! on. The test-only GUC `pg_kafka.test_pre_commit_delay_ms` injects a sleep
//! between the Produce handler (which buffers/sends the response) and the
//! commit, widening the window to 300ms:
//!   - pre-barrier build: ack arrives ~300ms BEFORE the commit is visible →
//!     an immediate COUNT(*) after the ack reliably sees the previous count →
//!     this test FAILS (fail-before verified);
//!   - post-barrier build: the response is flushed only after commit, so the
//!     ack itself takes >= 300ms and the immediate read always sees the row.
//!
//! Registered `parallel_safe: false`: it mutates a global GUC (Produce-gated,
//! but serializing avoids slowing concurrent produce-heavy tests).

use crate::common::{create_db_client, create_producer, TestResult};
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DELAY_MS: u64 = 300;
/// Ack RTT / visibility-gap threshold used to detect that the worker has
/// picked up the GUC — comfortably below DELAY_MS but far above a normal
/// produce RTT (single-digit ms locally).
const DETECT_MS: u64 = 250;

async fn count_messages(db: &tokio_postgres::Client, topic: &str) -> Result<i64, String> {
    db.query_one(
        "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
        &[&topic],
    )
    .await
    .map(|row| row.get(0))
    .map_err(|e| format!("count query failed: {e}"))
}

async fn set_pre_commit_delay(
    db: &tokio_postgres::Client,
    value: Option<u64>,
) -> Result<(), String> {
    let stmt = match value {
        Some(v) => format!("ALTER SYSTEM SET pg_kafka.test_pre_commit_delay_ms = '{v}'"),
        None => "ALTER SYSTEM RESET pg_kafka.test_pre_commit_delay_ms".to_string(),
    };
    db.simple_query(&stmt)
        .await
        .map_err(|e| format!("{stmt} failed: {e}"))?;
    db.simple_query("SELECT pg_reload_conf()")
        .await
        .map_err(|e| format!("pg_reload_conf failed: {e}"))?;
    Ok(())
}

async fn produce_one(
    producer: &rdkafka::producer::FutureProducer,
    topic: &str,
    payload: &str,
) -> Result<Duration, String> {
    let start = Instant::now();
    producer
        .send(
            FutureRecord::to(topic).key("k").payload(payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| format!("produce failed: {e}"))?;
    Ok(start.elapsed())
}

/// Inner body with String errors so it can run under `tokio::spawn` (the
/// outer wrapper guarantees the GUC reset even if an assertion panics here —
/// though this body deliberately uses Err returns, not panics).
async fn run_barrier_check() -> Result<(), String> {
    let db = create_db_client()
        .await
        .map_err(|e| format!("db connect failed: {e}"))?;
    let producer = create_producer().map_err(|e| format!("producer create failed: {e}"))?;

    // ── Handshake: wait until the worker has picked up the GUC ──────────
    // SIGHUP reload is immediate but asynchronous. Detection works on BOTH
    // builds: post-barrier the ack RTT itself is >= DELAY_MS; pre-barrier the
    // ack stays fast but the ack→visibility gap is >= DELAY_MS.
    println!("Step 1: Waiting for worker to pick up pg_kafka.test_pre_commit_delay_ms...");
    let warmup_topic = format!("ack-visibility-warmup-{}", Uuid::new_v4());
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut warmup_count: i64 = 0;
    let mut guc_active = false;
    while Instant::now() < deadline {
        let ack_rtt = produce_one(&producer, &warmup_topic, "warmup").await?;
        warmup_count += 1;
        if ack_rtt >= Duration::from_millis(DETECT_MS) {
            println!(
                "   GUC active (ack RTT {}ms — post-barrier shape)",
                ack_rtt.as_millis()
            );
            guc_active = true;
            break;
        }
        // Fast ack: measure how long the row takes to become visible.
        let ack_instant = Instant::now();
        loop {
            if count_messages(&db, &warmup_topic).await? >= warmup_count {
                break;
            }
            if ack_instant.elapsed() > Duration::from_secs(3) {
                return Err(format!(
                    "warmup row never became visible within 3s (produced {warmup_count})"
                ));
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        if ack_instant.elapsed() >= Duration::from_millis(DETECT_MS) {
            println!(
                "   GUC active (ack→visibility gap {}ms — pre-barrier shape)",
                ack_instant.elapsed().as_millis()
            );
            guc_active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if !guc_active {
        return Err("worker did not pick up test_pre_commit_delay_ms within 15s".to_string());
    }
    println!("✅ Fault injection active\n");

    // ── The barrier assertion ────────────────────────────────────────────
    // Each acked produce must be immediately visible on the FIRST read from a
    // separate connection — no polling, no retries.
    println!("Step 2: Producing 5 messages, asserting immediate visibility after each ack...");
    let topic = format!("ack-visibility-{}", Uuid::new_v4());
    for i in 1..=5i64 {
        produce_one(&producer, &topic, &format!("msg-{i}")).await?;
        let count = count_messages(&db, &topic).await?;
        if count != i {
            return Err(format!(
                "BARRIER VIOLATION on produce #{i}: immediate read after ack saw {count} rows \
                 (expected {i}) — the response was delivered before the commit was visible"
            ));
        }
        println!("   produce #{i}: acked and immediately visible ✓");
    }
    println!("✅ All acked produces were immediately visible\n");

    // Cleanup of test topics (rows themselves are harmless, but be tidy).
    let _ = db
        .simple_query(&format!(
            "DELETE FROM kafka.messages WHERE topic_id IN (SELECT id FROM kafka.topics WHERE name IN ('{warmup_topic}', '{topic}'));\
             DELETE FROM kafka.topics WHERE name IN ('{warmup_topic}', '{topic}')"
        ))
        .await;
    Ok(())
}

/// RB-1: an acks>=1 produce ack implies the write is committed and visible.
pub async fn test_produce_ack_implies_committed_visibility() -> TestResult {
    println!("=== Test: Produce Ack Implies Committed Visibility (RB-1 barrier) ===\n");

    let db = create_db_client().await?;

    // Defensive reset first: a crashed earlier run may have left the GUC set
    // in postgresql.auto.conf.
    set_pre_commit_delay(&db, None).await.map_err(box_err)?;
    set_pre_commit_delay(&db, Some(DELAY_MS))
        .await
        .map_err(box_err)?;

    // Run the body in a task so the GUC reset below runs on EVERY exit path,
    // including a panic inside the body.
    let body_result = tokio::spawn(run_barrier_check()).await;

    // ── Always-reset cleanup ────────────────────────────────────────────
    set_pre_commit_delay(&db, None).await.map_err(box_err)?;

    // Confirm the delay is really gone before handing the cluster to the next
    // test: ack RTT must return to normal within the reload window.
    let producer = create_producer()?;
    let reset_topic = format!("ack-visibility-reset-{}", Uuid::new_v4());
    let reset_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let rtt = produce_one(&producer, &reset_topic, "reset-probe")
            .await
            .map_err(box_err)?;
        if rtt < Duration::from_millis(DETECT_MS) {
            break;
        }
        if Instant::now() > reset_deadline {
            return Err("GUC reset did not take effect within 15s (produce still slow)".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let _ = db
        .simple_query(&format!(
            "DELETE FROM kafka.messages WHERE topic_id IN (SELECT id FROM kafka.topics WHERE name = '{reset_topic}');\
             DELETE FROM kafka.topics WHERE name = '{reset_topic}'"
        ))
        .await;

    match body_result {
        Ok(Ok(())) => {
            println!("✅ Test PASSED\n");
            Ok(())
        }
        Ok(Err(e)) => Err(e.into()),
        Err(join_err) => Err(format!("test body panicked: {join_err}").into()),
    }
}

fn box_err(e: String) -> Box<dyn std::error::Error> {
    e.into()
}
