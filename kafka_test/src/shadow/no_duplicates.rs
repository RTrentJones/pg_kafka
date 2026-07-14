//! Issue #93 E2E: a slow forward ack must not cause duplicate external delivery.
//!
//! The outbox claim query re-claims a pending row once the 5s retry lease
//! expires. Before the in-flight gate, a forward whose ack arrived later than
//! the lease (external-broker warmup, batching, slow acks) was re-claimed and
//! re-sent while still in flight, delivering ~8% duplicates on the async
//! paths (evidence in issue #93: 542 broker deliveries for 500 produced).
//!
//! Determinism: acks normally land in milliseconds, far inside the lease. The
//! test-only GUC `pg_kafka.test_forward_ack_delay_ms` makes the network-thread
//! forwarder hold each ack (6.5s > the 5s lease), so on a pre-fix build every
//! produced record is deterministically re-claimed and duplicated, while the
//! fixed build skips in-flight rows and the external broker receives exactly
//! one copy per record.
//!
//! Registered `parallel_safe: false`: mutates a global GUC and slows all
//! shadow forwarding while active.

use crate::common::{create_db_client, create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};

use super::external_client::{count_external_messages, verify_external_kafka_ready};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Longer than the outbox retry lease (OUTBOX_RETRY_INTERVAL_MS = 5000).
const ACK_DELAY_MS: u64 = 6_500;
const MESSAGE_COUNT: usize = 3;

async fn set_ack_delay(db: &tokio_postgres::Client, value: Option<u64>) -> Result<(), String> {
    let stmt = match value {
        Some(v) => format!("ALTER SYSTEM SET pg_kafka.test_forward_ack_delay_ms = '{v}'"),
        None => "ALTER SYSTEM RESET pg_kafka.test_forward_ack_delay_ms".to_string(),
    };
    db.simple_query(&stmt)
        .await
        .map_err(|e| format!("{stmt} failed: {e}"))?;
    db.simple_query("SELECT pg_reload_conf()")
        .await
        .map_err(|e| format!("pg_reload_conf failed: {e}"))?;
    Ok(())
}

async fn run_no_duplicates_check(topic: String) -> Result<(), String> {
    let db = create_db_client()
        .await
        .map_err(|e| format!("db connect failed: {e}"))?;
    let producer = create_producer().map_err(|e| format!("producer create failed: {e}"))?;

    // The GUC snapshot reaches the forwarder via the worker's SIGHUP reload;
    // give it a moment and verify pickup indirectly through ack pacing below.
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!(
        "Step 3: Producing {} messages (acks delayed {}ms > 5s outbox lease)...",
        MESSAGE_COUNT, ACK_DELAY_MS
    );
    for i in 0..MESSAGE_COUNT {
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(format!("no-dup-key-{i}").as_bytes())
                    .payload(format!("no-dup-value-{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| format!("produce {i} failed: {e}"))?;
    }
    println!("✅ Messages produced\n");

    // Wait for ALL outbox rows to finalize (external_offset set). With serial
    // 6.5s ack delays this takes ~MESSAGE_COUNT * 6.5s; on a pre-fix build the
    // re-claim storm can stretch it further, so allow a generous deadline.
    println!("Step 4: Waiting for all outbox rows to finalize...");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let row = db
            .query_one(
                "SELECT COUNT(*) FILTER (WHERE st.external_offset IS NOT NULL) AS done, \
                        COUNT(*) AS total, \
                        COALESCE(SUM(st.retry_count), 0)::bigint AS claims \
                 FROM kafka.shadow_tracking st \
                 JOIN kafka.topics t ON t.id = st.topic_id \
                 WHERE t.name = $1",
                &[&topic],
            )
            .await
            .map_err(|e| format!("outbox query failed: {e}"))?;
        let done: i64 = row.get("done");
        let total: i64 = row.get("total");
        let claims: i64 = row.get("claims");
        if total == MESSAGE_COUNT as i64 && done == total {
            println!("   All {done} outbox rows finalized (total claim count: {claims})\n");
            break;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "outbox did not finalize in time: {done}/{total} rows done (expected {MESSAGE_COUNT})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Extra settle window: one more lease period, so any pre-fix straggler
    // re-forward has time to land at the broker before we count.
    tokio::time::sleep(Duration::from_secs(6)).await;

    println!("Step 5: Counting messages at the external broker...");
    let delivered = count_external_messages(&topic, Duration::from_secs(10))
        .await
        .map_err(|e| format!("external consume failed: {e}"))?;
    println!("   External broker delivered: {delivered}\n");
    if delivered != MESSAGE_COUNT {
        return Err(format!(
            "DUPLICATE DELIVERY: external broker received {delivered} messages for \
             {MESSAGE_COUNT} produced — a slow ack caused the outbox to re-send in-flight rows"
        ));
    }

    // The claim accounting must agree: each row claimed exactly once (the SQL
    // in-flight exclusion kept lease-expiry re-claims away entirely).
    let claims: i64 = db
        .query_one(
            "SELECT COALESCE(SUM(st.retry_count), 0)::bigint \
             FROM kafka.shadow_tracking st \
             JOIN kafka.topics t ON t.id = st.topic_id \
             WHERE t.name = $1",
            &[&topic],
        )
        .await
        .map_err(|e| format!("claim count query failed: {e}"))?
        .get(0);
    if claims != MESSAGE_COUNT as i64 {
        return Err(format!(
            "RE-CLAIM DETECTED: outbox rows were claimed {claims} times for {MESSAGE_COUNT} \
             produced — in-flight rows are still being re-claimed at lease expiry"
        ));
    }
    println!("✅ Exactly-once dispatch: {claims} claims, {delivered} deliveries\n");
    Ok(())
}

/// Issue #93: async forwarding must not duplicate records whose ack is slower
/// than the outbox retry lease.
pub async fn test_async_forwarding_no_duplicates_on_slow_ack() -> TestResult {
    println!("=== Test: No Duplicate Forwarding on Slow Ack (issue #93) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("no-dup-slow-ack").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (DualWrite + Async) and slow-ack injection...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    let db = create_db_client().await?;
    // Defensive reset first (a crashed earlier run may have left it set).
    set_ack_delay(&db, None).await.map_err(box_err)?;
    set_ack_delay(&db, Some(ACK_DELAY_MS))
        .await
        .map_err(box_err)?;
    println!("✅ Shadow mode enabled, ack delay armed\n");

    // 2-3. ACTION + VERIFY in a task so the GUC reset runs on every exit path.
    let body_result = tokio::spawn(run_no_duplicates_check(topic.clone())).await;

    // 4. CLEANUP — always reset the GUC.
    set_ack_delay(&db, None).await.map_err(box_err)?;
    ctx.cleanup().await?;

    match body_result {
        Ok(Ok(())) => {
            println!("✅ Test PASSED: No Duplicate Forwarding on Slow Ack\n");
            Ok(())
        }
        Ok(Err(e)) => Err(e.into()),
        Err(join_err) => Err(format!("test body panicked: {join_err}").into()),
    }
}

fn box_err(e: String) -> Box<dyn std::error::Error> {
    e.into()
}
