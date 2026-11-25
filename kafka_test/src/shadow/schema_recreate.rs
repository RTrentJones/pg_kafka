//! Shadow forwarding across DROP EXTENSION + CREATE EXTENSION.
//!
//! An outbox row is keyed by `(topic_id, partition_id, local_offset)`, and
//! `kafka.topics.id` is a SERIAL that a re-created extension starts again, so the
//! first records of a re-created topic get the keys of records the old schema had
//! dispatched. A forward still outstanding when the schema went away belongs to
//! the old schema: its ack must not finalize the new row that happens to share its
//! key, and its key must not keep the new row from being claimed.
//!
//! The forwarder holds every ack for `ACK_DELAY_MS`
//! (`pg_kafka.test_forward_ack_delay_ms`), so the old schema's forwards are still
//! outstanding when the extension is re-created and the new topic's records are
//! produced under the same keys. Each new record must then reach the new external
//! topic itself; an old ack applied to its row would mark it delivered with the
//! old record's external offset while the new external topic stays empty.

use crate::common::{create_db_client, create_producer, TestResult};
use crate::setup::{SystemGucGuard, TestContext};
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};

use super::external_client::{consume_from_external, verify_external_kafka_ready};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Long enough that the old schema's acks arrive after the new topic's records
/// are written.
const ACK_DELAY_MS: u64 = 10_000;
const MESSAGE_COUNT: usize = 3;

fn async_config(external_topic: &str) -> ShadowTopicConfig {
    ShadowTopicConfig {
        mode: ShadowMode::Shadow,
        forward_percentage: 100,
        external_topic_name: Some(external_topic.to_string()),
        sync_mode: SyncMode::Async,
        write_mode: WriteMode::DualWrite,
    }
}

async fn produce(topic: &str, prefix: &str) -> TestResult {
    let producer = create_producer()?;
    for i in 0..MESSAGE_COUNT {
        producer
            .send(
                FutureRecord::to(topic)
                    .key(format!("{prefix}-key-{i}").as_bytes())
                    .payload(format!("{prefix}-value-{i}").as_bytes()),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(e, _)| format!("produce {prefix} {i} failed: {e}"))?;
    }
    Ok(())
}

async fn topic_id(
    db: &tokio_postgres::Client,
    topic: &str,
) -> Result<i32, Box<dyn std::error::Error>> {
    Ok(db
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0))
}

/// Acks of forwards dispatched before DROP EXTENSION + CREATE EXTENSION must not
/// finalize the re-created schema's outbox rows that reuse their keys.
pub async fn test_shadow_acks_from_a_dropped_schema_are_ignored() -> TestResult {
    println!("=== Test: Shadow Acks From A Dropped Schema Are Ignored ===\n");

    let ctx = TestContext::new().await?;
    let old_topic = ctx.unique_topic("shadow-recreate-old").await;
    let new_topic = ctx.unique_topic("shadow-recreate-new").await;
    let old_external = format!("{old_topic}-ext");
    let new_external = format!("{new_topic}-ext");
    verify_external_kafka_ready().await?;

    let ack_delay = SystemGucGuard::set(
        ctx.db(),
        "pg_kafka.test_forward_ack_delay_ms",
        &ACK_DELAY_MS.to_string(),
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or("pg_kafka.test_forward_ack_delay_ms is not available")?;

    let result = run(&ctx, &old_topic, &new_topic, &old_external, &new_external)
        .await
        .map_err(|e| e.to_string());
    ack_delay.restore().await.map_err(|e| e.to_string())?;
    ctx.cleanup().await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn run(
    ctx: &TestContext,
    old_topic: &str,
    new_topic: &str,
    old_external: &str,
    new_external: &str,
) -> TestResult {
    println!("Step 1: Producing {MESSAGE_COUNT} records on the old schema (acks held {ACK_DELAY_MS} ms)...");
    enable_shadow_mode(ctx.db(), old_topic, &async_config(old_external)).await?;
    let old_id = topic_id(ctx.db(), old_topic).await?;
    produce(old_topic, "old").await?;
    // Let the outbox tick dispatch them; their acks are held by the forwarder.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let outstanding: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.shadow_tracking WHERE topic_id = $1 AND external_offset IS NULL",
            &[&old_id],
        )
        .await?
        .get(0);
    assert_eq!(
        outstanding,
        i64::try_from(MESSAGE_COUNT)?,
        "the old records' forwards must still be unacknowledged"
    );

    println!(
        "Step 2: DROP EXTENSION + CREATE EXTENSION, the new topic reusing topic_id {old_id}..."
    );
    let ddl = create_db_client().await?;
    ddl.batch_execute("DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka;")
        .await?;
    ddl.execute(
        "SELECT setval('kafka.topics_id_seq', $1, false)",
        &[&i64::from(old_id)],
    )
    .await?;
    enable_shadow_mode(&ddl, new_topic, &async_config(new_external)).await?;
    let new_id = topic_id(&ddl, new_topic).await?;
    assert_eq!(
        new_id, old_id,
        "the new topic must reuse the old topic's id"
    );

    println!("Step 3: Producing {MESSAGE_COUNT} records on the new schema under the same keys...");
    produce(new_topic, "new").await?;

    println!("Step 4: Waiting for the new outbox rows to be finalized...");
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let done: i64 = ddl
            .query_one(
                "SELECT count(*) FROM kafka.shadow_tracking WHERE topic_id = $1 AND external_offset IS NOT NULL",
                &[&new_id],
            )
            .await?
            .get(0);
        if done == i64::try_from(MESSAGE_COUNT)? {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!("only {done}/{MESSAGE_COUNT} new outbox rows finalized").into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("Step 5: Every row marked delivered must be on the new external topic...");
    let external =
        consume_from_external(new_external, MESSAGE_COUNT, Duration::from_secs(15)).await?;
    let mut values: Vec<String> = external
        .iter()
        .filter_map(|m| m.value.as_deref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .collect();
    values.sort();
    let expected: Vec<String> = (0..MESSAGE_COUNT)
        .map(|i| format!("new-value-{i}"))
        .collect();
    assert_eq!(
        values, expected,
        "the new outbox rows are marked delivered, so their records must be on {new_external} \
         (an ack from the dropped schema finalized them instead)"
    );
    println!("✅ The new schema's records were forwarded themselves\n");
    Ok(())
}
