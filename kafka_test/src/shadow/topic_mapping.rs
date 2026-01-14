//! Topic name mapping tests
//!
//! Tests for external topic name mapping (local-topic -> external-topic).

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

use super::assertions::assert_message_in_external_kafka;
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test topic name mapping
///
/// Produces a message to local topic and verifies it appears in
/// the mapped external topic name.
pub async fn test_topic_name_mapping() -> TestResult {
    println!("=== Test: Topic Name Mapping ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let local_topic = ctx.unique_topic("local-topic").await;
    let external_topic = format!("{}-external", local_topic);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode with topic mapping...");
    println!("  Local topic:    {}", local_topic);
    println!("  External topic: {}", external_topic);
    enable_shadow_mode(
        ctx.db(),
        &local_topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: Some(external_topic.clone()),
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled with topic mapping\n");

    // 2. ACTION
    println!("Step 3: Producing message to local topic...");
    let producer = create_producer()?;
    let key = b"mapping-key";
    let value = b"mapping-value";

    producer
        .send(
            FutureRecord::to(&local_topic)
                .key(&key[..])
                .payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced to local topic\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Message should appear in the MAPPED external topic, not the local topic name
    println!(
        "Checking message in mapped external topic '{}'...",
        external_topic
    );
    assert_message_in_external_kafka(
        &external_topic,
        Some(key),
        Some(value),
        Duration::from_secs(10),
    )
    .await?;
    println!("✅ Message found in mapped external topic\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Topic Name Mapping\n");
    Ok(())
}
