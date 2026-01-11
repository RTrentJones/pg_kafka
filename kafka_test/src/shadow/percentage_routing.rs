//! Percentage-based routing tests
//!
//! Tests for 0%, 50%, 100% routing and deterministic key-based routing.

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

use super::assertions::{
    assert_external_kafka_empty, assert_external_message_count,
    assert_external_message_count_in_range,
};
use super::external_client::{consume_from_external, verify_external_kafka_ready};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test 0% forwarding (all messages stay local)
///
/// Produces 10 messages with 0% forward percentage and verifies
/// none are forwarded to external Kafka.
pub async fn test_zero_percent_forwarding() -> TestResult {
    println!("=== Test: 0% Forwarding ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("zero-percent").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode with 0% forwarding...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 0,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled with 0% forwarding\n");

    // 2. ACTION
    println!("Step 3: Producing 10 messages to pg_kafka...");
    let producer = create_producer()?;
    let message_count = 10;

    for i in 0..message_count {
        let key = format!("zero-key-{}", i);
        let value = format!("zero-value-{}", i);
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(key.as_bytes())
                    .payload(value.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ {} messages produced\n", message_count);

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Checking external Kafka (should be empty)...");
    assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;
    println!("✅ No messages in external Kafka (0% forwarding confirmed)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: 0% Forwarding\n");
    Ok(())
}

/// Test 100% forwarding (all messages forwarded)
///
/// Produces 10 messages with 100% forward percentage and verifies
/// all are forwarded to external Kafka.
pub async fn test_hundred_percent_forwarding() -> TestResult {
    println!("=== Test: 100% Forwarding ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("hundred-percent").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode with 100% forwarding...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled with 100% forwarding\n");

    // 2. ACTION
    println!("Step 3: Producing 10 messages to pg_kafka...");
    let producer = create_producer()?;
    let message_count = 10;

    for i in 0..message_count {
        let key = format!("hundred-key-{}", i);
        let value = format!("hundred-value-{}", i);
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(key.as_bytes())
                    .payload(value.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ {} messages produced\n", message_count);

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka...");
    assert_external_message_count(&topic, message_count, Duration::from_secs(10)).await?;
    println!(
        "✅ All {} messages found in external Kafka (100% forwarding confirmed)\n",
        message_count
    );

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: 100% Forwarding\n");
    Ok(())
}

/// Test 50% forwarding (approximately half forwarded)
///
/// Produces 20 messages with 50% forward percentage and verifies
/// approximately 40-60% are forwarded to external Kafka.
pub async fn test_fifty_percent_forwarding() -> TestResult {
    println!("=== Test: 50% Forwarding ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("fifty-percent").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode with 50% forwarding...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled with 50% forwarding\n");

    // 2. ACTION
    println!("Step 3: Producing 20 messages to pg_kafka...");
    let producer = create_producer()?;
    let message_count = 20;

    for i in 0..message_count {
        let key = format!("fifty-key-{}", i);
        let value = format!("fifty-value-{}", i);
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(key.as_bytes())
                    .payload(value.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ {} messages produced\n", message_count);

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Expect 40-60% (8-12 messages out of 20)
    let min_expected = 8; // 40%
    let max_expected = 12; // 60%

    println!(
        "Checking external Kafka (expecting {}-{} messages)...",
        min_expected, max_expected
    );
    assert_external_message_count_in_range(
        &topic,
        min_expected,
        max_expected,
        Duration::from_secs(10),
    )
    .await?;

    let messages = consume_from_external(&topic, max_expected + 5, Duration::from_secs(5)).await?;
    let actual_percentage = (messages.len() as f64 / message_count as f64) * 100.0;
    println!(
        "✅ Found {} messages ({:.1}%) in external Kafka\n",
        messages.len(),
        actual_percentage
    );

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: 50% Forwarding\n");
    Ok(())
}

/// Test deterministic routing (same key always routes the same way)
///
/// Produces multiple messages with the same key and verifies they all
/// get the same routing decision (all forwarded or all skipped).
pub async fn test_deterministic_routing() -> TestResult {
    println!("=== Test: Deterministic Key-Based Routing ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("deterministic").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode with 50% forwarding...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 50,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled with 50% forwarding\n");

    // 2. ACTION - Produce multiple messages with same key
    println!("Step 3: Producing 10 messages with same key...");
    let producer = create_producer()?;
    let message_count = 10;
    let test_key = b"deterministic-same-key";

    for i in 0..message_count {
        let value = format!("deterministic-value-{}", i);
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(&test_key[..])
                    .payload(value.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ {} messages produced with same key\n", message_count);

    // 3. VERIFY - All messages with same key should have same routing decision
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let messages =
        consume_from_external(&topic, message_count + 5, Duration::from_secs(10)).await?;
    let external_count = messages.len();

    println!(
        "Found {} messages in external Kafka (out of {})",
        external_count, message_count
    );

    // With same key, all should route the same way: either 0 or message_count
    if external_count != 0 && external_count != message_count {
        return Err(format!(
            "Expected deterministic routing (0 or {}), but got {} messages. \
             Same key should always route the same way.",
            message_count, external_count
        )
        .into());
    }

    if external_count == 0 {
        println!("✅ All messages skipped (key hashes below 50% threshold)\n");
    } else {
        println!("✅ All messages forwarded (key hashes above 50% threshold)\n");
    }

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Deterministic Key-Based Routing\n");
    Ok(())
}
