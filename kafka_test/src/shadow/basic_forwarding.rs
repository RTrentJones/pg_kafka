//! Basic shadow mode forwarding tests
//!
//! Tests for DualWrite/ExternalOnly modes with sync/async forwarding.

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

use super::assertions::{
    assert_external_kafka_empty, assert_local_message_count, assert_message_in_external_kafka,
};
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test DualWrite mode with synchronous forwarding
///
/// Produces a message and verifies it appears in both local DB and external Kafka.
pub async fn test_dual_write_sync() -> TestResult {
    println!("=== Test: DualWrite Sync Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("dual-write-sync").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (DualWrite + Sync)...");
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
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION
    println!("Step 3: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"dual-write-sync-key";
    let value = b"dual-write-sync-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Give some time for sync forwarding to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3a. Local DB verification
    println!("Checking local database...");
    assert_local_message_count(ctx.db(), &topic, 1).await?;
    println!("✅ Message found in local database\n");

    // 3b. External Kafka verification
    println!("Checking external Kafka...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: DualWrite Sync Mode\n");
    Ok(())
}

/// Test DualWrite mode with asynchronous forwarding
///
/// Produces a message and verifies it appears in both local DB and external Kafka
/// (with some delay for async delivery).
pub async fn test_dual_write_async() -> TestResult {
    println!("=== Test: DualWrite Async Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("dual-write-async").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (DualWrite + Async)...");
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
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION
    println!("Step 3: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"dual-write-async-key";
    let value = b"dual-write-async-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // 3a. Local DB verification (should be immediate)
    println!("Checking local database...");
    assert_local_message_count(ctx.db(), &topic, 1).await?;
    println!("✅ Message found in local database\n");

    // 3b. External Kafka verification (may need to wait for async delivery)
    println!("Checking external Kafka (waiting for async forwarding)...");
    // Async mode may take longer, use extended timeout
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(15))
        .await?;
    println!("✅ Message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: DualWrite Async Mode\n");
    Ok(())
}

/// Test ExternalOnly mode
///
/// Produces a message and verifies it only appears in external Kafka,
/// NOT in the local database (when external succeeds).
pub async fn test_external_only_mode() -> TestResult {
    println!("=== Test: ExternalOnly Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("external-only").await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode (ExternalOnly + Sync)...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::ExternalOnly,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION
    println!("Step 3: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"external-only-key";
    let value = b"external-only-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3a. External Kafka verification - should have the message
    println!("Checking external Kafka...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Message found in external Kafka\n");

    // 3b. Local DB verification - should NOT have the message (ExternalOnly skips local on success)
    println!("Checking local database (should be empty)...");
    assert_local_message_count(ctx.db(), &topic, 0).await?;
    println!("✅ No message in local database (as expected for ExternalOnly)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: ExternalOnly Mode\n");
    Ok(())
}

/// Test LocalOnly mode (shadow disabled)
///
/// Produces a message and verifies it only appears in local database,
/// NOT forwarded to external Kafka.
pub async fn test_local_only_mode() -> TestResult {
    println!("=== Test: LocalOnly Mode ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("local-only").await;

    println!("Step 1: Enabling shadow mode with LocalOnly (shadow disabled)...");
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::LocalOnly,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ LocalOnly mode enabled\n");

    // 2. ACTION
    println!("Step 2: Producing message to pg_kafka...");
    let producer = create_producer()?;
    let key = b"local-only-key";
    let value = b"local-only-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3a. Local DB verification - should have the message
    println!("Checking local database...");
    assert_local_message_count(ctx.db(), &topic, 1).await?;
    println!("✅ Message found in local database\n");

    // 3b. External Kafka verification - should NOT have the message
    println!("Checking external Kafka (should be empty)...");
    // Use shorter timeout since we expect nothing
    assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;
    println!("✅ No message in external Kafka (as expected for LocalOnly)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: LocalOnly Mode\n");
    Ok(())
}
