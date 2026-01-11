//! Transaction integration tests for shadow mode
//!
//! Tests for transaction commit/abort behavior with shadow mode forwarding.

use crate::common::{create_transactional_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;

use super::assertions::{assert_external_kafka_empty, assert_message_in_external_kafka};
use super::external_client::verify_external_kafka_ready;
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

/// Test that committed transactions are forwarded
///
/// Produces a message within a transaction, commits, and verifies
/// it appears in external Kafka.
pub async fn test_committed_transaction_forwarded() -> TestResult {
    println!("=== Test: Committed Transaction Forwarded ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-commit").await;
    let txn_id = format!("shadow-txn-{}", ctx.test_id);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode...");
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

    // 2. ACTION - Produce within transaction and commit
    println!("Step 3: Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("✅ Transactional producer initialized\n");

    println!("Step 4: Beginning transaction...");
    producer.begin_transaction()?;
    println!("✅ Transaction started\n");

    println!("Step 5: Producing message within transaction...");
    let key = b"txn-commit-key";
    let value = b"txn-commit-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    println!("Step 6: Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction committed\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka for committed message...");
    assert_message_in_external_kafka(&topic, Some(key), Some(value), Duration::from_secs(10))
        .await?;
    println!("✅ Committed message found in external Kafka\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Committed Transaction Forwarded\n");
    Ok(())
}

/// Test that aborted transactions are NOT forwarded
///
/// Produces a message within a transaction, aborts, and verifies
/// it does NOT appear in external Kafka.
pub async fn test_aborted_transaction_not_forwarded() -> TestResult {
    println!("=== Test: Aborted Transaction Not Forwarded ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("txn-abort").await;
    let txn_id = format!("shadow-abort-{}", ctx.test_id);

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!("Step 2: Enabling shadow mode...");
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

    // 2. ACTION - Produce within transaction and abort
    println!("Step 3: Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("✅ Transactional producer initialized\n");

    println!("Step 4: Beginning transaction...");
    producer.begin_transaction()?;
    println!("✅ Transaction started\n");

    println!("Step 5: Producing message within transaction...");
    let key = b"txn-abort-key";
    let value = b"txn-abort-value";

    producer
        .send(
            FutureRecord::to(&topic).key(&key[..]).payload(&value[..]),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Message produced\n");

    println!("Step 6: Aborting transaction...");
    producer.abort_transaction(Duration::from_secs(10))?;
    println!("✅ Transaction aborted\n");

    // 3. VERIFY
    println!("=== Verification ===\n");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("Checking external Kafka (should be empty)...");
    assert_external_kafka_empty(&topic, Duration::from_secs(3)).await?;
    println!("✅ No message in external Kafka (aborted transaction not forwarded)\n");

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Aborted Transaction Not Forwarded\n");
    Ok(())
}
