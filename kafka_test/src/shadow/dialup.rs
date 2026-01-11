//! Feature dial-up tests
//!
//! High-volume tests producing 500 messages at various percentages to verify
//! allocation accuracy and metrics tracking.

use crate::common::{create_batch_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

use super::assertions::assert_percentage_in_range;
use super::external_client::{consume_from_external, verify_external_kafka_ready};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};

const MESSAGE_COUNT: usize = 500;

/// Helper to run a dial-up test at a specific percentage
async fn run_dialup_test(
    percentage: u8,
    expected_min_pct: f64,
    expected_max_pct: f64,
) -> TestResult {
    println!(
        "=== Test: Dial-Up {}% Forwarding ({} messages) ===\n",
        percentage, MESSAGE_COUNT
    );

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic(&format!("dialup-{}", percentage)).await;

    println!("Step 1: Verifying external Kafka is ready...");
    verify_external_kafka_ready().await?;
    println!("✅ External Kafka ready\n");

    println!(
        "Step 2: Enabling shadow mode with {}% forwarding...",
        percentage
    );
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: percentage,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;
    println!("✅ Shadow mode enabled\n");

    // 2. ACTION - Produce 500 messages with unique keys
    println!("Step 3: Producing {} messages to pg_kafka...", MESSAGE_COUNT);
    let producer = create_batch_producer()?;

    for i in 0..MESSAGE_COUNT {
        let key = format!("dialup-{}-key-{}", percentage, i);
        let value = format!("dialup-{}-value-{}", percentage, i);
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(key.as_bytes())
                    .payload(value.as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ {} messages produced\n", MESSAGE_COUNT);

    // 3. VERIFY
    println!("=== Verification ===\n");

    // Wait for forwarding to complete
    println!("Waiting for forwarding to complete...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Count messages in external Kafka
    let external_messages =
        consume_from_external(&topic, MESSAGE_COUNT + 100, Duration::from_secs(15)).await?;
    let forwarded = external_messages.len();
    let skipped = MESSAGE_COUNT - forwarded;

    let actual_percentage = (forwarded as f64 / MESSAGE_COUNT as f64) * 100.0;

    println!("  Produced:  {}", MESSAGE_COUNT);
    println!("  Forwarded: {} ({:.1}%)", forwarded, actual_percentage);
    println!("  Skipped:   {} ({:.1}%)\n", skipped, 100.0 - actual_percentage);

    // Verify percentage within tolerance
    if percentage == 0 {
        // 0% should be exactly 0
        if forwarded != 0 {
            return Err(format!("Expected 0 forwarded for 0%, got {}", forwarded).into());
        }
        println!("✅ Exactly 0 messages forwarded (as expected for 0%)\n");
    } else if percentage == 100 {
        // 100% should be exactly all
        if forwarded != MESSAGE_COUNT {
            return Err(format!(
                "Expected {} forwarded for 100%, got {}",
                MESSAGE_COUNT, forwarded
            )
            .into());
        }
        println!("✅ All {} messages forwarded (as expected for 100%)\n", MESSAGE_COUNT);
    } else {
        // Other percentages within tolerance
        let tolerance = (expected_max_pct - expected_min_pct) / 2.0;
        let expected_pct = (expected_min_pct + expected_max_pct) / 2.0;
        assert_percentage_in_range(forwarded, MESSAGE_COUNT, expected_pct, tolerance)?;
        println!(
            "✅ Forwarding percentage {:.1}% within expected range ({:.0}%-{:.0}%)\n",
            actual_percentage, expected_min_pct, expected_max_pct
        );
    }

    // 4. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED: Dial-Up {}% Forwarding\n", percentage);
    Ok(())
}

/// Test 0% dial-up (exactly 0 forwarded)
pub async fn test_dialup_0_percent() -> TestResult {
    run_dialup_test(0, 0.0, 0.0).await
}

/// Test 10% dial-up (7-13% tolerance)
pub async fn test_dialup_10_percent() -> TestResult {
    run_dialup_test(10, 7.0, 13.0).await
}

/// Test 25% dial-up (20-30% tolerance)
pub async fn test_dialup_25_percent() -> TestResult {
    run_dialup_test(25, 20.0, 30.0).await
}

/// Test 50% dial-up (40-60% tolerance)
pub async fn test_dialup_50_percent() -> TestResult {
    run_dialup_test(50, 40.0, 60.0).await
}

/// Test 75% dial-up (65-85% tolerance)
pub async fn test_dialup_75_percent() -> TestResult {
    run_dialup_test(75, 65.0, 85.0).await
}

/// Test 100% dial-up (exactly 500 forwarded)
pub async fn test_dialup_100_percent() -> TestResult {
    run_dialup_test(100, 100.0, 100.0).await
}
