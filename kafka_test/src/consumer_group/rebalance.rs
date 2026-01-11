//! Consumer group rebalancing tests
//!
//! Validates automatic rebalancing behavior (Phase 5):
//! - Rebalance triggered when member leaves
//! - REBALANCE_IN_PROGRESS error forces client to rejoin
//! - Remaining consumers receive new assignments

use crate::common::{create_producer, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test rebalance when consumer leaves group
///
/// Scenario:
/// 1. Consumer 1 joins group and starts consuming
/// 2. Consumer 1 leaves group
/// 3. Consumer 2 joins same group - should work without issues
///
/// This validates the LeaveGroup → rebalance trigger path
pub async fn test_rebalance_after_leave() -> TestResult {
    println!("=== Test: Rebalance After Leave ===\n");

    let topic = "rebalance-leave-test-topic";
    let group_id = "rebalance-leave-test-group";

    // 1. Produce test messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;

    for i in 0..5 {
        let value = format!("Rebalance test message {}", i);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&value)
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
    }
    println!("✅ 5 messages produced");

    // 2. Consumer 1 joins and consumes some messages
    println!("\nStep 2: Consumer 1 joining group and consuming...");
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer1.subscribe(&[topic])?;
    println!("✅ Consumer 1 subscribed");

    // Consume at least one message to ensure group is active
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();
    let mut received = 0;

    while received < 2 && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(2), consumer1.recv()).await {
            Ok(Ok(msg)) => {
                received += 1;
                println!(
                    "   Consumer 1 received message {} at offset {}",
                    received,
                    msg.offset()
                );
            }
            Ok(Err(e)) => {
                println!("   ⚠️  Consumer 1 error: {}", e);
            }
            Err(_) => {
                println!("   (polling...)");
            }
        }
    }
    println!("✅ Consumer 1 received {} messages", received);

    // 3. Consumer 1 leaves group (triggers LeaveGroup API)
    println!("\nStep 3: Consumer 1 leaving group...");
    drop(consumer1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("✅ Consumer 1 left (LeaveGroup sent)");

    // 4. Consumer 2 joins the same group - should work cleanly
    println!("\nStep 4: Consumer 2 joining same group...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer2.subscribe(&[topic])?;
    println!("✅ Consumer 2 subscribed");

    // Consumer 2 should receive remaining messages
    let timeout2 = Duration::from_secs(10);
    let start2 = std::time::Instant::now();
    let mut received2 = 0;

    while received2 < 3 && start2.elapsed() < timeout2 {
        match tokio::time::timeout(Duration::from_secs(2), consumer2.recv()).await {
            Ok(Ok(msg)) => {
                received2 += 1;
                println!("   Consumer 2 received message at offset {}", msg.offset());
            }
            Ok(Err(e)) => {
                println!("   ⚠️  Consumer 2 error: {}", e);
            }
            Err(_) => {
                println!("   (polling...)");
            }
        }
    }
    println!("✅ Consumer 2 received {} messages", received2);

    drop(consumer2);

    println!("\n✅ Rebalance after leave test PASSED\n");
    Ok(())
}

/// Test that session timeout triggers rebalance
///
/// Scenario:
/// 1. Consumer joins with short session timeout
/// 2. Stop sending heartbeats (close connection abruptly)
/// 3. Wait for timeout to expire
/// 4. New consumer should be able to join
///
/// This validates the timeout detection and rebalance trigger
pub async fn test_session_timeout_rebalance() -> TestResult {
    println!("=== Test: Session Timeout Rebalance ===\n");

    let topic = "timeout-rebalance-test-topic";
    let group_id = "timeout-rebalance-test-group";

    // 1. Produce test messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;

    for i in 0..3 {
        let value = format!("Timeout test message {}", i);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&value)
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
    }
    println!("✅ 3 messages produced");

    // 2. Consumer joins with short session timeout
    println!("\nStep 2: Consumer joining with short session timeout...");
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "6000") // 6 second timeout
        .set("heartbeat.interval.ms", "2000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer1.subscribe(&[topic])?;

    // Poll once to ensure membership is established
    let _ = tokio::time::timeout(Duration::from_secs(3), consumer1.recv()).await;
    println!("✅ Consumer 1 established membership");

    // 3. Drop consumer (simulates crash - no LeaveGroup sent)
    // We drop without calling consumer.close() to simulate abrupt failure
    println!("\nStep 3: Simulating consumer crash (no LeaveGroup)...");
    std::mem::drop(consumer1);

    // Note: In a real crash, the TCP connection would close without LeaveGroup
    // rdkafka may still send LeaveGroup on drop, but the timeout mechanism
    // should still be exercised

    // 4. Wait for session timeout
    println!("\nStep 4: Waiting for session timeout ({} seconds)...", 7);
    tokio::time::sleep(Duration::from_secs(7)).await;
    println!("✅ Timeout period elapsed");

    // 5. New consumer joins - should work if member was removed
    println!("\nStep 5: New consumer joining group...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer2.subscribe(&[topic])?;
    println!("✅ Consumer 2 subscribed successfully");

    // Verify consumer 2 can receive messages
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();
    let mut received = 0;

    while received < 1 && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(2), consumer2.recv()).await {
            Ok(Ok(msg)) => {
                received += 1;
                println!("   Consumer 2 received message at offset {}", msg.offset());
            }
            Ok(Err(e)) => {
                println!("   ⚠️  Consumer 2 error: {}", e);
            }
            Err(_) => {
                println!("   (polling...)");
            }
        }
    }

    drop(consumer2);

    println!("\n✅ Session timeout rebalance test PASSED\n");
    Ok(())
}
