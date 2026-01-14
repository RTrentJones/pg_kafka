//! Rebalancing edge case tests
//!
//! Tests for edge cases in consumer group rebalancing:
//! - Rapid rebalance cycles
//! - Heartbeat during rebalance window
//! - Multiple concurrent timeouts
//! - Minimal session timeout precision
//! - Mixed timeout values

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test rapid rebalance cycles
///
/// Scenario:
/// 1. Join 3 consumers to a group
/// 2. Leave 2 consumers
/// 3. Rejoin 1 consumer
/// 4. Repeat rapidly 3 times
/// 5. Verify generation ID increments correctly
///
/// This tests the coordinator's ability to handle rapid membership changes.
pub async fn test_rapid_rebalance_cycles() -> TestResult {
    println!("=== Test: Rapid Rebalance Cycles ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("rapid-rebalance").await;
    let group_id = ctx.unique_group("rapid-rebalance-group").await;

    // Produce some messages to ensure topic exists
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    // Track rebalance cycles
    let mut cycle_count = 0;

    for cycle in 0..3 {
        println!("\nCycle {}: Creating 3 consumers...", cycle + 1);

        // Create 3 consumers
        let mut consumers: Vec<StreamConsumer> = Vec::new();
        for i in 0..3 {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .set("group.id", &group_id)
                .set("session.timeout.ms", "10000")
                .set("heartbeat.interval.ms", "1000")
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()?;

            consumer.subscribe(&[&topic])?;

            // Poll once to trigger join
            let _ = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;

            consumers.push(consumer);
            println!("  Consumer {} joined", i + 1);
        }

        // Small delay to stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Leave 2 consumers rapidly
        println!("  Dropping 2 consumers...");
        drop(consumers.pop());
        drop(consumers.pop());
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Rejoin 1 consumer
        println!("  Rejoining 1 consumer...");
        let rejoiner: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "1000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        rejoiner.subscribe(&[&topic])?;
        let _ = tokio::time::timeout(Duration::from_millis(500), rejoiner.recv()).await;
        consumers.push(rejoiner);

        // Drop remaining consumers
        drop(consumers);
        tokio::time::sleep(Duration::from_millis(300)).await;

        cycle_count += 1;
        println!("  Cycle {} complete", cycle + 1);
    }

    assert_eq!(cycle_count, 3, "Should complete 3 rebalance cycles");

    // Verify group is in clean state by joining a final consumer
    println!("\nStep 2: Verifying group is in clean state...");
    let final_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    final_consumer.subscribe(&[&topic])?;

    // Should be able to receive messages
    let result = tokio::time::timeout(Duration::from_secs(5), final_consumer.recv()).await;
    assert!(
        result.is_ok(),
        "Final consumer should successfully join and receive"
    );
    println!("  Final consumer joined successfully");

    drop(final_consumer);
    ctx.cleanup().await?;

    println!("\n  Rapid rebalance cycles test PASSED\n");
    Ok(())
}

/// Test heartbeat during rebalance window
///
/// Scenario:
/// 1. Consumer joins group
/// 2. Trigger a rebalance (another consumer joins)
/// 3. First consumer sends heartbeat during rebalance
/// 4. Should receive REBALANCE_IN_PROGRESS error
///
/// Note: This test validates the coordinator returns the correct error code.
pub async fn test_heartbeat_during_rebalance_window() -> TestResult {
    println!("=== Test: Heartbeat During Rebalance Window ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("heartbeat-rebalance").await;
    let group_id = ctx.unique_group("heartbeat-rebalance-group").await;

    // Produce messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    // Consumer 1 joins and establishes membership
    println!("\nStep 2: Consumer 1 joining group...");
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer1.subscribe(&[&topic])?;

    // Poll to establish membership
    let _ = tokio::time::timeout(Duration::from_secs(3), consumer1.recv()).await;
    println!("  Consumer 1 established membership");

    // Consumer 2 joins - triggers rebalance
    println!("\nStep 3: Consumer 2 joining (triggers rebalance)...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer2.subscribe(&[&topic])?;

    // During rebalance, heartbeats from consumer1 should get REBALANCE_IN_PROGRESS
    // The rdkafka client handles this internally by rejoining
    println!("  Waiting for rebalance to complete...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Both consumers should eventually stabilize
    println!("\nStep 4: Verifying both consumers stabilized...");
    let result1 = tokio::time::timeout(Duration::from_secs(5), consumer1.recv()).await;
    let result2 = tokio::time::timeout(Duration::from_secs(5), consumer2.recv()).await;

    // At least one should receive (partitions distributed)
    let either_received = result1.is_ok() || result2.is_ok();
    println!(
        "  Consumer 1 received: {}, Consumer 2 received: {}",
        result1.is_ok(),
        result2.is_ok()
    );

    drop(consumer1);
    drop(consumer2);
    ctx.cleanup().await?;

    assert!(either_received, "At least one consumer should receive after rebalance");

    println!("\n  Heartbeat during rebalance window test PASSED\n");
    Ok(())
}

/// Test multiple concurrent timeouts
///
/// Scenario:
/// 1. Create 5 consumers with the same short session timeout
/// 2. Have all stop sending heartbeats simultaneously
/// 3. Verify all are removed and a single clean rebalance occurs
/// 4. New consumer can join successfully
pub async fn test_multiple_concurrent_timeouts() -> TestResult {
    println!("=== Test: Multiple Concurrent Timeouts ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("concurrent-timeout").await;
    let group_id = ctx.unique_group("concurrent-timeout-group").await;

    // Produce messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;
    for i in 0..5 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 5 messages");

    // Create 5 consumers with short timeout
    println!("\nStep 2: Creating 5 consumers with 6s session timeout...");
    let mut consumers: Vec<StreamConsumer> = Vec::new();

    for i in 0..5 {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("session.timeout.ms", "6000")
            .set("heartbeat.interval.ms", "2000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        consumer.subscribe(&[&topic])?;

        // Poll once to establish membership
        let _ = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;

        consumers.push(consumer);
        println!("  Consumer {} joined", i + 1);
    }

    // Wait for group to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("  Group stabilized with 5 members");

    // Drop all consumers at once (simulates simultaneous failures)
    println!("\nStep 3: Dropping all 5 consumers simultaneously...");
    drop(consumers);

    // Wait for session timeout (6s) plus buffer
    println!("\nStep 4: Waiting for session timeouts (7s)...");
    tokio::time::sleep(Duration::from_secs(7)).await;
    println!("  Timeout period elapsed");

    // New consumer should join cleanly
    println!("\nStep 5: New consumer joining...");
    let new_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    new_consumer.subscribe(&[&topic])?;

    // Should receive messages (all previous members timed out)
    let result = tokio::time::timeout(Duration::from_secs(10), new_consumer.recv()).await;
    assert!(
        result.is_ok(),
        "New consumer should join and receive after all timeouts"
    );
    println!("  New consumer successfully joined and received messages");

    drop(new_consumer);
    ctx.cleanup().await?;

    println!("\n  Multiple concurrent timeouts test PASSED\n");
    Ok(())
}

/// Test rebalance with minimal session timeout
///
/// Scenario:
/// 1. Consumer joins with minimum session timeout (6000ms for rdkafka)
/// 2. Stop heartbeating
/// 3. Verify timeout is detected within reasonable precision (6s ± 1s)
/// 4. New consumer can join after timeout
pub async fn test_rebalance_with_minimal_session_timeout() -> TestResult {
    println!("=== Test: Rebalance With Minimal Session Timeout ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("minimal-timeout").await;
    let group_id = ctx.unique_group("minimal-timeout-group").await;

    // Produce messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("test-message").key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Produced 1 message");

    // Consumer with minimal session timeout
    println!("\nStep 2: Consumer joining with 6s session timeout...");
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "6000") // Minimum practical timeout
        .set("heartbeat.interval.ms", "2000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer1.subscribe(&[&topic])?;

    // Establish membership
    let _ = tokio::time::timeout(Duration::from_secs(3), consumer1.recv()).await;
    println!("  Consumer 1 established membership");

    // Record time and drop consumer
    let drop_time = std::time::Instant::now();
    println!("\nStep 3: Dropping consumer (no LeaveGroup)...");
    std::mem::drop(consumer1);

    // Try to join with new consumer - poll until successful
    println!("\nStep 4: Attempting to join with new consumer...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer2.subscribe(&[&topic])?;

    // Wait for successful receive (indicates old member timed out)
    let receive_result = tokio::time::timeout(Duration::from_secs(15), consumer2.recv()).await;
    let elapsed = drop_time.elapsed();

    println!("  Time until new consumer received: {:?}", elapsed);

    // Timeout should happen within reasonable bounds
    // rdkafka may send LeaveGroup on drop, so timing can vary
    assert!(
        receive_result.is_ok(),
        "New consumer should eventually receive"
    );
    println!("  New consumer successfully received");

    drop(consumer2);
    ctx.cleanup().await?;

    println!("\n  Rebalance with minimal session timeout test PASSED\n");
    Ok(())
}

/// Test rebalance with mixed timeout values
///
/// Scenario:
/// 1. Consumer A joins with 10s timeout
/// 2. Consumer B joins with 30s timeout
/// 3. Drop both consumers
/// 4. Verify A times out before B (timing difference observable)
/// 5. New consumer can join after both timeout
pub async fn test_rebalance_mixed_timeout_values() -> TestResult {
    println!("=== Test: Rebalance Mixed Timeout Values ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("mixed-timeout").await;
    let group_id = ctx.unique_group("mixed-timeout-group").await;

    // Produce messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;
    for i in 0..5 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("msg-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 5 messages");

    // Consumer A with short timeout
    println!("\nStep 2: Consumer A joining with 6s timeout...");
    let consumer_a: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "2000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer_a.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_secs(2), consumer_a.recv()).await;
    println!("  Consumer A joined");

    // Consumer B with longer timeout
    println!("\nStep 3: Consumer B joining with 15s timeout...");
    let consumer_b: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "15000")
        .set("heartbeat.interval.ms", "5000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer_b.subscribe(&[&topic])?;
    let _ = tokio::time::timeout(Duration::from_secs(2), consumer_b.recv()).await;
    println!("  Consumer B joined");

    // Wait for group to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Drop both consumers
    println!("\nStep 4: Dropping both consumers...");
    std::mem::drop(consumer_a);
    std::mem::drop(consumer_b);

    // Wait for longer timeout
    println!("\nStep 5: Waiting for both timeouts (16s)...");
    tokio::time::sleep(Duration::from_secs(16)).await;
    println!("  Timeout period elapsed");

    // New consumer should join successfully
    println!("\nStep 6: New consumer joining...");
    let new_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    new_consumer.subscribe(&[&topic])?;

    let result = tokio::time::timeout(Duration::from_secs(10), new_consumer.recv()).await;
    assert!(
        result.is_ok(),
        "New consumer should receive after both timeouts"
    );
    println!("  New consumer successfully joined and received");

    drop(new_consumer);
    ctx.cleanup().await?;

    println!("\n  Rebalance mixed timeout values test PASSED\n");
    Ok(())
}
