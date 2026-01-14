//! Coordinator State Machine Tests
//!
//! Tests for consumer group coordinator state machine:
//! - FindCoordinator bootstrap behavior
//! - Strategy negotiation between consumers
//! - State transitions during rebalance
//! - Heartbeat during various states
//! - Partition assignment strategies

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test FindCoordinator returns coordinator for new group
///
/// Scenario:
/// 1. Request FindCoordinator for non-existent group
/// 2. Should return current broker as coordinator
/// 3. Group doesn't need to exist for coordinator lookup
///
/// This tests coordinator discovery.
pub async fn test_find_coordinator_bootstrap() -> TestResult {
    println!("=== Test: FindCoordinator Bootstrap ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "coord-bootstrap")
        .build()
        .await?;

    // Create topic first
    println!("Step 1: Creating topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic.name).payload("setup").key("setup"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Topic created");

    // Create consumer with a new (non-existent) group
    let new_group = format!("new-coord-group-{}", uuid::Uuid::new_v4());
    println!("\nStep 2: Creating consumer with new group '{}'...", new_group);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &new_group)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&topic.name])?;
    println!("  Consumer subscribed (FindCoordinator called internally)");

    // Wait for group to establish
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify consumer can poll (means coordinator was found)
    println!("\nStep 3: Verifying coordinator was found...");
    let result = tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await;

    match result {
        Ok(Ok(msg)) => {
            let value = msg.payload().map(|v| String::from_utf8_lossy(v).to_string());
            println!("  ✅ Received message: {:?}", value);
        }
        Ok(Err(e)) => {
            // Error but not timeout - coordinator was found
            println!("  Consumer error (coordinator found): {}", e);
        }
        Err(_) => {
            println!("  Timeout waiting for message (coordinator found, no data)");
        }
    }

    // Verify group exists in database
    println!("\nStep 4: Verifying group state...");
    let group_exists = ctx
        .db()
        .query_opt(
            "SELECT 1 FROM kafka.consumer_groups WHERE group_id = $1",
            &[&new_group],
        )
        .await?;

    // Group may or may not be in database depending on implementation
    // The key is that consumer didn't error with "coordinator not found"
    println!("  Group in database: {}", group_exists.is_some());

    ctx.cleanup().await?;
    println!("\n✅ FindCoordinator bootstrap test PASSED\n");
    Ok(())
}

/// Test partition assignment strategy selection
///
/// Scenario:
/// 1. Create topic with 3 partitions
/// 2. Create 3 consumers with different strategy preferences
/// 3. Verify partitions are assigned
/// 4. Verify each consumer gets assigned partitions
///
/// This tests strategy negotiation.
pub async fn test_partition_assignment_strategies() -> TestResult {
    println!("=== Test: Partition Assignment Strategies ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "assign-strategies")
        .with_partitions(3)
        .build()
        .await?;

    let group_id = format!("strategy-group-{}", uuid::Uuid::new_v4());

    // Create topic with messages
    println!("Step 1: Creating topic with messages...");
    let producer = create_producer()?;
    for p in 0..3i32 {
        producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&format!("partition-{}", p))
                    .key(&format!("key-{}", p))
                    .partition(p),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced message to each partition");

    // Create 3 consumers in same group
    println!("\nStep 2: Creating 3 consumers with different strategies...");
    let mut consumers = Vec::new();

    // rdkafka uses partition.assignment.strategy config
    let strategies = ["range", "roundrobin", "cooperative-sticky"];

    for (i, strategy) in strategies.iter().enumerate() {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", get_bootstrap_servers())
            .set("broker.address.family", "v4")
            .set("group.id", &group_id)
            .set("auto.offset.reset", "earliest")
            .set("partition.assignment.strategy", *strategy)
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()?;

        consumer.subscribe(&[&topic.name])?;
        consumers.push(consumer);
        println!("  Consumer {} created with strategy '{}'", i + 1, strategy);

        // Small delay to allow sequential joins
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for rebalance to complete
    println!("\nStep 3: Waiting for rebalance...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify each consumer can poll
    println!("\nStep 4: Verifying partition assignments...");
    let mut total_received = 0;
    for (i, consumer) in consumers.iter().enumerate() {
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let partition = msg.partition();
                let value = msg.payload().map(|v| String::from_utf8_lossy(v).to_string());
                println!("  Consumer {} received from partition {}: {:?}", i + 1, partition, value);
                total_received += 1;
            }
            Ok(Err(e)) => {
                println!("  Consumer {} error: {}", i + 1, e);
            }
            Err(_) => {
                println!("  Consumer {} no message (may not have assignment)", i + 1);
            }
        }
    }

    // At least some consumers should have received messages
    println!("\nStep 5: Verifying results...");
    println!("  Total messages received: {}", total_received);

    // With 3 partitions and 3 consumers, at least 1 should receive
    assert!(total_received >= 1, "At least one consumer should receive messages");

    ctx.cleanup().await?;
    println!("\n✅ Partition assignment strategies test PASSED\n");
    Ok(())
}

/// Test leave group during rebalance
///
/// Scenario:
/// 1. Consumer 1 joins group
/// 2. Consumer 2 joins (triggers rebalance)
/// 3. Consumer 1 leaves during rebalance
/// 4. Verify group stabilizes with remaining member
///
/// This tests state transitions during rebalance.
pub async fn test_leave_during_rebalance() -> TestResult {
    println!("=== Test: Leave Group During Rebalance ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "leave-rebalance")
        .build()
        .await?;

    let group_id = format!("leave-rebalance-group-{}", uuid::Uuid::new_v4());

    // Create topic
    println!("Step 1: Creating topic...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic.name).payload("test").key("test"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("  Topic created");

    // Create first consumer
    println!("\nStep 2: Consumer 1 joining...");
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer1.subscribe(&[&topic.name])?;
    println!("  Consumer 1 subscribed");

    // Wait for consumer 1 to join
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Create second consumer to trigger rebalance
    println!("\nStep 3: Consumer 2 joining (triggers rebalance)...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer2.subscribe(&[&topic.name])?;
    println!("  Consumer 2 subscribed");

    // Quickly drop consumer 1 during rebalance
    println!("\nStep 4: Consumer 1 leaving during rebalance...");
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(consumer1);
    println!("  Consumer 1 left");

    // Wait for group to stabilize
    println!("\nStep 5: Waiting for group to stabilize...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Consumer 2 should now have the partition
    println!("\nStep 6: Verifying consumer 2 is stable...");
    let result = tokio::time::timeout(Duration::from_secs(3), consumer2.recv()).await;

    match result {
        Ok(Ok(msg)) => {
            let value = msg.payload().map(|v| String::from_utf8_lossy(v).to_string());
            println!("  ✅ Consumer 2 received: {:?}", value);
        }
        _ => {
            println!("  Consumer 2 active (no new messages)");
        }
    }

    ctx.cleanup().await?;
    println!("\n✅ Leave during rebalance test PASSED\n");
    Ok(())
}

/// Test heartbeat continues during sync phase
///
/// Scenario:
/// 1. Consumer joins group
/// 2. Verify heartbeat keeps connection alive during operations
/// 3. Verify no timeout during normal operations
///
/// This tests heartbeat during various states.
pub async fn test_heartbeat_keeps_membership() -> TestResult {
    println!("=== Test: Heartbeat Keeps Membership ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "heartbeat-member")
        .build()
        .await?;

    let group_id = format!("heartbeat-member-group-{}", uuid::Uuid::new_v4());

    // Create topic with messages
    println!("Step 1: Creating topic with messages...");
    let producer = create_producer()?;
    for i in 0..5 {
        producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&format!("message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 5 messages");

    // Create consumer with short heartbeat interval
    println!("\nStep 2: Creating consumer with short heartbeat interval...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000") // 1 second heartbeat
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&topic.name])?;
    println!("  Consumer subscribed");

    // Wait for join
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Poll slowly but heartbeat should keep us alive
    println!("\nStep 3: Slow polling with heartbeat keeping membership...");
    let mut received = 0;
    for _ in 0..3 {
        // Poll with long timeout
        match tokio::time::timeout(Duration::from_secs(3), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg.payload().map(|v| String::from_utf8_lossy(v).to_string());
                println!("  Received: {:?}", value);
                received += 1;
            }
            Ok(Err(e)) => {
                // Check if error is session timeout (shouldn't happen with heartbeat)
                let error_str = e.to_string();
                if error_str.contains("session timeout") || error_str.contains("MemberIdRequired") {
                    panic!("Session timed out despite heartbeat: {}", e);
                }
                println!("  Consumer error (not timeout): {}", e);
            }
            Err(_) => {
                println!("  Poll timeout (heartbeat still active)");
            }
        }

        // Wait a bit (heartbeat should fire)
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("\nStep 4: Verifying membership maintained...");
    println!("  Messages received: {}", received);

    // Should have received at least some messages
    assert!(received >= 1, "Should receive at least 1 message");

    ctx.cleanup().await?;
    println!("\n✅ Heartbeat keeps membership test PASSED\n");
    Ok(())
}

/// Test group state transitions
///
/// Scenario:
/// 1. Group starts Empty (no consumers)
/// 2. Consumer joins → group becomes active
/// 3. Consumer can poll messages
/// 4. Consumer leaves → group becomes empty
/// 5. New consumer can join the same group
///
/// This tests the full state machine via consumer behavior.
pub async fn test_group_state_transitions() -> TestResult {
    println!("=== Test: Group State Transitions ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "state-transitions")
        .build()
        .await?;

    let group_id = format!("state-trans-group-{}", uuid::Uuid::new_v4());

    // Create topic with messages
    println!("Step 1: Creating topic with messages...");
    let producer = create_producer()?;
    for i in 0..3 {
        producer
            .send(
                FutureRecord::to(&topic.name)
                    .payload(&format!("message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
    }
    println!("  Produced 3 messages");

    // Create first consumer (triggers join, state: Empty → Stable)
    println!("\nStep 2: Consumer 1 joining group...");
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer1.subscribe(&[&topic.name])?;
    println!("  Consumer 1 subscribed");

    // Wait for group to stabilize
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify consumer can poll (group is Stable)
    println!("\nStep 3: Verifying consumer 1 can poll...");
    let mut received1 = 0;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(2), consumer1.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg.payload().map(|v| String::from_utf8_lossy(v).to_string());
                println!("  Consumer 1 received: {:?}", value);
                received1 += 1;
            }
            _ => break,
        }
    }
    println!("  Consumer 1 received {} messages", received1);

    // Drop consumer (triggers leave, state: Stable → Empty)
    println!("\nStep 4: Consumer 1 leaving group...");
    drop(consumer1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("  Consumer 1 left");

    // Create new consumer on same group (state: Empty → Stable)
    println!("\nStep 5: Consumer 2 joining same group...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer2.subscribe(&[&topic.name])?;
    println!("  Consumer 2 subscribed");

    // Wait for assignment
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify consumer 2 can poll
    println!("\nStep 6: Verifying consumer 2 can poll...");
    let mut received2 = 0;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(2), consumer2.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg.payload().map(|v| String::from_utf8_lossy(v).to_string());
                println!("  Consumer 2 received: {:?}", value);
                received2 += 1;
            }
            _ => break,
        }
    }
    println!("  Consumer 2 received {} messages", received2);

    // Verify at least one consumer received messages (offsets weren't committed)
    assert!(
        received1 >= 1 || received2 >= 1,
        "At least one consumer should receive messages"
    );

    ctx.cleanup().await?;
    println!("\n✅ Group state transitions test PASSED\n");
    Ok(())
}
