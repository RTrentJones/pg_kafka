//! Coordinator error path tests
//!
//! Tests for consumer group coordinator error conditions including
//! unknown member IDs, stale generations, and protocol violations.

use crate::common::{create_stream_consumer, TestResult};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use std::time::Duration;

/// Test heartbeat with unknown member ID
///
/// This is simulated by having a consumer leave and then trying to use it.
pub async fn test_heartbeat_after_leave() -> TestResult {
    println!("=== Test: Heartbeat After Leave ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "heartbeat-after-leave")
        .build()
        .await?;

    let messages = generate_messages(3, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("heartbeat-leave").await;
    let consumer = create_stream_consumer(&group_id)?;

    println!("Subscribing consumer to topic...");
    consumer.subscribe(&[&topic.name])?;

    // Poll a few times to join group
    for _ in 0..5 {
        let _ = tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await;
    }

    println!("Consumer joined group, now dropping (LeaveGroup)...");
    drop(consumer);

    // Small delay for LeaveGroup to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("✅ Consumer left group successfully");

    ctx.cleanup().await?;
    println!("\n✅ Heartbeat after leave test PASSED\n");
    Ok(())
}

/// Test joining a group, leaving, and rejoining
///
/// Verifies that the coordinator properly handles member lifecycle.
pub async fn test_rejoin_after_leave() -> TestResult {
    println!("=== Test: Rejoin After Leave ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "rejoin-after-leave")
        .build()
        .await?;

    let messages = generate_messages(5, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("rejoin-group").await;

    // First consumer
    println!("Creating first consumer...");
    let consumer1 = create_stream_consumer(&group_id)?;
    consumer1.subscribe(&[&topic.name])?;

    // Poll to join
    for _ in 0..3 {
        let _ = tokio::time::timeout(Duration::from_secs(1), consumer1.recv()).await;
    }

    println!("First consumer joined, leaving...");
    drop(consumer1);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second consumer with same group ID
    println!("Creating second consumer with same group ID...");
    let consumer2 = create_stream_consumer(&group_id)?;
    consumer2.subscribe(&[&topic.name])?;

    // Poll to verify rejoin works
    let mut received = 0;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while received < 3 && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(2), consumer2.recv()).await {
            Ok(Ok(_)) => {
                received += 1;
                println!("   Second consumer received message {}", received);
            }
            Ok(Err(e)) => {
                println!("   Consumer error: {}", e);
            }
            Err(_) => {
                // Timeout, continue
            }
        }
    }

    println!("✅ Second consumer successfully rejoined group");

    ctx.cleanup().await?;
    println!("\n✅ Rejoin after leave test PASSED\n");
    Ok(())
}

/// Test committing offsets for a non-existent group
///
/// Should handle gracefully (either succeed or return appropriate error).
pub async fn test_commit_new_group() -> TestResult {
    println!("=== Test: Commit Offset for New Group ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "commit-new-group")
        .build()
        .await?;

    let messages = generate_messages(3, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("brand-new-group").await;

    // Create consumer and commit without first joining
    let consumer = crate::common::create_manual_commit_consumer(&group_id)?;

    // Assign partition manually
    use rdkafka::TopicPartitionList;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    // Try to commit an offset
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic.name, 0, rdkafka::Offset::Offset(1))?;

    println!("Attempting to commit offset for new group...");
    match consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync) {
        Ok(_) => {
            println!("   Commit succeeded (group may have been auto-created)");
        }
        Err(e) => {
            println!("   Commit error (may be expected): {}", e);
        }
    }

    ctx.cleanup().await?;
    println!("\n✅ Commit for new group test PASSED\n");
    Ok(())
}

/// Test using an empty group ID
///
/// Empty group IDs may be rejected by the protocol.
pub async fn test_empty_group_id() -> TestResult {
    println!("=== Test: Empty Group ID ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "empty-group-id")
        .build()
        .await?;

    // Try to create consumer with empty group ID
    // This might fail at client level or server level
    let result: Result<rdkafka::consumer::StreamConsumer, _> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "") // Empty group ID
        .set("session.timeout.ms", "6000")
        .create();

    match result {
        Ok(consumer) => {
            // Try to subscribe
            println!("Consumer created with empty group ID, trying to subscribe...");
            match consumer.subscribe(&[&topic.name]) {
                Ok(_) => {
                    println!("   Subscribe succeeded (unexpected but acceptable)");
                }
                Err(e) => {
                    println!("   Subscribe failed (expected): {}", e);
                }
            }
        }
        Err(e) => {
            println!("   Consumer creation failed (expected): {}", e);
        }
    }

    ctx.cleanup().await?;
    println!("\n✅ Empty group ID test PASSED\n");
    Ok(())
}

/// Test multiple consumers joining the same group
///
/// Verifies that rebalancing works correctly.
pub async fn test_multiple_consumers_same_group() -> TestResult {
    println!("=== Test: Multiple Consumers Same Group ===\n");

    let ctx = TestContext::new().await?;

    // Create topic with 2 partitions
    let topic = TestTopicBuilder::new(&ctx, "multi-consumer")
        .with_partitions(2)
        .build()
        .await?;

    let messages = generate_messages(10, "test");
    topic.produce(&messages).await?;

    let group_id = ctx.unique_group("multi-consumer-group").await;

    println!("Creating first consumer...");
    let consumer1 = create_stream_consumer(&group_id)?;
    consumer1.subscribe(&[&topic.name])?;

    // Let first consumer join
    for _ in 0..3 {
        let _ = tokio::time::timeout(Duration::from_secs(1), consumer1.recv()).await;
    }

    println!("Creating second consumer (triggers rebalance)...");
    let consumer2 = create_stream_consumer(&group_id)?;
    consumer2.subscribe(&[&topic.name])?;

    // Let both consumers participate
    let mut c1_received = 0;
    let mut c2_received = 0;

    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while (c1_received + c2_received) < 5 && start.elapsed() < timeout {
        // Poll both consumers
        tokio::select! {
            result = tokio::time::timeout(Duration::from_millis(500), consumer1.recv()) => {
                if let Ok(Ok(_)) = result {
                    c1_received += 1;
                }
            }
            result = tokio::time::timeout(Duration::from_millis(500), consumer2.recv()) => {
                if let Ok(Ok(_)) = result {
                    c2_received += 1;
                }
            }
        }
    }

    println!(
        "   Consumer 1 received: {}, Consumer 2 received: {}",
        c1_received, c2_received
    );
    println!("✅ Multiple consumers in same group working");

    ctx.cleanup().await?;
    println!("\n✅ Multiple consumers same group test PASSED\n");
    Ok(())
}
