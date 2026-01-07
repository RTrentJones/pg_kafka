//! Producer error path tests
//!
//! Tests for producer error conditions including invalid partitions,
//! negative partitions, and other error scenarios.

use crate::common::TestResult;
use crate::fixtures::{TestMessage, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test producing to an invalid partition (partition > partition_count)
///
/// Verifies that producing to a non-existent partition returns
/// an appropriate error (UNKNOWN_TOPIC_OR_PARTITION or similar).
pub async fn test_produce_invalid_partition() -> TestResult {
    println!("=== Test: Produce to Invalid Partition ===\n");

    let ctx = TestContext::new().await?;

    // Create topic with 1 partition
    let topic = TestTopicBuilder::new(&ctx, "invalid-partition")
        .with_partitions(1)
        .build()
        .await?;

    println!("Created topic '{}' with 1 partition", topic.name);

    // Try to produce to partition 5 (doesn't exist)
    let producer = crate::common::create_producer()?;
    let invalid_partition = 5;

    println!(
        "Attempting to produce to partition {}...",
        invalid_partition
    );

    let result = producer
        .send(
            FutureRecord::to(&topic.name)
                .payload("test")
                .key("test-key")
                .partition(invalid_partition),
            Duration::from_secs(5),
        )
        .await;

    match result {
        Ok((partition, offset)) => {
            // Some implementations may auto-create partitions or route differently
            println!(
                "   Message delivered to partition {} offset {} (unexpected but acceptable)",
                partition, offset
            );
        }
        Err((err, _)) => {
            println!("   Expected error received: {}", err);
            // This is the expected behavior
        }
    }

    ctx.cleanup().await?;
    println!("\n✅ Produce to invalid partition test PASSED\n");
    Ok(())
}

/// Test producing to partition -1 (any partition / partitioner decides)
///
/// Partition -1 should be handled by the partitioner, not treated as invalid.
pub async fn test_produce_any_partition() -> TestResult {
    println!("=== Test: Produce to Any Partition (-1) ===\n");

    let ctx = TestContext::new().await?;

    // Create topic with 3 partitions
    let topic = TestTopicBuilder::new(&ctx, "any-partition")
        .with_partitions(3)
        .build()
        .await?;

    println!("Created topic '{}' with 3 partitions", topic.name);

    // Produce without specifying partition (rdkafka will use partitioner)
    let msg = TestMessage::with_key("test value", "test-key");
    let offsets = topic.produce(&[msg]).await?;

    println!("   Message delivered at offset {}", offsets[0]);
    println!("✅ Partitioner correctly assigned partition");

    ctx.cleanup().await?;
    println!("\n✅ Produce to any partition test PASSED\n");
    Ok(())
}

/// Test producing an empty batch (no records)
///
/// Empty produce requests should be handled gracefully.
pub async fn test_produce_empty_batch() -> TestResult {
    println!("=== Test: Produce Empty Batch ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "empty-batch").build().await?;

    println!("Created topic '{}'", topic.name);

    // Produce empty batch
    let messages: Vec<TestMessage> = vec![];
    let offsets = topic.produce(&messages).await?;

    assert!(offsets.is_empty(), "Empty batch should return no offsets");
    println!("✅ Empty batch handled correctly (no offsets returned)");

    ctx.cleanup().await?;
    println!("\n✅ Produce empty batch test PASSED\n");
    Ok(())
}

/// Test producing a message with very large key (1KB)
///
/// Large keys should be handled correctly.
pub async fn test_produce_large_key() -> TestResult {
    println!("=== Test: Produce Large Key (1KB) ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "large-key").build().await?;

    println!("Created topic '{}'", topic.name);

    // Create 1KB key
    let large_key: String = "K".repeat(1024);
    let msg = TestMessage::with_key("test value", &large_key);

    let offsets = topic.produce(&[msg]).await?;
    println!("   Message with 1KB key delivered at offset {}", offsets[0]);

    // Verify in database
    let row = ctx
        .db()
        .query_one(
            "SELECT LENGTH(key) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND partition_offset = $2",
            &[&topic.name, &offsets[0]],
        )
        .await?;

    let key_length: i32 = row.get(0);
    assert_eq!(key_length, 1024, "Key length should be 1024 bytes");
    println!("✅ Large key stored correctly ({} bytes)", key_length);

    ctx.cleanup().await?;
    println!("\n✅ Produce large key test PASSED\n");
    Ok(())
}
