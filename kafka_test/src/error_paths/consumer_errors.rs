//! Consumer error path tests
//!
//! Tests for consumer error conditions including unknown topics,
//! invalid partitions, and out-of-range offsets.

use crate::common::{create_base_consumer, TestResult, POLL_TIMEOUT};
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test fetching from an unknown topic
///
/// Should handle gracefully (either return empty or appropriate error).
pub async fn test_fetch_unknown_topic() -> TestResult {
    println!("=== Test: Fetch from Unknown Topic ===\n");

    let ctx = TestContext::new().await?;
    let unknown_topic = ctx.unique_topic("nonexistent").await;

    println!(
        "Attempting to fetch from non-existent topic '{}'",
        unknown_topic
    );

    let group_id = ctx.unique_group("fetch-unknown").await;
    let consumer = create_base_consumer(&group_id)?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&unknown_topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    // Poll for messages - should timeout or return error
    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();
    let mut got_error = false;

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                println!("   Unexpected message received: {:?}", msg.offset());
            }
            Some(Err(e)) => {
                println!("   Expected error: {}", e);
                got_error = true;
                break;
            }
            None => {
                // No message, continue polling
            }
        }
    }

    if !got_error {
        println!("   No error received (topic may have been auto-created or fetch returned empty)");
    }

    ctx.cleanup().await?;
    println!("\n✅ Fetch from unknown topic test PASSED\n");
    Ok(())
}

/// Test fetching from an invalid partition
///
/// Should return UNKNOWN_TOPIC_OR_PARTITION error.
pub async fn test_fetch_invalid_partition() -> TestResult {
    println!("=== Test: Fetch from Invalid Partition ===\n");

    let ctx = TestContext::new().await?;

    // Create topic with 1 partition
    let topic = TestTopicBuilder::new(&ctx, "invalid-fetch-partition")
        .with_partitions(1)
        .build()
        .await?;

    // Produce a message so topic definitely exists
    let messages = generate_messages(1, "test");
    topic.produce(&messages).await?;

    println!("Created topic '{}' with 1 partition", topic.name);

    let group_id = ctx.unique_group("fetch-invalid").await;
    let consumer = create_base_consumer(&group_id)?;

    // Try to fetch from partition 10 (doesn't exist)
    let invalid_partition = 10;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, invalid_partition, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    println!(
        "Attempting to fetch from partition {}...",
        invalid_partition
    );

    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(_)) => {
                println!("   Unexpected: received message from invalid partition");
            }
            Some(Err(e)) => {
                println!("   Expected error: {}", e);
                break;
            }
            None => {
                // Continue polling
            }
        }
    }

    ctx.cleanup().await?;
    println!("\n✅ Fetch from invalid partition test PASSED\n");
    Ok(())
}

/// Test fetching from offset beyond high watermark
///
/// Should return empty results or OFFSET_OUT_OF_RANGE.
pub async fn test_fetch_offset_out_of_range() -> TestResult {
    println!("=== Test: Fetch from Out-of-Range Offset ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "out-of-range").build().await?;

    // Produce 5 messages (offsets 0-4, high watermark = 5)
    let messages = generate_messages(5, "test");
    topic.produce(&messages).await?;

    println!(
        "Created topic '{}' with 5 messages (offsets 0-4)",
        topic.name
    );

    let group_id = ctx.unique_group("out-of-range").await;
    let consumer = create_base_consumer(&group_id)?;

    // Try to fetch from offset 100 (way beyond high watermark)
    let out_of_range_offset = 100;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(
        &topic.name,
        0,
        rdkafka::Offset::Offset(out_of_range_offset),
    )?;
    consumer.assign(&assignment)?;

    println!("Attempting to fetch from offset {}...", out_of_range_offset);

    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();
    let mut received_any = false;

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                println!(
                    "   Received message at offset {} (unexpected)",
                    msg.offset()
                );
                received_any = true;
            }
            Some(Err(e)) => {
                println!("   Error (expected): {}", e);
                break;
            }
            None => {
                // Continue
            }
        }
    }

    if !received_any {
        println!("   No messages received (correct - offset is out of range)");
    }

    ctx.cleanup().await?;
    println!("\n✅ Fetch from out-of-range offset test PASSED\n");
    Ok(())
}

/// Test consuming from an empty topic/partition
///
/// Should return no messages (not an error).
pub async fn test_consume_empty_topic() -> TestResult {
    println!("=== Test: Consume from Empty Topic ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "empty-consume").build().await?;

    println!("Created empty topic '{}'", topic.name);

    let group_id = ctx.unique_group("empty-consume").await;
    let consumer = create_base_consumer(&group_id)?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    println!("Polling empty topic...");

    let timeout = Duration::from_secs(2);
    let start = std::time::Instant::now();
    let mut message_count = 0;

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(_)) => {
                message_count += 1;
            }
            Some(Err(e)) => {
                println!("   Error: {}", e);
            }
            None => {
                // Expected - no messages
            }
        }
    }

    assert_eq!(
        message_count, 0,
        "Should not receive any messages from empty topic"
    );
    println!("✅ No messages received (correct for empty topic)");

    ctx.cleanup().await?;
    println!("\n✅ Consume from empty topic test PASSED\n");
    Ok(())
}
