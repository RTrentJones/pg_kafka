//! Basic consumer functionality test
//!
//! Validates that messages can be consumed via Kafka protocol
//! using manual partition assignment.

use crate::common::{
    create_base_consumer, create_producer, TestResult, POLL_TIMEOUT, TEST_TIMEOUT,
};
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test basic consumer functionality with manual partition assignment
///
/// This test:
/// 1. Produces a test message to a topic
/// 2. Creates a consumer with manual partition assignment
/// 3. Assigns the consumer to the specific partition/offset
/// 4. Polls and verifies the message content matches
pub async fn test_consumer_basic() -> TestResult {
    println!("=== Test: Basic Consumer Functionality ===\n");

    let topic = "consumer-test-topic";

    // 1. Produce a message first
    println!("Step 1: Producing test message...");
    let producer = create_producer()?;

    let test_key = "consumer-key-1";
    let test_value = "Consumer test message 1";

    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic).payload(test_value).key(test_key),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| err)?;

    println!(
        "✅ Message produced: partition={}, offset={}",
        partition, offset
    );

    // 2. Create consumer with manual partition assignment
    println!("\nStep 2: Creating consumer with manual partition assignment...");
    let consumer = create_base_consumer("manual-consumer")?;
    println!("✅ Consumer created");

    // 3. Manually assign partition
    println!("\nStep 3: Manually assigning partition...");
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset))?;
    consumer.assign(&assignment)?;
    println!("✅ Partition assigned");

    // 4. Poll for the message
    println!("\nStep 4: Polling for message...");
    let start = std::time::Instant::now();

    let mut received = false;
    while start.elapsed() < TEST_TIMEOUT {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                let msg_key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let msg_value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                let msg_offset = msg.offset();
                let msg_partition = msg.partition();

                println!("✅ Message received:");
                println!("   Key: {:?}", msg_key);
                println!("   Value: {:?}", msg_value);
                println!("   Offset: {}", msg_offset);
                println!("   Partition: {}", msg_partition);

                // Verify message content
                assert_eq!(msg_key, Some(test_key.to_string()), "Key mismatch");
                assert_eq!(msg_value, Some(test_value.to_string()), "Value mismatch");
                assert_eq!(msg_offset, offset, "Offset mismatch");
                assert_eq!(msg_partition, partition, "Partition mismatch");

                received = true;
                break;
            }
            Some(Err(e)) => {
                println!("⚠️  Consumer error: {}", e);
            }
            None => {
                continue;
            }
        }
    }

    assert!(received, "Failed to receive message within timeout");

    println!("\n✅ Basic consumer test PASSED\n");
    Ok(())
}
