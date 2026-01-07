//! Multiple message consumption test
//!
//! Validates that consumers can receive multiple messages
//! from a topic in sequence.

use crate::common::{create_base_consumer, create_producer, TestResult, POLL_TIMEOUT};
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test consuming multiple messages from a topic
///
/// This test:
/// 1. Produces N messages to a topic
/// 2. Creates a consumer starting from offset 0
/// 3. Polls until all messages are received
/// 4. Verifies the count matches
pub async fn test_consumer_multiple_messages() -> TestResult {
    println!("=== Test: Consumer Multiple Messages ===\n");

    let topic = "multi-message-topic";
    let message_count = 5;
    let partition = 0;

    // 1. Produce multiple messages
    println!("Step 1: Producing {} messages...", message_count);
    let producer = create_producer()?;

    for i in 0..message_count {
        let key = format!("key-{}", i);
        let value = format!("Message number {}", i);

        producer
            .send(
                FutureRecord::to(topic).payload(&value).key(&key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
    }

    println!("✅ {} messages produced", message_count);

    // 2. Consume all messages using manual partition assignment
    println!("\nStep 2: Consuming {} messages...", message_count);
    let consumer = create_base_consumer("manual-multi-consumer")?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, partition, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    let mut received_count = 0;
    let timeout = Duration::from_secs(15);
    let start = std::time::Instant::now();

    while received_count < message_count && start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                let msg_value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string())
                    .unwrap_or_default();

                println!("   Received: {}", msg_value);
                received_count += 1;
            }
            Some(Err(e)) => {
                println!("⚠️  Consumer error: {}", e);
            }
            None => {
                continue;
            }
        }
    }

    assert_eq!(
        received_count, message_count,
        "Did not receive all messages"
    );
    println!("✅ All {} messages received", received_count);

    println!("\n✅ Multiple messages test PASSED\n");
    Ok(())
}
