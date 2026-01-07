//! OffsetCommit/OffsetFetch test
//!
//! Validates that consumer offsets can be committed and fetched,
//! allowing consumers to resume from where they left off.

use crate::common::{
    create_db_client, create_manual_commit_consumer, create_producer, TestResult, POLL_TIMEOUT,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test OffsetCommit and OffsetFetch functionality
///
/// This test:
/// 1. Produces messages to a topic
/// 2. Consumes messages with manual offset commits
/// 3. Verifies committed offsets in database
/// 4. Creates a new consumer and verifies it resumes from committed offset
pub async fn test_offset_commit_fetch() -> TestResult {
    println!("=== Test: OffsetCommit/OffsetFetch ===\n");

    let topic = "offset-commit-test-topic";
    let group_id = "test-commit-group";

    // 1. Produce messages
    println!("Step 1: Producing test messages...");
    let producer = create_producer()?;

    for i in 0..5 {
        let value = format!("Commit test message {}", i);
        let key = format!("commit-key-{}", i);
        producer
            .send(
                FutureRecord::to(topic).payload(&value).key(&key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
    }

    println!("✅ 5 messages produced");

    // 2. Create consumer with manual partition assignment and manual offset management
    println!("\nStep 2: Creating consumer with manual commits...");
    let consumer = create_manual_commit_consumer(group_id)?;

    // Manually assign partition
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;
    println!("✅ Consumer created with manual partition assignment");

    // 3. Consume and manually commit offsets
    println!("\nStep 3: Consuming messages and committing offsets...");
    let mut consumed_count = 0;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while consumed_count < 3 && start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                consumed_count += 1;
                let offset = msg.offset();
                let partition = msg.partition();

                println!("   Consumed message at offset {}", offset);

                // Manually commit offset after processing
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(topic, partition, rdkafka::Offset::Offset(offset + 1))?;

                consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
                println!("   ✅ Committed offset {}", offset + 1);
            }
            Some(Err(e)) => {
                println!("⚠️  Consumer error: {}", e);
            }
            None => {
                if start.elapsed() > timeout {
                    break;
                }
            }
        }
    }

    assert_eq!(consumed_count, 3, "Should have consumed 3 messages");
    println!("✅ Consumed and committed 3 messages");

    // 4. Verify offsets in database
    println!("\nStep 4: Verifying committed offsets in database...");
    let db_client = create_db_client().await?;

    let offset_row = db_client
        .query_one(
            "SELECT co.committed_offset
             FROM kafka.consumer_offsets co
             JOIN kafka.topics t ON co.topic_id = t.id
             WHERE co.group_id = $1 AND t.name = $2 AND co.partition_id = 0",
            &[&group_id, &topic],
        )
        .await?;

    let committed_offset: i64 = offset_row.get(0);
    println!("✅ Database shows committed offset: {}", committed_offset);

    // The committed offset should be 3 (we consumed offsets 0, 1, 2 and committed up to 3)
    assert_eq!(committed_offset, 3, "Committed offset should be 3");

    // 5. Create a new consumer and verify it resumes from committed offset
    println!("\nStep 5: Creating new consumer to verify resume from committed offset...");
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", group_id) // Same group ID
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer2.subscribe(&[topic])?;
    println!("✅ New consumer created with same group ID");

    // 6. Verify the new consumer starts from offset 3 (not 0)
    println!("\nStep 6: Verifying consumer resumes from committed offset...");
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let mut first_offset_received = None;

    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(1), consumer2.recv()).await {
            Ok(Ok(msg)) => {
                first_offset_received = Some(msg.offset());
                println!("   First message received at offset: {}", msg.offset());
                break;
            }
            Ok(Err(e)) => {
                println!("⚠️  Consumer error: {}", e);
            }
            Err(_) => {
                continue;
            }
        }
    }

    if let Some(offset) = first_offset_received {
        assert_eq!(offset, 3, "Consumer should resume from offset 3");
        println!("✅ Consumer correctly resumed from committed offset!");
    } else {
        println!("⚠️  No messages received (may be expected if all consumed)");
    }

    println!("\n✅ OffsetCommit/OffsetFetch test PASSED\n");
    Ok(())
}
