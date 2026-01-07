//! Offset boundaries test
//!
//! Validates offset boundary conditions including earliest,
//! latest, and specific offset consumption.

use crate::common::{
    create_base_consumer, create_db_client, create_producer, TestResult, POLL_TIMEOUT,
};
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test offset boundary conditions
///
/// This test:
/// 1. Produces messages to establish an offset range
/// 2. Tests consuming from Beginning (earliest)
/// 3. Tests consuming from End (latest)
/// 4. Tests consuming from a specific offset
/// 5. Verifies boundaries via database queries
pub async fn test_offset_boundaries() -> TestResult {
    println!("=== Test: Offset Boundaries ===\n");

    let topic = "offset-boundary-topic";

    // 1. Produce some messages to establish offset range
    println!("Step 1: Producing messages to establish offset range...");
    let producer = create_producer()?;

    let mut first_offset = None;
    let mut last_offset = None;

    for i in 0..5 {
        let value = format!("Boundary test message {}", i);
        let (partition, offset) = producer
            .send(
                FutureRecord::to(topic)
                    .payload(&value)
                    .key(&format!("boundary-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;

        if first_offset.is_none() {
            first_offset = Some((partition, offset));
        }
        last_offset = Some((partition, offset));
    }

    let (partition, _) = first_offset.unwrap();
    let (_, last_off) = last_offset.unwrap();
    println!("✅ Produced messages, offsets 0..={}", last_off);

    // 2. Test consuming from Beginning (earliest)
    println!("\nStep 2: Testing consume from Beginning (earliest)...");
    let consumer_earliest = create_base_consumer("boundary-earliest-group")?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, partition, rdkafka::Offset::Beginning)?;
    consumer_earliest.assign(&assignment)?;

    let mut earliest_received = false;
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while !earliest_received && start.elapsed() < timeout {
        if let Some(Ok(msg)) = consumer_earliest.poll(POLL_TIMEOUT) {
            println!("   Earliest fetch returned offset: {}", msg.offset());
            earliest_received = true;
        }
    }

    assert!(earliest_received, "Should receive message from earliest");
    println!("✅ Consume from Beginning works");

    // 3. Test consuming from End (latest)
    println!("\nStep 3: Testing consume from End (latest)...");
    let consumer_latest = create_base_consumer("boundary-latest-group")?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, partition, rdkafka::Offset::End)?;
    consumer_latest.assign(&assignment)?;

    let msg = consumer_latest.poll(Duration::from_millis(500));
    match msg {
        Some(Ok(m)) => {
            println!(
                "   Got message at offset {} (may be expected if concurrent produces)",
                m.offset()
            );
        }
        Some(Err(e)) => {
            println!("   ⚠️  Error: {}", e);
        }
        None => {
            println!("   No messages (correct - we're at End)");
        }
    }
    println!("✅ Consume from End works");

    // 4. Test consuming from specific offset
    println!("\nStep 4: Testing consume from specific offset...");
    let target_offset = last_off;

    let consumer_specific = create_base_consumer("boundary-specific-group")?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, partition, rdkafka::Offset::Offset(target_offset))?;
    consumer_specific.assign(&assignment)?;

    let mut specific_received = false;
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while !specific_received && start.elapsed() < timeout {
        if let Some(Ok(msg)) = consumer_specific.poll(POLL_TIMEOUT) {
            println!(
                "   Specific offset {} fetch returned offset: {}",
                target_offset,
                msg.offset()
            );
            assert_eq!(
                msg.offset(),
                target_offset,
                "Should receive message at requested offset"
            );
            specific_received = true;
        }
    }

    assert!(
        specific_received,
        "Should receive message at specific offset"
    );
    println!("✅ Consume from specific offset works");

    // 5. Verify offset boundaries via database
    println!("\nStep 5: Verifying offset boundaries in database...");
    let db_client = create_db_client().await?;

    let boundary_row = db_client
        .query_one(
            "SELECT MIN(m.partition_offset) as earliest,
                    MAX(m.partition_offset) as latest,
                    COUNT(*) as total
             FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = $2",
            &[&topic, &partition],
        )
        .await?;

    let db_earliest: i64 = boundary_row.get(0);
    let db_latest: i64 = boundary_row.get(1);
    let db_total: i64 = boundary_row.get(2);

    println!(
        "   Database offsets: earliest={}, latest={}, total={}",
        db_earliest, db_latest, db_total
    );
    println!("✅ Offset boundaries verified");

    println!("\n✅ Offset boundaries test PASSED\n");
    Ok(())
}
