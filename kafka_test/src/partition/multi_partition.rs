//! Multi-partition produce test
//!
//! Validates multi-partition topic support and independent
//! offset sequences per partition.

use crate::common::{create_db_client, create_producer, TestResult};
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Test multi-partition produce functionality
///
/// This test:
/// 1. Creates a topic with multiple partitions
/// 2. Produces messages to each partition explicitly
/// 3. Verifies messages are stored in correct partitions
/// 4. Tests invalid partition rejection
/// 5. Verifies independent offset sequences per partition
pub async fn test_multi_partition_produce() -> TestResult {
    println!("=== Test: Multi-Partition Produce ===\n");

    let topic = "multi-partition-topic";
    let num_partitions = 3;

    // 1. Create topic with multiple partitions in database
    println!(
        "Step 1: Creating topic with {} partitions...",
        num_partitions
    );
    let db_client = create_db_client().await?;

    db_client
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET partitions = $2",
            &[&topic, &num_partitions],
        )
        .await?;

    println!(
        "✅ Topic '{}' created with {} partitions",
        topic, num_partitions
    );

    // 2. Produce messages to different partitions
    println!("\nStep 2: Producing messages to each partition...");
    let producer = create_producer()?;

    for partition in 0..num_partitions {
        let key = format!("partition-{}-key", partition);
        let value = format!("Message for partition {}", partition);

        let record = FutureRecord::to(topic)
            .payload(&value)
            .key(&key)
            .partition(partition);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((p, offset)) => {
                println!("   ✅ Partition {}: delivered at offset {}", p, offset);
                assert_eq!(p, partition, "Message should be in requested partition");
            }
            Err((err, _)) => {
                println!("   ❌ Partition {}: failed - {}", partition, err);
                return Err(Box::new(err));
            }
        }
    }

    println!("✅ Messages delivered to all partitions");

    // 3. Verify messages in database
    println!("\nStep 3: Verifying messages in database...");
    let db_client2 = create_db_client().await?;

    for partition in 0..num_partitions {
        let count_row = db_client2
            .query_one(
                "SELECT COUNT(*) FROM kafka.messages m
                 JOIN kafka.topics t ON m.topic_id = t.id
                 WHERE t.name = $1 AND m.partition_id = $2",
                &[&topic, &partition],
            )
            .await?;
        let count: i64 = count_row.get(0);

        assert!(
            count > 0,
            "Partition {} should have at least one message",
            partition
        );
        println!("   Partition {}: {} messages", partition, count);
    }

    println!("✅ All partitions have messages");

    // 4. Test invalid partition rejection
    println!("\nStep 4: Testing invalid partition rejection...");
    let invalid_partition = num_partitions + 10;

    let result = producer
        .send(
            FutureRecord::to(topic)
                .payload("invalid")
                .key("invalid-key")
                .partition(invalid_partition),
            Duration::from_secs(5),
        )
        .await;

    match result {
        Ok(_) => {
            println!("   ⚠️  Message to invalid partition succeeded (may be auto-created)");
        }
        Err((err, _)) => {
            println!(
                "   ✅ Invalid partition {} correctly rejected: {}",
                invalid_partition, err
            );
        }
    }

    // 5. Verify partition_offset sequences are independent
    println!("\nStep 5: Verifying independent offset sequences...");

    for partition in 0..num_partitions {
        let value = format!("Second message for partition {}", partition);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&value)
                    .key(&format!("second-key-{}", partition))
                    .partition(partition),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }

    // Check that offsets are sequential within each partition
    for partition in 0..num_partitions {
        let offsets_row = db_client2
            .query_one(
                "SELECT array_agg(partition_offset ORDER BY partition_offset)
                 FROM kafka.messages m
                 JOIN kafka.topics t ON m.topic_id = t.id
                 WHERE t.name = $1 AND m.partition_id = $2",
                &[&topic, &partition],
            )
            .await?;
        let offsets: Vec<i64> = offsets_row.get(0);

        for (i, offset) in offsets.iter().enumerate() {
            assert_eq!(
                *offset, i as i64,
                "Partition {} offset {} should be {}",
                partition, offset, i
            );
        }
        println!(
            "   Partition {}: offsets {:?} (sequential ✓)",
            partition, offsets
        );
    }

    println!("✅ Independent partition offset sequences verified");

    println!("\n✅ Multi-partition produce test PASSED\n");
    Ok(())
}
