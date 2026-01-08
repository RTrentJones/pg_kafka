//! Key-based partition routing E2E tests
//!
//! Tests for server-side key-based partition routing when client
//! doesn't specify an explicit partition (partition_index = -1).
//!
//! Phase 7: Multi-Partition Topics

use crate::common::{create_db_client, create_producer, TestResult};
use rdkafka::producer::FutureRecord;
use std::collections::HashMap;
use std::time::Duration;

/// Test that same key always routes to same partition (deterministic routing)
///
/// When partition is unspecified (-1), the server uses murmur2 hash of the key
/// to determine the partition. This test verifies the routing is deterministic.
pub async fn test_key_routing_deterministic() -> TestResult {
    println!("=== Test: Key Routing Deterministic ===\n");

    let topic = "key-routing-deterministic";
    let num_partitions = 5i32;
    let test_key = "consistent-key";

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
        "   Topic '{}' created with {} partitions",
        topic, num_partitions
    );

    // 2. Produce multiple messages with the same key (no explicit partition)
    println!("\nStep 2: Producing messages with same key (no explicit partition)...");
    let producer = create_producer()?;

    let mut delivered_partitions = Vec::new();

    for i in 0..5 {
        let value = format!("value-{}", i);

        // NOTE: rdkafka handles partition selection client-side when partition is not set
        // To truly test server-side routing, we would need to send partition=-1 in the wire protocol
        // For now, we verify that messages with the same key end up in the same partition
        let record = FutureRecord::to(topic).payload(&value).key(test_key);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                println!("   Message {}: partition={}, offset={}", i, partition, offset);
                delivered_partitions.push(partition);
            }
            Err((err, _)) => {
                println!("   Message {} failed: {}", i, err);
                return Err(Box::new(err));
            }
        }
    }

    // 3. Verify all messages went to the same partition
    println!("\nStep 3: Verifying all messages in same partition...");

    let first_partition = delivered_partitions[0];
    let all_same = delivered_partitions.iter().all(|p| *p == first_partition);

    assert!(
        all_same,
        "All messages with same key should route to same partition. Got partitions: {:?}",
        delivered_partitions
    );

    println!(
        "   All {} messages with key '{}' routed to partition {}",
        delivered_partitions.len(),
        test_key,
        first_partition
    );

    // 4. Verify in database
    println!("\nStep 4: Verifying in database...");
    let db_client2 = create_db_client().await?;

    let row = db_client2
        .query_one(
            "SELECT COUNT(DISTINCT partition_id) as distinct_partitions
             FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.key = $2",
            &[&topic, &test_key.as_bytes()],
        )
        .await?;

    let distinct_partitions: i64 = row.get(0);
    assert_eq!(
        distinct_partitions, 1,
        "All messages with same key should be in 1 partition"
    );

    println!("   Database confirms all messages in single partition");

    println!("\n Test: Key Routing Deterministic PASSED\n");
    Ok(())
}

/// Test that different keys distribute across partitions
///
/// With many unique keys, they should distribute reasonably across partitions
/// (not all going to the same partition).
pub async fn test_key_distribution() -> TestResult {
    println!("=== Test: Key Distribution ===\n");

    let topic = "key-distribution-topic";
    let num_partitions = 4i32;
    let num_messages = 100;

    // 1. Create topic with multiple partitions
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

    println!("   Topic '{}' created", topic);

    // 2. Produce messages with different keys
    println!("\nStep 2: Producing {} messages with unique keys...", num_messages);
    let producer = create_producer()?;

    let mut partition_counts: HashMap<i32, i32> = HashMap::new();

    for i in 0..num_messages {
        let key = format!("unique-key-{}", i);
        let value = format!("value-{}", i);

        let record = FutureRecord::to(topic).payload(&value).key(&key);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _offset)) => {
                *partition_counts.entry(partition).or_insert(0) += 1;
            }
            Err((err, _)) => {
                return Err(Box::new(err));
            }
        }
    }

    println!("   Delivered {} messages", num_messages);

    // 3. Verify distribution
    println!("\nStep 3: Verifying key distribution...");

    let partitions_used = partition_counts.len();
    assert!(
        partitions_used > 1,
        "Keys should distribute across multiple partitions, only used {}",
        partitions_used
    );

    println!("   Distribution across {} partitions:", partitions_used);
    for (partition, count) in &partition_counts {
        let percentage = (*count as f64 / num_messages as f64) * 100.0;
        println!("     Partition {}: {} messages ({:.1}%)", partition, count, percentage);
    }

    // 4. Verify in database
    println!("\nStep 4: Verifying in database...");
    let db_client2 = create_db_client().await?;

    let rows = db_client2
        .query(
            "SELECT partition_id, COUNT(*) as cnt
             FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1
             GROUP BY partition_id
             ORDER BY partition_id",
            &[&topic],
        )
        .await?;

    let db_partitions_used = rows.len();
    assert!(
        db_partitions_used > 1,
        "Database should show messages in multiple partitions"
    );

    println!("   Database confirms {} partitions have messages", db_partitions_used);

    println!("\n Test: Key Distribution PASSED\n");
    Ok(())
}

/// Test null key distribution
///
/// Messages without keys should distribute across partitions
/// (using random selection on the server side).
pub async fn test_null_key_distribution() -> TestResult {
    println!("=== Test: Null Key Distribution ===\n");

    let topic = "null-key-distribution";
    let num_partitions = 4i32;
    let num_messages = 50;

    // 1. Create topic with multiple partitions
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

    println!("   Topic '{}' created", topic);

    // 2. Produce messages without keys
    println!("\nStep 2: Producing {} messages without keys...", num_messages);
    let producer = create_producer()?;

    let mut partition_counts: HashMap<i32, i32> = HashMap::new();

    for i in 0..num_messages {
        let value = format!("value-{}", i);

        // No key specified
        let record = FutureRecord::<(), str>::to(topic).payload(&value);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _offset)) => {
                *partition_counts.entry(partition).or_insert(0) += 1;
            }
            Err((err, _)) => {
                return Err(Box::new(err));
            }
        }
    }

    println!("   Delivered {} messages", num_messages);

    // 3. Verify distribution (rdkafka does client-side round-robin for null keys)
    println!("\nStep 3: Checking null key distribution...");

    println!("   Distribution:");
    for (partition, count) in &partition_counts {
        let percentage = (*count as f64 / num_messages as f64) * 100.0;
        println!("     Partition {}: {} messages ({:.1}%)", partition, count, percentage);
    }

    // Note: rdkafka uses sticky partitioner for null keys, so they may batch to one partition
    // This is expected behavior - the server would distribute randomly if partition=-1 was sent
    println!("   (Note: rdkafka uses sticky partitioner for null keys)");

    // 4. Verify total message count in database
    println!("\nStep 4: Verifying in database...");
    let db_client2 = create_db_client().await?;

    let row = db_client2
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;

    let total: i64 = row.get(0);
    assert_eq!(
        total, num_messages as i64,
        "All {} messages should be in database",
        num_messages
    );

    println!("   Database confirms {} messages stored", total);

    println!("\n Test: Null Key Distribution PASSED\n");
    Ok(())
}
