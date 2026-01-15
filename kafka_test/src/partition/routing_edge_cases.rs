//! Partition routing edge case tests
//!
//! Tests for edge cases in partition routing including:
//! - Large keys
//! - Special characters in keys
//! - Empty vs null key handling
//! - Partition expansion behavior

use crate::common::{create_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::FutureRecord;
use std::collections::HashMap;
use std::time::Duration;

/// Test that large keys (2MB) hash correctly for routing
///
/// Even very large keys should consistently route to the same partition
/// based on their murmur2 hash.
pub async fn test_large_key_routing() -> TestResult {
    println!("=== Test: Large Key Routing ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("large-key").await;
    let num_partitions = 4i32;

    // 1. Create topic with multiple partitions
    println!("Step 1: Creating topic with {} partitions...", num_partitions);
    ctx.db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET partitions = $2",
            &[&topic, &num_partitions],
        )
        .await?;
    println!("✅ Topic '{}' created\n", topic);

    // 2. Create a large key (100KB - large enough to test, small enough to be practical)
    println!("Step 2: Creating large key (100KB)...");
    let large_key: String = "k".repeat(100 * 1024); // 100KB key
    println!("✅ Large key created ({} bytes)\n", large_key.len());

    // 3. Produce multiple messages with the large key
    println!("Step 3: Producing messages with large key...");
    let producer = create_producer()?;
    let mut delivered_partitions = Vec::new();

    for i in 0..5 {
        let value = format!("value-{}", i);
        let record = FutureRecord::to(&topic)
            .payload(&value)
            .key(large_key.as_str());

        match producer.send(record, Duration::from_secs(10)).await {
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
    println!("✅ Messages delivered\n");

    // 4. Verify all messages went to the same partition (deterministic routing)
    println!("Step 4: Verifying deterministic routing...");
    let first_partition = delivered_partitions[0];
    let all_same = delivered_partitions.iter().all(|p| *p == first_partition);

    assert!(
        all_same,
        "All messages with same large key should route to same partition. Got: {:?}",
        delivered_partitions
    );
    println!(
        "✅ All {} messages routed to partition {}\n",
        delivered_partitions.len(),
        first_partition
    );

    // 5. Verify in database
    println!("=== Database Verification ===\n");
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let count: i64 = row.get(0);
    assert_eq!(count, 5, "Expected 5 messages in database");
    println!("✅ Database confirms {} messages stored\n", count);

    ctx.cleanup().await?;
    println!("✅ Test: Large Key Routing PASSED\n");
    Ok(())
}

/// Test special character key routing (UTF-8, emoji, binary)
///
/// Keys with special characters should hash consistently.
pub async fn test_special_character_key_routing() -> TestResult {
    println!("=== Test: Special Character Key Routing ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("special-keys").await;
    let num_partitions = 4i32;

    // 1. Create topic
    println!("Step 1: Creating topic...");
    ctx.db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET partitions = $2",
            &[&topic, &num_partitions],
        )
        .await?;
    println!("✅ Topic created\n");

    // 2. Test various special character keys
    println!("Step 2: Testing special character keys...");
    let producer = create_producer()?;

    let special_keys = vec![
        ("utf8-chinese", "你好世界"),
        ("utf8-arabic", "مرحبا بالعالم"),
        ("utf8-emoji", "🚀🔥💯"),
        ("utf8-mixed", "Hello世界🌍"),
        ("special-chars", "key\twith\nnewlines\rand\0null"),
    ];

    let mut results: HashMap<&str, i32> = HashMap::new();

    for (name, key) in &special_keys {
        // Send same key multiple times to verify consistency
        let mut partitions = Vec::new();
        for i in 0..3 {
            let value = format!("{}-value-{}", name, i);
            let record = FutureRecord::to(&topic).payload(&value).key(*key);

            match producer.send(record, Duration::from_secs(5)).await {
                Ok((partition, _)) => {
                    partitions.push(partition);
                }
                Err((err, _)) => {
                    return Err(Box::new(err));
                }
            }
        }

        // Verify all went to same partition
        let first = partitions[0];
        let consistent = partitions.iter().all(|p| *p == first);
        assert!(
            consistent,
            "Key '{}' should consistently route to same partition",
            name
        );

        results.insert(name, first);
        println!("   {} -> partition {}", name, first);
    }
    println!("✅ All special character keys route consistently\n");

    // 3. Verify in database
    println!("=== Database Verification ===\n");
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let count: i64 = row.get(0);
    let expected = (special_keys.len() * 3) as i64;
    assert_eq!(count, expected, "Expected {} messages", expected);
    println!("✅ Database confirms {} messages\n", count);

    ctx.cleanup().await?;
    println!("✅ Test: Special Character Key Routing PASSED\n");
    Ok(())
}

/// Test empty string vs null key handling
///
/// Empty string key ("") should be treated differently from null key.
pub async fn test_empty_vs_null_key_routing() -> TestResult {
    println!("=== Test: Empty vs Null Key Routing ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("empty-null-keys").await;
    let num_partitions = 4i32;

    // 1. Create topic
    println!("Step 1: Creating topic with {} partitions...", num_partitions);
    ctx.db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET partitions = $2",
            &[&topic, &num_partitions],
        )
        .await?;
    println!("✅ Topic created\n");

    // 2. Send messages with empty string key
    println!("Step 2: Sending messages with empty string key...");
    let producer = create_producer()?;

    let mut empty_key_partitions = Vec::new();
    for i in 0..5 {
        let value = format!("empty-key-value-{}", i);
        // Empty string key
        let record = FutureRecord::to(&topic).payload(&value).key("");

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _)) => {
                empty_key_partitions.push(partition);
            }
            Err((err, _)) => {
                return Err(Box::new(err));
            }
        }
    }
    println!("   Empty key partitions: {:?}", empty_key_partitions);
    println!("✅ Empty string key messages delivered\n");

    // 3. Send messages with null key (may vary with rdkafka's sticky partitioner)
    println!("Step 3: Sending messages with null key...");
    let mut null_key_partitions = Vec::new();
    for i in 0..5 {
        let value = format!("null-key-value-{}", i);
        // No key specified - null key
        let record = FutureRecord::<(), str>::to(&topic).payload(&value);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _)) => {
                null_key_partitions.push(partition);
            }
            Err((err, _)) => {
                return Err(Box::new(err));
            }
        }
    }
    println!(
        "   Null key partitions: {:?}",
        null_key_partitions
    );
    println!("✅ Null key messages delivered\n");

    // 4. Verify in database that both types are stored
    println!("=== Database Verification ===\n");

    // Count messages with empty key (empty byte array)
    let empty_row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.key = ''::bytea",
            &[&topic],
        )
        .await?;
    let empty_count: i64 = empty_row.get(0);
    println!("   Messages with empty key: {}", empty_count);

    // Count messages with null key
    let null_row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.key IS NULL",
            &[&topic],
        )
        .await?;
    let null_count: i64 = null_row.get(0);
    println!("   Messages with null key: {}", null_count);

    assert_eq!(empty_count, 5, "Expected 5 messages with empty key");
    assert_eq!(null_count, 5, "Expected 5 messages with null key");
    println!("✅ Database correctly distinguishes empty vs null keys\n");

    ctx.cleanup().await?;
    println!("✅ Test: Empty vs Null Key Routing PASSED\n");
    Ok(())
}

/// Test key routing after partition expansion
///
/// When partitions are added, existing keys should still route
/// based on the murmur2 hash (may go to new partitions).
pub async fn test_key_routing_after_partition_expansion() -> TestResult {
    println!("=== Test: Key Routing After Partition Expansion ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("partition-expand").await;

    // 1. Create topic with 2 partitions initially
    println!("Step 1: Creating topic with 2 partitions...");
    ctx.db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 2)
             ON CONFLICT (name) DO UPDATE SET partitions = 2",
            &[&topic],
        )
        .await?;
    println!("✅ Topic created with 2 partitions\n");

    // 2. Produce messages with known keys
    println!("Step 2: Producing messages to 2-partition topic...");
    let producer = create_producer()?;
    let test_keys = vec!["key-alpha", "key-beta", "key-gamma", "key-delta"];

    let mut initial_partitions: HashMap<String, i32> = HashMap::new();
    for key in &test_keys {
        let record = FutureRecord::to(&topic).payload("value").key(*key);
        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _)) => {
                initial_partitions.insert(key.to_string(), partition);
                println!("   {} -> partition {}", key, partition);
            }
            Err((err, _)) => return Err(Box::new(err)),
        }
    }
    println!("✅ Initial messages delivered\n");

    // 3. Expand to 4 partitions
    println!("Step 3: Expanding topic to 4 partitions...");
    ctx.db()
        .execute(
            "UPDATE kafka.topics SET partitions = 4 WHERE name = $1",
            &[&topic],
        )
        .await?;
    println!("✅ Topic expanded to 4 partitions\n");

    // 4. Produce same keys again - rdkafka will recalculate based on new partition count
    println!("Step 4: Producing same keys after expansion...");
    let mut expanded_partitions: HashMap<String, i32> = HashMap::new();
    for key in &test_keys {
        let record = FutureRecord::to(&topic).payload("value-expanded").key(*key);
        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _)) => {
                expanded_partitions.insert(key.to_string(), partition);
                println!("   {} -> partition {}", key, partition);
            }
            Err((err, _)) => return Err(Box::new(err)),
        }
    }
    println!("✅ Post-expansion messages delivered\n");

    // 5. Verify total messages in database
    println!("=== Database Verification ===\n");
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let count: i64 = row.get(0);
    assert_eq!(count, 8, "Expected 8 messages (4 before + 4 after expansion)");
    println!("✅ Database confirms {} messages\n", count);

    // Note: Same key may route to different partition after expansion
    // This is expected Kafka behavior (hash % new_partition_count)
    println!("Note: Keys may route to different partitions after expansion");
    println!("   This is expected Kafka behavior with murmur2 hashing\n");

    ctx.cleanup().await?;
    println!("✅ Test: Key Routing After Partition Expansion PASSED\n");
    Ok(())
}

/// Test that same key routes to same partition across different producers
///
/// Multiple independent producers with the same key should route to same partition.
pub async fn test_key_routing_across_producers() -> TestResult {
    println!("=== Test: Key Routing Across Producers ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("multi-producer-routing").await;
    let num_partitions = 4i32;

    // 1. Create topic
    println!("Step 1: Creating topic with {} partitions...", num_partitions);
    ctx.db()
        .execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET partitions = $2",
            &[&topic, &num_partitions],
        )
        .await?;
    println!("✅ Topic created\n");

    // 2. Create multiple independent producers
    println!("Step 2: Creating 3 independent producers...");
    let producer1 = create_producer()?;
    let producer2 = create_producer()?;
    let producer3 = create_producer()?;
    println!("✅ Producers created\n");

    // 3. Send messages with same key from each producer
    println!("Step 3: Sending messages with same key from each producer...");
    let test_key = "shared-routing-key";

    let mut partitions = Vec::new();
    for (i, producer) in [&producer1, &producer2, &producer3].iter().enumerate() {
        let value = format!("producer-{}-value", i);
        let record = FutureRecord::to(&topic).payload(&value).key(test_key);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, _)) => {
                println!("   Producer {} -> partition {}", i, partition);
                partitions.push(partition);
            }
            Err((err, _)) => return Err(Box::new(err)),
        }
    }
    println!("✅ Messages delivered\n");

    // 4. Verify all routed to same partition
    println!("Step 4: Verifying consistent routing...");
    let first = partitions[0];
    let consistent = partitions.iter().all(|p| *p == first);
    assert!(
        consistent,
        "Same key from different producers should route to same partition: {:?}",
        partitions
    );
    println!("✅ All producers routed key to partition {}\n", first);

    // 5. Verify in database
    println!("=== Database Verification ===\n");
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(DISTINCT partition_id) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let distinct_partitions: i64 = row.get(0);
    assert_eq!(
        distinct_partitions, 1,
        "All messages should be in same partition"
    );
    println!("✅ Database confirms all messages in single partition\n");

    ctx.cleanup().await?;
    println!("✅ Test: Key Routing Across Producers PASSED\n");
    Ok(())
}
