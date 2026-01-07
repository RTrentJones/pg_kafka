//! Multi-producer concurrent tests

use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use std::sync::Arc;

/// Test concurrent producers to same topic
pub async fn test_concurrent_producers_same_topic() -> TestResult {
    println!("=== Test: Concurrent Producers Same Topic ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "concurrent-produce")
        .build()
        .await?;

    let topic_name = Arc::new(topic.name.clone());
    let messages_per_producer = 10;
    let num_producers = 3;

    // Spawn multiple producer tasks
    let mut handles = Vec::new();

    for producer_id in 0..num_producers {
        let topic_name = topic_name.clone();
        let handle = tokio::spawn(async move {
            let producer = crate::common::create_producer().unwrap();
            let mut offsets = Vec::new();

            for i in 0..messages_per_producer {
                let value = format!("producer-{}-msg-{}", producer_id, i);
                let key = format!("key-{}-{}", producer_id, i);

                let result = producer
                    .send(
                        rdkafka::producer::FutureRecord::to(&topic_name)
                            .payload(&value)
                            .key(&key),
                        std::time::Duration::from_secs(10),
                    )
                    .await;

                if let Ok((_, offset)) = result {
                    offsets.push(offset);
                }
            }

            offsets
        });

        handles.push(handle);
    }

    // Wait for all producers
    let mut total_produced = 0;
    for handle in handles {
        let offsets = handle.await?;
        total_produced += offsets.len();
    }

    println!("   Total messages produced: {}", total_produced);
    assert_eq!(total_produced, num_producers * messages_per_producer);

    // Verify in database
    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
         JOIN kafka.topics t ON m.topic_id = t.id
         WHERE t.name = $1",
            &[&topic.name],
        )
        .await?;

    let db_count: i64 = row.get(0);
    assert_eq!(db_count, (num_producers * messages_per_producer) as i64);
    println!("✅ All {} messages verified in database", db_count);

    ctx.cleanup().await?;
    println!("\n✅ Concurrent producers same topic test PASSED\n");
    Ok(())
}

/// Test concurrent producers to different partitions with explicit partition targeting
pub async fn test_concurrent_producers_different_partitions() -> TestResult {
    println!("=== Test: Concurrent Producers Different Partitions ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "concurrent-partitions")
        .with_partitions(3)
        .build()
        .await?;

    let topic_name = Arc::new(topic.name.clone());

    // Each producer writes to its own partition
    let mut handles = Vec::new();

    for partition in 0..3i32 {
        let topic_name = topic_name.clone();
        let handle = tokio::spawn(async move {
            let producer = crate::common::create_producer().unwrap();
            let mut count = 0;
            let mut errors = Vec::new();

            for i in 0..5 {
                let value = format!("partition-{}-msg-{}", partition, i);

                let result = producer
                    .send(
                        rdkafka::producer::FutureRecord::to(&topic_name)
                            .payload(&value)
                            .key(&format!("key-{}", i))
                            .partition(partition),
                        std::time::Duration::from_secs(10),
                    )
                    .await;

                match result {
                    Ok(_) => count += 1,
                    Err((e, _)) => errors.push(format!("partition {}: {}", partition, e)),
                }
            }

            (count, errors)
        });

        handles.push(handle);
    }

    let mut total = 0;
    let mut all_errors = Vec::new();
    for handle in handles {
        let (count, errors) = handle.await?;
        total += count;
        all_errors.extend(errors);
    }

    // Report any errors
    if !all_errors.is_empty() {
        println!("   Errors encountered:");
        for err in &all_errors {
            println!("     - {}", err);
        }
    }

    assert_eq!(total, 15, "Expected 15 messages (3 partitions * 5 each), got {}", total);
    println!("✅ Produced {} messages across 3 partitions", total);

    // Verify each partition has 5 messages
    for partition in 0..3i32 {
        let row = ctx
            .db()
            .query_one(
                "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1 AND m.partition_id = $2",
                &[&topic.name, &partition],
            )
            .await?;

        let count: i64 = row.get(0);
        assert_eq!(count, 5, "Partition {} should have 5 messages", partition);
    }
    println!("✅ Each partition has correct message count");

    ctx.cleanup().await?;
    println!("\n✅ Concurrent producers different partitions test PASSED\n");
    Ok(())
}
