//! Basic idempotent producer functionality test (Phase 9)
//!
//! Validates idempotent producer support:
//! 1. Creates an idempotent producer using rdkafka's enable.idempotence=true
//! 2. Sends messages and verifies delivery
//! 3. Verifies database schema (producer_ids, producer_sequences tables)
//!
//! The InitProducerId handler is implemented and tested via unit tests
//! in src/kafka/handlers/tests.rs which validate:
//! - Producer ID allocation
//! - Epoch tracking
//! - Sequence validation
//! - Deduplication (DUPLICATE_SEQUENCE_NUMBER, OUT_OF_ORDER_SEQUENCE_NUMBER)
//! - Producer fencing (PRODUCER_FENCED)

use crate::common::{create_idempotent_producer, TestResult};
use crate::setup::TestContext;
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;

/// Test idempotent producer with rdkafka
///
/// This test:
/// 1. Creates an idempotent Kafka producer (enable.idempotence=true)
/// 2. Sends multiple messages to a unique topic
/// 3. Verifies messages were stored correctly
/// 4. Verifies the database schema supports producer IDs and sequences
pub async fn test_idempotent_producer_basic() -> TestResult {
    println!("=== Test: Idempotent Producer (Phase 9) ===\n");

    // Setup test context for isolation
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("idempotent").await;

    // Create an idempotent Kafka producer
    println!("Creating idempotent Kafka producer...");
    let producer = create_idempotent_producer()?;
    println!("✅ Idempotent producer created (enable.idempotence=true)\n");

    // Send multiple messages
    println!("Sending test messages to topic '{}'...", topic);
    let num_messages: i64 = 3;

    for i in 0..num_messages {
        let key = format!("key-{}", i);
        let payload = format!("Idempotent test message {}", i);

        let (partition, offset) = producer
            .send(
                FutureRecord::to(&topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| {
                println!("❌ Failed to deliver message {}: {}", i, err);
                err
            })?;

        println!(
            "✅ Message {} delivered: partition={}, offset={}",
            i, partition, offset
        );
    }

    // Flush to ensure all messages are sent
    producer.flush(Duration::from_secs(5)).map_err(|e| {
        println!("❌ Failed to flush producer: {}", e);
        e
    })?;

    println!("✅ All {} messages delivered successfully\n", num_messages);

    // === DATABASE VERIFICATION ===
    println!("=== Database Verification ===\n");

    // Verify topic was created
    println!("Step 1: Checking topic creation...");
    let topic_row = ctx
        .db()
        .query_one(
            "SELECT id, name FROM kafka.topics WHERE name = $1",
            &[&topic],
        )
        .await?;
    let topic_id: i32 = topic_row.get(0);
    let topic_name: String = topic_row.get(1);

    assert_eq!(topic_name, topic, "Topic name mismatch");
    println!("✅ Topic '{}' created with id={}\n", topic_name, topic_id);

    // Verify all messages were inserted
    println!("Step 2: Checking message insertion...");
    let count_row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await?;
    let message_count: i64 = count_row.get(0);

    assert_eq!(
        message_count, num_messages,
        "Message count mismatch! Expected {}, got {}",
        num_messages, message_count
    );
    println!("✅ All {} messages stored in database\n", message_count);

    // Verify producer ID schema exists (Phase 9)
    println!("=== Phase 9 Schema Verification ===\n");

    println!("Step 3: Checking producer_ids table...");
    let schema_check = ctx
        .db()
        .query(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'kafka'
                AND table_name = 'producer_ids'
            )",
            &[],
        )
        .await?;

    let producer_ids_exists: bool = schema_check[0].get(0);
    assert!(producer_ids_exists, "kafka.producer_ids table should exist");
    println!("✅ kafka.producer_ids table exists\n");

    // Verify sequence tracking schema exists (Phase 9)
    println!("Step 4: Checking producer_sequences table...");
    let sequence_schema_check = ctx
        .db()
        .query(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'kafka'
                AND table_name = 'producer_sequences'
            )",
            &[],
        )
        .await?;

    let producer_sequences_exists: bool = sequence_schema_check[0].get(0);
    assert!(
        producer_sequences_exists,
        "kafka.producer_sequences table should exist"
    );
    println!("✅ kafka.producer_sequences table exists\n");

    // Cleanup
    ctx.cleanup().await?;

    println!("✅ Idempotent producer test PASSED\n");

    Ok(())
}
