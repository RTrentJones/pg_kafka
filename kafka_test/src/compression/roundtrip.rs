//! Compression roundtrip tests
//!
//! Tests that verify complete compression/decompression cycle:
//! - Producer sends compressed messages
//! - Server decompresses and stores
//! - Consumer receives decompressed messages

use crate::common::{create_db_client, get_bootstrap_servers, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use uuid::Uuid;

/// Create a Kafka producer with specified compression type
fn create_compressed_producer(
    compression_type: &str,
) -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "5000")
        .set("compression.type", compression_type)
        .create()?;

    Ok(producer)
}

/// Create a consumer for verification
fn create_consumer(group_id: &str) -> Result<StreamConsumer, Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .create()?;

    Ok(consumer)
}

/// Test complete roundtrip: compressed produce -> database storage -> consume
///
/// This test verifies:
/// 1. Compressed messages are accepted by the broker
/// 2. Messages are stored correctly in PostgreSQL (decompressed)
/// 3. Messages can be consumed back (re-encoded, possibly compressed)
pub async fn test_compression_roundtrip() -> TestResult {
    println!("=== Test: Compression Roundtrip ===\n");

    let topic = format!("compress-roundtrip-{}", Uuid::new_v4());
    let group_id = format!("compress-roundtrip-group-{}", Uuid::new_v4());

    // Test message content
    let test_value = "This is a test message for compression roundtrip verification";
    let test_key = "roundtrip-key";

    // Step 1: Produce with gzip compression
    println!("Step 1: Producing gzip-compressed message...");
    let producer = create_compressed_producer("gzip")?;
    producer
        .send(
            FutureRecord::to(&topic).payload(test_value).key(test_key),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Message produced with gzip compression");

    // Step 2: Verify storage in PostgreSQL
    println!("\nStep 2: Verifying message in PostgreSQL...");
    let db_client = create_db_client().await?;

    // Wait for message to be stored
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Query the message from the database
    let rows = db_client
        .query(
            "SELECT convert_from(value, 'UTF8') as value, convert_from(key, 'UTF8') as key
             FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;

    assert!(!rows.is_empty(), "Message should be stored in database");
    let stored_value: String = rows[0].get("value");
    let stored_key: String = rows[0].get("key");
    assert_eq!(stored_value, test_value, "Value should match");
    assert_eq!(stored_key, test_key, "Key should match");
    println!("   Database contains decompressed message: {}", stored_value);

    // Step 3: Consume the message
    println!("\nStep 3: Consuming message via Kafka protocol...");
    let consumer = create_consumer(&group_id)?;
    consumer.subscribe(&[&topic])?;

    let mut received_value = None;
    let mut received_key = None;
    let start = std::time::Instant::now();

    while received_value.is_none() && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                received_value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                received_key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                if received_value.is_some() {
                    println!("   Received message: {:?}", received_value);
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    // Step 4: Verify roundtrip
    println!("\nStep 4: Verifying roundtrip...");
    assert!(received_value.is_some(), "Should receive the message");
    assert_eq!(
        received_value.as_deref(),
        Some(test_value),
        "Consumed value should match original"
    );
    assert_eq!(
        received_key.as_deref(),
        Some(test_key),
        "Consumed key should match original"
    );
    println!("   Roundtrip verification successful!");

    println!("\n=== Compression Roundtrip PASSED ===\n");
    Ok(())
}
