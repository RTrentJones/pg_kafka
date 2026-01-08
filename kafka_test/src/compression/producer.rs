//! Compressed producer tests
//!
//! Tests that verify pg_kafka can receive compressed messages from producers.
//! The kafka-protocol crate handles decompression automatically.

use crate::common::{get_bootstrap_servers, TestResult};
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

/// Test that a producer can send gzip-compressed messages
pub async fn test_compressed_producer_gzip() -> TestResult {
    println!("=== Test: Compressed Producer (gzip) ===\n");

    let topic = format!("compress-gzip-{}", Uuid::new_v4());
    let group_id = format!("compress-gzip-group-{}", Uuid::new_v4());

    // Create gzip-compressed producer
    println!("Step 1: Creating gzip-compressed producer...");
    let producer = create_compressed_producer("gzip")?;

    // Produce messages
    println!("\nStep 2: Producing 3 compressed messages...");
    for i in 1..=3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("gzip-message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
        println!("   Produced gzip-message-{}", i);
    }

    // Consume and verify
    println!("\nStep 3: Consuming messages...");
    let consumer = create_consumer(&group_id)?;
    consumer.subscribe(&[&topic])?;

    let mut received = 0;
    let start = std::time::Instant::now();
    while received < 3 && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = &value {
                    if v.starts_with("gzip-message-") {
                        received += 1;
                        println!("   Received: {}", v);
                    }
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    println!("\nStep 4: Verifying results...");
    assert!(
        received >= 2,
        "Should receive at least 2 of 3 messages, got {}",
        received
    );
    println!("   Received {} messages", received);

    println!("\n=== Compressed Producer (gzip) PASSED ===\n");
    Ok(())
}

/// Test that a producer can send snappy-compressed messages
pub async fn test_compressed_producer_snappy() -> TestResult {
    println!("=== Test: Compressed Producer (snappy) ===\n");

    let topic = format!("compress-snappy-{}", Uuid::new_v4());
    let group_id = format!("compress-snappy-group-{}", Uuid::new_v4());

    // Create snappy-compressed producer
    println!("Step 1: Creating snappy-compressed producer...");
    let producer = create_compressed_producer("snappy")?;

    // Produce messages
    println!("\nStep 2: Producing 3 compressed messages...");
    for i in 1..=3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("snappy-message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
        println!("   Produced snappy-message-{}", i);
    }

    // Consume and verify
    println!("\nStep 3: Consuming messages...");
    let consumer = create_consumer(&group_id)?;
    consumer.subscribe(&[&topic])?;

    let mut received = 0;
    let start = std::time::Instant::now();
    while received < 3 && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = &value {
                    if v.starts_with("snappy-message-") {
                        received += 1;
                        println!("   Received: {}", v);
                    }
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    println!("\nStep 4: Verifying results...");
    assert!(
        received >= 2,
        "Should receive at least 2 of 3 messages, got {}",
        received
    );
    println!("   Received {} messages", received);

    println!("\n=== Compressed Producer (snappy) PASSED ===\n");
    Ok(())
}

/// Test that a producer can send lz4-compressed messages
pub async fn test_compressed_producer_lz4() -> TestResult {
    println!("=== Test: Compressed Producer (lz4) ===\n");

    let topic = format!("compress-lz4-{}", Uuid::new_v4());
    let group_id = format!("compress-lz4-group-{}", Uuid::new_v4());

    // Create lz4-compressed producer
    println!("Step 1: Creating lz4-compressed producer...");
    let producer = create_compressed_producer("lz4")?;

    // Produce messages
    println!("\nStep 2: Producing 3 compressed messages...");
    for i in 1..=3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("lz4-message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
        println!("   Produced lz4-message-{}", i);
    }

    // Consume and verify
    println!("\nStep 3: Consuming messages...");
    let consumer = create_consumer(&group_id)?;
    consumer.subscribe(&[&topic])?;

    let mut received = 0;
    let start = std::time::Instant::now();
    while received < 3 && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = &value {
                    if v.starts_with("lz4-message-") {
                        received += 1;
                        println!("   Received: {}", v);
                    }
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    println!("\nStep 4: Verifying results...");
    assert!(
        received >= 2,
        "Should receive at least 2 of 3 messages, got {}",
        received
    );
    println!("   Received {} messages", received);

    println!("\n=== Compressed Producer (lz4) PASSED ===\n");
    Ok(())
}

/// Test that a producer can send zstd-compressed messages
pub async fn test_compressed_producer_zstd() -> TestResult {
    println!("=== Test: Compressed Producer (zstd) ===\n");

    let topic = format!("compress-zstd-{}", Uuid::new_v4());
    let group_id = format!("compress-zstd-group-{}", Uuid::new_v4());

    // Create zstd-compressed producer
    println!("Step 1: Creating zstd-compressed producer...");
    let producer = create_compressed_producer("zstd")?;

    // Produce messages
    println!("\nStep 2: Producing 3 compressed messages...");
    for i in 1..=3 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&format!("zstd-message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| err)?;
        println!("   Produced zstd-message-{}", i);
    }

    // Consume and verify
    println!("\nStep 3: Consuming messages...");
    let consumer = create_consumer(&group_id)?;
    consumer.subscribe(&[&topic])?;

    let mut received = 0;
    let start = std::time::Instant::now();
    while received < 3 && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = &value {
                    if v.starts_with("zstd-message-") {
                        received += 1;
                        println!("   Received: {}", v);
                    }
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    println!("\nStep 4: Verifying results...");
    assert!(
        received >= 2,
        "Should receive at least 2 of 3 messages, got {}",
        received
    );
    println!("   Received {} messages", received);

    println!("\n=== Compressed Producer (zstd) PASSED ===\n");
    Ok(())
}
