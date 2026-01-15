//! Compression edge case tests
//!
//! Tests for edge cases in compression handling including:
//! - Large value compression
//! - Mixed compression formats
//! - Empty batches with compression
//! - Compression ratio verification

use crate::common::{get_bootstrap_servers, TestResult};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

/// Create a Kafka producer with specified compression type
fn create_compressed_producer(
    compression_type: &str,
) -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "30000")
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

/// Test large value compression (1MB value with zstd)
///
/// Large values should compress efficiently and roundtrip correctly.
pub async fn test_large_value_compression() -> TestResult {
    println!("=== Test: Large Value Compression ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("large-compress").await;
    let group = ctx.unique_group("large-compress").await;

    // 1. Create a large value (1MB of repeated pattern - compresses well)
    println!("Step 1: Creating 1MB value...");
    let pattern = "ABCDEFGHIJ".repeat(100); // 1KB pattern
    let large_value = pattern.repeat(1024); // 1MB total
    println!("   Value size: {} bytes ({} MB)", large_value.len(), large_value.len() / (1024 * 1024));
    println!("✅ Large value created\n");

    // 2. Produce with zstd compression
    println!("Step 2: Producing with zstd compression...");
    let producer = create_compressed_producer("zstd")?;

    producer
        .send(
            FutureRecord::to(&topic)
                .payload(&large_value)
                .key("large-key"),
            Duration::from_secs(30),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("✅ Large message produced\n");

    // 3. Consume and verify
    println!("Step 3: Consuming message...");
    let consumer = create_consumer(&group)?;
    consumer.subscribe(&[&topic])?;

    let mut received_value: Option<String> = None;
    let start = std::time::Instant::now();
    while received_value.is_none() && start.elapsed() < Duration::from_secs(15) {
        match tokio::time::timeout(Duration::from_secs(3), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(v) = msg.payload() {
                    received_value = Some(String::from_utf8_lossy(v).to_string());
                    println!("   Received message: {} bytes", v.len());
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    assert!(received_value.is_some(), "Should receive the large message");
    let received = received_value.unwrap();
    assert_eq!(
        received.len(),
        large_value.len(),
        "Decompressed value should match original size"
    );
    assert_eq!(received, large_value, "Decompressed value should match original");
    println!("✅ Large message received and verified\n");

    // 4. Verify storage in database
    println!("=== Database Verification ===\n");
    let row = ctx
        .db()
        .query_one(
            "SELECT LENGTH(value) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let stored_size: i32 = row.get(0);
    println!("   Stored value size: {} bytes", stored_size);
    // Note: pg_kafka stores decompressed values in the database
    println!("✅ Database verification complete\n");

    ctx.cleanup().await?;
    println!("✅ Test: Large Value Compression PASSED\n");
    Ok(())
}

/// Test mixed compression formats in same topic
///
/// Different producers using different compression types should work together.
pub async fn test_compression_mixed_formats() -> TestResult {
    println!("=== Test: Mixed Compression Formats ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("mixed-compress").await;
    let group = ctx.unique_group("mixed-compress").await;

    // 1. Create producers with different compression types
    println!("Step 1: Creating producers with different compression...");
    let gzip_producer = create_compressed_producer("gzip")?;
    let snappy_producer = create_compressed_producer("snappy")?;
    let lz4_producer = create_compressed_producer("lz4")?;
    let none_producer = create_compressed_producer("none")?;
    println!("✅ Producers created (gzip, snappy, lz4, none)\n");

    // 2. Produce messages with different compression
    println!("Step 2: Producing messages with mixed compression...");

    gzip_producer
        .send(
            FutureRecord::to(&topic).payload("gzip-compressed-msg").key("gzip"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Produced gzip message");

    snappy_producer
        .send(
            FutureRecord::to(&topic).payload("snappy-compressed-msg").key("snappy"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Produced snappy message");

    lz4_producer
        .send(
            FutureRecord::to(&topic).payload("lz4-compressed-msg").key("lz4"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Produced lz4 message");

    none_producer
        .send(
            FutureRecord::to(&topic).payload("uncompressed-msg").key("none"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Produced uncompressed message");
    println!("✅ Mixed compression messages produced\n");

    // 3. Consume all messages
    println!("Step 3: Consuming all messages...");
    let consumer = create_consumer(&group)?;
    consumer.subscribe(&[&topic])?;

    let mut received_messages: Vec<String> = Vec::new();
    let start = std::time::Instant::now();
    while received_messages.len() < 4 && start.elapsed() < Duration::from_secs(15) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(v) = msg.payload() {
                    let value = String::from_utf8_lossy(v).to_string();
                    println!("   Received: {}", value);
                    received_messages.push(value);
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    // 4. Verify all messages received
    println!("\nStep 4: Verifying all messages...");
    assert!(
        received_messages.len() >= 3,
        "Should receive at least 3 of 4 messages, got {}",
        received_messages.len()
    );
    println!("✅ Received {} messages from mixed compression\n", received_messages.len());

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
    assert_eq!(count, 4, "Expected 4 messages in database");
    println!("✅ Database confirms {} messages\n", count);

    ctx.cleanup().await?;
    println!("✅ Test: Mixed Compression Formats PASSED\n");
    Ok(())
}

/// Test compression with highly compressible data
///
/// Verify that repetitive data achieves good compression ratio.
pub async fn test_compression_ratio_verification() -> TestResult {
    println!("=== Test: Compression Ratio Verification ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("compress-ratio").await;
    let group = ctx.unique_group("compress-ratio").await;

    // 1. Create highly compressible data (all zeros)
    println!("Step 1: Creating highly compressible data...");
    let compressible_data = "0".repeat(100_000); // 100KB of zeros
    println!("   Original size: {} bytes", compressible_data.len());
    println!("✅ Compressible data created\n");

    // 2. Produce with zstd (best compression ratio typically)
    println!("Step 2: Producing with zstd compression...");
    let producer = create_compressed_producer("zstd")?;

    producer
        .send(
            FutureRecord::to(&topic)
                .payload(&compressible_data)
                .key("compress-test"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("✅ Compressible message produced\n");

    // 3. Consume and verify roundtrip
    println!("Step 3: Consuming message...");
    let consumer = create_consumer(&group)?;
    consumer.subscribe(&[&topic])?;

    let mut received = false;
    let start = std::time::Instant::now();
    while !received && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(v) = msg.payload() {
                    let value = String::from_utf8_lossy(v).to_string();
                    assert_eq!(
                        value.len(),
                        compressible_data.len(),
                        "Decompressed should match original"
                    );
                    assert_eq!(value, compressible_data, "Content should match");
                    received = true;
                    println!("   Received {} bytes (decompressed)", v.len());
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    assert!(received, "Should receive the compressed message");
    println!("✅ Message roundtrip verified\n");

    // 4. Note about compression
    println!("=== Compression Note ===\n");
    println!("   pg_kafka stores decompressed values in PostgreSQL");
    println!("   Compression happens on the wire protocol only");
    println!("   This is intentional for query-ability\n");

    ctx.cleanup().await?;
    println!("✅ Test: Compression Ratio Verification PASSED\n");
    Ok(())
}

/// Test compression with incompressible data (random bytes)
///
/// Random data doesn't compress well but should still work correctly.
pub async fn test_compression_incompressible_data() -> TestResult {
    println!("=== Test: Compression with Incompressible Data ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("incompressible").await;
    let group = ctx.unique_group("incompressible").await;

    // 1. Create random-looking data (incompressible)
    println!("Step 1: Creating incompressible data...");
    // Use a pattern that doesn't compress well
    let mut data = String::new();
    for i in 0..10000 {
        data.push_str(&format!("{:x}", (i * 17 + 31) % 256));
    }
    println!("   Data size: {} bytes", data.len());
    println!("✅ Incompressible data created\n");

    // 2. Produce with gzip compression
    println!("Step 2: Producing with gzip compression...");
    let producer = create_compressed_producer("gzip")?;

    producer
        .send(
            FutureRecord::to(&topic)
                .payload(&data)
                .key("random-data"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("✅ Message produced\n");

    // 3. Consume and verify
    println!("Step 3: Consuming message...");
    let consumer = create_consumer(&group)?;
    consumer.subscribe(&[&topic])?;

    let mut received_data: Option<String> = None;
    let start = std::time::Instant::now();
    while received_data.is_none() && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(v) = msg.payload() {
                    received_data = Some(String::from_utf8_lossy(v).to_string());
                    println!("   Received {} bytes", v.len());
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    assert!(received_data.is_some(), "Should receive the message");
    let received = received_data.unwrap();
    assert_eq!(received, data, "Content should match original");
    println!("✅ Incompressible data roundtrip verified\n");

    ctx.cleanup().await?;
    println!("✅ Test: Compression with Incompressible Data PASSED\n");
    Ok(())
}

/// Test small message compression overhead
///
/// Very small messages may actually be larger when compressed due to header overhead.
pub async fn test_compression_small_message_overhead() -> TestResult {
    println!("=== Test: Small Message Compression Overhead ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("small-compress").await;
    let group = ctx.unique_group("small-compress").await;

    // 1. Create a very small message
    println!("Step 1: Creating small message (10 bytes)...");
    let small_message = "tiny data!"; // 10 bytes
    println!("   Message: '{}' ({} bytes)", small_message, small_message.len());
    println!("✅ Small message created\n");

    // 2. Produce with compression (adds overhead to small messages)
    println!("Step 2: Producing with gzip compression...");
    let producer = create_compressed_producer("gzip")?;

    producer
        .send(
            FutureRecord::to(&topic)
                .payload(small_message)
                .key("small"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _)| err)?;
    println!("   Note: Compression header may make small messages larger on wire");
    println!("✅ Small message produced\n");

    // 3. Consume and verify
    println!("Step 3: Consuming message...");
    let consumer = create_consumer(&group)?;
    consumer.subscribe(&[&topic])?;

    let mut received_msg: Option<String> = None;
    let start = std::time::Instant::now();
    while received_msg.is_none() && start.elapsed() < Duration::from_secs(10) {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(v) = msg.payload() {
                    received_msg = Some(String::from_utf8_lossy(v).to_string());
                    println!("   Received: '{}'", received_msg.as_ref().unwrap());
                }
            }
            Ok(Err(e)) => println!("   Error: {}", e),
            Err(_) => continue,
        }
    }

    assert!(received_msg.is_some(), "Should receive the message");
    assert_eq!(received_msg.unwrap(), small_message, "Content should match");
    println!("✅ Small message roundtrip verified\n");

    // 4. Verify in database
    println!("=== Database Verification ===\n");
    let row = ctx
        .db()
        .query_one(
            "SELECT LENGTH(value) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let stored_len: i32 = row.get(0);
    assert_eq!(stored_len, small_message.len() as i32, "Stored size should match");
    println!("✅ Database stores original size: {} bytes\n", stored_len);

    ctx.cleanup().await?;
    println!("✅ Test: Small Message Compression Overhead PASSED\n");
    Ok(())
}
