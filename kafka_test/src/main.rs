// Automated E2E test for pg_kafka Produce functionality
//
// This test:
// 1. Uses a real Kafka client (rdkafka) to send messages to pg_kafka
// 2. Automatically verifies messages are written correctly to the database
// 3. Returns exit code 0 (success) or 1 (failure) for CI/CD integration

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use postgres::{Client, NoTls};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== pg_kafka Produce Test ===\n");

    // Create a Kafka producer pointing to pg_kafka on port 9092
    println!("Creating Kafka producer...");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .set("client.id", "test-client")
        .create()?;

    println!("✅ Producer created\n");

    // Send a test message
    println!("Sending message to topic 'test-topic'...");
    let topic = "test-topic";
    let key = "test-key";
    let payload = "Hello from rdkafka test client!";

    let (partition, offset) = producer
        .send(
            FutureRecord::to(topic)
                .payload(payload)
                .key(key),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| {
            println!("❌ Failed to deliver message: {}", err);
            err
        })?;

    println!("✅ Message delivered successfully!");
    println!("   Topic: {}", topic);
    println!("   Partition: {}", partition);
    println!("   Offset: {}", offset);

    // === AUTOMATED VERIFICATION ===
    println!("\n=== Automated Database Verification ===");

    // Connect to PostgreSQL
    println!("Connecting to PostgreSQL...");
    let mut client = Client::connect("host=localhost user=postgres dbname=postgres", NoTls)?;
    println!("✅ Connected to database\n");

    // Verify topic was created
    println!("Checking topic creation...");
    let topic_row = client.query_one(
        "SELECT id, name FROM kafka.topics WHERE name = $1",
        &[&topic],
    )?;
    let topic_id: i32 = topic_row.get(0);
    let topic_name: String = topic_row.get(1);

    assert_eq!(topic_name, topic, "Topic name mismatch");
    println!("✅ Topic '{}' created with id={}", topic_name, topic_id);

    // Verify message was inserted
    println!("\nChecking message insertion...");
    let rows = client.query(
        "SELECT topic_id, partition_id, partition_offset, key, value
         FROM kafka.messages
         WHERE topic_id = $1
         ORDER BY partition_offset DESC
         LIMIT 1",
        &[&topic_id],
    )?;

    assert!(!rows.is_empty(), "No messages found in database!");

    let row = &rows[0];
    let db_topic_id: i32 = row.get(0);
    let db_partition: i32 = row.get(1);
    let db_offset: i64 = row.get(2);
    let db_key: Vec<u8> = row.get(3);
    let db_value: Vec<u8> = row.get(4);

    // Verify message data
    assert_eq!(db_topic_id, topic_id, "Topic ID mismatch");
    assert_eq!(db_partition, partition, "Partition mismatch");
    assert_eq!(db_offset, offset, "Offset mismatch");
    assert_eq!(db_key, key.as_bytes(), "Key mismatch");
    assert_eq!(db_value, payload.as_bytes(), "Payload mismatch");

    println!("✅ Message verified in database:");
    println!("   Topic ID: {}", db_topic_id);
    println!("   Partition: {}", db_partition);
    println!("   Offset: {}", db_offset);
    println!("   Key: {}", String::from_utf8_lossy(&db_key));
    println!("   Value: {}", String::from_utf8_lossy(&db_value));

    // Count total messages for this topic
    let count_row = client.query_one(
        "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1",
        &[&topic_id],
    )?;
    let message_count: i64 = count_row.get(0);
    println!("\n✅ Total messages in '{}': {}", topic, message_count);

    println!("\n=== ✅ ALL TESTS PASSED ===");
    println!("Automated verification complete. Exit code 0.");

    Ok(())
}
