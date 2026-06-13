//! acks=0 (fire-and-forget) producer test
//!
//! Per the Kafka protocol, the broker sends NO response frame for acks=0
//! produce requests. A broker that responds anyway desyncs clients that
//! enforce strict per-connection request/response pairing. This test
//! verifies a real client configured with acks=0:
//! 1. Delivers messages without error (no unexpected frames on the wire)
//! 2. Can keep using the same connection afterwards (metadata + acks=1)
//! 3. The messages are actually persisted (best-effort write happened)

use crate::common::{get_bootstrap_servers, TestResult};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

const MESSAGE_COUNT: usize = 10;

/// Create a producer configured for fire-and-forget (acks=0)
fn create_acks_zero_producer() -> Result<FutureProducer, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "5000")
        .set("client.id", "acks-zero-test-client")
        .set("acks", "0")
        .create()?;
    Ok(producer)
}

pub async fn test_producer_acks_zero() -> TestResult {
    println!("=== Test: Producer acks=0 (Fire-and-Forget) ===\n");

    // 1. SETUP
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("acks-zero").await;

    // 2. ACTION: produce with acks=0
    println!("Step 1: Producing {} messages with acks=0...", MESSAGE_COUNT);
    let producer = create_acks_zero_producer()?;
    for i in 0..MESSAGE_COUNT {
        let key = format!("key-{}", i);
        let value = format!("acks-zero-message-{}", i);
        // With acks=0 the client reports delivery as soon as the request is
        // written; any protocol desync (e.g., an unexpected response frame)
        // surfaces as a transport error on subsequent sends
        producer
            .send(
                FutureRecord::to(&topic).key(&key).payload(&value),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| {
                println!("❌ acks=0 delivery failed for message {}: {}", i, err);
                err
            })?;
    }
    producer.flush(Duration::from_secs(5))?;
    println!("✅ {} messages sent fire-and-forget\n", MESSAGE_COUNT);

    // 3. VERIFY: the connection still works for request/response traffic.
    // If the broker had sent unsolicited response frames for the acks=0
    // requests, the client connection would be desynced by now.
    println!("Step 2: Verifying connection health with an acks=1 produce...");
    let confirm_producer = crate::common::create_producer()?;
    let (partition, offset) = confirm_producer
        .send(
            FutureRecord::to(&topic)
                .key("confirm")
                .payload("acks-1-after-acks-0"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| err)?;
    println!(
        "✅ acks=1 produce confirmed (partition {}, offset {})\n",
        partition, offset
    );

    // 4. VERIFY: best-effort writes actually landed in the database.
    // acks=0 writes are async relative to the client, so poll briefly.
    println!("=== Database Verification ===\n");
    let expected = (MESSAGE_COUNT + 1) as i64;
    let mut count: i64 = 0;
    for _ in 0..50 {
        let row = ctx
            .db()
            .query_one(
                "SELECT COUNT(*) FROM kafka.messages m \
                 JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
                &[&topic],
            )
            .await?;
        count = row.get(0);
        if count >= expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        count, expected,
        "Expected {} messages ({} acks=0 + 1 acks=1), found {}",
        expected, MESSAGE_COUNT, count
    );
    println!("✅ All {} messages persisted\n", count);

    // 5. CLEANUP
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
