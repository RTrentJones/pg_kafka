//! Record-header roundtrip test (DR-8, DEEP-REVIEW-2026-07).
//!
//! Fail-before/pass-after: before DR-8, headers were written to
//! `kafka.messages.headers` on every produce but the fetch path never selected
//! the column and re-encoded batches with empty headers — so a consumer always
//! saw none. This test produces a record with headers and asserts the consumer
//! receives them back over the wire.

use crate::common::{
    create_base_consumer, create_producer, TestResult, POLL_TIMEOUT, TEST_TIMEOUT,
};
use rdkafka::consumer::Consumer;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

pub async fn test_consumer_receives_headers() -> TestResult {
    println!("=== Test: Consumer Receives Record Headers ===\n");

    let topic = format!("headers-roundtrip-{}", Uuid::new_v4());

    // 1. Produce a record carrying headers.
    println!("Step 1: Producing message with headers...");
    let producer = create_producer()?;
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "trace-id",
            value: Some(b"abc123".as_slice()),
        })
        .insert(Header {
            key: "content-type",
            value: Some(b"application/json".as_slice()),
        });

    let (partition, offset) = producer
        .send(
            FutureRecord::to(&topic)
                .payload("payload-with-headers")
                .key("hk")
                .headers(headers),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| err)?;
    println!(
        "  ✅ Produced with headers: partition={}, offset={}\n",
        partition, offset
    );

    // 2. Consume it back and assert the headers survived the roundtrip.
    println!("Step 2: Consuming and checking headers...");
    let consumer = create_base_consumer("headers-consumer")?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset))?;
    consumer.assign(&assignment)?;

    let start = std::time::Instant::now();
    let mut received = false;
    while start.elapsed() < TEST_TIMEOUT {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                let got: HashMap<String, Vec<u8>> = msg
                    .headers()
                    .map(|hs| {
                        hs.iter()
                            .map(|h| (h.key.to_string(), h.value.unwrap_or(&[]).to_vec()))
                            .collect()
                    })
                    .unwrap_or_default();
                println!("  Received {} header(s): {:?}", got.len(), got.keys());

                assert_eq!(
                    got.get("trace-id").map(|v| v.as_slice()),
                    Some(b"abc123".as_slice()),
                    "trace-id header must roundtrip through storage and fetch"
                );
                assert_eq!(
                    got.get("content-type").map(|v| v.as_slice()),
                    Some(b"application/json".as_slice()),
                    "content-type header must roundtrip through storage and fetch"
                );
                received = true;
                break;
            }
            Some(Err(e)) => println!("  ⚠️ Consumer error: {}", e),
            None => continue,
        }
    }
    assert!(received, "Failed to receive message within timeout");
    println!("  ✅ Headers roundtripped\n");

    println!("✅ Test PASSED\n");
    Ok(())
}
