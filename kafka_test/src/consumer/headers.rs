//! Record-header roundtrip test (DR-8, DEEP-REVIEW-2026-07).
//!
//! Headers are stored in `kafka.messages.headers` and the fetch path must select
//! them and encode them into the batches it returns (a fetch that re-encodes with
//! empty headers hands a consumer none). The test produces a record with headers and
//! asserts the consumer receives them back over the wire.

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
    println!("  ✅ Produced with headers: partition={partition}, offset={offset}\n");

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
                let got: HashMap<String, Vec<u8>> = msg.headers().map_or_default(|hs| {
                    hs.iter()
                        .map(|h| (h.key.to_string(), h.value.unwrap_or(&[]).to_vec()))
                        .collect()
                });
                println!("  Received {} header(s): {:?}", got.len(), got.keys());

                assert_eq!(
                    got.get("trace-id").map(Vec::as_slice),
                    Some(b"abc123".as_slice()),
                    "trace-id header must roundtrip through storage and fetch"
                );
                assert_eq!(
                    got.get("content-type").map(Vec::as_slice),
                    Some(b"application/json".as_slice()),
                    "content-type header must roundtrip through storage and fetch"
                );
                received = true;
                break;
            }
            Some(Err(e)) => println!("  ⚠️ Consumer error: {e}"),
            None => {}
        }
    }
    assert!(received, "Failed to receive message within timeout");
    println!("  ✅ Headers roundtripped\n");

    println!("✅ Test PASSED\n");
    Ok(())
}

/// A header key holding U+0000 is any other key to Kafka 3.7. JSONB cannot hold the
/// `\u0000` escape, so storing such a key as JSON text failed the whole Produce with
/// UNKNOWN_SERVER_ERROR, which a Java producer does not retry.
///
/// Asserts: a record whose header keys are "a\0b" and "\0" (values with NUL bytes)
/// next to a plain "k" header is produced with error 0, and a fetch returns the record
/// with those three headers, keys and values byte for byte; the stored `record_bytes`
/// counts the keys at their own lengths.
pub async fn test_header_key_with_nul_roundtrips() -> TestResult {
    use crate::setup::TestContext;
    use crate::wire::records::{encode, plain_record};
    use crate::wire::Wire;
    use bytes::Bytes;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{Compression, RecordBatchDecoder};
    println!("=== Test: a header key holding U+0000 roundtrips ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::TestTopicBuilder::new(&ctx, "headers-nul")
        .build()
        .await?
        .name;
    let headers: [(&str, &[u8]); 3] = [("a\0b", b"\0v\0"), ("k", b"plain"), ("\0", b"")];
    let mut record = plain_record(0, None, b"value");
    for (k, v) in headers {
        record.headers.insert(
            StrBytes::from_string(k.to_string()),
            Some(Bytes::copy_from_slice(v)),
        );
    }
    let mut wire = Wire::connect().await?;
    let produced = wire
        .produce(&topic, 0, encode(&[record], Compression::None), None)
        .await?;
    assert_eq!(produced, (0, 0), "the record is stored at offset 0");
    println!("Step 1: produced a record with header keys \"a\\0b\", \"k\" and \"\\0\"");

    let p = wire.fetch(&topic, 0, 0, 0).await?;
    assert_eq!(p.error_code, 0);
    let mut records = p.records.unwrap_or_default();
    let batches = RecordBatchDecoder::decode_all(&mut records)?;
    let fetched: Vec<(String, Vec<u8>)> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .flat_map(|r| {
            r.headers.iter().map(|(k, v)| {
                (
                    k.to_string(),
                    v.as_ref().map_or_else(Vec::new, |v| v.to_vec()),
                )
            })
        })
        .collect();
    let expected: Vec<(String, Vec<u8>)> = headers
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.to_vec()))
        .collect();
    assert_eq!(fetched, expected, "every header, byte for byte");
    println!("Step 2: fetched the record with its three headers unchanged");

    let stored: i32 = ctx
        .db()
        .query_one(
            "SELECT m.record_bytes FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    // Value 5, header keys and values ("a\0b" 3 + 3, "k" 1 + 5, "\0" 1 + 0), 84 per record
    // and 10 per header.
    assert_eq!(stored, 5 + 6 + 6 + 1 + 84 + 3 * 10);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

