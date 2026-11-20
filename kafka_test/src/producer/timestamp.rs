//! Producer record timestamp round-trip (audit BUG-7)
//!
//! The producer's record timestamp must be stored and returned to consumers, not silently replaced
//! by the broker's insert time.

use crate::common::{
    create_base_consumer, create_producer, TestResult, POLL_TIMEOUT, TEST_TIMEOUT,
};
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, Timestamp};
use rdkafka::producer::FutureRecord;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// BUG-7: produce a record with a fixed CreateTime timestamp well in the past and assert the
/// consumed message carries the same epoch-ms value — proving the producer timestamp is persisted
/// and emitted, rather than overwritten by `NOW()` at insert time.
pub async fn test_producer_timestamp_roundtrip() -> TestResult {
    println!("=== Test: Producer record timestamp round-trip (BUG-7) ===\n");

    let topic = format!("ts-roundtrip-{}", uuid::Uuid::new_v4());
    let producer = create_producer()?;

    // 2020-09-13T12:26:40Z — clearly distinct from the broker's "now".
    let expected_ts: i64 = 1_600_000_000_000;

    let (partition, offset) = producer
        .send(
            FutureRecord::to(&topic)
                .payload("ts-payload")
                .key("k")
                .timestamp(expected_ts),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| err)?;
    println!("Produced at partition={partition}, offset={offset}, ts={expected_ts}");

    let consumer = create_base_consumer("ts-probe")?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset))?;
    consumer.assign(&assignment)?;

    let start = std::time::Instant::now();
    while start.elapsed() < TEST_TIMEOUT {
        if let Some(Ok(msg)) = consumer.poll(POLL_TIMEOUT) {
            let ts = match msg.timestamp() {
                Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => Some(ms),
                Timestamp::NotAvailable => None,
            };
            println!("Consumed timestamp = {ts:?} (expected {expected_ts})");
            if ts != Some(expected_ts) {
                return Err(format!(
                    "BUG-7 regression: expected record timestamp {expected_ts}, got {ts:?} (broker insert time?)"
                )
                .into());
            }
            println!("✅ producer timestamp preserved end-to-end");
            return Ok(());
        }
    }
    Err("timed out waiting for the produced message".into())
}

/// Record timestamps are the batch's base timestamp plus a varlong delta: records 30 days
/// before the batch's first and 2^36 ms (about 2 years) after it, deltas past the 32 bits of
/// a varint, are stored and consumed with the timestamps they were sent with. (A 5-byte
/// varint read of the delta stored other timestamps, or refused the batch.)
pub async fn test_producer_timestamps_far_from_the_batch_base_roundtrip() -> TestResult {
    use crate::network::produce_decoding::{batch_with, record, varint, BatchHeader};
    use crate::wire::Wire;
    println!("=== Test: record timestamps far from the batch's base timestamp ===\n");
    let ctx = crate::setup::TestContext::new().await?;
    let topic = ctx.unique_topic("ts-varlong").await;
    let created = crate::wire::requests::create_topics_raw(vec![crate::wire::requests::creatable(
        &topic, 1, 1,
    )])
    .await?;
    assert_eq!(created[0].error_code, 0, "{created:?}");

    let base: i64 = 1_763_600_000_000; // 2025-11-20
    let month: i64 = 30 * 24 * 3600 * 1000;
    let deltas = [0, -month, 1 << 36];
    let section: Vec<u8> = (0..)
        .zip(deltas)
        .flat_map(|(i, delta)| {
            let value = format!("v{i}");
            record(&[
                &[0],
                &varint(delta),
                &varint(i),
                &varint(-1),
                &varint(2),
                value.as_bytes(),
                &varint(0),
            ])
        })
        .collect();
    let header = BatchHeader {
        base_timestamp: base,
        ..BatchHeader::default()
    };
    let batch = batch_with(&section, 3, header);
    let mut wire = Wire::connect().await?;
    assert_eq!(wire.produce(&topic, 0, batch, None).await?, (0, 0));
    let expected: Vec<i64> = deltas.iter().map(|d| base + d).collect();
    println!("Produced 3 records at timestamps {expected:?}");

    let stored: Vec<i64> = ctx
        .db()
        .query(
            "SELECT m.timestamp_ms FROM kafka.messages m JOIN kafka.topics t \
             ON t.id = m.topic_id WHERE t.name = $1 ORDER BY m.partition_offset",
            &[&topic],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(stored, expected, "stored timestamps");
    println!("✅ stored {stored:?}");

    let consumer = create_base_consumer("ts-varlong-probe")?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;
    let mut consumed = Vec::new();
    let start = std::time::Instant::now();
    while consumed.len() < 3 && start.elapsed() < TEST_TIMEOUT {
        if let Some(Ok(msg)) = consumer.poll(POLL_TIMEOUT) {
            match msg.timestamp() {
                Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => consumed.push(ms),
                Timestamp::NotAvailable => return Err("a record without a timestamp".into()),
            }
        }
    }
    assert_eq!(consumed, expected, "consumed timestamps");
    println!("✅ consumed {consumed:?}");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
