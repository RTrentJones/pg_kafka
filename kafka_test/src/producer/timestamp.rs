//! Producer record timestamp round-trip (audit BUG-7)
//!
//! The producer's record timestamp must be stored and returned to consumers, not silently replaced
//! by the broker's insert time.

use crate::common::{create_base_consumer, create_producer, TestResult, POLL_TIMEOUT, TEST_TIMEOUT};
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
    println!(
        "Produced at partition={}, offset={}, ts={}",
        partition, offset, expected_ts
    );

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
            println!("Consumed timestamp = {:?} (expected {})", ts, expected_ts);
            if ts != Some(expected_ts) {
                return Err(format!(
                    "BUG-7 regression: expected record timestamp {}, got {:?} (broker insert time?)",
                    expected_ts, ts
                )
                .into());
            }
            println!("✅ producer timestamp preserved end-to-end");
            return Ok(());
        }
    }
    Err("timed out waiting for the produced message".into())
}
