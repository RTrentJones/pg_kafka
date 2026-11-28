//! Consumer error path tests
//!
//! Tests for consumer error conditions including unknown topics,
//! invalid partitions, and out-of-range offsets.

use crate::common::{create_base_consumer, get_bootstrap_servers, TestResult, POLL_TIMEOUT};
use crate::error_codes;
use crate::fixtures::{generate_messages, TestTopicBuilder};
use crate::setup::TestContext;
use crate::wire::requests::fetch_request;
use crate::wire::Wire;
use kafka_protocol::messages::FetchResponse;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Test fetching from an unknown topic
///
/// Should handle gracefully (either return empty or appropriate error).
pub async fn test_fetch_unknown_topic() -> TestResult {
    println!("=== Test: Fetch from Unknown Topic ===\n");

    let ctx = TestContext::new().await?;
    let unknown_topic = ctx.unique_topic("nonexistent").await;

    println!("Attempting to fetch from non-existent topic '{unknown_topic}'");

    let group_id = ctx.unique_group("fetch-unknown").await;
    let consumer = create_base_consumer(&group_id)?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&unknown_topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    // Poll for messages - should timeout or return error
    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();
    let mut got_error = false;
    let mut got_message = false;

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                println!("   Unexpected message received: {:?}", msg.offset());
                got_message = true;
            }
            Some(Err(e)) => {
                println!("   Expected error: {e}");
                got_error = true;
                break;
            }
            None => {
                // No message, continue polling
            }
        }
    }

    if !got_error {
        println!("   No error received (topic may have been auto-created or fetch returned empty)");
    }

    ctx.cleanup().await?;
    // QA-3: whether pg_kafka errors or auto-creates the (empty) topic is implementation-defined, but
    // either way there is no data — a real message must never be delivered from a topic that never
    // received one.
    if got_message {
        return Err("fetch from a non-existent topic delivered a message".into());
    }
    Ok(())
}

/// Test fetching from an invalid partition
///
/// Should return UNKNOWN_TOPIC_OR_PARTITION error.
pub async fn test_fetch_invalid_partition() -> TestResult {
    println!("=== Test: Fetch from Invalid Partition ===\n");

    let ctx = TestContext::new().await?;

    // Create topic with 1 partition
    let topic = TestTopicBuilder::new(&ctx, "invalid-fetch-partition")
        .with_partitions(1)
        .build()
        .await?;

    // Produce a message so topic definitely exists
    let messages = generate_messages(1, "test");
    topic.produce(&messages).await?;

    println!("Created topic '{}' with 1 partition", topic.name);

    let group_id = ctx.unique_group("fetch-invalid").await;
    let consumer = create_base_consumer(&group_id)?;

    // Try to fetch from partition 10 (doesn't exist)
    let invalid_partition = 10;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, invalid_partition, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    println!("Attempting to fetch from partition {invalid_partition}...");

    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();
    let mut got_message = false;

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(_)) => {
                println!("   Unexpected: received message from invalid partition");
                got_message = true;
            }
            Some(Err(e)) => {
                println!("   Expected error: {e}");
                break;
            }
            None => {
                // Continue polling
            }
        }
    }

    ctx.cleanup().await?;
    // QA-3: partition 10 of a 1-partition topic does not exist, so no record can come from it —
    // an error or an empty fetch are both acceptable, a delivered message is not.
    if got_message {
        return Err("fetch from a non-existent partition delivered a message".into());
    }
    Ok(())
}

/// A fetch outside the log is answered OFFSET_OUT_OF_RANGE with Kafka 3.7's entry
/// (offsets -1, null aborted transactions, an empty but non-null records field), so an
/// rdkafka consumer applies `auto.offset.reset`: from past the high watermark and from
/// before a log start that DeleteRecords moved, `earliest` brings it to the first record
/// still in the log, and it reads every record from there. A null records field makes
/// librdkafka drop the whole response ("invalid MessageSetSize -1") without ever reading
/// the error code, so the consumer refetches the same offset forever and gets nothing.
pub async fn test_fetch_offset_out_of_range() -> TestResult {
    println!("=== Test: Fetch from Out-of-Range Offset ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "out-of-range").build().await?;
    let messages = generate_messages(5, "test");
    topic.produce(&messages).await?;
    println!(
        "Step 1: topic '{}' with 5 messages (offsets 0-4)",
        topic.name
    );

    let mut wire = Wire::connect().await?;
    println!("\nStep 2: raw Fetch v4 and v11 from offset 100 (past the high watermark 5)");
    check_out_of_range_entry(&mut wire, &topic.name, 100).await?;

    println!("\nStep 3: an rdkafka consumer assigned offset 100 resets to the earliest record");
    consume_after_reset(&ctx, &topic.name, 100, 0..5).await?;

    println!("\nStep 4: DeleteRecords before offset 2, then fetch from offset 0");
    let (error, low_watermark) = wire.delete_records(&topic.name, 0, 2).await?;
    if (error, low_watermark) != (0, 2) {
        return Err(
            format!("DeleteRecords answered error {error}, low watermark {low_watermark}").into(),
        );
    }
    check_out_of_range_entry(&mut wire, &topic.name, 0).await?;
    consume_after_reset(&ctx, &topic.name, 0, 2..5).await?;

    ctx.cleanup().await?;
    println!("\n✅ Fetch out-of-range test PASSED\n");
    Ok(())
}

/// Fetch partition 0 of `topic` from `offset`, outside its log, at v4 and v11: the entry
/// must be Kafka 3.7's OFFSET_OUT_OF_RANGE entry with a non-null (length 0) records field
/// (the crate decodes a length of -1 as None).
async fn check_out_of_range_entry(wire: &mut Wire, topic: &str, offset: i64) -> TestResult {
    let v4: FetchResponse = wire.call(4, &fetch_request(topic, 0, offset, 0)).await?;
    let v11 = wire.fetch(topic, 0, offset, 0).await?;
    for (version, p) in [(4, &v4.responses[0].partitions[0]), (11, &v11)] {
        let records = p.records.as_ref().map(bytes::Bytes::len);
        println!(
            "   v{version}: error {}, hwm {}, lso {}, log start {}, aborted {:?}, records {:?}",
            p.error_code,
            p.high_watermark,
            p.last_stable_offset,
            p.log_start_offset,
            p.aborted_transactions.as_ref().map(Vec::len),
            records
        );
        if p.error_code != error_codes::OFFSET_OUT_OF_RANGE {
            return Err(format!(
                "v{version}: expected OFFSET_OUT_OF_RANGE, got {}",
                p.error_code
            )
            .into());
        }
        if records != Some(0) {
            return Err(format!(
                "v{version}: an OFFSET_OUT_OF_RANGE entry must carry an empty, non-null records \
                 field (length 0), got {records:?} (None is the null length -1)"
            )
            .into());
        }
        // log_start_offset is on the wire from v5.
        let log_start = if version >= 5 { p.log_start_offset } else { -1 };
        if (p.high_watermark, p.last_stable_offset, log_start) != (-1, -1, -1)
            || p.aborted_transactions.is_some()
        {
            return Err(format!(
                "v{version}: Kafka 3.7 answers OFFSET_OUT_OF_RANGE with offsets -1 and null \
                 aborted transactions, got {p:?}"
            )
            .into());
        }
    }
    Ok(())
}

/// Assign an `auto.offset.reset=earliest` consumer partition 0 of `topic` at `offset`
/// (outside the log) and require exactly the records at `expected` offsets, in order,
/// within 20 s.
async fn consume_after_reset(
    ctx: &TestContext,
    topic: &str,
    offset: i64,
    expected: std::ops::Range<i64>,
) -> TestResult {
    let group_id = ctx.unique_group("out-of-range").await;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, 0, rdkafka::Offset::Offset(offset))?;
    consumer.assign(&assignment)?;

    let want: Vec<i64> = expected.collect();
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while got.len() < want.len() && std::time::Instant::now() < deadline {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => got.push(msg.offset()),
            Some(Err(e)) => println!("   consumer error: {e}"),
            None => {}
        }
    }
    println!("   assigned offset {offset}: received offsets {got:?}");
    if got != want {
        return Err(format!(
            "a consumer assigned out-of-range offset {offset} with auto.offset.reset=earliest \
             must reset and receive offsets {want:?}, received {got:?}"
        )
        .into());
    }
    Ok(())
}

/// Test consuming from an empty topic/partition
///
/// Should return no messages (not an error).
pub async fn test_consume_empty_topic() -> TestResult {
    println!("=== Test: Consume from Empty Topic ===\n");

    let ctx = TestContext::new().await?;

    let topic = TestTopicBuilder::new(&ctx, "empty-consume").build().await?;

    println!("Created empty topic '{}'", topic.name);

    let group_id = ctx.unique_group("empty-consume").await;
    let consumer = create_base_consumer(&group_id)?;

    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(&topic.name, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&assignment)?;

    println!("Polling empty topic...");

    let timeout = Duration::from_secs(2);
    let start = std::time::Instant::now();
    let mut message_count = 0;

    while start.elapsed() < timeout {
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(_)) => {
                message_count += 1;
            }
            Some(Err(e)) => {
                println!("   Error: {e}");
            }
            None => {
                // Expected - no messages
            }
        }
    }

    assert_eq!(
        message_count, 0,
        "Should not receive any messages from empty topic"
    );
    println!("✅ No messages received (correct for empty topic)");

    ctx.cleanup().await?;
    println!("\n✅ Consume from empty topic test PASSED\n");
    Ok(())
}
