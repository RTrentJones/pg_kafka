//! Responses that must match Apache Kafka 3.7 field for field, checked on the wire
//! because rdkafka does not expose the fields involved.

use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use crate::w3_txn::wire::{batch, now_ms, Wire};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

const ERROR_NONE: i16 = 0;

/// Produce v8 of `values` to partition 0: `(error_code, base_offset, log_start_offset)`.
async fn produce_v8(
    wire: &mut Wire,
    topic: &str,
    values: &[&str],
) -> crate::w3_txn::wire::Result<(i16, i64, i64)> {
    let mut p = PartitionProduceData::default();
    p.index = 0;
    p.records = Some(batch(values, -1, -1, -1, false, None));
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    t.partition_data = vec![p];
    let mut req = ProduceRequest::default();
    req.acks = -1;
    req.timeout_ms = 10_000;
    req.topic_data = vec![t];
    let resp: ProduceResponse = wire.call(0, 8, &req).await?;
    let p = &resp.responses[0].partition_responses[0];
    Ok((p.error_code, p.base_offset, p.log_start_offset))
}

/// A successful ProduceResponse carries the partition's log start offset (Kafka's
/// value), not -1, and follows it when DeleteRecords advances it.
pub async fn test_produce_response_reports_log_start_offset() -> TestResult {
    println!("=== Test: ProduceResponse reports the log start offset ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-log-start")
        .build()
        .await?
        .name;
    let mut wire = Wire::connect().await?;

    assert_eq!(
        produce_v8(&mut wire, &topic, &["a", "b", "c"]).await?,
        (ERROR_NONE, 0, 0)
    );
    assert_eq!(wire.delete_records(&topic, 0, 2).await?, (ERROR_NONE, 2));
    assert_eq!(
        produce_v8(&mut wire, &topic, &["d"]).await?,
        (ERROR_NONE, 3, 2)
    );
    println!("✅ log_start_offset 0, then 2 after DeleteRecords\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// ListOffsets for a timestamp after every record, and MAX_TIMESTAMP on an empty
/// partition, answer NONE with offset -1 and timestamp -1, as Kafka does.
pub async fn test_list_offsets_without_a_matching_record_is_offset_minus_one() -> TestResult {
    println!("=== Test: ListOffsets with no matching record ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-list-offsets")
        .with_partitions(2)
        .build()
        .await?
        .name;
    let mut wire = Wire::connect().await?;
    let now = now_ms();
    let (code, _) = wire
        .produce(
            &topic,
            0,
            batch(&["a", "b"], -1, -1, -1, false, Some(&[now, now])),
            None,
        )
        .await?;
    assert_eq!(code, ERROR_NONE);

    let after_last = wire.list_offsets(&topic, 0, now + 3_600_000, 0).await?;
    assert_eq!(
        after_last,
        (ERROR_NONE, -1, -1),
        "timestamp after the last record"
    );
    let by_timestamp = wire.list_offsets(&topic, 0, now, 0).await?;
    assert_eq!(
        by_timestamp,
        (ERROR_NONE, 0, now),
        "a matching record still resolves"
    );
    let max_on_empty = wire.list_offsets(&topic, 1, -3, 0).await?;
    assert_eq!(
        max_on_empty,
        (ERROR_NONE, -1, -1),
        "MAX_TIMESTAMP of an empty partition"
    );
    println!("✅ NONE / -1 / -1 when no record matches\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
