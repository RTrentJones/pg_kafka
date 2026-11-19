//! Responses that must match Apache Kafka 3.7 field for field, checked on the wire
//! because rdkafka does not expose the fields involved.

use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use crate::w3_txn::wire::{batch, now_ms, Wire};
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_commit_response::OffsetCommitResponse;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::{GroupId, ProducerId, TopicName};
use kafka_protocol::protocol::StrBytes;

const ERROR_NONE: i16 = 0;
const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

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

/// OffsetCommit to a partition the topic does not have answers
/// UNKNOWN_TOPIC_OR_PARTITION for that partition and stores nothing for it.
pub async fn test_offset_commit_to_a_missing_partition_is_refused() -> TestResult {
    println!("=== Test: OffsetCommit to a missing partition ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-commit")
        .build()
        .await?
        .name;
    let group = ctx.unique_group("parity-commit").await;
    let mut wire = Wire::connect().await?;

    let partitions = [0, 5]
        .into_iter()
        .map(|partition_index| {
            let mut p = OffsetCommitRequestPartition::default();
            p.partition_index = partition_index;
            p.committed_offset = 1;
            p
        })
        .collect();
    let mut t = OffsetCommitRequestTopic::default();
    t.name = TopicName(StrBytes::from_string(topic.clone()));
    t.partitions = partitions;
    let mut req = OffsetCommitRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.clone()));
    req.generation_id_or_member_epoch = -1;
    req.topics = vec![t];
    let resp: OffsetCommitResponse = wire.call(8, 8, &req).await?;
    let codes: Vec<(i32, i16)> = resp.topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.error_code))
        .collect();
    assert_eq!(
        codes,
        vec![(0, ERROR_NONE), (5, ERROR_UNKNOWN_TOPIC_OR_PARTITION)]
    );

    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.consumer_offsets WHERE group_id = $1 AND partition_id = 5",
            &[&group],
        )
        .await?
        .get(0);
    assert_eq!(stored, 0, "nothing is stored for the missing partition");
    println!("✅ partition 5 refused, partition 0 committed\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// InitProducerId v4 without a transactional id: `(error_code, producer_id, epoch)`.
async fn init_idempotent(
    wire: &mut Wire,
    producer_id: i64,
    epoch: i16,
) -> crate::w3_txn::wire::Result<(i16, i64, i16)> {
    let mut req = InitProducerIdRequest::default();
    req.transactional_id = None;
    req.transaction_timeout_ms = 60_000;
    req.producer_id = ProducerId(producer_id);
    req.producer_epoch = epoch;
    let resp: InitProducerIdResponse = wire.call(22, 4, &req).await?;
    Ok((resp.error_code, resp.producer_id.0, resp.producer_epoch))
}

/// An idempotent InitProducerId presenting a producer id (KIP-360: a producer
/// resuming after its state expired, or retrying an epoch bump whose answer it lost)
/// gets a fresh producer id at epoch 0, as from Kafka 3.7, whether the presented id
/// is current, stale or unknown; the Java producer treats PRODUCER_FENCED and
/// UNKNOWN_PRODUCER_ID here as fatal. The new id produces from sequence 0.
pub async fn test_idempotent_reinit_with_an_old_producer_id_gets_a_new_one() -> TestResult {
    println!("=== Test: idempotent InitProducerId with an existing producer id ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-reinit")
        .build()
        .await?
        .name;
    let mut wire = Wire::connect().await?;

    let (code, first, epoch) = init_idempotent(&mut wire, -1, -1).await?;
    assert_eq!((code, epoch), (ERROR_NONE, 0));
    let mut seen = vec![first];
    for (presented, presented_epoch, what) in [
        (first, 0, "current id and epoch"),
        (first, 0, "a retry of the same request"),
        (first + 1_000_000, 0, "an id never issued"),
    ] {
        let (code, id, epoch) = init_idempotent(&mut wire, presented, presented_epoch).await?;
        assert_eq!((code, epoch), (ERROR_NONE, 0), "{what}");
        assert!(!seen.contains(&id), "{what}: a new producer id");
        seen.push(id);
    }

    let last = *seen.last().expect("ids were allocated");
    let (code, _) = wire
        .produce(&topic, 0, batch(&["a"], last, 0, 0, false, None), None)
        .await?;
    assert_eq!(code, ERROR_NONE, "the new id produces from sequence 0");
    println!("✅ every re-init got a fresh producer id\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
