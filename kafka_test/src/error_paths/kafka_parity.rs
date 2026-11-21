//! Responses that must match Apache Kafka 3.7 field for field, checked on the wire
//! because rdkafka does not expose the fields involved.

use crate::common::TestResult;
use crate::error_codes::{INVALID_GROUP_ID, NONE, UNKNOWN_TOPIC_OR_PARTITION};
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use crate::wire::records::{batch, now_ms};
use crate::wire::Wire;
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

/// Produce v8 of `values` to partition 0: `(error_code, base_offset, log_start_offset)`.
async fn produce_v8(
    wire: &mut Wire,
    topic: &str,
    values: &[&str],
) -> crate::common::DynResult<(i16, i64, i64)> {
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
    let resp: ProduceResponse = wire.call(8, &req).await?;
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
        (NONE, 0, 0)
    );
    assert_eq!(wire.delete_records(&topic, 0, 2).await?, (NONE, 2));
    assert_eq!(produce_v8(&mut wire, &topic, &["d"]).await?, (NONE, 3, 2));
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
    assert_eq!(code, NONE);

    let after_last = wire.list_offsets(&topic, 0, now + 3_600_000, 0).await?;
    assert_eq!(
        after_last,
        (NONE, -1, -1),
        "timestamp after the last record"
    );
    let by_timestamp = wire.list_offsets(&topic, 0, now, 0).await?;
    assert_eq!(
        by_timestamp,
        (NONE, 0, now),
        "a matching record still resolves"
    );
    let max_on_empty = wire.list_offsets(&topic, 1, -3, 0).await?;
    assert_eq!(
        max_on_empty,
        (NONE, -1, -1),
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
    let resp: OffsetCommitResponse = wire.call(8, &req).await?;
    let codes: Vec<(i32, i16)> = resp.topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.error_code))
        .collect();
    assert_eq!(codes, vec![(0, NONE), (5, UNKNOWN_TOPIC_OR_PARTITION)]);

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
) -> crate::common::DynResult<(i16, i64, i16)> {
    let mut req = InitProducerIdRequest::default();
    req.transactional_id = None;
    req.transaction_timeout_ms = 60_000;
    req.producer_id = ProducerId(producer_id);
    req.producer_epoch = epoch;
    let resp: InitProducerIdResponse = wire.call(4, &req).await?;
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
    assert_eq!((code, epoch), (NONE, 0));
    let mut seen = vec![first];
    for (presented, presented_epoch, what) in [
        (first, 0, "current id and epoch"),
        (first, 0, "a retry of the same request"),
        (first + 1_000_000, 0, "an id never issued"),
    ] {
        let (code, id, epoch) = init_idempotent(&mut wire, presented, presented_epoch).await?;
        assert_eq!((code, epoch), (NONE, 0), "{what}");
        assert!(!seen.contains(&id), "{what}: a new producer id");
        seen.push(id);
    }

    let last = *seen.last().expect("ids were allocated");
    let (code, _) = wire
        .produce(&topic, 0, batch(&["a"], last, 0, 0, false, None), None)
        .await?;
    assert_eq!(code, NONE, "the new id produces from sequence 0");
    println!("✅ every re-init got a fresh producer id\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The empty group id, as Kafka 3.7 answers it (`GroupCoordinator.isValidGroupId`):
/// JoinGroup, Heartbeat, SyncGroup, LeaveGroup and TxnOffsetCommit refuse it with
/// INVALID_GROUP_ID and change nothing, while AddOffsetsToTxn and DescribeGroups take it.
/// TxnOffsetCommit refuses it before any transaction check (no AddPartitionsToTxn yet, an
/// Ongoing transaction, the group added) and answers a partition that does not exist
/// UNKNOWN_TOPIC_OR_PARTITION.
pub async fn test_empty_group_id_is_invalid_group_id() -> TestResult {
    use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
    use kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse;
    use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
    use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
    use kafka_protocol::messages::join_group_request::{
        JoinGroupRequest, JoinGroupRequestProtocol,
    };
    use kafka_protocol::messages::join_group_response::JoinGroupResponse;
    use kafka_protocol::messages::leave_group_request::{LeaveGroupRequest, MemberIdentity};
    use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
    use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
    use kafka_protocol::messages::TransactionalId;

    println!("=== Test: the empty group id is INVALID_GROUP_ID ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-empty-group")
        .build()
        .await?
        .name;
    let txn = ctx.unique_group("parity-empty-group-txn").await;
    let missing = ctx.unique_topic("parity-empty-group-missing").await;
    let empty = || GroupId(StrBytes::from_static_str(""));
    let member = || StrBytes::from_static_str("zz");
    let mut wire = Wire::connect().await?;

    println!("Step 1: group membership APIs with group id \"\"...");
    let mut join = JoinGroupRequest::default();
    join.group_id = empty();
    join.session_timeout_ms = 10_000;
    join.rebalance_timeout_ms = 10_000;
    join.member_id = member();
    join.protocol_type = StrBytes::from_static_str("consumer");
    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = StrBytes::from_static_str("range");
    join.protocols = vec![protocol];
    let joined: JoinGroupResponse = wire.call(5, &join).await?;
    assert_eq!(
        (
            joined.error_code,
            joined.generation_id,
            joined.member_id.as_str()
        ),
        (INVALID_GROUP_ID, -1, "zz"),
        "JoinGroup echoes the member id"
    );
    assert_eq!(
        joined.protocol_name.as_deref(),
        Some(""),
        "protocol_name is not nullable before v7"
    );
    join.member_id = StrBytes::from_static_str("");
    let joined: JoinGroupResponse = wire.call(5, &join).await?;
    assert_eq!(
        (joined.error_code, joined.member_id.as_str()),
        (INVALID_GROUP_ID, ""),
        "JoinGroup without a member id"
    );

    let mut heartbeat = HeartbeatRequest::default();
    heartbeat.group_id = empty();
    heartbeat.generation_id = 1;
    heartbeat.member_id = member();
    let beat: HeartbeatResponse = wire.call(3, &heartbeat).await?;
    assert_eq!(beat.error_code, INVALID_GROUP_ID, "Heartbeat");

    let mut sync = SyncGroupRequest::default();
    sync.group_id = empty();
    sync.generation_id = 1;
    sync.member_id = member();
    let synced: SyncGroupResponse = wire.call(3, &sync).await?;
    assert_eq!(
        (synced.error_code, synced.assignment.len()),
        (INVALID_GROUP_ID, 0),
        "SyncGroup"
    );

    let mut leave = LeaveGroupRequest::default();
    leave.group_id = empty();
    let mut leaving = MemberIdentity::default();
    leaving.member_id = member();
    leave.members = vec![leaving];
    let left: LeaveGroupResponse = wire.call(3, &leave).await?;
    assert_eq!(
        (left.error_code, left.members.len()),
        (INVALID_GROUP_ID, 0),
        "LeaveGroup v3 answers at the top level"
    );

    // Nothing joined: the group "" does not exist.
    let mut describe = DescribeGroupsRequest::default();
    describe.groups = vec![empty()];
    let described: DescribeGroupsResponse = wire.call(0, &describe).await?;
    let group = &described.groups[0];
    assert_eq!(
        (
            group.error_code,
            group.group_state.as_str(),
            group.members.len()
        ),
        (NONE, "Dead", 0),
        "DescribeGroups takes \"\" and finds no group"
    );
    println!("✅ INVALID_GROUP_ID, no group created\n");

    println!("Step 2: TxnOffsetCommit with group id \"\" through a transaction...");
    let txn_offset_commit = |producer_id: i64, epoch: i16| {
        let partition = |partition_index| {
            let mut p = TxnOffsetCommitRequestPartition::default();
            p.partition_index = partition_index;
            p.committed_offset = 5;
            p.committed_leader_epoch = -1;
            p
        };
        let topic_entry = |name: &str, partitions: &[i32]| {
            let mut t = TxnOffsetCommitRequestTopic::default();
            t.name = TopicName(StrBytes::from_string(name.to_string()));
            t.partitions = partitions.iter().copied().map(partition).collect();
            t
        };
        let mut req = TxnOffsetCommitRequest::default();
        req.transactional_id = TransactionalId(StrBytes::from_string(txn.clone()));
        req.group_id = empty();
        req.producer_id = ProducerId(producer_id);
        req.producer_epoch = epoch;
        req.generation_id = 5;
        req.member_id = member();
        req.topics = vec![topic_entry(&topic, &[0, 7]), topic_entry(&missing, &[0])];
        req
    };
    let codes = |resp: TxnOffsetCommitResponse| -> Vec<(String, i32, i16)> {
        resp.topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |p| (t.name.to_string(), p.partition_index, p.error_code))
            })
            .collect()
    };
    let expected = vec![
        (topic.clone(), 0, INVALID_GROUP_ID),
        (topic.clone(), 7, UNKNOWN_TOPIC_OR_PARTITION),
        (missing.clone(), 0, UNKNOWN_TOPIC_OR_PARTITION),
    ];

    let unknown: TxnOffsetCommitResponse = wire.call(3, &txn_offset_commit(999_999, 3)).await?;
    assert_eq!(codes(unknown), expected, "a producer the broker never saw");
    let (code, producer_id, epoch) = wire.init_producer_id(Some(&txn), 60_000).await?;
    assert_eq!(code, NONE);
    let before_add: TxnOffsetCommitResponse =
        wire.call(3, &txn_offset_commit(producer_id, epoch)).await?;
    assert_eq!(codes(before_add), expected, "before AddPartitionsToTxn");
    assert_eq!(
        wire.add_partitions(&txn, producer_id, epoch, &[(topic.as_str(), &[0])])
            .await?,
        vec![(topic.clone(), 0, NONE)]
    );
    let ongoing: TxnOffsetCommitResponse =
        wire.call(3, &txn_offset_commit(producer_id, epoch)).await?;
    assert_eq!(codes(ongoing), expected, "Ongoing, group not added");
    let fenced: TxnOffsetCommitResponse = wire
        .call(3, &txn_offset_commit(producer_id, epoch + 1))
        .await?;
    assert_eq!(codes(fenced), expected, "a wrong epoch");
    assert_eq!(
        wire.add_offsets(&txn, producer_id, epoch, "").await?,
        NONE,
        "AddOffsetsToTxn takes \"\""
    );
    let added: TxnOffsetCommitResponse =
        wire.call(3, &txn_offset_commit(producer_id, epoch)).await?;
    assert_eq!(codes(added), expected, "after AddOffsetsToTxn(\"\")");
    assert_eq!(wire.end_txn(&txn, producer_id, epoch, true).await?, NONE);
    let (code, offsets) = wire.offset_fetch("", &topic, &[0], false).await?;
    assert_eq!(
        (code, offsets),
        (NONE, vec![(0, -1, NONE)]),
        "nothing committed"
    );
    println!("✅ INVALID_GROUP_ID before every transaction check, nothing committed\n");

    println!("=== Database Verification ===\n");
    let stored: i64 = ctx
        .db()
        .query_one(
            "SELECT (SELECT COUNT(*) FROM kafka.consumer_offsets o JOIN kafka.topics t
                       ON o.topic_id = t.id WHERE t.name = $1)
                  + (SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE transactional_id = $2)",
            &[&topic, &txn],
        )
        .await?
        .get(0);
    assert_eq!(stored, 0, "no committed or pending offsets");
    println!("✅ No offsets stored\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
