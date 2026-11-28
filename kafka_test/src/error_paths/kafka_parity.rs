//! Responses that must match Apache Kafka 3.7 field for field, checked on the wire
//! because rdkafka does not expose the fields involved.

use crate::common::TestResult;
use crate::error_codes::{
    ILLEGAL_GENERATION, INCONSISTENT_GROUP_PROTOCOL, INVALID_GROUP_ID, INVALID_PRODUCER_EPOCH,
    INVALID_PRODUCER_ID_MAPPING, INVALID_SESSION_TIMEOUT, INVALID_TXN_STATE, NONE,
    REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID, UNKNOWN_TOPIC_OR_PARTITION,
};
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

/// TxnOffsetCommit v3 of offset 5 to `entries` (`(topic, partitions)`), as a member of
/// `generation`: the `(topic, partition, error)` of each answered partition.
#[expect(
    clippy::too_many_arguments,
    reason = "one argument per TxnOffsetCommit field the test varies"
)]
async fn txn_offset_commit_v3(
    wire: &mut Wire,
    transactional_id: &str,
    group: &str,
    producer_id: i64,
    epoch: i16,
    generation: i32,
    member: &str,
    entries: &[(&str, &[i32])],
) -> crate::common::DynResult<Vec<(String, i32, i16)>> {
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
    use kafka_protocol::messages::TransactionalId;
    let mut req = TxnOffsetCommitRequest::default();
    req.transactional_id = TransactionalId(StrBytes::from_string(transactional_id.to_string()));
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.producer_id = ProducerId(producer_id);
    req.producer_epoch = epoch;
    req.generation_id = generation;
    req.member_id = StrBytes::from_string(member.to_string());
    req.topics = entries
        .iter()
        .map(|(name, partitions)| {
            let mut t = TxnOffsetCommitRequestTopic::default();
            t.name = TopicName(StrBytes::from_string(name.to_string()));
            t.partitions = partitions
                .iter()
                .map(|&partition_index| {
                    let mut p = TxnOffsetCommitRequestPartition::default();
                    p.partition_index = partition_index;
                    p.committed_offset = 5;
                    p.committed_leader_epoch = -1;
                    p
                })
                .collect();
            t
        })
        .collect();
    let resp: TxnOffsetCommitResponse = wire.call(3, &req).await?;
    Ok(resp
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(move |p| (t.name.to_string(), p.partition_index, p.error_code))
        })
        .collect())
}

/// A refused TxnOffsetCommit answers a partition that does not exist
/// UNKNOWN_TOPIC_OR_PARTITION and the refusal's code only on the partitions that exist, as
/// Kafka 3.7 does (it filters unknown partitions out before it asks the coordinator), for
/// every refusal: an unknown producer, an Empty transaction, a group not added, a fenced
/// epoch, a stale member. Nothing is stored.
pub async fn test_refused_txn_offset_commit_answers_unknown_partitions() -> TestResult {
    println!("=== Test: a refused TxnOffsetCommit answers unknown partitions 3 ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-txn-refused")
        .build()
        .await?
        .name;
    let missing = ctx.unique_topic("parity-txn-refused-missing").await;
    let group = ctx.unique_group("parity-txn-refused").await;
    let txn = ctx.unique_group("parity-txn-refused-txn").await;
    let entries: [(&str, &[i32]); 2] = [(&topic, &[0, 7]), (&missing, &[0])];
    let answered = |code| {
        vec![
            (topic.clone(), 0, code),
            (topic.clone(), 7, UNKNOWN_TOPIC_OR_PARTITION),
            (missing.clone(), 0, UNKNOWN_TOPIC_OR_PARTITION),
        ]
    };
    let mut wire = Wire::connect().await?;

    println!("Step 1: refusals before the transaction is Ongoing...");
    let unknown =
        txn_offset_commit_v3(&mut wire, &txn, &group, 999_999, 3, -1, "", &entries).await?;
    assert_eq!(
        unknown,
        answered(INVALID_PRODUCER_ID_MAPPING),
        "unknown producer"
    );
    let (code, producer_id, epoch) = wire.init_producer_id(Some(&txn), 60_000).await?;
    assert_eq!(code, NONE);
    let empty = txn_offset_commit_v3(
        &mut wire,
        &txn,
        &group,
        producer_id,
        epoch,
        -1,
        "",
        &entries,
    )
    .await?;
    assert_eq!(empty, answered(INVALID_TXN_STATE), "Empty transaction");

    println!("Step 2: refusals of an Ongoing transaction...");
    assert_eq!(
        wire.add_partitions(&txn, producer_id, epoch, &[(topic.as_str(), &[0])])
            .await?,
        vec![(topic.clone(), 0, NONE)]
    );
    let not_added = txn_offset_commit_v3(
        &mut wire,
        &txn,
        &group,
        producer_id,
        epoch,
        -1,
        "",
        &entries,
    )
    .await?;
    assert_eq!(not_added, answered(INVALID_TXN_STATE), "group not added");
    let fenced = txn_offset_commit_v3(
        &mut wire,
        &txn,
        &group,
        producer_id,
        epoch + 1,
        -1,
        "",
        &entries,
    )
    .await?;
    assert_eq!(fenced, answered(INVALID_PRODUCER_EPOCH), "fenced epoch");
    let wrong_id = txn_offset_commit_v3(
        &mut wire,
        &txn,
        &group,
        producer_id + 1,
        epoch,
        -1,
        "",
        &entries,
    )
    .await?;
    assert_eq!(
        wrong_id,
        answered(INVALID_PRODUCER_ID_MAPPING),
        "another producer id"
    );
    assert_eq!(
        wire.add_offsets(&txn, producer_id, epoch, &group).await?,
        NONE
    );
    let zombie = txn_offset_commit_v3(
        &mut wire,
        &txn,
        &group,
        producer_id,
        epoch,
        5,
        "zombie",
        &entries,
    )
    .await?;
    assert_eq!(zombie, answered(UNKNOWN_MEMBER_ID), "stale member");
    println!("✅ Every refusal answers only the partitions that exist\n");

    println!("=== Database Verification ===\n");
    let pending: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
            &[&txn],
        )
        .await?
        .get(0);
    assert_eq!(pending, 0, "no pending offsets");
    assert_eq!(wire.end_txn(&txn, producer_id, epoch, false).await?, NONE);
    println!("✅ Nothing stored\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// OffsetCommit v8 of `offset` to `entries` (`(topic, partitions)`) as `member` of
/// `generation`: the `(topic, partition, error)` of each answered partition.
async fn offset_commit_v8(
    wire: &mut Wire,
    group: &str,
    generation: i32,
    member: &str,
    offset: i64,
    entries: &[(&str, &[i32])],
) -> crate::common::DynResult<Vec<(String, i32, i16)>> {
    let mut req = OffsetCommitRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.generation_id_or_member_epoch = generation;
    req.member_id = StrBytes::from_string(member.to_string());
    req.topics = entries
        .iter()
        .map(|(name, partitions)| {
            let mut t = OffsetCommitRequestTopic::default();
            t.name = TopicName(StrBytes::from_string(name.to_string()));
            t.partitions = partitions
                .iter()
                .map(|&partition_index| {
                    let mut p = OffsetCommitRequestPartition::default();
                    p.partition_index = partition_index;
                    p.committed_offset = offset;
                    p.committed_leader_epoch = -1;
                    p
                })
                .collect();
            t
        })
        .collect();
    let resp: OffsetCommitResponse = wire.call(8, &req).await?;
    Ok(resp
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(move |p| (t.name.to_string(), p.partition_index, p.error_code))
        })
        .collect())
}

/// The committed offset of `group` on partition 0 of `topic` (-1 for none), read from the
/// database.
async fn committed_offset(
    ctx: &TestContext,
    group: &str,
    topic: &str,
) -> crate::common::DynResult<i64> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT COALESCE(MAX(o.committed_offset), -1) FROM kafka.consumer_offsets o
               JOIN kafka.topics t ON o.topic_id = t.id
              WHERE o.group_id = $1 AND t.name = $2 AND o.partition_id = 0",
            &[&group, &topic],
        )
        .await?
        .get(0))
}

/// OffsetCommit with a generation >= 0, as Kafka 3.7 answers it: ILLEGAL_GENERATION for a
/// group with neither members nor offsets, UNKNOWN_MEMBER_ID for a group known only by its
/// committed offsets (an Empty group in Kafka), ILLEGAL_GENERATION for a stale generation of
/// a live group; generation -1 commits to a group without members. Refused commits store
/// nothing.
pub async fn test_offset_commit_generation_for_offsets_only_group() -> TestResult {
    use kafka_protocol::messages::join_group_request::{
        JoinGroupRequest, JoinGroupRequestProtocol,
    };
    use kafka_protocol::messages::join_group_response::JoinGroupResponse;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
    use kafka_protocol::messages::sync_group_response::SyncGroupResponse;

    println!("=== Test: OffsetCommit generation for an offsets-only group ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-offsets-only")
        .build()
        .await?
        .name;
    let group = ctx.unique_group("parity-offsets-only").await;
    let live = ctx.unique_group("parity-offsets-live").await;
    let entries: [(&str, &[i32]); 1] = [(&topic, &[0])];
    let answered = |code| vec![(topic.clone(), 0, code)];
    let mut wire = Wire::connect().await?;

    println!("Step 1: a group with neither members nor offsets...");
    let unknown = offset_commit_v8(&mut wire, &group, 5, "zz", 1, &entries).await?;
    assert_eq!(unknown, answered(ILLEGAL_GENERATION), "no such group");
    assert_eq!(committed_offset(&ctx, &group, &topic).await?, -1);

    println!("Step 2: generation -1 commits, then the group is known by its offsets...");
    let simple = offset_commit_v8(&mut wire, &group, -1, "", 1, &entries).await?;
    assert_eq!(simple, answered(NONE), "generation -1");
    assert_eq!(committed_offset(&ctx, &group, &topic).await?, 1);
    let member = offset_commit_v8(&mut wire, &group, 5, "zz", 2, &entries).await?;
    assert_eq!(
        member,
        answered(UNKNOWN_MEMBER_ID),
        "offsets-only, generation 5"
    );
    let no_member = offset_commit_v8(&mut wire, &group, 0, "", 2, &entries).await?;
    assert_eq!(
        no_member,
        answered(UNKNOWN_MEMBER_ID),
        "offsets-only, generation 0"
    );
    assert_eq!(
        committed_offset(&ctx, &group, &topic).await?,
        1,
        "refused commits store nothing"
    );

    println!("Step 3: a stale generation of a live group...");
    let mut join = JoinGroupRequest::default();
    join.group_id = GroupId(StrBytes::from_string(live.clone()));
    join.session_timeout_ms = 30_000;
    join.rebalance_timeout_ms = 30_000;
    join.protocol_type = StrBytes::from_static_str("consumer");
    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = StrBytes::from_static_str("range");
    join.protocols = vec![protocol];
    let joined: JoinGroupResponse = wire.call(3, &join).await?;
    assert_eq!(joined.error_code, NONE);
    let mut sync = SyncGroupRequest::default();
    sync.group_id = join.group_id.clone();
    sync.generation_id = joined.generation_id;
    sync.member_id = joined.member_id.clone();
    let synced: SyncGroupResponse = wire.call(3, &sync).await?;
    assert_eq!(synced.error_code, NONE);
    let member_id = joined.member_id.to_string();
    let stale = offset_commit_v8(
        &mut wire,
        &live,
        joined.generation_id + 1,
        &member_id,
        1,
        &entries,
    )
    .await?;
    assert_eq!(stale, answered(ILLEGAL_GENERATION), "stale generation");
    assert_eq!(committed_offset(&ctx, &live, &topic).await?, -1);
    println!("✅ 22 / 0 / 25 / 22 as Kafka answers them\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A refused OffsetCommit answers a partition that does not exist
/// UNKNOWN_TOPIC_OR_PARTITION and the refusal's code only on the partitions that exist, as
/// Kafka 3.7 does (it filters unknown partitions out before it asks the coordinator).
pub async fn test_refused_offset_commit_answers_unknown_partitions() -> TestResult {
    println!("=== Test: a refused OffsetCommit answers unknown partitions 3 ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-commit-refused")
        .build()
        .await?
        .name;
    let missing = ctx.unique_topic("parity-commit-refused-missing").await;
    let group = ctx.unique_group("parity-commit-refused").await;
    let entries: [(&str, &[i32]); 2] = [(&topic, &[0, 7]), (&missing, &[0])];
    let answered = |code| {
        vec![
            (topic.clone(), 0, code),
            (topic.clone(), 7, UNKNOWN_TOPIC_OR_PARTITION),
            (missing.clone(), 0, UNKNOWN_TOPIC_OR_PARTITION),
        ]
    };
    let mut wire = Wire::connect().await?;

    let unknown = offset_commit_v8(&mut wire, &group, 5, "zz", 1, &entries).await?;
    assert_eq!(unknown, answered(ILLEGAL_GENERATION), "no such group");
    assert_eq!(committed_offset(&ctx, &group, &topic).await?, -1);
    println!("✅ ILLEGAL_GENERATION only where the partition exists\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// A JoinGroup of `group` as `member`, protocol type `protocol_type` and one "range"
/// protocol.
fn join_request(
    group: &str,
    member: &str,
    session_timeout_ms: i32,
    protocol_type: &str,
) -> kafka_protocol::messages::join_group_request::JoinGroupRequest {
    use kafka_protocol::messages::join_group_request::{
        JoinGroupRequest, JoinGroupRequestProtocol,
    };
    let mut join = JoinGroupRequest::default();
    join.group_id = GroupId(StrBytes::from_string(group.to_string()));
    join.session_timeout_ms = session_timeout_ms;
    join.rebalance_timeout_ms = 30_000;
    join.member_id = StrBytes::from_string(member.to_string());
    join.protocol_type = StrBytes::from_string(protocol_type.to_string());
    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = StrBytes::from_static_str("range");
    join.protocols = vec![protocol];
    join
}

/// JoinGroup v5 of `group` as `member` ([`join_request`]).
async fn join_v5(
    wire: &mut Wire,
    group: &str,
    member: &str,
    session_timeout_ms: i32,
    protocol_type: &str,
) -> crate::common::DynResult<kafka_protocol::messages::join_group_response::JoinGroupResponse> {
    wire.call(
        5,
        &join_request(group, member, session_timeout_ms, protocol_type),
    )
    .await
}

/// A SyncGroup v3 of `group` by `member` at `generation`, with no assignments.
fn sync_request(
    group: &str,
    member: &str,
    generation: i32,
) -> kafka_protocol::messages::sync_group_request::SyncGroupRequest {
    let mut sync = kafka_protocol::messages::sync_group_request::SyncGroupRequest::default();
    sync.group_id = GroupId(StrBytes::from_string(group.to_string()));
    sync.generation_id = generation;
    sync.member_id = StrBytes::from_string(member.to_string());
    sync
}

/// A Stable two-member group `group`: member `a` (the leader, on connection `a`)
/// and `b` (on `b`), both synced at the second generation. Their member ids.
async fn two_member_group(
    a: &mut tokio::net::TcpStream,
    b: &mut tokio::net::TcpStream,
    group: &str,
) -> crate::common::DynResult<(String, String)> {
    use crate::wire::{recv, roundtrip, send};
    use kafka_protocol::messages::join_group_request::JoinGroupRequest;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
    let first = roundtrip(a, 5, 1, &join_request(group, "", 30_000, "consumer")).await?;
    assert_eq!(first.error_code, NONE, "first join of {group}");
    let ma = first.member_id.to_string();
    let synced = roundtrip(a, 3, 2, &sync_request(group, &ma, first.generation_id)).await?;
    assert_eq!(synced.error_code, NONE, "first sync of {group}");
    // b's join is held until a rejoins.
    send(b, 5, 1, &join_request(group, "", 30_000, "consumer")).await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let second = roundtrip(a, 5, 3, &join_request(group, &ma, 30_000, "consumer")).await?;
    let joined_b = recv::<JoinGroupRequest>(b, 5, 1).await?;
    assert_eq!((second.error_code, joined_b.error_code), (NONE, NONE));
    let mb = joined_b.member_id.to_string();
    let generation = second.generation_id;
    send(b, 3, 2, &sync_request(group, &mb, generation)).await?;
    let synced = roundtrip(a, 3, 4, &sync_request(group, &ma, generation)).await?;
    let synced_b = recv::<SyncGroupRequest>(b, 3, 2).await?;
    assert_eq!((synced.error_code, synced_b.error_code), (NONE, NONE));
    Ok((ma, mb))
}

/// A one-member group `group`, joined and synced (Stable): its member id and generation.
async fn stable_group(wire: &mut Wire, group: &str) -> crate::common::DynResult<(String, i32)> {
    use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
    use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
    let joined = join_v5(wire, group, "", 30_000, "consumer").await?;
    assert_eq!(joined.error_code, NONE, "join {group}");
    let mut sync = SyncGroupRequest::default();
    sync.group_id = GroupId(StrBytes::from_string(group.to_string()));
    sync.generation_id = joined.generation_id;
    sync.member_id = joined.member_id.clone();
    let synced: SyncGroupResponse = wire.call(3, &sync).await?;
    assert_eq!(synced.error_code, NONE, "sync {group}");
    Ok((joined.member_id.to_string(), joined.generation_id))
}

/// Group member checks as a Kafka 3.7 broker answers them. TxnOffsetCommit v3 (pg_kafka let
/// generation -1 through whatever the member id, and fenced CompletingRebalance):
/// - a group with members: generation -1 with a member id it does not know is
///   UNKNOWN_MEMBER_ID, with one of its members ILLEGAL_GENERATION, with no member id it
///   commits;
/// - an unknown or Empty group: generation -1 commits whatever the member id;
/// - a CompletingRebalance group (joined, not synced): its member's current generation
///   commits (the transactional path checks no group state), where a plain OffsetCommit
///   is REBALANCE_IN_PROGRESS.
///
/// JoinGroup v5 errors answered at once carry the request's member id (pg_kafka answered
/// ""): INVALID_SESSION_TIMEOUT, UNKNOWN_MEMBER_ID of a new or an existing group, and
/// INCONSISTENT_GROUP_PROTOCOL of a known member; a new member's carry none. A held
/// JoinGroup whose member a LeaveGroup v3 removes is answered UNKNOWN_MEMBER_ID with
/// member id "" (pg_kafka echoed the request's).
pub async fn test_group_member_checks_match_kafka() -> TestResult {
    use kafka_protocol::messages::leave_group_request::LeaveGroupRequest;
    use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;

    println!("=== Test: TxnOffsetCommit and JoinGroup member checks ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-txn-member")
        .build()
        .await?
        .name;
    let live = ctx.unique_group("parity-txn-member-live").await;
    let unknown = ctx.unique_group("parity-txn-member-unknown").await;
    let empty = ctx.unique_group("parity-txn-member-empty").await;
    let completing = ctx.unique_group("parity-txn-member-completing").await;
    let txn = ctx.unique_group("parity-txn-member-txn").await;
    let entries: [(&str, &[i32]); 1] = [(&topic, &[0])];
    let answered = |code| vec![(topic.clone(), 0, code)];
    let mut wire = Wire::connect().await?;

    println!("Step 1: a Stable group, an Empty one, and a transaction that added them...");
    let (member, _generation) = stable_group(&mut wire, &live).await?;
    let (left_member, _) = stable_group(&mut wire, &empty).await?;
    let mut leave = LeaveGroupRequest::default();
    leave.group_id = GroupId(StrBytes::from_string(empty.clone()));
    leave.member_id = StrBytes::from_string(left_member.clone());
    let left: LeaveGroupResponse = wire.call(0, &leave).await?;
    assert_eq!(left.error_code, NONE, "leave");
    let joined = join_v5(&mut wire, &completing, "", 30_000, "consumer").await?;
    assert_eq!(joined.error_code, NONE, "join {completing}");
    let (completing_member, completing_generation) =
        (joined.member_id.to_string(), joined.generation_id);
    let (code, producer_id, epoch) = wire.init_producer_id(Some(&txn), 60_000).await?;
    assert_eq!(code, NONE);
    assert_eq!(
        wire.add_partitions(&txn, producer_id, epoch, &[(topic.as_str(), &[0])])
            .await?,
        vec![(topic.clone(), 0, NONE)]
    );
    for group in [&live, &unknown, &empty, &completing] {
        assert_eq!(
            wire.add_offsets(&txn, producer_id, epoch, group).await?,
            NONE
        );
    }
    let mut commit = async |group: &str, generation: i32, member: &str| {
        txn_offset_commit_v3(
            &mut wire,
            &txn,
            group,
            producer_id,
            epoch,
            generation,
            member,
            &entries,
        )
        .await
    };

    println!("Step 2: generation -1 to the Stable group...");
    assert_eq!(
        commit(&live, -1, "zz").await?,
        answered(UNKNOWN_MEMBER_ID),
        "generation -1, unknown member"
    );
    assert_eq!(
        commit(&live, -1, &member).await?,
        answered(ILLEGAL_GENERATION),
        "generation -1, the group's member"
    );
    assert_eq!(
        commit(&live, -1, "").await?,
        answered(NONE),
        "generation -1, no member id"
    );

    println!("Step 3: generation -1 with a member id to groups without members...");
    assert_eq!(
        commit(&unknown, -1, "zz").await?,
        answered(NONE),
        "unknown group"
    );
    assert_eq!(
        commit(&empty, -1, "zz").await?,
        answered(NONE),
        "Empty group"
    );
    assert_eq!(
        commit(&empty, -1, &left_member).await?,
        answered(NONE),
        "Empty group, its former member"
    );

    println!("Step 4: the current generation of a CompletingRebalance group...");
    let plain = offset_commit_v8(
        &mut wire,
        &completing,
        completing_generation,
        &completing_member,
        1,
        &entries,
    )
    .await?;
    assert_eq!(plain, answered(REBALANCE_IN_PROGRESS), "plain OffsetCommit");
    let mut commit = async |group: &str, generation: i32, member: &str| {
        txn_offset_commit_v3(
            &mut wire,
            &txn,
            group,
            producer_id,
            epoch,
            generation,
            member,
            &entries,
        )
        .await
    };
    assert_eq!(
        commit(&completing, completing_generation, &completing_member).await?,
        answered(NONE),
        "TxnOffsetCommit"
    );
    println!("✅ 25 / 22 / 0, 0 without members and 0 while completing, as Kafka answers them\n");

    println!("=== Database Verification ===\n");
    let pending: Vec<String> = ctx
        .db()
        .query(
            "SELECT group_id FROM kafka.txn_pending_offsets WHERE transactional_id = $1
              ORDER BY group_id",
            &[&txn],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let mut expected = vec![
        live.clone(),
        unknown.clone(),
        empty.clone(),
        completing.clone(),
    ];
    expected.sort();
    assert_eq!(pending, expected, "the accepted commits are pending");
    assert_eq!(wire.end_txn(&txn, producer_id, epoch, false).await?, NONE);
    println!("✅ Only the accepted commits are stored\n");

    println!("Step 5: JoinGroup errors carry the request's member id...");
    let fresh = ctx.unique_group("parity-txn-member-fresh").await;
    let mut join_error = async |group: &str, member: &str, timeout: i32, protocol_type: &str| {
        let r = join_v5(&mut wire, group, member, timeout, protocol_type).await?;
        Ok::<_, Box<dyn std::error::Error>>((
            r.error_code,
            r.generation_id,
            r.member_id.to_string(),
        ))
    };
    let error = |code: i16, member: &str| (code, -1, member.to_string());
    assert_eq!(
        join_error(&fresh, "zz", 1, "consumer").await?,
        error(INVALID_SESSION_TIMEOUT, "zz"),
        "invalid session timeout"
    );
    assert_eq!(
        join_error(&fresh, "", 1, "consumer").await?,
        error(INVALID_SESSION_TIMEOUT, ""),
        "invalid session timeout, new member"
    );
    assert_eq!(
        join_error(&fresh, "zz", 30_000, "consumer").await?,
        error(UNKNOWN_MEMBER_ID, "zz"),
        "unknown member of a new group"
    );
    assert_eq!(
        join_error(&live, "zz", 30_000, "consumer").await?,
        error(UNKNOWN_MEMBER_ID, "zz"),
        "unknown member of an existing group"
    );
    assert_eq!(
        join_error(&live, &member, 30_000, "other").await?,
        error(INCONSISTENT_GROUP_PROTOCOL, &member),
        "another protocol type, known member"
    );
    assert_eq!(
        join_error(&live, "", 30_000, "other").await?,
        error(INCONSISTENT_GROUP_PROTOCOL, ""),
        "another protocol type, new member"
    );
    println!("✅ 26 / 25 / 23 with the member id, as Kafka answers them\n");

    println!("Step 6: a held JoinGroup whose member a LeaveGroup removes...");
    {
        use crate::wire::{recv, send};
        use kafka_protocol::messages::join_group_request::JoinGroupRequest;
        let held = ctx.unique_group("parity-txn-member-held").await;
        let addr = crate::common::get_bootstrap_servers();
        let mut a = tokio::net::TcpStream::connect(addr.as_str()).await?;
        let mut b = tokio::net::TcpStream::connect(addr.as_str()).await?;
        let (ma, _mb) = two_member_group(&mut a, &mut b, &held).await?;
        // a's rejoin is held: b has not rejoined.
        send(&mut a, 5, 9, &join_request(&held, &ma, 30_000, "consumer")).await?;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let mut leave = LeaveGroupRequest::default();
        leave.group_id = GroupId(StrBytes::from_string(held.clone()));
        let mut identity = kafka_protocol::messages::leave_group_request::MemberIdentity::default();
        identity.member_id = StrBytes::from_string(ma.clone());
        leave.members = vec![identity];
        let left: LeaveGroupResponse = wire.call(3, &leave).await?;
        assert_eq!(
            (left.error_code, left.members[0].error_code),
            (NONE, NONE),
            "leave"
        );
        let answer = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            recv::<JoinGroupRequest>(&mut a, 5, 9),
        )
        .await??;
        assert_eq!(
            (
                answer.error_code,
                answer.generation_id,
                answer.member_id.to_string()
            ),
            (UNKNOWN_MEMBER_ID, -1, String::new()),
            "a held JoinGroup answered after its member left has no member id (it sent {ma})"
        );
    }
    println!("✅ 25 with member id \"\", as Kafka answers a held join of a removed member\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Fetch v11 of partition 0 of `topic` from `offset` with `partition_max_bytes`.
async fn fetch_with_partition_max(
    wire: &mut Wire,
    topic: &str,
    offset: i64,
    partition_max_bytes: i32,
) -> crate::common::DynResult<kafka_protocol::messages::fetch_response::PartitionData> {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
    use kafka_protocol::messages::fetch_response::FetchResponse;
    let partition = FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(offset)
        .with_partition_max_bytes(partition_max_bytes)
        .with_log_start_offset(-1)
        .with_current_leader_epoch(-1)
        .with_last_fetched_epoch(-1);
    let request = FetchRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId(-1))
        .with_max_bytes(1 << 20)
        .with_session_epoch(-1)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![partition])]);
    let response: FetchResponse = wire.call(11, &request).await?;
    Ok(response.responses[0].partitions[0].clone())
}

/// A Fetch with a negative `partition_max_bytes` of a partition with records to read is
/// answered as Kafka 3.7 answers it: UNKNOWN_SERVER_ERROR, high watermark, last stable
/// offset and log start -1, no records. Fetched at the log end, nothing is read and the
/// partition answers its bounds.
pub async fn test_negative_partition_max_bytes_is_unknown_server_error() -> TestResult {
    use crate::error_codes::UNKNOWN_SERVER_ERROR;
    println!("=== Test: A negative partition_max_bytes is UNKNOWN_SERVER_ERROR ===\n");
    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "parity-neg-pmax")
        .build()
        .await?
        .name;
    let mut wire = Wire::connect().await?;
    assert_eq!(
        produce_v8(&mut wire, &topic, &["a", "b", "c"]).await?.0,
        NONE
    );

    for limit in [-1, i32::MIN] {
        let p = fetch_with_partition_max(&mut wire, &topic, 0, limit).await?;
        println!(
            "   limit {limit}: error {}, hwm {}, records {:?}",
            p.error_code,
            p.high_watermark,
            p.records.as_ref().map(bytes::Bytes::len)
        );
        assert_eq!(p.error_code, UNKNOWN_SERVER_ERROR, "limit {limit}");
        assert_eq!(
            (p.high_watermark, p.last_stable_offset, p.log_start_offset),
            (-1, -1, -1),
            "limit {limit}"
        );
        assert!(p.records.as_ref().is_none_or(bytes::Bytes::is_empty));
    }
    let at_end = fetch_with_partition_max(&mut wire, &topic, 3, -1).await?;
    assert_eq!((at_end.error_code, at_end.high_watermark), (NONE, 3));
    assert!(at_end.records.as_ref().is_none_or(bytes::Bytes::is_empty));
    // A positive limit still reads the records.
    let read = fetch_with_partition_max(&mut wire, &topic, 0, 1 << 20).await?;
    assert_eq!(read.error_code, NONE);
    assert!(read.records.as_ref().is_some_and(|r| !r.is_empty()));

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

