//! Transaction answers checked against Kafka 3.7: EndTxn of an Empty transaction,
//! transactional commit metadata over the length limit, and the last stable offset a
//! read_uncommitted Fetch reports.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use kafka_protocol::messages::end_txn_request::EndTxnRequest;
use kafka_protocol::messages::end_txn_response::EndTxnResponse;
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
use kafka_protocol::messages::{GroupId, ProducerId, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

/// Kafka 3.7 answers EndTxn for a transaction that added nothing since InitProducerId
/// (state Empty) INVALID_TXN_STATE, commit or abort, on every version; pg_kafka answered
/// NONE. (The Java client and librdkafka send no EndTxn for such a transaction.) The
/// transaction is unchanged: it still starts and commits normally afterwards.
pub async fn test_end_txn_of_an_empty_transaction_is_invalid_txn_state() -> TestResult {
    println!("=== Test: EndTxn of an Empty transaction is INVALID_TXN_STATE ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-empty-end", 1).await?;
    let id = format!("txn-empty-end-{}", Uuid::new_v4());
    let mut w = Wire::connect().await?;
    let (err, pid, epoch) = w.init_producer_id(Some(&id), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    for version in 0..=3 {
        for committed in [true, false] {
            let mut req = EndTxnRequest::default();
            req.transactional_id = TransactionalId(s(&id));
            req.producer_id = ProducerId(pid);
            req.producer_epoch = epoch;
            req.committed = committed;
            let r: EndTxnResponse = w.call(version, &req).await?;
            assert_eq!(
                r.error_code,
                error_codes::INVALID_TXN_STATE,
                "EndTxn v{version} committed={committed}"
            );
        }
    }
    println!("✅ INVALID_TXN_STATE on v0-v3, commit and abort\n");
    let added = w
        .add_partitions(&id, pid, epoch, &[(t.as_str(), &[0])])
        .await?;
    assert_eq!(added[0].2, error_codes::NONE);
    let (err, _) = w
        .produce(&t, 0, batch(&["a"], pid, epoch, 0, true, None), Some(&id))
        .await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(w.end_txn(&id, pid, epoch, true).await?, error_codes::NONE);
    // A retried commit of the transaction that committed is still NONE.
    assert_eq!(w.end_txn(&id, pid, epoch, true).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka 3.7 (offset.metadata.max.bytes = 4096) answers a TxnOffsetCommit partition whose
/// metadata is longer OFFSET_METADATA_TOO_LARGE and stores the others as pending offsets.
pub async fn test_txn_offset_commit_metadata_longer_than_4096_is_refused() -> TestResult {
    println!("=== Test: TxnOffsetCommit metadata over 4096 characters ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-oc-metadata", 2).await?;
    let group = ctx.unique_group("txn-oc-metadata").await;
    let id = format!("txn-oc-metadata-{}", Uuid::new_v4());
    let mut w = Wire::connect().await?;
    let (err, pid, epoch) = w.init_producer_id(Some(&id), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(
        w.add_offsets(&id, pid, epoch, &group).await?,
        error_codes::NONE
    );
    let partition = |index, metadata: String| {
        let mut p = TxnOffsetCommitRequestPartition::default();
        p.partition_index = index;
        p.committed_offset = 7;
        p.committed_metadata = Some(s(&metadata));
        p
    };
    let mut topic_entry = TxnOffsetCommitRequestTopic::default();
    topic_entry.name = TopicName(s(&t));
    topic_entry.partitions = vec![
        partition(0, "x".repeat(4096)),
        partition(1, "x".repeat(4097)),
    ];
    let mut req = TxnOffsetCommitRequest::default();
    req.transactional_id = TransactionalId(s(&id));
    req.group_id = GroupId(s(&group));
    req.producer_id = ProducerId(pid);
    req.producer_epoch = epoch;
    req.generation_id = -1;
    req.topics = vec![topic_entry];
    let r: TxnOffsetCommitResponse = w.call(3, &req).await?;
    let codes: Vec<i16> = r.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert_eq!(
        codes,
        [error_codes::NONE, error_codes::OFFSET_METADATA_TOO_LARGE]
    );
    assert_eq!(w.end_txn(&id, pid, epoch, true).await?, error_codes::NONE);
    let (code, offsets) = w.offset_fetch(&group, &t, &[0, 1], false).await?;
    assert_eq!(code, error_codes::NONE);
    assert_eq!(
        offsets,
        vec![(0, 7, error_codes::NONE), (1, -1, error_codes::NONE)]
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka reports a partition's real last stable offset (the first offset of an open
/// transaction) in every Fetch response, read_uncommitted included; pg_kafka reported the
/// high watermark there. What each isolation level reads is unchanged.
pub async fn test_read_uncommitted_fetch_reports_the_last_stable_offset() -> TestResult {
    println!("=== Test: a read_uncommitted Fetch reports the last stable offset ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-ru-lso", 1).await?;
    let id = format!("txn-ru-lso-{}", Uuid::new_v4());
    let mut w = Wire::connect().await?;
    let (err, base) = w
        .produce(&t, 0, batch(&["a", "b"], -1, -1, -1, false, None), None)
        .await?;
    assert_eq!((err, base), (error_codes::NONE, 0));
    let (err, pid, epoch) = w.init_producer_id(Some(&id), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let added = w
        .add_partitions(&id, pid, epoch, &[(t.as_str(), &[0])])
        .await?;
    assert_eq!(added[0].2, error_codes::NONE);
    let (err, base) = w
        .produce(
            &t,
            0,
            batch(&["c", "d"], pid, epoch, 0, true, None),
            Some(&id),
        )
        .await?;
    assert_eq!((err, base), (error_codes::NONE, 2));

    for isolation in [0, 1] {
        let p = w.fetch(&t, 0, 0, isolation).await?;
        assert_eq!(p.error_code, error_codes::NONE);
        assert_eq!(
            (p.high_watermark, p.last_stable_offset),
            (4, 2),
            "isolation level {isolation}"
        );
    }
    assert_eq!(w.end_txn(&id, pid, epoch, true).await?, error_codes::NONE);
    for isolation in [0, 1] {
        let p = w.fetch(&t, 0, 0, isolation).await?;
        assert_eq!(
            (p.high_watermark, p.last_stable_offset),
            (4, 4),
            "isolation level {isolation} after the commit"
        );
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The group state ListGroups (v4) and DescribeGroups report for `group`:
/// `(listed state, described error, described state)`.
async fn group_state(
    w: &mut Wire,
    group: &str,
) -> Result<(Option<String>, i16, String), Box<dyn std::error::Error>> {
    use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
    use kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse;
    use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
    use kafka_protocol::messages::list_groups_response::ListGroupsResponse;
    let listed: ListGroupsResponse = w.call(4, &ListGroupsRequest::default()).await?;
    let listed = listed
        .groups
        .iter()
        .find(|g| g.group_id.as_str() == group)
        .map(|g| g.group_state.to_string());
    let mut req = DescribeGroupsRequest::default();
    req.groups = vec![GroupId(s(group))];
    let described: DescribeGroupsResponse = w.call(5, &req).await?;
    let d = &described.groups[0];
    Ok((listed, d.error_code, d.group_state.to_string()))
}

/// DeleteGroups v2 of one group: its error code.
async fn delete_group(w: &mut Wire, group: &str) -> Result<i16, Box<dyn std::error::Error>> {
    use kafka_protocol::messages::delete_groups_request::DeleteGroupsRequest;
    use kafka_protocol::messages::delete_groups_response::DeleteGroupsResponse;
    let mut req = DeleteGroupsRequest::default();
    req.groups_names = vec![GroupId(s(group))];
    let resp: DeleteGroupsResponse = w.call(2, &req).await?;
    Ok(resp.results[0].error_code)
}

/// A simple-consumer OffsetCommit v8 (generation -1) of one partition.
async fn commit_offset(
    w: &mut Wire,
    group: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<i16, Box<dyn std::error::Error>> {
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_commit_response::OffsetCommitResponse;
    let mut p = OffsetCommitRequestPartition::default();
    p.partition_index = partition;
    p.committed_offset = offset;
    let mut t = OffsetCommitRequestTopic::default();
    t.name = TopicName(s(topic));
    t.partitions = vec![p];
    let mut req = OffsetCommitRequest::default();
    req.group_id = GroupId(s(group));
    req.generation_id_or_member_epoch = -1;
    req.topics = vec![t];
    let resp: OffsetCommitResponse = w.call(8, &req).await?;
    Ok(resp.topics[0].partitions[0].error_code)
}

/// DeleteGroups of a group with a pending transactional offset commit, as Kafka 3.7
/// answers it: a TxnOffsetCommit makes the group exist (Empty, in ListGroups and
/// DescribeGroups) even without a committed offset, so DeleteGroups deletes it (NONE),
/// and with it the pending offsets: the transaction's commit then leaves the deleted
/// group without offsets (OffsetFetch -1), with or without an earlier committed offset.
pub async fn test_delete_groups_drops_pending_txn_offsets() -> TestResult {
    println!("=== Test: DeleteGroups drops the group's pending transactional offsets ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-delete-pending", 2).await?;
    let mut w = Wire::connect().await?;

    for committed_before in [false, true] {
        let id = format!("txn-delete-pending-{}", Uuid::new_v4());
        let group = format!("txn-delete-pending-g-{}", Uuid::new_v4());
        if committed_before {
            assert_eq!(
                commit_offset(&mut w, &group, &t, 1, 1).await?,
                error_codes::NONE
            );
        }
        let (err, pid, epoch) = w.init_producer_id(Some(&id), 60_000).await?;
        assert_eq!(err, error_codes::NONE);
        assert_eq!(
            w.add_offsets(&id, pid, epoch, &group).await?,
            error_codes::NONE
        );
        assert_eq!(
            w.txn_offset_commit(&id, pid, epoch, &group, &t, 0, 5)
                .await?,
            error_codes::NONE
        );
        println!("Step: committed_before={committed_before}: the group with a pending offset...");
        let (listed, described_err, described) = group_state(&mut w, &group).await?;
        assert_eq!(
            (listed.as_deref(), described_err, described.as_str()),
            (Some("Empty"), error_codes::NONE, "Empty"),
            "a group with a pending transactional offset exists, Empty, as in Kafka 3.7"
        );
        assert_eq!(
            delete_group(&mut w, &group).await?,
            error_codes::NONE,
            "DeleteGroups deletes it"
        );
        let pending: i64 = ctx
            .db()
            .query_one(
                "SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE group_id = $1",
                &[&group],
            )
            .await?
            .get(0);
        assert_eq!(pending, 0, "the group's pending offsets go with it");
        assert_eq!(w.end_txn(&id, pid, epoch, true).await?, error_codes::NONE);
        let (err, parts) = w.offset_fetch(&group, &t, &[0, 1], true).await?;
        assert_eq!(err, error_codes::NONE);
        assert_eq!(
            parts,
            vec![(0, -1, error_codes::NONE), (1, -1, error_codes::NONE)],
            "the commit leaves the deleted group without offsets"
        );
        let (listed, _, _) = group_state(&mut w, &group).await?;
        assert_eq!(listed, None, "the deleted group is not listed");
        println!("✅ Deleted with its pending offset; nothing committed for it\n");
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
