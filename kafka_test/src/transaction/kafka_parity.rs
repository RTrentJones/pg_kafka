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
