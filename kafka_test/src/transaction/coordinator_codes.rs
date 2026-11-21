//! The error codes of the transaction coordinator APIs (AddPartitionsToTxn,
//! AddOffsetsToTxn, EndTxn, TxnOffsetCommit) per request version, as Kafka 3.7 answers
//! them: an unknown transactional id is INVALID_PRODUCER_ID_MAPPING.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::Wire;
use kafka_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafka_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use kafka_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
};
use kafka_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
use kafka_protocol::messages::end_txn_request::EndTxnRequest;
use kafka_protocol::messages::end_txn_response::EndTxnResponse;
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
use kafka_protocol::messages::{GroupId, ProducerId, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

const INVALID_PRODUCER_ID_MAPPING: i16 = 49;

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

/// The requests of one transactional id, at the versions the test picks.
struct Txn<'a> {
    w: &'a mut Wire,
    id: String,
}

impl Txn<'_> {
    async fn add_partitions(
        &mut self,
        version: i16,
        (pid, epoch): (i64, i16),
        topic: &str,
    ) -> DynResult<i16> {
        let mut t = AddPartitionsToTxnTopic::default();
        t.name = TopicName(s(topic));
        t.partitions = vec![0];
        let mut req = AddPartitionsToTxnRequest::default();
        req.v3_and_below_transactional_id = TransactionalId(s(&self.id));
        req.v3_and_below_producer_id = ProducerId(pid);
        req.v3_and_below_producer_epoch = epoch;
        req.v3_and_below_topics = vec![t];
        let r: AddPartitionsToTxnResponse = self.w.call(version, &req).await?;
        Ok(r.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code)
    }

    async fn add_offsets(
        &mut self,
        version: i16,
        (pid, epoch): (i64, i16),
        group: &str,
    ) -> DynResult<i16> {
        let mut req = AddOffsetsToTxnRequest::default();
        req.transactional_id = TransactionalId(s(&self.id));
        req.producer_id = ProducerId(pid);
        req.producer_epoch = epoch;
        req.group_id = GroupId(s(group));
        let r: AddOffsetsToTxnResponse = self.w.call(version, &req).await?;
        Ok(r.error_code)
    }

    async fn end(
        &mut self,
        version: i16,
        (pid, epoch): (i64, i16),
        commit: bool,
    ) -> DynResult<i16> {
        let mut req = EndTxnRequest::default();
        req.transactional_id = TransactionalId(s(&self.id));
        req.producer_id = ProducerId(pid);
        req.producer_epoch = epoch;
        req.committed = commit;
        let r: EndTxnResponse = self.w.call(version, &req).await?;
        Ok(r.error_code)
    }

    async fn commit_offset(
        &mut self,
        version: i16,
        (pid, epoch): (i64, i16),
        group: &str,
        topic: &str,
    ) -> DynResult<i16> {
        let mut p = TxnOffsetCommitRequestPartition::default();
        p.partition_index = 0;
        p.committed_offset = 1;
        let mut t = TxnOffsetCommitRequestTopic::default();
        t.name = TopicName(s(topic));
        t.partitions = vec![p];
        let mut req = TxnOffsetCommitRequest::default();
        req.transactional_id = TransactionalId(s(&self.id));
        req.group_id = GroupId(s(group));
        req.producer_id = ProducerId(pid);
        req.producer_epoch = epoch;
        req.topics = vec![t];
        let r: TxnOffsetCommitResponse = self.w.call(version, &req).await?;
        Ok(r.topics[0].partitions[0].error_code)
    }
}

/// Kafka 3.7 answers a transactional id that has no state INVALID_PRODUCER_ID_MAPPING on
/// AddPartitionsToTxn, AddOffsetsToTxn, EndTxn and TxnOffsetCommit, on every version.
pub async fn test_unknown_transactional_id_is_invalid_producer_id_mapping() -> TestResult {
    println!("=== Test: an unknown transactional id is INVALID_PRODUCER_ID_MAPPING ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-unknown-id", 1).await?;
    let group = ctx.unique_group("txn-unknown-id").await;
    let mut w = Wire::connect().await?;
    let (err, pid, epoch) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let producer = (pid, epoch);
    for version in 0..=3 {
        let mut txn = Txn {
            w: &mut w,
            id: format!("unknown-txnid-{}", Uuid::new_v4()),
        };
        assert_eq!(
            txn.add_partitions(version, producer, &t).await?,
            INVALID_PRODUCER_ID_MAPPING,
            "AddPartitionsToTxn v{version}"
        );
        assert_eq!(
            txn.add_offsets(version, producer, &group).await?,
            INVALID_PRODUCER_ID_MAPPING,
            "AddOffsetsToTxn v{version}"
        );
        assert_eq!(
            txn.end(version, producer, true).await?,
            INVALID_PRODUCER_ID_MAPPING,
            "EndTxn v{version}"
        );
        assert_eq!(
            txn.commit_offset(version, producer, &group, &t).await?,
            INVALID_PRODUCER_ID_MAPPING,
            "TxnOffsetCommit v{version}"
        );
        println!("✅ v{version}");
    }
    let created: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.transactions WHERE transactional_id LIKE 'unknown-txnid-%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(created, 0, "no transactional id is created");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
