//! The error codes of the transaction coordinator APIs (InitProducerId, AddPartitionsToTxn,
//! AddOffsetsToTxn, EndTxn, TxnOffsetCommit) per request version, as Kafka 3.7 answers
//! them: a fenced producer epoch is PRODUCER_FENCED only from the version KIP-588 added it
//! in and INVALID_PRODUCER_EPOCH before, and an unknown transactional id is
//! INVALID_PRODUCER_ID_MAPPING.

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
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
use kafka_protocol::messages::{GroupId, ProducerId, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
const PRODUCER_FENCED: i16 = 90;

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

/// The requests of one transactional id, at the versions the test picks.
struct Txn<'a> {
    w: &'a mut Wire,
    id: String,
}

impl Txn<'_> {
    async fn init(&mut self, version: i16, presented: (i64, i16)) -> DynResult<(i16, i64, i16)> {
        let mut req = InitProducerIdRequest::default();
        req.transactional_id = Some(TransactionalId(s(&self.id)));
        req.transaction_timeout_ms = 60_000;
        req.producer_id = ProducerId(presented.0);
        req.producer_epoch = presented.1;
        let r: InitProducerIdResponse = self.w.call(version, &req).await?;
        Ok((r.error_code, r.producer_id.0, r.producer_epoch))
    }

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

/// A transactional id initialised twice: its producer id and current epoch (1).
async fn bumped(w: &mut Wire, prefix: &str) -> DynResult<(String, i64, i16)> {
    let id = format!("{prefix}-{}", Uuid::new_v4());
    w.init_producer_id(Some(&id), 60_000).await?;
    let (err, pid, epoch) = w.init_producer_id(Some(&id), 60_000).await?;
    assert_eq!((err, epoch), (error_codes::NONE, 1));
    Ok((id, pid, epoch))
}

/// KIP-588: Kafka 3.7 answers a fenced epoch PRODUCER_FENCED on AddPartitionsToTxn,
/// AddOffsetsToTxn and EndTxn from v2 and on InitProducerId from v4, and
/// INVALID_PRODUCER_EPOCH on the versions before; TxnOffsetCommit answers
/// INVALID_PRODUCER_EPOCH on every version. A fenced request changes nothing: the owner's
/// transaction still commits.
pub async fn test_fenced_epoch_code_follows_the_request_version() -> TestResult {
    println!("=== Test: a fenced epoch's error code follows the request version ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-kip588", 1).await?;
    let group = ctx.unique_group("txn-kip588").await;
    let mut w = Wire::connect().await?;
    let fenced = |version: i16, since: i16| {
        if version < since {
            error_codes::INVALID_PRODUCER_EPOCH
        } else {
            PRODUCER_FENCED
        }
    };

    for version in 0..=3 {
        println!("Step: v{version}");
        for zombie in [-1, 1] {
            let (id, pid, epoch) = bumped(&mut w, "kip588").await?;
            let mut txn = Txn { w: &mut w, id };
            let owner = (pid, epoch);
            let bad = (pid, epoch + zombie);
            assert_eq!(
                txn.add_partitions(version, bad, &t).await?,
                fenced(version, 2),
                "AddPartitionsToTxn v{version} epoch {}",
                bad.1
            );
            assert_eq!(
                txn.add_offsets(version, bad, &group).await?,
                fenced(version, 2),
                "AddOffsetsToTxn v{version} epoch {}",
                bad.1
            );
            assert_eq!(txn.add_partitions(1, owner, &t).await?, error_codes::NONE);
            assert_eq!(txn.add_offsets(1, owner, &group).await?, error_codes::NONE);
            assert_eq!(
                txn.commit_offset(version, bad, &group, &t).await?,
                error_codes::INVALID_PRODUCER_EPOCH,
                "TxnOffsetCommit v{version} epoch {}",
                bad.1
            );
            assert_eq!(
                txn.end(version, bad, true).await?,
                fenced(version, 2),
                "EndTxn v{version} epoch {}",
                bad.1
            );
            assert_eq!(
                txn.end(version, owner, true).await?,
                error_codes::NONE,
                "the owner's transaction is untouched"
            );
        }
    }
    println!("✅ AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit\n");

    for version in [3, 4] {
        let (id, pid, epoch) = bumped(&mut w, "kip588-init").await?;
        let mut txn = Txn { w: &mut w, id };
        for presented in [(pid, epoch - 1), (pid, epoch + 1), (pid + 999, epoch)] {
            assert_eq!(
                txn.init(version, presented).await?,
                (fenced(version, 4), -1, -1),
                "InitProducerId v{version} presenting {presented:?}"
            );
        }
        assert_eq!(
            txn.init(version, (pid, epoch)).await?,
            (error_codes::NONE, pid, epoch + 1),
            "the owner is still bumped"
        );
    }
    println!("✅ InitProducerId v3 / v4\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
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
