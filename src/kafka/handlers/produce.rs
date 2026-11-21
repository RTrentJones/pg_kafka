//! Produce: writes records to their partitions, with key routing for partition -1,
//! idempotent sequence checks and transactional writes.

use super::helpers::validate_ongoing_transaction;
use crate::kafka::constants::{
    ERROR_INVALID_PRODUCER_EPOCH, ERROR_INVALID_PRODUCER_ID_MAPPING, ERROR_INVALID_TXN_STATE,
    ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::{ProducerMetadata, Record, RefusedPartition, TopicProduceData};
use crate::kafka::partitioner::compute_partition;
use crate::kafka::response_builders::build_produce_partition_error_response;
use crate::kafka::storage::{
    batch_last_sequence, KafkaStore, LogStore, ProducerBatch, SequenceCheck, TopicStore,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::TopicName;
use std::collections::HashMap;

/// Handle Produce request
///
/// Inserts records into storage and returns base offsets for each partition.
/// When partition_index == -1, uses key-based partition routing.
///
/// For transactional producers:
/// - Records are inserted with txn_state='pending' (invisible to read_committed consumers)
/// - Records become visible when the transaction commits
/// - Records are marked 'aborted' if the transaction aborts
///
/// As in Kafka 3.7 (`ReplicaManager.appendRecords` verifies the transactional batches of a
/// request with the transaction coordinator), only a partition whose batch is transactional
/// is checked against the transaction, on its own: its producer must own the transactional
/// id (INVALID_PRODUCER_ID_MAPPING otherwise, for an unknown id too; INVALID_PRODUCER_EPOCH
/// for another epoch), the transaction must be Ongoing and hold the partition
/// (INVALID_TXN_STATE). A plain or idempotent batch in the same request is written as it is
/// without a transactional id.
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `topic_data` - Topics and records to produce
/// * `producer_metadata` - Request-level producer metadata (idempotent and transactional producers)
/// * `transactional_id` - Transactional ID for transactional producers
pub fn handle_produce(
    ctx: &HandlerContext,
    topic_data: Vec<TopicProduceData>,
    producer_metadata: Option<&ProducerMetadata>,
    transactional_id: Option<&str>,
) -> Result<ProduceResponse> {
    let mut transaction = TransactionCheck {
        transactional_id,
        owners: HashMap::new(),
    };
    let mut kafka_response = crate::kafka::build_produce_response();
    kafka_response.responses = topic_data
        .into_iter()
        .map(|topic| produce_topic(ctx, topic, producer_metadata, &mut transaction))
        .collect();
    Ok(kafka_response)
}

/// The transaction of a request's transactional batches, checked once per producer id and
/// epoch.
struct TransactionCheck<'a> {
    transactional_id: Option<&'a str>,
    /// Per producer id and epoch: the error that refuses its transactional batches, if any.
    owners: HashMap<(i64, i16), Option<i16>>,
}

impl<'a> TransactionCheck<'a> {
    /// The transactional id a partition's batch is written under: the request's, when the
    /// batch is transactional.
    fn of(&self, meta: Option<&ProducerMetadata>) -> Option<&'a str> {
        self.transactional_id
            .filter(|_| meta.is_some_and(|m| m.is_transactional))
    }

    /// Why the transaction of `meta`'s producer refuses its batches, if it does: the
    /// producer does not own `txn_id`, or its transaction is not Ongoing.
    fn refusal(
        &mut self,
        store: &dyn KafkaStore,
        txn_id: &str,
        meta: &ProducerMetadata,
    ) -> Option<i16> {
        *self
            .owners
            .entry((meta.producer_id, meta.producer_epoch))
            .or_insert_with(|| {
                validate_ongoing_transaction(
                    store,
                    txn_id,
                    meta.producer_id,
                    meta.producer_epoch,
                    "Produce",
                )
                .err()
                .map(|e| match e {
                    // Kafka's coordinator answers an unknown transactional id as one the
                    // producer does not own.
                    KafkaError::TransactionalIdNotFound { .. } => ERROR_INVALID_PRODUCER_ID_MAPPING,
                    e => produce_error_code(&e),
                })
            })
    }
}

/// Answer the partitions whose records failed validation, by topic: as Kafka 3.7 does
/// (`KafkaApis.handleProduceRequest`), a partition of a topic that does not exist, or
/// outside its topic, is UNKNOWN_TOPIC_OR_PARTITION whatever its data; any other gets the
/// error validation found. Looks the topics up without creating them, in one statement.
/// Topics are answered in the order they are first named, in time linear in the refused
/// partitions (a request may name hundreds of thousands of topics).
pub fn answer_refused_partitions<S: TopicStore + ?Sized>(
    store: &S,
    refused: &[RefusedPartition],
) -> Result<Vec<TopicProduceResponse>> {
    if refused.is_empty() {
        return Ok(Vec::new());
    }
    let mut topic_at: HashMap<&str, usize> = HashMap::new();
    let mut by_topic: Vec<(&str, Vec<&RefusedPartition>)> = Vec::new();
    for partition in refused {
        let at = *topic_at.entry(partition.topic.as_str()).or_insert_with(|| {
            by_topic.push((partition.topic.as_str(), Vec::new()));
            by_topic.len() - 1
        });
        by_topic[at].1.push(partition);
    }
    let names: Vec<String> = by_topic
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect();
    let partition_counts: HashMap<String, i32> = store
        .get_topic_metadata(Some(&names))?
        .into_iter()
        .map(|t| (t.name, t.partition_count))
        .collect();
    Ok(by_topic
        .into_iter()
        .map(|(name, partitions)| {
            let count = partition_counts.get(name).copied();
            let mut topic = TopicProduceResponse::default();
            topic.partition_responses = partitions
                .into_iter()
                .map(|p| {
                    let exists = count.is_some_and(|count| (0..count).contains(&p.partition));
                    failed_partition(
                        p.partition,
                        if exists {
                            p.error_code
                        } else {
                            ERROR_UNKNOWN_TOPIC_OR_PARTITION
                        },
                    )
                })
                .collect();
            topic.name = TopicName(name.to_string().into());
            topic
        })
        .collect())
}

/// Write one topic's records and answer for its partitions. A failure to resolve (or
/// create) the topic is reported on this topic's partitions only.
fn produce_topic(
    ctx: &HandlerContext,
    topic: TopicProduceData,
    producer_metadata: Option<&ProducerMetadata>,
    transaction: &mut TransactionCheck<'_>,
) -> TopicProduceResponse {
    let store = ctx.store;
    // Topic id and partition count in one query.
    let (topic_id, partition_count) =
        match super::helpers::get_or_create_valid_topic(store, &topic.name, ctx.default_partitions)
        {
            Ok(found) => found,
            Err(e) => {
                if e.is_server_error() {
                    crate::pg_warning!(
                        "Failed to resolve topic '{}' for produce: {}",
                        topic.name,
                        e
                    );
                }
                return build_produce_partition_error_response(
                    std::slice::from_ref(&topic),
                    e.to_kafka_error_code(),
                )
                .responses
                .remove(0);
            }
        };

    let mut topic_response = TopicProduceResponse::default();
    topic_response.name = TopicName(topic.name.clone().into());
    let target = TargetTopic {
        id: topic_id,
        partition_count,
    };
    let (by_partition, rejected) = group_by_partition(topic, partition_count, producer_metadata);
    for partition_index in rejected {
        topic_response.partition_responses.push(failed_partition(
            partition_index,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        ));
    }
    for (partition_index, (records, partition_meta)) in by_partition {
        topic_response.partition_responses.push(produce_partition(
            store,
            target,
            partition_index,
            &records,
            partition_meta.as_ref(),
            transaction,
        ));
    }
    topic_response
}

/// The topic a produce writes to.
#[derive(Clone, Copy)]
struct TargetTopic {
    id: i32,
    partition_count: i32,
}

/// One partition's records and its producer metadata.
type PartitionRecords = (Vec<Record>, Option<ProducerMetadata>);

/// Group a topic's records by target partition, keeping each partition's own producer
/// metadata: every RecordBatch carries its own base_sequence, so partitions must not
/// share one request-level sequence number. Records sent to partition -1 are routed by
/// key. Also returns the partitions that are refused before grouping (an idempotent
/// batch sent to -1).
fn group_by_partition(
    topic: TopicProduceData,
    partition_count: i32,
    producer_metadata: Option<&ProducerMetadata>,
) -> (HashMap<i32, PartitionRecords>, Vec<i32>) {
    let mut by_partition: HashMap<i32, PartitionRecords> = HashMap::new();
    let mut rejected = Vec::new();
    for partition_data in topic.partitions {
        let explicit_partition = partition_data.partition_index;
        // Per-partition metadata; fall back to the request-level metadata only when the
        // batch carried none (e.g. a legacy MessageSet).
        let partition_meta = partition_data
            .producer_metadata
            .or_else(|| producer_metadata.cloned());
        let is_idempotent = partition_meta.as_ref().is_some_and(|m| m.producer_id >= 0);

        if explicit_partition == -1 {
            // An idempotent batch's sequence numbers are tracked per partition. Splitting
            // one batch across several partitions by key would record the whole batch's
            // sequence range on each of them and break the producer's next batch, so
            // server-side routing is refused for it.
            if is_idempotent {
                rejected.push(explicit_partition);
                continue;
            }
            for record in partition_data.records {
                let target = compute_partition(record.key.as_deref(), partition_count, -1);
                by_partition
                    .entry(target)
                    .or_insert_with(|| (Vec::new(), None))
                    .0
                    .push(record);
            }
        } else {
            let entry = by_partition
                .entry(explicit_partition)
                .or_insert_with(|| (Vec::new(), None));
            entry.0.extend(partition_data.records);
            if entry.1.is_none() {
                entry.1 = partition_meta;
            }
        }
    }
    (by_partition, rejected)
}

/// Write one partition's records and answer for it.
fn produce_partition(
    store: &dyn KafkaStore,
    topic: TargetTopic,
    partition_index: i32,
    records: &[Record],
    partition_meta: Option<&ProducerMetadata>,
    transaction: &mut TransactionCheck<'_>,
) -> PartitionProduceResponse {
    let topic_id = topic.id;
    let transactional_id = transaction.of(partition_meta);
    if let Some(error_code) =
        partition_refusal(store, topic, partition_index, partition_meta, transaction)
    {
        return failed_partition(partition_index, error_code);
    }

    // Sequence metadata for this partition (idempotent producers only).
    let seq_meta = partition_meta.filter(|m| m.producer_id >= 0);
    let record_count = i32::try_from(records.len())
        .expect("a request of at most MAX_REQUEST_SIZE bytes holds fewer than i32::MAX records");

    // Validate an idempotent producer's sequence before inserting.
    if let Some(meta) = seq_meta {
        match store.check_and_update_sequence(
            meta.producer_id,
            meta.producer_epoch,
            topic_id,
            partition_index,
            meta.base_sequence,
            record_count,
        ) {
            Ok(SequenceCheck::Append) => {}
            // A retry of an appended batch: success with the original base offset and
            // nothing inserted (Kafka's idempotent producer semantics).
            Ok(SequenceCheck::Duplicate { base_offset }) => {
                return appended_partition(store, topic_id, partition_index, base_offset);
            }
            Err(e) => return failed_partition(partition_index, produce_error_code(&e)),
        }
    }

    let base_offset = match insert_partition_records(
        store,
        topic_id,
        partition_index,
        records,
        partition_meta.filter(|_| transactional_id.is_some()),
    ) {
        Ok(base_offset) => base_offset,
        Err(error_code) => return failed_partition(partition_index, error_code),
    };

    // The producer's sequence advances only after the insert succeeded, so a failed
    // insert leaves it in place and the retry is inserted instead of being dropped as a
    // false duplicate.
    if let Some(meta) = seq_meta {
        record_appended_batch(
            store,
            meta,
            topic_id,
            partition_index,
            record_count,
            base_offset,
        );
    }
    appended_partition(store, topic_id, partition_index, base_offset)
}

/// Why a partition cannot be written, if it cannot: it is outside the topic, or its batch
/// is transactional and the transaction refuses it ([`TransactionCheck::refusal`]) or does
/// not hold the partition (AddPartitionsToTxn), which Kafka answers INVALID_TXN_STATE.
fn partition_refusal(
    store: &dyn KafkaStore,
    topic: TargetTopic,
    partition_index: i32,
    partition_meta: Option<&ProducerMetadata>,
    transaction: &mut TransactionCheck<'_>,
) -> Option<i16> {
    if partition_index < 0 || partition_index >= topic.partition_count {
        return Some(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
    let txn_id = transaction.of(partition_meta)?;
    let meta = partition_meta?;
    if let Some(code) = transaction.refusal(store, txn_id, meta) {
        return Some(code);
    }
    match store.txn_has_partition(txn_id, topic.id, partition_index) {
        Ok(true) => None,
        Ok(false) => Some(ERROR_INVALID_TXN_STATE),
        Err(e) => Some(e.to_kafka_error_code()),
    }
}

/// Insert the records (pending in the transaction of `transactional`, the metadata of a
/// transactional batch): their base offset, or the partition's error code.
fn insert_partition_records(
    store: &dyn KafkaStore,
    topic_id: i32,
    partition_index: i32,
    records: &[Record],
    transactional: Option<&ProducerMetadata>,
) -> std::result::Result<i64, i16> {
    let inserted = match transactional {
        Some(meta) => store.insert_transactional_records(
            topic_id,
            partition_index,
            records,
            meta.producer_id,
            meta.producer_epoch,
        ),
        None => store.insert_records(topic_id, partition_index, records),
    };
    inserted.map_err(|e| {
        if e.is_server_error() {
            crate::pg_warning!(
                "Failed to insert records for topic_id={}, partition={}: {}",
                topic_id,
                partition_index,
                e
            );
        }
        e.to_kafka_error_code()
    })
}

/// Remember an appended idempotent batch with its offset, so a retry of it is answered
/// with that offset. The records are already written, so a failure only logs.
fn record_appended_batch(
    store: &dyn KafkaStore,
    meta: &ProducerMetadata,
    topic_id: i32,
    partition_index: i32,
    record_count: i32,
    base_offset: i64,
) {
    if let Err(e) = store.record_producer_batch(ProducerBatch {
        producer_id: meta.producer_id,
        topic_id,
        partition_id: partition_index,
        first_sequence: meta.base_sequence,
        last_sequence: batch_last_sequence(meta.base_sequence, record_count),
        base_offset,
    }) {
        crate::pg_warning!(
            "Failed to record producer sequence (topic_id={}, partition={}): {}",
            topic_id,
            partition_index,
            e
        );
    }
}

/// A partition answer for records stored at `base_offset`.
fn appended_partition(
    store: &dyn KafkaStore,
    topic_id: i32,
    partition_index: i32,
    base_offset: i64,
) -> PartitionProduceResponse {
    let mut response = PartitionProduceResponse::default();
    response.index = partition_index;
    response.error_code = ERROR_NONE;
    response.base_offset = base_offset;
    response.log_append_time_ms = -1;
    response.log_start_offset = reported_log_start_offset(store, topic_id, partition_index);
    response
}

/// A partition answer for records that were not stored.
fn failed_partition(partition_index: i32, error_code: i16) -> PartitionProduceResponse {
    let mut response = PartitionProduceResponse::default();
    response.index = partition_index;
    response.error_code = error_code;
    response.base_offset = -1;
    response.log_append_time_ms = -1;
    response.log_start_offset = -1;
    response
}

/// The Produce error code for `e`. A stale epoch is INVALID_PRODUCER_EPOCH (47) on
/// Produce in Kafka 3.7, for idempotent and transactional producers alike (since
/// Kafka 2.7 PRODUCER_FENCED is no longer sent in a produce response), so a fenced
/// transactional epoch maps to it too.
fn produce_error_code(e: &KafkaError) -> i16 {
    match e {
        KafkaError::ProducerFenced { .. } => ERROR_INVALID_PRODUCER_EPOCH,
        _ => e.to_kafka_error_code(),
    }
}

/// The log start offset a successful partition response reports, as Kafka does. The
/// records are already written, so a failed lookup does not fail the produce: the
/// field is then -1, the value Kafka sends when it has none.
fn reported_log_start_offset(store: &dyn LogStore, topic_id: i32, partition_id: i32) -> i64 {
    store
        .get_log_start_offset(topic_id, partition_id)
        .unwrap_or_else(|e| {
            crate::pg_warning!(
                "Failed to read the log start offset (topic_id={}, partition={}): {}",
                topic_id,
                partition_id,
                e
            );
            -1
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::error::KafkaError;
    use crate::kafka::messages::{PartitionProduceData, Record, TopicProduceData};
    use crate::kafka::storage::TransactionState;
    use crate::testing::mocks::MockKafkaStore;
    use crate::testing::TestEnv;

    fn request(partitions: &[i32]) -> Vec<TopicProduceData> {
        vec![TopicProduceData {
            name: "t".to_string(),
            partitions: partitions
                .iter()
                .map(|&p| PartitionProduceData {
                    partition_index: p,
                    records: vec![Record {
                        key: None,
                        value: Some(b"v".to_vec()),
                        headers: vec![],
                        timestamp: None,
                    }],
                    producer_metadata: Some(meta()),
                })
                .collect(),
        }]
    }

    fn meta() -> ProducerMetadata {
        ProducerMetadata {
            producer_id: 7,
            producer_epoch: 2,
            base_sequence: 0,
            is_transactional: true,
        }
    }

    fn run(mock: &MockKafkaStore, partitions: &[i32], txn: Option<&str>) -> Vec<(i32, i16, i64)> {
        let env = TestEnv::new();
        let ctx = env.ctx(mock);
        let mut out: Vec<(i32, i16, i64)> =
            handle_produce(&ctx, request(partitions), Some(&meta()), txn)
                .unwrap()
                .responses
                .iter()
                .flat_map(|t| t.partition_responses.iter())
                .map(|p| (p.index, p.error_code, p.base_offset))
                .collect();
        out.sort_unstable();
        out
    }

    /// Kafka: a transactional Produce to a partition the transaction never added
    /// (AddPartitionsToTxn) is INVALID_TXN_STATE, and nothing is written.
    #[test]
    fn transactional_produce_to_an_unregistered_partition_is_refused() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_validate_transaction()
            .returning(|_, _, _| Ok(()));
        mock.expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::Ongoing)));
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 2)));
        mock.expect_txn_has_partition()
            .returning(|_, _, p| Ok(p == 0));
        mock.expect_check_and_update_sequence()
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(SequenceCheck::Append));
        mock.expect_insert_transactional_records()
            .times(1)
            .returning(|_, _, _, _, _| Ok(11));
        mock.expect_record_producer_batch()
            .times(1)
            .returning(|_| Ok(()));
        assert_eq!(
            run(&mock, &[0, 1], Some("tx")),
            vec![(0, ERROR_NONE, 11), (1, ERROR_INVALID_TXN_STATE, -1)]
        );
    }

    /// Kafka reports the partition's log start offset on a successful produce, for an
    /// appended batch and for a retried duplicate alike.
    #[test]
    fn successful_produce_reports_the_log_start_offset() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 2)));
        mock.expect_check_and_update_sequence()
            .returning(|_, _, _, partition, _, _| {
                Ok(if partition == 0 {
                    SequenceCheck::Append
                } else {
                    SequenceCheck::Duplicate { base_offset: 3 }
                })
            });
        mock.expect_insert_records().returning(|_, _, _| Ok(11));
        mock.expect_record_producer_batch().returning(|_| Ok(()));
        mock.expect_get_log_start_offset()
            .returning(|_, partition| Ok(5 + i64::from(partition)));
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let response = handle_produce(&ctx, request(&[0, 1]), Some(&meta()), None).unwrap();
        let mut got: Vec<(i32, i16, i64, i64)> = response.responses[0]
            .partition_responses
            .iter()
            .map(|p| (p.index, p.error_code, p.base_offset, p.log_start_offset))
            .collect();
        got.sort_unstable();
        assert_eq!(got, vec![(0, ERROR_NONE, 11, 5), (1, ERROR_NONE, 3, 6)]);
    }

    /// A failed log-start lookup after the records are written leaves the produce
    /// successful and reports -1.
    #[test]
    fn log_start_lookup_failure_keeps_the_produce_successful() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));
        mock.expect_check_and_update_sequence()
            .returning(|_, _, _, _, _, _| Ok(SequenceCheck::Append));
        mock.expect_insert_records().returning(|_, _, _| Ok(4));
        mock.expect_record_producer_batch().returning(|_| Ok(()));
        mock.expect_get_log_start_offset()
            .returning(|_, _| Err(KafkaError::Internal("boom".into())));
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let response = handle_produce(&ctx, request(&[0]), Some(&meta()), None).unwrap();
        let p = &response.responses[0].partition_responses[0];
        assert_eq!(
            (p.error_code, p.base_offset, p.log_start_offset),
            (ERROR_NONE, 4, -1)
        );
    }

    /// A refused partition is UNKNOWN_TOPIC_OR_PARTITION when its topic does not exist or
    /// it is outside its topic, as in Kafka 3.7, and otherwise gets its validation error;
    /// the topics are looked up, never created, in one call.
    #[test]
    fn refused_partitions_of_unknown_topics_are_unknown_topic_or_partition() {
        use crate::kafka::constants::ERROR_CORRUPT_MESSAGE;
        use crate::kafka::storage::TopicMetadata;
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata()
            .times(1)
            .returning(|names| {
                assert_eq!(names, Some(&["t".to_string(), "gone".to_string()][..]));
                Ok(vec![TopicMetadata {
                    name: "t".to_string(),
                    id: 1,
                    partition_count: 2,
                }])
            });
        let refused = |topic: &str, partition| RefusedPartition {
            topic: topic.to_string(),
            partition,
            error_code: ERROR_CORRUPT_MESSAGE,
        };
        let answered = answer_refused_partitions(
            &mock,
            &[
                refused("t", 1),
                refused("gone", 0),
                refused("t", 2),
                refused("t", -1),
            ],
        )
        .unwrap();
        let got: Vec<(String, i32, i16)> = answered
            .iter()
            .flat_map(|t| {
                t.partition_responses
                    .iter()
                    .map(|p| (t.name.to_string(), p.index, p.error_code))
            })
            .collect();
        let t = |p, code| ("t".to_string(), p, code);
        assert_eq!(
            got,
            vec![
                t(1, ERROR_CORRUPT_MESSAGE),
                t(2, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                t(-1, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                ("gone".to_string(), 0, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            ]
        );
        assert_eq!(
            answer_refused_partitions(&MockKafkaStore::new(), &[])
                .unwrap()
                .len(),
            0
        );
    }

    /// Answering refused partitions takes time linear in them: a request of tens of
    /// thousands of topics of one refused partition each (a few hundred KB on the wire) is
    /// answered on the DB thread at once, not in seconds.
    #[test]
    fn refused_partitions_are_answered_in_linear_time() {
        use crate::kafka::constants::ERROR_CORRUPT_MESSAGE;
        use crate::kafka::storage::TopicMetadata;
        let mut mock = MockKafkaStore::new();
        mock.expect_get_topic_metadata().returning(|names| {
            Ok(names
                .unwrap_or_default()
                .iter()
                .map(|name| TopicMetadata {
                    name: name.clone(),
                    id: 1,
                    partition_count: 1,
                })
                .collect())
        });
        let refused = |n: usize| -> Vec<RefusedPartition> {
            (0..n)
                .map(|i| RefusedPartition {
                    topic: format!("q{i:07}"),
                    partition: 0,
                    error_code: ERROR_CORRUPT_MESSAGE,
                })
                .collect()
        };
        let growth = crate::testing::growth(8_000, |n| {
            let refused = refused(n);
            let answered = answer_refused_partitions(&mock, &refused).unwrap();
            assert_eq!(answered.len(), n);
        });
        assert!(growth < 8.0, "4x the topics took {growth:.1}x the time");
        let many = refused(60_000);
        let (answered, spent) =
            crate::testing::cpu_time(|| answer_refused_partitions(&mock, &many).unwrap());
        assert_eq!(answered.len(), 60_000);
        assert!(spent < std::time::Duration::from_millis(500), "{spent:?}");
    }

    /// Kafka 3.7 checks only the transactional batches of a transactional Produce against
    /// the transaction, once per producer, each partition on its own: an unknown
    /// transactional id (or another producer's) refuses them INVALID_PRODUCER_ID_MAPPING,
    /// and a plain batch beside them is written outside the transaction.
    #[test]
    fn only_transactional_batches_are_checked_against_the_transaction() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_log_start_offset().returning(|_, _| Ok(0));
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 3)));
        mock.expect_validate_transaction()
            .times(1)
            .returning(|id, _, _| Err(KafkaError::transactional_id_not_found(id)));
        mock.expect_insert_records().times(1).returning(|_, p, _| {
            assert_eq!(p, 2);
            Ok(4)
        });
        let mut data = request(&[0, 1, 2]);
        data[0].partitions[2].producer_metadata = Some(ProducerMetadata {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
        });
        let env = TestEnv::new();
        let ctx = env.ctx(&mock);
        let mut got: Vec<(i32, i16)> = handle_produce(&ctx, data, Some(&meta()), Some("tx"))
            .unwrap()
            .responses
            .iter()
            .flat_map(|t| t.partition_responses.iter())
            .map(|p| (p.index, p.error_code))
            .collect();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![
                (0, ERROR_INVALID_PRODUCER_ID_MAPPING),
                (1, ERROR_INVALID_PRODUCER_ID_MAPPING),
                (2, ERROR_NONE)
            ]
        );

        // Another producer id owning the transactional id is the same answer.
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));
        mock.expect_validate_transaction()
            .returning(|_, pid, _| Err(KafkaError::invalid_producer_id_mapping(pid)));
        assert_eq!(
            run(&mock, &[0], Some("tx")),
            vec![(0, ERROR_INVALID_PRODUCER_ID_MAPPING, -1)]
        );
    }

    /// Kafka 3.7 answers a stale epoch on Produce with INVALID_PRODUCER_EPOCH (47),
    /// for transactional producers too (was PRODUCER_FENCED, 90).
    #[test]
    fn stale_epoch_is_invalid_producer_epoch() {
        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));
        mock.expect_validate_transaction()
            .returning(|_, _, _| Err(KafkaError::producer_fenced(7, 2, 3)));
        assert_eq!(
            run(&mock, &[0], Some("tx")),
            vec![(0, ERROR_INVALID_PRODUCER_EPOCH, -1)]
        );

        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));
        mock.expect_check_and_update_sequence()
            .returning(|_, _, _, _, _, _| {
                Err(KafkaError::Protocol {
                    code: ERROR_INVALID_PRODUCER_EPOCH,
                    message: "stale".into(),
                })
            });
        assert_eq!(
            run(&mock, &[0], None),
            vec![(0, ERROR_INVALID_PRODUCER_EPOCH, -1)]
        );
    }
}
