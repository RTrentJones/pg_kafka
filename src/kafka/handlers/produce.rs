// Produce handler
//
// Handles ProduceRequest - writing messages to topics.
// Phase 7: Added server-side partition routing for partition_index == -1
// Phase 9: Added idempotent producer sequence validation
// Phase 10: Added transactional mode support

use crate::kafka::constants::{ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::ProducerMetadata;
use crate::kafka::partitioner::compute_partition;
use crate::kafka::storage::{batch_last_sequence, ProducerBatch, SequenceCheck};
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
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `topic_data` - Topics and records to produce
/// * `producer_metadata` - Optional producer metadata for idempotent producers (Phase 9)
/// * `transactional_id` - Optional transactional ID for transactional producers (Phase 10)
pub fn handle_produce(
    ctx: &HandlerContext,
    topic_data: Vec<crate::kafka::messages::TopicProduceData>,
    producer_metadata: Option<&ProducerMetadata>,
    transactional_id: Option<&str>,
) -> Result<kafka_protocol::messages::produce_response::ProduceResponse> {
    let store = ctx.store;
    let default_partitions = ctx.default_partitions;
    // Phase 10: Validate transaction state if transactional. ProduceResponse has no
    // top-level error code, so a request-level failure (fenced epoch, unknown
    // transactional id, transaction not Ongoing) is reported on every requested
    // partition; an empty response would leave the client without the error.
    if let Some(txn_id) = transactional_id {
        if let Some(meta) = producer_metadata {
            if meta.producer_id >= 0 {
                if let Err(e) = validate_ongoing_transaction(store, txn_id, meta) {
                    return Ok(
                        crate::kafka::response_builders::build_produce_partition_error_response(
                            &topic_data,
                            produce_error_code(&e),
                        ),
                    );
                }
            }
        }
    }
    let mut kafka_response = crate::kafka::build_produce_response();

    // Process each topic
    for topic in topic_data {
        let topic_name: String = topic.name.clone();

        // Get or create topic (returns both topic_id and partition_count in single query).
        // A failure here is reported on this topic's partitions only.
        let (topic_id, partition_count) =
            match super::helpers::get_or_create_valid_topic(store, &topic_name, default_partitions)
            {
                Ok(found) => found,
                Err(e) => {
                    if e.is_server_error() {
                        crate::pg_warning!(
                            "Failed to resolve topic '{}' for produce: {}",
                            topic_name,
                            e
                        );
                    }
                    let code = e.to_kafka_error_code();
                    kafka_response.responses.push(
                        crate::kafka::response_builders::build_produce_partition_error_response(
                            std::slice::from_ref(&topic),
                            code,
                        )
                        .responses
                        .remove(0),
                    );
                    continue;
                }
            };

        // Build topic response
        let mut topic_response =
            kafka_protocol::messages::produce_response::TopicProduceResponse::default();
        topic_response.name = kafka_protocol::messages::TopicName(topic_name.clone().into());

        // Group records by computed partition, keeping each partition's own producer
        // metadata: every RecordBatch carries its own base_sequence, so partitions must
        // not share one request-level sequence number.
        // When partition_index == -1, use key-based routing.
        let mut records_by_partition: HashMap<
            i32,
            (
                Vec<crate::kafka::messages::Record>,
                Option<ProducerMetadata>,
            ),
        > = HashMap::new();
        // Error responses for partitions that cannot be grouped (see below).
        let mut rejected_partitions: Vec<i32> = Vec::new();

        for partition_data in topic.partitions {
            let explicit_partition = partition_data.partition_index;
            // Per-partition metadata; fall back to the request-level metadata only when
            // the batch carried none (e.g. a legacy MessageSet).
            let partition_meta = partition_data
                .producer_metadata
                .or_else(|| producer_metadata.cloned());
            let is_idempotent = partition_meta.as_ref().is_some_and(|m| m.producer_id >= 0);

            if explicit_partition == -1 {
                // An idempotent batch's sequence numbers are tracked per partition.
                // Splitting one batch across several partitions by key would record
                // the whole batch's sequence range on each of them and break the
                // producer's next batch, so server-side routing is refused here.
                if is_idempotent {
                    rejected_partitions.push(explicit_partition);
                    continue;
                }
                // Server-side partition routing based on key
                for record in partition_data.records {
                    let target_partition =
                        compute_partition(record.key.as_deref(), partition_count, -1);
                    records_by_partition
                        .entry(target_partition)
                        .or_insert_with(|| (Vec::new(), None))
                        .0
                        .push(record);
                }
            } else {
                // Explicit partition - use as-is
                let entry = records_by_partition
                    .entry(explicit_partition)
                    .or_insert_with(|| (Vec::new(), None));
                entry.0.extend(partition_data.records);
                if entry.1.is_none() {
                    entry.1 = partition_meta;
                }
            }
        }

        for partition_index in rejected_partitions {
            let mut partition_response =
                kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
            partition_response.index = partition_index;
            partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
            partition_response.base_offset = -1;
            partition_response.log_append_time_ms = -1;
            partition_response.log_start_offset = -1;
            topic_response.partition_responses.push(partition_response);
        }

        // Process each partition group
        for (partition_index, (records, partition_meta)) in records_by_partition {
            let mut partition_response =
                kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
            partition_response.index = partition_index;

            // Validate partition index against actual partition count
            if partition_index < 0 || partition_index >= partition_count {
                partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                partition_response.base_offset = -1;
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = -1;
                topic_response.partition_responses.push(partition_response);
                continue;
            }

            // Wave 3: a transactional producer may only write to partitions it added to
            // the transaction (AddPartitionsToTxn); Kafka answers INVALID_TXN_STATE.
            if let Some(txn_id) = transactional_id {
                let registered = store.txn_has_partition(txn_id, topic_id, partition_index);
                if !matches!(registered, Ok(true)) {
                    partition_response.error_code = match registered {
                        Err(e) => e.to_kafka_error_code(),
                        _ => crate::kafka::constants::ERROR_INVALID_TXN_STATE,
                    };
                    partition_response.base_offset = -1;
                    partition_response.log_append_time_ms = -1;
                    partition_response.log_start_offset = -1;
                    topic_response.partition_responses.push(partition_response);
                    continue;
                }
            }

            // Sequence metadata for this partition (idempotent producers only).
            let seq_meta = partition_meta.as_ref().filter(|m| m.producer_id >= 0);

            // Phase 9: Validate sequence for idempotent producers BEFORE inserting
            let check = if let Some(meta) = seq_meta {
                match store.check_and_update_sequence(
                    meta.producer_id,
                    meta.producer_epoch,
                    topic_id,
                    partition_index,
                    meta.base_sequence,
                    records.len() as i32,
                ) {
                    Ok(check) => check,
                    Err(e) => {
                        // Sequence validation failed - return error for this partition
                        partition_response.error_code = produce_error_code(&e);
                        partition_response.base_offset = -1;
                        partition_response.log_append_time_ms = -1;
                        partition_response.log_start_offset = -1;
                        topic_response.partition_responses.push(partition_response);
                        continue;
                    }
                }
            } else {
                SequenceCheck::Append // Non-idempotent producer - always insert
            };

            // Insert records (skip for duplicates)
            if check == SequenceCheck::Append {
                // Phase 10: Use transactional insert if transactional_id is set
                let insert_result = if transactional_id.is_some() {
                    if let Some(meta) = producer_metadata {
                        store.insert_transactional_records(
                            topic_id,
                            partition_index,
                            &records,
                            meta.producer_id,
                            meta.producer_epoch,
                        )
                    } else {
                        // Transactional mode requires producer metadata
                        Err(crate::kafka::error::KafkaError::Internal(
                            "Transactional produce requires producer metadata".to_string(),
                        ))
                    }
                } else {
                    store.insert_records(topic_id, partition_index, &records)
                };

                match insert_result {
                    Ok(base_offset) => {
                        partition_response.error_code = ERROR_NONE;
                        partition_response.base_offset = base_offset;
                        partition_response.log_append_time_ms = -1;
                        partition_response.log_start_offset =
                            reported_log_start_offset(store, topic_id, partition_index);

                        // RV-8: advance the idempotent producer's sequence only AFTER the
                        // insert has succeeded. If the insert had failed, the sequence
                        // would stay un-advanced and a retry re-inserts, instead of being
                        // dropped as a false duplicate. The batch is remembered with its
                        // offset, so a retry is answered with it (wave 3).
                        if let Some(meta) = seq_meta {
                            if let Err(e) = store.record_producer_batch(ProducerBatch {
                                producer_id: meta.producer_id,
                                topic_id,
                                partition_id: partition_index,
                                first_sequence: meta.base_sequence,
                                last_sequence: batch_last_sequence(
                                    meta.base_sequence,
                                    records.len() as i32,
                                ),
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
                    }
                    Err(e) => {
                        // Use typed error's Kafka error code for proper protocol response
                        let error_code = e.to_kafka_error_code();
                        if e.is_server_error() {
                            crate::pg_warning!(
                                "Failed to insert records for topic_id={}, partition={}: {}",
                                topic_id,
                                partition_index,
                                e
                            );
                        }
                        partition_response.error_code = error_code;
                        partition_response.base_offset = -1;
                        partition_response.log_append_time_ms = -1;
                        partition_response.log_start_offset = -1;
                    }
                }
            } else if let SequenceCheck::Duplicate { base_offset } = check {
                // A retry of an appended batch: success with the ORIGINAL base offset,
                // nothing inserted (Kafka's idempotent producer semantics, wave 3).
                partition_response.error_code = ERROR_NONE;
                partition_response.base_offset = base_offset;
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset =
                    reported_log_start_offset(store, topic_id, partition_index);
            }

            topic_response.partition_responses.push(partition_response);
        }

        kafka_response.responses.push(topic_response);
    }

    Ok(kafka_response)
}

/// The Produce error code for `e`. A stale epoch is INVALID_PRODUCER_EPOCH (47) on
/// Produce in Kafka 3.7, for idempotent and transactional producers alike (since
/// Kafka 2.7 PRODUCER_FENCED is no longer sent in a produce response), so a fenced
/// transactional epoch maps to it too.
fn produce_error_code(e: &crate::kafka::error::KafkaError) -> i16 {
    match e {
        crate::kafka::error::KafkaError::ProducerFenced { .. } => {
            crate::kafka::constants::ERROR_INVALID_PRODUCER_EPOCH
        }
        _ => e.to_kafka_error_code(),
    }
}

/// The log start offset a successful partition response reports, as Kafka does. The
/// records are already written, so a failed lookup does not fail the produce: the
/// field is then -1, the value Kafka sends when it has none.
fn reported_log_start_offset(
    store: &dyn crate::kafka::storage::LogStore,
    topic_id: i32,
    partition_id: i32,
) -> i64 {
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

/// Check that `txn_id` belongs to this producer/epoch and is in the Ongoing state.
fn validate_ongoing_transaction(
    store: &dyn crate::kafka::storage::TransactionStore,
    txn_id: &str,
    meta: &ProducerMetadata,
) -> Result<()> {
    store.validate_transaction(txn_id, meta.producer_id, meta.producer_epoch)?;
    match store.get_transaction_state(txn_id)? {
        Some(crate::kafka::storage::TransactionState::Ongoing) => Ok(()),
        Some(other) => Err(crate::kafka::error::KafkaError::invalid_txn_state(
            txn_id,
            format!("{other:?}"),
            "Produce",
        )),
        None => Err(crate::kafka::error::KafkaError::transactional_id_not_found(
            txn_id,
        )),
    }
}

#[cfg(test)]
mod wave3_tests {
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
    /// (AddPartitionsToTxn) is INVALID_TXN_STATE; it used to be written.
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
            vec![
                (0, ERROR_NONE, 11),
                (1, crate::kafka::constants::ERROR_INVALID_TXN_STATE, -1)
            ]
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

    /// Kafka 3.7 answers a stale epoch on Produce with INVALID_PRODUCER_EPOCH (47),
    /// for transactional producers too (was PRODUCER_FENCED, 90).
    #[test]
    fn stale_epoch_is_invalid_producer_epoch() {
        let mut mock = MockKafkaStore::new();
        mock.expect_validate_transaction()
            .returning(|_, _, _| Err(KafkaError::producer_fenced(7, 2, 3)));
        assert_eq!(
            run(&mock, &[0], Some("tx")),
            vec![(0, crate::kafka::constants::ERROR_INVALID_PRODUCER_EPOCH, -1)]
        );

        let mut mock = MockKafkaStore::new();
        mock.expect_get_or_create_topic()
            .returning(|_, _| Ok((1, 1)));
        mock.expect_check_and_update_sequence()
            .returning(|_, _, _, _, _, _| {
                Err(KafkaError::Protocol {
                    code: crate::kafka::constants::ERROR_INVALID_PRODUCER_EPOCH,
                    message: "stale".into(),
                })
            });
        assert_eq!(
            run(&mock, &[0], None),
            vec![(0, crate::kafka::constants::ERROR_INVALID_PRODUCER_EPOCH, -1)]
        );
    }
}
