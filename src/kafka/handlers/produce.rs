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
                    return Ok(produce_error_response(&topic_data, e.to_kafka_error_code()));
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
            match store.get_or_create_topic(&topic_name, default_partitions) {
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
                        produce_error_response(std::slice::from_ref(&topic), code)
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

            // Sequence metadata for this partition (idempotent producers only).
            let seq_meta = partition_meta.as_ref().filter(|m| m.producer_id >= 0);

            // Phase 9: Validate sequence for idempotent producers BEFORE inserting
            let should_insert = if let Some(meta) = seq_meta {
                // Idempotent producer - check and update sequence
                match store.check_and_update_sequence(
                    meta.producer_id,
                    meta.producer_epoch,
                    topic_id,
                    partition_index,
                    meta.base_sequence,
                    records.len() as i32,
                ) {
                    Ok(should_insert) => should_insert, // true=insert, false=duplicate (skip insert)
                    Err(e) => {
                        // Sequence validation failed - return error for this partition
                        partition_response.error_code = e.to_kafka_error_code();
                        partition_response.base_offset = -1;
                        partition_response.log_append_time_ms = -1;
                        partition_response.log_start_offset = -1;
                        topic_response.partition_responses.push(partition_response);
                        continue;
                    }
                }
            } else {
                true // Non-idempotent producer - always insert
            };

            // Insert records (skip for duplicates)
            if should_insert {
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
                        partition_response.log_start_offset = -1;

                        // RV-8: advance the idempotent producer's sequence only AFTER the
                        // insert has succeeded. If the insert had failed, the sequence
                        // would stay un-advanced and a retry re-inserts, instead of being
                        // dropped as a false duplicate. new_last_sequence matches the value
                        // validate_sequence would have stored (base + count - 1).
                        if let Some(meta) = seq_meta {
                            let new_last_sequence = meta.base_sequence + records.len() as i32 - 1;
                            if let Err(e) = store.record_producer_sequence(
                                meta.producer_id,
                                topic_id,
                                partition_index,
                                new_last_sequence,
                            ) {
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
            } else {
                // Duplicate detected - return success (error_code=0) but skip insert
                // This is the correct Kafka idempotency behavior
                partition_response.error_code = ERROR_NONE;
                // The original batch's offset is not stored, so report it as unknown
                // (-1) rather than claim offset 0, which the client would record.
                partition_response.base_offset = -1;
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = -1;
            }

            topic_response.partition_responses.push(partition_response);
        }

        kafka_response.responses.push(topic_response);
    }

    Ok(kafka_response)
}

/// Check that `txn_id` belongs to this producer/epoch and is in the Ongoing state.
fn validate_ongoing_transaction(
    store: &dyn crate::kafka::storage::KafkaStore,
    txn_id: &str,
    meta: &ProducerMetadata,
) -> Result<()> {
    store.validate_transaction(txn_id, meta.producer_id, meta.producer_epoch)?;
    match store.get_transaction_state(txn_id)? {
        Some(crate::kafka::storage::TransactionState::Ongoing) => Ok(()),
        Some(other) => Err(crate::kafka::error::KafkaError::invalid_txn_state(
            txn_id,
            format!("{:?}", other),
            "Produce",
        )),
        None => Err(crate::kafka::error::KafkaError::transactional_id_not_found(
            txn_id,
        )),
    }
}

/// A ProduceResponse reporting `error_code` on every requested partition.
pub fn produce_error_response(
    topic_data: &[crate::kafka::messages::TopicProduceData],
    error_code: i16,
) -> kafka_protocol::messages::produce_response::ProduceResponse {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    let mut response = crate::kafka::build_produce_response();
    for topic in topic_data {
        let mut t = TopicProduceResponse::default();
        t.name = kafka_protocol::messages::TopicName(topic.name.clone().into());
        for p in &topic.partitions {
            let mut pr = PartitionProduceResponse::default();
            pr.index = p.partition_index;
            pr.error_code = error_code;
            pr.base_offset = -1;
            pr.log_append_time_ms = -1;
            pr.log_start_offset = -1;
            t.partition_responses.push(pr);
        }
        response.responses.push(t);
    }
    response
}
