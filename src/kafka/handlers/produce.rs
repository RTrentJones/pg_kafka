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
    // Phase 10: Validate transaction state if transactional
    if let Some(txn_id) = transactional_id {
        if let Some(meta) = producer_metadata {
            if meta.producer_id >= 0 {
                store.validate_transaction(txn_id, meta.producer_id, meta.producer_epoch)?;

                // Ensure transaction is in Ongoing state
                let state = store.get_transaction_state(txn_id)?;
                match state {
                    Some(crate::kafka::storage::TransactionState::Ongoing) => {
                        // Good, transaction is active
                    }
                    Some(other) => {
                        return Err(crate::kafka::error::KafkaError::invalid_txn_state(
                            txn_id,
                            format!("{:?}", other),
                            "Produce",
                        ));
                    }
                    None => {
                        return Err(crate::kafka::error::KafkaError::transactional_id_not_found(
                            txn_id,
                        ));
                    }
                }
            }
        }
    }
    let mut kafka_response = crate::kafka::build_produce_response();

    // Process each topic
    for topic in topic_data {
        let topic_name: String = topic.name.clone();

        // Get or create topic (returns both topic_id and partition_count in single query)
        let (topic_id, partition_count) =
            store.get_or_create_topic(&topic_name, default_partitions)?;

        // Build topic response
        let mut topic_response =
            kafka_protocol::messages::produce_response::TopicProduceResponse::default();
        topic_response.name = kafka_protocol::messages::TopicName(topic_name.clone().into());

        // Group records by computed partition
        // When partition_index == -1, use key-based routing
        let mut records_by_partition: HashMap<i32, Vec<crate::kafka::messages::Record>> =
            HashMap::new();

        for partition_data in topic.partitions {
            let explicit_partition = partition_data.partition_index;

            if explicit_partition == -1 {
                // Server-side partition routing based on key
                for record in partition_data.records {
                    let target_partition =
                        compute_partition(record.key.as_deref(), partition_count, -1);
                    records_by_partition
                        .entry(target_partition)
                        .or_default()
                        .push(record);
                }
            } else {
                // Explicit partition - use as-is
                records_by_partition
                    .entry(explicit_partition)
                    .or_default()
                    .extend(partition_data.records);
            }
        }

        // Process each partition group
        for (partition_index, records) in records_by_partition {
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

            // Phase 9: Validate sequence for idempotent producers BEFORE inserting
            let should_insert = if let Some(meta) = producer_metadata {
                if meta.producer_id >= 0 {
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
                }
            } else {
                true // No metadata - always insert
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
                partition_response.base_offset = 0; // Duplicate - offset doesn't matter
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = -1;
            }

            topic_response.partition_responses.push(partition_response);
        }

        kafka_response.responses.push(topic_response);
    }

    Ok(kafka_response)
}
