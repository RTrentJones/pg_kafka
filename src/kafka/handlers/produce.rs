// Produce handler
//
// Handles ProduceRequest - writing messages to topics.
// Phase 7: Added server-side partition routing for partition_index == -1
// Phase 9: Added idempotent producer sequence validation

use crate::kafka::constants::{ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION};
use crate::kafka::error::Result;
use crate::kafka::messages::ProducerMetadata;
use crate::kafka::partitioner::compute_partition;
use crate::kafka::storage::KafkaStore;
use std::collections::HashMap;

/// Handle Produce request
///
/// Inserts records into storage and returns base offsets for each partition.
/// When partition_index == -1, uses key-based partition routing.
///
/// # Arguments
/// * `store` - Storage backend
/// * `topic_data` - Topics and records to produce
/// * `default_partitions` - Partition count for auto-created topics
/// * `producer_metadata` - Optional producer metadata for idempotent producers (Phase 9)
pub fn handle_produce(
    store: &impl KafkaStore,
    topic_data: Vec<crate::kafka::messages::TopicProduceData>,
    default_partitions: i32,
    producer_metadata: Option<&ProducerMetadata>,
) -> Result<kafka_protocol::messages::produce_response::ProduceResponse> {
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
            if let Some(meta) = producer_metadata {
                if meta.producer_id >= 0 {
                    // Idempotent producer - check and update sequence
                    if let Err(e) = store.check_and_update_sequence(
                        meta.producer_id,
                        meta.producer_epoch,
                        topic_id,
                        partition_index,
                        meta.base_sequence,
                        records.len() as i32,
                    ) {
                        // Sequence validation failed - return error for this partition
                        partition_response.error_code = e.to_kafka_error_code();
                        partition_response.base_offset = -1;
                        partition_response.log_append_time_ms = -1;
                        partition_response.log_start_offset = -1;
                        topic_response.partition_responses.push(partition_response);
                        continue;
                    }
                }
            }

            // Insert records
            match store.insert_records(topic_id, partition_index, &records) {
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

            topic_response.partition_responses.push(partition_response);
        }

        kafka_response.responses.push(topic_response);
    }

    Ok(kafka_response)
}
