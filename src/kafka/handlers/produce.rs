// Produce handler
//
// Handles ProduceRequest - writing messages to topics.

use crate::kafka::constants::*;
use crate::kafka::error::Result;
use crate::kafka::storage::KafkaStore;

/// Handle Produce request
///
/// Inserts records into storage and returns base offsets for each partition
pub fn handle_produce(
    store: &impl KafkaStore,
    topic_data: Vec<crate::kafka::messages::TopicProduceData>,
) -> Result<kafka_protocol::messages::produce_response::ProduceResponse> {
    let mut kafka_response = crate::kafka::build_produce_response();

    // Process each topic
    for topic in topic_data {
        let topic_name: String = topic.name.clone();

        // Get or create topic
        let topic_id = store.get_or_create_topic(&topic_name)?;

        // Build topic response
        let mut topic_response =
            kafka_protocol::messages::produce_response::TopicProduceResponse::default();
        topic_response.name = kafka_protocol::messages::TopicName(topic_name.clone().into());

        // Process each partition
        for partition in topic.partitions {
            let partition_index = partition.partition_index;

            let mut partition_response =
                kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
            partition_response.index = partition_index;

            // Validate partition index (we only support partition 0 for now)
            if partition_index != 0 {
                partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                partition_response.base_offset = -1;
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = -1;
                topic_response.partition_responses.push(partition_response);
                continue;
            }

            // Insert records
            match store.insert_records(topic_id, partition_index, &partition.records) {
                Ok(base_offset) => {
                    partition_response.error_code = ERROR_NONE;
                    partition_response.base_offset = base_offset;
                    partition_response.log_append_time_ms = -1;
                    partition_response.log_start_offset = -1;
                }
                Err(e) => {
                    crate::pg_warning!(
                        "Failed to insert records for topic_id={}, partition={}: {}",
                        topic_id,
                        partition_index,
                        e
                    );
                    partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
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
