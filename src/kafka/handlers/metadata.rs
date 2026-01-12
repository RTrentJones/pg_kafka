// Metadata handlers
//
// Handlers for ApiVersions and Metadata requests.
// These are discovery-focused APIs that help clients understand broker capabilities.

use crate::kafka::constants::{DEFAULT_BROKER_ID, ERROR_NONE};
use crate::kafka::error::Result;
use crate::kafka::storage::KafkaStore;

/// Handle ApiVersions request
///
/// This handler doesn't need storage - it just returns static protocol information
pub fn handle_api_versions() -> kafka_protocol::messages::api_versions_response::ApiVersionsResponse
{
    crate::kafka::build_api_versions_response()
}

/// Handle Metadata request
///
/// Returns metadata for all topics or specific requested topics.
/// Auto-creates topics if they don't exist when specifically requested.
///
/// # Arguments
/// * `store` - Storage backend
/// * `requested_topics` - Specific topics to query, or None for all topics
/// * `broker_host` - Hostname for broker metadata
/// * `broker_port` - Port for broker metadata
/// * `default_partitions` - Partition count for auto-created topics
pub fn handle_metadata(
    store: &dyn KafkaStore,
    requested_topics: Option<Vec<String>>,
    broker_host: &str,
    broker_port: i32,
    default_partitions: i32,
) -> Result<kafka_protocol::messages::metadata_response::MetadataResponse> {
    let mut response = kafka_protocol::messages::metadata_response::MetadataResponse::default();

    // Set controller ID - needed for admin operations like CreateTopics
    response.controller_id = kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID);

    // Add broker
    response.brokers.push(crate::kafka::build_broker_metadata(
        DEFAULT_BROKER_ID,
        broker_host,
        broker_port,
    ));

    // Build topic metadata based on what was requested
    let topics_to_add = match requested_topics {
        None => {
            // Client wants all topics - query from storage
            let stored_topics = store.get_topic_metadata(None)?;
            stored_topics
                .into_iter()
                .map(|tm| {
                    // Build partition metadata for all partitions
                    let partitions: Vec<_> = (0..tm.partition_count)
                        .map(|partition_id| {
                            crate::kafka::build_partition_metadata(
                                partition_id,
                                DEFAULT_BROKER_ID,
                                vec![DEFAULT_BROKER_ID],
                                vec![DEFAULT_BROKER_ID],
                            )
                        })
                        .collect();
                    crate::kafka::build_topic_metadata(tm.name, ERROR_NONE, partitions)
                })
                .collect()
        }
        Some(topic_names) => {
            // Client wants specific topics - create them if needed and return metadata
            let mut topics_metadata = Vec::new();
            for topic_name in topic_names {
                // Auto-create topic if it doesn't exist (returns both topic_id and partition_count)
                match store.get_or_create_topic(&topic_name, default_partitions) {
                    Ok((_topic_id, partition_count)) => {
                        // Build partition metadata for all partitions
                        let partitions: Vec<_> = (0..partition_count)
                            .map(|partition_id| {
                                crate::kafka::build_partition_metadata(
                                    partition_id,
                                    DEFAULT_BROKER_ID,
                                    vec![DEFAULT_BROKER_ID],
                                    vec![DEFAULT_BROKER_ID],
                                )
                            })
                            .collect();

                        let topic =
                            crate::kafka::build_topic_metadata(topic_name, ERROR_NONE, partitions);
                        topics_metadata.push(topic);
                    }
                    Err(e) => {
                        // Use typed error's Kafka error code
                        let error_code = e.to_kafka_error_code();
                        if e.is_server_error() {
                            crate::pg_warning!(
                                "Failed to get_or_create topic '{}': {}",
                                topic_name,
                                e
                            );
                        }
                        let topic =
                            crate::kafka::build_topic_metadata(topic_name, error_code, vec![]);
                        topics_metadata.push(topic);
                    }
                }
            }
            topics_metadata
        }
    };

    response.topics = topics_to_add;
    Ok(response)
}
