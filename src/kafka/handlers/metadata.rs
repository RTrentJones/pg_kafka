// Metadata handlers
//
// Handlers for ApiVersions and Metadata requests.
// These are discovery-focused APIs that help clients understand broker capabilities.

use crate::kafka::constants::*;
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
pub fn handle_metadata(
    store: &impl KafkaStore,
    requested_topics: Option<Vec<String>>,
    broker_host: String,
    broker_port: i32,
) -> Result<kafka_protocol::messages::metadata_response::MetadataResponse> {
    let mut response = kafka_protocol::messages::metadata_response::MetadataResponse::default();

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
                    let partition = crate::kafka::build_partition_metadata(
                        0,
                        DEFAULT_BROKER_ID,
                        vec![DEFAULT_BROKER_ID],
                        vec![DEFAULT_BROKER_ID],
                    );
                    crate::kafka::build_topic_metadata(tm.name, ERROR_NONE, vec![partition])
                })
                .collect()
        }
        Some(topic_names) => {
            // Client wants specific topics - create them if needed and return metadata
            let mut topics_metadata = Vec::new();
            for topic_name in topic_names {
                // Auto-create topic if it doesn't exist
                match store.get_or_create_topic(&topic_name) {
                    Ok(_topic_id) => {
                        // Return metadata for this topic
                        let partition = crate::kafka::build_partition_metadata(
                            0,
                            DEFAULT_BROKER_ID,
                            vec![DEFAULT_BROKER_ID],
                            vec![DEFAULT_BROKER_ID],
                        );
                        let topic = crate::kafka::build_topic_metadata(
                            topic_name,
                            ERROR_NONE,
                            vec![partition],
                        );
                        topics_metadata.push(topic);
                    }
                    Err(e) => {
                        // Return error for this topic
                        crate::pg_warning!("Failed to get_or_create topic '{}': {}", topic_name, e);
                        let topic = crate::kafka::build_topic_metadata(
                            topic_name,
                            ERROR_UNKNOWN_SERVER_ERROR,
                            vec![],
                        );
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
