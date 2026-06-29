// Metadata handlers
//
// Handlers for ApiVersions and Metadata requests.
// These are discovery-focused APIs that help clients understand broker capabilities.

use crate::kafka::constants::{DEFAULT_BROKER_ID, ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;

/// Handle ApiVersions request
///
/// This handler doesn't need storage - it just returns static protocol information
pub fn handle_api_versions() -> kafka_protocol::messages::api_versions_response::ApiVersionsResponse
{
    crate::kafka::build_api_versions_response()
}

/// Handle Metadata request
///
/// Returns metadata for all topics or specific requested topics. A specifically-requested topic
/// that doesn't exist is auto-created only when `allow_auto_topic_creation` is true (CONF-2);
/// otherwise it is reported as UNKNOWN_TOPIC_OR_PARTITION and left uncreated.
///
/// # Arguments
/// * `ctx` - Handler context containing store, broker metadata, and default partitions
/// * `requested_topics` - Specific topics to query, or None for all topics
/// * `allow_auto_topic_creation` - Whether a missing requested topic may be created
pub fn handle_metadata(
    ctx: &HandlerContext,
    requested_topics: Option<Vec<String>>,
    allow_auto_topic_creation: bool,
) -> Result<kafka_protocol::messages::metadata_response::MetadataResponse> {
    let store = ctx.store;
    let broker_host = ctx.broker.host();
    let broker_port = ctx.broker.port();
    let default_partitions = ctx.default_partitions;
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
            let mut topics_metadata = Vec::new();
            for topic_name in topic_names {
                // Resolve the topic's partition count. With auto-creation enabled (the default, and
                // the only behaviour pre-v4) a missing topic is created; with it disabled (CONF-2)
                // a missing topic resolves to None and is reported as UNKNOWN_TOPIC_OR_PARTITION
                // rather than being created.
                let resolved: Result<Option<i32>> = if allow_auto_topic_creation {
                    store
                        .get_or_create_topic(&topic_name, default_partitions)
                        .map(|(_topic_id, partition_count)| Some(partition_count))
                } else {
                    store
                        .get_topic_metadata(Some(std::slice::from_ref(&topic_name)))
                        .map(|metas| metas.into_iter().next().map(|tm| tm.partition_count))
                };

                let topic = match resolved {
                    Ok(Some(partition_count)) => {
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
                        crate::kafka::build_topic_metadata(topic_name, ERROR_NONE, partitions)
                    }
                    Ok(None) => {
                        // Topic doesn't exist and auto-creation is disabled — report, don't create.
                        crate::kafka::build_topic_metadata(
                            topic_name,
                            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                            vec![],
                        )
                    }
                    Err(e) => {
                        let error_code = e.to_kafka_error_code();
                        if e.is_server_error() {
                            crate::pg_warning!("Failed to resolve topic '{}': {}", topic_name, e);
                        }
                        crate::kafka::build_topic_metadata(topic_name, error_code, vec![])
                    }
                };
                topics_metadata.push(topic);
            }
            topics_metadata
        }
    };

    response.topics = topics_to_add;
    Ok(response)
}
