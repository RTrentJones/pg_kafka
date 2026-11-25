//! ApiVersions and Metadata: what the broker supports, and topic and partition discovery with
//! auto-creation of requested topics.

use crate::kafka::constants::{
    DEFAULT_BROKER_ID, ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_UNKNOWN_SERVER_ERROR,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
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
/// Returns metadata for all topics or specific requested topics, each distinct requested
/// name once. A specifically-requested topic that doesn't exist is auto-created only when
/// `allow_auto_topic_creation` is true; otherwise it is reported as
/// UNKNOWN_TOPIC_OR_PARTITION and left uncreated. The missing names of one request are
/// created together or not at all: if they would take the partitions of all topics above
/// `pg_kafka.max_total_partitions`, none is created and each is POLICY_VIOLATION.
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
                .map(|tm| topic_metadata_for(tm.name, tm.partition_count))
                .collect()
        }
        Some(topic_names) => requested_topic_metadata(
            ctx,
            &super::helpers::distinct_names(topic_names.iter().map(String::as_str)),
            allow_auto_topic_creation,
        ),
    };

    response.topics = topics_to_add;
    Ok(response)
}

/// The Metadata entries of the requested `names` (distinct, in request order: Kafka 3.7
/// answers each distinct name once), looked up together. With auto-creation a missing
/// topic is created; without it a missing topic is UNKNOWN_TOPIC_OR_PARTITION, and either
/// way a missing name Kafka would not accept is INVALID_TOPIC_EXCEPTION and not created.
fn requested_topic_metadata(
    ctx: &HandlerContext,
    names: &[String],
    allow_auto_topic_creation: bool,
) -> Vec<kafka_protocol::messages::metadata_response::MetadataResponseTopic> {
    let mut found =
        match super::helpers::resolve_topics(ctx.store, names.iter().map(String::as_str)) {
            Ok(found) => found,
            Err(code) => {
                return names
                    .iter()
                    .map(|name| crate::kafka::build_topic_metadata(name.clone(), code, vec![]))
                    .collect()
            }
        };
    // With auto-creation, the missing names Kafka accepts are created together, however
    // many there are.
    let creatable: Vec<String> = names
        .iter()
        .filter(|name| {
            !found.contains_key(*name) && super::helpers::validate_topic_name(name).is_ok()
        })
        .cloned()
        .collect();
    // The error code of a failed creation, reported on the names it would have created.
    let mut create_failed = None;
    if allow_auto_topic_creation && !creatable.is_empty() {
        let partitions = crate::kafka::limits::auto_create_partitions(ctx.default_partitions);
        match ctx.store.get_or_create_topics(&creatable, partitions) {
            Ok(created) => found.extend(
                created
                    .into_iter()
                    .map(|tm| (tm.name, (tm.id, tm.partition_count))),
            ),
            Err(e) => {
                if e.is_server_error() {
                    crate::pg_warning!("Failed to create {} topics: {}", creatable.len(), e);
                }
                create_failed = Some(e.to_kafka_error_code());
            }
        }
    }
    names
        .iter()
        .map(|name| {
            if let Some(&(_, partition_count)) = found.get(name) {
                return topic_metadata_for(name.clone(), partition_count);
            }
            // Kafka 3.7 reports a missing name it would not accept as invalid, with or
            // without auto-creation, and any other missing name as unknown (or as the
            // failure to create it).
            let error_code = match (super::helpers::validate_topic_name(name), create_failed) {
                (Err(_), _) => ERROR_INVALID_TOPIC_EXCEPTION,
                (Ok(()), Some(code)) => code,
                (Ok(()), None) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            };
            crate::kafka::build_topic_metadata(name.clone(), error_code, vec![])
        })
        .collect()
}

/// One topic's Metadata entry with one partition entry per partition. A stored
/// partition count outside `1..=HARD_MAX_PARTITIONS` (only possible if the CHECK constraint on
/// `kafka.topics.partitions` was bypassed) is reported as UNKNOWN_SERVER_ERROR with no
/// partitions instead of being allocated: allocating a count of 2,000,000,000 would abort the
/// worker on an allocation failure and send Postgres into crash recovery on every Metadata
/// request.
pub fn topic_metadata_for(
    topic_name: String,
    partition_count: i32,
) -> kafka_protocol::messages::metadata_response::MetadataResponseTopic {
    let Some(count) = crate::kafka::limits::sane_partition_count(partition_count) else {
        crate::pg_warning!(
            "Topic '{}' has an invalid stored partition count {}; reporting it as unavailable",
            topic_name,
            partition_count
        );
        return crate::kafka::build_topic_metadata(topic_name, ERROR_UNKNOWN_SERVER_ERROR, vec![]);
    };
    let partitions: Vec<_> = (0..count)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_metadata_for_rejects_corrupt_partition_counts() {
        let ok = topic_metadata_for("t".to_string(), 3);
        assert_eq!(ok.error_code, ERROR_NONE);
        assert_eq!(ok.partitions.len(), 3);
        for bad in [
            0,
            -1,
            2_000_000_000,
            crate::kafka::limits::HARD_MAX_PARTITIONS + 1,
        ] {
            let t = topic_metadata_for("t".to_string(), bad);
            assert_eq!(t.error_code, ERROR_UNKNOWN_SERVER_ERROR, "count {bad}");
            assert_eq!(
                t.partitions,
                [] as [kafka_protocol::messages::metadata_response::MetadataResponsePartition; 0]
            );
        }
    }
}
