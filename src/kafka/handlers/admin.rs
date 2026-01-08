// Admin API handlers (Phase 6)
//
// This module contains handlers for Kafka admin operations:
// - CreateTopics: Programmatically create topics
// - DeleteTopics: Delete existing topics
// - CreatePartitions: Add partitions to existing topics
// - DeleteGroups: Delete consumer groups

use kafka_protocol::messages::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use kafka_protocol::messages::delete_groups_response::{
    DeletableGroupResult, DeleteGroupsResponse,
};
use kafka_protocol::messages::delete_topics_response::{
    DeletableTopicResult, DeleteTopicsResponse,
};
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::kafka::constants::*;
use crate::kafka::coordinator::GroupCoordinator;
use crate::kafka::error::Result;
use crate::kafka::messages::{CreatePartitionsTopicRequest, CreateTopicRequest};
use crate::kafka::storage::KafkaStore;

/// Handle CreateTopics request
///
/// Creates topics with the specified configuration. Topics that already exist
/// will return TOPIC_ALREADY_EXISTS error.
pub fn handle_create_topics(
    store: &impl KafkaStore,
    topics: Vec<CreateTopicRequest>,
    validate_only: bool,
) -> Result<CreateTopicsResponse> {
    let mut results = Vec::new();

    for topic in topics {
        let mut result = CreatableTopicResult::default();
        result.name = TopicName(StrBytes::from_string(topic.name.clone()));

        // Validate topic name
        if topic.name.is_empty() {
            result.error_code = ERROR_INVALID_TOPIC_EXCEPTION;
            result.error_message = Some(StrBytes::from_static_str("Topic name cannot be empty"));
            results.push(result);
            continue;
        }

        // Validate partition count
        let num_partitions = if topic.num_partitions <= 0 {
            DEFAULT_TOPIC_PARTITIONS // Use broker default
        } else {
            topic.num_partitions
        };

        if num_partitions < 1 {
            result.error_code = ERROR_INVALID_PARTITIONS;
            result.error_message = Some(StrBytes::from_static_str(
                "Number of partitions must be at least 1",
            ));
            results.push(result);
            continue;
        }

        // Check if topic already exists
        match store.topic_exists(&topic.name) {
            Ok(true) => {
                result.error_code = ERROR_TOPIC_ALREADY_EXISTS;
                result.error_message = Some(StrBytes::from_static_str(
                    "Topic with this name already exists",
                ));
                results.push(result);
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                result.error_message = Some(StrBytes::from_string(format!(
                    "Error checking topic: {}",
                    e
                )));
                results.push(result);
                continue;
            }
        }

        // If validate_only, don't actually create
        if validate_only {
            result.error_code = ERROR_NONE;
            result.num_partitions = num_partitions;
            result.replication_factor = 1; // Single-node, always 1
            results.push(result);
            continue;
        }

        // Create the topic
        match store.create_topic(&topic.name, num_partitions) {
            Ok(_topic_id) => {
                result.error_code = ERROR_NONE;
                // Note: topic_id field is a Uuid in v7+, we leave it as default (nil) for simplicity
                result.num_partitions = num_partitions;
                result.replication_factor = 1; // Single-node, always 1
            }
            Err(e) => {
                result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                result.error_message = Some(StrBytes::from_string(format!(
                    "Failed to create topic: {}",
                    e
                )));
            }
        }

        results.push(result);
    }

    let mut response = CreateTopicsResponse::default();
    response.topics = results;
    Ok(response)
}

/// Handle DeleteTopics request
///
/// Deletes topics and all their messages. Consumer offsets for deleted topics
/// are also removed.
pub fn handle_delete_topics(
    store: &impl KafkaStore,
    topic_names: Vec<String>,
) -> Result<DeleteTopicsResponse> {
    let mut results = Vec::new();

    for topic_name in topic_names {
        let mut result = DeletableTopicResult::default();
        result.name = Some(TopicName(StrBytes::from_string(topic_name.clone())));

        // Check if topic exists
        match store.get_topic_id(&topic_name) {
            Ok(Some(topic_id)) => {
                // Delete the topic (cascades to messages and consumer_offsets)
                match store.delete_topic(topic_id) {
                    Ok(()) => {
                        result.error_code = ERROR_NONE;
                    }
                    Err(e) => {
                        result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                        result.error_message = Some(StrBytes::from_string(format!(
                            "Failed to delete topic: {}",
                            e
                        )));
                    }
                }
            }
            Ok(None) => {
                result.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                result.error_message = Some(StrBytes::from_static_str("This topic does not exist"));
            }
            Err(e) => {
                result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                result.error_message = Some(StrBytes::from_string(format!(
                    "Error looking up topic: {}",
                    e
                )));
            }
        }

        results.push(result);
    }

    let mut response = DeleteTopicsResponse::default();
    response.responses = results;
    Ok(response)
}

/// Handle CreatePartitions request
///
/// Increases the partition count for existing topics.
/// Cannot decrease partition count.
pub fn handle_create_partitions(
    store: &impl KafkaStore,
    topics: Vec<CreatePartitionsTopicRequest>,
    validate_only: bool,
) -> Result<CreatePartitionsResponse> {
    let mut results = Vec::new();

    for topic in topics {
        let mut result = CreatePartitionsTopicResult::default();
        result.name = TopicName(StrBytes::from_string(topic.name.clone()));

        // Get current partition count
        match store.get_topic_partition_count(&topic.name) {
            Ok(Some(current_count)) => {
                if topic.count < current_count {
                    result.error_code = ERROR_INVALID_PARTITIONS;
                    result.error_message = Some(StrBytes::from_string(format!(
                        "Cannot reduce partition count from {} to {}",
                        current_count, topic.count
                    )));
                } else if topic.count == current_count {
                    // No change needed
                    result.error_code = ERROR_NONE;
                } else if validate_only {
                    // Validation passed
                    result.error_code = ERROR_NONE;
                } else {
                    // Actually increase partitions
                    match store.set_topic_partition_count(&topic.name, topic.count) {
                        Ok(()) => {
                            result.error_code = ERROR_NONE;
                        }
                        Err(e) => {
                            result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            result.error_message = Some(StrBytes::from_string(format!(
                                "Failed to increase partitions: {}",
                                e
                            )));
                        }
                    }
                }
            }
            Ok(None) => {
                result.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                result.error_message = Some(StrBytes::from_static_str("Topic does not exist"));
            }
            Err(e) => {
                result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                result.error_message = Some(StrBytes::from_string(format!(
                    "Error looking up topic: {}",
                    e
                )));
            }
        }

        results.push(result);
    }

    let mut response = CreatePartitionsResponse::default();
    response.results = results;
    Ok(response)
}

/// Handle DeleteGroups request
///
/// Deletes consumer groups. Groups with active members cannot be deleted
/// (returns NON_EMPTY_GROUP error).
pub fn handle_delete_groups(
    store: &impl KafkaStore,
    coordinator: &GroupCoordinator,
    group_names: Vec<String>,
) -> Result<DeleteGroupsResponse> {
    let mut results = Vec::new();

    for group_name in group_names {
        let mut result = DeletableGroupResult::default();
        result.group_id = GroupId(StrBytes::from_string(group_name.clone()));

        // Check if group exists and has members
        match coordinator.get_group_state(&group_name) {
            Some(group) => {
                // Check if group has active members
                if !group.members.is_empty() {
                    result.error_code = ERROR_NON_EMPTY_GROUP;
                    // Note: DeletableGroupResult doesn't have error_message field
                    results.push(result);
                    continue;
                }

                // Remove from coordinator
                coordinator.remove_group(&group_name);

                // Delete committed offsets from storage
                match store.delete_consumer_group_offsets(&group_name) {
                    Ok(()) => {
                        result.error_code = ERROR_NONE;
                    }
                    Err(_e) => {
                        result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                    }
                }
            }
            None => {
                // Group doesn't exist in coordinator - check if there are committed offsets
                match store.delete_consumer_group_offsets(&group_name) {
                    Ok(()) => {
                        // Successfully deleted any orphaned offsets, or nothing to delete
                        result.error_code = ERROR_NONE;
                    }
                    Err(_e) => {
                        result.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                    }
                }
            }
        }

        results.push(result);
    }

    let mut response = DeleteGroupsResponse::default();
    response.results = results;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mocks::MockKafkaStore;

    #[test]
    fn test_create_topics_success() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_create_topic().returning(|_, _| Ok(1));

        let topics = vec![CreateTopicRequest {
            name: "test-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }];

        let result = handle_create_topics(&store, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_create_topics_empty_name() {
        let store = MockKafkaStore::new();
        let topics = vec![CreateTopicRequest {
            name: "".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }];

        let result = handle_create_topics(&store, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_INVALID_TOPIC_EXCEPTION);
    }

    #[test]
    fn test_create_topics_validate_only() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        // create_topic should NOT be called when validate_only is true

        let topics = vec![CreateTopicRequest {
            name: "test-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }];

        let result = handle_create_topics(&store, topics, true);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_create_topics_already_exists() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(true));

        let topics = vec![CreateTopicRequest {
            name: "existing-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }];

        let result = handle_create_topics(&store, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_TOPIC_ALREADY_EXISTS);
    }

    #[test]
    fn test_delete_topics_success() {
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_id().returning(|_| Ok(Some(1)));
        store.expect_delete_topic().returning(|_| Ok(()));

        let result = handle_delete_topics(&store, vec!["test-topic".to_string()]);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_delete_topics_not_found() {
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_id().returning(|_| Ok(None));

        let result = handle_delete_topics(&store, vec!["nonexistent".to_string()]);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(
            response.responses[0].error_code,
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    #[test]
    fn test_create_partitions_success() {
        let mut store = MockKafkaStore::new();
        store
            .expect_get_topic_partition_count()
            .returning(|_| Ok(Some(1)));
        store
            .expect_set_topic_partition_count()
            .returning(|_, _| Ok(()));

        let topics = vec![CreatePartitionsTopicRequest {
            name: "test-topic".to_string(),
            count: 3,
        }];

        let result = handle_create_partitions(&store, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_create_partitions_cannot_decrease() {
        let mut store = MockKafkaStore::new();
        store
            .expect_get_topic_partition_count()
            .returning(|_| Ok(Some(5)));

        let topics = vec![CreatePartitionsTopicRequest {
            name: "test-topic".to_string(),
            count: 3, // Less than current 5
        }];

        let result = handle_create_partitions(&store, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.results[0].error_code, ERROR_INVALID_PARTITIONS);
    }
}
