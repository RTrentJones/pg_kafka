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

use super::helpers::validate_topic_name;
use crate::kafka::constants::{
    ERROR_GROUP_ID_NOT_FOUND, ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICATION_FACTOR,
    ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NON_EMPTY_GROUP,
    ERROR_TOPIC_ALREADY_EXISTS, ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::{CreatePartitionsTopicRequest, CreateTopicRequest};

/// Handle CreateTopics request
///
/// Validates each topic as Kafka 3.7 does (wave3 groups) and creates the valid ones:
/// - a name repeated in the request: one result, INVALID_REQUEST ("Duplicate topic name.");
/// - an invalid name, or one that collides with an existing topic once '.' and '_' are treated
///   as equal (`a.b` vs `a_b`): INVALID_TOPIC_EXCEPTION;
/// - `num_partitions` 0 or below -1, or above `pg_kafka.max_partitions_per_topic`:
///   INVALID_PARTITIONS (-1 means `pg_kafka.default_partitions`, capped at the maximum);
/// - `replication_factor` other than 1 or -1 (the default): INVALID_REPLICATION_FACTOR, since
///   this is a single-broker cluster;
/// - an existing topic: TOPIC_ALREADY_EXISTS.
pub fn handle_create_topics(
    ctx: &HandlerContext,
    topics: Vec<CreateTopicRequest>,
    validate_only: bool,
) -> Result<CreateTopicsResponse> {
    let store = ctx.store;
    let max_partitions = crate::kafka::limits::max_partitions_per_topic();
    let mut results = Vec::new();

    // A name given more than once fails as a whole, reported once (Kafka answers one
    // INVALID_REQUEST entry and creates nothing for it).
    let mut seen = std::collections::HashMap::<String, usize>::new();
    for t in &topics {
        *seen.entry(t.name.clone()).or_default() += 1;
    }
    let mut reported = std::collections::HashSet::<String>::new();

    for topic in topics {
        if !reported.insert(topic.name.clone()) {
            continue;
        }
        let mut result = CreatableTopicResult::default();
        result.name = TopicName(StrBytes::from_string(topic.name.clone()));
        result.num_partitions = -1;
        result.replication_factor = -1;
        let fail = |mut result: CreatableTopicResult, code: i16, msg: String| {
            result.error_code = code;
            result.error_message = Some(StrBytes::from_string(msg));
            result
        };

        if seen.get(&topic.name).copied().unwrap_or(0) > 1 {
            results.push(fail(
                result,
                ERROR_INVALID_REQUEST,
                "Duplicate topic name.".to_string(),
            ));
            continue;
        }

        // Validate topic name (CONF-4: length, charset, and "."/".." in addition to empty).
        if let Err(msg) = validate_topic_name(&topic.name) {
            results.push(fail(result, ERROR_INVALID_TOPIC_EXCEPTION, msg.to_string()));
            continue;
        }

        // -1 means "use the broker default" (pg_kafka.default_partitions, capped like an
        // auto-created topic); any other value < 1 (e.g. 0) is rejected.
        let num_partitions = if topic.num_partitions == -1 {
            crate::kafka::limits::auto_create_partitions(ctx.default_partitions)
        } else {
            topic.num_partitions
        };
        if num_partitions < 1 {
            results.push(fail(
                result,
                ERROR_INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.".to_string(),
            ));
            continue;
        }
        if let Err(msg) =
            crate::kafka::limits::check_partition_count(num_partitions, max_partitions)
        {
            results.push(fail(result, ERROR_INVALID_PARTITIONS, msg));
            continue;
        }

        if let Err(msg) = validate_replication_factor(topic.replication_factor) {
            results.push(fail(result, ERROR_INVALID_REPLICATION_FACTOR, msg));
            continue;
        }

        // Check if topic already exists
        match store.topic_exists(&topic.name) {
            Ok(true) => {
                results.push(fail(
                    result,
                    ERROR_TOPIC_ALREADY_EXISTS,
                    format!("Topic '{}' already exists.", topic.name),
                ));
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                results.push(fail(
                    result,
                    ERROR_UNKNOWN_SERVER_ERROR,
                    format!("Error checking topic: {e}"),
                ));
                continue;
            }
        }

        // Kafka rejects a name that collides with an existing one once '.' and '_' are
        // treated as equal (their metric names would clash).
        match store.find_colliding_topic(&topic.name) {
            Ok(Some(existing)) => {
                results.push(fail(
                    result,
                    ERROR_INVALID_TOPIC_EXCEPTION,
                    format!(
                        "Topic '{}' collides with existing topic: {}",
                        topic.name, existing
                    ),
                ));
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                results.push(fail(
                    result,
                    ERROR_UNKNOWN_SERVER_ERROR,
                    format!("Error checking topic: {e}"),
                ));
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
                result = fail(
                    result,
                    ERROR_UNKNOWN_SERVER_ERROR,
                    format!("Failed to create topic: {e}"),
                );
            }
        }

        results.push(result);
    }

    let mut response = CreateTopicsResponse::default();
    response.throttle_time_ms = 0;
    response.topics = results;
    Ok(response)
}

/// Replication factor rule for this one-broker cluster (Kafka's messages): -1 (the broker
/// default, 1) and 1 are accepted; 0 or below -1 are invalid, above 1 cannot be placed.
pub fn validate_replication_factor(replication_factor: i16) -> std::result::Result<(), String> {
    match replication_factor {
        -1 | 1 => Ok(()),
        rf if rf < 1 => Err(
            "Replication factor must be larger than 0, or -1 to use the default value.".to_string(),
        ),
        rf => Err(format!(
            "Unable to replicate the partition {rf} time(s): The target replication factor of {rf} \
             cannot be reached because only 1 broker(s) are registered."
        )),
    }
}

/// Handle DeleteTopics request
///
/// Deletes topics and all their messages. Consumer offsets for deleted topics
/// are also removed.
pub fn handle_delete_topics(
    ctx: &HandlerContext,
    topic_names: Vec<String>,
) -> Result<DeleteTopicsResponse> {
    let store = ctx.store;
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
                            "Failed to delete topic: {e}"
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
                    "Error looking up topic: {e}"
                )));
            }
        }

        results.push(result);
    }

    let mut response = DeleteTopicsResponse::default();
    response.throttle_time_ms = 0;
    response.responses = results;
    Ok(response)
}

/// Handle CreatePartitions request
///
/// Increases the partition count for existing topics.
/// Cannot decrease partition count.
pub fn handle_create_partitions(
    ctx: &HandlerContext,
    topics: Vec<CreatePartitionsTopicRequest>,
    validate_only: bool,
) -> Result<CreatePartitionsResponse> {
    let store = ctx.store;
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
                    // CONF-4: Kafka rejects a no-op partition count as INVALID_PARTITIONS
                    // ("Topic already has N partitions") rather than silently succeeding.
                    result.error_code = ERROR_INVALID_PARTITIONS;
                    result.error_message = Some(StrBytes::from_string(format!(
                        "Topic already has {current_count} partitions"
                    )));
                } else if let Err(msg) = crate::kafka::limits::check_partition_count(
                    topic.count,
                    crate::kafka::limits::max_partitions_per_topic(),
                ) {
                    // wave3 groups: the per-topic partition cap applies to growth too.
                    result.error_code = ERROR_INVALID_PARTITIONS;
                    result.error_message = Some(StrBytes::from_string(msg));
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
                                "Failed to increase partitions: {e}"
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
                    "Error looking up topic: {e}"
                )));
            }
        }

        results.push(result);
    }

    let mut response = CreatePartitionsResponse::default();
    response.throttle_time_ms = 0;
    response.results = results;
    Ok(response)
}

/// Handle DeleteGroups request
///
/// Deletes consumer groups. Groups with active members cannot be deleted
/// (returns NON_EMPTY_GROUP error).
pub fn handle_delete_groups(
    ctx: &HandlerContext,
    group_names: Vec<String>,
) -> Result<DeleteGroupsResponse> {
    let store = ctx.store;
    let coordinator = ctx.coordinator;
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

                // Delete committed offsets first and drop the in-memory group only once
                // that succeeded: a failed delete reports an error and leaves the group
                // as it was, instead of half-deleting it (gone from the coordinator,
                // offsets still stored).
                match store.delete_consumer_group_offsets(&group_name) {
                    Ok(()) => {
                        coordinator.remove_group(&group_name);
                        result.error_code = ERROR_NONE;
                    }
                    Err(e) => {
                        result.error_code = e.to_kafka_error_code();
                    }
                }
            }
            None => {
                // wave3 groups: a group the coordinator does not hold exists only through its
                // committed offsets (an Empty group, as DescribeGroups/ListGroups report it).
                // Without any, it does not exist: GROUP_ID_NOT_FOUND, as Kafka answers.
                match store.group_has_offsets(&group_name) {
                    Ok(false) => result.error_code = ERROR_GROUP_ID_NOT_FOUND,
                    Ok(true) => match store.delete_consumer_group_offsets(&group_name) {
                        Ok(()) => result.error_code = ERROR_NONE,
                        Err(e) => result.error_code = e.to_kafka_error_code(),
                    },
                    Err(e) => result.error_code = e.to_kafka_error_code(),
                }
            }
        }

        results.push(result);
    }

    let mut response = DeleteGroupsResponse::default();
    response.throttle_time_ms = 0;
    response.results = results;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mocks::MockKafkaStore;

    fn make_test_context<'a>(
        store: &'a MockKafkaStore,
        coordinator: &'a crate::kafka::GroupCoordinator,
        broker: &'a crate::kafka::BrokerMetadata,
    ) -> HandlerContext<'a> {
        HandlerContext::new(
            store,
            coordinator,
            broker,
            1,
            kafka_protocol::records::Compression::None,
        )
    }

    #[test]
    fn test_create_topics_success() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        store.expect_create_topic().returning(|_, _| Ok(1));

        let topics = vec![CreateTopicRequest {
            name: "test-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }];

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_topics(&ctx, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_create_topics_empty_name() {
        let store = MockKafkaStore::new();
        let topics = vec![CreateTopicRequest {
            name: String::new(),
            num_partitions: 1,
            replication_factor: 1,
        }];

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_topics(&ctx, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_INVALID_TOPIC_EXCEPTION);
    }

    #[test]
    fn test_create_topics_minus_one_uses_default_partitions_and_zero_is_rejected() {
        // num_partitions = -1 means "broker default" (pg_kafka.default_partitions);
        // 0 is not a valid partition count.
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        store
            .expect_create_topic()
            .withf(|name, n| name == "defaulted" && *n == 6)
            .times(1)
            .returning(|_, _| Ok(1));

        let topics = vec![
            CreateTopicRequest {
                name: "defaulted".to_string(),
                num_partitions: -1,
                replication_factor: 1,
            },
            CreateTopicRequest {
                name: "zero".to_string(),
                num_partitions: 0,
                replication_factor: 1,
            },
        ];

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(
            &store,
            &coordinator,
            &broker,
            6,
            kafka_protocol::records::Compression::None,
        );
        let response = handle_create_topics(&ctx, topics, false).unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_NONE);
        assert_eq!(response.topics[0].num_partitions, 6);
        assert_eq!(response.topics[1].error_code, ERROR_INVALID_PARTITIONS);
    }

    #[test]
    fn test_create_topics_validate_only() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        // create_topic should NOT be called when validate_only is true

        let topics = vec![CreateTopicRequest {
            name: "test-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }];

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_topics(&ctx, topics, true);
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

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_topics(&ctx, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_TOPIC_ALREADY_EXISTS);
    }

    #[test]
    fn test_delete_topics_success() {
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_id().returning(|_| Ok(Some(1)));
        store.expect_delete_topic().returning(|_| Ok(()));

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_delete_topics(&ctx, vec!["test-topic".to_string()]);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_delete_topics_not_found() {
        let mut store = MockKafkaStore::new();
        store.expect_get_topic_id().returning(|_| Ok(None));

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_delete_topics(&ctx, vec!["nonexistent".to_string()]);
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

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_partitions(&ctx, topics, false);
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

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_partitions(&ctx, topics, false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.results[0].error_code, ERROR_INVALID_PARTITIONS);
    }

    #[test]
    fn test_create_partitions_no_op_rejected() {
        // CONF-4: increasing to the current count is a no-op and must be rejected with
        // INVALID_PARTITIONS, not silently succeed.
        let mut store = MockKafkaStore::new();
        store
            .expect_get_topic_partition_count()
            .returning(|_| Ok(Some(3)));

        let topics = vec![CreatePartitionsTopicRequest {
            name: "test-topic".to_string(),
            count: 3, // Same as current
        }];

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let result = handle_create_partitions(&ctx, topics, false);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().results[0].error_code,
            ERROR_INVALID_PARTITIONS
        );
    }

    #[test]
    fn test_validate_topic_name() {
        // CONF-4: name validation mirrors Apache Kafka's Topic.validate.
        assert!(validate_topic_name("valid.topic_name-1").is_ok());
        assert!(validate_topic_name(&"x".repeat(249)).is_ok());
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
        assert!(validate_topic_name("has space").is_err());
        assert!(validate_topic_name("bad/slash").is_err());
        assert!(validate_topic_name(&"x".repeat(250)).is_err());
    }

    #[test]
    fn test_delete_groups_unknown_group_deletes_offsets() {
        // A group absent from the coordinator but holding committed offsets is an Empty
        // group: its offsets are deleted and the result is NONE.
        let mut store = MockKafkaStore::new();
        store.expect_group_has_offsets().returning(|_| Ok(true));
        store
            .expect_delete_consumer_group_offsets()
            .times(1)
            .returning(|_| Ok(()));

        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = make_test_context(&store, &coordinator, &broker);
        let response = handle_delete_groups(&ctx, vec!["ghost".to_string()]).unwrap();
        assert_eq!(response.results[0].error_code, ERROR_NONE);
    }

    // ----- wave3 groups -----

    fn ctx_for<'a>(
        store: &'a MockKafkaStore,
        coordinator: &'a crate::kafka::GroupCoordinator,
        broker: &'a crate::kafka::BrokerMetadata,
    ) -> HandlerContext<'a> {
        make_test_context(store, coordinator, broker)
    }

    fn topic(name: &str, partitions: i32, rf: i16) -> CreateTopicRequest {
        CreateTopicRequest {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor: rf,
        }
    }

    #[test]
    fn test_delete_groups_nonexistent_group_is_not_found() {
        // Kafka answers GROUP_ID_NOT_FOUND for a group that has neither members nor offsets.
        let mut store = MockKafkaStore::new();
        store.expect_group_has_offsets().returning(|_| Ok(false));
        store.expect_delete_consumer_group_offsets().times(0);
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_delete_groups(&ctx, vec!["nobody".to_string()]).unwrap();
        assert_eq!(response.results[0].error_code, ERROR_GROUP_ID_NOT_FOUND);
    }

    #[test]
    fn test_create_topics_partition_cap() {
        // Item 1: a huge partition count is refused before anything is stored (it used to be
        // written and then crash every Metadata request).
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        store
            .expect_create_topic()
            .withf(|name, n| name == "at-cap" && *n == 10_000)
            .times(1)
            .returning(|_, _| Ok(1));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_create_topics(
            &ctx,
            vec![
                topic("huge", 2_000_000_000, 1),
                topic("over", 10_001, 1),
                topic("at-cap", 10_000, 1),
                topic("neg", -2, 1),
            ],
            false,
        )
        .unwrap();
        let codes: Vec<i16> = response.topics.iter().map(|t| t.error_code).collect();
        assert_eq!(
            codes,
            vec![
                ERROR_INVALID_PARTITIONS,
                ERROR_INVALID_PARTITIONS,
                ERROR_NONE,
                ERROR_INVALID_PARTITIONS
            ]
        );
    }

    #[test]
    fn test_create_topics_minus_one_default_is_capped() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        store
            .expect_create_topic()
            .withf(|_, n| *n == crate::kafka::limits::DEFAULT_MAX_PARTITIONS_PER_TOPIC)
            .times(1)
            .returning(|_, _| Ok(1));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(
            &store,
            &coordinator,
            &broker,
            50_000,
            kafka_protocol::records::Compression::None,
        );
        let response = handle_create_topics(&ctx, vec![topic("d", -1, -1)], false).unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_NONE);
    }

    #[test]
    fn test_create_topics_replication_factor() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        store.expect_create_topic().times(2).returning(|_, _| Ok(1));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_create_topics(
            &ctx,
            vec![
                topic("rf1", 1, 1),
                topic("rfdefault", 1, -1),
                topic("rf3", 1, 3),
                topic("rf0", 1, 0),
                topic("rfneg", 1, -2),
            ],
            false,
        )
        .unwrap();
        let codes: Vec<i16> = response.topics.iter().map(|t| t.error_code).collect();
        assert_eq!(
            codes,
            vec![
                ERROR_NONE,
                ERROR_NONE,
                ERROR_INVALID_REPLICATION_FACTOR,
                ERROR_INVALID_REPLICATION_FACTOR,
                ERROR_INVALID_REPLICATION_FACTOR
            ]
        );
        assert!(response.topics[2]
            .error_message
            .as_ref()
            .unwrap()
            .contains("only 1 broker(s)"));
    }

    #[test]
    fn test_create_topics_duplicate_name_in_request() {
        // Kafka: one INVALID_REQUEST result for the repeated name, nothing created for it;
        // the other topics are unaffected.
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store.expect_find_colliding_topic().returning(|_| Ok(None));
        store
            .expect_create_topic()
            .withf(|name, _| name == "other")
            .times(1)
            .returning(|_, _| Ok(1));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_create_topics(
            &ctx,
            vec![topic("dup", 1, 1), topic("other", 1, 1), topic("dup", 1, 1)],
            false,
        )
        .unwrap();
        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].name.0.as_str(), "dup");
        assert_eq!(response.topics[0].error_code, ERROR_INVALID_REQUEST);
        assert_eq!(response.topics[1].error_code, ERROR_NONE);
    }

    #[test]
    fn test_create_topics_name_collision() {
        let mut store = MockKafkaStore::new();
        store.expect_topic_exists().returning(|_| Ok(false));
        store
            .expect_find_colliding_topic()
            .returning(|_| Ok(Some("a.b".to_string())));
        store.expect_create_topic().times(0);
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_create_topics(&ctx, vec![topic("a_b", 1, 1)], false).unwrap();
        assert_eq!(response.topics[0].error_code, ERROR_INVALID_TOPIC_EXCEPTION);
        assert_eq!(
            response.topics[0].error_message.as_ref().unwrap().as_str(),
            "Topic 'a_b' collides with existing topic: a.b"
        );
    }

    #[test]
    fn test_create_partitions_over_cap_rejected() {
        let mut store = MockKafkaStore::new();
        store
            .expect_get_topic_partition_count()
            .returning(|_| Ok(Some(1)));
        store.expect_set_topic_partition_count().times(0);
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_create_partitions(
            &ctx,
            vec![CreatePartitionsTopicRequest {
                name: "t".to_string(),
                count: 2_000_000_000,
            }],
            false,
        )
        .unwrap();
        assert_eq!(response.results[0].error_code, ERROR_INVALID_PARTITIONS);
    }
}
