//! Admin handlers: CreateTopics, DeleteTopics, CreatePartitions and DeleteGroups.

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

use super::helpers::{resolve_topics, validate_topic_name};
use crate::kafka::constants::{
    ERROR_GROUP_ID_NOT_FOUND, ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICATION_FACTOR,
    ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NON_EMPTY_GROUP,
    ERROR_POLICY_VIOLATION, ERROR_TOPIC_ALREADY_EXISTS, ERROR_UNKNOWN_SERVER_ERROR,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::{CreatePartitionsTopicRequest, CreateTopicRequest};
use std::collections::{BTreeSet, HashMap, HashSet};

/// Handle CreateTopics request
///
/// Validates each topic as Kafka 3.7 does and creates the valid ones:
/// - a name repeated in the request: one result, INVALID_REQUEST ("Duplicate topic name.");
/// - an invalid name, or one that collides with an existing topic once '.' and '_' are treated
///   as equal (`a.b` vs `a_b`): INVALID_TOPIC_EXCEPTION;
/// - `num_partitions` 0 or below -1, or above `pg_kafka.max_partitions_per_topic`:
///   INVALID_PARTITIONS (-1 means `pg_kafka.default_partitions`, capped at the maximum);
/// - `replication_factor` other than 1 or -1 (the default): INVALID_REPLICATION_FACTOR, since
///   this is a single-broker cluster;
/// - an existing topic: TOPIC_ALREADY_EXISTS;
/// - a topic that would take the partitions of all topics, with the ones the request creates
///   before it (or, with `validate_only`, would create), above `pg_kafka.max_total_partitions`:
///   POLICY_VIOLATION.
pub fn handle_create_topics(
    ctx: &HandlerContext,
    topics: Vec<CreateTopicRequest>,
    validate_only: bool,
) -> Result<CreateTopicsResponse> {
    let max_partitions = crate::kafka::limits::max_partitions_per_topic();

    // A name given more than once fails as a whole, reported once (Kafka answers one
    // INVALID_REQUEST entry and creates nothing for it).
    let repeated = repeated_names(topics.iter().map(|t| t.name.as_str()));
    // The existing topics the names equal or collide with, in one lookup however many
    // topics the request names.
    let wanted: Vec<String> = topics
        .iter()
        .filter(|t| !repeated.contains(&t.name) && validate_topic_name(&t.name).is_ok())
        .map(|t| t.name.clone())
        .collect();
    let mut existing = if wanted.is_empty() {
        Ok(ExistingTopics::new(Vec::new()))
    } else {
        ctx.store
            .colliding_topics(&wanted)
            .map(ExistingTopics::new)
            .map_err(|e| format!("Error checking topic: {e}"))
    };
    let mut reported = HashSet::new();
    let mut budget = PartitionBudget::new(ctx.store);

    let mut results = Vec::new();
    for topic in topics {
        if !reported.insert(topic.name.clone()) {
            continue;
        }
        results.push(if repeated.contains(&topic.name) {
            failed_topic(
                &topic.name,
                ERROR_INVALID_REQUEST,
                "Duplicate topic name.".to_string(),
            )
        } else {
            create_topic_result(
                ctx,
                &topic,
                max_partitions,
                validate_only,
                &mut existing,
                &mut budget,
            )
        });
    }

    let mut response = CreateTopicsResponse::default();
    response.throttle_time_ms = 0;
    response.topics = results;
    Ok(response)
}

/// Validate one requested topic and, unless `validate_only`, create it.
fn create_topic_result(
    ctx: &HandlerContext,
    topic: &CreateTopicRequest,
    max_partitions: i32,
    validate_only: bool,
    existing: &mut std::result::Result<ExistingTopics, String>,
    budget: &mut PartitionBudget<'_>,
) -> CreatableTopicResult {
    let num_partitions = match validate_new_topic(ctx, topic, max_partitions, existing)
        .and_then(|n| budget.take(n).map(|()| n))
    {
        Ok(num_partitions) => num_partitions,
        Err((code, message)) => return failed_topic(&topic.name, code, message),
    };
    if validate_only {
        return created_topic(&topic.name, num_partitions);
    }
    match ctx.store.create_topic(&topic.name, num_partitions) {
        Ok(_topic_id) => {
            if let Ok(existing) = existing {
                existing.add(topic.name.clone());
            }
            created_topic(&topic.name, num_partitions)
        }
        Err(e) => failed_topic(
            &topic.name,
            ERROR_UNKNOWN_SERVER_ERROR,
            format!("Failed to create topic: {e}"),
        ),
    }
}

/// The partition count a requested topic gets, or the error code and message that refuse
/// it: an invalid name, partition count or replication factor, an existing topic, or a name
/// that collides with one.
fn validate_new_topic(
    ctx: &HandlerContext,
    topic: &CreateTopicRequest,
    max_partitions: i32,
    existing: &std::result::Result<ExistingTopics, String>,
) -> std::result::Result<i32, (i16, String)> {
    // Length, charset, and "."/".." in addition to empty.
    validate_topic_name(&topic.name)
        .map_err(|msg| (ERROR_INVALID_TOPIC_EXCEPTION, msg.to_string()))?;

    // -1 means "use the broker default" (pg_kafka.default_partitions, capped like an
    // auto-created topic); any other value < 1 (e.g. 0) is rejected.
    let num_partitions = if topic.num_partitions == -1 {
        crate::kafka::limits::auto_create_partitions(ctx.default_partitions)
    } else {
        topic.num_partitions
    };
    if num_partitions < 1 {
        return Err((
            ERROR_INVALID_PARTITIONS,
            "Number of partitions was set to an invalid non-positive value.".to_string(),
        ));
    }
    crate::kafka::limits::check_partition_count(num_partitions, max_partitions)
        .map_err(|msg| (ERROR_INVALID_PARTITIONS, msg))?;
    validate_replication_factor(topic.replication_factor)
        .map_err(|msg| (ERROR_INVALID_REPLICATION_FACTOR, msg))?;

    let existing = existing
        .as_ref()
        .map_err(|message| (ERROR_UNKNOWN_SERVER_ERROR, message.clone()))?;
    if existing.contains(&topic.name) {
        return Err((
            ERROR_TOPIC_ALREADY_EXISTS,
            format!("Topic '{}' already exists.", topic.name),
        ));
    }
    // Kafka rejects a name that collides with an existing one once '.' and '_' are
    // treated as equal (their metric names would clash).
    if let Some(existing) = existing.colliding(&topic.name) {
        return Err((
            ERROR_INVALID_TOPIC_EXCEPTION,
            format!(
                "Topic '{}' collides with existing topic: {}",
                topic.name, existing
            ),
        ));
    }
    Ok(num_partitions)
}

/// What `pg_kafka.max_total_partitions` leaves to an admin request: the partitions of all
/// topics, read once (on the first topic that needs it), plus the ones the request has taken
/// so far. All topic creation runs on the worker's single DB thread, inside the request's
/// transaction, so nothing else adds partitions between the read and the creates.
struct PartitionBudget<'a> {
    store: &'a dyn crate::kafka::storage::KafkaStore,
    max: i64,
    /// The total so far; `None` until read, `Err` with its message if the read failed.
    total: Option<std::result::Result<i64, String>>,
}

impl<'a> PartitionBudget<'a> {
    fn new(store: &'a dyn crate::kafka::storage::KafkaStore) -> Self {
        Self {
            store,
            max: crate::kafka::limits::max_total_partitions(),
            total: None,
        }
    }

    /// Take `adding` partitions, or the error code and message that refuse them.
    fn take(&mut self, adding: i32) -> std::result::Result<(), (i16, String)> {
        let store = self.store;
        let total = self.total.get_or_insert_with(|| {
            store
                .total_partition_count()
                .map_err(|e| format!("Error counting partitions: {e}"))
        });
        let current = *total
            .as_ref()
            .map_err(|message| (ERROR_UNKNOWN_SERVER_ERROR, message.clone()))?;
        let adding = i64::from(adding);
        crate::kafka::limits::check_total_partitions(current, adding, self.max)
            .map_err(|message| (ERROR_POLICY_VIOLATION, message))?;
        *total = Ok(current + adding.max(0));
        Ok(())
    }
}

/// The existing topics a CreateTopics request's names equal or collide with (once '.' and
/// '_' are treated as equal), with the topics the request creates added as it goes.
struct ExistingTopics {
    /// Topic names by their name with '.' replaced by '_', sorted.
    by_normalized: HashMap<String, BTreeSet<String>>,
}

impl ExistingTopics {
    fn new(names: Vec<String>) -> Self {
        let mut existing = Self {
            by_normalized: HashMap::new(),
        };
        for name in names {
            existing.add(name);
        }
        existing
    }

    fn add(&mut self, name: String) {
        self.by_normalized
            .entry(name.replace('.', "_"))
            .or_default()
            .insert(name);
    }

    fn contains(&self, name: &str) -> bool {
        self.by_normalized
            .get(&name.replace('.', "_"))
            .is_some_and(|names| names.contains(name))
    }

    /// The first existing topic, by name, that collides with `name` and is not `name`.
    fn colliding(&self, name: &str) -> Option<&String> {
        self.by_normalized
            .get(&name.replace('.', "_"))?
            .iter()
            .find(|existing| *existing != name)
    }
}

/// The names `names` gives more than once.
fn repeated_names<'a>(names: impl IntoIterator<Item = &'a str>) -> HashSet<String> {
    let mut seen = HashSet::new();
    names
        .into_iter()
        .filter(|name| !seen.insert(*name))
        .map(str::to_string)
        .collect()
}

/// `names` without repeats, in the order first given.
fn once_each(names: Vec<String>) -> impl Iterator<Item = String> {
    let mut seen = HashSet::new();
    names
        .into_iter()
        .filter(move |name| seen.insert(name.clone()))
}

/// A CreateTopics result for a refused topic.
fn failed_topic(name: &str, error_code: i16, message: String) -> CreatableTopicResult {
    let mut result = CreatableTopicResult::default();
    result.name = TopicName(StrBytes::from_string(name.to_string()));
    result.num_partitions = -1;
    result.replication_factor = -1;
    result.error_code = error_code;
    result.error_message = Some(StrBytes::from_string(message));
    result
}

/// A CreateTopics result for a topic that is (or, with validate_only, would be) created;
/// the replication factor is always 1 on this single-node broker.
fn created_topic(name: &str, num_partitions: i32) -> CreatableTopicResult {
    let mut result = CreatableTopicResult::default();
    result.name = TopicName(StrBytes::from_string(name.to_string()));
    result.error_code = ERROR_NONE;
    result.num_partitions = num_partitions;
    result.replication_factor = 1;
    result
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
    let repeated = repeated_names(topic_names.iter().map(String::as_str));
    // Every topic in one lookup however many the request names.
    let found = resolve_topics(store, topic_names.iter().map(String::as_str));

    for topic_name in once_each(topic_names) {
        let mut result = DeletableTopicResult::default();
        result.name = Some(TopicName(StrBytes::from_string(topic_name.clone())));
        if repeated.contains(&topic_name) {
            // Kafka 3.7 answers a name given more than once with one INVALID_REQUEST
            // entry and deletes nothing for it.
            result.error_code = ERROR_INVALID_REQUEST;
            result.error_message = Some(StrBytes::from_static_str("Duplicate topic name."));
            results.push(result);
            continue;
        }

        match found
            .as_ref()
            .map(|found| found.get(&topic_name).map(|&(id, _)| id))
        {
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
            Err(&code) => {
                result.error_code = code;
                result.error_message = Some(StrBytes::from_static_str("Error looking up topic"));
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
/// Cannot decrease partition count, and refuses growth above `pg_kafka.max_partitions_per_topic`
/// (INVALID_PARTITIONS) or that would take the partitions of all topics above
/// `pg_kafka.max_total_partitions` (POLICY_VIOLATION).
pub fn handle_create_partitions(
    ctx: &HandlerContext,
    topics: Vec<CreatePartitionsTopicRequest>,
    validate_only: bool,
) -> Result<CreatePartitionsResponse> {
    let store = ctx.store;
    let mut results = Vec::new();
    let repeated = repeated_names(topics.iter().map(|t| t.name.as_str()));
    // Every topic in one lookup however many the request names.
    let found = resolve_topics(store, topics.iter().map(|t| t.name.as_str()));
    let mut reported = HashSet::new();
    let mut budget = PartitionBudget::new(ctx.store);

    for topic in topics {
        if !reported.insert(topic.name.clone()) {
            continue;
        }
        let mut result = CreatePartitionsTopicResult::default();
        result.name = TopicName(StrBytes::from_string(topic.name.clone()));
        if repeated.contains(&topic.name) {
            // Kafka 3.7 answers a name given more than once with one INVALID_REQUEST
            // entry and changes nothing for it.
            result.error_code = ERROR_INVALID_REQUEST;
            result.error_message = Some(StrBytes::from_static_str("Duplicate topic name."));
            results.push(result);
            continue;
        }

        match found
            .as_ref()
            .map(|found| found.get(&topic.name).map(|&(_, count)| count))
        {
            Ok(Some(current_count)) => {
                if topic.count < current_count {
                    result.error_code = ERROR_INVALID_PARTITIONS;
                    result.error_message = Some(StrBytes::from_string(format!(
                        "Cannot reduce partition count from {} to {}",
                        current_count, topic.count
                    )));
                } else if topic.count == current_count {
                    // Kafka rejects a no-op partition count as INVALID_PARTITIONS
                    // ("Topic already has N partitions") rather than silently succeeding.
                    result.error_code = ERROR_INVALID_PARTITIONS;
                    result.error_message = Some(StrBytes::from_string(format!(
                        "Topic already has {current_count} partitions"
                    )));
                } else if let Err(msg) = crate::kafka::limits::check_partition_count(
                    topic.count,
                    crate::kafka::limits::max_partitions_per_topic(),
                ) {
                    // The per-topic partition cap applies to growth too.
                    result.error_code = ERROR_INVALID_PARTITIONS;
                    result.error_message = Some(StrBytes::from_string(msg));
                } else if let Err((code, msg)) = budget.take(topic.count - current_count) {
                    // So does the cap on all topics' partitions.
                    result.error_code = code;
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
            Err(&code) => {
                result.error_code = code;
                result.error_message = Some(StrBytes::from_static_str("Error looking up topic"));
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
/// (returns NON_EMPTY_GROUP error). A group named more than once is deleted and answered
/// once, as Kafka 3.7 does (it deletes `groupIds.toSet`).
pub fn handle_delete_groups(
    ctx: &HandlerContext,
    group_names: Vec<String>,
) -> Result<DeleteGroupsResponse> {
    let group_names: Vec<String> = once_each(group_names).collect();
    let store = ctx.store;
    let coordinator = ctx.coordinator;
    let mut results = Vec::new();
    // Which of the groups the coordinator does not hold have offsets (committed, or pending
    // in an open transaction), in one lookup however many groups the request names.
    let unheld: Vec<String> = group_names
        .iter()
        .filter(|g| !coordinator.has_group(g))
        .cloned()
        .collect();
    let with_offsets = if unheld.is_empty() {
        Ok(HashSet::new())
    } else {
        store.groups_with_offsets(&unheld)
    };

    for group_name in group_names {
        let mut result = DeletableGroupResult::default();
        result.group_id = GroupId(StrBytes::from_string(group_name.clone()));

        // Check if group exists and has members
        match coordinator.group_is_empty(&group_name) {
            Some(empty) => {
                // Check if group has active members
                if !empty {
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
                // A group the coordinator does not hold exists only through its offsets,
                // committed or pending in an open transaction (an Empty group, as
                // DescribeGroups/ListGroups report it; Kafka 3.7's TxnOffsetCommit creates
                // the group). Without any, it does not exist: GROUP_ID_NOT_FOUND, as Kafka
                // answers.
                match &with_offsets {
                    Ok(groups) if !groups.contains(&group_name) => {
                        result.error_code = ERROR_GROUP_ID_NOT_FOUND;
                    }
                    Ok(_) => match store.delete_consumer_group_offsets(&group_name) {
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store
            .expect_colliding_topics()
            .returning(|names| Ok(names.to_vec()));

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
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(None));

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
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 5))));

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
        // Increasing to the current count is a no-op and must be rejected with
        // INVALID_PARTITIONS, not silently succeed.
        let mut store = MockKafkaStore::new();
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 3))));

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
        // Name validation mirrors Apache Kafka's Topic.validate.
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store
            .expect_groups_with_offsets()
            .returning(|groups| Ok(groups.iter().cloned().collect()));
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

    // ----- CreateTopics / CreatePartitions validation and offsets-only groups -----

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

    /// Kafka 3.7 answers one result per distinct group (checked against a 3.7 broker:
    /// `["gdup", "gdup", "nosuchg", "nosuchg"]` is `[("gdup", NONE), ("nosuchg",
    /// GROUP_ID_NOT_FOUND)]`) and deletes a repeated group once.
    #[test]
    fn test_delete_groups_answers_a_repeated_group_once() {
        let mut store = MockKafkaStore::new();
        store.expect_total_partition_count().returning(|| Ok(0));
        store
            .expect_groups_with_offsets()
            .withf(|groups| groups == ["gdup", "nosuchg"])
            .times(1)
            .returning(|_| Ok(HashSet::from(["gdup".to_string()])));
        store
            .expect_delete_consumer_group_offsets()
            .withf(|group| group == "gdup")
            .times(1)
            .returning(|_| Ok(()));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let names = ["gdup", "gdup", "nosuchg", "nosuchg", "gdup"].map(String::from);
        let response = handle_delete_groups(&ctx, names.to_vec()).unwrap();
        let answers: Vec<(String, i16)> = response
            .results
            .iter()
            .map(|r| (r.group_id.0.to_string(), r.error_code))
            .collect();
        assert_eq!(
            answers,
            [
                ("gdup".to_string(), ERROR_NONE),
                ("nosuchg".to_string(), ERROR_GROUP_ID_NOT_FOUND)
            ]
        );
    }

    #[test]
    fn test_delete_groups_nonexistent_group_is_not_found() {
        // Kafka answers GROUP_ID_NOT_FOUND for a group that has neither members nor offsets.
        let mut store = MockKafkaStore::new();
        store.expect_total_partition_count().returning(|| Ok(0));
        store
            .expect_groups_with_offsets()
            .returning(|_| Ok(HashSet::new()));
        store.expect_delete_consumer_group_offsets().times(0);
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let response = handle_delete_groups(&ctx, vec!["nobody".to_string()]).unwrap();
        assert_eq!(response.results[0].error_code, ERROR_GROUP_ID_NOT_FOUND);
    }

    #[test]
    fn test_create_topics_partition_cap() {
        // A huge partition count is refused before anything is stored; stored, it would make
        // every Metadata request allocate for it.
        let mut store = MockKafkaStore::new();
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store.expect_colliding_topics().returning(|_| Ok(vec![]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        store
            .expect_colliding_topics()
            .returning(|_| Ok(vec!["a.b".to_string()]));
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
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
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

    /// Kafka 3.7 answers a DeleteTopics name given more than once with one INVALID_REQUEST
    /// entry and deletes nothing for it.
    #[test]
    fn test_delete_topics_repeated_name_is_one_invalid_request() {
        let mut store = MockKafkaStore::new();
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |name| Ok(Some((i32::from(name == "x"), 1))));
        store
            .expect_delete_topic()
            .with(mockall::predicate::eq(1))
            .times(1)
            .returning(|_| Ok(()));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let mut names = vec!["dup".to_string(); 10_000];
        names.insert(1, "x".to_string());
        let response = handle_delete_topics(&ctx, names).unwrap();
        let answers: Vec<(String, i16)> = response
            .responses
            .iter()
            .map(|r| {
                let name = r
                    .name
                    .as_ref()
                    .map_or_else(String::new, |n| n.0.to_string());
                (name, r.error_code)
            })
            .collect();
        assert_eq!(
            answers,
            [
                ("dup".to_string(), ERROR_INVALID_REQUEST),
                ("x".to_string(), ERROR_NONE)
            ]
        );
    }

    /// `pg_kafka.max_total_partitions` bounds the partitions of all topics together: with
    /// the default cap and 10,000 partitions already there, a CreateTopics of topics at the
    /// per-topic maximum creates them only while the total fits, counting the request's own
    /// topics (validate_only too), and CreatePartitions grows a topic only within it. Without
    /// the total cap one CreateTopics of 10,000 such topics made 1e8 partitions, and every
    /// all-topics Metadata then allocated one entry per partition.
    #[test]
    fn test_total_partition_cap() {
        let max = crate::kafka::limits::max_total_partitions();
        let per_topic = crate::kafka::limits::max_partitions_per_topic();
        let existing = 10_000_i64;
        let fit = usize::try_from((max - existing) / i64::from(per_topic)).unwrap();
        for validate_only in [false, true] {
            let mut store = MockKafkaStore::new();
            store
                .expect_total_partition_count()
                .times(1)
                .returning(move || Ok(existing));
            store.expect_colliding_topics().returning(|_| Ok(vec![]));
            store
                .expect_create_topic()
                .times(if validate_only { 0 } else { fit })
                .returning(|_, _| Ok(1));
            let coordinator = crate::kafka::GroupCoordinator::new();
            let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
            let ctx = ctx_for(&store, &coordinator, &broker);
            let topics = (0..fit + 2)
                .map(|i| topic(&format!("t{i}"), per_topic, 1))
                .collect();
            let response = handle_create_topics(&ctx, topics, validate_only).unwrap();
            let codes: Vec<i16> = response.topics.iter().map(|t| t.error_code).collect();
            assert_eq!(codes[..fit], vec![ERROR_NONE; fit][..], "{validate_only}");
            assert_eq!(codes[fit..], [ERROR_POLICY_VIOLATION; 2], "{validate_only}");
            let message = response.topics[fit].error_message.as_ref().unwrap();
            assert!(
                message.contains("pg_kafka.max_total_partitions"),
                "{message}"
            );
        }

        // CreatePartitions: growth that fits is applied, growth that does not is refused.
        let mut store = MockKafkaStore::new();
        store
            .expect_total_partition_count()
            .times(1)
            .returning(move || Ok(max - 5));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
        store
            .expect_set_topic_partition_count()
            .withf(|name, count| name == "a" && *count == 4)
            .times(1)
            .returning(|_, _| Ok(()));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let grow = |name: &str, count| CreatePartitionsTopicRequest {
            name: name.to_string(),
            count,
        };
        let response =
            handle_create_partitions(&ctx, vec![grow("a", 4), grow("b", 4)], false).unwrap();
        let codes: Vec<i16> = response.results.iter().map(|r| r.error_code).collect();
        assert_eq!(codes, [ERROR_NONE, ERROR_POLICY_VIOLATION]);
    }

    /// Kafka 3.7 answers a CreatePartitions name given more than once with one
    /// INVALID_REQUEST entry and changes nothing for it.
    #[test]
    fn test_create_partitions_repeated_name_is_one_invalid_request() {
        let mut store = MockKafkaStore::new();
        store.expect_total_partition_count().returning(|| Ok(0));
        crate::testing::expect_topics(&mut store, |_| Ok(Some((1, 1))));
        store
            .expect_set_topic_partition_count()
            .withf(|name, count| name == "x" && *count == 2)
            .times(1)
            .returning(|_, _| Ok(()));
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = ctx_for(&store, &coordinator, &broker);
        let request = |name: &str| CreatePartitionsTopicRequest {
            name: name.to_string(),
            count: 2,
        };
        let response = handle_create_partitions(
            &ctx,
            vec![request("dup"), request("x"), request("dup")],
            false,
        )
        .unwrap();
        let codes: Vec<(&str, i16)> = response
            .results
            .iter()
            .map(|r| (r.name.0.as_str(), r.error_code))
            .collect();
        assert_eq!(codes, [("dup", ERROR_INVALID_REQUEST), ("x", ERROR_NONE)]);
        assert_eq!(
            response.results[0].error_message.as_deref(),
            Some("Duplicate topic name.")
        );
    }
}
