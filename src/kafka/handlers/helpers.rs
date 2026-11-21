//! Helpers shared by the handlers: topic resolution and validation, and error-code mapping.

use crate::kafka::constants::{ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION};
use crate::kafka::error::KafkaError;
use crate::kafka::storage::{TopicStore, TransactionState, TransactionStore};
use std::collections::{HashMap, HashSet};

/// Result of resolving a topic name to its ID
pub enum TopicResolution {
    /// Topic found, contains topic_id
    Found(i32),
    /// Topic not found
    NotFound,
    /// Storage error occurred - contains the error code from the typed error
    Error(i16),
}

/// Resolves a topic name to its topic_id using the storage layer.
///
/// Returns:
/// - `TopicResolution::Found(topic_id)` if the topic exists
/// - `TopicResolution::NotFound` if the topic doesn't exist
/// - `TopicResolution::Error(code)` if a storage error occurred
pub fn resolve_topic_id<S: TopicStore + ?Sized>(store: &S, topic_name: &str) -> TopicResolution {
    match resolve_topic(store, topic_name) {
        Ok(Some((id, _))) => TopicResolution::Found(id),
        Ok(None) => TopicResolution::NotFound,
        Err(code) => TopicResolution::Error(code),
    }
}

/// Resolves a topic name to `(topic_id, partition_count)` with a single metadata lookup.
///
/// Returns `Ok(None)` if the topic doesn't exist, and `Err(code)` (the Kafka error code
/// of the typed storage error) if the lookup failed.
pub fn resolve_topic<S: TopicStore + ?Sized>(
    store: &S,
    topic_name: &str,
) -> Result<Option<(i32, i32)>, i16> {
    match store.get_topic_metadata(Some(std::slice::from_ref(&topic_name.to_string()))) {
        Ok(topics) => Ok(topics.first().map(|tm| (tm.id, tm.partition_count))),
        Err(e) => {
            // Use typed error for logging and error code
            if e.is_server_error() {
                crate::pg_warning!("Failed to resolve topic '{}': {}", topic_name, e);
            }
            Err(e.to_kafka_error_code())
        }
    }
}

/// The distinct names of `names`, in the order first named.
pub fn distinct_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut seen = HashSet::new();
    names
        .into_iter()
        .filter(|name| seen.insert(*name))
        .map(str::to_string)
        .collect()
}

/// `(topic_id, partition_count)` of each of `names` that names a topic, from one lookup of
/// the distinct names; or the Kafka error code of a failed lookup. A request may repeat a
/// name thousands of times or name thousands of topics, and the DB thread serves every
/// client: one statement per entry would stall the whole broker.
pub fn resolve_topics<'a, S: TopicStore + ?Sized>(
    store: &S,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<HashMap<String, (i32, i32)>, i16> {
    let names = distinct_names(names);
    match store.get_topic_metadata(Some(&names)) {
        Ok(found) => Ok(found
            .into_iter()
            .map(|tm| (tm.name, (tm.id, tm.partition_count)))
            .collect()),
        Err(e) => {
            if e.is_server_error() {
                crate::pg_warning!("Failed to resolve {} topics: {}", names.len(), e);
            }
            Err(e.to_kafka_error_code())
        }
    }
}

/// Merge entries repeated as Kafka keys them, by topic and partition: one entry per
/// distinct topic in the order first named, holding one entry per distinct partition in
/// the order first named, with the request's last value for it. Kafka 3.7 collects
/// OffsetCommit, TxnOffsetCommit, DeleteRecords and Fetch partitions into a map before it
/// acts, so a repeated partition is acted on once, with its last entry, and answered once.
pub fn merge_by_partition<P>(
    topics: impl IntoIterator<Item = (String, Vec<P>)>,
    partition_of: impl Fn(&P) -> i32,
) -> Vec<(String, Vec<P>)> {
    let mut merged: Vec<(String, Vec<P>)> = Vec::new();
    let mut topic_at: HashMap<String, usize> = HashMap::new();
    let mut partition_at: Vec<HashMap<i32, usize>> = Vec::new();
    for (name, partitions) in topics {
        let t = if let Some(&t) = topic_at.get(&name) {
            t
        } else {
            topic_at.insert(name.clone(), merged.len());
            merged.push((name, Vec::new()));
            partition_at.push(HashMap::new());
            merged.len() - 1
        };
        for partition in partitions {
            let entries = &mut merged[t].1;
            match partition_at[t].entry(partition_of(&partition)) {
                std::collections::hash_map::Entry::Occupied(at) => entries[*at.get()] = partition,
                std::collections::hash_map::Entry::Vacant(at) => {
                    at.insert(entries.len());
                    entries.push(partition);
                }
            }
        }
    }
    merged
}

/// Validate a topic name against Kafka's rules (mirrors `Topic.validate` in Apache Kafka):
/// non-empty, not "." or "..", at most 249 characters, and only `[a-zA-Z0-9._-]`.
pub fn validate_topic_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Topic name cannot be empty");
    }
    if name == "." || name == ".." {
        return Err("Topic name cannot be \".\" or \"..\"");
    }
    if name.len() > 249 {
        return Err("Topic name cannot be longer than 249 characters");
    }
    let allowed = |c: char| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-';
    if !name.chars().all(allowed) {
        return Err("Topic name may only contain ASCII alphanumerics, '.', '_', and '-'");
    }
    Ok(())
}

/// `get_or_create_topic` for the auto-create paths (Metadata, Produce,
/// AddPartitionsToTxn), with the same name rules CreateTopics enforces. Only a name
/// that fails validation costs an extra lookup, so a topic created before the rules
/// were enforced on auto-create keeps working.
pub fn get_or_create_valid_topic<S: TopicStore + ?Sized>(
    store: &S,
    topic_name: &str,
    default_partitions: i32,
) -> crate::kafka::error::Result<(i32, i32)> {
    if let Err(reason) = validate_topic_name(topic_name) {
        let existing =
            store.get_topic_metadata(Some(std::slice::from_ref(&topic_name.to_string())))?;
        return match existing.first() {
            Some(tm) => Ok((tm.id, tm.partition_count)),
            None => Err(KafkaError::InvalidTopic {
                topic: topic_name.to_string(),
                reason: reason.to_string(),
            }),
        };
    }
    // Never auto-create above pg_kafka.max_partitions_per_topic.
    store.get_or_create_topic(
        topic_name,
        crate::kafka::limits::auto_create_partitions(default_partitions),
    )
}

/// Returns the appropriate Kafka error code for a topic resolution failure.
pub fn topic_resolution_error_code(resolution: &TopicResolution) -> i16 {
    match resolution {
        TopicResolution::Found(_) => ERROR_NONE,
        TopicResolution::NotFound => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        TopicResolution::Error(code) => *code,
    }
}

/// Check that `txn_id` belongs to this producer and epoch and is in the Ongoing state;
/// `api` names the request in the INVALID_TXN_STATE error.
pub(super) fn validate_ongoing_transaction(
    store: &dyn TransactionStore,
    txn_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    api: &str,
) -> crate::kafka::error::Result<()> {
    store.validate_transaction(txn_id, producer_id, producer_epoch)?;
    match store.get_transaction_state(txn_id)? {
        Some(TransactionState::Ongoing) => Ok(()),
        Some(other) => Err(KafkaError::invalid_txn_state(
            txn_id,
            format!("{other:?}"),
            api,
        )),
        None => Err(KafkaError::transactional_id_not_found(txn_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mocks::MockKafkaStore;

    // ========== TopicResolution Enum Tests ==========

    #[test]
    fn test_topic_resolution_found_variant() {
        let resolution = TopicResolution::Found(42);
        if let TopicResolution::Found(id) = resolution {
            assert_eq!(id, 42);
        } else {
            panic!("Expected Found variant");
        }
    }

    #[test]
    fn test_topic_resolution_not_found_variant() {
        let resolution = TopicResolution::NotFound;
        assert!(matches!(resolution, TopicResolution::NotFound));
    }

    #[test]
    fn test_topic_resolution_error_variant() {
        let resolution = TopicResolution::Error(3); // UNKNOWN_TOPIC_OR_PARTITION
        if let TopicResolution::Error(code) = resolution {
            assert_eq!(code, 3);
        } else {
            panic!("Expected Error variant");
        }
    }

    // ========== topic_resolution_error_code Tests ==========

    #[test]
    fn test_topic_resolution_error_code_found() {
        let resolution = TopicResolution::Found(1);
        let code = topic_resolution_error_code(&resolution);
        assert_eq!(code, ERROR_NONE);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_topic_resolution_error_code_not_found() {
        let resolution = TopicResolution::NotFound;
        let code = topic_resolution_error_code(&resolution);
        assert_eq!(code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(code, 3);
    }

    #[test]
    fn test_topic_resolution_error_code_error() {
        let resolution = TopicResolution::Error(42);
        let code = topic_resolution_error_code(&resolution);
        assert_eq!(code, 42);
    }

    // ========== resolve_topic_id Tests with MockKafkaStore ==========

    #[test]
    fn test_resolve_existing_topic_returns_found() {
        use crate::kafka::storage::TopicMetadata;

        let mut store = MockKafkaStore::new();
        // Set up expectation for get_topic_metadata to return a topic
        store.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                id: 42,
                name: "existing-topic".to_string(),
                partition_count: 1,
            }])
        });

        let resolution = resolve_topic_id(&store, "existing-topic");
        if let TopicResolution::Found(id) = resolution {
            assert_eq!(id, 42);
        } else {
            panic!("Expected Found variant for existing topic");
        }
    }

    #[test]
    fn test_resolve_unknown_topic_returns_not_found() {
        let mut store = MockKafkaStore::new();
        // Set up expectation for get_topic_metadata to return empty
        store.expect_get_topic_metadata().returning(|_| Ok(vec![]));

        let resolution = resolve_topic_id(&store, "nonexistent-topic");
        assert!(
            matches!(resolution, TopicResolution::NotFound),
            "Expected NotFound for nonexistent topic"
        );
    }

    #[test]
    fn test_resolve_topic_returns_correct_id() {
        use crate::kafka::storage::TopicMetadata;

        let mut store = MockKafkaStore::new();
        // Set up expectation to return topic with ID 123
        store.expect_get_topic_metadata().returning(|_| {
            Ok(vec![TopicMetadata {
                id: 123,
                name: "test-topic".to_string(),
                partition_count: 3,
            }])
        });

        // Verify resolution returns correct ID
        if let TopicResolution::Found(id) = resolve_topic_id(&store, "test-topic") {
            assert_eq!(id, 123);
        } else {
            panic!("Expected Found variant");
        }
    }

    #[test]
    fn test_resolve_topic_error_returns_error() {
        use crate::kafka::error::KafkaError;

        let mut store = MockKafkaStore::new();
        // Set up expectation to return an error
        store
            .expect_get_topic_metadata()
            .returning(|_| Err(KafkaError::unknown_topic("test")));

        let resolution = resolve_topic_id(&store, "test-topic");
        if let TopicResolution::Error(code) = resolution {
            // UNKNOWN_TOPIC_OR_PARTITION = 3
            assert_eq!(code, 3);
        } else {
            panic!("Expected Error variant");
        }
    }

    // ========== distinct_names / merge_by_partition ==========

    #[test]
    fn test_distinct_names_keep_the_first_order() {
        assert_eq!(distinct_names(["b", "a", "b", "c", "a"]), ["b", "a", "c"]);
    }

    /// One entry per distinct topic and partition, in the order first named, holding the
    /// last value for it.
    #[test]
    fn test_merge_by_partition_keeps_the_last_entry_in_first_order() {
        let topics = vec![
            ("t".to_string(), vec![(0, 'a'), (1, 'b'), (0, 'c')]),
            ("u".to_string(), vec![(0, 'd')]),
            ("t".to_string(), vec![(2, 'e'), (1, 'f')]),
        ];
        let merged = merge_by_partition(topics, |&(p, _)| p);
        assert_eq!(
            merged,
            [
                ("t".to_string(), vec![(0, 'c'), (1, 'f'), (2, 'e')]),
                ("u".to_string(), vec![(0, 'd')]),
            ]
        );
    }

    /// Merging costs time linear in the entries, however many repeat one partition or
    /// one topic.
    #[test]
    fn test_merge_by_partition_is_linear() {
        let growth = crate::testing::growth(50_000, |n| {
            let topics: Vec<(String, Vec<i32>)> = (0..n)
                .map(|i| ("t".to_string(), vec![0, i32::try_from(i % 64).unwrap_or(0)]))
                .collect();
            assert_eq!(merge_by_partition(topics, |&p| p).len(), 1);
        });
        assert!(growth < 10.0, "4x the entries cost {growth:.1}x");
    }
}
