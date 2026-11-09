// Handler helper functions
//
// These helpers reduce code duplication across handlers by providing
// common operations like topic resolution and error code mapping.

use crate::kafka::constants::{ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION};
use crate::kafka::error::KafkaError;
use crate::kafka::storage::KafkaStore;

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
pub fn resolve_topic_id<S: KafkaStore + ?Sized>(store: &S, topic_name: &str) -> TopicResolution {
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
pub fn resolve_topic<S: KafkaStore + ?Sized>(
    store: &S,
    topic_name: &str,
) -> std::result::Result<Option<(i32, i32)>, i16> {
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

/// Validate a topic name against Kafka's rules (mirrors `Topic.validate` in Apache Kafka):
/// non-empty, not "." or "..", at most 249 characters, and only `[a-zA-Z0-9._-]`. (CONF-4)
pub fn validate_topic_name(name: &str) -> std::result::Result<(), &'static str> {
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
pub fn get_or_create_valid_topic<S: KafkaStore + ?Sized>(
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
    store.get_or_create_topic(topic_name, default_partitions)
}

/// Returns the appropriate Kafka error code for a topic resolution failure.
pub fn topic_resolution_error_code(resolution: &TopicResolution) -> i16 {
    match resolution {
        TopicResolution::Found(_) => ERROR_NONE,
        TopicResolution::NotFound => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        TopicResolution::Error(code) => *code,
    }
}

/// Creates an UnknownTopic error with proper Kafka error code
#[allow(dead_code)]
pub fn unknown_topic_error(topic: &str) -> KafkaError {
    KafkaError::unknown_topic(topic)
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

    // ========== unknown_topic_error Tests ==========

    #[test]
    fn test_unknown_topic_error_creates_error() {
        let err = unknown_topic_error("my-missing-topic");
        assert!(matches!(err, KafkaError::UnknownTopic { .. }));
        let msg = err.to_string();
        assert!(msg.contains("my-missing-topic"));
    }

    #[test]
    fn test_unknown_topic_error_has_correct_code() {
        let err = unknown_topic_error("test-topic");
        let code = err.to_kafka_error_code();
        assert_eq!(code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
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
}
