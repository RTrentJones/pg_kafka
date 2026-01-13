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
    match store.get_topic_metadata(Some(std::slice::from_ref(&topic_name.to_string()))) {
        Ok(topics) => {
            if let Some(tm) = topics.first() {
                TopicResolution::Found(tm.id)
            } else {
                TopicResolution::NotFound
            }
        }
        Err(e) => {
            // Use typed error for logging and error code
            if e.is_server_error() {
                crate::pg_warning!("Failed to resolve topic '{}': {}", topic_name, e);
            }
            TopicResolution::Error(e.to_kafka_error_code())
        }
    }
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
