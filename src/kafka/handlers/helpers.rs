// Handler helper functions
//
// These helpers reduce code duplication across handlers by providing
// common operations like topic resolution and error code mapping.

use crate::kafka::constants::*;
use crate::kafka::storage::KafkaStore;

/// Result of resolving a topic name to its ID
pub enum TopicResolution {
    /// Topic found, contains topic_id
    Found(i32),
    /// Topic not found
    NotFound,
    /// Storage error occurred
    Error(String),
}

/// Resolves a topic name to its topic_id using the storage layer.
///
/// Returns:
/// - `TopicResolution::Found(topic_id)` if the topic exists
/// - `TopicResolution::NotFound` if the topic doesn't exist
/// - `TopicResolution::Error(msg)` if a storage error occurred
pub fn resolve_topic_id<S: KafkaStore>(store: &S, topic_name: &str) -> TopicResolution {
    match store.get_topic_metadata(Some(std::slice::from_ref(&topic_name.to_string()))) {
        Ok(topics) => {
            if let Some(tm) = topics.first() {
                TopicResolution::Found(tm.id)
            } else {
                TopicResolution::NotFound
            }
        }
        Err(e) => {
            crate::pg_warning!("Failed to resolve topic '{}': {}", topic_name, e);
            TopicResolution::Error(e.to_string())
        }
    }
}

/// Returns the appropriate Kafka error code for a topic resolution failure.
pub fn topic_resolution_error_code(resolution: &TopicResolution) -> i16 {
    match resolution {
        TopicResolution::Found(_) => ERROR_NONE,
        TopicResolution::NotFound => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        TopicResolution::Error(_) => ERROR_UNKNOWN_SERVER_ERROR,
    }
}
