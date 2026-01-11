//! Shadow mode error types
//!
//! This module defines error types specific to shadow mode operations,
//! including forwarding failures, configuration issues, and producer errors.

use std::fmt;

/// Result type for shadow mode operations
pub type ShadowResult<T> = std::result::Result<T, ShadowError>;

/// Errors that can occur during shadow mode operations
#[derive(Debug)]
pub enum ShadowError {
    /// Shadow mode is not enabled
    NotEnabled,

    /// Shadow mode is not properly configured (missing bootstrap servers, etc.)
    NotConfigured(String),

    /// Failed to connect to external Kafka
    ConnectionFailed(String),

    /// Failed to forward message to external Kafka
    ForwardFailed {
        topic: String,
        partition: i32,
        error: String,
    },

    /// Producer error from rdkafka
    ProducerError(String),

    /// Configuration error
    ConfigError(String),

    /// Database error during shadow operations
    DatabaseError(String),

    /// Timeout waiting for external Kafka acknowledgment
    Timeout {
        topic: String,
        partition: i32,
        timeout_ms: u64,
    },

    /// External Kafka is unavailable
    KafkaUnavailable(String),

    /// Invalid topic configuration
    InvalidTopicConfig { topic_id: i32, reason: String },

    /// Replay operation failed
    ReplayFailed {
        topic: String,
        from_offset: i64,
        to_offset: i64,
        error: String,
    },
}

impl fmt::Display for ShadowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShadowError::NotEnabled => {
                write!(f, "Shadow mode is not enabled")
            }
            ShadowError::NotConfigured(msg) => {
                write!(f, "Shadow mode not configured: {}", msg)
            }
            ShadowError::ConnectionFailed(msg) => {
                write!(f, "Failed to connect to external Kafka: {}", msg)
            }
            ShadowError::ForwardFailed {
                topic,
                partition,
                error,
            } => {
                write!(
                    f,
                    "Failed to forward message to {}[{}]: {}",
                    topic, partition, error
                )
            }
            ShadowError::ProducerError(msg) => {
                write!(f, "Kafka producer error: {}", msg)
            }
            ShadowError::ConfigError(msg) => {
                write!(f, "Configuration error: {}", msg)
            }
            ShadowError::DatabaseError(msg) => {
                write!(f, "Database error: {}", msg)
            }
            ShadowError::Timeout {
                topic,
                partition,
                timeout_ms,
            } => {
                write!(
                    f,
                    "Timeout waiting for ack from {}[{}] after {}ms",
                    topic, partition, timeout_ms
                )
            }
            ShadowError::KafkaUnavailable(msg) => {
                write!(f, "External Kafka unavailable: {}", msg)
            }
            ShadowError::InvalidTopicConfig { topic_id, reason } => {
                write!(
                    f,
                    "Invalid shadow config for topic {}: {}",
                    topic_id, reason
                )
            }
            ShadowError::ReplayFailed {
                topic,
                from_offset,
                to_offset,
                error,
            } => {
                write!(
                    f,
                    "Replay failed for {} offsets {}-{}: {}",
                    topic, from_offset, to_offset, error
                )
            }
        }
    }
}

impl std::error::Error for ShadowError {}

impl ShadowError {
    /// Convert to a Kafka-compatible error code for protocol responses
    pub fn to_error_code(&self) -> i16 {
        use crate::kafka::constants::*;
        match self {
            ShadowError::NotEnabled | ShadowError::NotConfigured(_) => ERROR_SHADOW_NOT_CONFIGURED,
            ShadowError::ConnectionFailed(_) | ShadowError::KafkaUnavailable(_) => {
                ERROR_SHADOW_KAFKA_UNAVAILABLE
            }
            ShadowError::ForwardFailed { .. }
            | ShadowError::ProducerError(_)
            | ShadowError::Timeout { .. } => ERROR_SHADOW_FORWARD_FAILED,
            ShadowError::ConfigError(_) | ShadowError::InvalidTopicConfig { .. } => {
                ERROR_SHADOW_NOT_CONFIGURED
            }
            ShadowError::DatabaseError(_) => ERROR_SHADOW_FORWARD_FAILED,
            ShadowError::ReplayFailed { .. } => ERROR_SHADOW_REPLAY_FAILED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shadow_error_display() {
        let err = ShadowError::NotEnabled;
        assert_eq!(err.to_string(), "Shadow mode is not enabled");

        let err = ShadowError::NotConfigured("missing bootstrap servers".to_string());
        assert_eq!(
            err.to_string(),
            "Shadow mode not configured: missing bootstrap servers"
        );

        let err = ShadowError::ForwardFailed {
            topic: "test-topic".to_string(),
            partition: 0,
            error: "broker unavailable".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to forward message to test-topic[0]: broker unavailable"
        );

        let err = ShadowError::Timeout {
            topic: "test".to_string(),
            partition: 1,
            timeout_ms: 5000,
        };
        assert_eq!(
            err.to_string(),
            "Timeout waiting for ack from test[1] after 5000ms"
        );
    }

    #[test]
    fn test_shadow_error_codes() {
        assert_eq!(ShadowError::NotEnabled.to_error_code(), 1002);
        assert_eq!(
            ShadowError::KafkaUnavailable("test".to_string()).to_error_code(),
            1003
        );
        assert_eq!(
            ShadowError::ForwardFailed {
                topic: "t".to_string(),
                partition: 0,
                error: "e".to_string()
            }
            .to_error_code(),
            1001
        );
    }
}
