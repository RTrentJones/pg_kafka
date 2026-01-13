// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

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

    #[test]
    fn test_all_error_display_variants() {
        // NotEnabled
        let err = ShadowError::NotEnabled;
        assert!(err.to_string().contains("not enabled"));

        // NotConfigured
        let err = ShadowError::NotConfigured("test reason".to_string());
        assert!(err.to_string().contains("not configured"));
        assert!(err.to_string().contains("test reason"));

        // ConnectionFailed
        let err = ShadowError::ConnectionFailed("connection refused".to_string());
        assert!(err.to_string().contains("Failed to connect"));
        assert!(err.to_string().contains("connection refused"));

        // ForwardFailed
        let err = ShadowError::ForwardFailed {
            topic: "my-topic".to_string(),
            partition: 5,
            error: "broker down".to_string(),
        };
        assert!(err.to_string().contains("my-topic"));
        assert!(err.to_string().contains("[5]"));
        assert!(err.to_string().contains("broker down"));

        // ProducerError
        let err = ShadowError::ProducerError("queue full".to_string());
        assert!(err.to_string().contains("producer error"));
        assert!(err.to_string().contains("queue full"));

        // ConfigError
        let err = ShadowError::ConfigError("invalid protocol".to_string());
        assert!(err.to_string().contains("Configuration error"));
        assert!(err.to_string().contains("invalid protocol"));

        // DatabaseError
        let err = ShadowError::DatabaseError("connection lost".to_string());
        assert!(err.to_string().contains("Database error"));
        assert!(err.to_string().contains("connection lost"));

        // Timeout
        let err = ShadowError::Timeout {
            topic: "timeout-topic".to_string(),
            partition: 3,
            timeout_ms: 30000,
        };
        assert!(err.to_string().contains("Timeout"));
        assert!(err.to_string().contains("timeout-topic"));
        assert!(err.to_string().contains("[3]"));
        assert!(err.to_string().contains("30000ms"));

        // KafkaUnavailable
        let err = ShadowError::KafkaUnavailable("all brokers down".to_string());
        assert!(err.to_string().contains("unavailable"));
        assert!(err.to_string().contains("all brokers down"));

        // InvalidTopicConfig
        let err = ShadowError::InvalidTopicConfig {
            topic_id: 42,
            reason: "missing external topic".to_string(),
        };
        assert!(err.to_string().contains("Invalid shadow config"));
        assert!(err.to_string().contains("42"));
        assert!(err.to_string().contains("missing external topic"));

        // ReplayFailed
        let err = ShadowError::ReplayFailed {
            topic: "replay-topic".to_string(),
            from_offset: 100,
            to_offset: 200,
            error: "network error".to_string(),
        };
        assert!(err.to_string().contains("Replay failed"));
        assert!(err.to_string().contains("replay-topic"));
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("200"));
        assert!(err.to_string().contains("network error"));
    }

    #[test]
    fn test_all_error_codes_mapping() {
        use crate::kafka::constants::*;

        // NotEnabled -> ERROR_SHADOW_NOT_CONFIGURED
        assert_eq!(
            ShadowError::NotEnabled.to_error_code(),
            ERROR_SHADOW_NOT_CONFIGURED
        );

        // NotConfigured -> ERROR_SHADOW_NOT_CONFIGURED
        assert_eq!(
            ShadowError::NotConfigured("x".to_string()).to_error_code(),
            ERROR_SHADOW_NOT_CONFIGURED
        );

        // ConnectionFailed -> ERROR_SHADOW_KAFKA_UNAVAILABLE
        assert_eq!(
            ShadowError::ConnectionFailed("x".to_string()).to_error_code(),
            ERROR_SHADOW_KAFKA_UNAVAILABLE
        );

        // KafkaUnavailable -> ERROR_SHADOW_KAFKA_UNAVAILABLE
        assert_eq!(
            ShadowError::KafkaUnavailable("x".to_string()).to_error_code(),
            ERROR_SHADOW_KAFKA_UNAVAILABLE
        );

        // ForwardFailed -> ERROR_SHADOW_FORWARD_FAILED
        assert_eq!(
            ShadowError::ForwardFailed {
                topic: "t".to_string(),
                partition: 0,
                error: "e".to_string()
            }
            .to_error_code(),
            ERROR_SHADOW_FORWARD_FAILED
        );

        // ProducerError -> ERROR_SHADOW_FORWARD_FAILED
        assert_eq!(
            ShadowError::ProducerError("x".to_string()).to_error_code(),
            ERROR_SHADOW_FORWARD_FAILED
        );

        // Timeout -> ERROR_SHADOW_FORWARD_FAILED
        assert_eq!(
            ShadowError::Timeout {
                topic: "t".to_string(),
                partition: 0,
                timeout_ms: 1000
            }
            .to_error_code(),
            ERROR_SHADOW_FORWARD_FAILED
        );

        // ConfigError -> ERROR_SHADOW_NOT_CONFIGURED
        assert_eq!(
            ShadowError::ConfigError("x".to_string()).to_error_code(),
            ERROR_SHADOW_NOT_CONFIGURED
        );

        // InvalidTopicConfig -> ERROR_SHADOW_NOT_CONFIGURED
        assert_eq!(
            ShadowError::InvalidTopicConfig {
                topic_id: 1,
                reason: "x".to_string()
            }
            .to_error_code(),
            ERROR_SHADOW_NOT_CONFIGURED
        );

        // DatabaseError -> ERROR_SHADOW_FORWARD_FAILED
        assert_eq!(
            ShadowError::DatabaseError("x".to_string()).to_error_code(),
            ERROR_SHADOW_FORWARD_FAILED
        );

        // ReplayFailed -> ERROR_SHADOW_REPLAY_FAILED
        assert_eq!(
            ShadowError::ReplayFailed {
                topic: "t".to_string(),
                from_offset: 0,
                to_offset: 10,
                error: "e".to_string()
            }
            .to_error_code(),
            ERROR_SHADOW_REPLAY_FAILED
        );
    }

    #[test]
    fn test_shadow_error_is_std_error() {
        // Verify ShadowError implements std::error::Error
        fn assert_error<E: std::error::Error>(_: &E) {}

        let err = ShadowError::NotEnabled;
        assert_error(&err);
    }

    #[test]
    fn test_shadow_error_debug_format() {
        let err = ShadowError::ForwardFailed {
            topic: "test".to_string(),
            partition: 0,
            error: "err".to_string(),
        };
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("ForwardFailed"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_shadow_result_type() {
        fn returns_ok() -> ShadowResult<i32> {
            Ok(42)
        }

        fn returns_err() -> ShadowResult<i32> {
            Err(ShadowError::NotEnabled)
        }

        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }
}
