//! Kafka protocol error types
//!
//! This module defines a custom error type for Kafka protocol operations,
//! providing better type safety and more informative error messages than
//! using `Box<dyn std::error::Error>`.

use thiserror::Error;

/// Errors that can occur during Kafka protocol operations
#[derive(Error, Debug)]
pub enum KafkaError {
    /// Request size is invalid (negative, zero, or exceeds maximum)
    #[error("Invalid request size: {0} (must be between 1 and {MAX_REQUEST_SIZE})")]
    InvalidRequestSize(i32),

    /// API key is not supported by this broker
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),

    /// Request payload is shorter than expected
    #[error("Request too short: expected {expected} bytes, got {actual} bytes")]
    RequestTooShort { expected: usize, actual: usize },

    /// IO error occurred during network operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Error encoding or decoding protocol messages
    #[error("Encoding error: {0}")]
    Encoding(String),

    /// Invalid configuration value
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Kafka protocol error code
    #[error("Kafka error code {code}: {message}")]
    Protocol { code: i16, message: String },

    /// Error from kafka-protocol crate (anyhow::Error)
    #[error("Protocol encoding/decoding error: {0}")]
    ProtocolCodec(#[from] anyhow::Error),

    /// Internal storage/database error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Consumer group coordinator error
    #[error("Coordinator error (code {0}): {1}")]
    CoordinatorError(i16, String),

    /// Schema or required table not found
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    /// Topic does not exist
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
}

use crate::kafka::constants::{
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION,
};

impl KafkaError {
    /// Convert this error to a Kafka protocol error code
    ///
    /// This allows returning proper Kafka error codes to clients instead of
    /// generic server errors.
    pub fn to_kafka_error_code(&self) -> i16 {
        match self {
            KafkaError::UnsupportedApiKey(_) => ERROR_UNSUPPORTED_VERSION,
            KafkaError::TopicNotFound(_) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            KafkaError::SchemaNotFound(_) => ERROR_UNKNOWN_SERVER_ERROR,
            KafkaError::CoordinatorError(code, _) => *code,
            KafkaError::Protocol { code, .. } => *code,
            // All other errors map to unknown server error
            KafkaError::InvalidRequestSize(_)
            | KafkaError::RequestTooShort { .. }
            | KafkaError::Io(_)
            | KafkaError::Encoding(_)
            | KafkaError::InvalidConfig(_)
            | KafkaError::ProtocolCodec(_)
            | KafkaError::Internal(_) => ERROR_UNKNOWN_SERVER_ERROR,
        }
    }
}

/// Result type alias for Kafka operations
pub type Result<T> = std::result::Result<T, KafkaError>;

// Re-export for convenience
use crate::kafka::constants::MAX_REQUEST_SIZE;

// Implement From for pgrx SpiError to enable ? operator in storage layer
impl From<pgrx::spi::SpiError> for KafkaError {
    fn from(err: pgrx::spi::SpiError) -> Self {
        KafkaError::Internal(format!("Database error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = KafkaError::InvalidRequestSize(150_000_000);
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid request size"));
        assert!(msg.contains("150000000"));
    }

    #[test]
    fn test_unsupported_api_key() {
        let err = KafkaError::UnsupportedApiKey(99);
        let msg = format!("{}", err);
        assert!(msg.contains("Unsupported API key: 99"));
    }

    #[test]
    fn test_request_too_short() {
        let err = KafkaError::RequestTooShort {
            expected: 100,
            actual: 50,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("expected 100 bytes"));
        assert!(msg.contains("got 50 bytes"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed");
        let kafka_err: KafkaError = io_err.into();
        let msg = format!("{}", kafka_err);
        assert!(msg.contains("IO error"));
        assert!(msg.contains("connection closed"));
    }

    #[test]
    fn test_encoding_error() {
        let err = KafkaError::Encoding("Invalid UTF-8 sequence".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Encoding error"));
        assert!(msg.contains("Invalid UTF-8"));
    }
}
