//! Kafka protocol error types
//!
//! This module defines a custom error type for Kafka protocol operations,
//! following Google/Rust best practices:
//! - Typed errors with semantic meaning
//! - Error chains that preserve context
//! - Direct mapping to Kafka protocol error codes
//! - Structured error context for debugging
//!
//! # Error Handling Philosophy
//!
//! 1. **Be Specific**: Use distinct error variants for different failure modes
//! 2. **Preserve Context**: Include relevant IDs (correlation_id, topic, partition)
//! 3. **Map to Protocol**: Every error should map to a Kafka error code
//! 4. **Log at Source**: Log errors where they occur with full context

use thiserror::Error;

use crate::kafka::constants::{
    ERROR_CONCURRENT_TRANSACTIONS, ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_CORRUPT_MESSAGE,
    ERROR_DUPLICATE_SEQUENCE_NUMBER, ERROR_ILLEGAL_GENERATION, ERROR_INVALID_PARTITIONS,
    ERROR_INVALID_TXN_STATE, ERROR_NOT_COORDINATOR, ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER,
    ERROR_PRODUCER_FENCED, ERROR_REBALANCE_IN_PROGRESS, ERROR_TRANSACTIONAL_ID_NOT_FOUND,
    ERROR_TRANSACTION_TIMED_OUT, ERROR_UNKNOWN_MEMBER_ID, ERROR_UNKNOWN_PRODUCER_ID,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION,
    MAX_REQUEST_SIZE,
};

/// Errors that can occur during Kafka protocol operations
///
/// Each variant maps to a specific Kafka protocol error code via `to_kafka_error_code()`.
/// This ensures clients receive meaningful error responses they can act on.
#[derive(Error, Debug)]
pub enum KafkaError {
    // ===== Request Validation Errors =====
    /// Request size is invalid (negative, zero, or exceeds maximum)
    #[error("Invalid request size: {size} bytes (must be 1-{MAX_REQUEST_SIZE})")]
    InvalidRequestSize { size: i32 },

    /// API key is not supported by this broker
    #[error("Unsupported API key: {api_key}")]
    UnsupportedApiKey { api_key: i16 },

    /// API version is not supported for the given API key
    #[error("Unsupported API version {version} for API key {api_key}")]
    UnsupportedApiVersion { api_key: i16, version: i16 },

    /// Request payload is shorter than expected
    #[error("Request too short: expected {expected} bytes, got {actual}")]
    RequestTooShort { expected: usize, actual: usize },

    /// Malformed request body (failed to decode)
    #[error("Corrupt message: {message}")]
    CorruptMessage { message: String },

    // ===== Topic/Partition Errors =====
    /// Topic does not exist
    #[error("Unknown topic: {topic}")]
    UnknownTopic { topic: String },

    /// Partition does not exist for the topic
    #[error("Unknown partition {partition} for topic {topic}")]
    UnknownPartition { topic: String, partition: i32 },

    /// Invalid partition count (e.g., negative or zero)
    #[error("Invalid partition count: {count}")]
    InvalidPartitions { count: i32 },

    // ===== Consumer Group Coordinator Errors =====
    /// Consumer group coordinator is not available
    #[error("Coordinator not available for group: {group_id}")]
    CoordinatorNotAvailable { group_id: String },

    /// This broker is not the coordinator for the group
    #[error("Not coordinator for group: {group_id}")]
    NotCoordinator { group_id: String },

    /// Member ID is unknown to the coordinator
    #[error("Unknown member ID '{member_id}' in group '{group_id}'")]
    UnknownMemberId { group_id: String, member_id: String },

    /// Generation ID doesn't match current group generation
    #[error("Illegal generation {generation} for group '{group_id}' (expected {expected})")]
    IllegalGeneration {
        group_id: String,
        generation: i32,
        expected: i32,
    },

    /// Group is currently rebalancing
    #[error("Rebalance in progress for group: {group_id}")]
    RebalanceInProgress { group_id: String },

    // ===== Idempotent Producer Errors (Phase 9) =====
    /// Duplicate sequence number (idempotent producer deduplication)
    #[error("Duplicate sequence {sequence} for producer {producer_id} on partition {partition_id} (expected {expected})")]
    DuplicateSequence {
        producer_id: i64,
        partition_id: i32,
        sequence: i32,
        expected: i32,
    },

    /// Out of order sequence number (gap in sequence)
    #[error("Out of order sequence {sequence} for producer {producer_id} on partition {partition_id} (expected {expected})")]
    OutOfOrderSequence {
        producer_id: i64,
        partition_id: i32,
        sequence: i32,
        expected: i32,
    },

    /// Producer fenced (epoch mismatch - newer producer took over)
    #[error("Producer fenced: producer {producer_id} epoch {epoch} is older than current epoch {expected_epoch}")]
    ProducerFenced {
        producer_id: i64,
        epoch: i16,
        expected_epoch: i16,
    },

    /// Unknown producer ID (producer ID not found in storage)
    #[error("Unknown producer ID: {producer_id}")]
    UnknownProducerId { producer_id: i64 },

    // ===== Transaction Errors (Phase 10) =====
    /// Transactional ID not found
    #[error("Transactional ID not found: {transactional_id}")]
    TransactionalIdNotFound { transactional_id: String },

    /// Invalid transaction state transition
    #[error("Invalid transaction state: {transactional_id} is in state {current_state}, cannot perform {operation}")]
    InvalidTxnState {
        transactional_id: String,
        current_state: String,
        operation: String,
    },

    /// Concurrent transactions (producer has another active transaction)
    #[error("Concurrent transactions not allowed for producer {producer_id}")]
    ConcurrentTransactions { producer_id: i64 },

    /// Transaction timed out
    #[error("Transaction timed out: {transactional_id} exceeded {timeout_ms}ms")]
    TransactionTimedOut {
        transactional_id: String,
        timeout_ms: i32,
    },

    // ===== Storage/Database Errors =====
    /// Database operation failed
    #[error("Database error: {message}")]
    Database { message: String },

    /// Required schema or table not found
    #[error("Schema not ready: {message}")]
    SchemaNotReady { message: String },

    // ===== Network/IO Errors =====
    /// IO error occurred during network operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // ===== Protocol Encoding/Decoding Errors =====
    /// Error encoding response
    #[error("Encoding error: {message}")]
    Encoding { message: String },

    /// Error from kafka-protocol crate
    #[error("Protocol codec error: {0}")]
    ProtocolCodec(#[from] anyhow::Error),

    // ===== Configuration Errors =====
    /// Invalid configuration value
    #[error("Invalid configuration '{key}': {message}")]
    InvalidConfig { key: String, message: String },

    // ===== Generic Errors (use sparingly) =====
    /// Internal error with Kafka error code
    #[error("Kafka error {code}: {message}")]
    Protocol { code: i16, message: String },

    /// Catch-all internal error (prefer specific variants)
    #[error("Internal error: {0}")]
    Internal(String),
}

impl KafkaError {
    /// Convert this error to a Kafka protocol error code.
    ///
    /// This is the primary interface for returning errors to Kafka clients.
    /// Every error variant maps to a specific Kafka error code.
    pub fn to_kafka_error_code(&self) -> i16 {
        match self {
            // Request validation → CORRUPT_MESSAGE or UNSUPPORTED_VERSION
            KafkaError::InvalidRequestSize { .. } => ERROR_CORRUPT_MESSAGE,
            KafkaError::UnsupportedApiKey { .. } => ERROR_UNSUPPORTED_VERSION,
            KafkaError::UnsupportedApiVersion { .. } => ERROR_UNSUPPORTED_VERSION,
            KafkaError::RequestTooShort { .. } => ERROR_CORRUPT_MESSAGE,
            KafkaError::CorruptMessage { .. } => ERROR_CORRUPT_MESSAGE,

            // Topic/Partition → UNKNOWN_TOPIC_OR_PARTITION or INVALID_PARTITIONS
            KafkaError::UnknownTopic { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            KafkaError::UnknownPartition { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            KafkaError::InvalidPartitions { .. } => ERROR_INVALID_PARTITIONS,

            // Consumer group coordinator errors → specific codes
            KafkaError::CoordinatorNotAvailable { .. } => ERROR_COORDINATOR_NOT_AVAILABLE,
            KafkaError::NotCoordinator { .. } => ERROR_NOT_COORDINATOR,
            KafkaError::UnknownMemberId { .. } => ERROR_UNKNOWN_MEMBER_ID,
            KafkaError::IllegalGeneration { .. } => ERROR_ILLEGAL_GENERATION,
            KafkaError::RebalanceInProgress { .. } => ERROR_REBALANCE_IN_PROGRESS,

            // Idempotent producer errors → specific codes
            KafkaError::DuplicateSequence { .. } => ERROR_DUPLICATE_SEQUENCE_NUMBER,
            KafkaError::OutOfOrderSequence { .. } => ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER,
            KafkaError::ProducerFenced { .. } => ERROR_PRODUCER_FENCED,
            KafkaError::UnknownProducerId { .. } => ERROR_UNKNOWN_PRODUCER_ID,

            // Transaction errors → specific codes
            KafkaError::TransactionalIdNotFound { .. } => ERROR_TRANSACTIONAL_ID_NOT_FOUND,
            KafkaError::InvalidTxnState { .. } => ERROR_INVALID_TXN_STATE,
            KafkaError::ConcurrentTransactions { .. } => ERROR_CONCURRENT_TRANSACTIONS,
            KafkaError::TransactionTimedOut { .. } => ERROR_TRANSACTION_TIMED_OUT,

            // Storage/Schema errors → UNKNOWN_SERVER_ERROR
            KafkaError::Database { .. } => ERROR_UNKNOWN_SERVER_ERROR,
            KafkaError::SchemaNotReady { .. } => ERROR_UNKNOWN_SERVER_ERROR,

            // Network/IO errors → UNKNOWN_SERVER_ERROR
            KafkaError::Io(_) => ERROR_UNKNOWN_SERVER_ERROR,

            // Encoding errors → CORRUPT_MESSAGE (response encoding failed)
            KafkaError::Encoding { .. } => ERROR_CORRUPT_MESSAGE,
            KafkaError::ProtocolCodec(_) => ERROR_CORRUPT_MESSAGE,

            // Config errors → UNKNOWN_SERVER_ERROR
            KafkaError::InvalidConfig { .. } => ERROR_UNKNOWN_SERVER_ERROR,

            // Explicit protocol errors keep their code
            KafkaError::Protocol { code, .. } => *code,

            // Internal errors → UNKNOWN_SERVER_ERROR
            KafkaError::Internal(_) => ERROR_UNKNOWN_SERVER_ERROR,
        }
    }

    /// Returns true if this error should be logged at warning level.
    ///
    /// Client-induced errors (bad requests) are logged at debug/info level.
    /// Server-side errors (database, IO) are logged at warning/error level.
    pub fn is_server_error(&self) -> bool {
        matches!(
            self,
            KafkaError::Database { .. }
                | KafkaError::SchemaNotReady { .. }
                | KafkaError::Io(_)
                | KafkaError::Internal(_)
        )
    }

    /// Create a corrupt message error with context
    pub fn corrupt_message(context: impl Into<String>) -> Self {
        KafkaError::CorruptMessage {
            message: context.into(),
        }
    }

    /// Create an unknown topic error
    pub fn unknown_topic(topic: impl Into<String>) -> Self {
        KafkaError::UnknownTopic {
            topic: topic.into(),
        }
    }

    /// Create an unknown partition error
    pub fn unknown_partition(topic: impl Into<String>, partition: i32) -> Self {
        KafkaError::UnknownPartition {
            topic: topic.into(),
            partition,
        }
    }

    /// Create a database error
    pub fn database(message: impl Into<String>) -> Self {
        KafkaError::Database {
            message: message.into(),
        }
    }

    /// Create an encoding error
    pub fn encoding(message: impl Into<String>) -> Self {
        KafkaError::Encoding {
            message: message.into(),
        }
    }

    /// Create an unknown member ID error
    pub fn unknown_member(group_id: impl Into<String>, member_id: impl Into<String>) -> Self {
        KafkaError::UnknownMemberId {
            group_id: group_id.into(),
            member_id: member_id.into(),
        }
    }

    /// Create an illegal generation error
    pub fn illegal_generation(group_id: impl Into<String>, generation: i32, expected: i32) -> Self {
        KafkaError::IllegalGeneration {
            group_id: group_id.into(),
            generation,
            expected,
        }
    }

    /// Create a duplicate sequence error (Phase 9)
    pub fn duplicate_sequence(
        producer_id: i64,
        partition_id: i32,
        sequence: i32,
        expected: i32,
    ) -> Self {
        KafkaError::DuplicateSequence {
            producer_id,
            partition_id,
            sequence,
            expected,
        }
    }

    /// Create an out of order sequence error (Phase 9)
    pub fn out_of_order_sequence(
        producer_id: i64,
        partition_id: i32,
        sequence: i32,
        expected: i32,
    ) -> Self {
        KafkaError::OutOfOrderSequence {
            producer_id,
            partition_id,
            sequence,
            expected,
        }
    }

    /// Create a producer fenced error (Phase 9)
    pub fn producer_fenced(producer_id: i64, epoch: i16, expected_epoch: i16) -> Self {
        KafkaError::ProducerFenced {
            producer_id,
            epoch,
            expected_epoch,
        }
    }

    /// Create an unknown producer ID error (Phase 9)
    pub fn unknown_producer_id(producer_id: i64) -> Self {
        KafkaError::UnknownProducerId { producer_id }
    }

    /// Create a transactional ID not found error (Phase 10)
    pub fn transactional_id_not_found(transactional_id: impl Into<String>) -> Self {
        KafkaError::TransactionalIdNotFound {
            transactional_id: transactional_id.into(),
        }
    }

    /// Create an invalid transaction state error (Phase 10)
    pub fn invalid_txn_state(
        transactional_id: impl Into<String>,
        current_state: impl Into<String>,
        operation: impl Into<String>,
    ) -> Self {
        KafkaError::InvalidTxnState {
            transactional_id: transactional_id.into(),
            current_state: current_state.into(),
            operation: operation.into(),
        }
    }

    /// Create a concurrent transactions error (Phase 10)
    pub fn concurrent_transactions(producer_id: i64) -> Self {
        KafkaError::ConcurrentTransactions { producer_id }
    }

    /// Create a transaction timed out error (Phase 10)
    pub fn transaction_timed_out(transactional_id: impl Into<String>, timeout_ms: i32) -> Self {
        KafkaError::TransactionTimedOut {
            transactional_id: transactional_id.into(),
            timeout_ms,
        }
    }
}

/// Result type alias for Kafka operations
pub type Result<T> = std::result::Result<T, KafkaError>;

// ===== Error Conversions =====

impl From<pgrx::spi::SpiError> for KafkaError {
    fn from(err: pgrx::spi::SpiError) -> Self {
        KafkaError::Database {
            message: format!("SPI error: {}", err),
        }
    }
}

// ===== Backwards Compatibility =====
// These allow gradual migration from old error patterns

impl KafkaError {
    /// Create from legacy InvalidRequestSize(i32) pattern
    #[allow(non_snake_case)]
    pub fn InvalidRequestSize(size: i32) -> Self {
        KafkaError::InvalidRequestSize { size }
    }

    /// Create from legacy UnsupportedApiKey(i16) pattern
    #[allow(non_snake_case)]
    pub fn UnsupportedApiKey(api_key: i16) -> Self {
        KafkaError::UnsupportedApiKey { api_key }
    }

    /// Create from legacy Encoding(String) pattern
    #[allow(non_snake_case)]
    pub fn Encoding(message: String) -> Self {
        KafkaError::Encoding { message }
    }

    /// Create from legacy InvalidConfig(String) pattern
    #[allow(non_snake_case)]
    pub fn InvalidConfig(message: String) -> Self {
        KafkaError::InvalidConfig {
            key: "unknown".to_string(),
            message,
        }
    }

    /// Create from legacy SchemaNotFound(String) pattern
    #[allow(non_snake_case)]
    pub fn SchemaNotFound(message: String) -> Self {
        KafkaError::SchemaNotReady { message }
    }

    /// Create from legacy TopicNotFound(String) pattern
    #[allow(non_snake_case)]
    pub fn TopicNotFound(topic: String) -> Self {
        KafkaError::UnknownTopic { topic }
    }

    /// Create from legacy CoordinatorError(i16, String) pattern
    #[allow(non_snake_case)]
    pub fn CoordinatorError(code: i16, message: String) -> Self {
        KafkaError::Protocol { code, message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = KafkaError::InvalidRequestSize { size: 150_000_000 };
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid request size"));
        assert!(msg.contains("150000000"));
    }

    #[test]
    fn test_unsupported_api_key() {
        let err = KafkaError::UnsupportedApiKey { api_key: 99 };
        let msg = format!("{}", err);
        assert!(msg.contains("Unsupported API key: 99"));
        assert_eq!(err.to_kafka_error_code(), ERROR_UNSUPPORTED_VERSION);
    }

    #[test]
    fn test_request_too_short() {
        let err = KafkaError::RequestTooShort {
            expected: 100,
            actual: 50,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("expected 100 bytes"));
        assert!(msg.contains("got 50"));
        assert_eq!(err.to_kafka_error_code(), ERROR_CORRUPT_MESSAGE);
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed");
        let kafka_err: KafkaError = io_err.into();
        let msg = format!("{}", kafka_err);
        assert!(msg.contains("IO error"));
        assert!(msg.contains("connection closed"));
        assert_eq!(kafka_err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
        assert!(kafka_err.is_server_error());
    }

    #[test]
    fn test_encoding_error() {
        let err = KafkaError::encoding("Invalid UTF-8 sequence");
        let msg = format!("{}", err);
        assert!(msg.contains("Encoding error"));
        assert!(msg.contains("Invalid UTF-8"));
    }

    #[test]
    fn test_unknown_topic_error_code() {
        let err = KafkaError::unknown_topic("test-topic");
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert!(!err.is_server_error());
    }

    #[test]
    fn test_coordinator_errors() {
        let err = KafkaError::unknown_member("my-group", "member-123");
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_MEMBER_ID);

        let err = KafkaError::illegal_generation("my-group", 5, 6);
        assert_eq!(err.to_kafka_error_code(), ERROR_ILLEGAL_GENERATION);

        let err = KafkaError::RebalanceInProgress {
            group_id: "my-group".to_string(),
        };
        assert_eq!(err.to_kafka_error_code(), ERROR_REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn test_database_error_is_server_error() {
        let err = KafkaError::database("Connection refused");
        assert!(err.is_server_error());
        assert_eq!(err.to_kafka_error_code(), ERROR_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn test_backwards_compat_constructors() {
        // Test that old-style constructor patterns still work
        let err = KafkaError::InvalidRequestSize(100);
        assert!(matches!(err, KafkaError::InvalidRequestSize { size: 100 }));

        let err = KafkaError::UnsupportedApiKey(42);
        assert!(matches!(err, KafkaError::UnsupportedApiKey { api_key: 42 }));
    }

    #[test]
    fn test_transaction_errors() {
        // TransactionalIdNotFound
        let err = KafkaError::transactional_id_not_found("my-txn-id");
        assert_eq!(err.to_kafka_error_code(), ERROR_TRANSACTIONAL_ID_NOT_FOUND);
        assert!(!err.is_server_error());

        // InvalidTxnState
        let err = KafkaError::invalid_txn_state("my-txn-id", "Empty", "EndTxn");
        assert_eq!(err.to_kafka_error_code(), ERROR_INVALID_TXN_STATE);
        let msg = format!("{}", err);
        assert!(msg.contains("my-txn-id"));
        assert!(msg.contains("Empty"));
        assert!(msg.contains("EndTxn"));

        // ConcurrentTransactions
        let err = KafkaError::concurrent_transactions(12345);
        assert_eq!(err.to_kafka_error_code(), ERROR_CONCURRENT_TRANSACTIONS);

        // TransactionTimedOut
        let err = KafkaError::transaction_timed_out("my-txn-id", 60000);
        assert_eq!(err.to_kafka_error_code(), ERROR_TRANSACTION_TIMED_OUT);
        let msg = format!("{}", err);
        assert!(msg.contains("my-txn-id"));
        assert!(msg.contains("60000"));
    }
}
