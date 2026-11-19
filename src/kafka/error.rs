//! [`KafkaError`], the error type of request handling, and its mapping to Kafka error codes.
//!
//! Handlers and stores return a typed variant; the dispatch layer answers the client with
//! [`KafkaError::to_kafka_error_code`] and logs the `Display` text, so each variant carries
//! the identifiers (topic, group, producer) a log line needs.

use thiserror::Error;

use crate::kafka::constants::{
    ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_CORRUPT_MESSAGE, ERROR_DUPLICATE_SEQUENCE_NUMBER,
    ERROR_ILLEGAL_GENERATION, ERROR_INVALID_PARTITIONS, ERROR_INVALID_RECORD,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_INVALID_TXN_STATE, ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER,
    ERROR_PRODUCER_FENCED, ERROR_REBALANCE_IN_PROGRESS, ERROR_TRANSACTIONAL_ID_NOT_FOUND,
    ERROR_UNKNOWN_MEMBER_ID, ERROR_UNKNOWN_PRODUCER_ID, ERROR_UNKNOWN_SERVER_ERROR,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};

/// A failed request, typed by what went wrong; [`KafkaError::to_kafka_error_code`] gives the
/// Kafka error code the client receives.
#[derive(Error, Debug)]
pub enum KafkaError {
    // ===== Request Validation Errors =====
    /// Malformed request body (failed to decode)
    #[error("Corrupt message: {message}")]
    CorruptMessage { message: String },

    /// Topic name that violates Kafka's naming rules (INVALID_TOPIC_EXCEPTION)
    #[error("Invalid topic name '{topic}': {reason}")]
    InvalidTopic { topic: String, reason: String },

    /// Well-formed records that fail broker-side validation (INVALID_RECORD)
    #[error("Invalid record: {message}")]
    InvalidRecord { message: String },

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

    // ===== Idempotent producers =====
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

    // ===== Transactions =====
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

    // ===== Storage/Database Errors =====
    /// Database operation failed
    #[error("Database error: {message}")]
    Database { message: String },

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

    // ===== Generic Errors (use sparingly) =====
    /// Internal error with Kafka error code
    #[error("Kafka error {code}: {message}")]
    Protocol { code: i16, message: String },

    /// Catch-all internal error (prefer specific variants)
    #[error("Internal error: {0}")]
    Internal(String),
}

impl KafkaError {
    /// The Kafka error code a client receives for this error.
    pub fn to_kafka_error_code(&self) -> i16 {
        match self {
            // A body that does not decode, and a response that cannot be encoded.
            KafkaError::CorruptMessage { .. }
            | KafkaError::Encoding { .. }
            | KafkaError::ProtocolCodec(_) => ERROR_CORRUPT_MESSAGE,
            KafkaError::InvalidRecord { .. } => ERROR_INVALID_RECORD,
            KafkaError::InvalidTopic { .. } => ERROR_INVALID_TOPIC_EXCEPTION,

            KafkaError::UnknownTopic { .. } | KafkaError::UnknownPartition { .. } => {
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            }
            KafkaError::InvalidPartitions { .. } => ERROR_INVALID_PARTITIONS,

            KafkaError::CoordinatorNotAvailable { .. } => ERROR_COORDINATOR_NOT_AVAILABLE,
            KafkaError::UnknownMemberId { .. } => ERROR_UNKNOWN_MEMBER_ID,
            KafkaError::IllegalGeneration { .. } => ERROR_ILLEGAL_GENERATION,
            KafkaError::RebalanceInProgress { .. } => ERROR_REBALANCE_IN_PROGRESS,

            KafkaError::DuplicateSequence { .. } => ERROR_DUPLICATE_SEQUENCE_NUMBER,
            KafkaError::OutOfOrderSequence { .. } => ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER,
            KafkaError::ProducerFenced { .. } => ERROR_PRODUCER_FENCED,
            KafkaError::UnknownProducerId { .. } => ERROR_UNKNOWN_PRODUCER_ID,

            KafkaError::TransactionalIdNotFound { .. } => ERROR_TRANSACTIONAL_ID_NOT_FOUND,
            KafkaError::InvalidTxnState { .. } => ERROR_INVALID_TXN_STATE,

            // Explicit protocol errors keep their code.
            KafkaError::Protocol { code, .. } => *code,

            KafkaError::Database { .. } | KafkaError::Io(_) | KafkaError::Internal(_) => {
                ERROR_UNKNOWN_SERVER_ERROR
            }
        }
    }

    /// Whether the error is the broker's fault (database, IO, internal) rather than the
    /// client's; handlers log those at warning level.
    pub fn is_server_error(&self) -> bool {
        matches!(
            self,
            KafkaError::Database { .. } | KafkaError::Io(_) | KafkaError::Internal(_)
        )
    }

    /// An [`KafkaError::UnknownTopic`].
    pub fn unknown_topic(topic: impl Into<String>) -> Self {
        KafkaError::UnknownTopic {
            topic: topic.into(),
        }
    }

    /// An [`KafkaError::UnknownPartition`].
    pub fn unknown_partition(topic: impl Into<String>, partition: i32) -> Self {
        KafkaError::UnknownPartition {
            topic: topic.into(),
            partition,
        }
    }

    /// A [`KafkaError::Database`].
    pub fn database(message: impl Into<String>) -> Self {
        KafkaError::Database {
            message: message.into(),
        }
    }

    /// An [`KafkaError::Encoding`].
    pub fn encoding(message: impl Into<String>) -> Self {
        KafkaError::Encoding {
            message: message.into(),
        }
    }

    /// An [`KafkaError::UnknownMemberId`].
    pub fn unknown_member(group_id: impl Into<String>, member_id: impl Into<String>) -> Self {
        KafkaError::UnknownMemberId {
            group_id: group_id.into(),
            member_id: member_id.into(),
        }
    }

    /// An [`KafkaError::IllegalGeneration`].
    pub fn illegal_generation(group_id: impl Into<String>, generation: i32, expected: i32) -> Self {
        KafkaError::IllegalGeneration {
            group_id: group_id.into(),
            generation,
            expected,
        }
    }

    /// An [`KafkaError::OutOfOrderSequence`].
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

    /// A [`KafkaError::ProducerFenced`].
    pub fn producer_fenced(producer_id: i64, epoch: i16, expected_epoch: i16) -> Self {
        KafkaError::ProducerFenced {
            producer_id,
            epoch,
            expected_epoch,
        }
    }

    /// An [`KafkaError::UnknownProducerId`].
    pub fn unknown_producer_id(producer_id: i64) -> Self {
        KafkaError::UnknownProducerId { producer_id }
    }

    /// A [`KafkaError::TransactionalIdNotFound`].
    pub fn transactional_id_not_found(transactional_id: impl Into<String>) -> Self {
        KafkaError::TransactionalIdNotFound {
            transactional_id: transactional_id.into(),
        }
    }

    /// An [`KafkaError::InvalidTxnState`].
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
}

/// Result type of request handling.
pub type Result<T> = std::result::Result<T, KafkaError>;

impl From<pgrx::spi::SpiError> for KafkaError {
    fn from(err: pgrx::spi::SpiError) -> Self {
        KafkaError::Database {
            message: format!("SPI error: {err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(text: &str) -> String {
        text.to_string()
    }

    /// Every variant: its Kafka error code, whether it is a server error, and a fragment its
    /// `Display` text must contain.
    fn every_variant() -> Vec<(KafkaError, i16, bool, &'static str)> {
        vec![
            (
                KafkaError::CorruptMessage { message: s("bad") },
                ERROR_CORRUPT_MESSAGE,
                false,
                "Corrupt message: bad",
            ),
            (
                KafkaError::InvalidTopic {
                    topic: s("t!"),
                    reason: s("illegal character"),
                },
                ERROR_INVALID_TOPIC_EXCEPTION,
                false,
                "Invalid topic name 't!'",
            ),
            (
                KafkaError::InvalidRecord { message: s("x") },
                ERROR_INVALID_RECORD,
                false,
                "Invalid record",
            ),
            (
                KafkaError::unknown_topic("test-topic"),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                false,
                "test-topic",
            ),
            (
                KafkaError::unknown_partition("my-topic", 5),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                false,
                "Unknown partition 5 for topic my-topic",
            ),
            (
                KafkaError::InvalidPartitions { count: -1 },
                ERROR_INVALID_PARTITIONS,
                false,
                "-1",
            ),
            (
                KafkaError::CoordinatorNotAvailable { group_id: s("g") },
                ERROR_COORDINATOR_NOT_AVAILABLE,
                false,
                "g",
            ),
            (
                KafkaError::unknown_member("my-group", "member-123"),
                ERROR_UNKNOWN_MEMBER_ID,
                false,
                "member-123",
            ),
            (
                KafkaError::illegal_generation("my-group", 5, 6),
                ERROR_ILLEGAL_GENERATION,
                false,
                "Illegal generation 5",
            ),
            (
                KafkaError::RebalanceInProgress {
                    group_id: s("my-group"),
                },
                ERROR_REBALANCE_IN_PROGRESS,
                false,
                "my-group",
            ),
            (
                KafkaError::DuplicateSequence {
                    producer_id: 100,
                    partition_id: 0,
                    sequence: 5,
                    expected: 5,
                },
                ERROR_DUPLICATE_SEQUENCE_NUMBER,
                false,
                "Duplicate sequence 5 for producer 100",
            ),
            (
                KafkaError::out_of_order_sequence(100, 0, 10, 5),
                ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER,
                false,
                "expected 5",
            ),
            (
                KafkaError::producer_fenced(100, 1, 2),
                ERROR_PRODUCER_FENCED,
                false,
                "epoch 1 is older than current epoch 2",
            ),
            (
                KafkaError::unknown_producer_id(999),
                ERROR_UNKNOWN_PRODUCER_ID,
                false,
                "999",
            ),
            (
                KafkaError::transactional_id_not_found("my-txn-id"),
                ERROR_TRANSACTIONAL_ID_NOT_FOUND,
                false,
                "my-txn-id",
            ),
            (
                KafkaError::invalid_txn_state("my-txn-id", "Empty", "EndTxn"),
                ERROR_INVALID_TXN_STATE,
                false,
                "my-txn-id is in state Empty, cannot perform EndTxn",
            ),
            (
                KafkaError::database("Connection refused"),
                ERROR_UNKNOWN_SERVER_ERROR,
                true,
                "Database error: Connection refused",
            ),
            (
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed").into(),
                ERROR_UNKNOWN_SERVER_ERROR,
                true,
                "IO error: connection closed",
            ),
            (
                KafkaError::encoding("Invalid UTF-8 sequence"),
                ERROR_CORRUPT_MESSAGE,
                false,
                "Encoding error: Invalid UTF-8",
            ),
            (
                anyhow::anyhow!("short buffer").into(),
                ERROR_CORRUPT_MESSAGE,
                false,
                "Protocol codec error: short buffer",
            ),
            (
                KafkaError::Protocol {
                    code: 42,
                    message: s("m"),
                },
                42,
                false,
                "Kafka error 42: m",
            ),
            (
                KafkaError::Internal(s("internal failure")),
                ERROR_UNKNOWN_SERVER_ERROR,
                true,
                "Internal error: internal failure",
            ),
        ]
    }

    #[test]
    fn test_every_variant_maps_to_its_code() {
        for (err, code, _, _) in every_variant() {
            assert_eq!(err.to_kafka_error_code(), code, "{err:?}");
        }
    }

    #[test]
    fn test_only_broker_faults_are_server_errors() {
        for (err, _, server, _) in every_variant() {
            assert_eq!(err.is_server_error(), server, "{err:?}");
        }
    }

    #[test]
    fn test_every_variant_displays_its_context() {
        for (err, _, _, fragment) in every_variant() {
            let display = err.to_string();
            assert!(display.contains(fragment), "{display:?} lacks {fragment:?}");
        }
    }

    #[test]
    fn test_constructors_build_their_variant() {
        assert!(matches!(
            KafkaError::encoding("e"),
            KafkaError::Encoding { message } if message == "e"
        ));
        assert!(matches!(
            KafkaError::unknown_partition("t", 5),
            KafkaError::UnknownPartition { topic, partition: 5 } if topic == "t"
        ));
        assert!(matches!(
            KafkaError::database("q"),
            KafkaError::Database { message } if message == "q"
        ));
    }

    #[test]
    fn test_error_debug_format() {
        let debug = format!("{:?}", KafkaError::unknown_topic("test"));
        assert!(debug.contains("UnknownTopic"));
        assert!(debug.contains("test"));
    }
}
