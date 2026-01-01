//! Kafka protocol constants
//!
//! This module centralizes all magic numbers used in the Kafka protocol implementation.
//! Using named constants improves code readability and maintainability.
//!
//! # Terminology
//! - **API Key**: Identifies which operation/request type (e.g., 18 = ApiVersions, 3 = Metadata)
//! - **API Version**: Identifies which version of that operation (e.g., Metadata v9, ApiVersions v3)

// ===== API Keys =====
// These identify the type of request being made
// See: https://kafka.apache.org/protocol.html#protocol_api_key

/// API key for Produce requests (Phase 2)
///
/// Used to write messages to a topic partition
pub const API_KEY_PRODUCE: i16 = 0;

/// API key for Fetch requests (Phase 3)
///
/// Used to read messages from a topic partition
pub const API_KEY_FETCH: i16 = 1;

/// API key for Metadata requests
///
/// Used to discover broker and topic/partition information
pub const API_KEY_METADATA: i16 = 3;

/// API key for ApiVersions requests
///
/// Used to discover which API versions the broker supports
pub const API_KEY_API_VERSIONS: i16 = 18;

// ===== Configuration Defaults =====

/// Default Kafka protocol port
///
/// Standard Kafka broker port (9092)
pub const DEFAULT_KAFKA_PORT: i32 = 9092;

/// Minimum allowed port number (above privileged ports)
pub const MIN_PORT: i32 = 1024;

/// Maximum allowed port number
pub const MAX_PORT: i32 = 65535;

// ===== Protocol Limits =====

/// Maximum request size (100MB)
///
/// This limit prevents DoS attacks via extremely large requests
pub const MAX_REQUEST_SIZE: i32 = 100_000_000;

/// Default graceful shutdown timeout (milliseconds)
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: i32 = 5000;

/// Minimum shutdown timeout (milliseconds)
pub const MIN_SHUTDOWN_TIMEOUT_MS: i32 = 100;

/// Maximum shutdown timeout (milliseconds)
pub const MAX_SHUTDOWN_TIMEOUT_MS: i32 = 60000;

// ===== Broker Identifiers =====

/// Default broker node ID for single-node setup
///
/// In Phase 1, we act as a single broker with ID 1
pub const DEFAULT_BROKER_ID: i32 = 1;

// ===== Topic Configuration =====

/// Default number of partitions for new topics
///
/// Phase 1 only supports single-partition topics.
/// In future phases, this will become configurable per-topic.
pub const DEFAULT_TOPIC_PARTITIONS: i32 = 1;

// ===== Protocol Version Constants =====

/// Kafka protocol version where flexible format begins
///
/// Starting with version 9, Kafka uses "flexible" format with tagged fields.
/// This requires ResponseHeader v1 instead of v0.
pub const FLEXIBLE_FORMAT_MIN_VERSION: i16 = 9;

/// ResponseHeader version for ApiVersions responses
///
/// ApiVersions is special: it ALWAYS uses ResponseHeader v0, even for v3+ requests.
/// This differs from other APIs where v3+ uses ResponseHeader v1 (flexible format).
/// See: https://github.com/Baylox/kafka-mock
pub const API_VERSIONS_RESPONSE_HEADER_VERSION: i16 = 0;

// ===== Worker Timing Constants =====

/// Async I/O processing interval (milliseconds)
///
/// How long to process async network I/O before checking for database requests.
/// Balances network throughput vs database operation latency.
/// Too high: Database operations wait longer for processing
/// Too low: Overhead from frequent context switches
pub const ASYNC_IO_INTERVAL_MS: u64 = 100;

/// Signal check interval (milliseconds)
///
/// How often to check for Postgres shutdown signals (SIGTERM).
/// Must be short for responsive shutdown, but not too short to avoid busy-waiting.
pub const SIGNAL_CHECK_INTERVAL_MS: u64 = 1;

// ===== Network Hosts =====

/// Default host for production (bind to all interfaces)
pub const DEFAULT_HOST: &str = "0.0.0.0";

/// Default host for testing (localhost only)
pub const TEST_HOST: &str = "localhost";

// ===== Kafka Error Codes =====
// See: https://kafka.apache.org/protocol.html#protocol_error_codes

/// No error
pub const ERROR_NONE: i16 = 0;

/// Unknown server error
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;

/// Corrupt message (invalid RecordBatch format)
pub const ERROR_CORRUPT_MESSAGE: i16 = 2;

/// Unknown topic or partition
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Unsupported version
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;

/// Invalid number of partitions
pub const ERROR_INVALID_PARTITIONS: i16 = 37;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_keys_match_kafka_spec() {
        // Verify against official Kafka protocol specification
        // https://kafka.apache.org/protocol.html#protocol_api_key
        assert_eq!(API_KEY_PRODUCE, 0, "Produce API key should be 0");
        assert_eq!(API_KEY_FETCH, 1, "Fetch API key should be 1");
        assert_eq!(API_KEY_METADATA, 3, "Metadata API key should be 3");
        assert_eq!(API_KEY_API_VERSIONS, 18, "ApiVersions API key should be 18");
    }

    #[test]
    fn test_port_configuration_valid() {
        assert!(MIN_PORT >= 1024, "Min port should avoid privileged ports");
        assert!(MAX_PORT <= 65535, "Max port must fit in u16");
        assert!(
            DEFAULT_KAFKA_PORT >= MIN_PORT && DEFAULT_KAFKA_PORT <= MAX_PORT,
            "Default port must be in valid range"
        );
    }

    #[test]
    fn test_shutdown_timeout_range_valid() {
        assert!(
            MIN_SHUTDOWN_TIMEOUT_MS > 0,
            "Shutdown timeout must be positive"
        );
        assert!(
            MAX_SHUTDOWN_TIMEOUT_MS > MIN_SHUTDOWN_TIMEOUT_MS,
            "Max must be greater than min"
        );
        assert!(
            DEFAULT_SHUTDOWN_TIMEOUT_MS >= MIN_SHUTDOWN_TIMEOUT_MS
                && DEFAULT_SHUTDOWN_TIMEOUT_MS <= MAX_SHUTDOWN_TIMEOUT_MS,
            "Default must be in valid range"
        );
    }

    #[test]
    fn test_error_codes_match_kafka_spec() {
        // Verify against Kafka protocol error codes
        assert_eq!(ERROR_NONE, 0);
        assert_eq!(ERROR_UNKNOWN_SERVER_ERROR, -1);
        assert_eq!(ERROR_CORRUPT_MESSAGE, 2);
        assert_eq!(ERROR_UNKNOWN_TOPIC_OR_PARTITION, 3);
        assert_eq!(ERROR_UNSUPPORTED_VERSION, 35);
        assert_eq!(ERROR_INVALID_PARTITIONS, 37);
    }

    #[test]
    fn test_max_request_size_reasonable() {
        assert!(MAX_REQUEST_SIZE > 0, "Max request size must be positive");
        assert!(
            MAX_REQUEST_SIZE <= 1_000_000_000,
            "Max request size should be reasonable (< 1GB)"
        );
    }
}
