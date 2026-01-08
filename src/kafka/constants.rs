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

/// API key for ListOffsets requests (Phase 3)
///
/// Used to query earliest/latest offsets for partitions
pub const API_KEY_LIST_OFFSETS: i16 = 2;

/// API key for Metadata requests
///
/// Used to discover broker and topic/partition information
pub const API_KEY_METADATA: i16 = 3;

/// API key for OffsetCommit requests (Phase 3)
///
/// Used to commit consumed offsets for a consumer group
pub const API_KEY_OFFSET_COMMIT: i16 = 8;

/// API key for OffsetFetch requests (Phase 3)
///
/// Used to fetch committed offsets for a consumer group
pub const API_KEY_OFFSET_FETCH: i16 = 9;

/// API key for FindCoordinator requests (Phase 3B)
///
/// Used to discover the coordinator for a consumer group
pub const API_KEY_FIND_COORDINATOR: i16 = 10;

/// API key for JoinGroup requests (Phase 3B)
///
/// Used for a consumer to join a consumer group
pub const API_KEY_JOIN_GROUP: i16 = 11;

/// API key for Heartbeat requests (Phase 3B)
///
/// Used to maintain consumer group membership
pub const API_KEY_HEARTBEAT: i16 = 12;

/// API key for LeaveGroup requests (Phase 3B)
///
/// Used for a consumer to leave a consumer group gracefully
pub const API_KEY_LEAVE_GROUP: i16 = 13;

/// API key for SyncGroup requests (Phase 3B)
///
/// Used to synchronize partition assignments within a consumer group
pub const API_KEY_SYNC_GROUP: i16 = 14;

/// API key for DescribeGroups requests (Phase 4)
///
/// Used to get consumer group state and members
pub const API_KEY_DESCRIBE_GROUPS: i16 = 15;

/// API key for ListGroups requests (Phase 4)
///
/// Used to list all consumer groups
pub const API_KEY_LIST_GROUPS: i16 = 16;

/// API key for ApiVersions requests
///
/// Used to discover which API versions the broker supports
pub const API_KEY_API_VERSIONS: i16 = 18;

/// API key for CreateTopics requests (Phase 6)
///
/// Used to programmatically create topics
pub const API_KEY_CREATE_TOPICS: i16 = 19;

/// API key for DeleteTopics requests (Phase 6)
///
/// Used to programmatically delete topics
pub const API_KEY_DELETE_TOPICS: i16 = 20;

/// API key for CreatePartitions requests (Phase 6)
///
/// Used to add partitions to existing topics
pub const API_KEY_CREATE_PARTITIONS: i16 = 37;

/// API key for DeleteGroups requests (Phase 6)
///
/// Used to delete consumer groups
pub const API_KEY_DELETE_GROUPS: i16 = 42;

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

// ===== Flexible Format Thresholds =====
// These define the minimum API version where flexible format (ResponseHeader v1) is used.
// Below this version, ResponseHeader v0 is used.
// See: https://kafka.apache.org/protocol.html

/// Lookup table for response header version thresholds.
///
/// Returns the minimum API version where ResponseHeader v1 (flexible format) is used.
/// For API versions below this threshold, ResponseHeader v0 is used.
/// ApiVersions is a special case - it always uses v0.
///
/// Returns None for unknown API keys (which should use v0 as fallback).
pub fn get_flexible_format_threshold(api_key: i16) -> Option<i16> {
    match api_key {
        API_KEY_API_VERSIONS => None, // Special case: always uses v0
        API_KEY_PRODUCE => Some(9),
        API_KEY_FETCH => Some(12),
        API_KEY_LIST_OFFSETS => Some(6),
        API_KEY_METADATA => Some(9),
        API_KEY_OFFSET_COMMIT => Some(8),
        API_KEY_OFFSET_FETCH => Some(6),
        API_KEY_FIND_COORDINATOR => Some(3),
        API_KEY_JOIN_GROUP => Some(6),
        API_KEY_HEARTBEAT => Some(4),
        API_KEY_LEAVE_GROUP => Some(4),
        API_KEY_SYNC_GROUP => Some(4),
        API_KEY_DESCRIBE_GROUPS => Some(5),
        API_KEY_LIST_GROUPS => Some(3),
        API_KEY_CREATE_TOPICS => Some(5),
        API_KEY_DELETE_TOPICS => Some(4),
        API_KEY_CREATE_PARTITIONS => Some(2),
        API_KEY_DELETE_GROUPS => Some(2),
        _ => None, // Unknown API keys default to v0
    }
}

/// Determines the response header version for a given API key and version.
///
/// Returns 1 for flexible format (tagged fields), 0 for non-flexible format.
/// ApiVersions always returns 0 regardless of version.
pub fn get_response_header_version(api_key: i16, api_version: i16) -> i16 {
    match get_flexible_format_threshold(api_key) {
        Some(threshold) if api_version >= threshold => 1,
        _ => 0,
    }
}

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

// ===== Database Configuration =====

/// Default database name for SPI connections
///
/// The background worker connects to this database for all SPI operations.
/// This should match the database where CREATE EXTENSION was run.
pub const DEFAULT_DATABASE: &str = "postgres";

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

// ===== Consumer Group Coordinator Error Codes =====

/// Coordinator not available
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// Not coordinator for group
pub const ERROR_NOT_COORDINATOR: i16 = 16;

/// Illegal generation (consumer group generation mismatch)
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;

/// Unknown member ID
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;

/// Rebalance in progress
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;

// ===== Admin API Error Codes =====

/// Invalid topic exception (invalid topic name)
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;

/// Topic already exists
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;

/// Group ID not found
pub const ERROR_GROUP_ID_NOT_FOUND: i16 = 69;

/// Non-empty group (cannot delete group with members)
pub const ERROR_NON_EMPTY_GROUP: i16 = 68;

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

    #[test]
    fn test_get_response_header_version_api_versions() {
        // ApiVersions always uses v0
        assert_eq!(get_response_header_version(API_KEY_API_VERSIONS, 0), 0);
        assert_eq!(get_response_header_version(API_KEY_API_VERSIONS, 3), 0);
        assert_eq!(get_response_header_version(API_KEY_API_VERSIONS, 10), 0);
    }

    #[test]
    fn test_get_response_header_version_metadata() {
        // Metadata v9+ uses flexible format
        assert_eq!(get_response_header_version(API_KEY_METADATA, 0), 0);
        assert_eq!(get_response_header_version(API_KEY_METADATA, 8), 0);
        assert_eq!(get_response_header_version(API_KEY_METADATA, 9), 1);
        assert_eq!(get_response_header_version(API_KEY_METADATA, 12), 1);
    }

    #[test]
    fn test_get_response_header_version_fetch() {
        // Fetch v12+ uses flexible format
        assert_eq!(get_response_header_version(API_KEY_FETCH, 0), 0);
        assert_eq!(get_response_header_version(API_KEY_FETCH, 11), 0);
        assert_eq!(get_response_header_version(API_KEY_FETCH, 12), 1);
        assert_eq!(get_response_header_version(API_KEY_FETCH, 15), 1);
    }

    #[test]
    fn test_get_response_header_version_coordinator_apis() {
        // FindCoordinator v3+ uses flexible format
        assert_eq!(get_response_header_version(API_KEY_FIND_COORDINATOR, 2), 0);
        assert_eq!(get_response_header_version(API_KEY_FIND_COORDINATOR, 3), 1);

        // SyncGroup/Heartbeat/LeaveGroup v4+ uses flexible format
        assert_eq!(get_response_header_version(API_KEY_SYNC_GROUP, 3), 0);
        assert_eq!(get_response_header_version(API_KEY_SYNC_GROUP, 4), 1);
        assert_eq!(get_response_header_version(API_KEY_HEARTBEAT, 3), 0);
        assert_eq!(get_response_header_version(API_KEY_HEARTBEAT, 4), 1);
        assert_eq!(get_response_header_version(API_KEY_LEAVE_GROUP, 3), 0);
        assert_eq!(get_response_header_version(API_KEY_LEAVE_GROUP, 4), 1);
    }

    #[test]
    fn test_get_response_header_version_unknown_api() {
        // Unknown API keys should return v0
        assert_eq!(get_response_header_version(999, 0), 0);
        assert_eq!(get_response_header_version(999, 100), 0);
    }
}
