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

/// API key for InitProducerId requests (Phase 9)
///
/// Used to allocate producer IDs for idempotent/transactional producers
pub const API_KEY_INIT_PRODUCER_ID: i16 = 22;

/// API key for AddPartitionsToTxn requests (Phase 10)
///
/// Used to register partitions in an active transaction
pub const API_KEY_ADD_PARTITIONS_TO_TXN: i16 = 24;

/// API key for AddOffsetsToTxn requests (Phase 10)
///
/// Used to include consumer offset commits in a transaction
pub const API_KEY_ADD_OFFSETS_TO_TXN: i16 = 25;

/// API key for EndTxn requests (Phase 10)
///
/// Used to commit or abort a transaction
pub const API_KEY_END_TXN: i16 = 26;

/// API key for TxnOffsetCommit requests (Phase 10)
///
/// Used to commit offsets as part of a transaction
pub const API_KEY_TXN_OFFSET_COMMIT: i16 = 28;

/// API key for DeleteRecords requests
///
/// Used to delete records below an offset (GDPR/compliance truncation)
pub const API_KEY_DELETE_RECORDS: i16 = 21;

/// API key for DescribeConfigs requests
///
/// Used by admin tooling (kafka-configs.sh --describe) to read configs
pub const API_KEY_DESCRIBE_CONFIGS: i16 = 32;

/// API key for IncrementalAlterConfigs requests
///
/// Used by admin tooling to SET/DELETE individual configs (e.g. retention.ms)
pub const API_KEY_INCREMENTAL_ALTER_CONFIGS: i16 = 44;

// ----- auth stream: SASL (handled on the network thread, src/kafka/auth/session.rs) -----

/// API key for SaslHandshake requests (mechanism negotiation)
pub const API_KEY_SASL_HANDSHAKE: i16 = 17;

/// API key for SaslAuthenticate requests (SASL tokens, KIP-152)
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;

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

/// Maximum number of requests one connection may have in flight (read from the
/// socket but whose response has not yet been written back).
///
/// Responses are written strictly in request order, so a client that pipelines
/// requests without reading the responses would otherwise make the connection
/// buffer an unbounded number of encoded-but-unsent responses. When a connection
/// reaches this many, its reader stops reading further frames until the writer
/// has sent a response (TCP backpressure on the client). The value is well above
/// what real clients pipeline (librdkafka's default
/// `max.in.flight.requests.per.connection` is 1,000,000 but in practice it holds a
/// handful; the Java client defaults to 5) and above the per-connection long-poll
/// cap (64), so a full set of long polls never starves the connection.
pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: usize = 128;

/// Maximum concurrent long-poll fetch tasks per connection (RV-10 follow-up).
///
/// A fetch over the cap is served by the immediate (non-waiting) path. Must stay
/// below [`MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION`].
pub const MAX_LONG_POLLS_PER_CONNECTION: usize = 64;

/// Maximum number of elements to pre-allocate from a wire-supplied array length.
///
/// Array length prefixes in the wire protocol are attacker-controlled. Passing one
/// straight to `Vec::with_capacity`/`HashMap::with_capacity` lets a tiny request claim a
/// huge element count and force a multi-GB up-front allocation (memory-exhaustion DoS).
/// We cap the *capacity hint* at this value; the collection still grows past it if the
/// data genuinely contains more elements (each of which is bounds-checked as it is read),
/// so correctness is preserved while the pre-allocation stays bounded.
pub const MAX_PREALLOC_ELEMENTS: usize = 4096;

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

/// Default number of partitions for a topic auto-created without an explicit
/// count. Multi-partition topics are fully supported (partitioned produce/fetch,
/// routing, consumer-group assignment); this is only the fallback default and is
/// overridden by the `pg_kafka.default_partitions` GUC.
pub const DEFAULT_TOPIC_PARTITIONS: i32 = 1;

// ===== Long Polling Configuration =====

/// Default safety-net poll interval for long polling (milliseconds)
///
/// A waiting FetchRequest is re-run when a new-data notification arrives
/// (Produce, or EndTxn for `read_committed` waiters) or when `max_wait_ms`
/// expires. This interval is only the fallback for a notification that was
/// dropped (the notify channel is best-effort) or for data that became visible
/// without one, so it is deliberately slow: an idle long poll costs one
/// fetch per second instead of one per 100 ms.
pub const DEFAULT_FETCH_POLL_INTERVAL_MS: i32 = 1000;

/// Minimum poll interval (milliseconds)
pub const MIN_FETCH_POLL_INTERVAL_MS: i32 = 10;

/// Maximum poll interval (milliseconds)
pub const MAX_FETCH_POLL_INTERVAL_MS: i32 = 60_000;

// ===== Protocol Version Constants =====
// Response header versions (v1 = flexible, with tagged fields) come from each API's
// kafka-protocol response type (`HeaderVersion`); see `crate::kafka::api`.

// Note: ASYNC_IO_INTERVAL_MS and SIGNAL_CHECK_INTERVAL_MS were removed.
// The two-thread architecture now uses blocking recv_timeout() in worker.rs
// with constants defined there (RECV_TIMEOUT, TIMEOUT_CHECK_INTERVAL).

// ===== Network Hosts =====

/// Default bind host: loopback only.
///
/// The Kafka listener performs no authentication, so the default must not
/// expose it to other machines. Operators who need remote clients set
/// `pg_kafka.host` explicitly (and protect the port with network controls).
pub const DEFAULT_HOST: &str = "127.0.0.1";

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
/// wave3 net: Produce with acks other than -1, 0, 1.
pub const ERROR_INVALID_REQUIRED_ACKS: i16 = 21;

/// Message too large (request exceeds the configured maximum size)
pub const ERROR_MESSAGE_TOO_LARGE: i16 = 10;

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

/// Session timeout not within the broker's accepted range
/// (`[group.min.session.timeout.ms, group.max.session.timeout.ms]`)
pub const ERROR_INVALID_SESSION_TIMEOUT: i16 = 26;

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

/// Consumer group has reached its configured maximum size (CG-4)
pub const ERROR_GROUP_MAX_SIZE_REACHED: i16 = 81;

// ===== Idempotent Producer Error Codes (Phase 9) =====

/// Duplicate sequence number (idempotent producer deduplication)
pub const ERROR_DUPLICATE_SEQUENCE_NUMBER: i16 = 46;

/// Out of order sequence number (gap in sequence)
pub const ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;

/// Producer fenced (epoch mismatch - newer producer took over)
pub const ERROR_PRODUCER_FENCED: i16 = 90;

/// Unknown producer ID (producer ID not found)
pub const ERROR_UNKNOWN_PRODUCER_ID: i16 = 59;

// ===== Transaction Error Codes (Phase 10) =====

/// Concurrent transactions (producer has another active transaction)
pub const ERROR_CONCURRENT_TRANSACTIONS: i16 = 51;

/// Transactional ID not found
pub const ERROR_TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;

/// Invalid transaction state (invalid state transition)
pub const ERROR_INVALID_TXN_STATE: i16 = 48;

/// OFFSET_OUT_OF_RANGE: requested offset is outside the partition's log range
pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;

/// INVALID_REQUEST: the request is malformed or targets an unsupported entity
pub const ERROR_INVALID_REQUEST: i16 = 42;

/// INVALID_RECORD: a record (or record batch) failed broker-side validation, e.g. a
/// Produce v3+ partition carrying more than one record batch
pub const ERROR_INVALID_RECORD: i16 = 87;

/// INVALID_CONFIG: the given configuration key/value is invalid or unsupported
pub const ERROR_INVALID_CONFIG: i16 = 40;

// ----- auth stream: SASL error codes -----

/// UNSUPPORTED_SASL_MECHANISM: the requested SASL mechanism is not enabled
pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;

/// ILLEGAL_SASL_STATE: a SASL request arrived in the wrong state
pub const ERROR_ILLEGAL_SASL_STATE: i16 = 34;

/// SASL_AUTHENTICATION_FAILED: SASL authentication failed
pub const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;

// wave3 txn: idempotence, transactions and KIP-447 offsets
/// INVALID_PRODUCER_EPOCH: a Produce carries an epoch older than the producer's current one
pub const ERROR_INVALID_PRODUCER_EPOCH: i16 = 47;
/// INVALID_TRANSACTION_TIMEOUT: InitProducerId timeout <= 0 or above transaction.max.timeout.ms
pub const ERROR_INVALID_TRANSACTION_TIMEOUT: i16 = 50;
/// OPERATION_NOT_ATTEMPTED: the request failed for another of its entries
pub const ERROR_OPERATION_NOT_ATTEMPTED: i16 = 55;
/// UNSTABLE_OFFSET_COMMIT: a transactional offset commit for the partition is pending (KIP-447)
pub const ERROR_UNSTABLE_OFFSET_COMMIT: i16 = 88;
/// Default of pg_kafka.transaction_max_timeout_ms (Kafka's transaction.max.timeout.ms: 15 minutes)
pub const DEFAULT_TRANSACTION_MAX_TIMEOUT_MS: i32 = 900_000;

// NOTE: there is no "transaction timed out" code in the Kafka protocol — 94 is
// INCONSISTENT_VOTER_SET. A server-side transaction timeout fences the producer, so the
// internal TransactionTimedOut error maps to ERROR_PRODUCER_FENCED (see error.rs). (BUG-1)

// ===== Compression =====

use kafka_protocol::records::Compression;

/// Parse a compression type string into a Compression enum.
///
/// Supports: none, gzip, snappy, lz4, zstd (case-insensitive).
/// Invalid values fall back to None.
pub fn parse_compression_type(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "gzip" => Compression::Gzip,
        "snappy" => Compression::Snappy,
        "lz4" => Compression::Lz4,
        "zstd" => Compression::Zstd,
        "none" | "" => Compression::None,
        _ => Compression::None,
    }
}

/// Default compression type for FetchResponse encoding
pub const DEFAULT_COMPRESSION_TYPE: &str = "none";

// ===== Shadow Mode Configuration (Phase 11) =====

/// Default shadow mode enabled state
pub const DEFAULT_SHADOW_MODE_ENABLED: bool = false;

/// Default shadow bootstrap servers (empty = disabled)
pub const DEFAULT_SHADOW_BOOTSTRAP_SERVERS: &str = "";

/// Default shadow security protocol
pub const DEFAULT_SHADOW_SECURITY_PROTOCOL: &str = "SASL_SSL";

/// Default shadow SASL mechanism
pub const DEFAULT_SHADOW_SASL_MECHANISM: &str = "PLAIN";

/// Default shadow batch size (messages per batch)
pub const DEFAULT_SHADOW_BATCH_SIZE: i32 = 1000;

/// Minimum shadow batch size
pub const MIN_SHADOW_BATCH_SIZE: i32 = 1;

/// Maximum shadow batch size
pub const MAX_SHADOW_BATCH_SIZE: i32 = 100_000;

/// Default shadow linger time (milliseconds)
pub const DEFAULT_SHADOW_LINGER_MS: i32 = 10;

/// Minimum shadow linger time
pub const MIN_SHADOW_LINGER_MS: i32 = 0;

/// Maximum shadow linger time
pub const MAX_SHADOW_LINGER_MS: i32 = 60_000;

/// Default shadow retry backoff (milliseconds)
pub const DEFAULT_SHADOW_RETRY_BACKOFF_MS: i32 = 100;

/// Minimum shadow retry backoff
pub const MIN_SHADOW_RETRY_BACKOFF_MS: i32 = 10;

/// Maximum shadow retry backoff
pub const MAX_SHADOW_RETRY_BACKOFF_MS: i32 = 60_000;

/// Default shadow max retries per message: failed delivery attempts after which
/// an outbox row is dead-lettered (wave 3: this is the outbox cap; it was the
/// hard-coded 10 while the GUC fed librdkafka's `retries`).
pub const DEFAULT_SHADOW_MAX_RETRIES: i32 = 10;

/// Minimum shadow max retries
pub const MIN_SHADOW_MAX_RETRIES: i32 = 0;

/// Maximum shadow max retries
pub const MAX_SHADOW_MAX_RETRIES: i32 = 100;

/// Default shadow metrics enabled
pub const DEFAULT_SHADOW_METRICS_ENABLED: bool = true;

/// Default `pg_kafka.shadow_sync_timeout_ms`: how long a sync-mode produce
/// response waits for the external ack (wave 3).
pub const DEFAULT_SHADOW_SYNC_TIMEOUT_MS: i32 = 5_000;
/// Minimum (0 = sync responses are not held).
pub const MIN_SHADOW_SYNC_TIMEOUT_MS: i32 = 0;
/// Maximum.
pub const MAX_SHADOW_SYNC_TIMEOUT_MS: i32 = 120_000;

/// Configuration reload interval (milliseconds) - reloads both GUCs and shadow config
pub const DEFAULT_CONFIG_RELOAD_MS: i32 = 30_000; // 30 seconds

/// Minimum config reload interval (milliseconds)
pub const MIN_CONFIG_RELOAD_MS: i32 = 100; // 0.1 seconds

/// Maximum config reload interval (milliseconds)
pub const MAX_CONFIG_RELOAD_MS: i32 = 300_000; // 5 minutes

// ===== Shadow Mode Error Codes =====

/// Shadow mode forwarding failed
pub const ERROR_SHADOW_FORWARD_FAILED: i16 = 1001;

/// Shadow mode not configured
pub const ERROR_SHADOW_NOT_CONFIGURED: i16 = 1002;

/// Shadow mode external Kafka unavailable
pub const ERROR_SHADOW_KAFKA_UNAVAILABLE: i16 = 1003;

/// Shadow mode replay operation failed
pub const ERROR_SHADOW_REPLAY_FAILED: i16 = 1004;

// wave3 groups: consumer-group and admin validation error codes
/// INCONSISTENT_GROUP_PROTOCOL: a JoinGroup whose protocol type or protocols do not match the group
pub const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
/// INVALID_REPLICATION_FACTOR: replication factor not 1 / -1 on this single-broker cluster
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
// end wave3 groups

#[cfg(test)]
mod tests {
    // Constant-range invariants asserted on purpose (documentation-as-test).
    #![allow(clippy::assertions_on_constants)]
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
        assert_eq!(ERROR_MESSAGE_TOO_LARGE, 10);
        assert_eq!(ERROR_UNSUPPORTED_VERSION, 35);
        assert_eq!(ERROR_INVALID_PARTITIONS, 37);

        // Idempotent-producer and transaction codes — pinned to the canonical values in
        // Apache Kafka's org.apache.kafka.common.protocol.Errors (BUG-1 in AUDIT-2026-06).
        assert_eq!(ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER, 45);
        assert_eq!(ERROR_DUPLICATE_SEQUENCE_NUMBER, 46);
        assert_eq!(ERROR_INVALID_TXN_STATE, 48);
        assert_eq!(ERROR_CONCURRENT_TRANSACTIONS, 51);
        assert_eq!(ERROR_UNKNOWN_PRODUCER_ID, 59);
        assert_eq!(ERROR_PRODUCER_FENCED, 90);
        assert_eq!(ERROR_TRANSACTIONAL_ID_NOT_FOUND, 105);
    }

    #[test]
    fn test_in_flight_cap_exceeds_long_poll_cap() {
        // A connection holding its full set of long polls must still be able to read
        // (and answer) other requests, so the in-flight cap must leave headroom.
        assert!(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION > MAX_LONG_POLLS_PER_CONNECTION);
        assert!(MAX_LONG_POLLS_PER_CONNECTION > 0);
    }

    #[test]
    fn test_default_host_is_loopback() {
        // The listener has no authentication: the out-of-the-box bind must not be
        // reachable from other hosts.
        let addr: std::net::IpAddr = DEFAULT_HOST.parse().unwrap();
        assert!(addr.is_loopback());
    }

    #[test]
    fn test_default_fetch_poll_interval_is_a_slow_fallback() {
        // Idle long polls wake on notifications; the poll interval is only a safety net.
        assert!(DEFAULT_FETCH_POLL_INTERVAL_MS >= 1000);
        assert!(MIN_FETCH_POLL_INTERVAL_MS <= DEFAULT_FETCH_POLL_INTERVAL_MS);
        assert!(DEFAULT_FETCH_POLL_INTERVAL_MS <= MAX_FETCH_POLL_INTERVAL_MS);
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
    fn test_parse_compression_type_valid_values() {
        assert_eq!(parse_compression_type("none"), Compression::None);
        assert_eq!(parse_compression_type("gzip"), Compression::Gzip);
        assert_eq!(parse_compression_type("snappy"), Compression::Snappy);
        assert_eq!(parse_compression_type("lz4"), Compression::Lz4);
        assert_eq!(parse_compression_type("zstd"), Compression::Zstd);
    }

    #[test]
    fn test_parse_compression_type_case_insensitive() {
        assert_eq!(parse_compression_type("GZIP"), Compression::Gzip);
        assert_eq!(parse_compression_type("Gzip"), Compression::Gzip);
        assert_eq!(parse_compression_type("SNAPPY"), Compression::Snappy);
        assert_eq!(parse_compression_type("LZ4"), Compression::Lz4);
        assert_eq!(parse_compression_type("ZSTD"), Compression::Zstd);
        assert_eq!(parse_compression_type("NONE"), Compression::None);
    }

    #[test]
    fn test_parse_compression_type_invalid_fallback() {
        // Invalid values should fall back to None
        assert_eq!(parse_compression_type("invalid"), Compression::None);
        assert_eq!(parse_compression_type(""), Compression::None);
        assert_eq!(parse_compression_type("deflate"), Compression::None);
        assert_eq!(parse_compression_type("bzip2"), Compression::None);
    }
}
