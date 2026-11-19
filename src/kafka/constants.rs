//! Kafka protocol constants: API keys, error codes, limits and configuration defaults.
//!
//! Wire values live here once, under names, so the code never spells a bare protocol number.
//! The API keys are taken from the kafka-protocol request types; the error codes follow
//! Apache Kafka's `org.apache.kafka.common.protocol.Errors`
//! (<https://kafka.apache.org/protocol.html#protocol_error_codes>).

use kafka_protocol::messages::{
    add_offsets_to_txn_request, add_partitions_to_txn_request, api_versions_request,
    create_partitions_request, create_topics_request, delete_groups_request,
    delete_records_request, delete_topics_request, describe_configs_request,
    describe_groups_request, end_txn_request, fetch_request, find_coordinator_request,
    heartbeat_request, incremental_alter_configs_request, init_producer_id_request,
    join_group_request, leave_group_request, list_groups_request, list_offsets_request,
    metadata_request, offset_commit_request, offset_fetch_request, produce_request,
    sasl_authenticate_request, sasl_handshake_request, sync_group_request,
    txn_offset_commit_request,
};
use kafka_protocol::protocol::Request;
use kafka_protocol::records::Compression;

// ===== API keys =====
// Each key is the one kafka-protocol assigns the request type, so the two cannot disagree.
// The API registry (`crate::kafka::api`) uses the types directly; these names serve code that
// matches on a raw key (the listener, SASL gating, tests).

/// API key of Produce requests.
pub const API_KEY_PRODUCE: i16 = produce_request::ProduceRequest::KEY;
/// API key of Fetch requests.
pub const API_KEY_FETCH: i16 = fetch_request::FetchRequest::KEY;
/// API key of ListOffsets requests.
pub const API_KEY_LIST_OFFSETS: i16 = list_offsets_request::ListOffsetsRequest::KEY;
/// API key of Metadata requests.
pub const API_KEY_METADATA: i16 = metadata_request::MetadataRequest::KEY;
/// API key of OffsetCommit requests.
pub const API_KEY_OFFSET_COMMIT: i16 = offset_commit_request::OffsetCommitRequest::KEY;
/// API key of OffsetFetch requests.
pub const API_KEY_OFFSET_FETCH: i16 = offset_fetch_request::OffsetFetchRequest::KEY;
/// API key of FindCoordinator requests.
pub const API_KEY_FIND_COORDINATOR: i16 = find_coordinator_request::FindCoordinatorRequest::KEY;
/// API key of JoinGroup requests.
pub const API_KEY_JOIN_GROUP: i16 = join_group_request::JoinGroupRequest::KEY;
/// API key of Heartbeat requests.
pub const API_KEY_HEARTBEAT: i16 = heartbeat_request::HeartbeatRequest::KEY;
/// API key of LeaveGroup requests.
pub const API_KEY_LEAVE_GROUP: i16 = leave_group_request::LeaveGroupRequest::KEY;
/// API key of SyncGroup requests.
pub const API_KEY_SYNC_GROUP: i16 = sync_group_request::SyncGroupRequest::KEY;
/// API key of DescribeGroups requests.
pub const API_KEY_DESCRIBE_GROUPS: i16 = describe_groups_request::DescribeGroupsRequest::KEY;
/// API key of ListGroups requests.
pub const API_KEY_LIST_GROUPS: i16 = list_groups_request::ListGroupsRequest::KEY;
/// API key of SaslHandshake requests.
pub const API_KEY_SASL_HANDSHAKE: i16 = sasl_handshake_request::SaslHandshakeRequest::KEY;
/// API key of ApiVersions requests.
pub const API_KEY_API_VERSIONS: i16 = api_versions_request::ApiVersionsRequest::KEY;
/// API key of CreateTopics requests.
pub const API_KEY_CREATE_TOPICS: i16 = create_topics_request::CreateTopicsRequest::KEY;
/// API key of DeleteTopics requests.
pub const API_KEY_DELETE_TOPICS: i16 = delete_topics_request::DeleteTopicsRequest::KEY;
/// API key of DeleteRecords requests.
pub const API_KEY_DELETE_RECORDS: i16 = delete_records_request::DeleteRecordsRequest::KEY;
/// API key of InitProducerId requests.
pub const API_KEY_INIT_PRODUCER_ID: i16 = init_producer_id_request::InitProducerIdRequest::KEY;
/// API key of AddPartitionsToTxn requests.
pub const API_KEY_ADD_PARTITIONS_TO_TXN: i16 =
    add_partitions_to_txn_request::AddPartitionsToTxnRequest::KEY;
/// API key of AddOffsetsToTxn requests.
pub const API_KEY_ADD_OFFSETS_TO_TXN: i16 = add_offsets_to_txn_request::AddOffsetsToTxnRequest::KEY;
/// API key of EndTxn requests.
pub const API_KEY_END_TXN: i16 = end_txn_request::EndTxnRequest::KEY;
/// API key of TxnOffsetCommit requests.
pub const API_KEY_TXN_OFFSET_COMMIT: i16 = txn_offset_commit_request::TxnOffsetCommitRequest::KEY;
/// API key of DescribeConfigs requests.
pub const API_KEY_DESCRIBE_CONFIGS: i16 = describe_configs_request::DescribeConfigsRequest::KEY;
/// API key of SaslAuthenticate requests.
pub const API_KEY_SASL_AUTHENTICATE: i16 = sasl_authenticate_request::SaslAuthenticateRequest::KEY;
/// API key of CreatePartitions requests.
pub const API_KEY_CREATE_PARTITIONS: i16 = create_partitions_request::CreatePartitionsRequest::KEY;
/// API key of DeleteGroups requests.
pub const API_KEY_DELETE_GROUPS: i16 = delete_groups_request::DeleteGroupsRequest::KEY;
/// API key of IncrementalAlterConfigs requests.
pub const API_KEY_INCREMENTAL_ALTER_CONFIGS: i16 =
    incremental_alter_configs_request::IncrementalAlterConfigsRequest::KEY;

// ===== Listener configuration =====

/// Default Kafka protocol port.
pub const DEFAULT_KAFKA_PORT: i32 = 9092;

/// Lowest port `pg_kafka.port` accepts (above the privileged ports).
pub const MIN_PORT: i32 = 1024;

/// Highest port `pg_kafka.port` accepts.
pub const MAX_PORT: i32 = 65535;

// Unprivileged, within u16, and the default inside the range.
const _: () = assert!(
    1024 <= MIN_PORT
        && MIN_PORT <= DEFAULT_KAFKA_PORT
        && DEFAULT_KAFKA_PORT <= MAX_PORT
        && MAX_PORT <= 65_535
);

/// Default bind host: loopback only, so a fresh install is not reachable from other
/// machines. Operators who need remote clients set `pg_kafka.host` (and protect the port
/// with TLS/SASL listeners or network controls).
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// Host the unit tests' default configuration uses.
pub const TEST_HOST: &str = "localhost";

/// Default graceful shutdown timeout of the listener (milliseconds).
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: i32 = 5000;

/// Minimum shutdown timeout (milliseconds).
pub const MIN_SHUTDOWN_TIMEOUT_MS: i32 = 100;

/// Maximum shutdown timeout (milliseconds).
pub const MAX_SHUTDOWN_TIMEOUT_MS: i32 = 60_000;

const _: () = assert!(
    0 < MIN_SHUTDOWN_TIMEOUT_MS
        && MIN_SHUTDOWN_TIMEOUT_MS < MAX_SHUTDOWN_TIMEOUT_MS
        && MIN_SHUTDOWN_TIMEOUT_MS <= DEFAULT_SHUTDOWN_TIMEOUT_MS
        && DEFAULT_SHUTDOWN_TIMEOUT_MS <= MAX_SHUTDOWN_TIMEOUT_MS
);

/// Default database the background worker connects to: the one `CREATE EXTENSION` ran in.
pub const DEFAULT_DATABASE: &str = "postgres";

/// Node id of the single broker pg_kafka presents.
pub const DEFAULT_BROKER_ID: i32 = 1;

// ===== Request limits =====

/// Largest request frame accepted (100 MB), against memory-exhaustion by huge requests.
/// A byte count compared with frame lengths; it stays below `i32::MAX`, the largest
/// length the 4-byte size prefix can carry.
pub const MAX_REQUEST_SIZE: usize = 100_000_000;

const _: () = assert!(0 < MAX_REQUEST_SIZE && MAX_REQUEST_SIZE <= 1_000_000_000);

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
/// cap, so a full set of long polls never starves the connection.
pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: usize = 128;

/// Maximum concurrent long-poll fetch tasks per connection. A fetch over the cap is
/// served by the immediate (non-waiting) path.
pub const MAX_LONG_POLLS_PER_CONNECTION: usize = 64;

// A connection holding its full set of long polls must still read (and answer) other
// requests.
const _: () = assert!(
    0 < MAX_LONG_POLLS_PER_CONNECTION
        && MAX_LONG_POLLS_PER_CONNECTION < MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
);

/// Maximum number of elements to pre-allocate from a wire-supplied array length.
///
/// Array length prefixes in the wire protocol are attacker-controlled. Passing one
/// straight to `Vec::with_capacity`/`HashMap::with_capacity` lets a tiny request claim a
/// huge element count and force a multi-GB up-front allocation. The *capacity hint* is
/// capped at this value; the collection still grows past it if the data really contains
/// more elements (each of which is bounds-checked as it is read).
pub const MAX_PREALLOC_ELEMENTS: usize = 4096;

// ===== Topics and fetches =====

/// Default number of partitions of an auto-created topic (`pg_kafka.default_partitions`).
pub const DEFAULT_TOPIC_PARTITIONS: i32 = 1;

/// Default safety-net poll interval for long polling (milliseconds).
///
/// A waiting FetchRequest is re-run when a new-data notification arrives
/// (Produce, or EndTxn for `read_committed` waiters) or when `max_wait_ms`
/// expires. This interval is only the fallback for a notification that was
/// dropped (the notify channel is best-effort) or for data that became visible
/// without one, so it is deliberately slow: an idle long poll costs one
/// fetch per second.
pub const DEFAULT_FETCH_POLL_INTERVAL_MS: i32 = 1000;

/// Minimum poll interval (milliseconds).
pub const MIN_FETCH_POLL_INTERVAL_MS: i32 = 10;

/// Maximum poll interval (milliseconds).
pub const MAX_FETCH_POLL_INTERVAL_MS: i32 = 60_000;

// Idle long polls wake on notifications; the poll interval is only a slow safety net.
const _: () = assert!(
    1_000 <= DEFAULT_FETCH_POLL_INTERVAL_MS
        && MIN_FETCH_POLL_INTERVAL_MS <= DEFAULT_FETCH_POLL_INTERVAL_MS
        && DEFAULT_FETCH_POLL_INTERVAL_MS <= MAX_FETCH_POLL_INTERVAL_MS
);

/// Default of `pg_kafka.transaction_max_timeout_ms` (Kafka's `transaction.max.timeout.ms`:
/// 15 minutes).
pub const DEFAULT_TRANSACTION_MAX_TIMEOUT_MS: i32 = 900_000;

/// Default compression of FetchResponse record batches (`pg_kafka.compression_type`).
pub const DEFAULT_COMPRESSION_TYPE: &str = "none";

/// The compression `pg_kafka.compression_type` names: none, gzip, snappy, lz4 or zstd,
/// case-insensitive. Anything else means no compression.
pub fn parse_compression_type(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "gzip" => Compression::Gzip,
        "snappy" => Compression::Snappy,
        "lz4" => Compression::Lz4,
        "zstd" => Compression::Zstd,
        _ => Compression::None,
    }
}

// ===== Kafka error codes =====

/// No error.
pub const ERROR_NONE: i16 = 0;
/// UNKNOWN_SERVER_ERROR: an unexpected broker-side failure.
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;
/// OFFSET_OUT_OF_RANGE: the requested offset is outside the partition's log range.
pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
/// CORRUPT_MESSAGE: a request or record batch that does not decode.
pub const ERROR_CORRUPT_MESSAGE: i16 = 2;
/// UNKNOWN_TOPIC_OR_PARTITION.
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// COORDINATOR_NOT_AVAILABLE.
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// INVALID_TOPIC_EXCEPTION: a topic name that breaks Kafka's naming rules.
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;
/// INVALID_REQUIRED_ACKS: a Produce with acks other than -1, 0 and 1.
pub const ERROR_INVALID_REQUIRED_ACKS: i16 = 21;
/// ILLEGAL_GENERATION: the consumer group generation does not match.
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;
/// INCONSISTENT_GROUP_PROTOCOL: a JoinGroup whose protocol type or protocols do not match
/// the group's.
pub const ERROR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
/// UNKNOWN_MEMBER_ID.
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
/// INVALID_SESSION_TIMEOUT: a session timeout outside the broker's accepted range
/// (`[group.min.session.timeout.ms, group.max.session.timeout.ms]`).
pub const ERROR_INVALID_SESSION_TIMEOUT: i16 = 26;
/// REBALANCE_IN_PROGRESS.
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
/// UNSUPPORTED_SASL_MECHANISM: the requested SASL mechanism is not enabled.
pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
/// ILLEGAL_SASL_STATE: a SASL request arrived in the wrong state.
pub const ERROR_ILLEGAL_SASL_STATE: i16 = 34;
/// UNSUPPORTED_VERSION.
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
/// TOPIC_ALREADY_EXISTS.
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
/// INVALID_PARTITIONS: an invalid partition count.
pub const ERROR_INVALID_PARTITIONS: i16 = 37;
/// INVALID_REPLICATION_FACTOR: a replication factor other than 1 or -1 on this
/// single-broker cluster.
pub const ERROR_INVALID_REPLICATION_FACTOR: i16 = 38;
/// INVALID_CONFIG: a configuration key or value that is invalid or unsupported.
pub const ERROR_INVALID_CONFIG: i16 = 40;
/// INVALID_REQUEST: a malformed request, or one that targets an unsupported entity.
pub const ERROR_INVALID_REQUEST: i16 = 42;
/// OUT_OF_ORDER_SEQUENCE_NUMBER: a gap in an idempotent producer's sequence.
pub const ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// DUPLICATE_SEQUENCE_NUMBER.
pub const ERROR_DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// INVALID_PRODUCER_EPOCH: a Produce carries an epoch older than the producer's current one.
pub const ERROR_INVALID_PRODUCER_EPOCH: i16 = 47;
/// INVALID_TXN_STATE: an invalid transaction state transition.
pub const ERROR_INVALID_TXN_STATE: i16 = 48;
/// INVALID_TRANSACTION_TIMEOUT: an InitProducerId timeout <= 0 or above
/// `transaction.max.timeout.ms`.
pub const ERROR_INVALID_TRANSACTION_TIMEOUT: i16 = 50;
/// CONCURRENT_TRANSACTIONS: the producer has another transaction completing.
pub const ERROR_CONCURRENT_TRANSACTIONS: i16 = 51;
/// OPERATION_NOT_ATTEMPTED: the request failed for another of its entries.
pub const ERROR_OPERATION_NOT_ATTEMPTED: i16 = 55;
/// SASL_AUTHENTICATION_FAILED.
pub const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;
/// UNKNOWN_PRODUCER_ID.
pub const ERROR_UNKNOWN_PRODUCER_ID: i16 = 59;
/// NON_EMPTY_GROUP: a group with members cannot be deleted.
pub const ERROR_NON_EMPTY_GROUP: i16 = 68;
/// GROUP_ID_NOT_FOUND.
pub const ERROR_GROUP_ID_NOT_FOUND: i16 = 69;
/// GROUP_MAX_SIZE_REACHED: the consumer group has its configured maximum of members.
pub const ERROR_GROUP_MAX_SIZE_REACHED: i16 = 81;
/// INVALID_RECORD: a record (or record batch) failed broker-side validation, e.g. a
/// Produce v3+ partition carrying more than one record batch.
pub const ERROR_INVALID_RECORD: i16 = 87;
/// UNSTABLE_OFFSET_COMMIT: a transactional offset commit for the partition is pending
/// (KIP-447).
pub const ERROR_UNSTABLE_OFFSET_COMMIT: i16 = 88;
/// PRODUCER_FENCED: a newer producer with the same transactional id took over. Kafka has
/// no "transaction timed out" code (94 is INCONSISTENT_VOTER_SET): a transaction that times
/// out fences its producer, which then sees this code.
pub const ERROR_PRODUCER_FENCED: i16 = 90;
/// TRANSACTIONAL_ID_NOT_FOUND.
pub const ERROR_TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;

// ===== Shadow mode configuration =====

/// Default of `pg_kafka.shadow_mode_enabled`.
pub const DEFAULT_SHADOW_MODE_ENABLED: bool = false;

/// Default of `pg_kafka.shadow_bootstrap_servers` (empty: no external cluster).
pub const DEFAULT_SHADOW_BOOTSTRAP_SERVERS: &str = "";

/// Default of `pg_kafka.shadow_security_protocol`.
pub const DEFAULT_SHADOW_SECURITY_PROTOCOL: &str = "SASL_SSL";

/// Default of `pg_kafka.shadow_sasl_mechanism`.
pub const DEFAULT_SHADOW_SASL_MECHANISM: &str = "PLAIN";

/// Default shadow batch size (records per outbox claim and librdkafka batch).
pub const DEFAULT_SHADOW_BATCH_SIZE: i32 = 1000;

/// Minimum shadow batch size.
pub const MIN_SHADOW_BATCH_SIZE: i32 = 1;

/// Maximum shadow batch size.
pub const MAX_SHADOW_BATCH_SIZE: i32 = 100_000;

/// Default shadow linger time (milliseconds).
pub const DEFAULT_SHADOW_LINGER_MS: i32 = 10;

/// Minimum shadow linger time.
pub const MIN_SHADOW_LINGER_MS: i32 = 0;

/// Maximum shadow linger time.
pub const MAX_SHADOW_LINGER_MS: i32 = 60_000;

/// Default shadow retry backoff (milliseconds).
pub const DEFAULT_SHADOW_RETRY_BACKOFF_MS: i32 = 100;

/// Minimum shadow retry backoff.
pub const MIN_SHADOW_RETRY_BACKOFF_MS: i32 = 10;

/// Maximum shadow retry backoff.
pub const MAX_SHADOW_RETRY_BACKOFF_MS: i32 = 60_000;

/// Default shadow max retries: failed delivery attempts after which an outbox row is
/// dead-lettered.
pub const DEFAULT_SHADOW_MAX_RETRIES: i32 = 10;

/// Minimum shadow max retries.
pub const MIN_SHADOW_MAX_RETRIES: i32 = 0;

/// Maximum shadow max retries.
pub const MAX_SHADOW_MAX_RETRIES: i32 = 100;

/// Default of `pg_kafka.shadow_metrics_enabled`.
pub const DEFAULT_SHADOW_METRICS_ENABLED: bool = true;

/// Default of `pg_kafka.shadow_sync_timeout_ms`: how long a sync-mode produce response
/// waits for the external ack.
pub const DEFAULT_SHADOW_SYNC_TIMEOUT_MS: i32 = 5_000;
/// Minimum (0 = sync responses are not held).
pub const MIN_SHADOW_SYNC_TIMEOUT_MS: i32 = 0;
/// Maximum.
pub const MAX_SHADOW_SYNC_TIMEOUT_MS: i32 = 120_000;

/// Default of `pg_kafka.config_reload_interval_ms`: how often the worker reloads GUCs and
/// the shadow configuration (30 seconds).
pub const DEFAULT_CONFIG_RELOAD_MS: i32 = 30_000;

/// Minimum config reload interval (milliseconds).
pub const MIN_CONFIG_RELOAD_MS: i32 = 100;

/// Maximum config reload interval (5 minutes).
pub const MAX_CONFIG_RELOAD_MS: i32 = 300_000;

// ===== Shadow mode error codes (internal, outside Kafka's range) =====

/// Shadow forwarding failed.
pub const ERROR_SHADOW_FORWARD_FAILED: i16 = 1001;

/// Shadow mode is not configured.
pub const ERROR_SHADOW_NOT_CONFIGURED: i16 = 1002;

/// The external Kafka cluster is unavailable.
pub const ERROR_SHADOW_KAFKA_UNAVAILABLE: i16 = 1003;

/// A shadow replay failed.
pub const ERROR_SHADOW_REPLAY_FAILED: i16 = 1004;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_keys_match_kafka_spec() {
        // https://kafka.apache.org/protocol.html#protocol_api_key
        assert_eq!(API_KEY_PRODUCE, 0, "Produce API key should be 0");
        assert_eq!(API_KEY_FETCH, 1, "Fetch API key should be 1");
        assert_eq!(API_KEY_METADATA, 3, "Metadata API key should be 3");
        assert_eq!(API_KEY_API_VERSIONS, 18, "ApiVersions API key should be 18");
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

        // Idempotent-producer and transaction codes, as in Apache Kafka's
        // org.apache.kafka.common.protocol.Errors.
        assert_eq!(ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER, 45);
        assert_eq!(ERROR_DUPLICATE_SEQUENCE_NUMBER, 46);
        assert_eq!(ERROR_INVALID_TXN_STATE, 48);
        assert_eq!(ERROR_CONCURRENT_TRANSACTIONS, 51);
        assert_eq!(ERROR_UNKNOWN_PRODUCER_ID, 59);
        assert_eq!(ERROR_PRODUCER_FENCED, 90);
        assert_eq!(ERROR_TRANSACTIONAL_ID_NOT_FOUND, 105);
    }

    #[test]
    fn test_default_host_is_loopback() {
        // The listener has no authentication: the out-of-the-box bind must not be
        // reachable from other hosts.
        let addr: std::net::IpAddr = DEFAULT_HOST.parse().unwrap();
        assert!(addr.is_loopback());
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
