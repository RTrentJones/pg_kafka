// Message types module for pg_kafka
//
// This module defines the message types used to communicate between
// the network thread (tokio) and the database thread (main BGWorker).
//
// ## Two-Thread Architecture
//
// - Network thread: Parses Kafka requests, sends via crossbeam channel
// - Database thread: Receives requests, processes via SPI, sends responses
// - Response channel: Per-connection tokio mpsc for async response handling
//
// The crossbeam channel is created in worker.rs and passed to the listener.

/// Kafka request types that can be sent from async tasks to the main worker thread
#[derive(Debug)]
pub enum KafkaRequest {
    /// ApiVersions request - asks which API versions the broker supports
    ApiVersions {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Channel to send the response back to the specific connection
        /// Using tokio::sync::mpsc for async-friendly response delivery
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// Metadata request - asks for topic and broker metadata
    Metadata {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// List of topics to get metadata for (None = all topics)
        topics: Option<Vec<String>>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// Produce request - write messages to topic partitions
    Produce {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Acknowledgment level (0=none, 1=leader, -1=all ISR)
        acks: i16,
        /// Timeout for waiting for acknowledgments (milliseconds)
        timeout_ms: i32,
        /// Topic data (topic → partitions → records)
        topic_data: Vec<TopicProduceData>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// Fetch request - read messages from topic partitions
    Fetch {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Maximum time to wait for data (milliseconds) - used for long polling
        max_wait_ms: i32,
        /// Minimum bytes to wait for before responding (for batching efficiency)
        min_bytes: i32,
        /// Maximum bytes to return (total across all partitions)
        max_bytes: i32,
        /// Topic-partition fetch data
        topic_data: Vec<TopicFetchData>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// OffsetCommit request - commit consumed offsets for a consumer group
    OffsetCommit {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Consumer group ID
        group_id: String,
        /// Topic-partition offset data to commit
        topics: Vec<OffsetCommitTopicData>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// OffsetFetch request - fetch committed offsets for a consumer group
    OffsetFetch {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Consumer group ID
        group_id: String,
        /// Topics to fetch offsets for (None = all topics)
        topics: Option<Vec<OffsetFetchTopicData>>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// FindCoordinator request - discover the coordinator for a consumer group
    FindCoordinator {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Coordinator key (consumer group ID for consumer groups)
        key: String,
        /// Key type (0 = consumer group, 1 = transaction coordinator)
        key_type: i8,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// JoinGroup request - consumer joins a consumer group
    JoinGroup {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Consumer group ID
        group_id: String,
        /// Session timeout in milliseconds
        session_timeout_ms: i32,
        /// Rebalance timeout in milliseconds (v1+)
        rebalance_timeout_ms: i32,
        /// Member ID ("" for first join, assigned ID for rejoin)
        member_id: String,
        /// Static group instance ID for static membership (v5+)
        group_instance_id: Option<String>,
        /// Protocol type (e.g., "consumer")
        protocol_type: String,
        /// Supported assignment strategies with metadata
        protocols: Vec<JoinGroupProtocol>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// SyncGroup request - synchronize partition assignments
    SyncGroup {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Consumer group ID
        group_id: String,
        /// Generation ID from JoinGroup response
        generation_id: i32,
        /// Member ID from JoinGroup response
        member_id: String,
        /// Static group instance ID (v3+)
        group_instance_id: Option<String>,
        /// Protocol type (v5+)
        protocol_type: Option<String>,
        /// Protocol name (v5+)
        protocol_name: Option<String>,
        /// Partition assignments (only leader sends non-empty)
        assignments: Vec<SyncGroupAssignment>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// Heartbeat request - maintain consumer group membership
    Heartbeat {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Consumer group ID
        group_id: String,
        /// Generation ID from JoinGroup response
        generation_id: i32,
        /// Member ID from JoinGroup response
        member_id: String,
        /// Static group instance ID (v3+)
        group_instance_id: Option<String>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// LeaveGroup request - consumer leaves a consumer group
    LeaveGroup {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Consumer group ID
        group_id: String,
        /// Member ID from JoinGroup response (v0-v2)
        member_id: String,
        /// Members leaving (v3+, batch support)
        members: Vec<MemberIdentity>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// ListOffsets request - query earliest/latest offsets for partitions
    ListOffsets {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Replica ID (-1 for consumer, broker ID for replicas)
        replica_id: i32,
        /// Isolation level (0=READ_UNCOMMITTED, 1=READ_COMMITTED) - v2+
        isolation_level: i8,
        /// Topics to list offsets for
        topics: Vec<ListOffsetsTopicData>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// DescribeGroups request - get consumer group state and members
    DescribeGroups {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// List of group IDs to describe
        groups: Vec<String>,
        /// Include authorized operations (v3+)
        include_authorized_operations: bool,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// ListGroups request - list all consumer groups
    ListGroups {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// States filter (v4+) - empty means all states
        states_filter: Vec<String>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// CreateTopics request - create topics programmatically (Phase 6)
    CreateTopics {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Topics to create with configuration
        topics: Vec<CreateTopicRequest>,
        /// Timeout for the request in milliseconds
        timeout_ms: i32,
        /// If true, check if topics can be created but don't actually create them
        validate_only: bool,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// DeleteTopics request - delete topics programmatically (Phase 6)
    DeleteTopics {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Topic names to delete
        topic_names: Vec<String>,
        /// Timeout for the request in milliseconds
        timeout_ms: i32,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// CreatePartitions request - add partitions to existing topics (Phase 6)
    CreatePartitions {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Topics with new partition counts
        topics: Vec<CreatePartitionsTopicRequest>,
        /// Timeout for the request in milliseconds
        timeout_ms: i32,
        /// If true, check if partitions can be created but don't actually create them
        validate_only: bool,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
    /// DeleteGroups request - delete consumer groups (Phase 6)
    DeleteGroups {
        /// Correlation ID from client - MUST be echoed back in response
        correlation_id: i32,
        /// Optional client identifier string
        client_id: Option<String>,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// Group IDs to delete
        groups_names: Vec<String>,
        /// Channel to send the response back to the specific connection
        response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    },
}

/// Kafka response types sent back from main thread to async tasks
///
/// Phase 2 Refactoring: We wrap kafka-protocol response types directly to eliminate
/// the verbose conversion code in protocol.rs::encode_response()
#[derive(Debug, Clone)]
pub enum KafkaResponse {
    /// ApiVersions response - wraps kafka-protocol's ApiVersionsResponse
    ApiVersions {
        /// Correlation ID from request - client uses this to match responses
        correlation_id: i32,
        /// API version to use for encoding the response
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::api_versions_response::ApiVersionsResponse,
    },
    /// Metadata response - wraps kafka-protocol's MetadataResponse
    Metadata {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::metadata_response::MetadataResponse,
    },
    /// Produce response - wraps kafka-protocol's ProduceResponse
    Produce {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::produce_response::ProduceResponse,
    },
    /// Fetch response - wraps kafka-protocol's FetchResponse
    Fetch {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::fetch_response::FetchResponse,
    },
    /// OffsetCommit response - wraps kafka-protocol's OffsetCommitResponse
    OffsetCommit {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::offset_commit_response::OffsetCommitResponse,
    },
    /// OffsetFetch response - wraps kafka-protocol's OffsetFetchResponse
    OffsetFetch {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::offset_fetch_response::OffsetFetchResponse,
    },
    /// FindCoordinator response - wraps kafka-protocol's FindCoordinatorResponse
    FindCoordinator {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse,
    },
    /// JoinGroup response - wraps kafka-protocol's JoinGroupResponse
    JoinGroup {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::join_group_response::JoinGroupResponse,
    },
    /// SyncGroup response - wraps kafka-protocol's SyncGroupResponse
    SyncGroup {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::sync_group_response::SyncGroupResponse,
    },
    /// Heartbeat response - wraps kafka-protocol's HeartbeatResponse
    Heartbeat {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::heartbeat_response::HeartbeatResponse,
    },
    /// LeaveGroup response - wraps kafka-protocol's LeaveGroupResponse
    LeaveGroup {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::leave_group_response::LeaveGroupResponse,
    },
    /// ListOffsets response - wraps kafka-protocol's ListOffsetsResponse
    ListOffsets {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::list_offsets_response::ListOffsetsResponse,
    },
    /// DescribeGroups response - wraps kafka-protocol's DescribeGroupsResponse
    DescribeGroups {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::describe_groups_response::DescribeGroupsResponse,
    },
    /// ListGroups response - wraps kafka-protocol's ListGroupsResponse
    ListGroups {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::list_groups_response::ListGroupsResponse,
    },
    /// CreateTopics response - wraps kafka-protocol's CreateTopicsResponse
    CreateTopics {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::create_topics_response::CreateTopicsResponse,
    },
    /// DeleteTopics response - wraps kafka-protocol's DeleteTopicsResponse
    DeleteTopics {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::delete_topics_response::DeleteTopicsResponse,
    },
    /// CreatePartitions response - wraps kafka-protocol's CreatePartitionsResponse
    CreatePartitions {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::create_partitions_response::CreatePartitionsResponse,
    },
    /// DeleteGroups response - wraps kafka-protocol's DeleteGroupsResponse
    DeleteGroups {
        /// Correlation ID from request
        correlation_id: i32,
        /// API version from the request (needed for response encoding)
        api_version: i16,
        /// The kafka-protocol response struct (ready to encode)
        response: kafka_protocol::messages::delete_groups_response::DeleteGroupsResponse,
    },
    /// Error response for unsupported or malformed requests
    Error {
        /// Correlation ID from request
        correlation_id: i32,
        /// Error code (Kafka error codes)
        error_code: i16,
        /// Human-readable error message
        error_message: Option<String>,
    },
}

/// Data for producing to a topic
#[derive(Debug, Clone)]
pub struct TopicProduceData {
    /// Topic name
    pub name: String,
    /// Partitions to write to
    pub partitions: Vec<PartitionProduceData>,
}

/// Data for producing to a partition
#[derive(Debug, Clone)]
pub struct PartitionProduceData {
    /// Partition ID
    pub partition_index: i32,
    /// Records to write
    pub records: Vec<Record>,
}

/// A single Kafka record/message
#[derive(Debug, Clone)]
pub struct Record {
    /// Optional message key (used for partitioning and log compaction)
    pub key: Option<Vec<u8>>,
    /// Optional message value (payload)
    pub value: Option<Vec<u8>>,
    /// Message headers (key-value metadata)
    pub headers: Vec<RecordHeader>,
    /// Timestamp (milliseconds since epoch, optional)
    pub timestamp: Option<i64>,
}

/// Record header (key-value metadata)
#[derive(Debug, Clone)]
pub struct RecordHeader {
    /// Header key (UTF-8 string)
    pub key: String,
    /// Header value (binary data)
    pub value: Vec<u8>,
}

/// Response for a topic in ProduceResponse
#[derive(Debug, Clone)]
pub struct TopicProduceResponse {
    /// Topic name
    pub name: String,
    /// Per-partition responses
    pub partitions: Vec<PartitionProduceResponse>,
}

/// Response for a partition in ProduceResponse
#[derive(Debug, Clone)]
pub struct PartitionProduceResponse {
    /// Partition ID
    pub partition_index: i32,
    /// Error code (0 = success)
    pub error_code: i16,
    /// Base offset assigned to the first message in the batch
    /// This is the partition_offset (NOT global_offset)
    pub base_offset: i64,
    /// Timestamp when the log was appended (-1 if not used)
    pub log_append_time: i64,
    /// Earliest available offset in this partition (-1 if not tracked)
    pub log_start_offset: i64,
}

/// Data for fetching from a topic
#[derive(Debug, Clone)]
pub struct TopicFetchData {
    /// Topic name
    pub name: String,
    /// Partitions to fetch from
    pub partitions: Vec<PartitionFetchData>,
}

/// Data for fetching from a partition
#[derive(Debug, Clone)]
pub struct PartitionFetchData {
    /// Partition ID
    pub partition_index: i32,
    /// Offset to start fetching from
    pub fetch_offset: i64,
    /// Maximum bytes to fetch from this partition
    pub partition_max_bytes: i32,
}

/// Data for committing offsets for a topic
#[derive(Debug, Clone)]
pub struct OffsetCommitTopicData {
    /// Topic name
    pub name: String,
    /// Partitions to commit offsets for
    pub partitions: Vec<OffsetCommitPartitionData>,
}

/// Data for committing offset for a partition
#[derive(Debug, Clone)]
pub struct OffsetCommitPartitionData {
    /// Partition ID
    pub partition_index: i32,
    /// Offset to commit (consumer will fetch from committed_offset + 1)
    pub committed_offset: i64,
    /// Optional metadata
    pub metadata: Option<String>,
}

/// Data for fetching offsets for a topic
#[derive(Debug, Clone)]
pub struct OffsetFetchTopicData {
    /// Topic name
    pub name: String,
    /// Partitions to fetch offsets for
    pub partition_indexes: Vec<i32>,
}

/// JoinGroup protocol (assignment strategy with metadata)
#[derive(Debug, Clone)]
pub struct JoinGroupProtocol {
    /// Protocol name (e.g., "range", "roundrobin", "sticky")
    pub name: String,
    /// Protocol-specific subscription metadata (encoded MemberSubscription)
    pub metadata: Vec<u8>,
}

/// SyncGroup assignment (member ID → partition assignment)
#[derive(Debug, Clone)]
pub struct SyncGroupAssignment {
    /// Member ID to assign partitions to
    pub member_id: String,
    /// Encoded MemberAssignment for this member
    pub assignment: Vec<u8>,
}

/// Member identity for LeaveGroup request (v3+)
#[derive(Debug, Clone)]
pub struct MemberIdentity {
    /// Member ID leaving the group
    pub member_id: String,
    /// Static group instance ID (if using static membership)
    pub group_instance_id: Option<String>,
}

/// Data for listing offsets for a topic
#[derive(Debug, Clone)]
pub struct ListOffsetsTopicData {
    /// Topic name
    pub name: String,
    /// Partitions to list offsets for
    pub partitions: Vec<ListOffsetsPartitionData>,
}

/// Data for listing offsets for a partition
#[derive(Debug, Clone)]
pub struct ListOffsetsPartitionData {
    /// Partition ID
    pub partition_index: i32,
    /// Current leader epoch (for fencing, -1 if not used)
    pub current_leader_epoch: i32,
    /// Timestamp to query:
    /// - -2 = earliest offset
    /// - -1 = latest offset
    /// - >= 0 = offset at timestamp (v1+)
    pub timestamp: i64,
}

// ========== Admin API Request Types ==========

/// Request data for creating a single topic (Phase 6)
#[derive(Debug, Clone)]
pub struct CreateTopicRequest {
    /// Topic name
    pub name: String,
    /// Number of partitions (use -1 for broker default)
    pub num_partitions: i32,
    /// Replication factor (use -1 for broker default)
    pub replication_factor: i16,
}

/// Request data for adding partitions to a topic (Phase 6)
#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicRequest {
    /// Topic name
    pub name: String,
    /// Total number of partitions after the operation
    pub count: i32,
}

// ========== From implementations for type conversions ==========
// These reduce boilerplate conversion code in decoding.rs

impl From<kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol>
    for JoinGroupProtocol
{
    fn from(p: kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol) -> Self {
        JoinGroupProtocol {
            name: p.name.to_string(),
            metadata: p.metadata.to_vec(),
        }
    }
}

impl From<kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment>
    for SyncGroupAssignment
{
    fn from(a: kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment) -> Self {
        SyncGroupAssignment {
            member_id: a.member_id.to_string(),
            assignment: a.assignment.to_vec(),
        }
    }
}

impl From<kafka_protocol::messages::leave_group_request::MemberIdentity> for MemberIdentity {
    fn from(m: kafka_protocol::messages::leave_group_request::MemberIdentity) -> Self {
        MemberIdentity {
            member_id: m.member_id.to_string(),
            group_instance_id: m.group_instance_id.map(|s| s.to_string()),
        }
    }
}

impl From<kafka_protocol::messages::list_offsets_request::ListOffsetsPartition>
    for ListOffsetsPartitionData
{
    fn from(p: kafka_protocol::messages::list_offsets_request::ListOffsetsPartition) -> Self {
        ListOffsetsPartitionData {
            partition_index: p.partition_index,
            current_leader_epoch: p.current_leader_epoch,
            timestamp: p.timestamp,
        }
    }
}

// Note: The request channel is now created in worker.rs and passed explicitly
// to the listener. This enables:
// - Bounded channels for backpressure (10,000 capacity)
// - Cleaner ownership model (no global static)
// - Easier testing with injected channels
