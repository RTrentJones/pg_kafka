//! What travels between the network thread and the DB thread.
//!
//! [`KafkaRequest`] and [`KafkaResponse`] are the envelopes around the per-API bodies, which
//! the API registry (`crate::kafka::api`) generates; [`RequestLanes`] are the two channels
//! requests cross on, created in `worker::network` and handed to the listener; the remaining types
//! are the request data the handlers take.

use std::collections::HashMap;
use std::sync::Arc;

use crate::kafka::api::{Lane, RequestBody, RequestHeader, ResponseBody};

/// Type alias for TxnOffsetCommit topic data: (topic_name, [(partition, offset, metadata)])
pub type TxnOffsetCommitTopics = Vec<(String, Vec<(i32, i64, Option<String>)>)>;

/// Type alias for IncrementalAlterConfigs resources:
/// (resource_type, resource_name, [(config_name, operation, value)])
pub type AlterConfigsResources = Vec<(i8, String, Vec<(String, i8, Option<String>)>)>;

/// The two request lanes from the network thread to the single DB thread.
///
/// On one FIFO channel a heavy Produce (large UNNEST, advisory-lock wait) would delay
/// every queued Heartbeat behind it, and under DB pressure delayed heartbeats cascade into
/// spurious consumer-group rebalances, which add more load. Heartbeat is served purely from
/// the in-memory coordinator, so it travels on a dedicated `liveness` lane that the worker
/// drains *before* taking the next main-lane request: a heartbeat can wait behind the one
/// request executing, never behind the queue. Which lane an API uses is
/// [`crate::kafka::api::KafkaApi::LANE`].
#[derive(Clone)]
pub struct RequestLanes {
    /// Everything except liveness traffic (Produce, Fetch, Metadata, ...).
    pub main: crossbeam_channel::Sender<KafkaRequest>,
    /// Liveness traffic (Heartbeat), drained with priority by the DB thread.
    pub liveness: crossbeam_channel::Sender<KafkaRequest>,
}

impl RequestLanes {
    /// Pick the lane for a parsed request.
    pub fn route(&self, request: &KafkaRequest) -> &crossbeam_channel::Sender<KafkaRequest> {
        match request.body.lane() {
            Lane::Liveness => &self.liveness,
            Lane::Main => &self.main,
        }
    }
}

/// A parsed request on its way from the network thread to the DB thread.
#[derive(Debug)]
pub struct KafkaRequest {
    /// API key, version, correlation id and client id.
    pub header: RequestHeader,
    /// The API-specific request.
    pub body: RequestBody,
    /// Where the response goes (the connection's per-request slot).
    pub response_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
}

impl KafkaRequest {
    /// Replace this request's response channel, returning the previous sender.
    ///
    /// The worker swaps in a same-typed buffer channel before dispatching
    /// the request and forwards buffered responses to the returned (real) sender
    /// only after `BackgroundWorker::transaction` has committed, so a client can
    /// never observe an ack for state that is not yet durable/visible (RB-1).
    ///
    /// APIs whose reply the coordinator may hold past the request's transaction
    /// ([`crate::kafka::api::KafkaApi::DEFERRED_REPLY`]: JoinGroup, SyncGroup) are the
    /// exceptions. Neither writes to the database, so there is nothing to commit first.
    /// Their own sender is left in place, `new_tx` is dropped, and a closed sender is
    /// returned, so the worker's flush and commit-failure paths deliver nothing for them.
    pub fn swap_response_tx(
        &mut self,
        new_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    ) -> tokio::sync::mpsc::UnboundedSender<KafkaResponse> {
        if self.body.deferred_reply() {
            drop(new_tx);
            let (closed_tx, _) = tokio::sync::mpsc::unbounded_channel();
            closed_tx
        } else {
            std::mem::replace(&mut self.response_tx, new_tx)
        }
    }
}

/// A response on its way from the DB thread (or the decoder) to the connection writer.
#[derive(Debug, Clone)]
pub struct KafkaResponse {
    /// Correlation id of the request it answers.
    pub correlation_id: i32,
    /// Version to encode the body at.
    pub api_version: i16,
    /// The API-specific response body.
    pub body: ResponseBody,
}

impl KafkaResponse {
    /// A response envelope.
    pub fn new(correlation_id: i32, api_version: i16, body: ResponseBody) -> Self {
        Self {
            correlation_id,
            api_version,
            body,
        }
    }
}

/// Data for producing to a topic
#[derive(Debug, Clone)]
pub struct TopicProduceData {
    /// Topic name
    pub name: String,
    /// Partitions to write to
    pub partitions: Vec<PartitionProduceData>,
}

/// A Produce partition whose data failed Kafka's validation before or while it decoded:
/// nothing of it is written, and it is answered with its own error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedPartition {
    /// Topic name, one allocation shared by the refused partitions of a topic entry: a
    /// partition entry takes 8 bytes on the wire and a name up to the whole frame, so a copy
    /// per partition would take memory of the frame's size squared.
    pub topic: Arc<str>,
    /// Partition ID
    pub partition: i32,
    /// The error validation found (UNKNOWN_TOPIC_OR_PARTITION takes its place when the
    /// topic or partition does not exist).
    pub error_code: i16,
}

/// The places of the topics of refused partitions in a list of topics, by name. The
/// refused partitions of a topic entry share one name ([`RefusedPartition::topic`]) and come
/// one after another, so a name is hashed once per such run, not once per partition: a
/// request of a long name and many partitions would otherwise hash the name per partition,
/// time of the frame's size squared.
#[derive(Debug, Default)]
pub struct TopicPlaces {
    at: HashMap<Arc<str>, usize>,
    last: Option<(Arc<str>, usize)>,
}

impl TopicPlaces {
    /// Places for `names`, the first at 0.
    pub fn of<'a>(names: impl IntoIterator<Item = &'a str>) -> Self {
        let mut places = Self::default();
        for (at, name) in names.into_iter().enumerate() {
            places.at.entry(Arc::from(name)).or_insert(at);
        }
        places
    }

    /// The place of `topic`; `add` places a name not seen yet and returns its place.
    pub fn place(&mut self, topic: &Arc<str>, add: impl FnOnce() -> usize) -> usize {
        if let Some((last, at)) = &self.last {
            if Arc::ptr_eq(last, topic) {
                return *at;
            }
        }
        let at = *self.at.entry(Arc::clone(topic)).or_insert_with(add);
        self.last = Some((Arc::clone(topic), at));
        at
    }
}

/// Data for producing to a partition
#[derive(Debug, Clone)]
pub struct PartitionProduceData {
    /// Partition ID
    pub partition_index: i32,
    /// Records to write
    pub records: Vec<Record>,
    /// Producer metadata from the RecordBatch header; `None` when the partition carried no
    /// record data (null records).
    pub producer_metadata: Option<ProducerMetadata>,
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

/// The producer identity and sequence of a RecordBatch, for idempotent-producer
/// validation.
#[derive(Debug, Clone, Default)]
pub struct ProducerMetadata {
    /// Producer ID allocated by InitProducerId (-1 for non-idempotent producers)
    pub producer_id: i64,
    /// Producer epoch for fencing (-1 for non-idempotent producers)
    pub producer_epoch: i16,
    /// Base sequence number for this batch (first record's sequence)
    pub base_sequence: i32,
    /// Whether the batch is transactional (its records belong to the producer's
    /// transaction).
    pub is_transactional: bool,
}

/// Record header (key-value metadata)
#[derive(Debug, Clone)]
pub struct RecordHeader {
    /// Header key (UTF-8 string)
    pub key: String,
    /// Header value (binary data)
    pub value: Vec<u8>,
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

/// Request data for creating a single topic.
#[derive(Debug, Clone)]
pub struct CreateTopicRequest {
    /// Topic name
    pub name: String,
    /// Number of partitions (use -1 for broker default)
    pub num_partitions: i32,
    /// Replication factor (use -1 for broker default)
    pub replication_factor: i16,
}

/// Request data for adding partitions to a topic.
#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicRequest {
    /// Topic name
    pub name: String,
    /// Total number of partitions after the operation
    pub count: i32,
}

// ========== Conversions from the wire types ==========

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::{HeartbeatRequestData, ProduceRequestData, SyncGroupRequestData};

    fn header(api_key: i16, correlation_id: i32) -> RequestHeader {
        RequestHeader {
            api_key,
            api_version: 0,
            correlation_id,
            client_id: None,
        }
    }

    fn unknown_api_response(correlation_id: i32) -> KafkaResponse {
        KafkaResponse::new(
            correlation_id,
            0,
            ResponseBody::UnknownApi { error_code: 0 },
        )
    }

    // ========== RequestLanes Tests ==========

    /// Heartbeat must route to the liveness lane; everything else to the
    /// main lane. Misrouting either way silently loses the liveness guarantee
    /// (heartbeats queue behind produce) or starves the main lane's ordering.
    #[test]
    fn test_request_lanes_route_heartbeat_to_liveness() {
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(4);
        let (liveness_tx, liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(4);
        let lanes = RequestLanes {
            main: main_tx,
            liveness: liveness_tx,
        };
        let (response_tx, _response_rx) = tokio::sync::mpsc::unbounded_channel();

        let heartbeat = KafkaRequest {
            header: header(crate::kafka::constants::API_KEY_HEARTBEAT, 1),
            body: RequestBody::Heartbeat(HeartbeatRequestData {
                group_id: "g".to_string(),
                generation_id: 1,
                member_id: "m".to_string(),
                group_instance_id: None,
            }),
            response_tx: response_tx.clone(),
        };
        lanes.route(&heartbeat).send(heartbeat).unwrap();
        assert!(
            liveness_rx.try_recv().is_ok(),
            "Heartbeat must land on the liveness lane"
        );
        assert!(
            main_rx.try_recv().is_err(),
            "Heartbeat must not land on the main lane"
        );

        let api_versions = KafkaRequest {
            header: header(crate::kafka::constants::API_KEY_API_VERSIONS, 2),
            body: RequestBody::ApiVersions(crate::kafka::api::ApiVersionsRequestData),
            response_tx,
        };
        lanes.route(&api_versions).send(api_versions).unwrap();
        assert!(
            main_rx.try_recv().is_ok(),
            "Non-heartbeat requests must land on the main lane"
        );
        assert!(
            liveness_rx.try_recv().is_err(),
            "Non-heartbeat requests must not land on the liveness lane"
        );
    }

    /// Only Heartbeat uses the liveness lane, and only JoinGroup/SyncGroup defer their
    /// reply; every other registered API takes the main lane and the commit barrier.
    #[test]
    fn test_lane_and_deferred_reply_per_api() {
        use crate::kafka::constants::*;
        for info in crate::kafka::api::tests::worker_apis() {
            let liveness = info.key == API_KEY_HEARTBEAT;
            let deferred = info.key == API_KEY_JOIN_GROUP || info.key == API_KEY_SYNC_GROUP;
            let body = crate::kafka::api::tests::sample_request(info.key);
            assert_eq!(body.api_key(), info.key);
            assert_eq!(body.lane() == Lane::Liveness, liveness, "{}", info.name);
            assert_eq!(body.deferred_reply(), deferred, "{}", info.name);
        }
    }

    // ========== RB-1 barrier accessor tests ==========

    /// `swap_response_tx` must install the new sender on the request and
    /// hand back the original one. If the swap silently returned the wrong end,
    /// the worker's post-commit flush would deliver responses to the buffer
    /// instead of the client (hang) or vice versa (barrier bypassed).
    #[test]
    fn test_swap_response_tx_returns_original_sender() {
        let (orig_tx, mut orig_rx) = tokio::sync::mpsc::unbounded_channel();
        let (buf_tx, mut buf_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut request = KafkaRequest {
            header: header(crate::kafka::constants::API_KEY_PRODUCE, 7),
            body: RequestBody::Produce(ProduceRequestData {
                acks: 1,
                timeout_ms: 1000,
                refused: Vec::new(),
                topic_data: vec![],
                transactional_id: None,
                decoded: crate::kafka::protocol::DecodedCharge::default(),
                pending: None,
            }),
            response_tx: orig_tx,
        };

        let returned = request.swap_response_tx(buf_tx);

        // The returned sender is the original: sending on it reaches orig_rx.
        returned.send(unknown_api_response(7)).unwrap();
        assert!(
            orig_rx.try_recv().is_ok(),
            "returned sender must be the original"
        );

        // The request now holds the buffer sender: a handler-side send lands in buf_rx.
        request.response_tx.send(unknown_api_response(7)).unwrap();
        assert!(
            buf_rx.try_recv().is_ok(),
            "request must now hold the new sender"
        );
    }

    /// Every non-deferred API gets the buffer installed by `swap_response_tx`.
    #[test]
    fn test_swap_response_tx_all_apis() {
        for info in crate::kafka::api::tests::worker_apis() {
            let (orig_tx, mut orig_rx) = tokio::sync::mpsc::unbounded_channel();
            let (buf_tx, mut buf_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut request = KafkaRequest {
                header: header(info.key, 1),
                body: crate::kafka::api::tests::sample_request(info.key),
                response_tx: orig_tx,
            };
            let deferred = request.body.deferred_reply();
            let returned = request.swap_response_tx(buf_tx);
            request.response_tx.send(unknown_api_response(1)).unwrap();
            if deferred {
                assert!(
                    returned.send(unknown_api_response(1)).is_err(),
                    "{}",
                    info.name
                );
                assert!(buf_rx.try_recv().is_err(), "{}", info.name);
                assert!(orig_rx.try_recv().is_ok(), "{}", info.name);
            } else {
                assert!(buf_rx.try_recv().is_ok(), "{}", info.name);
                returned.send(unknown_api_response(1)).unwrap();
                assert!(orig_rx.try_recv().is_ok(), "{}", info.name);
            }
        }
    }

    /// SyncGroup, like JoinGroup, keeps the client's own sender: a follower's SyncGroup
    /// may be answered after its transaction, when the leader syncs.
    #[test]
    fn test_swap_response_tx_leaves_sync_group_sender() {
        let (orig_tx, mut orig_rx) = tokio::sync::mpsc::unbounded_channel();
        let (buf_tx, mut buf_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut request = KafkaRequest {
            header: header(crate::kafka::constants::API_KEY_SYNC_GROUP, 4),
            body: RequestBody::SyncGroup(SyncGroupRequestData {
                group_id: "g".to_string(),
                generation_id: 1,
                member_id: "m".to_string(),
                group_instance_id: None,
                protocol_type: None,
                protocol_name: None,
                assignments: vec![],
            }),
            response_tx: orig_tx,
        };
        let returned = request.swap_response_tx(buf_tx);
        assert!(
            returned.send(unknown_api_response(4)).is_err(),
            "returned sender is closed"
        );
        assert!(buf_rx.try_recv().is_err(), "the buffer is not installed");
        request.response_tx.send(unknown_api_response(4)).unwrap();
        assert!(
            orig_rx.try_recv().is_ok(),
            "the request keeps the client sender"
        );
    }

    // ========== RecordHeader Tests ==========

    #[test]
    fn test_record_header_new() {
        let header = RecordHeader {
            key: "content-type".to_string(),
            value: b"application/json".to_vec(),
        };
        assert_eq!(header.key, "content-type");
        assert_eq!(header.value, b"application/json".to_vec());
    }

    #[test]
    fn test_record_header_with_empty_key() {
        let header = RecordHeader {
            key: String::new(),
            value: b"value".to_vec(),
        };
        assert_eq!(header.key, "");
        assert_eq!(header.value, b"value".to_vec());
    }

    #[test]
    fn test_record_header_with_empty_value() {
        let header = RecordHeader {
            key: "key".to_string(),
            value: vec![],
        };
        assert_eq!(header.key, "key");
        assert_eq!(header.value, [] as [u8; 0]);
    }

    #[test]
    fn test_record_header_clone() {
        let header = RecordHeader {
            key: "test-key".to_string(),
            value: vec![1, 2, 3, 4, 5],
        };
        let cloned = header.clone();
        assert_eq!(cloned.key, header.key);
        assert_eq!(cloned.value, header.value);
    }

    #[test]
    fn test_record_header_debug_format() {
        let header = RecordHeader {
            key: "debug-key".to_string(),
            value: vec![0xDE, 0xAD],
        };
        let debug_str = format!("{header:?}");
        assert!(debug_str.contains("debug-key"));
        assert!(debug_str.contains("RecordHeader"));
    }

    // ========== Record Tests ==========

    #[test]
    fn test_record_new_minimal() {
        let record = Record {
            key: None,
            value: None,
            headers: vec![],
            timestamp: None,
        };
        assert!(record.key.is_none());
        assert!(record.value.is_none());
        assert!(record.headers.is_empty());
        assert!(record.timestamp.is_none());
    }

    #[test]
    fn test_record_with_all_fields() {
        let record = Record {
            key: Some(b"my-key".to_vec()),
            value: Some(b"my-value".to_vec()),
            headers: vec![RecordHeader {
                key: "h1".to_string(),
                value: b"v1".to_vec(),
            }],
            timestamp: Some(1_234_567_890_123),
        };
        assert_eq!(record.key, Some(b"my-key".to_vec()));
        assert_eq!(record.value, Some(b"my-value".to_vec()));
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.timestamp, Some(1_234_567_890_123));
    }

    #[test]
    fn test_record_with_headers() {
        let record = Record {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![
                RecordHeader {
                    key: "h1".to_string(),
                    value: b"v1".to_vec(),
                },
                RecordHeader {
                    key: "h2".to_string(),
                    value: b"v2".to_vec(),
                },
                RecordHeader {
                    key: "h3".to_string(),
                    value: b"v3".to_vec(),
                },
            ],
            timestamp: None,
        };
        assert_eq!(record.headers.len(), 3);
        assert_eq!(record.headers[0].key, "h1");
        assert_eq!(record.headers[1].key, "h2");
        assert_eq!(record.headers[2].key, "h3");
    }

    #[test]
    fn test_record_clone() {
        let record = Record {
            key: Some(b"clone-key".to_vec()),
            value: Some(b"clone-value".to_vec()),
            headers: vec![RecordHeader {
                key: "hdr".to_string(),
                value: b"val".to_vec(),
            }],
            timestamp: Some(999),
        };
        let cloned = record.clone();
        assert_eq!(cloned.key, record.key);
        assert_eq!(cloned.value, record.value);
        assert_eq!(cloned.headers.len(), record.headers.len());
        assert_eq!(cloned.timestamp, record.timestamp);
    }

    #[test]
    fn test_record_with_timestamp() {
        let record = Record {
            key: None,
            value: None,
            headers: vec![],
            timestamp: Some(0), // Epoch
        };
        assert_eq!(record.timestamp, Some(0));

        let record2 = Record {
            key: None,
            value: None,
            headers: vec![],
            timestamp: Some(i64::MAX),
        };
        assert_eq!(record2.timestamp, Some(i64::MAX));
    }

    // ========== ProducerMetadata Tests ==========

    #[test]
    fn test_producer_metadata_default() {
        let meta = ProducerMetadata::default();
        assert_eq!(meta.producer_id, 0);
        assert_eq!(meta.producer_epoch, 0);
        assert_eq!(meta.base_sequence, 0);
    }

    #[test]
    fn test_producer_metadata_with_values() {
        let meta = ProducerMetadata {
            producer_id: 12345,
            producer_epoch: 5,
            base_sequence: 100,
            is_transactional: false,
        };
        assert_eq!(meta.producer_id, 12345);
        assert_eq!(meta.producer_epoch, 5);
        assert_eq!(meta.base_sequence, 100);
    }

    #[test]
    fn test_producer_metadata_non_idempotent() {
        // Non-idempotent producers use -1 for producer_id and epoch
        let meta = ProducerMetadata {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
        };
        assert_eq!(meta.producer_id, -1);
        assert_eq!(meta.producer_epoch, -1);
        assert_eq!(meta.base_sequence, -1);
    }

    #[test]
    fn test_producer_metadata_clone() {
        let meta = ProducerMetadata {
            producer_id: 999,
            producer_epoch: 7,
            base_sequence: 42,
            is_transactional: false,
        };
        let cloned = meta.clone();
        assert_eq!(cloned.producer_id, meta.producer_id);
        assert_eq!(cloned.producer_epoch, meta.producer_epoch);
        assert_eq!(cloned.base_sequence, meta.base_sequence);
    }

    // ========== TopicProduceData Tests ==========

    #[test]
    fn test_topic_produce_data_construction() {
        let data = TopicProduceData {
            name: "test-topic".to_string(),
            partitions: vec![],
        };
        assert_eq!(data.name, "test-topic");
        assert!(data.partitions.is_empty());
    }

    #[test]
    fn test_topic_produce_data_with_partitions() {
        let data = TopicProduceData {
            name: "multi-partition".to_string(),
            partitions: vec![
                PartitionProduceData {
                    partition_index: 0,
                    records: vec![],
                    producer_metadata: None,
                },
                PartitionProduceData {
                    partition_index: 1,
                    records: vec![],
                    producer_metadata: None,
                },
            ],
        };
        assert_eq!(data.partitions.len(), 2);
        assert_eq!(data.partitions[0].partition_index, 0);
        assert_eq!(data.partitions[1].partition_index, 1);
    }

    // ========== PartitionProduceData Tests ==========

    #[test]
    fn test_partition_produce_data_empty() {
        let data = PartitionProduceData {
            partition_index: 0,
            records: vec![],
            producer_metadata: None,
        };
        assert_eq!(data.partition_index, 0);
        assert!(data.records.is_empty());
        assert!(data.producer_metadata.is_none());
    }

    #[test]
    fn test_partition_produce_data_with_records() {
        let data = PartitionProduceData {
            partition_index: 5,
            records: vec![
                Record {
                    key: Some(b"k1".to_vec()),
                    value: Some(b"v1".to_vec()),
                    headers: vec![],
                    timestamp: None,
                },
                Record {
                    key: Some(b"k2".to_vec()),
                    value: Some(b"v2".to_vec()),
                    headers: vec![],
                    timestamp: None,
                },
            ],
            producer_metadata: Some(ProducerMetadata {
                producer_id: 100,
                producer_epoch: 1,
                base_sequence: 0,
                is_transactional: false,
            }),
        };
        assert_eq!(data.partition_index, 5);
        assert_eq!(data.records.len(), 2);
        assert!(data.producer_metadata.is_some());
    }

    // ========== TopicFetchData Tests ==========

    #[test]
    fn test_topic_fetch_data_construction() {
        let data = TopicFetchData {
            name: "fetch-topic".to_string(),
            partitions: vec![],
        };
        assert_eq!(data.name, "fetch-topic");
        assert!(data.partitions.is_empty());
    }

    #[test]
    fn test_topic_fetch_data_with_partitions() {
        let data = TopicFetchData {
            name: "multi-fetch".to_string(),
            partitions: vec![
                PartitionFetchData {
                    partition_index: 0,
                    fetch_offset: 100,
                    partition_max_bytes: 1024,
                },
                PartitionFetchData {
                    partition_index: 1,
                    fetch_offset: 200,
                    partition_max_bytes: 2048,
                },
            ],
        };
        assert_eq!(data.partitions.len(), 2);
    }

    // ========== PartitionFetchData Tests ==========

    #[test]
    fn test_partition_fetch_data() {
        let data = PartitionFetchData {
            partition_index: 3,
            fetch_offset: 12345,
            partition_max_bytes: 1_048_576,
        };
        assert_eq!(data.partition_index, 3);
        assert_eq!(data.fetch_offset, 12345);
        assert_eq!(data.partition_max_bytes, 1_048_576);
    }

    // ========== OffsetCommitTopicData Tests ==========

    #[test]
    fn test_offset_commit_topic_data() {
        let data = OffsetCommitTopicData {
            name: "commit-topic".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 500,
                metadata: Some("test-metadata".to_string()),
            }],
        };
        assert_eq!(data.name, "commit-topic");
        assert_eq!(data.partitions.len(), 1);
        assert_eq!(data.partitions[0].committed_offset, 500);
    }

    // ========== OffsetCommitPartitionData Tests ==========

    #[test]
    fn test_offset_commit_partition_data_with_metadata() {
        let data = OffsetCommitPartitionData {
            partition_index: 2,
            committed_offset: 999,
            metadata: Some("my-consumer-metadata".to_string()),
        };
        assert_eq!(data.partition_index, 2);
        assert_eq!(data.committed_offset, 999);
        assert_eq!(data.metadata, Some("my-consumer-metadata".to_string()));
    }

    #[test]
    fn test_offset_commit_partition_data_without_metadata() {
        let data = OffsetCommitPartitionData {
            partition_index: 0,
            committed_offset: 0,
            metadata: None,
        };
        assert!(data.metadata.is_none());
    }

    // ========== OffsetFetchTopicData Tests ==========

    #[test]
    fn test_offset_fetch_topic_data() {
        let data = OffsetFetchTopicData {
            name: "fetch-offsets-topic".to_string(),
            partition_indexes: vec![0, 1, 2, 3],
        };
        assert_eq!(data.name, "fetch-offsets-topic");
        assert_eq!(data.partition_indexes, vec![0, 1, 2, 3]);
    }

    // ========== JoinGroupProtocol Tests ==========

    #[test]
    fn test_join_group_protocol() {
        let proto = JoinGroupProtocol {
            name: "range".to_string(),
            metadata: vec![0, 1, 2, 3],
        };
        assert_eq!(proto.name, "range");
        assert_eq!(proto.metadata.len(), 4);
    }

    // ========== SyncGroupAssignment Tests ==========

    #[test]
    fn test_sync_group_assignment() {
        let assign = SyncGroupAssignment {
            member_id: "member-123".to_string(),
            assignment: vec![10, 20, 30],
        };
        assert_eq!(assign.member_id, "member-123");
        assert_eq!(assign.assignment, vec![10, 20, 30]);
    }

    // ========== MemberIdentity Tests ==========

    #[test]
    fn test_member_identity_with_instance_id() {
        let id = MemberIdentity {
            member_id: "m1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
        };
        assert_eq!(id.member_id, "m1");
        assert_eq!(id.group_instance_id, Some("instance-1".to_string()));
    }

    #[test]
    fn test_member_identity_without_instance_id() {
        let id = MemberIdentity {
            member_id: "m2".to_string(),
            group_instance_id: None,
        };
        assert_eq!(id.member_id, "m2");
        assert!(id.group_instance_id.is_none());
    }

    // ========== ListOffsetsTopicData Tests ==========

    #[test]
    fn test_list_offsets_topic_data() {
        let data = ListOffsetsTopicData {
            name: "offsets-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: -2, // Earliest
            }],
        };
        assert_eq!(data.name, "offsets-topic");
        assert_eq!(data.partitions.len(), 1);
    }

    // ========== ListOffsetsPartitionData Tests ==========

    #[test]
    fn test_list_offsets_partition_data_earliest() {
        let data = ListOffsetsPartitionData {
            partition_index: 0,
            current_leader_epoch: -1,
            timestamp: -2, // Earliest offset
        };
        assert_eq!(data.timestamp, -2);
    }

    #[test]
    fn test_list_offsets_partition_data_latest() {
        let data = ListOffsetsPartitionData {
            partition_index: 0,
            current_leader_epoch: -1,
            timestamp: -1, // Latest offset
        };
        assert_eq!(data.timestamp, -1);
    }

    #[test]
    fn test_list_offsets_partition_data_at_timestamp() {
        let data = ListOffsetsPartitionData {
            partition_index: 1,
            current_leader_epoch: 5,
            timestamp: 1_704_067_200_000, // 2024-01-01 00:00:00 UTC
        };
        assert_eq!(data.timestamp, 1_704_067_200_000);
        assert_eq!(data.current_leader_epoch, 5);
    }

    // ========== CreateTopicRequest Tests ==========

    #[test]
    fn test_create_topic_request() {
        let req = CreateTopicRequest {
            name: "new-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        };
        assert_eq!(req.name, "new-topic");
        assert_eq!(req.num_partitions, 3);
        assert_eq!(req.replication_factor, 1);
    }

    #[test]
    fn test_create_topic_request_broker_defaults() {
        let req = CreateTopicRequest {
            name: "default-topic".to_string(),
            num_partitions: -1,     // Broker default
            replication_factor: -1, // Broker default
        };
        assert_eq!(req.num_partitions, -1);
        assert_eq!(req.replication_factor, -1);
    }

    // ========== CreatePartitionsTopicRequest Tests ==========

    #[test]
    fn test_create_partitions_topic_request() {
        let req = CreatePartitionsTopicRequest {
            name: "expand-topic".to_string(),
            count: 10,
        };
        assert_eq!(req.name, "expand-topic");
        assert_eq!(req.count, 10);
    }
}
