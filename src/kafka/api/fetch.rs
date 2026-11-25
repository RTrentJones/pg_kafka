//! Fetch (1) and ListOffsets (2).

use kafka_protocol::messages::fetch_request::FetchRequest;
use kafka_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::TopicName;

use super::{ApiEnv, ApiSchema, Call, KafkaApi, Rejection, RequestHeader, TopicPartitions};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;
use crate::kafka::messages::{
    ListOffsetsPartitionData, ListOffsetsTopicData, PartitionFetchData, TopicFetchData,
};
use crate::kafka::storage::IsolationLevel;

/// Fetch: read record batches.
pub struct FetchApi;

/// Fetch request.
#[derive(Debug, Clone)]
pub struct FetchRequestData {
    /// Long-poll budget (milliseconds); 0 = answer immediately.
    pub max_wait_ms: i32,
    /// Bytes to wait for before answering.
    pub min_bytes: i32,
    /// Response size budget across all partitions.
    pub max_bytes: i32,
    /// 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED.
    pub isolation_level: i8,
    /// Topic → partition fetch positions.
    pub topic_data: Vec<TopicFetchData>,
    /// Whether the first record of the first partition with data is returned even when it
    /// is larger than the limits, as Kafka returns the first record batch (KIP-74). The
    /// listener clears it for a Fetch read while its connection held other responses or
    /// waited for one, so what its response holds stays within the `max_bytes` it reserved
    /// of the connection's response budget (`listener::response_budget`).
    pub whole_first_record: bool,
}

impl ApiSchema for FetchApi {
    type Wire = FetchRequest;
    type Echo = TopicPartitions;
    const NAME: &'static str = "Fetch";

    // The crate cannot decode Fetch v0-3.
    const MIN_VERSION: i16 = 4;

    // v13 (KIP-516) drops the topic name for a topic id; pg_kafka resolves topics by name.
    const MAX_VERSION: i16 = 11;

    /// Top-level error code (v7+) plus the code on every requested partition (the only
    /// place v4-6 can carry it).
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> FetchResponse {
        let mut response = FetchResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = error_code;
        for (name, partitions) in echo.into_iter().flatten() {
            let mut topic = FetchableTopicResponse::default();
            topic.topic = TopicName(name.clone().into());
            topic.partitions = partitions
                .iter()
                .map(|&index| {
                    let mut p = PartitionData::default();
                    p.partition_index = index;
                    p.error_code = error_code;
                    p.high_watermark = -1;
                    p.last_stable_offset = -1;
                    p.log_start_offset = -1;
                    p
                })
                .collect();
            response.responses.push(topic);
        }
        response
    }

    fn response_has_error(response: &FetchResponse) -> bool {
        response.error_code != 0
            || response
                .responses
                .iter()
                .flat_map(|t| t.partitions.iter())
                .any(|p| p.error_code != 0)
    }
}

impl KafkaApi for FetchApi {
    type Request = FetchRequestData;

    fn decode(
        wire: FetchRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(FetchRequestData {
            max_wait_ms: wire.max_wait_ms,
            min_bytes: wire.min_bytes,
            max_bytes: wire.max_bytes,
            isolation_level: wire.isolation_level,
            topic_data: wire
                .topics
                .into_iter()
                .map(|t| TopicFetchData {
                    name: t.topic.to_string(),
                    partitions: t
                        .partitions
                        .into_iter()
                        .map(|p| PartitionFetchData {
                            partition_index: p.partition,
                            fetch_offset: p.fetch_offset,
                            partition_max_bytes: p.partition_max_bytes,
                        })
                        .collect(),
                })
                .collect(),
            whole_first_record: true,
        })
    }

    fn echo(request: &Self::Request) -> TopicPartitions {
        request
            .topic_data
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.partitions.iter().map(|p| p.partition_index).collect(),
                )
            })
            .collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_fetch_with_max_bytes(
                &env.ctx,
                r.topic_data,
                IsolationLevel::from_i8(r.isolation_level),
                r.max_bytes,
                r.whole_first_record,
            )
        })
    }
}

/// ListOffsets: earliest/latest/by-timestamp offsets.
pub struct ListOffsetsApi;

/// ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestData {
    /// -1 for a consumer, a broker id for a replica.
    pub replica_id: i32,
    /// 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED (v2+).
    pub isolation_level: i8,
    /// Topics and partitions to look up.
    pub topics: Vec<ListOffsetsTopicData>,
}

impl ApiSchema for ListOffsetsApi {
    type Wire = ListOffsetsRequest;
    type Echo = TopicPartitions;
    const NAME: &'static str = "ListOffsets";

    // The crate cannot decode ListOffsets v0.
    const MIN_VERSION: i16 = 1;
    const MAX_VERSION: i16 = 7;

    /// No top-level error code: report it on every requested partition.
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> ListOffsetsResponse {
        let mut response = ListOffsetsResponse::default();
        response.throttle_time_ms = 0;
        for (name, partitions) in echo.into_iter().flatten() {
            let mut topic = ListOffsetsTopicResponse::default();
            topic.name = TopicName(name.clone().into());
            topic.partitions = partitions
                .iter()
                .map(|&index| {
                    let mut p = ListOffsetsPartitionResponse::default();
                    p.partition_index = index;
                    p.error_code = error_code;
                    p.timestamp = -1;
                    p.offset = -1;
                    p
                })
                .collect();
            response.topics.push(topic);
        }
        response
    }

    fn response_has_error(response: &ListOffsetsResponse) -> bool {
        response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0)
    }
}

impl KafkaApi for ListOffsetsApi {
    type Request = ListOffsetsRequestData;

    fn decode(
        wire: ListOffsetsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(ListOffsetsRequestData {
            replica_id: wire.replica_id.0,
            isolation_level: wire.isolation_level,
            topics: wire
                .topics
                .into_iter()
                .map(|t| ListOffsetsTopicData {
                    name: t.name.to_string(),
                    partitions: t
                        .partitions
                        .into_iter()
                        .map(ListOffsetsPartitionData::from)
                        .collect(),
                })
                .collect(),
        })
    }

    fn echo(request: &Self::Request) -> TopicPartitions {
        request
            .topics
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.partitions.iter().map(|p| p.partition_index).collect(),
                )
            })
            .collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_list_offsets(
                &env.ctx,
                r.topics,
                IsolationLevel::from_i8(r.isolation_level),
            )
        })
    }
}
