//! OffsetCommit (8) and OffsetFetch (9).

use kafka_protocol::messages::offset_commit_request::OffsetCommitRequest;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequest;
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::TopicName;

use super::{ApiEnv, Call, KafkaApi, Rejection, RequestHeader, TopicPartitions};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;
use crate::kafka::messages::{
    OffsetCommitPartitionData, OffsetCommitTopicData, OffsetFetchTopicData,
};

/// OffsetCommit: store a group's consumed offsets.
pub struct OffsetCommitApi;

/// OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestData {
    /// Consumer group id.
    pub group_id: String,
    /// Generation the member believes it holds (-1 for a simple consumer). CONF-1:
    /// validated against the coordinator to reject zombie commits.
    pub generation_id: i32,
    /// Member id ("" for a simple consumer).
    pub member_id: String,
    /// Offsets to commit.
    pub topics: Vec<OffsetCommitTopicData>,
}

impl KafkaApi for OffsetCommitApi {
    type Wire = OffsetCommitRequest;
    type Request = OffsetCommitRequestData;
    type Echo = TopicPartitions;
    const NAME: &'static str = "OffsetCommit";
    // The crate cannot decode OffsetCommit v0-1.
    const MIN_VERSION: i16 = 2;
    const MAX_VERSION: i16 = 8;

    fn decode(
        wire: OffsetCommitRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(OffsetCommitRequestData {
            group_id: wire.group_id.to_string(),
            generation_id: wire.generation_id_or_member_epoch,
            member_id: wire.member_id.to_string(),
            topics: wire
                .topics
                .into_iter()
                .map(|t| OffsetCommitTopicData {
                    name: t.name.to_string(),
                    partitions: t
                        .partitions
                        .into_iter()
                        .map(|p| OffsetCommitPartitionData {
                            partition_index: p.partition_index,
                            committed_offset: p.committed_offset,
                            metadata: p
                                .committed_metadata
                                .map(|s| s.to_string())
                                .filter(|s| !s.is_empty()),
                        })
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

    /// No top-level error code: report it on every requested partition.
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> OffsetCommitResponse {
        let mut response = OffsetCommitResponse::default();
        response.throttle_time_ms = 0;
        for (name, partitions) in echo.into_iter().flatten() {
            let mut topic = OffsetCommitResponseTopic::default();
            topic.name = TopicName(name.clone().into());
            topic.partitions = partitions
                .iter()
                .map(|&index| {
                    let mut p = OffsetCommitResponsePartition::default();
                    p.partition_index = index;
                    p.error_code = error_code;
                    p
                })
                .collect();
            response.topics.push(topic);
        }
        response
    }

    fn response_has_error(response: &OffsetCommitResponse) -> bool {
        response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0)
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_offset_commit(
                &env.ctx,
                r.group_id,
                r.generation_id,
                r.member_id,
                r.topics,
            )
        })
    }
}

/// OffsetFetch: read a group's committed offsets.
pub struct OffsetFetchApi;

/// OffsetFetch request.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestData {
    /// Consumer group id.
    pub group_id: String,
    /// Topics to read; `None` (v2+ null array) = every partition of the group. An empty
    /// array asks for nothing.
    pub topics: Option<Vec<OffsetFetchTopicData>>,
}

impl KafkaApi for OffsetFetchApi {
    type Wire = OffsetFetchRequest;
    type Request = OffsetFetchRequestData;
    type Echo = TopicPartitions;
    const NAME: &'static str = "OffsetFetch";
    // The crate cannot decode OffsetFetch v0.
    const MIN_VERSION: i16 = 1;
    // v8+ answers several groups per request, which is not implemented.
    const MAX_VERSION: i16 = 7;

    fn decode(
        wire: OffsetFetchRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(OffsetFetchRequestData {
            group_id: wire.group_id.to_string(),
            topics: wire.topics.map(|topics| {
                topics
                    .into_iter()
                    .map(|t| OffsetFetchTopicData {
                        name: t.name.to_string(),
                        partition_indexes: t.partition_indexes,
                    })
                    .collect()
            }),
        })
    }

    fn echo(request: &Self::Request) -> TopicPartitions {
        request
            .topics
            .iter()
            .flatten()
            .map(|t| (t.name.clone(), t.partition_indexes.clone()))
            .collect()
    }

    /// Top-level error code (v2+) plus the code on every requested partition (the only
    /// place v1 can carry it).
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> OffsetFetchResponse {
        let mut response = OffsetFetchResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = error_code;
        for (name, partitions) in echo.into_iter().flatten() {
            let mut topic = OffsetFetchResponseTopic::default();
            topic.name = TopicName(name.clone().into());
            topic.partitions = partitions
                .iter()
                .map(|&index| {
                    let mut p = OffsetFetchResponsePartition::default();
                    p.partition_index = index;
                    p.committed_offset = -1;
                    p.committed_leader_epoch = -1;
                    p.error_code = error_code;
                    p
                })
                .collect();
            response.topics.push(topic);
        }
        response
    }

    fn response_has_error(response: &OffsetFetchResponse) -> bool {
        response.error_code != 0
            || response
                .topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .any(|p| p.error_code != 0)
            || response.groups.iter().any(|g| {
                g.error_code != 0
                    || g.topics
                        .iter()
                        .flat_map(|t| t.partitions.iter())
                        .any(|p| p.error_code != 0)
            })
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_offset_fetch(&env.ctx, r.group_id, r.topics)
        })
    }
}
