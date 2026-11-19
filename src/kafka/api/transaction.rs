//! Idempotent and transactional producer APIs: InitProducerId (22), AddPartitionsToTxn (24),
//! AddOffsetsToTxn (25), EndTxn (26), TxnOffsetCommit (28).

use kafka_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafka_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponse, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::end_txn_request::EndTxnRequest;
use kafka_protocol::messages::end_txn_response::EndTxnResponse;
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequest;
use kafka_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
use kafka_protocol::messages::TopicName;

use super::produce::notify_best_effort;
use super::{ApiEnv, ApiSchema, Call, KafkaApi, Rejection, RequestHeader, TopicPartitions};
use crate::kafka::constants::ERROR_INVALID_REQUEST;
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;
use crate::kafka::messages::TxnOffsetCommitTopics;
use crate::kafka::notifications::InternalNotification;

/// InitProducerId: allocate (or re-init) a producer id and epoch.
pub struct InitProducerIdApi;

/// InitProducerId request.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequestData {
    /// Transactional id of a transactional producer.
    pub transactional_id: Option<String>,
    /// Transaction timeout (milliseconds).
    pub transaction_timeout_ms: i32,
    /// Existing producer id when reconnecting (-1 for a new producer).
    pub producer_id: i64,
    /// Existing producer epoch when reconnecting (-1 for a new producer).
    pub producer_epoch: i16,
}

impl ApiSchema for InitProducerIdApi {
    type Wire = InitProducerIdRequest;
    type Echo = ();
    const NAME: &'static str = "InitProducerId";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 4;

    fn error_response(
        _echo: Option<&()>,
        _version: i16,
        error_code: i16,
    ) -> InitProducerIdResponse {
        crate::kafka::response_builders::build_init_producer_id_error_response(error_code)
    }

    fn response_has_error(response: &InitProducerIdResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for InitProducerIdApi {
    type Request = InitProducerIdRequestData;

    fn decode(
        wire: InitProducerIdRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(InitProducerIdRequestData {
            transactional_id: wire.transactional_id.map(|s| s.to_string()),
            transaction_timeout_ms: wire.transaction_timeout_ms,
            producer_id: wire.producer_id.0,
            producer_epoch: wire.producer_epoch,
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, header| {
            handlers::handle_init_producer_id(
                &env.ctx,
                r.transactional_id.as_deref(),
                r.transaction_timeout_ms,
                header.client_id.as_deref(),
            )
        })
    }
}

/// AddPartitionsToTxn: register partitions in an ongoing transaction.
pub struct AddPartitionsToTxnApi;

/// AddPartitionsToTxn request (one transaction; v4+ batches are not served).
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequestData {
    /// Transactional id.
    pub transactional_id: String,
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// `(topic, [partition])` to add.
    pub topics: TopicPartitions,
}

impl ApiSchema for AddPartitionsToTxnApi {
    type Wire = AddPartitionsToTxnRequest;
    type Echo = TopicPartitions;
    const NAME: &'static str = "AddPartitionsToTxn";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 3;

    /// Top-level error code (v4+) plus the code on every requested partition (v0-3).
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> AddPartitionsToTxnResponse {
        let mut response = AddPartitionsToTxnResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = error_code;
        response.results_by_topic_v3_and_below = echo
            .into_iter()
            .flatten()
            .map(|(name, partitions)| {
                let mut t = AddPartitionsToTxnTopicResult::default();
                t.name = TopicName(name.clone().into());
                t.results_by_partition = partitions
                    .iter()
                    .map(|&index| {
                        let mut p = AddPartitionsToTxnPartitionResult::default();
                        p.partition_index = index;
                        p.partition_error_code = error_code;
                        p
                    })
                    .collect();
                t
            })
            .collect();
        response
    }

    fn response_has_error(response: &AddPartitionsToTxnResponse) -> bool {
        let topic_error = |t: &AddPartitionsToTxnTopicResult| {
            t.results_by_partition
                .iter()
                .any(|p| p.partition_error_code != 0)
        };
        response.error_code != 0
            || response
                .results_by_topic_v3_and_below
                .iter()
                .any(topic_error)
            || response
                .results_by_transaction
                .iter()
                .flat_map(|r| r.topic_results.iter())
                .any(topic_error)
    }
}

impl KafkaApi for AddPartitionsToTxnApi {
    type Request = AddPartitionsToTxnRequestData;

    fn decode(
        wire: AddPartitionsToTxnRequest,
        header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        let topics = |topics: &[kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic]| {
            topics
                .iter()
                .map(|t| (t.name.to_string(), t.partitions.clone()))
                .collect::<TopicPartitions>()
        };
        if header.api_version >= 4 {
            // v4+ batches transactions; only a single one is supported.
            let Some(first) = wire.transactions.first() else {
                return Err(Rejection::Reply {
                    error_code: ERROR_INVALID_REQUEST,
                    echo: None,
                });
            };
            Ok(AddPartitionsToTxnRequestData {
                transactional_id: first.transactional_id.to_string(),
                producer_id: first.producer_id.0,
                producer_epoch: first.producer_epoch,
                topics: topics(&first.topics),
            })
        } else {
            Ok(AddPartitionsToTxnRequestData {
                transactional_id: wire.v3_and_below_transactional_id.to_string(),
                producer_id: wire.v3_and_below_producer_id.0,
                producer_epoch: wire.v3_and_below_producer_epoch,
                topics: topics(&wire.v3_and_below_topics),
            })
        }
    }

    fn echo(request: &Self::Request) -> TopicPartitions {
        request.topics.clone()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_add_partitions_to_txn(
                &env.ctx,
                &r.transactional_id,
                r.producer_id,
                r.producer_epoch,
                &r.topics,
            )
        })
    }
}

/// AddOffsetsToTxn: add a group's offsets to a transaction.
pub struct AddOffsetsToTxnApi;

/// AddOffsetsToTxn request.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequestData {
    /// Transactional id.
    pub transactional_id: String,
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Consumer group id.
    pub group_id: String,
}

impl ApiSchema for AddOffsetsToTxnApi {
    type Wire = AddOffsetsToTxnRequest;
    type Echo = ();
    const NAME: &'static str = "AddOffsetsToTxn";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 3;

    fn error_response(
        _echo: Option<&()>,
        _version: i16,
        error_code: i16,
    ) -> AddOffsetsToTxnResponse {
        let mut response = AddOffsetsToTxnResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = error_code;
        response
    }

    fn response_has_error(response: &AddOffsetsToTxnResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for AddOffsetsToTxnApi {
    type Request = AddOffsetsToTxnRequestData;

    fn decode(
        wire: AddOffsetsToTxnRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(AddOffsetsToTxnRequestData {
            transactional_id: wire.transactional_id.to_string(),
            producer_id: wire.producer_id.0,
            producer_epoch: wire.producer_epoch,
            group_id: wire.group_id.to_string(),
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_add_offsets_to_txn(
                &env.ctx,
                &r.transactional_id,
                r.producer_id,
                r.producer_epoch,
                &r.group_id,
            )
        })
    }
}

/// EndTxn: commit or abort a transaction.
pub struct EndTxnApi;

/// EndTxn request.
#[derive(Debug, Clone)]
pub struct EndTxnRequestData {
    /// Transactional id.
    pub transactional_id: String,
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// true = commit, false = abort.
    pub committed: bool,
}

impl ApiSchema for EndTxnApi {
    type Wire = EndTxnRequest;
    type Echo = ();
    const NAME: &'static str = "EndTxn";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 3;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> EndTxnResponse {
        let mut response = EndTxnResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = error_code;
        response
    }

    fn response_has_error(response: &EndTxnResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for EndTxnApi {
    type Request = EndTxnRequestData;

    fn decode(
        wire: EndTxnRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(EndTxnRequestData {
            transactional_id: wire.transactional_id.to_string(),
            producer_id: wire.producer_id.0,
            producer_epoch: wire.producer_epoch,
            committed: wire.committed,
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_end_txn(
                &env.ctx,
                &r.transactional_id,
                r.producer_id,
                r.producer_epoch,
                r.committed,
            )
            // Wake read_committed long polls: commit/abort moves the last stable offset.
            .inspect(|_| emit_txn_end_notification(env.notify_tx))
        })
    }
}

/// Wake `read_committed` long-poll consumers after a successful EndTxn (commit or abort).
/// Best effort: a dropped wake-up only delays delivery to the fallback poll.
pub(crate) fn emit_txn_end_notification(
    notify_tx: &crossbeam_channel::Sender<InternalNotification>,
) {
    notify_best_effort(notify_tx, InternalNotification::TransactionEnded);
}

/// TxnOffsetCommit: commit offsets as part of a transaction.
pub struct TxnOffsetCommitApi;

/// TxnOffsetCommit request.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequestData {
    /// Transactional id.
    pub transactional_id: String,
    /// Consumer group id.
    pub group_id: String,
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Group generation (KIP-447, v3+); -1 for a simple consumer / pre-v3 client, which is
    /// exempt from group fencing.
    pub generation_id: i32,
    /// Group member id ("" for a simple consumer / pre-v3 client).
    pub member_id: String,
    /// `(topic, [(partition, offset, metadata)])`.
    pub topics: TxnOffsetCommitTopics,
}

impl ApiSchema for TxnOffsetCommitApi {
    type Wire = TxnOffsetCommitRequest;

    /// The requested topics and partitions (offsets are not needed for the error).
    type Echo = TxnOffsetCommitTopics;
    const NAME: &'static str = "TxnOffsetCommit";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 3;

    /// No top-level error code: report it on every requested partition (an empty response
    /// would read as success).
    fn error_response(
        echo: Option<&TxnOffsetCommitTopics>,
        _version: i16,
        error_code: i16,
    ) -> TxnOffsetCommitResponse {
        match echo {
            Some(topics) => handlers::txn_offset_commit_error_response(topics, error_code),
            None => TxnOffsetCommitResponse::default(),
        }
    }

    fn response_has_error(response: &TxnOffsetCommitResponse) -> bool {
        response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0)
    }
}

impl KafkaApi for TxnOffsetCommitApi {
    type Request = TxnOffsetCommitRequestData;

    fn decode(
        wire: TxnOffsetCommitRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(TxnOffsetCommitRequestData {
            transactional_id: wire.transactional_id.to_string(),
            group_id: wire.group_id.to_string(),
            producer_id: wire.producer_id.0,
            producer_epoch: wire.producer_epoch,
            generation_id: wire.generation_id,
            member_id: wire.member_id.to_string(),
            topics: wire
                .topics
                .iter()
                .map(|t| {
                    (
                        t.name.to_string(),
                        t.partitions
                            .iter()
                            .map(|p| {
                                (
                                    p.partition_index,
                                    p.committed_offset,
                                    p.committed_metadata.as_ref().map(ToString::to_string),
                                )
                            })
                            .collect(),
                    )
                })
                .collect(),
        })
    }

    fn echo(request: &Self::Request) -> TxnOffsetCommitTopics {
        request
            .topics
            .iter()
            .map(|(name, partitions)| {
                (
                    name.clone(),
                    partitions.iter().map(|(p, o, _)| (*p, *o, None)).collect(),
                )
            })
            .collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_txn_offset_commit(
                &env.ctx,
                &r.transactional_id,
                r.producer_id,
                r.producer_epoch,
                &r.group_id,
                r.generation_id,
                &r.member_id,
                r.topics,
            )
        })
    }
}
