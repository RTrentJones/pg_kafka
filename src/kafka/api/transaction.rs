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
use crate::kafka::constants::{
    ERROR_INVALID_PRODUCER_EPOCH, ERROR_INVALID_REQUEST, ERROR_PRODUCER_FENCED,
};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;
use crate::kafka::messages::TxnOffsetCommitTopics;
use crate::kafka::notifications::InternalNotification;
use crate::kafka::storage::TxnPartitions;

/// The code a fenced producer epoch is answered with at `version`. KIP-588 (Kafka 2.7)
/// answers it PRODUCER_FENCED from the version each transaction API took for it,
/// `fenced_since`; Kafka 3.7 converts it to INVALID_PRODUCER_EPOCH for the versions
/// before (`KafkaApis`), the code older clients know. Any other code is unchanged.
fn fenced_code(code: i16, version: i16, fenced_since: i16) -> i16 {
    if code == ERROR_PRODUCER_FENCED && version < fenced_since {
        ERROR_INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

/// The first InitProducerId version answered PRODUCER_FENCED.
const INIT_PRODUCER_ID_FENCED_SINCE: i16 = 4;
/// The first AddPartitionsToTxn, AddOffsetsToTxn and EndTxn version answered
/// PRODUCER_FENCED.
const TXN_FENCED_SINCE: i16 = 2;
/// TxnOffsetCommit is never answered PRODUCER_FENCED: Kafka 3.7's group coordinator
/// answers a fenced epoch INVALID_PRODUCER_EPOCH at every version.
const TXN_OFFSET_COMMIT_FENCED_SINCE: i16 = i16::MAX;

/// InitProducerId: allocate a producer id and epoch. An idempotent producer always gets
/// a new id; for a transactional id the producer id and epoch the request presents
/// (v3+) decide between a bump, a retry's answer and PRODUCER_FENCED.
pub struct InitProducerIdApi;

/// InitProducerId request.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequestData {
    /// Transactional id of a transactional producer.
    pub transactional_id: Option<String>,
    /// Transaction timeout (milliseconds).
    pub transaction_timeout_ms: i32,
    /// The producer id and epoch the request presents (None for -1/-1, and before v3).
    pub presented: Option<(i64, i16)>,
}

impl ApiSchema for InitProducerIdApi {
    type Wire = InitProducerIdRequest;
    type Echo = ();
    const NAME: &'static str = "InitProducerId";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 4;

    fn error_response(_echo: Option<&()>, version: i16, error_code: i16) -> InitProducerIdResponse {
        crate::kafka::response_builders::build_init_producer_id_error_response(fenced_code(
            error_code,
            version,
            INIT_PRODUCER_ID_FENCED_SINCE,
        ))
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
        // Kafka: both -1 is a new producer, an id without an epoch (or the reverse)
        // is INVALID_REQUEST.
        let presented = match (wire.producer_id.0, wire.producer_epoch) {
            (-1, -1) => None,
            (-1, _) | (_, -1) => {
                return Err(Rejection::Reply {
                    error_code: ERROR_INVALID_REQUEST,
                    echo: None,
                })
            }
            (producer_id, epoch) => Some((producer_id, epoch)),
        };
        Ok(InitProducerIdRequestData {
            transactional_id: wire.transactional_id.map(|s| s.to_string()),
            transaction_timeout_ms: wire.transaction_timeout_ms,
            presented,
        })
    }

    fn echo(_request: &Self::Request) {}

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, header| {
            handlers::init_producer(
                &env.ctx,
                r.transactional_id.as_deref(),
                r.transaction_timeout_ms,
                r.presented,
                header.client_id.as_deref(),
            )
            .map(|(mut response, aborted)| {
                // An open transaction it aborted moves the last stable offset of its
                // partitions; nothing else an InitProducerId does changes a Fetch.
                emit_txn_end_notification(env.notify_tx, aborted);
                response.error_code = fenced_code(
                    response.error_code,
                    header.api_version,
                    INIT_PRODUCER_ID_FENCED_SINCE,
                );
                response
            })
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
        version: i16,
        error_code: i16,
    ) -> AddPartitionsToTxnResponse {
        let error_code = fenced_code(error_code, version, TXN_FENCED_SINCE);
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
        call.run(request, |r, header| {
            handlers::handle_add_partitions_to_txn(
                &env.ctx,
                &r.transactional_id,
                r.producer_id,
                r.producer_epoch,
                &r.topics,
            )
            .map(|mut response| {
                // A refused transaction is answered on every partition.
                for p in response
                    .results_by_topic_v3_and_below
                    .iter_mut()
                    .flat_map(|t| t.results_by_partition.iter_mut())
                {
                    p.partition_error_code =
                        fenced_code(p.partition_error_code, header.api_version, TXN_FENCED_SINCE);
                }
                response
            })
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
        version: i16,
        error_code: i16,
    ) -> AddOffsetsToTxnResponse {
        let mut response = AddOffsetsToTxnResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = fenced_code(error_code, version, TXN_FENCED_SINCE);
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

    fn error_response(_echo: Option<&()>, version: i16, error_code: i16) -> EndTxnResponse {
        let mut response = EndTxnResponse::default();
        response.throttle_time_ms = 0;
        response.error_code = fenced_code(error_code, version, TXN_FENCED_SINCE);
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
            handlers::end_txn(
                &env.ctx,
                &r.transactional_id,
                r.producer_id,
                r.producer_epoch,
                r.committed,
            )
            // Wake the read_committed long polls of the transaction's partitions: commit
            // and abort move their last stable offset.
            .map(|(response, ended)| {
                emit_txn_end_notification(env.notify_tx, ended);
                response
            })
        })
    }
}

/// Wake the `read_committed` long polls of `partitions`, the partitions of a transaction
/// that ended (commit, abort, a fencing InitProducerId or its timeout); nothing when it
/// names none. Best effort (see [`notify_best_effort`]).
pub(crate) fn emit_txn_end_notification(
    notify_tx: &crossbeam_channel::Sender<InternalNotification>,
    partitions: TxnPartitions,
) {
    if !partitions.is_empty() {
        notify_best_effort(
            notify_tx,
            InternalNotification::TransactionEnded { partitions },
        );
    }
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
        version: i16,
        error_code: i16,
    ) -> TxnOffsetCommitResponse {
        let error_code = fenced_code(error_code, version, TXN_OFFSET_COMMIT_FENCED_SINCE);
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
        call.run(request, |r, header| {
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
            .map(|mut response| {
                // A refused transaction is answered on every partition that exists.
                for p in response
                    .topics
                    .iter_mut()
                    .flat_map(|t| t.partitions.iter_mut())
                {
                    p.error_code = fenced_code(
                        p.error_code,
                        header.api_version,
                        TXN_OFFSET_COMMIT_FENCED_SINCE,
                    );
                }
                response
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_presented(producer_id: i64, epoch: i16) -> Result<Option<(i64, i16)>, i16> {
        let mut wire = InitProducerIdRequest::default();
        wire.transaction_timeout_ms = 60_000;
        wire.producer_id = kafka_protocol::messages::ProducerId(producer_id);
        wire.producer_epoch = epoch;
        let header = RequestHeader {
            api_key: 22,
            api_version: 4,
            correlation_id: 1,
            client_id: None,
        };
        match InitProducerIdApi::decode(wire, &header) {
            Ok(r) => Ok(r.presented),
            Err(Rejection::Reply { error_code, .. }) => Err(error_code),
            Err(_) => Err(i16::MIN),
        }
    }

    #[test]
    fn a_fenced_epoch_is_invalid_producer_epoch_before_kip_588_versions() {
        use crate::kafka::constants::{ERROR_INVALID_PRODUCER_ID_MAPPING, ERROR_NONE};
        let fenced =
            |version| (0..4).map(move |since| fenced_code(ERROR_PRODUCER_FENCED, version, since));
        assert_eq!(
            fenced(2).collect::<Vec<_>>(),
            [
                ERROR_PRODUCER_FENCED,
                ERROR_PRODUCER_FENCED,
                ERROR_PRODUCER_FENCED,
                ERROR_INVALID_PRODUCER_EPOCH
            ]
        );
        for code in [
            ERROR_NONE,
            ERROR_INVALID_PRODUCER_ID_MAPPING,
            ERROR_INVALID_PRODUCER_EPOCH,
        ] {
            assert_eq!(fenced_code(code, 0, 2), code);
        }
        let answers = |version| {
            let end = EndTxnApi::error_response(None, version, ERROR_PRODUCER_FENCED).error_code;
            let offsets =
                AddOffsetsToTxnApi::error_response(None, version, ERROR_PRODUCER_FENCED).error_code;
            let echo: TopicPartitions = vec![("t".to_string(), vec![0])];
            let partitions =
                AddPartitionsToTxnApi::error_response(Some(&echo), version, ERROR_PRODUCER_FENCED);
            let init =
                InitProducerIdApi::error_response(None, version, ERROR_PRODUCER_FENCED).error_code;
            let commit_echo: TxnOffsetCommitTopics = vec![("t".to_string(), vec![(0, 1, None)])];
            let commit = TxnOffsetCommitApi::error_response(
                Some(&commit_echo),
                version,
                ERROR_PRODUCER_FENCED,
            );
            (
                end,
                offsets,
                partitions.results_by_topic_v3_and_below[0].results_by_partition[0]
                    .partition_error_code,
                init,
                commit.topics[0].partitions[0].error_code,
            )
        };
        let (old, new) = (ERROR_INVALID_PRODUCER_EPOCH, ERROR_PRODUCER_FENCED);
        assert_eq!(answers(1), (old, old, old, old, old));
        assert_eq!(answers(2), (new, new, new, old, old));
        assert_eq!(answers(3), (new, new, new, old, old));
        assert_eq!(answers(4), (new, new, new, new, old));
    }

    #[test]
    fn init_producer_id_presented_id_and_epoch() {
        assert_eq!(decode_presented(-1, -1), Ok(None));
        assert_eq!(decode_presented(5, 2), Ok(Some((5, 2))));
        assert_eq!(decode_presented(0, 0), Ok(Some((0, 0))));
        assert_eq!(decode_presented(-1, 3), Err(ERROR_INVALID_REQUEST));
        assert_eq!(decode_presented(5, -1), Err(ERROR_INVALID_REQUEST));
    }
}
