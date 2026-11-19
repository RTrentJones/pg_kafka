//! Produce (0).

use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use tracing::warn;

use super::{ApiEnv, ApiSchema, Call, KafkaApi, Rejection, RequestHeader, TopicPartitions};
use crate::kafka::constants::{ERROR_CORRUPT_MESSAGE, ERROR_INVALID_REQUIRED_ACKS, ERROR_NONE};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers;
use crate::kafka::messages::{PartitionProduceData, ProducerMetadata, TopicProduceData};
use crate::kafka::notifications::InternalNotification;
use crate::kafka::protocol::{parse_record_batch_with_metadata, ParsedRecordBatch};
use crate::kafka::storage::{LogStore, TopicStore};

/// Produce: append record batches.
pub struct ProduceApi;

/// Produce request.
#[derive(Debug, Clone)]
pub struct ProduceRequestData {
    /// Acknowledgment level (0 = none, 1 = leader, -1 = all ISR).
    pub acks: i16,
    /// Acknowledgment timeout (milliseconds).
    pub timeout_ms: i32,
    /// Topic → partitions → records.
    pub topic_data: Vec<TopicProduceData>,
    /// Transactional id of a transactional producer (Phase 10).
    pub transactional_id: Option<String>,
}

impl ApiSchema for ProduceApi {
    type Wire = ProduceRequest;
    type Echo = TopicPartitions;
    const NAME: &'static str = "Produce";

    // RecordBatch format only (v3+); v0-2 use the legacy MessageSet the crate cannot decode.
    const MIN_VERSION: i16 = 3;
    const MAX_VERSION: i16 = 9;

    /// ProduceResponse has no top-level error code: report it on every requested partition.
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> ProduceResponse {
        let mut response = crate::kafka::response_builders::build_produce_response();
        for (name, partitions) in echo.into_iter().flatten() {
            let mut topic = TopicProduceResponse::default();
            topic.name = kafka_protocol::messages::TopicName(name.clone().into());
            for &index in partitions {
                let mut p = PartitionProduceResponse::default();
                p.index = index;
                p.error_code = error_code;
                p.base_offset = -1;
                p.log_append_time_ms = -1;
                p.log_start_offset = -1;
                topic.partition_responses.push(p);
            }
            response.responses.push(topic);
        }
        response
    }

    fn response_has_error(response: &ProduceResponse) -> bool {
        response
            .responses
            .iter()
            .flat_map(|t| t.partition_responses.iter())
            .any(|p| p.error_code != 0)
    }
}

impl KafkaApi for ProduceApi {
    type Request = ProduceRequestData;

    fn decode(
        wire: ProduceRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        let acks = wire.acks;
        let requested: TopicPartitions = wire
            .topic_data
            .iter()
            .map(|t| {
                (
                    t.name.to_string(),
                    t.partition_data.iter().map(|p| p.index).collect(),
                )
            })
            .collect();

        // wave3 net (item 5): Kafka accepts only acks -1, 0 and 1; anything else is
        // answered INVALID_REQUIRED_ACKS on every partition and nothing is written.
        if !valid_acks(acks) {
            warn!("Rejecting Produce with invalid acks={}", acks);
            return Err(Rejection::Reply {
                error_code: ERROR_INVALID_REQUIRED_ACKS,
                echo: Some(requested),
            });
        }

        let mut topic_data = Vec::with_capacity(wire.topic_data.len());
        for topic in wire.topic_data {
            let name = topic.name.to_string();
            let mut partitions = Vec::with_capacity(topic.partition_data.len());
            for partition in topic.partition_data {
                let (records, producer_metadata) = match &partition.records {
                    Some(batch) => match parse_record_batch_with_metadata(batch) {
                        Ok(ParsedRecordBatch {
                            records,
                            producer_metadata,
                        }) => (records, Some(producer_metadata)),
                        Err(e) => {
                            warn!(
                                "Failed to parse RecordBatch for topic={}, partition={}: {}",
                                name, partition.index, e
                            );
                            // acks=0 never gets a response: an unsolicited one would be read
                            // as the reply to the client's next request. Kafka closes the
                            // connection instead (wave3 net item 4), so the client notices.
                            if acks == 0 {
                                return Err(Rejection::Close(
                                    "acks=0 Produce with a corrupt record batch",
                                ));
                            }
                            let error_code = match e {
                                KafkaError::InvalidRecord { .. } => e.to_kafka_error_code(),
                                _ => ERROR_CORRUPT_MESSAGE,
                            };
                            return Err(Rejection::Reply {
                                error_code,
                                echo: Some(requested),
                            });
                        }
                    },
                    None => (Vec::new(), None),
                };
                partitions.push(PartitionProduceData {
                    partition_index: partition.index,
                    records,
                    producer_metadata,
                });
            }
            topic_data.push(TopicProduceData { name, partitions });
        }

        Ok(ProduceRequestData {
            acks,
            timeout_ms: wire.timeout_ms,
            topic_data,
            transactional_id: wire.transactional_id.map(|s| s.to_string()),
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

    /// The broker sends NO response frame at all for acks=0 (a reply desyncs clients
    /// that pair requests and responses strictly, like the Java client).
    fn expects_response(request: &Self::Request) -> bool {
        request.acks != 0
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        let store = env.ctx.store;
        if request.acks == 0 {
            // Best effort, no response. Idempotency (Phase 9) and transactions (Phase 10)
            // need acks >= 1 to mean anything, so neither is applied here. A request-level
            // failure rolls back like the acks >= 1 path.
            //
            // wave3 net (item 4): as in Kafka, any error (a partition error, or the whole
            // request failing) closes the connection, so the client refreshes its metadata
            // instead of producing into the void. The partitions that succeeded still
            // commit: the close instruction travels through the response channel, so the
            // worker releases it only after the batch committed (RB-1).
            let (_header, response_tx) = call.into_parts();
            let close = || {
                let _ = response_tx.send(crate::kafka::messages::KafkaResponse::new(
                    -1,
                    -1,
                    super::ResponseBody::CloseConnection,
                ));
            };
            return match handlers::handle_produce(&env.ctx, request.topic_data, None, None) {
                Ok(response) => {
                    emit_produce_notifications(&response, store, env.notify_tx);
                    if acks0_must_close(&response) {
                        close();
                    }
                    RequestOutcome::Commit
                }
                Err(e) => {
                    if e.is_server_error() {
                        crate::pg_warning!("acks=0 produce error (data may be lost): {}", e);
                    }
                    close();
                    RequestOutcome::Rollback
                }
            };
        }

        // The long-poll wake-up runs inside the guarded handler, so a Postgres ERROR in its
        // SPI reads is answered like a handler panic instead of escaping unguarded (RV-3).
        call.run(request, |r, _| {
            let producer_metadata = extract_producer_metadata(&r.topic_data);
            let response = handlers::handle_produce(
                &env.ctx,
                r.topic_data,
                producer_metadata.as_ref(),
                r.transactional_id.as_deref(),
            )?;
            emit_produce_notifications(&response, store, env.notify_tx);
            Ok(response)
        })
    }
}

/// wave3 net: the `acks` values Kafka accepts (all ISR, none, leader).
pub(crate) fn valid_acks(acks: i16) -> bool {
    matches!(acks, -1..=1)
}

/// wave3 net: whether an acks=0 Produce must close the connection (any partition error).
pub(crate) fn acks0_must_close(response: &ProduceResponse) -> bool {
    ProduceApi::response_has_error(response)
}

/// The producer identity of a request: the metadata of the first batch written by an
/// idempotent producer (producer_id >= 0), or `None` for a non-idempotent producer. Its
/// `base_sequence` belongs to that first batch only, so `handle_produce` checks each
/// partition against its own batch.
pub(crate) fn extract_producer_metadata(
    topic_data: &[TopicProduceData],
) -> Option<ProducerMetadata> {
    topic_data
        .iter()
        .flat_map(|t| t.partitions.iter())
        .filter_map(|p| p.producer_metadata.as_ref())
        .find(|m| m.producer_id >= 0)
        .cloned()
}

/// Send a best-effort notification to the network thread without blocking the single DB
/// thread (SEC-4): the notify channel is bounded and `try_send` drops on full. Waiting
/// fetches fall back to their poll interval, so a dropped wake-up only adds latency.
pub(crate) fn notify_best_effort(
    tx: &crossbeam_channel::Sender<InternalNotification>,
    notification: InternalNotification,
) {
    let _ = tx.try_send(notification);
}

/// Wake long-poll consumers for every partition a produce appended to. A missing topic id,
/// a failed high-watermark read or a full notify channel skips that wake-up.
fn emit_produce_notifications<S: TopicStore + LogStore + ?Sized>(
    response: &ProduceResponse,
    store: &S,
    notify_tx: &crossbeam_channel::Sender<InternalNotification>,
) {
    for topic_response in &response.responses {
        let topic_name = topic_response.name.0.as_str();
        let Ok(Some(topic_id)) = store.get_topic_id(topic_name) else {
            continue;
        };
        for partition in &topic_response.partition_responses {
            if partition.error_code != ERROR_NONE {
                continue;
            }
            if let Ok(high_watermark) = store.get_high_watermark(topic_id, partition.index) {
                notify_best_effort(
                    notify_tx,
                    InternalNotification::NewMessages {
                        topic_name: topic_name.to_string(),
                        topic_id,
                        partition_id: partition.index,
                        high_watermark,
                    },
                );
            }
        }
    }
}
