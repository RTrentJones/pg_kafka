//! Produce (0).

use crate::kafka::constants::ERROR_MESSAGE_TOO_LARGE;
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
use crate::kafka::protocol::decode_budget::{
    run_large_decode, DecodeBudget, DecodedCharge, Exhausted, INLINE_DECODE_BYTES,
    MAX_DECODED_REQUEST_BYTES,
};
use crate::kafka::protocol::{
    least_decoded_memory, parse_record_batch_with_metadata, validate_produce_records,
    ParsedRecordBatch,
};
use crate::kafka::storage::{LogStore, TopicStore};

/// Produce: append record batches.
pub struct ProduceApi;

/// Produce request.
#[derive(Debug)]
pub struct ProduceRequestData {
    /// Acknowledgment level (0 = none, 1 = leader, -1 = all ISR).
    pub acks: i16,
    /// Acknowledgment timeout (milliseconds).
    pub timeout_ms: i32,
    /// Topic → partitions → records.
    pub topic_data: Vec<TopicProduceData>,
    /// Transactional id of a transactional producer.
    pub transactional_id: Option<String>,
    /// The decoded records' share of the process-wide decode budget, given back when the
    /// request is dropped after its handler ran.
    pub decoded: DecodedCharge,
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
        header: &RequestHeader,
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

        // Kafka accepts only acks -1, 0 and 1; anything else is
        // answered INVALID_REQUIRED_ACKS on every partition and nothing is written.
        if !valid_acks(acks) {
            warn!("Rejecting Produce with invalid acks={acks}");
            return Err(Rejection::Reply {
                error_code: ERROR_INVALID_REQUIRED_ACKS,
                echo: Some(requested),
            });
        }

        let zstd_allowed = header.api_version >= MIN_ZSTD_VERSION;
        let (topic_data, decoded) = match decode_all_records(&wire.topic_data, zstd_allowed) {
            Ok(decoded) => decoded,
            Err(failure) => {
                // acks=0 never gets a response: an unsolicited one would be read as the
                // reply to the client's next request. Kafka closes the connection instead,
                // so the client notices. A full process-wide budget (or a full queue of large
                // decodes) closes any connection, as a full frame budget does; the client
                // reconnects and retries.
                let error_code = match failure {
                    DecodeFailure::Busy => {
                        return Err(Rejection::Close(
                            "decoded-records budget exhausted or large decodes queued",
                        ))
                    }
                    _ if acks == 0 => {
                        return Err(Rejection::Close(
                            "acks=0 Produce with a corrupt or oversized record batch",
                        ))
                    }
                    DecodeFailure::TooLarge => ERROR_MESSAGE_TOO_LARGE,
                    DecodeFailure::Rejected(code) => code,
                };
                return Err(Rejection::Reply {
                    error_code,
                    echo: Some(requested),
                });
            }
        };

        Ok(ProduceRequestData {
            acks,
            timeout_ms: wire.timeout_ms,
            topic_data,
            transactional_id: wire.transactional_id.map(|s| s.to_string()),
            decoded,
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
            // Best effort, no response. Idempotency and transactions
            // need acks >= 1 to mean anything, so neither is applied here. A request-level
            // failure rolls back like the acks >= 1 path.
            //
            // As in Kafka, any error (a partition error, or the whole
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
        // SPI reads is answered like a handler panic instead of escaping unguarded.
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

/// The first Produce version that may carry zstd batches (KIP-110).
const MIN_ZSTD_VERSION: i16 = 7;

/// Why a Produce request's records did not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeFailure {
    /// They take more memory than one request may hold ([`MAX_DECODED_REQUEST_BYTES`]).
    TooLarge,
    /// The process-wide decode budget is exhausted, or too many large decodes wait.
    Busy,
    /// A batch is corrupt or invalid: this code on every partition.
    Rejected(i16),
}

/// Records of every partition, and their charge to the decode budget.
type DecodedTopics = (Vec<TopicProduceData>, DecodedCharge);

/// Decode the records of every partition of `topics` within `budget`.
fn decode_records(
    topics: &[kafka_protocol::messages::produce_request::TopicProduceData],
    budget: DecodeBudget,
) -> Result<DecodedTopics, DecodeFailure> {
    let mut topic_data = Vec::with_capacity(topics.len());
    for topic in topics {
        let name = topic.name.to_string();
        let mut partitions = Vec::with_capacity(topic.partition_data.len());
        for partition in &topic.partition_data {
            let (records, producer_metadata) = match &partition.records {
                Some(batch) => match parse_record_batch_with_metadata(batch, &budget) {
                    Ok(ParsedRecordBatch {
                        records,
                        producer_metadata,
                    }) => (records, Some(producer_metadata)),
                    Err(e) => {
                        return Err(match budget.exhausted() {
                            Some(Exhausted::Limit) => DecodeFailure::TooLarge,
                            Some(Exhausted::Global) => DecodeFailure::Busy,
                            None => {
                                warn!(
                                    "Failed to parse RecordBatch for topic={}, partition={}: {}",
                                    name, partition.index, e
                                );
                                DecodeFailure::Rejected(match e {
                                    KafkaError::InvalidRecord { .. } => e.to_kafka_error_code(),
                                    _ => ERROR_CORRUPT_MESSAGE,
                                })
                            }
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
    Ok((topic_data, budget.into_charge()))
}

/// What Kafka checks of every partition's data while it parses the request
/// ([`validate_produce_records`]), before any partition decodes: the first failure fails the
/// request.
fn validate_all_records(
    topics: &[kafka_protocol::messages::produce_request::TopicProduceData],
    zstd_allowed: bool,
) -> Result<(), DecodeFailure> {
    for topic in topics {
        for partition in &topic.partition_data {
            let Some(batch) = &partition.records else {
                continue;
            };
            if let Err(code) = validate_produce_records(batch, zstd_allowed) {
                warn!(
                    "Refusing the records of topic={}, partition={}: error {code}",
                    topic.name.0.as_str(),
                    partition.index
                );
                return Err(DecodeFailure::Rejected(code));
            }
        }
    }
    Ok(())
}

/// The memory the batches of `topics` take decoded by what they declare (their bytes and
/// record counts), the least they can take.
fn declared_memory(
    topics: &[kafka_protocol::messages::produce_request::TopicProduceData],
) -> usize {
    topics
        .iter()
        .flat_map(|t| t.partition_data.iter())
        .filter_map(|p| p.records.as_ref())
        .map(|batch| least_decoded_memory(batch))
        .fold(0, usize::saturating_add)
}

/// Decode the records of `topics`, keeping large decodes off the I/O threads.
///
/// A request whose batches take at most [`INLINE_DECODE_BYTES`] of memory by what they
/// declare ([`declared_memory`]) is decoded in place, within that budget: the common case of
/// small (compressed or not) batches. One that declares more, or that expands past it, is
/// decoded by [`run_large_decode`] within [`MAX_DECODED_REQUEST_BYTES`] (or turned away as
/// [`DecodeFailure::Busy`] when too many already wait for it).
fn decode_all_records(
    topics: &[kafka_protocol::messages::produce_request::TopicProduceData],
    zstd_allowed: bool,
) -> Result<DecodedTopics, DecodeFailure> {
    validate_all_records(topics, zstd_allowed)?;
    if declared_memory(topics) <= INLINE_DECODE_BYTES {
        match decode_records(topics, DecodeBudget::new(INLINE_DECODE_BYTES)) {
            Err(DecodeFailure::TooLarge) => {}
            done => return done,
        }
    }
    run_large_decode(|| decode_records(topics, DecodeBudget::new(MAX_DECODED_REQUEST_BYTES)))
        .unwrap_or(Err(DecodeFailure::Busy))
}

/// The `acks` values Kafka accepts (all ISR, none, leader).
pub(crate) fn valid_acks(acks: i16) -> bool {
    matches!(acks, -1..=1)
}

/// Whether an acks=0 Produce must close the connection (any partition error).
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
/// thread: the notify channel is bounded and `try_send` drops on full. Waiting
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::protocol::recordbatch_test_batches::{batch_with, record, v, Header};
    use crate::kafka::protocol::ERROR_UNSUPPORTED_COMPRESSION_TYPE;
    use kafka_protocol::messages::produce_request::{
        PartitionProduceData as WirePartition, TopicProduceData as WireTopic,
    };

    /// One topic of `batches`, one per partition.
    fn topics(batches: Vec<bytes::Bytes>) -> Vec<WireTopic> {
        let mut topic = WireTopic::default();
        topic.name = kafka_protocol::messages::TopicName("t".into());
        topic.partition_data = (0..)
            .zip(batches)
            .map(|(index, batch)| {
                let mut p = WirePartition::default();
                p.index = index;
                p.records = Some(batch);
                p
            })
            .collect();
        vec![topic]
    }

    /// A batch of `n` records of a 10-byte key and a `len`-byte value.
    fn batch_of(n: usize, len: usize) -> bytes::Bytes {
        let value = vec![b'v'; len];
        let section: Vec<u8> = (0..n)
            .flat_map(|i| {
                let i = i64::try_from(i).unwrap();
                let value_len = v(i64::try_from(len).unwrap());
                record(&[
                    &[0],
                    &v(0),
                    &v(i),
                    &v(10),
                    b"key-000000",
                    &value_len,
                    &value,
                    &v(0),
                ])
            })
            .collect();
        batch_with(&section, i32::try_from(n).unwrap(), Header::default())
    }

    #[test]
    fn zstd_is_refused_before_produce_v7_before_any_batch_decodes() {
        use crate::kafka::constants::ERROR_INVALID_RECORD;
        let x = record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"x", &v(0)]);
        let zstd = batch_with(
            &x,
            1,
            Header {
                attributes: 4,
                ..Header::default()
            },
        );
        let rejected = |data: Vec<bytes::Bytes>, zstd_allowed| match decode_all_records(
            &topics(data),
            zstd_allowed,
        ) {
            Err(DecodeFailure::Rejected(code)) => Some(code),
            _ => None,
        };
        // The batch is not zstd data at all: refused for its codec first.
        assert_eq!(
            rejected(vec![zstd.clone()], false),
            Some(ERROR_UNSUPPORTED_COMPRESSION_TYPE)
        );
        assert_eq!(
            rejected(vec![batch_of(1, 1), zstd.clone()], false),
            Some(ERROR_UNSUPPORTED_COMPRESSION_TYPE),
            "on any partition"
        );
        // From v7 on the codec is accepted and the batch fails to decompress.
        assert_eq!(
            rejected(vec![zstd.clone()], true),
            Some(ERROR_CORRUPT_MESSAGE)
        );
        // An earlier partition's framing error comes first, as in Kafka.
        assert_eq!(
            rejected(vec![bytes::Bytes::from_static(b"short"), zstd], false),
            Some(ERROR_INVALID_RECORD)
        );
    }

    #[test]
    fn requests_of_the_clients_default_size_decode_inline() {
        // Java's max.request.size and librdkafka's message.max.bytes default to 1 MiB: such
        // a request of 16 KB batches (the default batch.size) of 1 KB records, or of 200-byte
        // records, decodes on the I/O thread and never waits for a large-decode slot.
        for (records, len) in [(15, 1000), (70, 200)] {
            let batch = batch_of(records, len);
            let topics = topics(vec![batch.clone(); 1024 * 1024 / batch.len()]);
            let declared = declared_memory(&topics);
            assert!(
                declared <= INLINE_DECODE_BYTES,
                "{len}-byte records declare {declared}"
            );
            let budget = DecodeBudget::new(INLINE_DECODE_BYTES);
            assert!(
                decode_records(&topics, budget).is_ok(),
                "{len}-byte records"
            );
        }
    }
}
