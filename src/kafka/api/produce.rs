//! Produce (0).

use std::sync::Arc;

use crate::kafka::constants::{ERROR_MESSAGE_TOO_LARGE, ERROR_REQUEST_TIMED_OUT};
use kafka_protocol::messages::produce_request::{
    ProduceRequest, TopicProduceData as WireTopicProduceData,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use tracing::warn;

use super::{
    ApiEnv, ApiSchema, Call, Decoded, KafkaApi, Rejection, RequestBody, RequestHeader,
    ResponseBody, TopicPartitions,
};
use crate::kafka::constants::{ERROR_CORRUPT_MESSAGE, ERROR_INVALID_REQUIRED_ACKS, ERROR_NONE};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers;
use crate::kafka::messages::{
    KafkaRequest, KafkaResponse, PartitionProduceData, ProducerMetadata, TopicProduceData,
};
use crate::kafka::notifications::InternalNotification;
use crate::kafka::protocol::decode_budget::{
    decode_wait, DecodeBudget, DecodedCharge, Exhausted, HeldFrame, NotRun, DECODED,
    INLINE_DECODE_BYTES, LARGE_DECODES, MAX_DECODED_REQUEST_BYTES,
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
    /// Records still to decode, once there is room: `topic_data` is empty until
    /// [`finish_decode`] decoded them. Only the network thread sees a request that has them.
    pub pending: Option<PendingRecords>,
}

/// The records of a Produce request that could not decode at once: it waits for a
/// large-decode slot or for decode budget, until `deadline` (see
/// `protocol::decode_budget`).
pub struct PendingRecords {
    topics: Arc<Vec<WireTopicProduceData>>,
    requested: TopicPartitions,
    wait: Wait,
    deadline: tokio::time::Instant,
    /// The request's frame, charged to the frame budget while it waits.
    _frame: HeldFrame,
}

impl std::fmt::Debug for PendingRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRecords")
            .field("requested", &self.requested)
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

/// What a pending decode waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    /// A large-decode slot: the records take more than [`INLINE_DECODE_BYTES`].
    Slot,
    /// This much free in the process-wide budget, before an inline decode.
    Budget(usize),
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
        let decode = decode_all_records(&wire.topic_data, zstd_allowed);
        let transactional_id = wire.transactional_id.map(|s| s.to_string());
        let (topic_data, decoded, pending) = match decode {
            Ok(Decode::Done((topic_data, decoded))) => (topic_data, decoded, None),
            Ok(Decode::Pending(wait)) => {
                let Some(frame) = HeldFrame::charge(record_bytes(&wire.topic_data)) else {
                    return Err(rejection(DecodeFailure::TimedOut, acks, requested));
                };
                let pending = PendingRecords {
                    topics: Arc::new(wire.topic_data),
                    requested,
                    wait,
                    deadline: tokio::time::Instant::now() + decode_wait(wire.timeout_ms),
                    _frame: frame,
                };
                (Vec::new(), DecodedCharge::default(), Some(pending))
            }
            Err(failure) => return Err(rejection(failure, acks, requested)),
        };

        Ok(ProduceRequestData {
            acks,
            timeout_ms: wire.timeout_ms,
            topic_data,
            transactional_id,
            decoded,
            pending,
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
        debug_assert!(
            request.pending.is_none(),
            "the network thread decodes pending records before dispatch"
        );
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
                let _ = response_tx.send(KafkaResponse::new(-1, -1, ResponseBody::CloseConnection));
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
    /// The process-wide decode budget lacks room: it needs this much free in all.
    Busy(usize),
    /// No room to decode came before the request's wait ended
    /// ([`decode_wait`]), or there is none to hold its frame while it waits.
    TimedOut,
    /// The decoder panicked.
    Panicked,
    /// A batch is corrupt or invalid: this code on every partition.
    Rejected(i16),
}

/// How the network thread answers a Produce whose records did not decode.
///
/// acks=0 never gets a response: an unsolicited one would be read as the reply to the
/// client's next request. Kafka closes the connection instead, so the client notices. A
/// request that found no room to decode in time is answered REQUEST_TIMED_OUT, which
/// Kafka answers a Produce that ran out of time with, and which clients retry.
fn rejection(
    failure: DecodeFailure,
    acks: i16,
    requested: TopicPartitions,
) -> Rejection<TopicPartitions> {
    let error_code = match failure {
        _ if acks == 0 => {
            return Rejection::Close(match failure {
                DecodeFailure::TimedOut | DecodeFailure::Busy(_) => {
                    "acks=0 Produce found no room to decode in time"
                }
                _ => "acks=0 Produce with a corrupt or oversized record batch",
            })
        }
        DecodeFailure::Panicked => return Rejection::Close("Produce records decoder panicked"),
        DecodeFailure::TooLarge => ERROR_MESSAGE_TOO_LARGE,
        DecodeFailure::TimedOut | DecodeFailure::Busy(_) => ERROR_REQUEST_TIMED_OUT,
        DecodeFailure::Rejected(code) => code,
    };
    Rejection::Reply {
        error_code,
        echo: Some(requested),
    }
}

/// A decode done, or waiting for room.
#[derive(Debug)]
enum Decode {
    Done(DecodedTopics),
    Pending(Wait),
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
                            Some(Exhausted::Global) => DecodeFailure::Busy(budget.wanted()),
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

/// The bytes of the record batches of `topics` (what holding the request's frame holds).
fn record_bytes(topics: &[WireTopicProduceData]) -> usize {
    topics
        .iter()
        .flat_map(|t| t.partition_data.iter())
        .filter_map(|p| p.records.as_ref())
        .map(bytes::Bytes::len)
        .fold(0, usize::saturating_add)
}

/// Decode the records of `topics` at once if that can be done on the I/O thread.
///
/// A request whose batches take at most [`INLINE_DECODE_BYTES`] of memory by what they
/// declare ([`declared_memory`]) is decoded in place, within that budget: the common case of
/// small (compressed or not) batches. One that declares more, or that expands past it, is
/// left to [`finish_decode`] under a large-decode slot ([`Wait::Slot`]); one that finds the
/// process-wide budget exhausted waits there for room ([`Wait::Budget`]).
fn decode_all_records(
    topics: &[WireTopicProduceData],
    zstd_allowed: bool,
) -> Result<Decode, DecodeFailure> {
    validate_all_records(topics, zstd_allowed)?;
    if declared_memory(topics) > INLINE_DECODE_BYTES {
        return Ok(Decode::Pending(Wait::Slot));
    }
    match decode_records(topics, DecodeBudget::new(INLINE_DECODE_BYTES)) {
        Ok(done) => Ok(Decode::Done(done)),
        Err(DecodeFailure::TooLarge) => Ok(Decode::Pending(Wait::Slot)),
        Err(DecodeFailure::Busy(wanted)) => Ok(Decode::Pending(Wait::Budget(wanted))),
        Err(failure) => Err(failure),
    }
}

/// Decode pending records once there is room, waiting (without holding a thread) for a
/// large-decode slot or for decode budget until the request's deadline.
async fn decode_pending(pending: &PendingRecords) -> Result<DecodedTopics, DecodeFailure> {
    let mut wait = pending.wait;
    loop {
        let result = match wait {
            Wait::Budget(wanted) => {
                if !DECODED.wait_for(wanted, pending.deadline).await {
                    return Err(DecodeFailure::TimedOut);
                }
                let topics = &pending.topics;
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    decode_records(topics, DecodeBudget::new(INLINE_DECODE_BYTES))
                }))
                .unwrap_or(Err(DecodeFailure::Panicked))
            }
            Wait::Slot => {
                let topics = Arc::clone(&pending.topics);
                LARGE_DECODES
                    .run(pending.deadline, move || {
                        decode_records(&topics, DecodeBudget::new(MAX_DECODED_REQUEST_BYTES))
                    })
                    .await
                    .unwrap_or_else(|not_run| {
                        Err(match not_run {
                            NotRun::TimedOut => DecodeFailure::TimedOut,
                            NotRun::Panicked => DecodeFailure::Panicked,
                        })
                    })
            }
        };
        wait = match (result, wait) {
            (Err(DecodeFailure::TooLarge), Wait::Budget(_)) => Wait::Slot,
            (Err(DecodeFailure::Busy(wanted)), Wait::Slot) => {
                if !DECODED.wait_for(wanted, pending.deadline).await {
                    return Err(DecodeFailure::TimedOut);
                }
                Wait::Slot
            }
            (Err(DecodeFailure::Busy(wanted)), Wait::Budget(_)) => Wait::Budget(wanted),
            (done, _) => return done,
        };
    }
}

/// Finish decoding `request` if its records wait to decode ([`PendingRecords`]): wait for
/// room, decode them, and return the request for the DB thread, or answer it (an error
/// response, REQUEST_TIMED_OUT when no room came in time) or close the connection
/// (acks=0) as a failed decode is. Any other request is returned as it is.
pub async fn finish_decode(mut request: KafkaRequest) -> Decoded {
    let RequestBody::Produce(produce) = &mut request.body else {
        return Decoded::Request(request);
    };
    let Some(pending) = produce.pending.take() else {
        return Decoded::Request(request);
    };
    match decode_pending(&pending).await {
        Ok((topic_data, decoded)) => {
            produce.topic_data = topic_data;
            produce.decoded = decoded;
            Decoded::Request(request)
        }
        Err(failure) => {
            if failure == DecodeFailure::TimedOut {
                warn!(
                    "No room to decode a Produce request within its wait; answering REQUEST_TIMED_OUT"
                );
            }
            match rejection(failure, produce.acks, pending.requested) {
                Rejection::Reply { error_code, echo } => {
                    let version = request.header.api_version;
                    // A closed channel means the connection is gone; there is nobody to
                    // answer.
                    let _ = request.response_tx.send(KafkaResponse::new(
                        request.header.correlation_id,
                        version,
                        ResponseBody::Produce(ProduceApi::error_response(
                            echo.as_ref(),
                            version,
                            error_code,
                        )),
                    ));
                    Decoded::Answered
                }
                Rejection::Close(reason) => Decoded::Close(reason.to_string()),
                Rejection::Silent => Decoded::Answered,
            }
        }
    }
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
    use crate::kafka::constants::ERROR_UNSUPPORTED_COMPRESSION_TYPE;
    use crate::kafka::protocol::recordbatch_test_batches::{batch_with, record, v, Header};
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

    #[test]
    fn requests_that_cannot_decode_inline_are_left_pending() {
        let small = topics(vec![batch_of(15, 1000)]);
        assert!(matches!(
            decode_all_records(&small, true),
            Ok(Decode::Done(_))
        ));
        // 3000 records of 1000 bytes declare more than the inline limit.
        let large = topics(vec![batch_of(3000, 1000)]);
        assert!(declared_memory(&large) > INLINE_DECODE_BYTES);
        assert!(matches!(
            decode_all_records(&large, true),
            Ok(Decode::Pending(Wait::Slot))
        ));
    }

    /// A Produce request (`acks`) for two partitions whose records wait for `wait` until
    /// `wait_for` from now.
    fn pending_request(
        acks: i16,
        wait: Wait,
        wait_for: std::time::Duration,
    ) -> (
        KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
    ) {
        let wire = topics(vec![batch_of(1, 10), batch_of(1, 10)]);
        let pending = PendingRecords {
            requested: vec![("t".to_string(), vec![0, 1])],
            topics: Arc::new(wire),
            wait,
            deadline: tokio::time::Instant::now() + wait_for,
            _frame: HeldFrame::charge(0).unwrap(),
        };
        let (response_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let request = KafkaRequest {
            header: RequestHeader {
                api_key: 0,
                api_version: 7,
                correlation_id: 5,
                client_id: None,
            },
            body: RequestBody::Produce(ProduceRequestData {
                acks,
                timeout_ms: 30_000,
                topic_data: Vec::new(),
                transactional_id: None,
                decoded: DecodedCharge::default(),
                pending: Some(pending),
            }),
            response_tx,
        };
        (request, rx)
    }

    #[tokio::test]
    async fn pending_records_decode_once_there_is_room() {
        let (request, _rx) =
            pending_request(1, Wait::Budget(1), std::time::Duration::from_secs(30));
        let Decoded::Request(request) = finish_decode(request).await else {
            panic!("expected the decoded request");
        };
        let RequestBody::Produce(produce) = request.body else {
            panic!("expected a Produce request");
        };
        assert!(produce.pending.is_none());
        assert_eq!(produce.topic_data[0].partitions.len(), 2);
        assert_eq!(produce.topic_data[0].partitions[1].records.len(), 1);
        assert!(produce.decoded.bytes() > 0, "the decode is charged");
    }

    #[tokio::test]
    async fn a_request_with_no_room_in_time_is_answered_request_timed_out() {
        // More budget than there is: the wait can only end at its deadline.
        let started = std::time::Instant::now();
        let (request, mut rx) = pending_request(
            1,
            Wait::Budget(usize::MAX),
            std::time::Duration::from_millis(200),
        );
        assert!(matches!(finish_decode(request).await, Decoded::Answered));
        assert!(started.elapsed() >= std::time::Duration::from_millis(200));
        let response = rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 5);
        let ResponseBody::Produce(produce) = response.body else {
            panic!("expected a Produce response");
        };
        let codes: Vec<i16> = produce.responses[0]
            .partition_responses
            .iter()
            .map(|p| p.error_code)
            .collect();
        assert_eq!(
            codes,
            vec![ERROR_REQUEST_TIMED_OUT; 2],
            "on every partition"
        );

        // acks=0 cannot be answered: the connection closes, as for any acks=0 failure.
        let (request, mut rx) = pending_request(
            0,
            Wait::Budget(usize::MAX),
            std::time::Duration::from_millis(50),
        );
        assert!(matches!(finish_decode(request).await, Decoded::Close(_)));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn decode_failures_map_to_kafka_answers() {
        let code = |failure| match rejection(failure, 1, Vec::new()) {
            Rejection::Reply { error_code, .. } => Some(error_code),
            _ => None,
        };
        assert_eq!(code(DecodeFailure::TimedOut), Some(ERROR_REQUEST_TIMED_OUT));
        assert_eq!(code(DecodeFailure::Busy(1)), Some(ERROR_REQUEST_TIMED_OUT));
        assert_eq!(code(DecodeFailure::TooLarge), Some(ERROR_MESSAGE_TOO_LARGE));
        assert_eq!(code(DecodeFailure::Rejected(87)), Some(87));
        assert_eq!(code(DecodeFailure::Panicked), None, "closes the connection");
        assert!(matches!(
            rejection(DecodeFailure::TimedOut, 0, Vec::new()),
            Rejection::Close(_)
        ));
    }

    #[tokio::test]
    async fn requests_without_pending_records_pass_through() {
        let (request, _rx) = pending_request(1, Wait::Slot, std::time::Duration::from_secs(30));
        let mut request = request;
        if let RequestBody::Produce(p) = &mut request.body {
            p.pending = None;
        }
        assert!(matches!(finish_decode(request).await, Decoded::Request(_)));
    }
}
