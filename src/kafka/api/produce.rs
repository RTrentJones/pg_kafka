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
use crate::kafka::constants::{
    ERROR_CORRUPT_MESSAGE, ERROR_INVALID_REQUIRED_ACKS, ERROR_NONE,
    ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers;
use crate::kafka::messages::{
    KafkaRequest, KafkaResponse, PartitionProduceData, ProducerMetadata, RefusedPartition,
    TopicProduceData,
};
use crate::kafka::notifications::InternalNotification;
use crate::kafka::protocol::decode_budget::{
    decode_wait, DecodeBudget, DecodedCharge, Exhausted, HeldFrame, NotRun, DECODED,
    INLINE_DECODE_BYTES, INLINE_DECODE_WORK_BYTES, LARGE_DECODES, MAX_DECODED_REQUEST_BYTES,
    SLOT_DECODE_WORK_BYTES,
};
use crate::kafka::protocol::{
    first_batch_is_transactional, least_decoded_memory, parse_record_batch_with_metadata,
    validate_produce_records, ParsedRecordBatch,
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
    /// Topic → partitions → records, of the partitions whose records decoded.
    pub topic_data: Vec<TopicProduceData>,
    /// The partitions whose records failed validation, answered with their own errors.
    pub refused: Vec<RefusedPartition>,
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
    zstd_allowed: bool,
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
        mut wire: ProduceRequest,
        header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        let acks = wire.acks;
        wire.topic_data = last_entry_per_partition(std::mem::take(&mut wire.topic_data));
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
        let decode = decode_all_records(
            &wire.topic_data,
            zstd_allowed,
            wire.transactional_id.is_some(),
        );
        let transactional_id = wire.transactional_id.map(|s| s.to_string());
        let (DecodedTopics(topic_data, refused, decoded), pending) = match decode {
            Ok(Decode::Done(done)) => (done, None),
            Ok(Decode::Pending(wait)) => {
                let Some(frame) = HeldFrame::charge(record_bytes(&wire.topic_data)) else {
                    return Err(rejection(DecodeFailure::TimedOut, acks, requested));
                };
                let pending = PendingRecords {
                    topics: Arc::new(wire.topic_data),
                    zstd_allowed,
                    requested,
                    wait,
                    deadline: tokio::time::Instant::now() + decode_wait(wire.timeout_ms),
                    _frame: frame,
                };
                (DecodedTopics::default(), Some(pending))
            }
            Err(failure) => return Err(rejection(failure, acks, requested)),
        };

        Ok(ProduceRequestData {
            acks,
            timeout_ms: wire.timeout_ms,
            topic_data,
            refused,
            transactional_id,
            decoded,
            pending,
        })
    }

    fn echo(request: &Self::Request) -> TopicPartitions {
        let mut echo: TopicPartitions = request
            .topic_data
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.partitions.iter().map(|p| p.partition_index).collect(),
                )
            })
            .collect();
        for refused in &request.refused {
            match echo.iter_mut().find(|(name, _)| *name == refused.topic) {
                Some((_, partitions)) => partitions.push(refused.partition),
                None => echo.push((refused.topic.clone(), vec![refused.partition])),
            }
        }
        echo
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
            let refused = !request.refused.is_empty();
            return match handlers::handle_produce(&env.ctx, request.topic_data, None, None) {
                Ok(response) => {
                    emit_produce_notifications(&response, store, env.notify_tx);
                    if refused || acks0_must_close(&response) {
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
        //
        // Refused partitions are answered before the others are written, so a topic that
        // a valid partition auto-creates is still unknown to them, as it was to the client.
        call.run(request, |r, _| {
            let refused = handlers::answer_refused_partitions(store, &r.refused)?;
            let producer_metadata = extract_producer_metadata(&r.topic_data);
            let mut response = handlers::handle_produce(
                &env.ctx,
                r.topic_data,
                producer_metadata.as_ref(),
                r.transactional_id.as_deref(),
            )?;
            emit_produce_notifications(&response, store, env.notify_tx);
            merge_topic_responses(&mut response, refused);
            Ok(response)
        })
    }
}

/// The first Produce version that may carry zstd batches (KIP-110).
const MIN_ZSTD_VERSION: i16 = 7;

/// Why a Produce request's records did not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeFailure {
    /// A partition takes more memory than the inline limit leaves it
    /// ([`INLINE_DECODE_BYTES`]): the request decodes again under a large-decode slot,
    /// where a partition that does not fit is refused on its own.
    TooLarge,
    /// The process-wide decode budget lacks room: it needs this much free in all.
    Busy(usize),
    /// No room to decode came before the request's wait ended
    /// ([`decode_wait`]), or there is none to hold its frame while it waits.
    TimedOut,
    /// The decoder panicked.
    Panicked,
    /// Refused before any partition is looked at (a first batch Kafka cannot size, a
    /// transactional batch without a transactional id): this code on every partition.
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

/// The records of the partitions that decoded, the partitions refused, and the decoded
/// records' charge to the decode budget.
#[derive(Debug, Default)]
struct DecodedTopics(Vec<TopicProduceData>, Vec<RefusedPartition>, DecodedCharge);

/// What a decode does with a partition that does not fit in its budget's limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverLimit {
    /// Fail the decode ([`DecodeFailure::TooLarge`]): the inline limit, past which the
    /// request decodes again under a large-decode slot.
    Retry,
    /// Refuse the partition MESSAGE_TOO_LARGE and go on with the next: the request limit.
    Refuse,
}

/// The records of one partition, or the error that refuses it: Kafka 3.7 validates each
/// partition on its own (`KafkaApis.handleProduceRequest` answers a partition whose data
/// `ProduceRequest.validateRecords` refuses with that error, and the log append that
/// validates the batch fails that partition only), and writes the others. A partition
/// that does not fit in what the request may still hold is, under [`OverLimit::Refuse`],
/// refused MESSAGE_TOO_LARGE, as Kafka refuses a batch past `message.max.bytes`. A refused
/// partition's charge is given back, so it does not count against the partitions after it;
/// only the process-wide budget running out fails the whole decode, which then waits for
/// room.
fn decode_partition(
    batch: &bytes::Bytes,
    zstd_allowed: bool,
    budget: &DecodeBudget,
    over_limit: OverLimit,
) -> Result<Result<ParsedRecordBatch, i16>, DecodeFailure> {
    if let Err(code) = validate_produce_records(batch, zstd_allowed) {
        return Ok(Err(code));
    }
    if budget.work_exhausted() {
        // The request decoded all it may (refused partitions' work included): the rest
        // are refused without decoding.
        return Ok(Err(ERROR_MESSAGE_TOO_LARGE));
    }
    let mark = budget.charged();
    let e = match parse_record_batch_with_metadata(batch, budget) {
        Ok(parsed) => return Ok(Ok(parsed)),
        Err(e) => e,
    };
    let code = match budget.exhausted() {
        Some(Exhausted::Global) => return Err(DecodeFailure::Busy(budget.wanted())),
        Some(Exhausted::Limit | Exhausted::Work) if over_limit == OverLimit::Retry => {
            return Err(DecodeFailure::TooLarge)
        }
        Some(Exhausted::Limit | Exhausted::Work) => ERROR_MESSAGE_TOO_LARGE,
        None => match e {
            KafkaError::InvalidRecord { .. } | KafkaError::Protocol { .. } => {
                e.to_kafka_error_code()
            }
            _ => ERROR_CORRUPT_MESSAGE,
        },
    };
    budget.rewind(mark);
    Ok(Err(code))
}

/// Decode the records of every partition of `topics` within `budget`. A topic all of whose
/// partitions are refused is left out of the decoded topics, so nothing is written to it
/// (and it is not auto-created).
fn decode_records(
    topics: &[WireTopicProduceData],
    zstd_allowed: bool,
    budget: DecodeBudget,
    over_limit: OverLimit,
) -> Result<DecodedTopics, DecodeFailure> {
    let mut topic_data = Vec::with_capacity(topics.len());
    let mut refused = Vec::new();
    for topic in topics {
        let name = topic.name.to_string();
        let mut partitions = Vec::with_capacity(topic.partition_data.len());
        for partition in &topic.partition_data {
            let (records, producer_metadata) = match &partition.records {
                Some(batch) => match decode_partition(batch, zstd_allowed, &budget, over_limit)? {
                    Ok(ParsedRecordBatch {
                        records,
                        producer_metadata,
                    }) => (records, Some(producer_metadata)),
                    Err(error_code) => {
                        warn!(
                            "Refusing the records of topic={name}, partition={}: error {error_code}",
                            partition.index
                        );
                        refused.push(RefusedPartition {
                            topic: name.clone(),
                            partition: partition.index,
                            error_code,
                        });
                        continue;
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
        if !partitions.is_empty() || topic.partition_data.is_empty() {
            topic_data.push(TopicProduceData { name, partitions });
        }
    }
    Ok(DecodedTopics(topic_data, refused, budget.into_charge()))
}

/// What Kafka 3.7 reads of the request before it looks at any partition
/// (`RequestUtils.hasTransactionalRecords`, [`first_batch_is_transactional`]): the first
/// batch of each partition, in order, until one is transactional. One whose length or
/// magic cannot be read fails the whole request with CORRUPT_MESSAGE. A transactional
/// one in a request without a transactional id fails it with
/// TRANSACTIONAL_ID_AUTHORIZATION_FAILED (`KafkaApis.handleProduceRequest` authorizes
/// the transactional id of such a request, and a null one never is).
fn check_first_batches(
    topics: &[WireTopicProduceData],
    has_transactional_id: bool,
) -> Result<(), DecodeFailure> {
    for topic in topics {
        for partition in &topic.partition_data {
            let Some(batch) = &partition.records else {
                continue;
            };
            match first_batch_is_transactional(batch) {
                Ok(true) if has_transactional_id => return Ok(()),
                Ok(true) => {
                    warn!("Refusing a transactional Produce request without a transactional id");
                    return Err(DecodeFailure::Rejected(
                        ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                    ));
                }
                Ok(false) => {}
                Err(code) => {
                    warn!(
                        "Refusing a Produce request: the first batch of topic={}, partition={} cannot be sized",
                        topic.name.0.as_str(),
                        partition.index
                    );
                    return Err(DecodeFailure::Rejected(code));
                }
            }
        }
    }
    Ok(())
}

/// The memory the batches of `topics` take decoded by what they declare (their bytes and
/// record counts), the least they can take. Batches refused before they decode
/// ([`least_decoded_memory`]) take none.
fn declared_memory(topics: &[WireTopicProduceData], zstd_allowed: bool) -> usize {
    topics
        .iter()
        .flat_map(|t| t.partition_data.iter())
        .filter_map(|p| p.records.as_ref())
        .map(|batch| least_decoded_memory(batch, zstd_allowed))
        .fold(0, usize::saturating_add)
}

/// Add the answers of refused partitions to `response`, under their topics.
fn merge_topic_responses(response: &mut ProduceResponse, refused: Vec<TopicProduceResponse>) {
    for topic in refused {
        match response.responses.iter_mut().find(|t| t.name == topic.name) {
            Some(answered) => answered
                .partition_responses
                .extend(topic.partition_responses),
            None => response.responses.push(topic),
        }
    }
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

/// The budget of a decode on an I/O thread: at most [`INLINE_DECODE_BYTES`] held and
/// [`INLINE_DECODE_WORK_BYTES`] decoded, so that partitions refused after they decode cannot
/// keep the thread decoding (past either the request decodes again under a large-decode
/// slot).
fn inline_budget() -> DecodeBudget {
    DecodeBudget::new(INLINE_DECODE_BYTES).limit_work(INLINE_DECODE_WORK_BYTES)
}

/// One entry per partition, the last one the request names, as Kafka 3.7 keeps them
/// (`KafkaApis.handleProduceRequest` collects a request's partitions in maps keyed by topic
/// and partition, so a later entry replaces an earlier one, in its topic entry or in another
/// of the same name): an earlier entry is neither decoded, written nor answered. Topic
/// entries of one name merge into the first.
fn last_entry_per_partition(topics: Vec<WireTopicProduceData>) -> Vec<WireTopicProduceData> {
    let entries: usize = topics.iter().map(|t| t.partition_data.len()).sum();
    let mut topic_at: std::collections::HashMap<kafka_protocol::messages::TopicName, usize> =
        std::collections::HashMap::with_capacity(topics.len());
    let mut partition_at: std::collections::HashMap<(usize, i32), usize> =
        std::collections::HashMap::with_capacity(entries);
    let mut out: Vec<WireTopicProduceData> = Vec::with_capacity(topics.len());
    for mut topic in topics {
        let partitions = std::mem::take(&mut topic.partition_data);
        let t = *topic_at.entry(topic.name.clone()).or_insert_with(|| {
            out.push(topic);
            out.len() - 1
        });
        for partition in partitions {
            match partition_at.entry((t, partition.index)) {
                std::collections::hash_map::Entry::Occupied(at) => {
                    out[t].partition_data[*at.get()] = partition;
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(out[t].partition_data.len());
                    out[t].partition_data.push(partition);
                }
            }
        }
    }
    out
}

/// Decode the records of `topics` at once if that can be done on the I/O thread.
///
/// A request whose batches take at most [`INLINE_DECODE_BYTES`] of memory by what they
/// declare ([`declared_memory`]) is decoded in place, within that budget: the common case of
/// small (compressed or not) batches. One that declares more, or that expands past it, is
/// left to [`finish_decode`] under a large-decode slot ([`Wait::Slot`]); one that finds the
/// process-wide budget exhausted waits there for room ([`Wait::Budget`]). A batch that
/// declares more than any request may hold ([`MAX_DECODED_REQUEST_BYTES`]) can never
/// decode: it declares nothing here and is refused as it decodes, before anything is
/// charged, so it never leaves a request waiting for a slot it would time out waiting for.
fn decode_all_records(
    topics: &[WireTopicProduceData],
    zstd_allowed: bool,
    has_transactional_id: bool,
) -> Result<Decode, DecodeFailure> {
    check_first_batches(topics, has_transactional_id)?;
    if declared_memory(topics, zstd_allowed) > INLINE_DECODE_BYTES {
        return Ok(Decode::Pending(Wait::Slot));
    }
    match decode_records(topics, zstd_allowed, inline_budget(), OverLimit::Retry) {
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
                    decode_records(
                        topics,
                        pending.zstd_allowed,
                        inline_budget(),
                        OverLimit::Retry,
                    )
                }))
                .unwrap_or(Err(DecodeFailure::Panicked))
            }
            Wait::Slot => {
                let topics = Arc::clone(&pending.topics);
                let zstd_allowed = pending.zstd_allowed;
                LARGE_DECODES
                    .run(pending.deadline, move || {
                        decode_records(
                            &topics,
                            zstd_allowed,
                            DecodeBudget::new(MAX_DECODED_REQUEST_BYTES)
                                .limit_work(SLOT_DECODE_WORK_BYTES),
                            OverLimit::Refuse,
                        )
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
        Ok(DecodedTopics(topic_data, refused, decoded)) => {
            produce.topic_data = topic_data;
            produce.refused = refused;
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
        batch_of_with(n, len, Header::default())
    }

    /// [`batch_of`] with the header fields of `header`.
    fn batch_of_with(n: usize, len: usize, header: Header) -> bytes::Bytes {
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
        batch_with(&section, i32::try_from(n).unwrap(), header)
    }

    /// The partitions decoded, and the partitions refused with their codes.
    type Outcome = (Vec<i32>, Vec<(i32, i16)>);

    /// What a request of `data` (one batch per partition) decodes to; `Err` for a request
    /// refused whole.
    fn outcome(data: Vec<bytes::Bytes>, zstd_allowed: bool) -> Result<Outcome, DecodeFailure> {
        outcome_of(data, zstd_allowed, false)
    }

    /// [`outcome`] of a request with a transactional id or without.
    fn outcome_of(
        data: Vec<bytes::Bytes>,
        zstd_allowed: bool,
        has_transactional_id: bool,
    ) -> Result<Outcome, DecodeFailure> {
        let Decode::Done(DecodedTopics(topic_data, refused, _)) =
            decode_all_records(&topics(data), zstd_allowed, has_transactional_id)?
        else {
            panic!("a small request decodes inline");
        };
        Ok((
            topic_data
                .iter()
                .flat_map(|t| t.partitions.iter().map(|p| p.partition_index))
                .collect(),
            refused
                .iter()
                .map(|r| (r.partition, r.error_code))
                .collect(),
        ))
    }

    #[test]
    fn zstd_is_refused_before_produce_v7_on_its_partition() {
        let x = record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"x", &v(0)]);
        let zstd = batch_with(
            &x,
            1,
            Header {
                attributes: 4,
                ..Header::default()
            },
        );
        // The batch is not zstd data at all: refused for its codec first.
        assert_eq!(
            outcome(vec![zstd.clone(), batch_of(1, 1)], false),
            Ok((vec![1], vec![(0, ERROR_UNSUPPORTED_COMPRESSION_TYPE)]))
        );
        // From v7 on the codec is accepted and the batch fails to decompress.
        assert_eq!(
            outcome(vec![batch_of(1, 1), zstd], true),
            Ok((vec![0], vec![(1, ERROR_CORRUPT_MESSAGE)]))
        );
    }

    #[test]
    fn each_partition_is_validated_on_its_own() {
        use crate::kafka::constants::ERROR_INVALID_RECORD;
        let good = batch_of(1, 1);
        let mut bad_crc = good.to_vec();
        *bad_crc.last_mut().unwrap() ^= 1;
        let two = bytes::Bytes::from([&good[..], &good[..]].concat());
        assert_eq!(
            outcome(
                vec![bad_crc.into(), good.clone(), two, bytes::Bytes::new()],
                true
            ),
            Ok((
                vec![1],
                vec![
                    (0, ERROR_CORRUPT_MESSAGE),
                    (2, ERROR_INVALID_RECORD),
                    (3, ERROR_INVALID_RECORD)
                ]
            ))
        );
        // A topic all of whose partitions are refused is not passed on (nor created).
        let bad = bytes::Bytes::from_static(b"short");
        let Ok(Decode::Done(DecodedTopics(topic_data, refused, _))) =
            decode_all_records(&topics(vec![bad]), true, false)
        else {
            panic!("decodes inline");
        };
        assert!(topic_data.is_empty());
        assert_eq!(refused.len(), 1);
    }

    #[test]
    fn only_a_first_batch_that_cannot_be_sized_fails_the_whole_request() {
        let good = batch_of(1, 1);
        // A batch length under 14, and a magic byte past 2: Kafka cannot size the batch.
        let mut short_length = good.to_vec();
        short_length[8..12].copy_from_slice(&13i32.to_be_bytes());
        let mut magic_9 = good.to_vec();
        magic_9[16] = 9;
        for first in [short_length.clone(), magic_9] {
            assert_eq!(
                outcome(vec![good.clone(), first.into()], true),
                Err(DecodeFailure::Rejected(ERROR_CORRUPT_MESSAGE))
            );
        }
        // Past the first batch of a partition, only that partition is refused.
        let after = bytes::Bytes::from([&good[..], &short_length[..12]].concat());
        assert_eq!(
            outcome(vec![after, good.clone()], true),
            Ok((vec![1], vec![(0, ERROR_CORRUPT_MESSAGE)]))
        );
        // Kafka stops reading first batches at the first transactional one.
        let transactional = batch_with(
            &record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"t", &v(0)]),
            1,
            Header {
                attributes: 0x10,
                producer_id: 7,
                producer_epoch: 0,
                base_sequence: 0,
                ..Header::default()
            },
        );
        assert_eq!(
            outcome_of(vec![transactional, short_length.into()], true, true),
            Ok((vec![0], vec![(1, ERROR_CORRUPT_MESSAGE)]))
        );
    }

    /// A batch of one record, transactional.
    fn transactional_batch() -> bytes::Bytes {
        batch_with(
            &record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"t", &v(0)]),
            1,
            Header {
                attributes: 0x10,
                producer_id: 7,
                producer_epoch: 0,
                base_sequence: 0,
                ..Header::default()
            },
        )
    }

    #[test]
    fn a_transactional_batch_needs_a_transactional_id() {
        // Kafka 3.7 refuses the request on every partition, wherever the batch is.
        for data in [
            vec![transactional_batch(), batch_of(1, 1)],
            vec![batch_of(1, 1), transactional_batch()],
        ] {
            assert_eq!(
                outcome_of(data.clone(), true, false),
                Err(DecodeFailure::Rejected(
                    ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                ))
            );
            assert_eq!(outcome_of(data, true, true), Ok((vec![0, 1], vec![])));
        }
        // Only the first batch of each partition is read for it, as Kafka reads it.
        let after = bytes::Bytes::from([&batch_of(1, 1)[..], &transactional_batch()[..]].concat());
        assert_eq!(
            outcome(vec![after], true),
            Ok((
                vec![],
                vec![(0, crate::kafka::constants::ERROR_INVALID_RECORD)]
            ))
        );
    }

    #[test]
    fn refused_partitions_are_merged_under_their_topics_and_echoed() {
        let answered = |name: &str, partitions: &[(i32, i16)]| {
            let mut topic = TopicProduceResponse::default();
            topic.name = kafka_protocol::messages::TopicName(name.to_string().into());
            topic.partition_responses = partitions
                .iter()
                .map(|&(index, error_code)| {
                    let mut p = PartitionProduceResponse::default();
                    p.index = index;
                    p.error_code = error_code;
                    p
                })
                .collect();
            topic
        };
        let mut response = ProduceResponse::default();
        response.responses = vec![answered("a", &[(0, 0)])];
        merge_topic_responses(
            &mut response,
            vec![answered("a", &[(1, 2)]), answered("b", &[(0, 3)])],
        );
        let got: Vec<(String, i32, i16)> = response
            .responses
            .iter()
            .flat_map(|t| {
                t.partition_responses
                    .iter()
                    .map(|p| (t.name.to_string(), p.index, p.error_code))
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), 0, 0),
                ("a".to_string(), 1, 2),
                ("b".to_string(), 0, 3)
            ]
        );

        let (request, _rx) = pending_request(1, Wait::Slot, std::time::Duration::from_secs(1));
        let RequestBody::Produce(mut produce) = request.body else {
            panic!("a Produce request");
        };
        produce.topic_data = vec![TopicProduceData {
            name: "a".to_string(),
            partitions: Vec::new(),
        }];
        produce.refused = vec![
            RefusedPartition {
                topic: "a".to_string(),
                partition: 1,
                error_code: 2,
            },
            RefusedPartition {
                topic: "b".to_string(),
                partition: 0,
                error_code: 2,
            },
        ];
        assert_eq!(
            ProduceApi::echo(&produce),
            vec![("a".to_string(), vec![1]), ("b".to_string(), vec![0])],
            "an error response answers the refused partitions too"
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
            let declared = declared_memory(&topics, true);
            assert!(
                declared <= INLINE_DECODE_BYTES,
                "{len}-byte records declare {declared}"
            );
            let budget = DecodeBudget::new(INLINE_DECODE_BYTES);
            assert!(
                decode_records(&topics, true, budget, OverLimit::Retry).is_ok(),
                "{len}-byte records"
            );
        }
    }

    #[test]
    fn requests_that_cannot_decode_inline_are_left_pending() {
        let small = topics(vec![batch_of(15, 1000)]);
        assert!(matches!(
            decode_all_records(&small, true, false),
            Ok(Decode::Done(_))
        ));
        // 3000 records of 1000 bytes declare more than the inline limit.
        let large = topics(vec![batch_of(3000, 1000)]);
        assert!(declared_memory(&large, true) > INLINE_DECODE_BYTES);
        assert!(matches!(
            decode_all_records(&large, true, false),
            Ok(Decode::Pending(Wait::Slot))
        ));
    }

    #[test]
    fn a_partition_declaring_too_many_records_is_refused_on_its_own() {
        let x = record(&[&[0], &v(0), &v(0), &v(-1), &v(1), b"x", &v(0)]);
        let liar = batch_with(
            &x,
            2_000_000,
            Header {
                last_offset_delta: Some(0),
                ..Header::default()
            },
        );
        assert_eq!(
            outcome(vec![batch_of(1, 1), liar], true),
            Ok((
                vec![0],
                vec![(1, crate::kafka::constants::ERROR_INVALID_RECORD)]
            ))
        );
        // Consistent with its lastOffsetDelta, its one record still cannot be 2 million:
        // Kafka answers INVALID_RECORD once it runs out of records.
        let consistent = batch_with(&x, 2_000_000, Header::default());
        assert_eq!(
            outcome(vec![consistent, batch_of(1, 1)], true),
            Ok((
                vec![1],
                vec![(0, crate::kafka::constants::ERROR_INVALID_RECORD)]
            ))
        );
    }

    #[test]
    fn a_partition_that_can_never_decode_is_refused_at_once_on_its_own() {
        use crate::kafka::protocol::recordbatch_test_batches::{raw_batch, tiny_records};
        // 1.2 million 7-byte records, uncompressed and gzipped: 105 MB of records, past
        // the 100 MB a request may hold. Refused MESSAGE_TOO_LARGE before anything decodes, so
        // the request is not left waiting for a large-decode slot (under saturation, the
        // wait would end in REQUEST_TIMED_OUT, which the client retries); the other
        // partition decodes inline.
        let n = 1_200_000;
        for gzip in [false, true] {
            let bomb = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), gzip);
            let data = topics(vec![batch_of(1, 1), bomb]);
            assert!(declared_memory(&data, true) <= INLINE_DECODE_BYTES);
            let Ok(Decode::Done(DecodedTopics(_, refused, charge))) =
                decode_all_records(&data, true, false)
            else {
                panic!("decodes inline");
            };
            assert_eq!(
                refused[0].error_code, ERROR_MESSAGE_TOO_LARGE,
                "gzip {gzip}"
            );
            assert!(
                charge.bytes() < 1000,
                "the refused partition holds no charge"
            );
        }
    }

    /// A test pool holding a request's worth: what a refused partition charged must be
    /// given back to it too.
    static POOL: crate::kafka::protocol::decode_budget::DecodePool =
        crate::kafka::protocol::decode_budget::DecodePool::new(MAX_DECODED_REQUEST_BYTES);

    #[test]
    fn a_refused_partition_gives_back_what_it_charged() {
        use crate::kafka::constants::{ERROR_INVALID_RECORD, ERROR_INVALID_TIMESTAMP};
        use crate::kafka::protocol::recordbatch_test_batches::{raw_batch, tiny_records};
        // Against a 3 MB request limit: each partition before the last charges some 2 MB
        // and is refused, for its record count (10,000 declared over 150,000 records, found
        // once they decompressed), its LogAppendTime type (once its records are read) or
        // its size (4 MB of gzip-compressed zeros). The last needs some 1.9 MB, which it
        // has only if each refused partition gave its charge back.
        let limit = 3_000_000;
        let wrong_count = raw_batch(&tiny_records(150_000), 10_000, true);
        let log_append_time = batch_of_with(
            1_500,
            1000,
            Header {
                attributes: 0x08,
                max_timestamp: Some(-1),
                ..Header::default()
            },
        );
        let zeros = vec![0u8; 4_000_000];
        let too_large = raw_batch(
            &record(&[&[0], &v(0), &v(0), &v(-1), &v(4_000_000), &zeros, &v(0)]),
            1,
            true,
        );
        let last = batch_of(5_000, 200);
        let data = topics(vec![wrong_count, log_append_time, too_large, last]);
        let budget = DecodeBudget::with_pool(limit, &POOL);
        let DecodedTopics(topic_data, refused, charge) =
            decode_records(&data, true, budget, OverLimit::Refuse).unwrap();
        let refused: Vec<(i32, i16)> = refused
            .iter()
            .map(|r| (r.partition, r.error_code))
            .collect();
        assert_eq!(
            refused,
            vec![
                (0, ERROR_INVALID_RECORD),
                (1, ERROR_INVALID_TIMESTAMP),
                (2, ERROR_MESSAGE_TOO_LARGE)
            ]
        );
        assert_eq!(topic_data[0].partitions[0].partition_index, 3);
        assert_eq!(
            POOL.remaining(),
            MAX_DECODED_REQUEST_BYTES - charge.bytes(),
            "the pool holds the decoded partition's charge only"
        );
        drop(charge);
        assert_eq!(POOL.remaining(), MAX_DECODED_REQUEST_BYTES);

        // Inline, a partition past the limit fails the decode, to be retried under a
        // large-decode slot.
        let data = topics(vec![batch_of(5_000, 200), batch_of(5_000, 200)]);
        let budget = DecodeBudget::with_pool(limit, &POOL);
        assert_eq!(
            decode_records(&data, true, budget, OverLimit::Retry).unwrap_err(),
            DecodeFailure::TooLarge
        );
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
            zstd_allowed: true,
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
                refused: Vec::new(),
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

    /// A gzip batch of one record of `n` zeros with LogAppendTime and maxTimestamp -1: a
    /// few KB on the wire, it decodes `n` bytes and is then refused INVALID_TIMESTAMP.
    fn refused_after_decoding(n: usize) -> bytes::Bytes {
        use std::io::Write;
        let zeros = vec![0u8; n];
        let section = record(&[
            &[0],
            &v(0),
            &v(0),
            &v(-1),
            &v(i64::try_from(n).unwrap()),
            &zeros,
            &v(0),
        ]);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        gz.write_all(&section).unwrap();
        batch_with(
            &gz.finish().unwrap(),
            1,
            Header {
                attributes: 0x09,
                max_timestamp: Some(-1),
                ..Header::default()
            },
        )
    }

    #[test]
    fn refused_partitions_count_against_the_inline_decode_work() {
        use crate::kafka::constants::ERROR_INVALID_TIMESTAMP;
        // One partition decoding 1 MB and refused decodes inline.
        assert_eq!(
            outcome(vec![refused_after_decoding(1 << 20)], true),
            Ok((vec![], vec![(0, ERROR_INVALID_TIMESTAMP)]))
        );
        // Eight declare little but would decode 8 MB on the I/O thread: left to a slot.
        let data = topics(vec![refused_after_decoding(1 << 20); 8]);
        assert!(declared_memory(&data, true) <= INLINE_DECODE_BYTES);
        assert!(matches!(
            decode_all_records(&data, true, false),
            Ok(Decode::Pending(Wait::Slot))
        ));
    }

    #[test]
    fn refused_partitions_count_against_the_slot_decode_work() {
        use crate::kafka::constants::ERROR_INVALID_TIMESTAMP;
        // Against a 3 MB limit and 6 MB of work, partitions decoding 1 MB each and refused:
        // a few decode, the rest are refused without decoding (the valid last one too).
        let mut batches = vec![refused_after_decoding(1 << 20); 20];
        batches.push(batch_of(1, 1));
        let data = topics(batches);
        let budget = DecodeBudget::new(3_000_000).limit_work(6_000_000);
        let DecodedTopics(topic_data, refused, _) =
            decode_records(&data, true, budget, OverLimit::Refuse).unwrap();
        assert!(topic_data.is_empty());
        let codes: Vec<i16> = refused.iter().map(|r| r.error_code).collect();
        let decoded = codes
            .iter()
            .take_while(|&&c| c == ERROR_INVALID_TIMESTAMP)
            .count();
        assert!((1..=6).contains(&decoded), "{codes:?}");
        assert!(
            codes[decoded..]
                .iter()
                .all(|&c| c == ERROR_MESSAGE_TOO_LARGE),
            "{codes:?}"
        );
        assert_eq!(codes.len(), 21);
        // The slot's work limit, four times the request limit, leaves room for a partition
        // refused at the request limit besides partitions that fit.
        assert_eq!(SLOT_DECODE_WORK_BYTES, 4 * MAX_DECODED_REQUEST_BYTES);
        let limit = 3_000_000;
        let budget = DecodeBudget::new(limit).limit_work(4 * limit);
        let data = topics(vec![
            refused_after_decoding(limit + 1),
            refused_after_decoding(1_500_000),
            batch_of(1000, 1000),
        ]);
        let DecodedTopics(topic_data, refused, _) =
            decode_records(&data, true, budget, OverLimit::Refuse).unwrap();
        let refused: Vec<(i32, i16)> = refused
            .iter()
            .map(|r| (r.partition, r.error_code))
            .collect();
        assert_eq!(
            refused,
            vec![(0, ERROR_MESSAGE_TOO_LARGE), (1, ERROR_INVALID_TIMESTAMP)]
        );
        assert_eq!(topic_data[0].partitions[0].partition_index, 2);
    }

    /// Each topic's partitions and their data, in order.
    type Kept<'a> = Vec<(String, Vec<(i32, &'a [u8])>)>;

    #[test]
    fn only_the_last_entry_of_a_partition_is_kept() {
        let entry = |index, data: &'static [u8]| {
            let mut p = WirePartition::default();
            p.index = index;
            p.records = Some(bytes::Bytes::from_static(data));
            p
        };
        let topic = |name: &'static str, partitions| {
            let mut t = WireTopic::default();
            t.name = kafka_protocol::messages::TopicName(name.into());
            t.partition_data = partitions;
            t
        };
        let kept = last_entry_per_partition(vec![
            topic("a", vec![entry(0, b"1"), entry(1, b"2"), entry(0, b"3")]),
            topic("b", vec![entry(0, b"4")]),
            topic("a", vec![entry(1, b"5"), entry(2, b"6")]),
            topic("c", vec![]),
        ]);
        let got: Kept<'_> = kept
            .iter()
            .map(|t| {
                (
                    t.name.to_string(),
                    t.partition_data
                        .iter()
                        .map(|p| (p.index, &p.records.as_ref().unwrap()[..]))
                        .collect(),
                )
            })
            .collect();
        let want: Kept<'_> = vec![
            ("a".into(), vec![(0, b"3"), (1, b"5"), (2, b"6")]),
            ("b".into(), vec![(0, b"4")]),
            ("c".into(), vec![]),
        ];
        assert_eq!(got, want);
    }
}
