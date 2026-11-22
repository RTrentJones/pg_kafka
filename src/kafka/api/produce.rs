//! Produce (0).

use std::sync::Arc;

use crate::kafka::constants::{ERROR_MESSAGE_TOO_LARGE, ERROR_REQUEST_TIMED_OUT};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData as WirePartitionProduceData, ProduceRequest,
    TopicProduceData as WireTopicProduceData,
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
    ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED, ERROR_UNKNOWN_SERVER_ERROR,
};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::error::KafkaError;
use crate::kafka::handlers;
use crate::kafka::messages::{
    KafkaRequest, KafkaResponse, PartitionProduceData, ProducerMetadata, RefusedPartition,
    TopicPlaces, TopicProduceData,
};
use crate::kafka::notifications::InternalNotification;
use crate::kafka::protocol::decode_budget::{
    decode_wait, DecodeBudget, DecodedCharge, Exhausted, HeldFrame, NotRun, DECODED,
    INLINE_DECODE_BYTES, INLINE_DECODE_TIME, INLINE_DECODE_WORK_BYTES, LARGE_DECODES,
    MAX_DECODED_REQUEST_BYTES, SLOT_DECODE_TIME, SLOT_DECODE_WORK_BYTES,
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
    pub pending: Option<Box<PendingRecords>>,
}

/// The records of a Produce request that could not decode at once: it waits for a
/// large-decode slot or for decode budget, until `deadline` (see
/// `protocol::decode_budget`).
pub struct PendingRecords {
    topics: Arc<Vec<WireTopicProduceData>>,
    zstd_allowed: bool,
    /// The answers of entries refused besides the one kept for their partition
    /// ([`last_entry_per_partition`]).
    refused_entries: Vec<RefusedPartition>,
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
        let zstd_allowed = header.api_version >= MIN_ZSTD_VERSION;
        // Kafka 3.7 reads the first batches of the request as it was sent, before it keeps
        // one entry per partition.
        let first_batches = check_first_batches(&wire.topic_data, wire.transactional_id.is_some());
        let OneEntryPerPartition {
            topics,
            refused_entries,
        } = last_entry_per_partition(std::mem::take(&mut wire.topic_data), zstd_allowed);
        wire.topic_data = topics;
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
        if let Err(failure) = first_batches {
            return Err(rejection(failure, acks, requested));
        }

        // Kafka accepts only acks -1, 0 and 1; anything else is answered
        // INVALID_REQUIRED_ACKS and nothing is written, once the checks Kafka makes before
        // the log append passed (`invalid_acks_answers`).
        if !valid_acks(acks) {
            warn!("Rejecting Produce with invalid acks={acks}");
            let refused = with_refused_entries(
                invalid_acks_answers(&wire.topic_data, zstd_allowed),
                refused_entries,
            );
            return Ok(ProduceRequestData {
                acks,
                timeout_ms: wire.timeout_ms,
                topic_data: Vec::new(),
                refused,
                transactional_id: wire.transactional_id.map(|s| s.to_string()),
                decoded: DecodedCharge::default(),
                pending: None,
            });
        }

        let decode = decode_all_records(&wire.topic_data, zstd_allowed);
        let transactional_id = wire.transactional_id.map(|s| s.to_string());
        let (DecodedTopics(topic_data, refused, decoded), pending) = match decode {
            Ok(Decode::Done(DecodedTopics(topic_data, refused, decoded))) => (
                DecodedTopics(
                    topic_data,
                    with_refused_entries(refused, refused_entries),
                    decoded,
                ),
                None,
            ),
            Ok(Decode::Pending(wait)) => {
                let Some(frame) = HeldFrame::charge(record_bytes(&wire.topic_data)) else {
                    return Err(rejection(DecodeFailure::TimedOut, acks, requested));
                };
                let pending = PendingRecords {
                    topics: Arc::new(wire.topic_data),
                    zstd_allowed,
                    refused_entries,
                    requested,
                    wait,
                    deadline: tokio::time::Instant::now() + decode_wait(wire.timeout_ms),
                    _frame: frame,
                };
                (DecodedTopics::default(), Some(Box::new(pending)))
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
        if request.refused.is_empty() {
            return echo;
        }
        // Topic names are unique in `topic_data`; a refused partition joins its topic's
        // entry through a map, so a request of many topics echoes in linear time. A
        // partition both written and refused (for another of its entries) is echoed once.
        let mut topic_at = TopicPlaces::of(echo.iter().map(|(name, _)| name.as_str()));
        let mut echoed: std::collections::HashSet<(usize, i32)> = echo
            .iter()
            .enumerate()
            .flat_map(|(at, (_, partitions))| partitions.iter().map(move |&p| (at, p)))
            .collect();
        for refused in &request.refused {
            let at = topic_at.place(&refused.topic, || {
                echo.push((refused.topic.to_string(), Vec::new()));
                echo.len() - 1
            });
            if echoed.insert((at, refused.partition)) {
                echo[at].1.push(refused.partition);
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
            // No response. The records are checked as with acks >= 1 (an idempotent
            // producer's sequence, a transactional batch's transaction), as Kafka checks
            // them. A request-level failure rolls back like the acks >= 1 path.
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
            let producer_metadata = extract_producer_metadata(&request.topic_data);
            return match handlers::handle_produce(
                &env.ctx,
                request.topic_data,
                producer_metadata.as_ref(),
                request.transactional_id.as_deref(),
            ) {
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
    /// ([`INLINE_DECODE_BYTES`]), or the decode more work or time than an I/O thread may
    /// give it: the request decodes again under a large-decode slot, where a partition
    /// that does not fit is refused on its own.
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
    if budget.check_time().is_err() {
        // The request decoded as long as it may.
        return match over_limit {
            OverLimit::Retry => Err(DecodeFailure::TooLarge),
            OverLimit::Refuse => Ok(Err(ERROR_REQUEST_TIMED_OUT)),
        };
    }
    let mark = budget.charged();
    let e = match parse_record_batch_with_metadata(batch, budget) {
        Ok(parsed) => return Ok(Ok(parsed)),
        Err(e) => e,
    };
    let code = match budget.exhausted() {
        Some(Exhausted::Global) => return Err(DecodeFailure::Busy(budget.wanted())),
        Some(Exhausted::Limit | Exhausted::Work | Exhausted::Time)
            if over_limit == OverLimit::Retry =>
        {
            return Err(DecodeFailure::TooLarge)
        }
        Some(Exhausted::Limit | Exhausted::Work) => ERROR_MESSAGE_TOO_LARGE,
        Some(Exhausted::Time) => ERROR_REQUEST_TIMED_OUT,
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
        let mut refusals = Refusals::new(&name, &mut refused);
        let mut partitions = Vec::with_capacity(topic.partition_data.len());
        for partition in &topic.partition_data {
            // Null records: Kafka 3.7 fails the partition UNKNOWN_SERVER_ERROR (its append
            // has no batch to read) and writes the others. Refused here, it never reaches
            // the handler, so no producer state is taken from it.
            let Some(batch) = &partition.records else {
                refusals.push(partition.index, ERROR_UNKNOWN_SERVER_ERROR);
                continue;
            };
            let (records, producer_metadata) =
                match decode_partition(batch, zstd_allowed, &budget, over_limit)? {
                    Ok(ParsedRecordBatch {
                        records,
                        producer_metadata,
                    }) => (records, Some(producer_metadata)),
                    Err(error_code) => {
                        refusals.push_invalid(partition.index, error_code);
                        continue;
                    }
                };
            partitions.push(PartitionProduceData {
                partition_index: partition.index,
                records,
                producer_metadata,
            });
        }
        drop(refusals);
        if !partitions.is_empty() {
            topic_data.push(TopicProduceData { name, partitions });
        }
    }
    Ok(DecodedTopics(topic_data, refused, budget.into_charge()))
}

/// The refused partitions of one topic entry: they share one copy of its name, made at the
/// first refusal, and the refusals of records that did not decode are logged once for the
/// topic when it is done (a line per partition would write the name per partition).
struct Refusals<'a> {
    name: &'a str,
    shared: Option<Arc<str>>,
    refused: &'a mut Vec<RefusedPartition>,
    invalid: usize,
    first_invalid: Option<(i32, i16)>,
}

impl<'a> Refusals<'a> {
    fn new(name: &'a str, refused: &'a mut Vec<RefusedPartition>) -> Self {
        Self {
            name,
            shared: None,
            refused,
            invalid: 0,
            first_invalid: None,
        }
    }

    /// Refuse `partition` with `error_code`.
    fn push(&mut self, partition: i32, error_code: i16) {
        let topic = Arc::clone(self.shared.get_or_insert_with(|| Arc::from(self.name)));
        self.refused.push(RefusedPartition {
            topic,
            partition,
            error_code,
        });
    }

    /// Refuse `partition`, whose records did not decode, with `error_code`.
    fn push_invalid(&mut self, partition: i32, error_code: i16) {
        self.invalid += 1;
        self.first_invalid.get_or_insert((partition, error_code));
        self.push(partition, error_code);
    }
}

impl Drop for Refusals<'_> {
    fn drop(&mut self) {
        if let Some((partition, error_code)) = self.first_invalid {
            warn!(
                "Refusing the records of {} partition(s) of topic={} (partition={partition}: error {error_code})",
                self.invalid,
                log_name(self.name)
            );
        }
    }
}

/// The longest topic name a log line shows whole; a longer one (Kafka refuses topics of more
/// than 249 characters) is cut, so a line stays short whatever the request sent.
const LOGGED_NAME_CHARS: usize = 249;

/// `name` as a log line shows it: whole, or its first [`LOGGED_NAME_CHARS`] characters and
/// its length in bytes.
fn log_name(name: &str) -> std::borrow::Cow<'_, str> {
    match name.char_indices().nth(LOGGED_NAME_CHARS) {
        None => name.into(),
        Some((cut, _)) => format!("{}... ({} bytes)", &name[..cut], name.len()).into(),
    }
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
                        log_name(topic.name.0.as_str()),
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

/// Add the answers of refused partitions to `response`, under their topics, in time linear
/// in the partitions of both. A refused partition that was also written (its kept entry,
/// refused for another of its entries; [`last_entry_per_partition`]) is answered with its
/// refusal instead, as Kafka 3.7 answers it.
fn merge_topic_responses(response: &mut ProduceResponse, refused: Vec<TopicProduceResponse>) {
    if refused.is_empty() {
        return;
    }
    let mut topic_at: std::collections::HashMap<kafka_protocol::messages::TopicName, usize> =
        response
            .responses
            .iter()
            .enumerate()
            .map(|(at, t)| (t.name.clone(), at))
            .collect();
    let mut answered: std::collections::HashMap<(usize, i32), usize> = response
        .responses
        .iter()
        .enumerate()
        .flat_map(|(at, t)| {
            t.partition_responses
                .iter()
                .enumerate()
                .map(move |(i, p)| ((at, p.index), i))
        })
        .collect();
    for topic in refused {
        let at = match topic_at.entry(topic.name.clone()) {
            std::collections::hash_map::Entry::Occupied(at) => *at.get(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(response.responses.len());
                let mut empty = TopicProduceResponse::default();
                empty.name = topic.name;
                response.responses.push(empty);
                response.responses.len() - 1
            }
        };
        let partitions = &mut response.responses[at].partition_responses;
        for partition in topic.partition_responses {
            match answered.entry((at, partition.index)) {
                std::collections::hash_map::Entry::Occupied(i) => {
                    partitions[*i.get()] = partition;
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(partitions.len());
                    partitions.push(partition);
                }
            }
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

/// The budget of a decode on an I/O thread: at most [`INLINE_DECODE_BYTES`] held,
/// [`INLINE_DECODE_WORK_BYTES`] decoded and [`INLINE_DECODE_TIME`] spent, so that neither
/// partitions refused after they decode nor input that decodes to little can keep the
/// thread decoding (past any of them the request decodes again under a large-decode slot).
fn inline_budget() -> DecodeBudget {
    DecodeBudget::new(INLINE_DECODE_BYTES)
        .limit_work(INLINE_DECODE_WORK_BYTES)
        .limit_time(INLINE_DECODE_TIME)
}

/// The entries of a Produce request, one per partition ([`last_entry_per_partition`]).
#[derive(Debug, Default)]
struct OneEntryPerPartition {
    /// The kept entry of each partition, its topic entries merged into the first.
    topics: Vec<WireTopicProduceData>,
    /// The partitions whose kept entry is answered with the error of another entry of
    /// theirs, which `ProduceRequest.validateRecords` refuses.
    refused_entries: Vec<RefusedPartition>,
}

/// One entry per partition, as Kafka 3.7 keeps them: `KafkaApis.handleProduceRequest`
/// walks the entries in order, keeping the records of each partition in one map (a later
/// entry replaces an earlier one, in its topic entry or in another of the same name) and,
/// in another, the error of an entry `ProduceRequest.validateRecords` refuses (not one
/// batch of magic 2, or zstd before v7; [`validate_produce_records`]). The records kept
/// are written, and a partition with such an error is answered with it, whatever its
/// written records were answered. So the entry kept is the last one validation accepts
/// (records or none), or failing that the last one, which is refused as it decodes; when
/// an entry validation accepts is kept, the last refused entry's error is its answer
/// (`refused_entries`). Topic entries of one name merge into the first, a partition at
/// the place of its first entry; a topic left with no partition entry is dropped.
///
/// A request of distinct partitions in increasing order per topic, what clients send, is
/// kept as it is. Otherwise what this holds besides the request is some 30 bytes per
/// topic entry and 12 per partition entry of a topic that repeats or reorders its
/// partitions, less than the decoded entries themselves take.
fn last_entry_per_partition(
    mut topics: Vec<WireTopicProduceData>,
    zstd_allowed: bool,
) -> OneEntryPerPartition {
    // Where each topic entry goes: the first entry of its name.
    let first_of: Vec<u32> = {
        let mut at: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
        topics
            .iter()
            .enumerate()
            .map(|(i, t)| *at.entry(t.name.0.as_str()).or_insert(position(i)))
            .collect()
    };
    let mut merged_away = false;
    for (i, &first) in first_of.iter().enumerate() {
        let first = at(first);
        if first != i {
            let partitions = std::mem::take(&mut topics[i].partition_data);
            topics[first].partition_data.extend(partitions);
            merged_away = true;
        }
    }
    if merged_away {
        let mut i = 0;
        topics.retain(|_| {
            i += 1;
            at(first_of[i - 1]) == i - 1
        });
    }
    drop(first_of);
    // A topic none of whose entries names a partition is not answered at all: Kafka 3.7
    // answers each partition it was sent and nothing else, so it neither answers nor
    // auto-creates such a topic, whether the request is written, refused (invalid acks,
    // a transactional request) or sent with acks=0.
    topics.retain(|t| !t.partition_data.is_empty());
    let mut refused_entries = Vec::new();
    for topic in &mut topics {
        let name = topic.name.0.as_str();
        let partitions = std::mem::take(&mut topic.partition_data);
        let mut refusals = Refusals::new(name, &mut refused_entries);
        topic.partition_data =
            one_entry_per_index(partitions, zstd_allowed, |index, error_code| {
                refusals.push(index, error_code);
            });
    }
    OneEntryPerPartition {
        topics,
        refused_entries,
    }
}

/// The place of entry `at` of a request as a `u32`: a frame holds fewer than 2^32 entries.
fn position(at: usize) -> u32 {
    u32::try_from(at).unwrap_or(u32::MAX)
}

/// The entry at a [`position`].
fn at(position: u32) -> usize {
    usize::try_from(position).unwrap_or(usize::MAX)
}

/// The entries of one topic, one per partition index, chosen as
/// [`last_entry_per_partition`] chooses them; `refused` gets each kept entry's index and
/// the error that answers it instead.
fn one_entry_per_index(
    mut entries: Vec<WirePartitionProduceData>,
    zstd_allowed: bool,
    mut refused: impl FnMut(i32, i16),
) -> Vec<WirePartitionProduceData> {
    if entries.windows(2).all(|w| w[0].index < w[1].index) {
        return entries;
    }
    let mut by_index: Vec<(i32, u32)> = entries
        .iter()
        .enumerate()
        .map(|(i, p)| (p.index, position(i)))
        .collect();
    by_index.sort_unstable();
    // `source[i]`: the entry that takes the place of entry `i`, none when it goes.
    let mut source = vec![u32::MAX; entries.len()];
    for group in by_index.chunk_by(|a, b| a.0 == b.0) {
        // An entry without records passes this check, as in Kafka: it takes part in
        // "the last entry wins" and is refused only as it decodes (`decode_records`).
        let refusal = |&(_, p): &(i32, u32)| {
            entries[at(p)]
                .records
                .as_ref()
                .and_then(|batch| validate_produce_records(batch, zstd_allowed).err())
        };
        let (index, first) = group[0];
        let last = group[group.len() - 1].1;
        let accepted = || group.iter().rev().find(|e| refusal(e).is_none());
        let kept = match (group.len() > 1).then(accepted).flatten() {
            Some(&(_, kept)) => {
                if let Some(code) = group.iter().rev().find_map(refusal) {
                    refused(index, code);
                }
                kept
            }
            None => last,
        };
        source[at(first)] = kept;
    }
    drop(by_index);
    // In place: an entry moves to the place of its partition's first entry, at or before
    // its own, and no later partition's entry is at or before that place.
    let mut kept = 0;
    for &from in &source {
        if from == u32::MAX {
            continue;
        }
        entries[kept] = std::mem::take(&mut entries[at(from)]);
        kept += 1;
    }
    entries.truncate(kept);
    entries
}

/// `refused` with the answers of `entries` (partitions refused for another of their
/// entries, [`last_entry_per_partition`]), which take the place of their own answer.
fn with_refused_entries(
    mut refused: Vec<RefusedPartition>,
    entries: Vec<RefusedPartition>,
) -> Vec<RefusedPartition> {
    if entries.is_empty() {
        return refused;
    }
    // Partitions are keyed by their topic's place, so a name is hashed once per topic
    // ([`TopicPlaces`]), not once per partition.
    let mut topics = TopicPlaces::default();
    let mut placed = 0;
    let mut place = |topic: &Arc<str>| {
        topics.place(topic, || {
            placed += 1;
            placed - 1
        })
    };
    let mut at: std::collections::HashMap<(usize, i32), usize> = refused
        .iter()
        .enumerate()
        .map(|(i, r)| ((place(&r.topic), r.partition), i))
        .collect();
    for entry in entries {
        match at.entry((place(&entry.topic), entry.partition)) {
            std::collections::hash_map::Entry::Occupied(i) => {
                refused[*i.get()].error_code = entry.error_code;
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(refused.len());
                refused.push(entry);
            }
        }
    }
    refused
}

/// The answers of a Produce whose `acks` Kafka refuses, in Kafka 3.7's order: what it reads
/// of the request first ([`check_first_batches`], run before this on the request as sent)
/// fails the whole request; then a partition
/// whose data `ProduceRequest.validateRecords` refuses gets that error
/// ([`validate_produce_records`]); every other partition is INVALID_REQUIRED_ACKS
/// (`ReplicaManager.appendRecords` checks acks before the log append, so records are not
/// decoded). Partitions of unknown topics are answered UNKNOWN_TOPIC_OR_PARTITION once
/// looked up, as every refused partition is.
fn invalid_acks_answers(
    topics: &[WireTopicProduceData],
    zstd_allowed: bool,
) -> Vec<RefusedPartition> {
    topics
        .iter()
        .flat_map(|topic| {
            let name: Arc<str> = Arc::from(topic.name.0.as_str());
            topic
                .partition_data
                .iter()
                .map(move |partition| RefusedPartition {
                    topic: Arc::clone(&name),
                    partition: partition.index,
                    error_code: partition
                        .records
                        .as_ref()
                        .and_then(|batch| validate_produce_records(batch, zstd_allowed).err())
                        .unwrap_or(ERROR_INVALID_REQUIRED_ACKS),
                })
        })
        .collect()
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
) -> Result<Decode, DecodeFailure> {
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
                                .limit_work(SLOT_DECODE_WORK_BYTES)
                                .limit_time(SLOT_DECODE_TIME),
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
            produce.refused = with_refused_entries(refused, pending.refused_entries);
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
        let data = topics(data);
        check_first_batches(&data, has_transactional_id)?;
        let Decode::Done(DecodedTopics(topic_data, refused, _)) =
            decode_all_records(&data, zstd_allowed)?
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
            decode_all_records(&topics(vec![bad]), true)
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
                topic: "a".into(),
                partition: 1,
                error_code: 2,
            },
            RefusedPartition {
                topic: "b".into(),
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

    /// Echoing a request and merging its refused partitions' answers take time linear in
    /// its topics: both run on the DB thread for every Produce, and a request may name
    /// tens of thousands of topics of one refused partition each.
    #[test]
    fn refused_partitions_merge_and_echo_in_linear_time() {
        let name = |i: usize| format!("q{i:07}");
        let topics = |n: usize| -> Vec<TopicProduceResponse> {
            (0..n)
                .map(|i| {
                    let mut topic = TopicProduceResponse::default();
                    topic.name = kafka_protocol::messages::TopicName(name(i).into());
                    topic.partition_responses = vec![PartitionProduceResponse::default()];
                    topic
                })
                .collect()
        };
        let growth = crate::testing::growth(5_000, |n| {
            let mut response = ProduceResponse::default();
            response.responses = topics(n);
            merge_topic_responses(&mut response, topics(2 * n).split_off(n / 2));
            assert_eq!(response.responses.len(), 2 * n);
        });
        assert!(
            growth < 8.0,
            "merge: 4x the topics took {growth:.1}x the time"
        );

        let (request, _rx) = pending_request(1, Wait::Slot, std::time::Duration::from_secs(1));
        let RequestBody::Produce(mut produce) = request.body else {
            panic!("a Produce request");
        };
        let growth = crate::testing::growth(5_000, |n| {
            produce.topic_data = (0..n)
                .map(|i| TopicProduceData {
                    name: name(i),
                    partitions: Vec::new(),
                })
                .collect();
            produce.refused = (n / 2..2 * n)
                .map(|i| RefusedPartition {
                    topic: name(i).into(),
                    partition: 0,
                    error_code: 2,
                })
                .collect();
            assert_eq!(ProduceApi::echo(&produce).len(), 2 * n);
        });
        assert!(
            growth < 8.0,
            "echo: 4x the topics took {growth:.1}x the time"
        );
    }

    /// Input that decodes to nothing (1.7 MB of empty gzip blocks, some 0.35 s to decode)
    /// leaves an I/O thread after the inline decode time and decodes again under a slot,
    /// where the decode time refuses it and the partitions after it REQUEST_TIMED_OUT.
    #[test]
    fn input_that_decodes_to_nothing_is_bounded_by_the_decode_time() {
        use crate::kafka::protocol::recordbatch_test_batches::gzip_empty_blocks_batch;
        let batch = gzip_empty_blocks_batch(150_000);
        let data = topics(vec![batch.clone(), batch.clone(), batch]);
        let (decode, spent) = crate::testing::cpu_time(|| decode_all_records(&data, true));
        assert!(
            matches!(decode, Ok(Decode::Pending(Wait::Slot))),
            "{decode:?}"
        );
        assert!(
            spent < INLINE_DECODE_TIME + std::time::Duration::from_millis(80),
            "{spent:?} on the I/O thread"
        );

        let limit = std::time::Duration::from_millis(30);
        let slot = DecodeBudget::new(MAX_DECODED_REQUEST_BYTES)
            .limit_work(SLOT_DECODE_WORK_BYTES)
            .limit_time(limit);
        let (decoded, spent) =
            crate::testing::cpu_time(|| decode_records(&data, true, slot, OverLimit::Refuse));
        let DecodedTopics(topic_data, refused, _) = decoded.unwrap();
        assert!(topic_data.is_empty());
        let codes: Vec<(i32, i16)> = refused
            .iter()
            .map(|r| (r.partition, r.error_code))
            .collect();
        assert_eq!(
            codes,
            vec![
                (0, ERROR_REQUEST_TIMED_OUT),
                (1, ERROR_REQUEST_TIMED_OUT),
                (2, ERROR_REQUEST_TIMED_OUT)
            ]
        );
        assert!(
            spent < limit + std::time::Duration::from_millis(80),
            "{spent:?} in the slot"
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
            decode_all_records(&small, true),
            Ok(Decode::Done(_))
        ));
        // 3000 records of 1000 bytes declare more than the inline limit.
        let large = topics(vec![batch_of(3000, 1000)]);
        assert!(declared_memory(&large, true) > INLINE_DECODE_BYTES);
        assert!(matches!(
            decode_all_records(&large, true),
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
                decode_all_records(&data, true)
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
            refused_entries: Vec::new(),
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
                pending: Some(Box::new(pending)),
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
            decode_all_records(&data, true),
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
        let kept = last_entry_per_partition(
            vec![
                topic("a", vec![entry(0, b"1"), entry(1, b"2"), entry(0, b"3")]),
                topic("b", vec![entry(0, b"4")]),
                topic("a", vec![entry(1, b"5"), entry(2, b"6")]),
                topic("c", vec![]),
            ],
            true,
        );
        let got: Kept<'_> = kept
            .topics
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
        ];
        assert_eq!(got, want);
    }

    /// The partitions a decode keeps with their first record's value, and the refused ones
    /// with their codes.
    type Outcomes = (Vec<(i32, Vec<u8>)>, Vec<(i32, i16)>);

    /// What `ProduceApi::decode` makes of a request of `entries` (partition, batch) of one
    /// topic: the partitions kept with their first record's value, and the refused ones
    /// with their codes; or the code that refuses the whole request.
    fn decoded(
        entries: &[(i32, bytes::Bytes)],
        acks: i16,
        transactional_id: Option<&str>,
    ) -> Result<Outcomes, i16> {
        let mut topic = WireTopic::default();
        topic.name = kafka_protocol::messages::TopicName("t".into());
        topic.partition_data = entries
            .iter()
            .map(|(index, batch)| {
                let mut p = WirePartition::default();
                p.index = *index;
                p.records = Some(batch.clone());
                p
            })
            .collect();
        let mut wire = ProduceRequest::default();
        wire.acks = acks;
        wire.timeout_ms = 1000;
        wire.transactional_id = transactional_id
            .map(|id| kafka_protocol::messages::TransactionalId(id.to_string().into()));
        wire.topic_data = vec![topic];
        let header = RequestHeader {
            api_key: 0,
            api_version: 7,
            correlation_id: 1,
            client_id: None,
        };
        match ProduceApi::decode(wire, &header) {
            Ok(request) => Ok((
                request
                    .topic_data
                    .iter()
                    .flat_map(|t| t.partitions.iter())
                    .map(|p| {
                        let value = p.records[0].value.clone().unwrap_or_default();
                        (p.partition_index, value)
                    })
                    .collect(),
                request
                    .refused
                    .iter()
                    .map(|r| (r.partition, r.error_code))
                    .collect(),
            )),
            Err(Rejection::Reply { error_code, .. }) => Err(error_code),
            Err(Rejection::Close(reason)) => panic!("closed: {reason}"),
            Err(Rejection::Silent) => panic!("not answered"),
        }
    }

    /// A one-record batch of `value`.
    fn value_batch(value: &[u8]) -> bytes::Bytes {
        let len = v(i64::try_from(value.len()).unwrap());
        batch_with(
            &record(&[&[0], &v(0), &v(0), &v(-1), &len, value, &v(0)]),
            1,
            Header::default(),
        )
    }

    /// Kafka 3.7 reads the first batch of every entry of the request, as sent, before it
    /// keeps one entry per partition: an entry later replaced still fails the request.
    #[test]
    fn first_batches_are_read_before_repeated_entries_collapse() {
        let (a, b) = (value_batch(b"a"), value_batch(b"b"));
        let mut short_length = a.to_vec();
        short_length[8..12].copy_from_slice(&13i32.to_be_bytes());
        let short_length = bytes::Bytes::from(short_length);
        for acks in [-1, 5] {
            assert_eq!(
                decoded(
                    &[(0, transactional_batch()), (0, a.clone()), (1, b.clone())],
                    acks,
                    None
                ),
                Err(ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
            );
            assert_eq!(
                decoded(
                    &[(0, short_length.clone()), (0, a.clone()), (1, b.clone())],
                    acks,
                    None
                ),
                Err(ERROR_CORRUPT_MESSAGE)
            );
        }
        // The same entries with a transactional id read on to the kept ones.
        assert_eq!(
            decoded(
                &[(0, transactional_batch()), (0, a.clone()), (1, b.clone())],
                -1,
                Some("x")
            ),
            Ok((vec![(0, b"a".to_vec()), (1, b"b".to_vec())], vec![]))
        );
    }

    /// Of a partition's entries Kafka 3.7 writes the last one `validateRecords` accepts and
    /// answers the last one it refuses, when one is: a valid entry and one of two batches,
    /// in either order, write the valid one and answer INVALID_RECORD; the last of two
    /// valid ones is written; a corrupt last entry (refused at the log append) replaces a
    /// valid one.
    #[test]
    fn repeated_entries_write_the_last_valid_one_and_answer_the_last_invalid_one() {
        use crate::kafka::constants::{ERROR_INVALID_RECORD, ERROR_INVALID_REQUIRED_ACKS};
        let (a, b) = (value_batch(b"a"), value_batch(b"b"));
        let two = |x: &bytes::Bytes| bytes::Bytes::from([&x[..], &x[..]].concat());
        let mut corrupt = b.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        let corrupt = bytes::Bytes::from(corrupt);
        let written = |v: &[u8]| vec![(0, v.to_vec())];
        assert_eq!(
            decoded(&[(0, a.clone()), (0, two(&b))], -1, None),
            Ok((written(b"a"), vec![(0, ERROR_INVALID_RECORD)]))
        );
        assert_eq!(
            decoded(&[(0, two(&a)), (0, b.clone())], -1, None),
            Ok((written(b"b"), vec![(0, ERROR_INVALID_RECORD)]))
        );
        assert_eq!(
            decoded(&[(0, a.clone()), (0, b.clone())], -1, None),
            Ok((written(b"b"), vec![]))
        );
        assert_eq!(
            decoded(&[(0, a.clone()), (0, corrupt.clone())], -1, None),
            Ok((vec![], vec![(0, ERROR_CORRUPT_MESSAGE)]))
        );
        // Kept entry refused as it decodes and another refused by validation: the
        // validation error answers.
        assert_eq!(
            decoded(&[(0, two(&a)), (0, corrupt)], -1, None),
            Ok((vec![], vec![(0, ERROR_INVALID_RECORD)]))
        );
        // Only invalid entries: the last one is refused with its error.
        assert_eq!(
            decoded(&[(1, two(&a)), (0, a.clone()), (1, two(&b))], -1, None),
            Ok((vec![(0, b"a".to_vec())], vec![(1, ERROR_INVALID_RECORD)]))
        );
        // With invalid acks nothing is written, and the invalid entry answers.
        assert_eq!(
            decoded(&[(0, a.clone()), (0, two(&b)), (1, b.clone())], 5, None),
            Ok((
                vec![],
                vec![(0, ERROR_INVALID_RECORD), (1, ERROR_INVALID_REQUIRED_ACKS)]
            ))
        );
    }

    /// The written answer of a partition refused for another of its entries is replaced
    /// by its refusal, and the partition is echoed once.
    #[test]
    fn a_refused_entry_replaces_its_partitions_written_answer() {
        let answered = |index: i32, error_code: i16| {
            let mut p = PartitionProduceResponse::default();
            p.index = index;
            p.error_code = error_code;
            p
        };
        let topic = |partitions: Vec<PartitionProduceResponse>| {
            let mut t = TopicProduceResponse::default();
            t.name = kafka_protocol::messages::TopicName("t".into());
            t.partition_responses = partitions;
            t
        };
        let mut response = ProduceResponse::default();
        response.responses = vec![topic(vec![answered(0, 0), answered(1, 0)])];
        merge_topic_responses(&mut response, vec![topic(vec![answered(1, 87)])]);
        let got: Vec<(i32, i16)> = response.responses[0]
            .partition_responses
            .iter()
            .map(|p| (p.index, p.error_code))
            .collect();
        assert_eq!(got, vec![(0, 0), (1, 87)]);

        let (request, _rx) = pending_request(1, Wait::Slot, std::time::Duration::from_secs(1));
        let RequestBody::Produce(mut produce) = request.body else {
            panic!("a Produce request");
        };
        produce.topic_data = vec![TopicProduceData {
            name: "t".to_string(),
            partitions: vec![PartitionProduceData {
                partition_index: 1,
                records: Vec::new(),
                producer_metadata: None,
            }],
        }];
        produce.refused = vec![RefusedPartition {
            topic: "t".into(),
            partition: 1,
            error_code: 87,
        }];
        assert_eq!(ProduceApi::echo(&produce), vec![("t".to_string(), vec![1])]);
    }

    /// Collapsing repeated entries holds little besides the request: nothing for a
    /// request of distinct partitions in order, some 12 bytes per entry otherwise.
    #[test]
    fn one_entry_per_partition_holds_little_memory() {
        let entries = |order: &dyn Fn(i32) -> i32| -> Vec<WireTopic> {
            let mut topic = WireTopic::default();
            topic.name = kafka_protocol::messages::TopicName("t".into());
            topic.partition_data = (0..1_000_000)
                .map(|i| {
                    let mut p = WirePartition::default();
                    p.index = order(i);
                    p
                })
                .collect();
            vec![topic]
        };
        for (order, most) in [
            (&(|i| i) as &dyn Fn(i32) -> i32, 1024),
            (&|i| -i, 13_000_000),
            (&|i| i % 1000, 13_000_000),
        ] {
            let topics = entries(order);
            let (kept, peak) = crate::testing::heap_peak(|| last_entry_per_partition(topics, true));
            assert!(peak <= most, "held {peak} bytes besides the request");
            let n = kept.topics[0].partition_data.len();
            assert!(n == 1_000_000 || n == 1000, "{n}");
        }
    }

    #[test]
    fn a_repeated_entry_without_records_takes_part_in_last_entry_wins() {
        // Kafka 3.7 does not refuse a null entry in validateRecords: the last entry wins,
        // and a null last entry fails as it is appended (nothing written for the
        // partition).
        let a = value_batch(b"a");
        let mut data = topics(vec![batch_of(1, 1)]);
        let entry = |records: Option<bytes::Bytes>| {
            WirePartitionProduceData::default()
                .with_index(0)
                .with_records(records)
        };
        let kept = |entries: Vec<WirePartitionProduceData>| {
            let mut refused = Vec::new();
            let kept = one_entry_per_index(entries, true, |i, code| refused.push((i, code)));
            let kept: Vec<Option<bytes::Bytes>> = kept.into_iter().map(|e| e.records).collect();
            (kept, refused)
        };
        assert_eq!(
            kept(vec![entry(Some(a.clone())), entry(None)]),
            (vec![None], vec![])
        );
        assert_eq!(
            kept(vec![entry(None), entry(Some(a.clone()))]),
            (vec![Some(a)], vec![])
        );
        // The kept null entry is refused -1 when the records decode, and nothing of the
        // partition is written.
        data[0].partition_data[0].records = None;
        let DecodedTopics(decoded, refused, _) =
            decode_records(&data, true, inline_budget(), OverLimit::Refuse).unwrap();
        assert!(decoded.iter().all(|t| t.partitions.is_empty()));
        let refused: Vec<(i32, i16)> = refused
            .iter()
            .map(|r| (r.partition, r.error_code))
            .collect();
        assert_eq!(refused, vec![(0, ERROR_UNKNOWN_SERVER_ERROR)]);
    }

    /// Kafka 3.7 answers a Produce topic entry that names no partition with no entry at
    /// all and does not create the topic (checked against a 3.7 broker, for acks -1, 0
    /// and 5 and a transactional request): it is gone once decoded, so neither the
    /// handler (which would auto-create it), the invalid-acks answer nor the echo of an
    /// error response sees it. A name repeated with a partition elsewhere is answered for
    /// that partition.
    #[test]
    fn a_topic_entry_without_partitions_is_dropped() {
        let named = |name: &str, batches: Vec<bytes::Bytes>| {
            let mut topic = topics(batches).remove(0);
            topic.name = kafka_protocol::messages::TopicName(name.to_string().into());
            topic
        };
        let header = RequestHeader {
            api_key: 0,
            api_version: 7,
            correlation_id: 1,
            client_id: None,
        };
        for (acks, transactional_id) in [(-1, None), (0, None), (5, None), (1, Some("tx"))] {
            let mut wire = ProduceRequest::default();
            wire.acks = acks;
            wire.timeout_ms = 1000;
            wire.transactional_id = transactional_id
                .map(|id: &str| kafka_protocol::messages::TransactionalId(id.into()));
            wire.topic_data = vec![
                named("zp1", vec![]),
                named("t", vec![]),
                named("zp2", vec![]),
                named("t", vec![value_batch(b"a")]),
                named("zp1", vec![]),
            ];
            let request = ProduceApi::decode(wire, &header).expect("decodes");
            let mut answered: Vec<String> = ProduceApi::echo(&request)
                .into_iter()
                .map(|(name, partitions)| format!("{name}{partitions:?}"))
                .collect();
            answered.extend(request.topic_data.iter().map(|t| t.name.clone()));
            answered.extend(request.refused.iter().map(|r| r.topic.to_string()));
            assert!(
                answered.iter().all(|a| a.starts_with('t')),
                "acks={acks}: {answered:?}"
            );
            assert_eq!(answered[0], "t[0]", "acks={acks}");
        }

        // Only entries without partitions: nothing to answer, nothing to create.
        let mut wire = ProduceRequest::default();
        wire.acks = -1;
        wire.topic_data = vec![named("zp1", vec![]), named("zp2", vec![])];
        let request = ProduceApi::decode(wire, &header).expect("decodes");
        assert!(request.topic_data.is_empty() && request.refused.is_empty());
        assert_eq!(ProduceApi::echo(&request), TopicPartitions::new());
    }

    #[test]
    fn a_partition_with_null_records_is_refused() {
        let mut data = topics(vec![batch_of(1, 1), batch_of(1, 1)]);
        data[0].partition_data[1].records = None;
        let DecodedTopics(decoded, refused, _) =
            decode_records(&data, true, inline_budget(), OverLimit::Refuse).unwrap();
        let written: Vec<(i32, usize, bool)> = decoded
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| {
                (
                    p.partition_index,
                    p.records.len(),
                    p.producer_metadata.is_some(),
                )
            })
            .collect();
        assert_eq!(written, vec![(0, 1, true)]);
        let refused: Vec<(&str, i32, i16)> = refused
            .iter()
            .map(|r| (&*r.topic, r.partition, r.error_code))
            .collect();
        assert_eq!(refused, vec![("t", 1, ERROR_UNKNOWN_SERVER_ERROR)]);

        // A topic whose only partition is null is left out (not auto-created).
        data[0].partition_data.remove(0);
        let DecodedTopics(decoded, refused, _) =
            decode_records(&data, true, inline_budget(), OverLimit::Refuse).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(refused.len(), 1);
    }

    #[test]
    fn invalid_acks_are_answered_after_kafkas_earlier_checks() {
        use crate::kafka::constants::{ERROR_INVALID_RECORD, ERROR_INVALID_REQUIRED_ACKS};
        let good = batch_of(1, 1);
        let mut bad_crc = good.to_vec();
        *bad_crc.last_mut().unwrap() ^= 1;
        let two = bytes::Bytes::from([&good[..], &good[..]].concat());
        let answers = invalid_acks_answers(&topics(vec![bad_crc.into(), two, good.clone()]), true);
        let codes: Vec<(i32, i16)> = answers
            .iter()
            .map(|r| (r.partition, r.error_code))
            .collect();
        assert_eq!(
            codes,
            vec![
                (0, ERROR_INVALID_REQUIRED_ACKS),
                (1, ERROR_INVALID_RECORD),
                (2, ERROR_INVALID_REQUIRED_ACKS)
            ]
        );
        let mut short_length = good.to_vec();
        short_length[8..12].copy_from_slice(&13i32.to_be_bytes());
        assert_eq!(
            check_first_batches(&topics(vec![short_length.into()]), false).unwrap_err(),
            DecodeFailure::Rejected(ERROR_CORRUPT_MESSAGE)
        );
        assert_eq!(
            check_first_batches(&topics(vec![good, transactional_batch()]), false).unwrap_err(),
            DecodeFailure::Rejected(ERROR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        );
    }

    /// A Produce of one long topic name (32,000 bytes, as long as a frame allows) and many
    /// partition entries of 8 bytes each: decoding it, echoing it and answering its refused
    /// partitions hold the name once per topic, not once per partition (5,000 copies would
    /// be 160 MB), whichever way the partitions are refused.
    #[test]
    fn refused_partitions_hold_their_topic_name_once() {
        use crate::testing::heap_peak;
        /// An entry's partition index and whether it has records, by its place.
        type Entry = fn(i32) -> (i32, bool);
        const NAME_BYTES: usize = 32_000;
        const ENTRIES: i32 = 5_000;
        let one_byte = bytes::Bytes::from_static(&[0]);
        let shapes: [(&str, i16, Entry); 4] = [
            ("null records", 1, |i| (i, false)),
            ("records that do not decode", 1, |i| (i, true)),
            ("invalid acks", 5, |i| (i, true)),
            // Two entries per partition, the earlier one refused: answered for the later.
            ("repeated entries", 1, |i| (i / 2, i % 2 == 0)),
        ];
        for (shape, acks, entry) in shapes {
            let mut topic = WireTopic::default();
            topic.name = kafka_protocol::messages::TopicName("n".repeat(NAME_BYTES).into());
            topic.partition_data = (0..ENTRIES)
                .map(|i| {
                    let (index, records) = entry(i);
                    let mut p = WirePartition::default();
                    p.index = index;
                    p.records = records.then(|| one_byte.clone());
                    p
                })
                .collect();
            let mut wire = ProduceRequest::default();
            wire.acks = acks;
            wire.timeout_ms = 30_000;
            wire.topic_data = vec![topic];
            let header = RequestHeader {
                api_key: 0,
                api_version: 7,
                correlation_id: 1,
                client_id: None,
            };
            let mut store = crate::testing::mocks::MockKafkaStore::new();
            store
                .expect_get_topic_metadata()
                .returning(|_| Ok(Vec::new()));
            let (answers, peak) = heap_peak(|| {
                let request = ProduceApi::decode(wire, &header).expect("decodes");
                let echo = ProduceApi::echo(&request);
                let answered = handlers::answer_refused_partitions(&store, &request.refused);
                (
                    request.refused.len(),
                    echo.len(),
                    answered.map(|a| a.len()).ok(),
                )
            });
            let expected =
                usize::try_from(ENTRIES / if shape == "repeated entries" { 2 } else { 1 }).unwrap();
            assert_eq!(answers, (expected, 1, Some(1)), "{shape}");
            assert!(peak < 2 << 20, "{shape}: decoding held {peak} bytes");
        }
    }

    /// Decoding and echoing a request of many refused partitions of one long topic name
    /// takes about as long as of a short one: the name is hashed and copied once per topic,
    /// not once per partition (which would take some 200 MB of hashing here).
    #[test]
    fn refused_partitions_hash_their_topic_name_once() {
        let request = |name_bytes: usize| {
            let mut topic = WireTopic::default();
            topic.name = kafka_protocol::messages::TopicName("n".repeat(name_bytes).into());
            topic.partition_data = (0..12_000)
                .map(|i| {
                    let mut p = WirePartition::default();
                    // Two entries per partition, the first refused (`with_refused_entries`).
                    p.index = i / 2;
                    p.records = (i % 2 == 0).then(|| bytes::Bytes::from_static(&[0]));
                    p
                })
                .collect();
            let mut wire = ProduceRequest::default();
            wire.acks = 1;
            wire.topic_data = vec![topic];
            wire
        };
        let header = RequestHeader {
            api_key: 0,
            api_version: 7,
            correlation_id: 1,
            client_id: None,
        };
        let time = |name_bytes: usize| {
            let wire = request(name_bytes);
            (0..3)
                .map(|_| {
                    crate::testing::cpu_time(|| {
                        for _ in 0..5 {
                            let request =
                                ProduceApi::decode(wire.clone(), &header).expect("decodes");
                            assert_eq!(ProduceApi::echo(&request)[0].1.len(), 6_000);
                        }
                    })
                    .1
                })
                .min()
                .unwrap_or_default()
                .max(std::time::Duration::from_micros(1))
        };
        let short = time(1);
        let long = time(32_000);
        assert!(
            long < 4 * short,
            "a 32,000-byte name took {long:?}, a 1-byte one {short:?}"
        );
    }
}
