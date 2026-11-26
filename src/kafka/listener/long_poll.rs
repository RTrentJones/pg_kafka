//! Fetch long polling: answer a Fetch once it has `min_bytes`, a partition error, or its
//! `max_wait_ms` deadline passes, re-reading only what may have changed in between.
//!
//! As Kafka's `DelayedFetch`, a waiting Fetch is re-checked when a partition it watches
//! changes, not on a timer:
//! 1. Every requested topic-partition is registered in the [`PendingFetchRegistry`] (one
//!    shared [`Wake`], each partition a numbered slot of it) BEFORE the first fetch attempt,
//!    and stays registered for the whole long poll. A notification that lands while an
//!    attempt is in flight stays marked, so there is no missed wake-up window between "fetch
//!    returned too little" and "start waiting".
//! 2. The first attempt reads every partition; its response is returned if it meets
//!    `min_bytes`, carries a partition error, or the deadline has passed. Otherwise the bytes
//!    each partition has available are kept.
//! 3. The long poll waits for a wake-up, its deadline, or its safety-net interval
//!    (`pg_kafka.fetch_poll_interval_ms`). A wake-up names the partitions that changed
//!    (Produce, DeleteRecords, DeleteTopics; every partition of a `read_committed` long poll
//!    when a transaction ends) and only those are re-read. When the bytes available reach
//!    `min_bytes`, or a re-read partition has an error, the whole Fetch is read once more and
//!    answered.
//! 4. A re-check that no notification names (the safety-net interval, a retention sweep or
//!    a dropped notification, see `notifications`) reads every partition, and only within
//!    the safety net's allowance: [`SAFETY_NET_PARTITIONS_PER_INTERVAL`] partitions per
//!    interval for all long polls together ([`PendingFetchRegistry::take_safety_net`]).
//! 5. At the deadline the whole Fetch is read once more and answered; when nothing was
//!    marked since a read that returned no records, that read's response is answered as is.
//!
//! Cost on the DB thread, for a long poll of P partitions: one read of P at the start and at
//! most one at the end; one read of the marked partitions per wake-up (each partition at most
//! once per wake-up, so the reads a Produce causes are one per long poll waiting on each
//! partition it wrote, as Kafka's `DelayedFetch` checks each watcher); P per transaction end
//! for a `read_committed` long poll; and the safety net's shared allowance. An idle long poll
//! of a caught-up consumer therefore costs one read of its partitions per request.

use bytes::Bytes;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

use super::dispatch::send_with_backpressure;
use super::response_budget::{ReplyCharge, ResponseCharge};
use crate::kafka::api::{FetchRequestData, RequestBody, RequestHeader, ResponseBody};
use crate::kafka::constants::API_KEY_FETCH;
use crate::kafka::handlers::merge_by_partition;
use crate::kafka::messages::{KafkaRequest, KafkaResponse, PartitionFetchData, TopicFetchData};
#[cfg(doc)]
use crate::kafka::pending_fetches::SAFETY_NET_PARTITIONS_PER_INTERVAL;
use crate::kafka::pending_fetches::{PendingFetchRegistry, Taken, Wake};
use crate::kafka::protocol::DecodedCharge;
use crate::kafka::storage::IsolationLevel;
use kafka_protocol::messages::fetch_response::PartitionData;

/// The safety-net interval for a `pg_kafka.fetch_poll_interval_ms` value (at least 1 ms).
pub(super) fn poll_interval(fetch_poll_interval_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(fetch_poll_interval_ms).unwrap_or(0).max(1))
}

/// Why a long poll ended without answering its client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LongPollError {
    /// The worker's request channel is closed.
    WorkerGone,
    /// The worker dropped a fetch attempt without answering it.
    NoResponse,
}

impl std::fmt::Display for LongPollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LongPollError::WorkerGone => "Worker channel closed",
            LongPollError::NoResponse => "Response channel closed",
        })
    }
}

impl std::error::Error for LongPollError {}

/// One Fetch request that waits for data.
pub(super) struct LongPoll {
    header: RequestHeader,
    fetch: FetchRequestData,
    /// The worker's main lane.
    request_tx: Sender<KafkaRequest>,
    /// The client's response slot.
    response_tx: UnboundedSender<KafkaResponse>,
}

/// What the next fetch attempt of a waiting long poll reads.
enum Check {
    /// Every partition.
    Full,
    /// These slots (partitions) only.
    Some(Vec<usize>),
    /// Nothing: wait again.
    Nothing,
}

/// A long poll's partitions by slot (the order it registers them in).
struct Slots<'a> {
    /// The `(topic index, partition index)` in the Fetch of each slot.
    at: Vec<(usize, usize)>,
    /// `(topic name, partition id)` -> slot.
    by_key: HashMap<(&'a str, i32), usize>,
}

impl<'a> Slots<'a> {
    /// The slots of `fetch`, whose repeated partitions are merged. O(partitions).
    fn new(fetch: &'a FetchRequestData) -> Self {
        let mut at = Vec::new();
        let mut by_key = HashMap::new();
        for (t, topic) in fetch.topic_data.iter().enumerate() {
            for (p, partition) in topic.partitions.iter().enumerate() {
                by_key.insert((topic.name.as_str(), partition.partition_index), at.len());
                at.push((t, p));
            }
        }
        Slots { at, by_key }
    }

    fn len(&self) -> usize {
        self.at.len()
    }

    /// The Fetch of `slots` only, in request order: O(d log d) for d slots, plus the bytes of
    /// the names of the topics they belong to.
    fn subset(&self, fetch: &FetchRequestData, mut slots: Vec<usize>) -> Vec<TopicFetchData> {
        slots.sort_unstable();
        let mut topic_data: Vec<TopicFetchData> = Vec::new();
        let mut last_topic = None;
        for slot in slots {
            let Some(&(t, p)) = self.at.get(slot) else {
                continue;
            };
            let topic = &fetch.topic_data[t];
            if last_topic != Some(t) {
                topic_data.push(TopicFetchData {
                    name: topic.name.clone(),
                    partitions: Vec::new(),
                });
                last_topic = Some(t);
            }
            if let Some(entry) = topic_data.last_mut() {
                entry.partitions.push(topic.partitions[p].clone());
            }
        }
        topic_data
    }

    /// Call `f(slot, bytes)` for each requested partition in `response` with the bytes it
    /// has available ([`partition_available`]). O(partitions in the response).
    fn count(
        &self,
        fetch: &FetchRequestData,
        response: &KafkaResponse,
        read_committed: bool,
        mut f: impl FnMut(usize, u64),
    ) {
        let ResponseBody::Fetch(response) = &response.body else {
            return;
        };
        for topic in &response.responses {
            for p in &topic.partitions {
                let Some(&slot) = self.by_key.get(&(&**topic.topic, p.partition_index)) else {
                    continue;
                };
                let (t, i) = self.at[slot];
                let asked = &fetch.topic_data[t].partitions[i];
                f(slot, partition_available(p, asked, read_committed));
            }
        }
    }
}

/// The bytes each partition of a long poll has available, by slot, and their sum.
struct Tally {
    bytes: Vec<u64>,
    total: u64,
}

impl Tally {
    fn new(slots: usize) -> Self {
        Tally {
            bytes: vec![0; slots],
            total: 0,
        }
    }

    fn set(&mut self, slot: usize, bytes: u64) {
        if let Some(old) = self.bytes.get_mut(slot) {
            self.total = self.total.saturating_sub(*old).saturating_add(bytes);
            *old = bytes;
        }
    }

    /// Whether the bytes available meet `min_bytes` (a non-positive one is met by any).
    fn meets(&self, min_bytes: i32) -> bool {
        min_bytes <= 0 || u64::try_from(min_bytes).is_ok_and(|min| self.total >= min)
    }
}

impl LongPoll {
    /// The long poll of one Fetch request; `request_tx` is the worker's main lane.
    ///
    /// A repeated topic-partition is merged into one entry, its last, as the Fetch handler
    /// reads it (Kafka's fetch data is a map by partition): the long poll registers, sends
    /// and checks each distinct partition once. Cost: O(bytes of the request's topics)
    /// time, O(distinct partitions) memory.
    pub(super) fn new(
        header: RequestHeader,
        mut fetch: FetchRequestData,
        request_tx: Sender<KafkaRequest>,
        response_tx: UnboundedSender<KafkaResponse>,
    ) -> Self {
        debug!(
            "Fetch request with long polling: max_wait_ms={}, min_bytes={}, isolation_level={}",
            fetch.max_wait_ms, fetch.min_bytes, fetch.isolation_level
        );
        fetch.topic_data = merge_by_partition(
            std::mem::take(&mut fetch.topic_data)
                .into_iter()
                .map(|t| (t.name, t.partitions)),
            |p| p.partition_index,
        )
        .into_iter()
        .map(|(name, partitions)| TopicFetchData { name, partitions })
        .collect();
        LongPoll {
            header,
            fetch,
            request_tx,
            response_tx,
        }
    }

    /// Answer the client: register for wake-ups, fetch until done, unregister.
    /// Each attempt reserves the request's `max_bytes` of the connection's response budget
    /// first (`reply`), so the responses many long polls fetch at once wait for room instead
    /// of all being held together.
    pub(super) async fn run(
        self,
        registry: &PendingFetchRegistry,
        poll_interval: Duration,
        reply: &ReplyCharge,
    ) -> Result<(), LongPollError> {
        // The client is gone (its connection's writer, which owns the response slot,
        // has been torn down): do nothing rather than queue work for the DB thread.
        if self.response_tx.is_closed() {
            debug!("Long poll: client disconnected, abandoning fetch");
            return Ok(());
        }
        let max_wait = u64::try_from(self.fetch.max_wait_ms).unwrap_or(0);
        let deadline = Instant::now() + Duration::from_millis(max_wait);
        let read_committed =
            IsolationLevel::from_i8(self.fetch.isolation_level) == IsolationLevel::ReadCommitted;

        // `new` merged repeated partitions, so each is registered once, its slot its
        // position in `fetch.topic_data` (see `Slots`).
        let wake = std::sync::Arc::new(Wake::new());
        let topics = || self.fetch.topic_data.iter();
        let id = registry
            .register(
                topics().map(|t| {
                    let partitions = t.partitions.iter();
                    (
                        t.name.as_str(),
                        partitions.map(|p| (p.partition_index, p.fetch_offset)),
                    )
                }),
                &wake,
                read_committed,
            )
            .await;

        let result = self
            .fetch_until_done(
                Waiting {
                    read_committed,
                    wake: &wake,
                    deadline,
                    poll_interval,
                    registry,
                },
                reply,
            )
            .await;

        // Always drop the registrations, whatever the outcome.
        registry
            .unregister(
                id,
                topics().map(|t| {
                    let partitions = t.partitions.iter();
                    (t.name.as_str(), partitions.map(|p| p.partition_index))
                }),
            )
            .await;
        result
    }

    /// The fetch / wait loop (see the module docs); [`run`](Self::run) owns the
    /// registration around it. Memory: O(partitions) for the slots and their counts, plus,
    /// while it waits, the response of its last read of every partition when that holds no
    /// records (its partitions' entries only), to answer at the deadline.
    async fn fetch_until_done(
        &self,
        waiting: Waiting<'_>,
        reply: &ReplyCharge,
    ) -> Result<(), LongPollError> {
        let Waiting {
            read_committed,
            wake,
            deadline,
            poll_interval,
            registry,
        } = waiting;
        let slots = Slots::new(&self.fetch);
        let mut tally = Tally::new(slots.len());
        // The response of the last read of every partition, while it holds no records and
        // nothing was marked since.
        let mut unchanged: Option<KafkaResponse> = None;
        let mut next = Check::Full;
        let mut safety_net_at = Instant::now() + poll_interval;
        loop {
            match std::mem::replace(&mut next, Check::Nothing) {
                Check::Full => {
                    let Some((charge, alone)) = self.reserve(reply).await else {
                        return Ok(());
                    };
                    let response = self.fetch_once(None, alone).await?;
                    tally = Tally::new(slots.len());
                    slots.count(&self.fetch, &response, read_committed, |slot, bytes| {
                        tally.set(slot, bytes);
                    });
                    debug!(
                        "Long poll fetch found {} bytes available (min_bytes={})",
                        tally.total, self.fetch.min_bytes
                    );
                    // A partition error (unknown partition, offset out of range, ...) is
                    // returned straight away, as Kafka does, so the consumer can react
                    // instead of waiting out max_wait_ms for data that will not arrive.
                    if tally.meets(self.fetch.min_bytes)
                        || response_has_partition_error(&response)
                        || Instant::now() >= deadline
                    {
                        reply.hold(charge);
                        let _ = self.response_tx.send(response);
                        return Ok(());
                    }
                    drop(charge);
                    unchanged = (!response_has_records(&response)).then_some(response);
                }
                Check::Some(marked) => {
                    unchanged = None;
                    let Some((charge, alone)) = self.reserve(reply).await else {
                        return Ok(());
                    };
                    let subset = slots.subset(&self.fetch, marked);
                    let response = self.fetch_once(Some(subset), alone).await?;
                    slots.count(&self.fetch, &response, read_committed, |slot, bytes| {
                        tally.set(slot, bytes);
                    });
                    let error = response_has_partition_error(&response);
                    drop((response, charge));
                    if error || tally.meets(self.fetch.min_bytes) || Instant::now() >= deadline {
                        debug!("Long poll: changed partitions complete the fetch, answering");
                        return self.answer_full(reply).await;
                    }
                }
                Check::Nothing => {}
            }

            // Not enough data and not timed out: wait for a wake-up. The deadline wait is
            // exact; the safety-net interval only bounds how long a change no notification
            // names can go unseen (within the safety net's allowance).
            let wake_at = deadline.min(safety_net_at);
            tokio::select! {
                () = wake.notified() => {
                    debug!("Long poll: woken by a change notification");
                }
                () = tokio::time::sleep_until(wake_at.into()) => {}
                () = self.response_tx.closed() => {
                    debug!("Long poll: client disconnected while waiting");
                    return Ok(());
                }
            }
            let changed = wake.take();
            let now = Instant::now();
            if now >= deadline {
                debug!("Long poll: timeout reached, answering");
                // Anything marked since the last read (a coarse mark included: records may
                // have expired) is read again.
                return match unchanged.filter(|_| changed == Taken::default()) {
                    Some(response) => self.answer(response, reply).await,
                    None => self.answer_full(reply).await,
                };
            }
            if changed.coarse {
                unchanged = None;
            }
            let safety_net_due = now >= safety_net_at;
            if safety_net_due {
                safety_net_at = now + poll_interval;
            }
            next = if changed.all || changed.slots.len() >= slots.len() {
                Check::Full
            } else if (changed.coarse || safety_net_due)
                && registry.take_safety_net(slots.len(), poll_interval)
            {
                debug!("Long poll: safety-net re-check");
                Check::Full
            } else if changed.slots.is_empty() {
                Check::Nothing
            } else {
                Check::Some(changed.slots)
            };
        }
    }

    /// Reserve the request's `max_bytes` of the connection's response budget, or `None` once
    /// the client is gone.
    async fn reserve(&self, reply: &ReplyCharge) -> Option<(ResponseCharge, bool)> {
        tokio::select! {
            reserved = reply.reserve(self.fetch.max_bytes) => Some(reserved),
            () = self.response_tx.closed() => {
                debug!("Long poll: client disconnected while waiting for response room");
                None
            }
        }
    }

    /// Read every partition once more and answer that.
    async fn answer_full(&self, reply: &ReplyCharge) -> Result<(), LongPollError> {
        let Some((charge, alone)) = self.reserve(reply).await else {
            return Ok(());
        };
        let response = self.fetch_once(None, alone).await?;
        reply.hold(charge);
        let _ = self.response_tx.send(response);
        Ok(())
    }

    /// Answer `response`, read earlier, once its bytes are reserved.
    async fn answer(
        &self,
        response: KafkaResponse,
        reply: &ReplyCharge,
    ) -> Result<(), LongPollError> {
        let Some((charge, _)) = self.reserve(reply).await else {
            return Ok(());
        };
        reply.hold(charge);
        let _ = self.response_tx.send(response);
        Ok(())
    }

    /// One fetch attempt on the worker of `topic_data` (`None`: every partition), without
    /// waiting (`max_wait_ms = 0`) and accepting any amount of data (`min_bytes = 0`); `alone`
    /// is whether its connection held nothing else when it reserved its bytes (see
    /// `FetchRequestData::whole_first_record`).
    async fn fetch_once(
        &self,
        topic_data: Option<Vec<TopicFetchData>>,
        alone: bool,
    ) -> Result<KafkaResponse, LongPollError> {
        let (attempt_tx, mut attempt_rx) = tokio::sync::mpsc::unbounded_channel();
        let topic_data = topic_data.unwrap_or_else(|| self.fetch.topic_data.clone());
        let request = KafkaRequest {
            header: RequestHeader {
                api_key: API_KEY_FETCH,
                ..self.header.clone()
            },
            body: RequestBody::Fetch(FetchRequestData {
                max_wait_ms: 0,
                min_bytes: 0,
                whole_first_record: alone,
                topic_data,
                ..self.fetch
            }),
            response_tx: attempt_tx,
            // The long-poll task holds the original request's charge for as long as it runs.
            decode_charge: DecodedCharge::default(),
        };
        if !send_with_backpressure(&self.request_tx, request).await {
            warn!("Worker channel closed during long poll");
            return Err(LongPollError::WorkerGone);
        }
        attempt_rx.recv().await.ok_or_else(|| {
            warn!("Response channel closed during long poll");
            LongPollError::NoResponse
        })
    }
}

/// What a long poll waits with (see [`LongPoll::fetch_until_done`]).
struct Waiting<'a> {
    read_committed: bool,
    wake: &'a Wake,
    deadline: Instant,
    poll_interval: Duration,
    registry: &'a PendingFetchRegistry,
}

/// Calculate the total bytes in a FetchResponse
#[cfg(test)]
fn calculate_response_bytes(response: &KafkaResponse) -> usize {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .filter_map(|p| p.records.as_ref())
            .map(Bytes::len)
            .sum(),
        _ => 0,
    }
}

/// The bytes a Fetch response finds available, as Kafka 3.7's `DelayedFetch.tryComplete` counts
/// them against `min_bytes`: the sum of [`partition_available`] over its requested partitions.
#[cfg(test)]
fn available_bytes(
    response: &KafkaResponse,
    fetch: &FetchRequestData,
    read_committed: bool,
) -> u64 {
    let mut available = 0u64;
    Slots::new(fetch).count(fetch, response, read_committed, |_, bytes| {
        available = available.saturating_add(bytes);
    });
    available
}

/// The bytes one partition of a Fetch response has available, as Kafka 3.7's
/// `DelayedFetch.tryComplete` counts them against `min_bytes`: the bytes from its fetch offset
/// to its readable end (the high watermark, or the last stable offset for `read_committed`),
/// at most its `partition_max_bytes`. A partition read to its readable end counts the records
/// it returned (at most its `partition_max_bytes`). One that stopped before it, cut by its
/// `partition_max_bytes` or by what was left of the request's `max_bytes`, or not read once
/// that was spent, counts its whole `partition_max_bytes`: cut by its own limit, it has more
/// than that available; cut by the request's, the response holds what `max_bytes` allows,
/// which is at least `min_bytes` (lowered to `max_bytes`), so Kafka answers it too. A
/// partition with an error counts nothing (the response is answered for the error).
fn partition_available(p: &PartitionData, asked: &PartitionFetchData, read_committed: bool) -> u64 {
    if p.error_code != 0 {
        return 0;
    }
    let limit = u64::try_from(asked.partition_max_bytes).unwrap_or(0);
    let readable_end = if read_committed {
        p.last_stable_offset
    } else {
        p.high_watermark
    };
    let returned = p.records.as_ref();
    let next = returned
        .and_then(next_offset_after)
        .unwrap_or(asked.fetch_offset);
    if next < readable_end {
        limit
    } else {
        u64::try_from(returned.map_or(0, Bytes::len))
            .unwrap_or(u64::MAX)
            .min(limit)
    }
}

/// The offset after the last batch of encoded RecordBatch v2 `records` (base offset plus
/// last offset delta plus one), or `None` when they hold no whole batch.
fn next_offset_after(records: &Bytes) -> Option<i64> {
    // base offset (8), batch length (4), leader epoch (4), magic (1), CRC (4), attributes (2),
    // then the last offset delta (4).
    const LAST_OFFSET_DELTA_AT: usize = 23;
    let mut at = 0usize;
    let mut next = None;
    while let Some(header) = records.get(at..at + LAST_OFFSET_DELTA_AT + 4) {
        let base = i64::from_be_bytes(header[..8].try_into().ok()?);
        let length = usize::try_from(i32::from_be_bytes(header[8..12].try_into().ok()?)).ok()?;
        let delta = i32::from_be_bytes(header[LAST_OFFSET_DELTA_AT..].try_into().ok()?);
        next = Some(base + i64::from(delta) + 1);
        at += 12 + length;
    }
    next
}

/// Whether any partition in a Fetch response returned records.
fn response_has_records(response: &KafkaResponse) -> bool {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.records.as_ref().is_some_and(|r| !r.is_empty())),
        _ => false,
    }
}

/// Whether any partition in a Fetch response carries a non-zero error code.
fn response_has_partition_error(response: &KafkaResponse) -> bool {
    match &response.body {
        ResponseBody::Fetch(response) => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A reply charge on a roomy budget of its own.
    fn test_reply() -> ReplyCharge {
        ReplyCharge::new(
            super::super::response_budget::ResponseBudget::new(1 << 20),
            0,
        )
    }

    /// A Fetch v4 long poll (correlation id 1, `max_bytes` 1024) of `topics`.
    fn long_poll(
        max_wait_ms: i32,
        min_bytes: i32,
        isolation_level: i8,
        topics: Vec<TopicFetchData>,
        request_tx: Sender<KafkaRequest>,
        response_tx: UnboundedSender<KafkaResponse>,
    ) -> LongPoll {
        LongPoll {
            header: RequestHeader {
                api_key: API_KEY_FETCH,
                api_version: 4,
                correlation_id: 1,
                client_id: None,
            },
            fetch: FetchRequestData {
                max_wait_ms,
                min_bytes,
                max_bytes: 1024,
                isolation_level,
                topic_data: topics,
                whole_first_record: true,
            },
            request_tx,
            response_tx,
        }
    }

    /// Kafka's `DelayedFetch` count: a partition read to its readable end counts what it
    /// returned, one that stopped before it its `partition_max_bytes`, so a `min_bytes` above
    /// what a small `partition_max_bytes` lets through is waited for, and one the request's
    /// `max_bytes` cut short is met.
    #[test]
    fn test_available_bytes_count_each_partition_up_to_its_limit() {
        use kafka_protocol::messages::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        };
        use kafka_protocol::messages::TopicName;
        use kafka_protocol::protocol::StrBytes;
        // The 61-byte header of a batch of offsets 5..=7 (base offset 5, last offset
        // delta 2), records left out.
        let mut header = Vec::new();
        header.extend_from_slice(&5i64.to_be_bytes());
        header.extend_from_slice(&49i32.to_be_bytes());
        header.extend_from_slice(&[0; 11]);
        header.extend_from_slice(&2i32.to_be_bytes());
        header.resize(61, 0);
        let records = Bytes::from(header);
        assert_eq!(next_offset_after(&records), Some(8));
        assert!(next_offset_after(&Bytes::new()).is_none());
        // Partition 0 holds the batch, partition 1 nothing; each has `ends` as its high
        // watermark and last stable offset.
        let response = |ends: [(i64, i64); 2]| {
            let partitions = ends
                .iter()
                .enumerate()
                .map(|(i, &(high_watermark, last_stable_offset))| {
                    let mut partition = PartitionData::default();
                    partition.partition_index = i32::try_from(i).unwrap();
                    partition.high_watermark = high_watermark;
                    partition.last_stable_offset = last_stable_offset;
                    partition.records = Some(if i == 0 {
                        records.clone()
                    } else {
                        Bytes::new()
                    });
                    partition
                })
                .collect();
            let mut topic = FetchableTopicResponse::default();
            topic.topic = TopicName(StrBytes::from_static_str("t"));
            topic.partitions = partitions;
            let mut fetch = FetchResponse::default();
            fetch.responses = vec![topic];
            KafkaResponse::new(1, 4, ResponseBody::Fetch(fetch))
        };
        let request = |limits: [i32; 2]| FetchRequestData {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            isolation_level: 0,
            topic_data: vec![TopicFetchData {
                name: "t".to_string(),
                partitions: (0..2)
                    .map(|i| PartitionFetchData {
                        partition_index: i,
                        fetch_offset: 5,
                        partition_max_bytes: limits[usize::try_from(i).unwrap()],
                    })
                    .collect(),
            }],
            whole_first_record: true,
        };
        // Both read to the end: the 61 bytes returned.
        let at_end = response([(8, 8), (5, 5)]);
        assert_eq!(available_bytes(&at_end, &request([1000, 1000]), false), 61);
        // At most partition_max_bytes of them.
        assert_eq!(available_bytes(&at_end, &request([40, 1000]), false), 40);
        // Partition 0 stopped before offset 20: its whole partition_max_bytes.
        let short = response([(20, 20), (5, 5)]);
        assert_eq!(available_bytes(&short, &request([1000, 300]), false), 1000);
        // Partition 1 was not read although it has data: its limit too.
        let unread = response([(8, 8), (9, 9)]);
        assert_eq!(available_bytes(&unread, &request([1000, 300]), false), 361);
        // read_committed stops at the last stable offset.
        let stable = response([(20, 8), (9, 5)]);
        assert_eq!(available_bytes(&stable, &request([1000, 300]), true), 61);
        assert_eq!(available_bytes(&stable, &request([1000, 300]), false), 1300);
    }

    #[test]
    fn test_long_poll_stops_when_client_is_gone() {
        // A long poll whose client has disconnected (response slot closed) must return
        // straight away instead of re-fetching on the DB thread until max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel::<KafkaResponse>();
        drop(final_rx);
        let topic = TopicFetchData {
            name: "t".to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let started = Instant::now();
        rt.block_on(
            long_poll(i32::MAX, i32::MAX, 0, vec![topic], request_tx, final_tx).run(
                &PendingFetchRegistry::new(),
                Duration::from_millis(100),
                &test_reply(),
            ),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            request_rx.try_recv().is_err(),
            "no fetch may reach the worker for a disconnected client"
        );
    }

    fn one_partition_topic(name: &str) -> TopicFetchData {
        TopicFetchData {
            name: name.to_string(),
            partitions: vec![PartitionFetchData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }
    }

    /// A response to a Fetch of partition 0 of `topic`, with records or none.
    fn fetch_response(with_records: bool, topic_name: &str) -> KafkaResponse {
        use kafka_protocol::messages::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        };
        use kafka_protocol::messages::TopicName;
        use kafka_protocol::protocol::StrBytes;
        let mut response = FetchResponse::default();
        if with_records {
            let mut partition = PartitionData::default();
            partition.records = Some(Bytes::from_static(b"some-record-bytes"));
            let mut topic = FetchableTopicResponse::default();
            topic.topic = TopicName(StrBytes::from_string(topic_name.to_string()));
            topic.partitions = vec![partition];
            response.responses = vec![topic];
        }
        KafkaResponse::new(1, 4, ResponseBody::Fetch(response))
    }

    /// Fake DB thread: answers every Fetch it receives. `before_answer(n)` runs before
    /// answering the n-th fetch (1-based) and returns whether that answer carries records.
    /// Returns a counter of fetches served.
    fn spawn_fake_fetch_worker<F, Fut>(
        rx: crossbeam_channel::Receiver<KafkaRequest>,
        before_answer: F,
    ) -> Arc<AtomicUsize>
    where
        F: Fn(usize) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = bool> + Send,
    {
        let served = Arc::new(AtomicUsize::new(0));
        let served_out = served.clone();
        tokio::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(KafkaRequest {
                        body: RequestBody::Fetch(fetch),
                        response_tx,
                        ..
                    }) => {
                        let n = served.fetch_add(1, Ordering::SeqCst) + 1;
                        let with_records = before_answer(n).await;
                        let name = fetch.topic_data.first().map_or("", |t| t.name.as_str());
                        let _ = response_tx.send(fetch_response(with_records, name));
                    }
                    Ok(_) => {}
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                }
            }
        });
        served_out
    }

    /// A long poll of one partition named 50,000 times (plus a second topic entry naming it
    /// again, at a later offset) registers it once and sends it to the DB thread once, at its
    /// last entry's offset.
    #[tokio::test]
    async fn a_repeated_partition_is_registered_and_fetched_once() {
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                match request_rx.try_recv() {
                    Ok(KafkaRequest {
                        body: RequestBody::Fetch(fetch),
                        response_tx,
                        ..
                    }) => {
                        let sent: Vec<(String, Vec<(i32, i64)>)> = fetch
                            .topic_data
                            .iter()
                            .map(|t| {
                                let p = t.partitions.iter();
                                (
                                    t.name.clone(),
                                    p.map(|p| (p.partition_index, p.fetch_offset)).collect(),
                                )
                            })
                            .collect();
                        let _ = seen_tx.send((sent, reg.len().await));
                        let _ = response_tx.send(fetch_response(false, "t"));
                    }
                    Ok(_) => {}
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                }
            }
        });
        let entry = |fetch_offset| PartitionFetchData {
            partition_index: 0,
            fetch_offset,
            partition_max_bytes: 1024,
        };
        let topics = vec![
            TopicFetchData {
                name: "t".to_string(),
                partitions: (0..50_000).map(entry).collect(),
            },
            TopicFetchData {
                name: "t".to_string(),
                partitions: vec![entry(70_000)],
            },
        ];
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();
        let built = long_poll(0, 1, 0, topics, request_tx, final_tx);
        LongPoll::new(
            built.header,
            built.fetch,
            built.request_tx,
            built.response_tx,
        )
        .run(&registry, Duration::from_millis(10_000), &test_reply())
        .await
        .unwrap();
        let (sent, registered) = seen_rx.recv().await.unwrap();
        assert_eq!(sent, vec![("t".to_string(), vec![(0, 70_000)])]);
        assert_eq!(registered, 1, "registrations while the long poll ran");
        assert!(final_rx.try_recv().is_ok());
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn test_idle_long_poll_does_not_refetch_until_deadline() {
        // With no notification and a safety-net interval longer than max_wait, an idle long
        // poll whose first read returned no records reads once: nothing changed, so that
        // read's response is answered at the deadline.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let served = spawn_fake_fetch_worker(request_rx, |_| async { false });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();
        let registry = Arc::new(PendingFetchRegistry::new());

        let started = Instant::now();
        long_poll(
            600,
            1,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000), &test_reply())
        .await
        .unwrap();

        assert!(started.elapsed() >= Duration::from_millis(600));
        assert!(final_rx.try_recv().is_ok(), "the deadline response is sent");
        assert_eq!(served.load(Ordering::SeqCst), 1);
        assert!(registry.is_empty().await, "registrations are removed");
    }

    #[tokio::test]
    async fn test_long_poll_safety_net_interval_still_refetches() {
        // Data that becomes visible without a notification is picked up at the safety-net
        // interval, well before max_wait_ms.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let served = spawn_fake_fetch_worker(request_rx, |n| async move { n >= 2 });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        long_poll(
            10_000,
            1,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(
            &PendingFetchRegistry::new(),
            Duration::from_millis(200),
            &test_reply(),
        )
        .await
        .unwrap();

        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(200), "waited {elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "waited {elapsed:?}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }

    #[tokio::test]
    async fn test_notification_during_fetch_attempt_is_not_missed() {
        // Missed-wakeup window: the produce notification lands while the first fetch attempt is
        // still on the DB thread (it returned "no data"). The long poll registered before that
        // attempt, so the wake-up is kept and the record is returned at once — not after the
        // safety-net interval (10 s here) or max_wait_ms (10 s).
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let served = spawn_fake_fetch_worker(request_rx, move |n| {
            let reg = reg.clone();
            async move {
                if n == 1 {
                    reg.notify_new_data("t", 0, 1).await;
                    false
                } else {
                    true
                }
            }
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        long_poll(
            10_000,
            1,
            0,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000), &test_reply())
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn test_read_committed_long_poll_wakes_on_transaction_end() {
        // EndTxn → TransactionEnded wakes a read_committed long poll promptly.
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let served = spawn_fake_fetch_worker(request_rx, move |n| {
            let reg = reg.clone();
            async move {
                if n == 1 {
                    // Simulate the commit landing (after the first attempt saw nothing).
                    let reg = reg.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        reg.notify_transaction_ended().await;
                    });
                    false
                } else {
                    true
                }
            }
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();

        let started = Instant::now();
        long_poll(
            10_000,
            1,
            1,
            vec![one_partition_topic("t")],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000), &test_reply())
        .await
        .unwrap();

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(served.load(Ordering::SeqCst), 2);
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
    }

    /// Fake DB thread that answers every Fetch with no records and records how many
    /// partitions each attempt named; `with_records(n)` says whether the n-th answer
    /// (1-based) carries records for partition 0 of its first topic.
    fn spawn_counting_worker(
        rx: crossbeam_channel::Receiver<KafkaRequest>,
        with_records: impl Fn(usize) -> bool + Send + 'static,
    ) -> Arc<std::sync::Mutex<Vec<usize>>> {
        let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = attempts.clone();
        tokio::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(KafkaRequest {
                        body: RequestBody::Fetch(fetch),
                        response_tx,
                        ..
                    }) => {
                        let n = {
                            let mut seen = seen.lock().unwrap();
                            seen.push(fetch.topic_data.iter().map(|t| t.partitions.len()).sum());
                            seen.len()
                        };
                        let name = fetch.topic_data.first().map_or("", |t| t.name.as_str());
                        let _ = response_tx.send(fetch_response(with_records(n), name));
                    }
                    Ok(_) => {}
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                }
            }
        });
        attempts
    }

    /// One topic of `partitions` partitions, each fetched from offset 0.
    fn many_partitions(name: &str, partitions: i32) -> TopicFetchData {
        TopicFetchData {
            name: name.to_string(),
            partitions: (0..partitions)
                .map(|partition_index| PartitionFetchData {
                    partition_index,
                    fetch_offset: 0,
                    partition_max_bytes: 1024,
                })
                .collect(),
        }
    }

    /// 64 idle long polls of 1,000 partitions each, with a 10 ms safety-net interval and a
    /// 500 ms max wait, cost the DB thread one read of their partitions each (64,000
    /// partition reads), however short the interval: a long poll of more partitions than the
    /// safety net's allowance never takes it, and nothing changed, so each answers its first
    /// read at the deadline. A re-read per interval costs 64 x 1,000 x 50 = 3.2 million.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_long_polls_cost_one_read_whatever_the_safety_net_interval() {
        const POLLS: usize = 64;
        const PARTITIONS: i32 = 1_000;
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(1024);
        let attempts = spawn_counting_worker(request_rx, |_| false);
        let registry = Arc::new(PendingFetchRegistry::new());
        let mut polls = Vec::new();
        for _ in 0..POLLS {
            let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel();
            let poll = long_poll(
                500,
                1,
                0,
                vec![many_partitions("t", PARTITIONS)],
                request_tx.clone(),
                final_tx,
            );
            let registry = registry.clone();
            polls.push(tokio::spawn(async move {
                poll.run(&registry, Duration::from_millis(10), &test_reply())
                    .await
                    .unwrap();
                final_rx
            }));
        }
        for poll in polls {
            assert!(poll.await.unwrap().try_recv().is_ok(), "every poll answers");
        }
        let attempts = attempts.lock().unwrap().clone();
        let read: usize = attempts.iter().sum();
        assert_eq!(
            read,
            POLLS * 1_000,
            "partitions read by {} attempts",
            attempts.len()
        );
        assert!(registry.is_empty().await);
    }

    /// A long poll of 1,000 partitions woken by new data in one of them re-reads that one
    /// partition only, then, as it now has min_bytes, reads all of them once to answer.
    #[tokio::test]
    async fn a_notified_partition_is_re_read_alone() {
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let attempts = spawn_counting_worker(request_rx, |n| n >= 2);
        let registry = Arc::new(PendingFetchRegistry::new());
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();
        let reg = registry.clone();
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            reg.notify_new_data("t", 0, 1).await;
        });
        let started = Instant::now();
        long_poll(
            10_000,
            1,
            0,
            vec![many_partitions("t", 1_000)],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000), &test_reply())
        .await
        .unwrap();
        notifier.await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(calculate_response_bytes(&final_rx.try_recv().unwrap()) > 0);
        assert_eq!(*attempts.lock().unwrap(), vec![1_000, 1, 1_000]);
    }

    /// Long polls whose safety net fires every 10 ms re-read within the shared allowance
    /// only: 3,000 idle long polls of one partition over 100 ms read their first reads plus
    /// at most the allowance per interval. A re-read per interval each costs about 33,000.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn safety_net_rereads_share_one_allowance() {
        use crate::kafka::pending_fetches::SAFETY_NET_PARTITIONS_PER_INTERVAL;
        const POLLS: usize = 3_000;
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(1024);
        let attempts = spawn_counting_worker(request_rx, |_| false);
        let registry = Arc::new(PendingFetchRegistry::new());
        let started = Instant::now();
        let mut polls = Vec::new();
        for _ in 0..POLLS {
            let (final_tx, final_rx) = tokio::sync::mpsc::unbounded_channel();
            let poll = long_poll(
                100,
                1,
                0,
                vec![one_partition_topic("t")],
                request_tx.clone(),
                final_tx,
            );
            let registry = registry.clone();
            polls.push(tokio::spawn(async move {
                poll.run(&registry, Duration::from_millis(10), &test_reply())
                    .await
                    .unwrap();
                final_rx
            }));
        }
        for poll in polls {
            poll.await.unwrap();
        }
        let intervals = u64::try_from(started.elapsed().as_millis() / 10 + 1).unwrap();
        let read = u64::try_from(attempts.lock().unwrap().iter().sum::<usize>()).unwrap();
        let polls = u64::try_from(POLLS).unwrap();
        assert!(
            read <= polls + intervals * SAFETY_NET_PARTITIONS_PER_INTERVAL,
            "{read} partition reads in {intervals} intervals"
        );
        assert!(read > polls, "the safety net re-read some");
    }

    /// A long poll of more partitions than the safety net's allowance, woken coarsely (a
    /// retention sweep removed records somewhere), cannot re-check before its deadline, and
    /// then reads again instead of answering its first read, which may be out of date.
    #[tokio::test]
    async fn a_coarse_wake_is_read_again_at_the_deadline() {
        let (request_tx, request_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let attempts = spawn_counting_worker(request_rx, |_| false);
        let registry = Arc::new(PendingFetchRegistry::new());
        let reg = registry.clone();
        let notifier = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            reg.notify_coarse().await;
        });
        let (final_tx, mut final_rx) = tokio::sync::mpsc::unbounded_channel();
        long_poll(
            300,
            1,
            0,
            vec![many_partitions("t", 300)],
            request_tx,
            final_tx,
        )
        .run(&registry, Duration::from_millis(10_000), &test_reply())
        .await
        .unwrap();
        notifier.await.unwrap();
        assert!(final_rx.try_recv().is_ok());
        assert_eq!(*attempts.lock().unwrap(), vec![300, 300]);
    }
}
