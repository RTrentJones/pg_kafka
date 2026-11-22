// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! The shadow forwarder task: runs on the listener's tokio runtime (network
//! thread), receives [`ForwardRequest`]s from the DB thread and produces them to
//! the external Kafka cluster, answering each record with a [`ForwardAck`].
//!
//! Like the rest of the network thread it MUST NOT call pgrx (no SPI, no
//! pg_log!); it logs through `tracing`.
//!
//! ## Pipelining and ordering
//!
//! Records are handed to librdkafka as they arrive, without waiting for the
//! previous delivery report: many deliveries are in flight at once and the
//! throughput is bounded by the external broker, not by one round trip per
//! record. Per-partition order rests on three things:
//!
//! 1. the DB thread dispatches the records of a local partition in offset order
//!    over one FIFO channel and never dispatches a record behind an earlier one
//!    that is waiting to be retried (see the outbox claim);
//! 2. the idempotent producer keeps that order on the wire through its own
//!    retries, and `enable.gapless.guarantee` turns anything that could deliver
//!    a later record after an earlier failed one into a fatal producer error;
//! 3. the **fence** here: once a record of a local partition fails, every later
//!    record of that partition that reaches the forwarder is answered
//!    `NotAttempted` without being sent, until the partition restarts at the
//!    failed offset (or at the head of the partition, once the failed record is
//!    dead-lettered).
//!
//! A failure caused by another record (the fence, or a fatal producer error
//! raised by an earlier record of the same producer generation) is reported as
//! `NotAttempted`, so it does not consume the record's retry budget
//! (`pg_kafka.shadow_max_retries`).
//!
//! ## Delivery timeout
//!
//! librdkafka's own `message.timeout.ms` is infinite: a record it fails by
//! timeout is not covered by `enable.gapless.guarantee`, and since the timeout
//! runs per record from its enqueue, an earlier record would time out while a
//! later one of the same partition, with a later deadline, stays queued and is
//! delivered ahead of the earlier one's retry once the external cluster is back.
//! The forwarder enforces the timeout ([`DELIVERY_TIMEOUT`]) per producer
//! generation instead: once the oldest record outstanding on a generation has
//! waited that long, every partition with a record on it is fenced, the
//! generation is retired and its queued and in-flight records are purged, all
//! partitions at once, so each partition restarts at its first undelivered
//! record on a new producer.
//!
//! A record's clock starts when the forwarder receives it, not when librdkafka
//! accepts it: the time it waits in the backlog (see Backpressure) counts, and a
//! record still in the backlog when its timeout runs out is answered there
//! ([`ForwarderCore::expire_backlog`]). Every record the DB thread dispatches is
//! therefore answered within about [`DELIVERY_TIMEOUT`] of its dispatch, which
//! the DB thread's in-flight eviction (`INFLIGHT_EVICT_AFTER` in `store.rs`)
//! relies on: an entry evicted while its record was still held here would let
//! the outbox claim the row again (counting a lost attempt that never failed)
//! and send it a second time beside the first.
//!
//! Which of the purged records count as failed attempts: the lowest outstanding
//! record of each local partition (the one the partition waits on) that has been
//! held for at least half the timeout. The records behind it are collateral
//! (the fence would hold them back anyway), and so is a head that has not had a
//! fair window yet (received less than half a timeout ago, for example a
//! healthy partition whose records were flowing). A record that runs out of
//! time in the backlog counts the same way: it fails if it is the first record
//! its partition waits on, and is collateral behind an earlier failure. In a
//! persistent outage every stuck partition's head is held for close to a whole
//! timeout at each retirement (the outbox backoff, at most 30 s, is under half
//! of it), so it fails once per cycle and is dead-lettered after
//! `shadow_max_retries` cycles; the records behind it are not penalized, and
//! the next one becomes the head once it is dead-lettered.
//!
//! Purging in-flight records cannot reorder a partition either: the old
//! producer's in-flight requests may still be written by the broker (the purge
//! only voids their acks), but the broker writes a producer's records in
//! sequence order, so what lands is an in-order prefix of the old generation's
//! records, and the new producer re-sends from the partition's first record
//! not acked as delivered. The first copy of every record is therefore in
//! local order; the old prefix and the re-send can duplicate (at-least-once).
//!
//! ## Backpressure
//!
//! When librdkafka's local queue is full, the record and every later one wait,
//! in order, in the forwarder's backlog, retried on a timer of their own (and
//! answered there if the delivery timeout runs out first). The loop keeps
//! serving delivery reports, held-response timeouts, the ack flush and the
//! settings check meanwhile, so a full queue delays only the records behind it.
//!
//! ## Held sync-mode responses
//!
//! A [`SyncHold`] carries the buffered response of a committed sync-mode
//! produce. It is released as soon as every one of its records is delivered,
//! or when its timeout expires, so a slow or unreachable external cluster delays
//! only those responses, never the DB thread.

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, TrySendError};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeSet;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::producer::{EnqueueError, DELIVERY_TIMEOUT};
use super::{
    ForwardAck, ForwardOutcome, ForwardRecord, ForwardRequest, RowKey, ShadowConfig,
    ShadowProducer, SyncHold,
};

/// Max requests moved from the crossbeam channel to the async task per hop.
const BRIDGE_BATCH: usize = 2048;
/// How often held responses are checked for their timeout.
const HOLD_TICK: Duration = Duration::from_millis(20);
/// Wait between attempts to enqueue into a full librdkafka queue.
const QUEUE_FULL_BACKOFF: Duration = Duration::from_millis(5);
/// Records waiting for room in librdkafka's queue at which the forwarder stops
/// taking new requests from the DB thread (the in-flight cap bounds them anyway).
const MAX_BACKLOG: usize = 16_384;
/// How long the bridge thread waits for a request before checking whether
/// the task is gone.
const BRIDGE_POLL: Duration = Duration::from_millis(100);
/// Hold ticks between checks for changed producer settings (~0.5 s).
const SETTINGS_CHECK_TICKS: u64 = 25;
/// Producer generations timed out per hold tick at most (more than one at
/// once is rare: a producer replaced while it still holds records).
const MAX_TIMEOUTS_PER_TICK: usize = 8;

/// What to do with an incoming record.
#[derive(Debug, PartialEq)]
pub(crate) enum Admit {
    /// Hand it to the producer.
    Send,
    /// Its partition is fenced behind an earlier failure: answer NotAttempted.
    Fenced,
}

/// A record handed to a producer and not resolved yet.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Outstanding {
    generation: u64,
    /// When the forwarder received it: the start of its delivery timeout.
    received_at: Instant,
}

/// A record received from the DB thread, waiting in the backlog for room in
/// librdkafka's queue, with when it was received (its delivery timeout runs
/// from then).
pub(crate) struct Queued {
    pub(crate) record: ForwardRecord,
    pub(crate) received_at: Instant,
}

struct HoldState {
    remaining: usize,
    deadline: Instant,
    hold: SyncHold,
}

/// The forwarder's bookkeeping, free of IO so it can be unit-tested: ordering
/// fences, the classification of failures, held responses, and the acks waiting
/// for room on the ack channel.
#[derive(Default)]
pub(crate) struct ForwarderCore {
    /// Local partition -> lowest failed offset not yet restarted.
    fences: HashMap<(i32, i32), i64>,
    /// Producer generations whose failures are collateral: one that became
    /// fatal (after its first failure, the cause) or one retired by a settings
    /// change (its outstanding records were purged).
    collateral_generations: BTreeSet<u64>,
    /// Records handed to a producer whose delivery has not resolved, with the
    /// generation that holds them and when the forwarder received them.
    outstanding: HashMap<RowKey, Outstanding>,
    /// The same records, oldest first (the delivery timeout's queue).
    by_age: BTreeSet<(Instant, RowKey)>,
    /// Records of a timed-out generation whose purge counts as a failed
    /// attempt: generation and error.
    timed_out: HashMap<RowKey, (u64, String)>,
    holds: HashMap<u64, HoldState>,
    waiting: HashMap<RowKey, Vec<u64>>,
    next_hold: u64,
    /// Acks not yet accepted by the (bounded) ack channel. Never dropped: a lost
    /// ack would leave the row in flight until the DB thread's eviction.
    pub(crate) acks: VecDeque<ForwardAck>,
}

impl ForwarderCore {
    /// Ordering fence check for a record about to be sent.
    pub(crate) fn admit(&mut self, record: &ForwardRecord) -> Admit {
        let part = record.row.partition();
        match self.fences.get(&part) {
            None => Admit::Send,
            Some(&fence) if record.restart || record.row.local_offset <= fence => {
                self.fences.remove(&part);
                Admit::Send
            }
            Some(_) => Admit::Fenced,
        }
    }

    /// A record answered without being sent (fence, shadow mode off, ...).
    pub(crate) fn not_attempted(&mut self, key: RowKey, why: &str) {
        self.acks.push_back(ForwardAck::new(
            key,
            ForwardOutcome::NotAttempted(why.to_string()),
        ));
    }

    fn fence(&mut self, key: RowKey) {
        let fence = self
            .fences
            .entry(key.partition())
            .or_insert(key.local_offset);
        *fence = (*fence).min(key.local_offset);
    }

    /// `key`, received at `received_at`, was handed to the producer of
    /// `generation`. Its delivery timeout runs from `received_at`, so the time
    /// it waited in the backlog counts.
    pub(crate) fn sent(&mut self, key: RowKey, generation: u64, received_at: Instant) {
        let entry = Outstanding {
            generation,
            received_at,
        };
        if let Some(old) = self.outstanding.insert(key, entry) {
            self.by_age.remove(&(old.received_at, key));
        }
        self.by_age.insert((received_at, key));
    }

    fn resolved(&mut self, key: RowKey) {
        if let Some(old) = self.outstanding.remove(&key) {
            self.by_age.remove(&(old.received_at, key));
        }
    }

    /// Answer every record at the front of `backlog` whose delivery timeout ran
    /// out by `now` while it waited for room in librdkafka's queue (see the
    /// module docs). The backlog is in the order the records were received, so
    /// these are a prefix of it. A record behind an earlier failure of its
    /// partition is collateral; otherwise it is the record its partition waits
    /// on and its attempt failed: it fences its partition, so the records behind
    /// it wait for its retry.
    pub(crate) fn expire_backlog(
        &mut self,
        backlog: &mut VecDeque<Queued>,
        now: Instant,
        timeout: Duration,
    ) {
        while let Some(queued) = backlog.pop_front() {
            if now.saturating_duration_since(queued.received_at) < timeout {
                backlog.push_front(queued);
                return;
            }
            let key = queued.record.row;
            if self.admit(&queued.record) == Admit::Fenced {
                self.not_attempted(key, "an earlier record of the partition failed");
                continue;
            }
            self.fence(key);
            self.acks.push_back(ForwardAck::new(
                key,
                ForwardOutcome::Failed(format!("not delivered within {timeout:?}")),
            ));
        }
    }

    /// The generation whose oldest outstanding record has waited `timeout` by
    /// `now`, if any. Generations already retired are skipped: their records
    /// were purged and resolve on their own.
    pub(crate) fn expired_generation(&self, now: Instant, timeout: Duration) -> Option<u64> {
        let (received_at, key) = self.by_age.iter().find(|(_, key)| {
            self.outstanding
                .get(key)
                .is_some_and(|o| !self.collateral_generations.contains(&o.generation))
        })?;
        let generation = self.outstanding.get(key)?.generation;
        (now.saturating_duration_since(*received_at) >= timeout).then_some(generation)
    }

    /// `generation` ran out of delivery time at `now`: fence its partitions,
    /// retire it, and pick the records whose purge counts as a failed attempt
    /// (see the module docs). The caller purges the producer.
    pub(crate) fn time_out_generation(&mut self, generation: u64, now: Instant, timeout: Duration) {
        let mut heads: HashMap<(i32, i32), (RowKey, Instant)> = HashMap::new();
        for (&key, o) in &self.outstanding {
            if o.generation != generation {
                continue;
            }
            let head = heads.entry(key.partition()).or_insert((key, o.received_at));
            if key.local_offset < head.0.local_offset {
                *head = (key, o.received_at);
            }
        }
        let reason = format!("not delivered within {timeout:?}");
        for (key, received_at) in heads.into_values() {
            if now.saturating_duration_since(received_at) >= timeout / 2 {
                self.timed_out.insert(key, (generation, reason.clone()));
            }
        }
        self.fence_outstanding(generation);
        self.retire_generation(generation);
    }

    /// The error of `key` if its failure on `generation` is a timed-out attempt.
    fn take_timed_out(&mut self, key: RowKey, generation: u64) -> Option<String> {
        match self.timed_out.get(&key) {
            Some(&(g, _)) if g == generation => self.timed_out.remove(&key).map(|(_, e)| e),
            _ => None,
        }
    }

    /// Fence every partition with a record outstanding on `generation`, at its
    /// lowest such offset. Called when the producer is replaced or goes fatal:
    /// those records fail, but their failures arrive later, and until then a
    /// later record of the partition must not go out on the replacement ahead
    /// of their retry. A record of the generation that is delivered after all
    /// leaves a fence the partition's next head (`restart`) lifts.
    pub(crate) fn fence_outstanding(&mut self, generation: u64) {
        let keys: Vec<RowKey> = self
            .outstanding
            .iter()
            .filter(|(_, o)| o.generation == generation)
            .map(|(&key, _)| key)
            .collect();
        for key in keys {
            self.fence(key);
        }
    }

    /// A producer generation was retired (rebuilt for new settings, its
    /// outstanding records purged): its failures are not delivery failures.
    pub(crate) fn retire_generation(&mut self, generation: u64) {
        self.collateral_generations.insert(generation);
        // Bounded: generations far in the past have no deliveries left.
        while self.collateral_generations.len() > 64 {
            self.collateral_generations.pop_first();
        }
    }

    /// One delivery attempt resolved. `producer_fatal` says whether the producer
    /// of `generation` is (now) in a fatal state.
    pub(crate) fn on_result(
        &mut self,
        key: RowKey,
        generation: u64,
        result: Result<i64, String>,
        producer_fatal: bool,
    ) {
        self.resolved(key);
        let timed_out = self.take_timed_out(key, generation);
        match result {
            Ok(external_offset) => {
                self.acks.push_back(ForwardAck::new(
                    key,
                    ForwardOutcome::Delivered(external_offset),
                ));
                self.resolve(key);
            }
            Err(error) => {
                self.fence(key);
                if let Some(reason) = timed_out {
                    self.acks
                        .push_back(ForwardAck::new(key, ForwardOutcome::Failed(reason)));
                    return;
                }
                let collateral = self.collateral_generations.contains(&generation);
                if producer_fatal && !collateral {
                    // The first failure seen of a generation that went fatal is
                    // taken as its cause; every later failure is collateral.
                    self.retire_generation(generation);
                    self.fence_outstanding(generation);
                }
                let outcome = if collateral {
                    ForwardOutcome::NotAttempted(format!(
                        "failed with the producer after an earlier error: {error}"
                    ))
                } else {
                    ForwardOutcome::Failed(error)
                };
                self.acks.push_back(ForwardAck::new(key, outcome));
            }
        }
    }

    /// Register a held response. Released at once if it waits for nothing.
    pub(crate) fn add_hold(&mut self, hold: SyncHold, now: Instant) {
        if hold.keys.is_empty() {
            hold.release.release();
            return;
        }
        let id = self.next_hold;
        self.next_hold += 1;
        for &key in &hold.keys {
            self.waiting.entry(key).or_default().push(id);
        }
        self.holds.insert(
            id,
            HoldState {
                remaining: hold.keys.len(),
                deadline: now + hold.timeout,
                hold,
            },
        );
    }

    fn resolve(&mut self, key: RowKey) {
        let Some(ids) = self.waiting.remove(&key) else {
            return;
        };
        for id in ids {
            let done = match self.holds.get_mut(&id) {
                Some(state) => {
                    state.remaining -= 1;
                    state.remaining == 0
                }
                None => false,
            };
            if done {
                self.finish_hold(id);
            }
        }
    }

    fn finish_hold(&mut self, id: u64) {
        if let Some(state) = self.holds.remove(&id) {
            for key in &state.hold.keys {
                if let Some(ids) = self.waiting.get_mut(key) {
                    ids.retain(|&other| other != id);
                    if ids.is_empty() {
                        self.waiting.remove(key);
                    }
                }
            }
            state.hold.release.release();
        }
    }

    /// Release every hold whose timeout has passed. Returns how many.
    pub(crate) fn expire_holds(&mut self, now: Instant) -> usize {
        let expired: Vec<u64> = self
            .holds
            .iter()
            .filter(|(_, s)| s.deadline <= now)
            .map(|(&id, _)| id)
            .collect();
        for &id in &expired {
            self.finish_hold(id);
        }
        expired.len()
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.holds.len()
    }

    /// Move queued acks onto the channel until it is full. Returns false when
    /// the DB thread is gone.
    pub(crate) fn flush_acks(&mut self, ack_tx: &Sender<ForwardAck>) -> bool {
        while let Some(ack) = self.acks.pop_front() {
            match ack_tx.try_send(ack) {
                Ok(()) => {}
                Err(TrySendError::Full(ack)) => {
                    self.acks.push_front(ack);
                    return true;
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.acks.clear();
                    return false;
                }
            }
        }
        true
    }
}

/// Every producer-relevant setting, so any change (not only the bootstrap
/// servers) rebuilds the producer.
fn producer_fingerprint(c: &ShadowConfig) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        c.bootstrap_servers,
        c.security_protocol,
        c.sasl_mechanism,
        c.sasl_username,
        c.sasl_password,
        c.ssl_ca_location,
        c.batch_size,
        c.linger_ms,
        c.retry_backoff_ms
    )
}

struct LiveProducer {
    producer: Arc<ShadowProducer>,
    generation: u64,
    fingerprint: String,
}

/// The producer the forwarder sends through, rebuilt when a setting changes
/// or it becomes fatal. Each build is a new *generation*, so a failure can be
/// traced to the producer that reported it.
#[derive(Default)]
struct ProducerSlot {
    live: Option<LiveProducer>,
    /// The latest generation built.
    generation: u64,
    /// The last build error, so a persistent one is logged once.
    last_error: Option<String>,
    /// Producers replaced after going fatal, by generation, so one whose
    /// records outlive the delivery timeout can still be purged.
    replaced: Vec<(u64, Weak<ShadowProducer>)>,
}

impl ProducerSlot {
    /// The producer for the current configuration and its generation,
    /// (re)built when a setting changed or the old one became fatal. `Err`
    /// says why there is none.
    fn current(
        &mut self,
        core: &mut ForwarderCore,
        config: &crate::config::Config,
    ) -> Result<(Arc<ShadowProducer>, u64), String> {
        let mut shadow = ShadowConfig::from_config(config);
        let fingerprint = producer_fingerprint(&shadow);
        if let Some(l) = &self.live {
            if l.fingerprint == fingerprint && l.producer.fatal_error().is_none() {
                return Ok((l.producer.clone(), l.generation));
            }
        }
        if let Some(old) = self.live.take() {
            // Its outstanding records fail; fence their partitions before the
            // replacement sends anything, so nothing overtakes their retry.
            core.fence_outstanding(old.generation);
            if let Some(e) = old.producer.fatal_error() {
                warn!("Shadow producer is fatal ({e}); rebuilding it");
                // librdkafka purges only the queues of a fatal producer: a request
                // already in flight that then fails retriably goes back to a
                // queue the fatal producer never serves, and with no
                // per-message timeout its record would never resolve (holding
                // the producer, its threads and its row). Purge in-flight
                // records too, so every outstanding record of the generation
                // resolves now; failures after the first are collateral.
                old.producer.purge_outstanding();
                self.replaced.retain(|(_, p)| p.strong_count() > 0);
                self.replaced
                    .push((old.generation, Arc::downgrade(&old.producer)));
            } else {
                info!("Shadow producer settings changed; rebuilding the producer");
                // Fail the old producer's outstanding records now (instead of
                // at the delivery timeout) and do not count them: they are
                // re-sent by the new producer, in order.
                core.retire_generation(old.generation);
                old.producer.purge_outstanding();
            }
        }
        // The same validation the config-loading path applies (it also
        // normalizes the protocol/mechanism names).
        let built = shadow
            .validate()
            .map_err(|e| format!("invalid shadow configuration: {e}"))
            .and_then(|()| {
                ShadowProducer::new(Arc::new(shadow.clone())).map_err(|e| format!("{e:?}"))
            });
        match built {
            Ok(producer) => {
                self.generation += 1;
                self.last_error = None;
                info!(
                    "Shadow producer created for bootstrap_servers='{}'",
                    shadow.bootstrap_servers
                );
                let producer = Arc::new(producer);
                self.live = Some(LiveProducer {
                    producer: producer.clone(),
                    generation: self.generation,
                    fingerprint,
                });
                Ok((producer, self.generation))
            }
            Err(e) => {
                // Logged once per distinct error, not once per record.
                if self.last_error.as_deref() != Some(e.as_str()) {
                    warn!("Shadow producer unavailable: {e}");
                    self.last_error = Some(e.clone());
                }
                Err(e)
            }
        }
    }
}

impl ProducerSlot {
    /// `generation` ran out of delivery time: fail every record outstanding on
    /// it now (see the module docs). The live producer is dropped, so the next
    /// record builds a new generation.
    fn time_out(
        &mut self,
        core: &mut ForwarderCore,
        generation: u64,
        now: Instant,
        timeout: Duration,
    ) {
        core.time_out_generation(generation, now, timeout);
        let producer = if self
            .live
            .as_ref()
            .is_some_and(|l| l.generation == generation)
        {
            self.live.take().map(|l| l.producer)
        } else {
            self.replaced
                .iter()
                .find(|(g, _)| *g == generation)
                .and_then(|(_, p)| p.upgrade())
        };
        warn!(
            "Shadow forward not delivered within {timeout:?}; failing every record outstanding \
             on the producer and rebuilding it"
        );
        if let Some(producer) = producer {
            producer.purge_outstanding();
        }
    }
}

/// The delivery timeout: [`DELIVERY_TIMEOUT`], unless the test GUC
/// `pg_kafka.test_shadow_delivery_timeout_ms` shortens it.
fn delivery_timeout(config: &crate::config::Config) -> Duration {
    match u64::try_from(config.test_shadow_delivery_timeout_ms) {
        Ok(ms) if ms > 0 => Duration::from_millis(ms),
        _ => DELIVERY_TIMEOUT,
    }
}

/// One resolved delivery.
struct Delivery {
    key: RowKey,
    generation: u64,
    result: Result<i64, String>,
    producer: Arc<ShadowProducer>,
}

type Deliveries = FuturesUnordered<BoxFuture<'static, Delivery>>;

/// Shadow forwarder task (see the module docs).
///
/// Reads the configuration through the `RuntimeContext` on every record, so
/// shadow mode and every producer setting can change at runtime
/// (ALTER SYSTEM + pg_reload_conf()).
pub(crate) async fn run_shadow_forwarder(
    forward_rx: Receiver<ForwardRequest>,
    ack_tx: Sender<ForwardAck>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
) {
    info!("Shadow forwarder task started (pipelined)");

    // Bridge the DB thread's crossbeam channel into the runtime in batches, on
    // one blocking-pool thread, so the async loop below can wait for new records,
    // delivery reports and hold timeouts at once.
    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<Vec<ForwardRequest>>(4);
    tokio::task::spawn_blocking(move || bridge_requests(&forward_rx, &batch_tx));

    let mut core = ForwarderCore::default();
    let mut slot = ProducerSlot::default();
    let mut deliveries = Deliveries::new();
    let mut ticks: u64 = 0;
    let mut tick = tokio::time::interval(HOLD_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut backlog: VecDeque<Queued> = VecDeque::new();
    loop {
        if !core.flush_acks(&ack_tx) {
            info!("Forward ack channel closed, stopping shadow forwarder");
            break;
        }
        tokio::select! {
            batch = batch_rx.recv(), if backlog.len() < MAX_BACKLOG => {
                let Some(batch) = batch else {
                    info!("Forward channel disconnected, stopping shadow forwarder");
                    break;
                };
                let received_at = Instant::now();
                for message in batch {
                    match message {
                        ForwardRequest::Hold(hold) => core.add_hold(hold, received_at),
                        ForwardRequest::Record(record) => {
                            backlog.push_back(Queued { record, received_at });
                        }
                    }
                }
                pump(&mut backlog, &runtime_context, &mut core, &mut slot, &mut deliveries);
            }
            Some(delivery) = deliveries.next(), if !deliveries.is_empty() => {
                if let Err(e) = &delivery.result {
                    debug!("Forward FAILED {:?}: {e}", delivery.key);
                }
                let fatal = delivery.result.is_err() && delivery.producer.fatal_error().is_some();
                core.on_result(delivery.key, delivery.generation, delivery.result, fatal);
                // A delivery frees room in librdkafka's queue.
                pump(&mut backlog, &runtime_context, &mut core, &mut slot, &mut deliveries);
            }
            () = tokio::time::sleep(QUEUE_FULL_BACKOFF), if !backlog.is_empty() => {
                pump(&mut backlog, &runtime_context, &mut core, &mut slot, &mut deliveries);
            }
            _ = tick.tick() => {
                let now = Instant::now();
                core.expire_holds(now);
                let timeout = delivery_timeout(&runtime_context.config());
                // A retired generation is skipped by the next lookup; bounded
                // well below the retired generations `ForwarderCore` keeps.
                for _ in 0..MAX_TIMEOUTS_PER_TICK {
                    let Some(generation) = core.expired_generation(now, timeout) else {
                        break;
                    };
                    slot.time_out(&mut core, generation, now, timeout);
                }
                // After the generations, so a record behind a timed-out one
                // of its partition is collateral, not a second failure.
                core.expire_backlog(&mut backlog, now, timeout);
                pump(&mut backlog, &runtime_context, &mut core, &mut slot, &mut deliveries);
                // A settings change rebuilds the producer (and purges its
                // outstanding records) even when no new record arrives to
                // notice it.
                ticks = ticks.wrapping_add(1);
                if ticks.is_multiple_of(SETTINGS_CHECK_TICKS) && slot.live.is_some() {
                    let config = runtime_context.config();
                    if config.shadow_mode_enabled && !config.shadow_bootstrap_servers.is_empty() {
                        // A failed rebuild is logged by `current` and retried on the
                        // next record or check.
                        let _ = slot.current(&mut core, &config);
                    }
                }
            }
        }
    }

    info!("Shadow forwarder task stopped");
}

/// Move requests from the DB thread's channel to the async task, up to
/// `BRIDGE_BATCH` per hop, until either side is gone.
fn bridge_requests(
    forward_rx: &Receiver<ForwardRequest>,
    batch_tx: &tokio::sync::mpsc::Sender<Vec<ForwardRequest>>,
) {
    loop {
        match forward_rx.recv_timeout(BRIDGE_POLL) {
            Ok(first) => {
                let mut batch = vec![first];
                while batch.len() < BRIDGE_BATCH {
                    match forward_rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
                if batch_tx.blocking_send(batch).is_err() {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if batch_tx.is_closed() {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

/// Hand the backlog to the producer, in order, until it is empty or
/// librdkafka's queue is full (the rest then waits for the next attempt).
fn pump(
    backlog: &mut VecDeque<Queued>,
    runtime_context: &crate::kafka::RuntimeContext,
    core: &mut ForwarderCore,
    slot: &mut ProducerSlot,
    deliveries: &mut Deliveries,
) {
    while let Some(queued) = backlog.pop_front() {
        if let Some(queued) = forward_one(queued, runtime_context, core, slot, deliveries) {
            backlog.push_front(queued);
            return;
        }
    }
}

/// Send one record (or answer it without sending). Returns the record when
/// librdkafka's queue is full, so it is retried before any later record.
fn forward_one(
    queued: Queued,
    runtime_context: &crate::kafka::RuntimeContext,
    core: &mut ForwarderCore,
    slot: &mut ProducerSlot,
    deliveries: &mut Deliveries,
) -> Option<Queued> {
    let record = &queued.record;
    let key = record.row;
    let config = runtime_context.config();
    if !config.shadow_mode_enabled || config.shadow_bootstrap_servers.is_empty() {
        core.not_attempted(key, "shadow mode is not enabled or not configured");
        return None;
    }
    let (producer, generation) = match slot.current(core, &config) {
        Ok(p) => p,
        Err(e) => {
            // No producer can be built: a real delivery failure (it is retried
            // and eventually dead-lettered, like any other).
            core.on_result(key, slot.generation, Err(e), false);
            return None;
        }
    };
    // Checked on every attempt: a record that waited for queue room is fenced
    // if an earlier record of its partition failed meanwhile.
    if core.admit(record) == Admit::Fenced {
        core.not_attempted(key, "an earlier record of the partition failed");
        return None;
    }
    match producer.enqueue(record) {
        Ok(future) => {
            core.sent(key, generation, queued.received_at);
            // Test hook (pg_kafka.test_forward_ack_delay_ms, 0 in
            // production): delay the ack past the outbox retry lease. Only
            // this delivery's own future waits; the pipeline keeps going.
            let ack_delay =
                Duration::from_millis(u64::try_from(config.test_forward_ack_delay_ms).unwrap_or(0));
            deliveries.push(Box::pin(async move {
                let result = match future.await {
                    Ok(Ok((_partition, offset))) => Ok(offset),
                    Ok(Err((e, _message))) => Err(e.to_string()),
                    Err(_canceled) => Err("delivery canceled".to_string()),
                };
                if !ack_delay.is_zero() {
                    tokio::time::sleep(ack_delay).await;
                }
                Delivery {
                    key,
                    generation,
                    result,
                    producer,
                }
            }));
            None
        }
        Err(EnqueueError::QueueFull) => {
            // Backpressure: serve librdkafka's callbacks so its queue drains.
            producer.poll(Duration::ZERO);
            Some(queued)
        }
        Err(EnqueueError::Refused(e)) => {
            let fatal = producer.fatal_error().is_some();
            core.on_result(key, generation, Err(e), fatal);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::shadow::HeldRelease;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rec(p: i32, o: i64, restart: bool) -> ForwardRecord {
        ForwardRecord {
            row: RowKey::new(1, p, o),
            topic_name: "t".into(),
            key: None,
            value: None,
            headers: Vec::new(),
            timestamp_ms: None,
            restart,
        }
    }

    fn outcomes(core: &mut ForwarderCore) -> Vec<(RowKey, ForwardOutcome)> {
        core.acks.drain(..).map(|a| (a.row, a.outcome)).collect()
    }

    #[test]
    fn failure_fences_later_records_of_the_partition_only() {
        let mut core = ForwarderCore::default();
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        core.on_result(RowKey::new(1, 0, 5), 1, Err("boom".into()), false);
        // Later record of partition 0: fenced (would overtake the retry of 5).
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Fenced);
        // Another partition is unaffected.
        assert_eq!(core.admit(&rec(1, 6, false)), Admit::Send);
        // The retry of 5 restarts the partition.
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Send);
    }

    #[test]
    fn partition_head_lifts_the_fence_after_a_dead_letter() {
        let mut core = ForwarderCore::default();
        core.on_result(RowKey::new(1, 0, 5), 1, Err("poison".into()), false);
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Fenced);
        // 5 was dead-lettered: the DB marks 6 as the partition's head.
        assert_eq!(core.admit(&rec(0, 6, true)), Admit::Send);
    }

    #[test]
    fn fence_keeps_the_lowest_failed_offset() {
        let mut core = ForwarderCore::default();
        core.on_result(RowKey::new(1, 0, 9), 1, Err("x".into()), false);
        core.on_result(RowKey::new(1, 0, 7), 1, Err("x".into()), false);
        assert_eq!(core.admit(&rec(0, 8, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Send);
    }

    #[test]
    fn only_the_first_failure_of_a_fatal_generation_counts() {
        let mut core = ForwarderCore::default();
        core.on_result(RowKey::new(1, 0, 1), 3, Err("root cause".into()), true);
        core.on_result(RowKey::new(1, 1, 1), 3, Err("fatal".into()), true);
        core.on_result(RowKey::new(1, 0, 2), 3, Err("fatal".into()), true);
        let got = outcomes(&mut core);
        assert_eq!(got[0].1, ForwardOutcome::Failed("root cause".into()));
        assert!(matches!(got[1].1, ForwardOutcome::NotAttempted(_)));
        assert!(matches!(got[2].1, ForwardOutcome::NotAttempted(_)));
        // A late failure of that generation is still collateral; a new
        // producer generation counts failures again.
        core.on_result(RowKey::new(1, 0, 4), 3, Err("late".into()), false);
        assert!(matches!(
            outcomes(&mut core)[0].1,
            ForwardOutcome::NotAttempted(_)
        ));
        core.on_result(RowKey::new(1, 0, 3), 4, Err("again".into()), false);
        assert_eq!(
            outcomes(&mut core)[0].1,
            ForwardOutcome::Failed("again".into())
        );
    }

    #[test]
    fn failures_of_a_retired_generation_are_not_attempts() {
        let mut core = ForwarderCore::default();
        core.retire_generation(2);
        core.on_result(RowKey::new(1, 0, 1), 2, Err("purged".into()), false);
        core.on_result(RowKey::new(1, 0, 2), 3, Err("real".into()), false);
        let got = outcomes(&mut core);
        assert!(matches!(got[0].1, ForwardOutcome::NotAttempted(_)));
        assert_eq!(got[1].1, ForwardOutcome::Failed("real".into()));
        for g in 10..200 {
            core.retire_generation(g);
        }
        assert!(core.collateral_generations.len() <= 64);
    }

    /// A replaced producer's outstanding records fail later; until they do, a
    /// later record of their partitions must not go out on the replacement.
    #[test]
    fn replacing_a_producer_fences_its_outstanding_partitions() {
        let mut core = ForwarderCore::default();
        core.sent(RowKey::new(1, 0, 5), 1, Instant::now());
        core.sent(RowKey::new(1, 0, 6), 1, Instant::now());
        core.sent(RowKey::new(1, 1, 3), 1, Instant::now());
        core.on_result(RowKey::new(1, 1, 3), 1, Ok(9), false);
        core.sent(RowKey::new(1, 2, 8), 2, Instant::now());
        core.fence_outstanding(1);
        // Partition 0 waits for the retry of 5, the lowest outstanding offset.
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Fenced);
        // Partition 1's record was delivered and partition 2's is on another
        // generation: neither is fenced.
        assert_eq!(core.admit(&rec(1, 4, false)), Admit::Send);
        assert_eq!(core.admit(&rec(2, 9, false)), Admit::Send);
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Send);
    }

    /// The first failure of a generation that went fatal fences the partitions
    /// of its other outstanding records too.
    #[test]
    fn a_fatal_failure_fences_the_generations_other_partitions() {
        let mut core = ForwarderCore::default();
        core.sent(RowKey::new(1, 0, 1), 3, Instant::now());
        core.sent(RowKey::new(1, 1, 4), 3, Instant::now());
        core.on_result(RowKey::new(1, 0, 1), 3, Err("root cause".into()), true);
        assert_eq!(core.admit(&rec(1, 5, false)), Admit::Fenced);
        assert_eq!(core.outstanding.len(), 1);
    }

    const TIMEOUT: Duration = Duration::from_secs(120);

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_generation_expires_when_its_oldest_record_reaches_the_timeout() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        core.sent(RowKey::new(1, 0, 1), 1, t0);
        core.sent(RowKey::new(1, 0, 2), 1, t0 + secs(60));
        assert_eq!(core.expired_generation(t0 + secs(119), TIMEOUT), None);
        assert_eq!(core.expired_generation(t0 + TIMEOUT, TIMEOUT), Some(1));
        // Once the oldest is delivered, the next one's own clock applies.
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(0), false);
        assert_eq!(core.expired_generation(t0 + secs(179), TIMEOUT), None);
        assert_eq!(core.expired_generation(t0 + secs(180), TIMEOUT), Some(1));
        core.on_result(RowKey::new(1, 0, 2), 1, Ok(1), false);
        assert_eq!(core.expired_generation(t0 + secs(999), TIMEOUT), None);
    }

    /// The staggered-deadline case: a record whose own deadline is later
    /// fails with the earlier one, and the partition restarts at the earlier
    /// one, so the later one can never be delivered ahead of its retry.
    #[test]
    fn a_timeout_fails_the_whole_generation_and_fences_at_the_first_record() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        core.sent(RowKey::new(1, 0, 1), 1, t0);
        core.sent(RowKey::new(1, 0, 2), 1, t0 + secs(60));
        let now = t0 + TIMEOUT;
        let generation = core.expired_generation(now, TIMEOUT).unwrap();
        core.time_out_generation(generation, now, TIMEOUT);
        // A later record of the partition is held back until 1 restarts it.
        assert_eq!(core.admit(&rec(0, 3, false)), Admit::Fenced);
        // The purge reports: only the partition's head counts as an attempt.
        core.on_result(RowKey::new(1, 0, 2), 1, Err("purged".into()), true);
        core.on_result(RowKey::new(1, 0, 1), 1, Err("purged".into()), true);
        let got = outcomes(&mut core);
        assert!(
            matches!(got[0].1, ForwardOutcome::NotAttempted(_)),
            "{got:?}"
        );
        assert_eq!(
            got[1].1,
            ForwardOutcome::Failed("not delivered within 120s".into())
        );
        // Nothing of the retired generation can expire again.
        assert_eq!(core.expired_generation(now + secs(999), TIMEOUT), None);
        assert_eq!(core.admit(&rec(0, 2, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(0, 1, false)), Admit::Send);
        assert_eq!(core.admit(&rec(0, 2, false)), Admit::Send);
    }

    /// Each stuck partition's head counts; a head sent less than half a
    /// timeout ago (a partition that was flowing) and the records behind a
    /// head are collateral.
    #[test]
    fn a_timeout_counts_each_stuck_head_but_not_young_heads_or_followers() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        // Partition 0 stuck since t0, partition 1 since t0 + 50 s, partition 2
        // only has a record sent 10 s before the timeout.
        core.sent(RowKey::new(1, 0, 4), 7, t0);
        core.sent(RowKey::new(1, 0, 5), 7, t0 + secs(1));
        core.sent(RowKey::new(1, 1, 9), 7, t0 + secs(50));
        core.sent(RowKey::new(1, 1, 10), 7, t0 + secs(51));
        core.sent(RowKey::new(1, 2, 3), 7, t0 + secs(110));
        // Another generation is not touched.
        core.sent(RowKey::new(1, 3, 0), 8, t0 + secs(100));
        let now = t0 + TIMEOUT;
        core.time_out_generation(7, now, TIMEOUT);
        for (p, o) in [(0, 5), (1, 10), (2, 3), (1, 9), (0, 4)] {
            core.on_result(RowKey::new(1, p, o), 7, Err("purged".into()), true);
        }
        let failed: Vec<RowKey> = outcomes(&mut core)
            .into_iter()
            .filter(|(_, o)| matches!(o, ForwardOutcome::Failed(_)))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(failed, vec![RowKey::new(1, 1, 9), RowKey::new(1, 0, 4)]);
        // Partition 3's record on generation 8 keeps its own clock.
        assert_eq!(core.expired_generation(now, TIMEOUT), None);
        assert_eq!(core.expired_generation(t0 + secs(220), TIMEOUT), Some(8));
        assert_eq!(core.admit(&rec(3, 1, false)), Admit::Send);
    }

    /// A persistent outage: every cycle fails the stuck head once (it is
    /// re-sent within the outbox backoff, so it is always older than half a
    /// timeout at the next retirement), so it reaches the dead-letter cap after
    /// that many cycles while the record behind it never fails.
    #[test]
    fn a_persistent_outage_fails_the_head_once_per_cycle() {
        let mut core = ForwarderCore::default();
        let mut now = Instant::now();
        let (mut head_failures, mut follower_failures) = (0, 0);
        for generation in 1..=10 {
            core.sent(RowKey::new(1, 0, 1), generation, now);
            core.sent(RowKey::new(1, 0, 2), generation, now);
            now += TIMEOUT;
            assert_eq!(core.expired_generation(now, TIMEOUT), Some(generation));
            core.time_out_generation(generation, now, TIMEOUT);
            core.on_result(RowKey::new(1, 0, 1), generation, Err("purged".into()), true);
            core.on_result(RowKey::new(1, 0, 2), generation, Err("purged".into()), true);
            for (key, outcome) in outcomes(&mut core) {
                if matches!(outcome, ForwardOutcome::Failed(_)) {
                    if key.local_offset == 1 {
                        head_failures += 1;
                    } else {
                        follower_failures += 1;
                    }
                }
            }
            // The outbox backoff (at most 30 s) before the re-send.
            now += secs(30);
        }
        assert_eq!((head_failures, follower_failures), (10, 0));
    }

    /// A timed-out record whose delivery report says it was delivered after
    /// all (it landed just before the purge) is acked as delivered.
    #[test]
    fn a_timed_out_record_delivered_before_the_purge_is_delivered() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        core.sent(RowKey::new(1, 0, 1), 1, t0);
        core.time_out_generation(1, t0 + TIMEOUT, TIMEOUT);
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(42), false);
        assert_eq!(
            outcomes(&mut core),
            vec![(RowKey::new(1, 0, 1), ForwardOutcome::Delivered(42))]
        );
        assert!(core.timed_out.is_empty());
        // A later failure of the same row on a new generation is judged afresh.
        core.on_result(RowKey::new(1, 0, 1), 2, Err("real".into()), false);
        assert_eq!(
            outcomes(&mut core)[0].1,
            ForwardOutcome::Failed("real".into())
        );
    }

    #[test]
    fn re_sending_an_outstanding_key_keeps_one_entry() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        core.sent(RowKey::new(1, 0, 1), 1, t0);
        core.sent(RowKey::new(1, 0, 1), 2, t0 + secs(100));
        assert_eq!(core.by_age.len(), 1);
        assert_eq!(core.expired_generation(t0 + TIMEOUT, TIMEOUT), None);
        core.on_result(RowKey::new(1, 0, 1), 2, Ok(0), false);
        assert!(core.by_age.is_empty() && core.outstanding.is_empty());
    }

    fn queued(p: i32, o: i64, received_at: Instant) -> Queued {
        Queued {
            record: rec(p, o, false),
            received_at,
        }
    }

    /// The in-flight eviction of the DB thread (`INFLIGHT_EVICT_AFTER`).
    const EVICT: Duration = Duration::from_secs(150);

    /// A record that waited in the backlog behind a full queue keeps the time
    /// it waited: once it reaches a replacement producer it times out at its
    /// arrival + the timeout, not a whole timeout after it got room. Timed
    /// from the enqueue it would stay outstanding until about two timeouts
    /// after its dispatch (240 s), past the DB thread's in-flight eviction
    /// (150 s), and the outbox would claim and send it a second time.
    #[test]
    fn a_record_that_waited_in_the_backlog_keeps_its_arrival_as_its_deadline() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        // Partition 0 fills the queue of generation 1; partition 1 waits.
        core.sent(RowKey::new(1, 0, 1), 1, t0);
        let mut backlog = VecDeque::from([queued(1, 1, t0 + secs(1)), queued(1, 2, t0 + secs(1))]);
        let now = t0 + TIMEOUT;
        assert_eq!(core.expired_generation(now, TIMEOUT), Some(1));
        core.time_out_generation(1, now, TIMEOUT);
        core.expire_backlog(&mut backlog, now, TIMEOUT);
        assert_eq!(backlog.len(), 2, "1 s of their timeout is left");
        // Generation 2 takes them (forward_one passes the arrival time).
        for q in backlog.drain(..) {
            assert_eq!(core.admit(&q.record), Admit::Send);
            core.sent(q.record.row, 2, q.received_at);
        }
        let deadline = t0 + secs(1) + TIMEOUT;
        assert_eq!(
            core.expired_generation(
                t0 + secs(1) + TIMEOUT.saturating_sub(Duration::from_millis(1)),
                TIMEOUT
            ),
            None
        );
        assert_eq!(core.expired_generation(deadline, TIMEOUT), Some(2));
        assert!(deadline - (t0 + secs(1)) < EVICT);
        core.time_out_generation(2, deadline, TIMEOUT);
        core.on_result(RowKey::new(1, 1, 2), 2, Err("purged".into()), true);
        core.on_result(RowKey::new(1, 1, 1), 2, Err("purged".into()), true);
        let got = outcomes(&mut core);
        assert!(
            matches!(got[0].1, ForwardOutcome::NotAttempted(_)),
            "{got:?}"
        );
        assert_eq!(
            got[1].1,
            ForwardOutcome::Failed("not delivered within 120s".into()),
            "the partition's head waited a whole timeout: a failed attempt"
        );
    }

    /// A record whose timeout runs out while it still waits in the backlog is
    /// answered there: the first record a partition waits on fails and fences
    /// the partition, the records behind it (or behind an earlier failure)
    /// are collateral, and younger records keep waiting.
    #[test]
    fn a_record_whose_timeout_runs_out_in_the_backlog_is_answered_there() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        // Partition 2 already waits for the retry of its offset 1.
        core.on_result(RowKey::new(1, 2, 1), 1, Err("boom".into()), false);
        outcomes(&mut core);
        let mut backlog = VecDeque::from([
            queued(0, 5, t0),
            queued(0, 6, t0),
            queued(2, 4, t0),
            queued(1, 3, t0 + secs(10)),
        ]);
        core.expire_backlog(
            &mut backlog,
            t0 + TIMEOUT.saturating_sub(Duration::from_millis(1)),
            TIMEOUT,
        );
        assert!(core.acks.is_empty());
        assert_eq!(backlog.len(), 4);
        core.expire_backlog(&mut backlog, t0 + TIMEOUT, TIMEOUT);
        let got = outcomes(&mut core);
        assert_eq!(
            got[0],
            (
                RowKey::new(1, 0, 5),
                ForwardOutcome::Failed("not delivered within 120s".into())
            )
        );
        assert!(
            matches!(got[1].1, ForwardOutcome::NotAttempted(_)),
            "{got:?}"
        );
        assert!(
            matches!(got[2].1, ForwardOutcome::NotAttempted(_)),
            "{got:?}"
        );
        assert_eq!(got.len(), 3);
        assert_eq!(backlog.len(), 1, "partition 1's record has 10 s left");
        // Partition 0 restarts at 5, not at a later record.
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        assert_eq!(core.admit(&rec(1, 3, false)), Admit::Send);
    }

    /// A persistent outage behind a queue that holds only a few records:
    /// whatever waits in the backlog, every record the DB thread dispatches is
    /// answered within the timeout of its arrival (plus the 20 ms check), so
    /// before its in-flight entry is evicted. A model of the forwarder's loop
    /// (`pump`, the tick's timeout checks, the purge) over the real core.
    #[test]
    fn every_record_is_answered_within_the_timeout_of_its_arrival() {
        const QUEUE: usize = 3;
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        let mut now = t0;
        let mut generation = 1u64;
        let mut backlog: VecDeque<Queued> = VecDeque::new();
        // Row -> when it was dispatched (the DB thread's in-flight entry).
        let mut dispatched: HashMap<RowKey, Instant> = HashMap::new();
        // Row -> when it may be dispatched again (the outbox backoff).
        let mut due: Vec<(RowKey, Instant)> = (0..4)
            .flat_map(|p| (0..5).map(move |o| (RowKey::new(1, p, o), t0)))
            .collect();
        let mut longest = Duration::ZERO;
        while now < t0 + TIMEOUT * 10 {
            // The DB thread dispatches the due rows, partition by partition.
            due.sort_by_key(|(k, _)| *k);
            let (ready, later): (Vec<_>, Vec<_>) = due.drain(..).partition(|&(_, at)| at <= now);
            due = later;
            for (key, _) in ready {
                dispatched.insert(key, now);
                backlog.push_back(Queued {
                    record: rec(key.partition_id, key.local_offset, false),
                    received_at: now,
                });
            }
            // The tick.
            while let Some(g) = core.expired_generation(now, TIMEOUT) {
                core.time_out_generation(g, now, TIMEOUT);
                let purged: Vec<RowKey> = core
                    .outstanding
                    .iter()
                    .filter(|(_, o)| o.generation == g)
                    .map(|(&k, _)| k)
                    .collect();
                for key in purged {
                    core.on_result(key, g, Err("purged".into()), true);
                }
                generation += 1;
            }
            core.expire_backlog(&mut backlog, now, TIMEOUT);
            // `pump`: nothing is ever delivered, so the queue only frees up
            // when its generation is purged.
            while let Some(q) = backlog.pop_front() {
                if core.admit(&q.record) == Admit::Fenced {
                    core.not_attempted(q.record.row, "fenced");
                    continue;
                }
                let on_queue = core
                    .outstanding
                    .values()
                    .filter(|o| o.generation == generation)
                    .count();
                if on_queue == QUEUE {
                    backlog.push_front(q);
                    break;
                }
                core.sent(q.record.row, generation, q.received_at);
            }
            // The acks reach the DB thread; a failed row backs off 1 s.
            for (key, outcome) in outcomes(&mut core) {
                let started = dispatched.remove(&key).expect("one answer per dispatch");
                longest = longest.max(now - started);
                let backoff = if matches!(outcome, ForwardOutcome::Failed(_)) {
                    secs(1)
                } else {
                    Duration::ZERO
                };
                due.push((key, now + backoff));
            }
            now += HOLD_TICK;
        }
        println!("longest attempt: {longest:?}");
        assert!(longest >= TIMEOUT, "the queue must have held records back");
        assert!(
            longest <= TIMEOUT + HOLD_TICK,
            "an attempt took {longest:?}"
        );
        assert!(longest < EVICT);
    }

    #[test]
    fn the_test_guc_shortens_the_delivery_timeout() {
        let mut config = crate::config::Config::load();
        assert_eq!(delivery_timeout(&config), DELIVERY_TIMEOUT);
        config.test_shadow_delivery_timeout_ms = 6_000;
        assert_eq!(delivery_timeout(&config), Duration::from_secs(6));
    }

    #[test]
    fn success_is_acked_with_the_external_offset() {
        let mut core = ForwarderCore::default();
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(42), false);
        assert_eq!(
            outcomes(&mut core),
            vec![(RowKey::new(1, 0, 1), ForwardOutcome::Delivered(42))]
        );
    }

    fn counting_hold(keys: Vec<RowKey>, timeout: Duration, n: &Arc<AtomicUsize>) -> SyncHold {
        let n = n.clone();
        SyncHold {
            keys,
            timeout,
            release: HeldRelease::new(move || {
                n.fetch_add(1, Ordering::SeqCst);
            }),
        }
    }

    #[test]
    fn hold_is_released_when_every_record_is_delivered() {
        let mut core = ForwarderCore::default();
        let n = Arc::new(AtomicUsize::new(0));
        let now = Instant::now();
        core.add_hold(
            counting_hold(
                vec![RowKey::new(1, 0, 1), RowKey::new(1, 0, 2)],
                Duration::from_secs(5),
                &n,
            ),
            now,
        );
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(0), false);
        assert_eq!(n.load(Ordering::SeqCst), 0);
        // A failure does not release it: the retry may still land in time.
        core.on_result(RowKey::new(1, 0, 2), 1, Err("transient".into()), false);
        assert_eq!(n.load(Ordering::SeqCst), 0);
        core.on_result(RowKey::new(1, 0, 2), 1, Ok(1), false);
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert_eq!(core.held(), 0);
    }

    #[test]
    fn hold_is_released_at_its_timeout() {
        let mut core = ForwarderCore::default();
        let n = Arc::new(AtomicUsize::new(0));
        let now = Instant::now();
        core.add_hold(
            counting_hold(vec![RowKey::new(1, 0, 1)], Duration::from_millis(100), &n),
            now,
        );
        assert_eq!(core.expire_holds(now + Duration::from_millis(50)), 0);
        assert_eq!(core.expire_holds(now + Duration::from_millis(100)), 1);
        assert_eq!(n.load(Ordering::SeqCst), 1);
        // A late delivery report finds nothing to release.
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(0), false);
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn two_holds_on_one_record_are_both_released() {
        let mut core = ForwarderCore::default();
        let n = Arc::new(AtomicUsize::new(0));
        let now = Instant::now();
        core.add_hold(
            counting_hold(vec![RowKey::new(1, 0, 1)], Duration::from_secs(5), &n),
            now,
        );
        core.add_hold(
            counting_hold(vec![RowKey::new(1, 0, 1)], Duration::from_secs(5), &n),
            now,
        );
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(0), false);
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn empty_hold_releases_immediately() {
        let mut core = ForwarderCore::default();
        let n = Arc::new(AtomicUsize::new(0));
        core.add_hold(
            counting_hold(vec![], Duration::from_secs(5), &n),
            Instant::now(),
        );
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn acks_wait_for_room_instead_of_being_dropped() {
        let mut core = ForwarderCore::default();
        let (tx, rx) = crossbeam_channel::bounded(1);
        core.on_result(RowKey::new(1, 0, 1), 1, Ok(1), false);
        core.on_result(RowKey::new(1, 0, 2), 1, Ok(2), false);
        assert!(core.flush_acks(&tx));
        assert_eq!(core.acks.len(), 1, "second ack kept, not dropped");
        assert_eq!(rx.try_recv().unwrap().row.local_offset, 1);
        assert!(core.flush_acks(&tx));
        assert_eq!(rx.try_recv().unwrap().row.local_offset, 2);
        drop(rx);
        core.on_result(RowKey::new(1, 0, 3), 1, Ok(3), false);
        assert!(!core.flush_acks(&tx), "closed channel reported");
    }

    #[test]
    fn fingerprint_covers_every_producer_setting() {
        let base = ShadowConfig {
            enabled: true,
            bootstrap_servers: "a:1".into(),
            ..Default::default()
        };
        let f = producer_fingerprint(&base);
        for changed in [
            ShadowConfig {
                security_protocol: "SSL".into(),
                ..base.clone()
            },
            ShadowConfig {
                linger_ms: base.linger_ms + 1,
                ..base.clone()
            },
            ShadowConfig {
                batch_size: base.batch_size + 1,
                ..base.clone()
            },
            ShadowConfig {
                retry_backoff_ms: base.retry_backoff_ms + 1,
                ..base.clone()
            },
            ShadowConfig {
                sasl_password: "p".into(),
                ..base.clone()
            },
        ] {
            assert_ne!(producer_fingerprint(&changed), f);
        }
    }
}
