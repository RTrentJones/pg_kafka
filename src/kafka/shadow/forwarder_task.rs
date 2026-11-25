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
//! accepts it: the time it waits in the backlog (see Backpressure) counts. A
//! record is handed to a producer only while at least [`send_margin`] (a
//! quarter of the timeout) of it is left; a record still in the backlog with
//! less left is answered there ([`ForwarderCore::expire_backlog`], checked
//! before every attempt to send). So the forwarder's own queueing never puts a
//! record on a producer just before its deadline, where its generation would
//! run out of time soon after and purge every other record on it, those of
//! healthy partitions included: a generation times out only when a record had
//! at least the margin on the producer and was not delivered. Every record the
//! DB thread dispatches is therefore answered within about [`DELIVERY_TIMEOUT`]
//! of its dispatch (from the backlog by three quarters of it, from a producer by
//! all of it, plus the 20 ms timeout check), which the DB thread's in-flight
//! eviction (`INFLIGHT_EVICT_AFTER` in `store.rs`) relies on: an entry evicted
//! while its record was still held here would let the outbox claim the row
//! again (counting a lost attempt that never failed) and send it a second time
//! beside the first.
//!
//! Which of the purged records count as failed attempts: the lowest outstanding
//! record of each local partition (the one the partition waits on) that has been
//! held for at least half the timeout. The records behind it are collateral
//! (the fence would hold them back anyway), and so is a head that has not had a
//! fair window yet (received less than half a timeout ago, for example a
//! healthy partition whose records were flowing). A record that runs out of
//! time in the backlog is collateral (`NotAttempted`, and its partition is
//! fenced so its later records keep their order) behind an earlier failure of
//! its partition, behind an earlier record of its partition still on a
//! producer, or behind a record of any partition received no later than it and
//! still outstanding on a live generation. In the last case the queue was full
//! of older records (for example those of one partition whose external topic
//! is unavailable), and their generation times out, freeing the queue, by the
//! older record's deadline, which is before this one's: the record is claimed
//! again at once, its new deadline is past that timeout, and it goes out on the
//! replacement producer: a partition is not charged an attempt for waiting
//! behind another partition's stuck records. Only a record that found no room with
//! nothing older on a live generation (the queue held by a retired generation's
//! records) fails there, if it is the first record its partition waits on. In
//! a persistent outage every stuck partition's head is held for close to a whole
//! timeout at each retirement (the outbox backoff, at most 30 s, is under half
//! of it), so it fails once per cycle and is dead-lettered after
//! `shadow_max_retries` cycles; the records behind it are not penalized, and
//! the next one becomes the head once it is dead-lettered.
//!
//! The "fair window" is measured from a record's receipt, which includes its time
//! in the backlog, not from its hand-off to a producer. A head that waited in the
//! backlog and reached a producer late, seconds before its generation times out,
//! is still charged when that generation's purge fails it. This is deliberate: a
//! record that spent any time on a producer and was not delivered means its own
//! broker or partition is unreachable (a healthy partition's records are delivered
//! in milliseconds and are never purged), and in a persistent outage it brings the
//! dead letter forward by at most one cycle of `shadow_max_retries`.
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
//! answered there once less than [`send_margin`] of their delivery timeout is
//! left, see Delivery timeout). The loop keeps
//! serving delivery reports, held-response timeouts, the ack flush and the
//! settings check meanwhile, so a full queue delays only the records behind it.
//!
//! ## Panics
//!
//! The task runs detached on the network thread, so a panic in it would end
//! forwarding for every shadow topic with nothing to notice. Sending a record
//! (the work that handles what a client put into it) runs under `catch_unwind`:
//! a panic there fails that record, a counted attempt, and fences its
//! partition. The loop itself runs supervised: after a panic anywhere else the
//! records it held are answered `NotAttempted` and fence their partitions (see
//! [`ForwarderCore::recover_from_panic`]), its producers are dropped (which
//! purges them), and it runs again after [`RESTART_BACKOFF`].
//!
//! ## Held sync-mode responses
//!
//! A [`SyncHold`] carries the buffered response of a committed sync-mode
//! produce. It is released as soon as every one of its records is delivered,
//! or when its timeout expires, so a slow or unreachable external cluster delays
//! only those responses, never the DB thread.

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, TrySendError};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeSet;
use std::collections::{HashMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

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
/// Pause before the forwarder loop runs again after a panic, so a panic that
/// repeats does not spin the network thread.
const RESTART_BACKOFF: Duration = Duration::from_secs(1);

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
    /// ack would leave the row in flight until the DB thread's eviction. (A
    /// [`Self::reset`] drops them: the DB thread ignores them anyway.)
    pub(crate) acks: VecDeque<ForwardAck>,
    /// The schema epoch of the last [`ForwardRequest::Reset`]: every ack sent is
    /// stamped with it.
    pub(crate) epoch: u64,
}

impl ForwarderCore {
    /// The schema the forwards so far came from was dropped
    /// ([`ForwardRequest::Reset`]): forget every fence, outstanding record,
    /// timed-out or retired generation and queued ack of it, and release its held
    /// responses (their records are gone with the schema). A re-created schema
    /// reuses row keys, so none of this may apply to its rows. Acks sent from now
    /// on carry `epoch`; the caller drops the backlog and the deliveries of
    /// records sent before.
    pub(crate) fn reset(&mut self, epoch: u64) {
        let holds: Vec<u64> = self.holds.keys().copied().collect();
        for id in holds {
            self.finish_hold(id);
        }
        *self = Self {
            next_hold: self.next_hold,
            epoch,
            ..Self::default()
        };
    }

    /// Ordering fence check for a record about to be sent. A record below the
    /// fence goes out ahead of the failed record, as it should, but leaves the
    /// fence in place. The fence is lifted by the failed record's retry, or by
    /// a partition head (`restart`) above it: the claim's head is the lowest
    /// pending row, so a head above the fence means the failed record is no
    /// longer pending (delivered or dead-lettered). A head below it does not:
    /// a replay can make a lower row due while the failed record still waits
    /// for its retry, and lifting the fence then would let the later records
    /// of the same claim overtake that retry.
    pub(crate) fn admit(&mut self, record: &ForwardRecord) -> Admit {
        let part = record.row.partition();
        let offset = record.row.local_offset;
        match self.fences.get(&part) {
            None => Admit::Send,
            Some(&fence) if offset < fence => Admit::Send,
            Some(&fence) if offset == fence || record.restart => {
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

    /// Answer every record at the front of `backlog` that has less than
    /// [`send_margin`] of its delivery timeout left by `now`, while it still
    /// waits for room in librdkafka's queue (see the module docs): it is not
    /// handed to a producer, where it would run its generation out of time
    /// soon after and purge every other record on it. The backlog is in the
    /// order the records were received, so these are a prefix of it.
    ///
    /// Only the record a partition waits on counts a failed attempt, and only
    /// when nothing else explains why it found no room: a record behind an
    /// earlier failure of its partition (the fence), behind an earlier record
    /// of its partition still outstanding on a producer, or behind a record of
    /// any partition received no later than it and still outstanding on a live
    /// generation (the queue is full of older records, possibly of a partition
    /// whose external topic is unavailable: that generation times out and
    /// frees the queue by that record's deadline, before this one's) is
    /// collateral. Either way it fences its partition, so the records behind it
    /// wait for its retry.
    pub(crate) fn expire_backlog(
        &mut self,
        backlog: &mut VecDeque<Queued>,
        now: Instant,
        timeout: Duration,
    ) {
        let expire_at = timeout.saturating_sub(send_margin(timeout));
        // Lowest outstanding offset per partition, and when the oldest record
        // outstanding on a live generation was received: found at the first
        // expiry.
        let mut lowest: Option<HashMap<(i32, i32), i64>> = None;
        let mut oldest_live: Option<Option<Instant>> = None;
        while let Some(queued) = backlog.pop_front() {
            if now.saturating_duration_since(queued.received_at) < expire_at {
                backlog.push_front(queued);
                return;
            }
            let key = queued.record.row;
            if self.admit(&queued.record) == Admit::Fenced {
                self.not_attempted(key, "an earlier record of the partition failed");
                continue;
            }
            let lowest = lowest.get_or_insert_with(|| {
                let mut lowest = HashMap::new();
                for k in self.outstanding.keys() {
                    let offset = lowest.entry(k.partition()).or_insert(k.local_offset);
                    *offset = (*offset).min(k.local_offset);
                }
                lowest
            });
            let oldest_live = *oldest_live
                .get_or_insert_with(|| self.oldest_live().map(|(received_at, _)| received_at));
            self.fence(key);
            if lowest
                .get(&key.partition())
                .is_some_and(|&offset| offset < key.local_offset)
            {
                self.not_attempted(
                    key,
                    "queued behind an earlier record of the partition that is not delivered yet",
                );
                continue;
            }
            if oldest_live.is_some_and(|oldest| oldest <= queued.received_at) {
                self.not_attempted(
                    key,
                    "librdkafka's queue was full of earlier records not delivered yet",
                );
                continue;
            }
            self.acks.push_back(ForwardAck::new(
                key,
                ForwardOutcome::Failed(format!(
                    "not sent within {expire_at:?} of its {timeout:?} delivery timeout \
                     (librdkafka's queue was full)"
                )),
            ));
        }
    }

    /// Hand the backlog to `send`, in order, after answering the records too
    /// close to their deadline to be sent ([`Self::expire_backlog`]). `send`
    /// returns the record when librdkafka's queue is full: it goes back to the
    /// front of the backlog and the rest waits for the next attempt.
    pub(crate) fn pump_backlog(
        &mut self,
        backlog: &mut VecDeque<Queued>,
        now: Instant,
        timeout: Duration,
        mut send: impl FnMut(&mut Self, Queued) -> Option<Queued>,
    ) {
        self.expire_backlog(backlog, now, timeout);
        while let Some(queued) = backlog.pop_front() {
            let key = queued.record.row;
            // A panic while sending one record (a bug reached by what a client
            // put into it) fails that record and leaves the forwarder running.
            match catch_unwind(AssertUnwindSafe(|| send(self, queued))) {
                Ok(Some(queued)) => {
                    backlog.push_front(queued);
                    return;
                }
                Ok(None) => {}
                Err(_) => self.send_panicked(key),
            }
        }
    }

    /// Sending `key` panicked. If it reached a producer first, its delivery
    /// report is lost with the panic: it is no longer tracked. It is answered as
    /// a failed attempt (retried and eventually dead-lettered like any other
    /// failure, so a record that always panics does not block its partition for
    /// good) and fences its partition, so no later record overtakes its retry.
    pub(crate) fn send_panicked(&mut self, key: RowKey) {
        error!("Shadow forwarder panicked while sending {key:?}; failing the record");
        self.resolved(key);
        self.fence(key);
        self.acks.push_back(ForwardAck::new(
            key,
            ForwardOutcome::Failed("the shadow forwarder panicked while sending it".to_string()),
        ));
    }

    /// The forwarder loop panicked and is about to run again with new producers:
    /// the old ones (dropped with the loop, which purges them) and their delivery
    /// reports are gone. Every record the loop held, on a producer or in the
    /// `backlog`, is answered `NotAttempted` (its row becomes due again, without
    /// a counted attempt) and fences its partition, so a later record of the
    /// partition still on its way does not overtake the re-send. The producer
    /// generations are forgotten (the new loop numbers its producers from the
    /// start). Fences, held responses and queued acks stay: the DB thread still
    /// waits for them.
    pub(crate) fn recover_from_panic(&mut self, backlog: &mut VecDeque<Queued>) {
        let mut lost: Vec<RowKey> = self.outstanding.keys().copied().collect();
        lost.extend(backlog.drain(..).map(|queued| queued.record.row));
        lost.sort_unstable();
        self.outstanding.clear();
        self.by_age.clear();
        self.timed_out.clear();
        self.collateral_generations.clear();
        for key in lost {
            self.fence(key);
            self.not_attempted(key, "the shadow forwarder restarted");
        }
    }

    /// The oldest record outstanding on a generation not retired yet: when it
    /// was received and its generation.
    fn oldest_live(&self) -> Option<(Instant, u64)> {
        self.by_age.iter().find_map(|(received_at, key)| {
            self.outstanding
                .get(key)
                .filter(|o| !self.collateral_generations.contains(&o.generation))
                .map(|o| (*received_at, o.generation))
        })
    }

    /// The generation whose oldest outstanding record has waited `timeout` by
    /// `now`, if any. Generations already retired are skipped: their records
    /// were purged and resolve on their own.
    pub(crate) fn expired_generation(&self, now: Instant, timeout: Duration) -> Option<u64> {
        let (received_at, generation) = self.oldest_live()?;
        (now.saturating_duration_since(received_at) >= timeout).then_some(generation)
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

    /// No producer could be built for `record` (invalid or unbuildable
    /// settings, also after the previous producer was retired by a settings
    /// change or went fatal): a real delivery failure, retried and eventually
    /// dead-lettered like any other. It is not attributed to a producer
    /// generation: the record never reached one, and the latest generation may
    /// be retired already, which would make the failure collateral
    /// (`NotAttempted`, never counted) and leave the record to be claimed again
    /// and again without ever reaching the retry cap.
    ///
    /// As for a record that runs out of time in the backlog
    /// ([`Self::expire_backlog`]), only the record its partition waits on is
    /// charged the attempt: the first record of the partition the fence admits.
    /// It fences the partition, so the records behind it are answered
    /// `NotAttempted` (a partition is not charged an attempt for waiting behind
    /// another) and wait for its retry. Charging every record would dead-letter
    /// a whole backlog within a few backoffs of one bad setting, and switch
    /// `external_only` consumers to local reads with it, when the cause is one
    /// the operator fixes once.
    pub(crate) fn producer_unavailable(&mut self, record: &ForwardRecord, error: String) {
        let key = record.row;
        if self.admit(record) == Admit::Fenced {
            self.not_attempted(key, "an earlier record of the partition failed");
            return;
        }
        self.fence(key);
        self.acks
            .push_back(ForwardAck::new(key, ForwardOutcome::Failed(error)));
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
        while let Some(mut ack) = self.acks.pop_front() {
            ack.epoch = self.epoch;
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
    /// The schema of every outstanding record was dropped: purge them from every
    /// producer (their delivery reports are ignored, see [`Delivery::epoch`]), so
    /// none waits on a producer forever with no delivery timeout watching it. The
    /// live producer is dropped, so the next record builds a new generation.
    fn reset(&mut self) {
        if let Some(live) = self.live.take() {
            live.producer.purge_outstanding();
        }
        for (_, producer) in self.replaced.drain(..) {
            if let Some(producer) = producer.upgrade() {
                producer.purge_outstanding();
            }
        }
    }

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

/// The share of its delivery timeout a record must have left to be handed to a
/// producer: a quarter (30 s of [`DELIVERY_TIMEOUT`]). A record with less left
/// is answered from the backlog instead ([`ForwarderCore::expire_backlog`]), so
/// a record the forwarder's own queueing held back never reaches a producer
/// about to run out of time: its generation's timeout would purge every other
/// record on it, those of healthy partitions included. Every record on a
/// producer therefore has at least this long to be delivered.
const SEND_MARGIN_DIVISOR: u32 = 4;

/// See [`SEND_MARGIN_DIVISOR`].
pub(crate) fn send_margin(timeout: Duration) -> Duration {
    timeout / SEND_MARGIN_DIVISOR
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
    /// The schema epoch the record was sent in: a report of an earlier epoch
    /// (the schema was dropped meanwhile) is ignored.
    epoch: u64,
    result: Result<i64, String>,
    producer: Arc<ShadowProducer>,
}

type Deliveries = FuturesUnordered<BoxFuture<'static, Delivery>>;

/// Shadow forwarder task (see the module docs).
///
/// Reads the configuration through the `RuntimeContext` on every record, so
/// shadow mode and every producer setting can change at runtime
/// (ALTER SYSTEM + pg_reload_conf()).
///
/// The loop runs supervised: a panic in it (it runs detached, so nothing else
/// would notice) is caught, the records it held are answered
/// ([`ForwarderCore::recover_from_panic`]) and the loop runs again, so one bad
/// record or bug cannot stop forwarding for every shadow topic.
pub(crate) async fn run_shadow_forwarder(
    forward_rx: Receiver<ForwardRequest>,
    ack_tx: Sender<ForwardAck>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
) {
    info!("Shadow forwarder task started (pipelined)");

    // Bridge the DB thread's crossbeam channel into the runtime in batches, on
    // one blocking-pool thread, so the async loop below can wait for new records,
    // delivery reports and hold timeouts at once. The bridge outlives a restart of
    // the loop: it ends when either channel end is gone.
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Vec<ForwardRequest>>(4);
    tokio::task::spawn_blocking(move || bridge_requests(&forward_rx, &batch_tx));

    supervise(Forwarder::new(batch_rx, ack_tx, runtime_context)).await;

    info!("Shadow forwarder task stopped");
}

/// What the forwarder loop keeps across a restart after a panic: its inputs and
/// outputs, the bookkeeping the DB thread's view depends on, and the backlog
/// (records received and not answered yet). Producers and pending delivery
/// reports belong to one run of the loop.
struct Forwarder {
    batch_rx: tokio::sync::mpsc::Receiver<Vec<ForwardRequest>>,
    ack_tx: Sender<ForwardAck>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
    core: ForwarderCore,
    backlog: VecDeque<Queued>,
    /// Test hook: panic once after taking the next batch.
    #[cfg(test)]
    panic_on_next_batch: bool,
}

impl Forwarder {
    fn new(
        batch_rx: tokio::sync::mpsc::Receiver<Vec<ForwardRequest>>,
        ack_tx: Sender<ForwardAck>,
        runtime_context: Arc<crate::kafka::RuntimeContext>,
    ) -> Self {
        Self {
            batch_rx,
            ack_tx,
            runtime_context,
            core: ForwarderCore::default(),
            backlog: VecDeque::new(),
            #[cfg(test)]
            panic_on_next_batch: false,
        }
    }
}

/// Run the forwarder loop until it stops, running it again after a panic.
async fn supervise(mut forwarder: Forwarder) {
    loop {
        let run = AssertUnwindSafe(forward(&mut forwarder))
            .catch_unwind()
            .await;
        if run.is_ok() {
            return;
        }
        error!(
            "Shadow forwarder panicked; answering the records it held as not attempted \
             and restarting it"
        );
        forwarder.core.recover_from_panic(&mut forwarder.backlog);
        tokio::time::sleep(RESTART_BACKOFF).await;
    }
}

/// The forwarder loop (see the module docs). Returns when the DB thread is gone.
async fn forward(forwarder: &mut Forwarder) {
    let Forwarder {
        batch_rx,
        ack_tx,
        runtime_context,
        core,
        backlog,
        ..
    } = forwarder;
    #[cfg(test)]
    let panic_on_next_batch = &mut forwarder.panic_on_next_batch;
    let runtime_context: &crate::kafka::RuntimeContext = runtime_context;
    let mut slot = ProducerSlot::default();
    let mut deliveries = Deliveries::new();
    let mut ticks: u64 = 0;
    let mut tick = tokio::time::interval(HOLD_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if !core.flush_acks(ack_tx) {
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
                        ForwardRequest::Reset(epoch) => {
                            info!("Shadow schema dropped; forgetting its forwards");
                            backlog.clear();
                            slot.reset();
                            core.reset(epoch);
                        }
                    }
                }
                #[cfg(test)]
                assert!(!std::mem::take(panic_on_next_batch), "injected forwarder panic");
                pump(backlog, runtime_context, core, &mut slot, &mut deliveries);
            }
            Some(delivery) = deliveries.next(), if !deliveries.is_empty() => {
                if let Err(e) = &delivery.result {
                    debug!("Forward FAILED {:?}: {e}", delivery.key);
                }
                if delivery.epoch == core.epoch {
                    let fatal =
                        delivery.result.is_err() && delivery.producer.fatal_error().is_some();
                    core.on_result(delivery.key, delivery.generation, delivery.result, fatal);
                }
                // A delivery frees room in librdkafka's queue.
                pump(backlog, runtime_context, core, &mut slot, &mut deliveries);
            }
            () = tokio::time::sleep(QUEUE_FULL_BACKOFF), if !backlog.is_empty() => {
                pump(backlog, runtime_context, core, &mut slot, &mut deliveries);
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
                    slot.time_out(core, generation, now, timeout);
                }
                // After the generations (`pump` expires the backlog first),
                // so a record behind a timed-out one of its partition is
                // collateral, not a second failure.
                pump(backlog, runtime_context, core, &mut slot, &mut deliveries);
                // A settings change rebuilds the producer (and purges its
                // outstanding records) even when no new record arrives to
                // notice it.
                ticks = ticks.wrapping_add(1);
                if ticks.is_multiple_of(SETTINGS_CHECK_TICKS) && slot.live.is_some() {
                    let config = runtime_context.config();
                    if config.shadow_mode_enabled && !config.shadow_bootstrap_servers.is_empty() {
                        // A failed rebuild is logged by `current` and retried on the
                        // next record or check.
                        let _ = slot.current(core, &config);
                    }
                }
            }
        }
    }
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
/// Records with too little of their delivery timeout left are answered from
/// the backlog first, on every path that sends ([`ForwarderCore::pump_backlog`]).
fn pump(
    backlog: &mut VecDeque<Queued>,
    runtime_context: &crate::kafka::RuntimeContext,
    core: &mut ForwarderCore,
    slot: &mut ProducerSlot,
    deliveries: &mut Deliveries,
) {
    let timeout = delivery_timeout(&runtime_context.config());
    core.pump_backlog(backlog, Instant::now(), timeout, |core, queued| {
        forward_one(queued, runtime_context, core, slot, deliveries)
    });
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
            core.producer_unavailable(record, e);
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
            // production): delay the ack, to check that a slow ack is not
            // re-sent. Only this delivery's own future waits; the pipeline
            // keeps going.
            let ack_delay =
                Duration::from_millis(u64::try_from(config.test_forward_ack_delay_ms).unwrap_or(0));
            let epoch = core.epoch;
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
                    epoch,
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
    fn unbuildable_producer_charges_only_each_partitions_head() {
        let mut core = ForwarderCore::default();
        let failed = |o: &ForwardOutcome| matches!(o, ForwardOutcome::Failed(_));
        // One claim, two partitions: only the record each partition waits on
        // has its attempt counted; the rest wait behind it, uncounted.
        for r in [
            rec(0, 5, true),
            rec(0, 6, false),
            rec(1, 3, true),
            rec(0, 7, false),
        ] {
            core.producer_unavailable(&r, "invalid shadow configuration".into());
        }
        let got: Vec<(i32, i64, bool)> = outcomes(&mut core)
            .iter()
            .map(|(k, o)| (k.partition_id, k.local_offset, failed(o)))
            .collect();
        assert_eq!(
            got,
            [(0, 5, true), (0, 6, false), (1, 3, true), (0, 7, false)]
        );
        // The retry of 5: charged again; 6 still waits.
        core.producer_unavailable(&rec(0, 5, true), "x".into());
        core.producer_unavailable(&rec(0, 6, false), "x".into());
        let got: Vec<bool> = outcomes(&mut core).iter().map(|(_, o)| failed(o)).collect();
        assert_eq!(got, [true, false]);
        // 5 was dead-lettered: 6 is the partition's head and has its turn.
        core.producer_unavailable(&rec(0, 6, true), "x".into());
        core.producer_unavailable(&rec(0, 7, false), "x".into());
        let got: Vec<bool> = outcomes(&mut core).iter().map(|(_, o)| failed(o)).collect();
        assert_eq!(got, [true, false]);
        // Once a producer can be built, nothing of the partition overtakes 6.
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(0, 6, true)), Admit::Send);
    }

    #[test]
    fn reset_forgets_the_dropped_schemas_state_and_stamps_acks() {
        let mut core = ForwarderCore::default();
        let released = Arc::new(AtomicUsize::new(0));
        let r = released.clone();
        core.add_hold(
            SyncHold {
                keys: vec![RowKey::new(1, 0, 5)],
                timeout: Duration::from_secs(60),
                release: HeldRelease::new(move || {
                    r.fetch_add(1, Ordering::SeqCst);
                }),
            },
            Instant::now(),
        );
        core.sent(RowKey::new(1, 0, 6), 1, Instant::now());
        core.on_result(RowKey::new(1, 0, 5), 1, Err("boom".into()), false);
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Fenced);
        core.reset(2);
        // The old schema's hold is answered, its fence, outstanding record and
        // queued acks are gone: offset 7 of the re-created schema goes out.
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert_eq!(core.held(), 0);
        assert!(core.acks.is_empty());
        assert!(core
            .expired_generation(Instant::now(), Duration::ZERO)
            .is_none());
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Send);
        // Acks sent from now on carry the new epoch.
        core.on_result(RowKey::new(1, 0, 7), 3, Ok(1), false);
        let (tx, rx) = crossbeam_channel::unbounded();
        assert!(core.flush_acks(&tx));
        assert_eq!(rx.try_recv().map(|a| a.epoch), Ok(2));
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
    fn a_lower_head_does_not_lift_the_fence_of_a_later_failure() {
        let mut core = ForwarderCore::default();
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        core.on_result(RowKey::new(1, 0, 5), 1, Err("boom".into()), false);
        // Before the DB thread applies 5's failure, a replay makes 3 due: the
        // next claim dispatches 3 (now the partition's head) and 6. 3 may go
        // out, but 6 must not overtake the retry of 5.
        assert_eq!(core.admit(&rec(0, 3, true)), Admit::Send);
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Fenced);
        // 5's retry lifts the fence.
        assert_eq!(core.admit(&rec(0, 5, true)), Admit::Send);
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Send);
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

    /// Shadow settings for `forward_one`, with an unreachable bootstrap server
    /// (building a producer does not connect).
    fn unreachable_config(security_protocol: &str) -> crate::config::Config {
        let mut config = crate::config::Config::load();
        config.shadow_mode_enabled = true;
        config.shadow_bootstrap_servers = "127.0.0.1:1".into();
        config.shadow_security_protocol = security_protocol.into();
        config
    }

    /// Send `records` of partition 0 through `forward_one` with `config`.
    fn forward_records(
        config: crate::config::Config,
        core: &mut ForwarderCore,
        slot: &mut ProducerSlot,
        records: &[(i64, bool)],
    ) -> Vec<(RowKey, ForwardOutcome)> {
        let context = crate::kafka::RuntimeContext::new(config);
        let mut deliveries = Deliveries::new();
        for &(offset, restart) in records {
            let queued = Queued {
                record: rec(0, offset, restart),
                received_at: Instant::now(),
            };
            assert!(forward_one(queued, &context, core, slot, &mut deliveries).is_none());
        }
        assert!(deliveries.is_empty(), "nothing may be sent");
        outcomes(core)
    }

    /// `got` answers `failed` (offsets of partition 0 that failed with the
    /// configuration error) and `waiting` (answered `NotAttempted`), in order.
    fn assert_unbuildable_failures(
        got: &[(RowKey, ForwardOutcome)],
        failed: &[i64],
        waiting: &[i64],
    ) {
        let keys: Vec<RowKey> = got.iter().map(|(k, _)| *k).collect();
        let expected: Vec<RowKey> = failed
            .iter()
            .chain(waiting)
            .map(|&o| RowKey::new(1, 0, o))
            .collect();
        assert_eq!(keys, expected);
        for (i, (key, outcome)) in got.iter().enumerate() {
            if i < failed.len() {
                assert!(
                    matches!(outcome, ForwardOutcome::Failed(e) if e.contains("invalid shadow configuration")),
                    "{key:?}: {outcome:?}"
                );
            } else {
                assert!(
                    matches!(outcome, ForwardOutcome::NotAttempted(_)),
                    "{key:?}: {outcome:?}"
                );
            }
        }
    }

    /// Settings no producer can be built from are a delivery failure of the
    /// record its partition waits on (the records behind it wait, uncounted),
    /// also right after a settings change has retired the previous producer:
    /// the failure is not that retired generation's (whose failures are
    /// collateral and never counted), or the record would be claimed again and
    /// again and never dead-lettered.
    #[test]
    fn an_unbuildable_producer_fails_records_after_a_settings_change() {
        let mut core = ForwarderCore::default();
        let mut slot = ProducerSlot::default();
        slot.current(&mut core, &unreachable_config("PLAINTEXT"))
            .expect("a producer for valid settings");
        assert_eq!(slot.generation, 1);

        // The change retires generation 1; no replacement can be built.
        let got = forward_records(
            unreachable_config("BOGUS"),
            &mut core,
            &mut slot,
            &[(0, true), (1, false), (2, false)],
        );
        assert_unbuildable_failures(&got, &[0], &[1, 2]);
        // Every later attempt fails the same way.
        let got = forward_records(
            unreachable_config("BOGUS"),
            &mut core,
            &mut slot,
            &[(0, true)],
        );
        assert_unbuildable_failures(&got, &[0], &[]);

        // Valid settings again: the partition is fenced at its first failed
        // record, so nothing later goes out ahead of its retry.
        let got = forward_records(
            unreachable_config("PLAINTEXT"),
            &mut core,
            &mut slot,
            &[(1, false)],
        );
        assert!(
            matches!(got[0].1, ForwardOutcome::NotAttempted(_)),
            "{got:?}"
        );
        assert_eq!(slot.generation, 2);
    }

    /// The same settings at a fresh start (no producer ever built) fail the
    /// same way.
    #[test]
    fn an_unbuildable_producer_fails_records_at_a_fresh_start() {
        let mut core = ForwarderCore::default();
        let mut slot = ProducerSlot::default();
        let got = forward_records(
            unreachable_config("BOGUS"),
            &mut core,
            &mut slot,
            &[(0, true), (1, false)],
        );
        assert_unbuildable_failures(&got, &[0], &[1]);
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
        let arrived = t0 + secs(40);
        let mut backlog = VecDeque::from([queued(1, 1, arrived), queued(1, 2, arrived)]);
        let now = t0 + TIMEOUT;
        assert_eq!(core.expired_generation(now, TIMEOUT), Some(1));
        core.time_out_generation(1, now, TIMEOUT);
        core.expire_backlog(&mut backlog, now, TIMEOUT);
        assert_eq!(
            backlog.len(),
            2,
            "40 s of their timeout is left, more than the margin"
        );
        // Generation 2 takes them (forward_one passes the arrival time).
        for q in backlog.drain(..) {
            assert_eq!(core.admit(&q.record), Admit::Send);
            core.sent(q.record.row, 2, q.received_at);
        }
        let deadline = arrived + TIMEOUT;
        assert_eq!(
            core.expired_generation(
                arrived + TIMEOUT.saturating_sub(Duration::from_millis(1)),
                TIMEOUT
            ),
            None
        );
        assert_eq!(core.expired_generation(deadline, TIMEOUT), Some(2));
        assert!(deadline - arrived < EVICT);
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

    /// A record with less than the margin of its timeout left while it still
    /// waits in the backlog is answered there: the first record a partition
    /// waits on fails and fences the partition, the records behind it (or
    /// behind an earlier failure, or behind an earlier record of the partition
    /// still on a producer) are collateral, and younger records keep waiting.
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
            queued(3, 2, t0),
            queued(1, 3, t0 + secs(10)),
        ]);
        // Partition 3's offset 1 is still on a producer: partition 3 waits on it.
        // Its generation is retired (purged, its report still to come), so it
        // is not a live record the others wait behind.
        core.sent(RowKey::new(1, 3, 1), 1, t0);
        core.retire_generation(1);
        // Less than the margin (30 s) left: 90 s after arrival.
        assert_eq!(send_margin(TIMEOUT), secs(30));
        let expire_at = secs(90);
        core.expire_backlog(
            &mut backlog,
            t0 + expire_at.saturating_sub(Duration::from_millis(1)),
            TIMEOUT,
        );
        assert!(core.acks.is_empty());
        assert_eq!(backlog.len(), 5);
        core.expire_backlog(&mut backlog, t0 + expire_at, TIMEOUT);
        let got = outcomes(&mut core);
        assert_eq!(got.len(), 4, "{got:?}");
        assert_eq!(got[0].0, RowKey::new(1, 0, 5));
        assert!(matches!(got[0].1, ForwardOutcome::Failed(_)), "{got:?}");
        for (key, outcome) in &got[1..] {
            assert!(
                matches!(outcome, ForwardOutcome::NotAttempted(_)),
                "{key:?}: {got:?}"
            );
        }
        assert_eq!(backlog.len(), 1, "partition 1's record has 10 s left");
        // Partition 0 restarts at 5, not at a later record; partition 3 at 1
        // (the record still outstanding) or 2, never later.
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        assert_eq!(core.admit(&rec(1, 3, false)), Admit::Send);
        assert_eq!(core.admit(&rec(3, 3, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(3, 2, false)), Admit::Send);
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
            // `pump`: nothing is ever delivered, so the queue only frees up
            // when its generation is purged.
            core.pump_backlog(&mut backlog, now, TIMEOUT, |core, q| {
                if core.admit(&q.record) == Admit::Fenced {
                    core.not_attempted(q.record.row, "fenced");
                    return None;
                }
                let on_queue = core
                    .outstanding
                    .values()
                    .filter(|o| o.generation == generation)
                    .count();
                if on_queue == QUEUE {
                    return Some(q);
                }
                core.sent(q.record.row, generation, q.received_at);
                None
            });
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
        // A record never answered would not show in `longest`: every one still
        // waiting must be younger than the bound too.
        assert!(!dispatched.is_empty(), "the outage keeps records waiting");
        for (key, started) in &dispatched {
            let waiting = now - *started;
            assert!(
                waiting < TIMEOUT + HOLD_TICK,
                "{key:?} has waited {waiting:?} without an answer"
            );
        }
    }

    /// A model of `pump` for the tests below: room for every record.
    fn send_all(core: &mut ForwarderCore, q: &Queued, generation: u64) -> Option<Queued> {
        if core.admit(&q.record) == Admit::Fenced {
            core.not_attempted(q.record.row, "fenced");
        } else {
            core.sent(q.record.row, generation, q.received_at);
        }
        None
    }

    /// The next tick's timeout checks and purge (as `run_shadow_forwarder` and
    /// `ProducerSlot::time_out` do them). Returns the timed-out generations.
    fn tick(core: &mut ForwarderCore, backlog: &mut VecDeque<Queued>, now: Instant) -> Vec<u64> {
        let mut timed_out = Vec::new();
        while let Some(g) = core.expired_generation(now, TIMEOUT) {
            core.time_out_generation(g, now, TIMEOUT);
            timed_out.push(g);
        }
        core.pump_backlog(backlog, now, TIMEOUT, |core, q| send_all(core, &q, 2));
        let purged: Vec<(RowKey, u64)> = core
            .outstanding
            .iter()
            .filter(|(_, o)| timed_out.contains(&o.generation))
            .map(|(&k, o)| (k, o.generation))
            .collect();
        for (key, g) in purged {
            core.on_result(key, g, Err("purged".into()), false);
        }
        timed_out
    }

    /// Generation 1 is healthy: partitions 1-5 each have a record received
    /// 70 s ago (a deep but flowing backlog) and a fresh one. Partition 0's
    /// record waited its whole timeout in the backlog, and a delivery frees
    /// room just past its deadline, before the next tick. Sent to generation 1,
    /// it would time the generation out at that tick and purge (and fail) the
    /// healthy partitions' records with it: only partition 0's record may fail.
    #[test]
    fn a_record_past_its_deadline_is_not_sent_to_a_healthy_producer() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        // 5 ms past partition 0's deadline.
        let now = t0 + TIMEOUT + Duration::from_millis(5);
        for p in 1..=5 {
            // 70 s and 1 ms before `now`.
            core.sent(
                RowKey::new(1, p, 10),
                1,
                t0 + secs(50) + Duration::from_millis(5),
            );
            core.sent(
                RowKey::new(1, p, 11),
                1,
                t0 + TIMEOUT + Duration::from_millis(4),
            );
        }
        let mut backlog = VecDeque::from([queued(0, 3, t0)]);
        // The delivery arm's `pump`.
        core.pump_backlog(&mut backlog, now, TIMEOUT, |core, q| send_all(core, &q, 1));
        let timed_out = tick(&mut core, &mut backlog, t0 + TIMEOUT + HOLD_TICK);
        assert_eq!(timed_out, Vec::<u64>::new(), "the healthy generation lives");
        let got = outcomes(&mut core);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].0, RowKey::new(1, 0, 3));
        assert!(matches!(got[0].1, ForwardOutcome::Failed(_)), "{got:?}");
        assert_eq!(
            core.outstanding.len(),
            10,
            "the healthy records stay outstanding"
        );
        // Partition 0 waits for the retry of 3.
        assert_eq!(core.admit(&rec(0, 4, false)), Admit::Fenced);
    }

    /// A record with less than the margin of its timeout left when room frees
    /// up is answered from the backlog: on the producer it would time out its
    /// generation within the margin, purging the healthy records on it. One
    /// with more left is sent and keeps its arrival as its deadline.
    #[test]
    fn a_record_with_less_than_the_margin_left_is_answered_from_the_backlog() {
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        let margin = send_margin(TIMEOUT);
        assert_eq!(margin, secs(30));
        let now = t0 + TIMEOUT;
        // A healthy partition's fresh record on generation 1.
        core.sent(RowKey::new(1, 1, 0), 1, now);
        // Partition 0's record has 1 ms less than the margin left at `now`,
        // partition 2's 60 s, more than the margin.
        let late = t0 + Duration::from_millis(29_999);
        let early = t0 + secs(60);
        let mut backlog = VecDeque::from([queued(0, 3, late), queued(2, 7, early)]);
        core.pump_backlog(&mut backlog, now, TIMEOUT, |core, q| send_all(core, &q, 1));
        assert!(backlog.is_empty());
        let got = outcomes(&mut core);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].0, RowKey::new(1, 0, 3));
        assert!(matches!(got[0].1, ForwardOutcome::Failed(_)), "{got:?}");
        assert!(!core.outstanding.contains_key(&RowKey::new(1, 0, 3)));
        // The late record's deadline passes without purging generation 1.
        let late_deadline = late + TIMEOUT + HOLD_TICK;
        assert_eq!(
            tick(&mut core, &mut backlog, late_deadline),
            Vec::<u64>::new()
        );
        assert_eq!(core.outstanding.len(), 2);
        assert_eq!(outcomes(&mut core), Vec::new());
        // The record sent with more than the margin left times out at its
        // arrival + the timeout (were it never delivered).
        assert_eq!(tick(&mut core, &mut backlog, early + TIMEOUT), vec![1]);
    }

    /// One partition's external topic is unavailable and its records fill
    /// librdkafka's queue; healthy partitions' records received after that
    /// wait in the backlog. They reach the margin (90 s) before the stuck
    /// generation's timeout (its head's arrival + 120 s): they never had room
    /// through no fault of their partitions, so they are answered NotAttempted
    /// (fenced, so their partitions keep their order), not Failed, and go out
    /// once the timeout purges the stuck generation and frees the queue.
    #[test]
    fn a_record_held_back_by_another_partitions_stuck_records_is_not_a_failure() {
        const QUEUE: usize = 4;
        let mut core = ForwarderCore::default();
        let t0 = Instant::now();
        // Partition 0's records fill the queue of generation 1.
        for o in (0_i64..).take(QUEUE) {
            core.sent(RowKey::new(1, 0, o), 1, t0);
        }
        // Healthy partitions 1 and 2 (their earlier records were delivered).
        let arrived = t0 + secs(4);
        let mut backlog = VecDeque::from([
            queued(1, 40, arrived),
            queued(1, 41, arrived),
            queued(2, 40, arrived),
        ]);
        let mut live = 1u64;
        let send = |core: &mut ForwarderCore, q: Queued, live: u64| {
            if core.admit(&q.record) == Admit::Fenced {
                core.not_attempted(q.record.row, "fenced");
                return None;
            }
            let on_queue = core
                .outstanding
                .values()
                .filter(|o| o.generation == live)
                .count();
            if on_queue >= QUEUE {
                return Some(q);
            }
            core.sent(q.record.row, live, q.received_at);
            None
        };
        // 90 s after their arrival, 26 s before generation 1 times out.
        let now = arrived + secs(90);
        assert_eq!(core.expired_generation(now, TIMEOUT), None);
        core.pump_backlog(&mut backlog, now, TIMEOUT, |c, q| send(c, q, live));
        assert!(backlog.is_empty());
        let got = outcomes(&mut core);
        assert_eq!(got.len(), 3, "{got:?}");
        for (key, outcome) in &got {
            assert!(
                matches!(outcome, ForwardOutcome::NotAttempted(_)),
                "{key:?} was charged a failed attempt: {got:?}"
            );
        }
        // Fenced: a later record of partition 1 may not overtake 40.
        assert_eq!(core.admit(&rec(1, 42, false)), Admit::Fenced);
        // The DB thread dispatches them again at once (no backoff); 40 of each
        // partition is its head. They wait for room again.
        let again = now + HOLD_TICK;
        backlog.extend([
            Queued {
                record: rec(1, 40, true),
                received_at: again,
            },
            Queued {
                record: rec(1, 41, false),
                received_at: again,
            },
            Queued {
                record: rec(2, 40, true),
                received_at: again,
            },
        ]);
        core.pump_backlog(&mut backlog, again, TIMEOUT, |c, q| send(c, q, live));
        assert_eq!(backlog.len(), 3, "the queue is still full");
        // Generation 1 times out at its head's arrival + the timeout: only
        // partition 0's head counts, and the purge frees the queue.
        let deadline = t0 + TIMEOUT;
        assert_eq!(core.expired_generation(deadline, TIMEOUT), Some(1));
        core.time_out_generation(1, deadline, TIMEOUT);
        for o in (0_i64..).take(QUEUE) {
            core.on_result(RowKey::new(1, 0, o), 1, Err("purged".into()), false);
        }
        live = 2;
        core.pump_backlog(&mut backlog, deadline, TIMEOUT, |c, q| send(c, q, live));
        assert!(
            backlog.is_empty(),
            "the healthy records go out on generation 2"
        );
        for (p, o) in [(1, 40), (1, 41), (2, 40)] {
            core.on_result(RowKey::new(1, p, o), 2, Ok(o), false);
        }
        let got = outcomes(&mut core);
        let failed: Vec<RowKey> = got
            .iter()
            .filter(|(_, o)| matches!(o, ForwardOutcome::Failed(_)))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(failed, vec![RowKey::new(1, 0, 0)], "{got:?}");
        let delivered: Vec<RowKey> = got
            .iter()
            .filter(|(_, o)| matches!(o, ForwardOutcome::Delivered(_)))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(
            delivered,
            vec![
                RowKey::new(1, 1, 40),
                RowKey::new(1, 1, 41),
                RowKey::new(1, 2, 40)
            ]
        );
        assert_eq!(core.admit(&rec(1, 42, false)), Admit::Send);
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

    /// A panic while sending one record fails that record (a counted attempt, so
    /// a record that always panics is dead-lettered in the end) and fences its
    /// partition; the records after it are still sent.
    #[test]
    fn a_panic_while_sending_fails_only_that_record() {
        let mut core = ForwarderCore::default();
        let now = Instant::now();
        let mut backlog: VecDeque<Queued> = [rec(0, 1, true), rec(0, 2, false), rec(1, 1, true)]
            .into_iter()
            .map(|record| Queued {
                record,
                received_at: now,
            })
            .collect();
        core.pump_backlog(&mut backlog, now, TIMEOUT, |core, q| {
            core.sent(q.record.row, 1, q.received_at);
            assert_ne!(q.record.row, RowKey::new(1, 0, 1), "injected panic");
            None
        });
        assert!(backlog.is_empty());
        let got = outcomes(&mut core);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, RowKey::new(1, 0, 1));
        assert!(matches!(&got[0].1, ForwardOutcome::Failed(e) if e.contains("panicked")));
        assert!(!core.outstanding.contains_key(&RowKey::new(1, 0, 1)));
        assert_eq!(core.outstanding.len(), 2, "the later records were sent");
        assert_eq!(core.admit(&rec(0, 3, false)), Admit::Fenced);
        assert_eq!(
            core.admit(&rec(0, 1, false)),
            Admit::Send,
            "its retry lifts the fence"
        );
    }

    /// After a panic of the loop, every record it held (on a producer or in the
    /// backlog) is answered NotAttempted and fences its partition; acks already
    /// queued, fences and held responses are kept; producer generations are
    /// forgotten.
    #[test]
    fn recovering_from_a_panic_answers_every_held_record() {
        let mut core = ForwarderCore::default();
        let now = Instant::now();
        let released = Arc::new(AtomicUsize::new(0));
        core.add_hold(
            counting_hold(vec![RowKey::new(1, 0, 4)], TIMEOUT, &released),
            now,
        );
        core.on_result(RowKey::new(1, 2, 7), 1, Err("boom".into()), false);
        core.sent(RowKey::new(1, 0, 4), 3, now);
        core.retire_generation(3);
        let mut backlog: VecDeque<Queued> = VecDeque::from([Queued {
            record: rec(1, 9, true),
            received_at: now,
        }]);
        core.recover_from_panic(&mut backlog);
        assert!(backlog.is_empty());
        assert!(core.outstanding.is_empty() && core.by_age.is_empty());
        assert!(core.collateral_generations.is_empty());
        let got = outcomes(&mut core);
        assert!(matches!(got[0], (k, ForwardOutcome::Failed(_)) if k == RowKey::new(1, 2, 7)));
        let lost: Vec<RowKey> = got[1..].iter().map(|(k, _)| *k).collect();
        assert_eq!(lost, vec![RowKey::new(1, 0, 4), RowKey::new(1, 1, 9)]);
        assert!(got[1..]
            .iter()
            .all(|(_, o)| matches!(o, ForwardOutcome::NotAttempted(e) if e.contains("restarted"))));
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(2, 8, false)), Admit::Fenced);
        assert_eq!(core.held(), 1);
        assert_eq!(released.load(Ordering::SeqCst), 0);
    }

    /// A panic of the forwarder loop does not end forwarding: the records it held
    /// are answered, and the loop runs again and answers the records after them.
    /// It stops only when the DB thread's channel is gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_forwarder_runs_again_after_a_panic() {
        let mut config = crate::config::Config::load();
        config.shadow_mode_enabled = false;
        let context = Arc::new(crate::kafka::RuntimeContext::new(config));
        let (forward_tx, forward_rx) = crossbeam_channel::bounded(16);
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(16);
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(4);
        tokio::task::spawn_blocking(move || bridge_requests(&forward_rx, &batch_tx));
        let mut forwarder = Forwarder::new(batch_rx, ack_tx, context);
        forwarder.panic_on_next_batch = true;
        let task = tokio::spawn(supervise(forwarder));

        let next_ack = || {
            ack_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("an ack within 10 s")
        };
        forward_tx
            .send(ForwardRequest::Record(rec(0, 1, true)))
            .expect("the forwarder takes requests");
        let first = next_ack();
        assert_eq!(first.row, RowKey::new(1, 0, 1));
        assert!(
            matches!(&first.outcome, ForwardOutcome::NotAttempted(e) if e.contains("restarted")),
            "{first:?}"
        );
        forward_tx
            .send(ForwardRequest::Record(rec(1, 1, true)))
            .expect("the restarted forwarder takes requests");
        let second = next_ack();
        assert_eq!(second.row, RowKey::new(1, 1, 1));
        assert!(
            matches!(&second.outcome, ForwardOutcome::NotAttempted(e) if e.contains("not enabled")),
            "{second:?}"
        );
        drop(forward_tx);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the forwarder stops once its channel is gone")
            .expect("the forwarder task ends without a panic");
    }
}
