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
//! ## Pipelining and ordering (wave 3)
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
//! ## Held sync-mode responses
//!
//! A [`SyncHold`] carries the buffered response of a committed sync-mode
//! produce. It is released as soon as every one of its records is delivered,
//! or when its timeout expires, so a slow or unreachable external cluster delays
//! only those responses, never the DB thread.

use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::producer::EnqueueError;
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

/// What to do with an incoming record.
#[derive(Debug, PartialEq)]
pub(crate) enum Admit {
    /// Hand it to the producer.
    Send,
    /// Its partition is fenced behind an earlier failure: answer NotAttempted.
    Fenced,
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
    collateral_generations: std::collections::BTreeSet<u64>,
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
        let part = (record.topic_id, record.partition_id);
        match self.fences.get(&part) {
            None => Admit::Send,
            Some(&fence) if record.restart || record.local_offset <= fence => {
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
        let fence = self.fences.entry((key.0, key.1)).or_insert(key.2);
        *fence = (*fence).min(key.2);
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
                let collateral = self.collateral_generations.contains(&generation);
                if producer_fatal && !collateral {
                    // The first failure seen of a generation that went fatal is
                    // taken as its cause; every later failure is collateral.
                    self.retire_generation(generation);
                }
                let outcome = if collateral {
                    ForwardOutcome::NotAttempted(format!(
                        "failed with the producer after an earlier error: {}",
                        error
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

/// One resolved delivery.
type Delivery = (RowKey, u64, Result<i64, String>, Arc<ShadowProducer>);

/// The producer for the current configuration, (re)built when a setting
/// changed or the old one became fatal. `Err` carries why there is none.
fn current_producer(
    live: &mut Option<LiveProducer>,
    generation: &mut u64,
    last_error: &mut Option<String>,
    core: &mut ForwarderCore,
    config: &crate::config::Config,
) -> Result<(Arc<ShadowProducer>, u64), String> {
    let mut shadow = ShadowConfig::from_config(config);
    let fingerprint = producer_fingerprint(&shadow);
    if let Some(l) = live.as_ref() {
        if l.fingerprint == fingerprint && l.producer.fatal_error().is_none() {
            return Ok((l.producer.clone(), l.generation));
        }
    }
    if let Some(old) = live.take() {
        match old.producer.fatal_error() {
            Some(e) => warn!("Shadow producer is fatal ({}); rebuilding it", e),
            None => {
                info!("Shadow producer settings changed; rebuilding the producer");
                // Fail the old producer's outstanding records now (instead of
                // at message.timeout.ms) and do not count them: they are
                // re-sent by the new producer, in order.
                core.retire_generation(old.generation);
                old.producer.purge_outstanding();
            }
        }
    }
    // SH-1: the same validation the config-loading path applies (it also
    // normalizes the protocol/mechanism names).
    let built = shadow
        .validate()
        .map_err(|e| format!("invalid shadow configuration: {}", e))
        .and_then(|()| {
            ShadowProducer::new(Arc::new(shadow.clone())).map_err(|e| format!("{:?}", e))
        });
    match built {
        Ok(p) => {
            *generation += 1;
            *last_error = None;
            info!(
                "Shadow producer created for bootstrap_servers='{}'",
                shadow.bootstrap_servers
            );
            let p = Arc::new(p);
            *live = Some(LiveProducer {
                producer: p.clone(),
                generation: *generation,
                fingerprint,
            });
            Ok((p, *generation))
        }
        Err(e) => {
            // Logged once per distinct error, not once per record.
            if last_error.as_deref() != Some(e.as_str()) {
                warn!("Shadow producer unavailable: {}", e);
                *last_error = Some(e.clone());
            }
            Err(e)
        }
    }
}

/// Shadow forwarder task (see the module docs).
///
/// Uses RuntimeContext to read the configuration on every record, so shadow
/// mode and every producer setting can change at runtime (ALTER SYSTEM +
/// pg_reload_conf()).
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
    tokio::task::spawn_blocking(move || loop {
        match forward_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(first) => {
                let mut batch = vec![first];
                while batch.len() < BRIDGE_BATCH {
                    match forward_rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                    }
                }
                if batch_tx.blocking_send(batch).is_err() {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if batch_tx.is_closed() {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    });

    let mut core = ForwarderCore::default();
    let mut live: Option<LiveProducer> = None;
    let mut generation: u64 = 0;
    let mut last_error: Option<String> = None;
    let mut deliveries: FuturesUnordered<BoxFuture<'static, Delivery>> = FuturesUnordered::new();
    let mut ticks: u64 = 0;
    let mut tick = tokio::time::interval(HOLD_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if !core.flush_acks(&ack_tx) {
            info!("Forward ack channel closed, stopping shadow forwarder");
            break;
        }
        tokio::select! {
            batch = batch_rx.recv() => {
                let Some(batch) = batch else {
                    info!("Forward channel disconnected, stopping shadow forwarder");
                    break;
                };
                for message in batch {
                    match message {
                        ForwardRequest::Hold(hold) => core.add_hold(hold, Instant::now()),
                        ForwardRequest::Record(record) => {
                            forward_one(
                                record,
                                &runtime_context,
                                &mut core,
                                &mut live,
                                &mut generation,
                                &mut last_error,
                                &mut deliveries,
                            )
                            .await;
                        }
                    }
                }
            }
            Some((key, gen, result, producer)) = deliveries.next(), if !deliveries.is_empty() => {
                if let Err(e) = &result {
                    debug!("Forward FAILED {:?}: {}", key, e);
                }
                let fatal = result.is_err() && producer.fatal_error().is_some();
                core.on_result(key, gen, result, fatal);
            }
            _ = tick.tick() => {
                core.expire_holds(Instant::now());
                // A settings change rebuilds the producer (and purges its
                // outstanding records) within ~0.5 s, even when no new record
                // arrives to notice it.
                ticks = ticks.wrapping_add(1);
                if ticks % 25 == 0 && live.is_some() {
                    let config = runtime_context.config();
                    if config.shadow_mode_enabled && !config.shadow_bootstrap_servers.is_empty() {
                        let _ = current_producer(
                            &mut live,
                            &mut generation,
                            &mut last_error,
                            &mut core,
                            &config,
                        );
                    }
                }
            }
        }
    }

    info!("Shadow forwarder task stopped");
}

/// Send one record (or answer it without sending).
async fn forward_one(
    record: ForwardRecord,
    runtime_context: &crate::kafka::RuntimeContext,
    core: &mut ForwarderCore,
    live: &mut Option<LiveProducer>,
    generation: &mut u64,
    last_error: &mut Option<String>,
    deliveries: &mut FuturesUnordered<BoxFuture<'static, Delivery>>,
) {
    let key = record.row_key();
    let config = runtime_context.config();
    if !config.shadow_mode_enabled || config.shadow_bootstrap_servers.is_empty() {
        core.not_attempted(key, "shadow mode is not enabled or not configured");
        return;
    }
    let (producer, gen) = match current_producer(live, generation, last_error, core, &config) {
        Ok(p) => p,
        Err(e) => {
            // No producer can be built: a real delivery failure (it is retried
            // and eventually dead-lettered, like any other).
            core.on_result(key, *generation, Err(e), false);
            return;
        }
    };
    if core.admit(&record) == Admit::Fenced {
        core.not_attempted(key, "an earlier record of the partition failed");
        return;
    }
    loop {
        match producer.enqueue(&record) {
            Ok(future) => {
                // Issue #93 test hook: hold the ack so it arrives later than
                // the outbox retry lease. 0 in production; only this delivery's
                // own future waits, the pipeline keeps going.
                let ack_delay_ms = config.test_forward_ack_delay_ms;
                let producer = producer.clone();
                deliveries.push(Box::pin(async move {
                    let result = match future.await {
                        Ok(Ok((_partition, offset))) => Ok(offset),
                        Ok(Err((e, _message))) => Err(e.to_string()),
                        Err(_canceled) => Err("delivery canceled".to_string()),
                    };
                    if ack_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(ack_delay_ms as u64)).await;
                    }
                    (key, gen, result, producer)
                }));
                return;
            }
            Err(EnqueueError::QueueFull) => {
                // Backpressure: wait for librdkafka to drain, keeping order.
                producer.poll(Duration::ZERO);
                tokio::time::sleep(QUEUE_FULL_BACKOFF).await;
            }
            Err(EnqueueError::Refused(e)) => {
                let fatal = producer.fatal_error().is_some();
                core.on_result(key, gen, Err(e), fatal);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::shadow::HeldRelease;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rec(p: i32, o: i64, restart: bool) -> ForwardRecord {
        let mut r = ForwardRecord::new(1, "t".into(), p, None, None, o);
        r.restart = restart;
        r
    }

    fn outcomes(core: &mut ForwarderCore) -> Vec<(RowKey, ForwardOutcome)> {
        core.acks
            .drain(..)
            .map(|a| (a.row_key(), a.outcome))
            .collect()
    }

    #[test]
    fn failure_fences_later_records_of_the_partition_only() {
        let mut core = ForwarderCore::default();
        assert_eq!(core.admit(&rec(0, 5, false)), Admit::Send);
        core.on_result((1, 0, 5), 1, Err("boom".into()), false);
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
        core.on_result((1, 0, 5), 1, Err("poison".into()), false);
        assert_eq!(core.admit(&rec(0, 6, false)), Admit::Fenced);
        // 5 was dead-lettered: the DB marks 6 as the partition's head.
        assert_eq!(core.admit(&rec(0, 6, true)), Admit::Send);
    }

    #[test]
    fn fence_keeps_the_lowest_failed_offset() {
        let mut core = ForwarderCore::default();
        core.on_result((1, 0, 9), 1, Err("x".into()), false);
        core.on_result((1, 0, 7), 1, Err("x".into()), false);
        assert_eq!(core.admit(&rec(0, 8, false)), Admit::Fenced);
        assert_eq!(core.admit(&rec(0, 7, false)), Admit::Send);
    }

    #[test]
    fn only_the_first_failure_of_a_fatal_generation_counts() {
        let mut core = ForwarderCore::default();
        core.on_result((1, 0, 1), 3, Err("root cause".into()), true);
        core.on_result((1, 1, 1), 3, Err("fatal".into()), true);
        core.on_result((1, 0, 2), 3, Err("fatal".into()), true);
        let got = outcomes(&mut core);
        assert_eq!(got[0].1, ForwardOutcome::Failed("root cause".into()));
        assert!(matches!(got[1].1, ForwardOutcome::NotAttempted(_)));
        assert!(matches!(got[2].1, ForwardOutcome::NotAttempted(_)));
        // A late failure of that generation is still collateral; a new
        // producer generation counts failures again.
        core.on_result((1, 0, 4), 3, Err("late".into()), false);
        assert!(matches!(
            outcomes(&mut core)[0].1,
            ForwardOutcome::NotAttempted(_)
        ));
        core.on_result((1, 0, 3), 4, Err("again".into()), false);
        assert_eq!(
            outcomes(&mut core)[0].1,
            ForwardOutcome::Failed("again".into())
        );
    }

    #[test]
    fn failures_of_a_retired_generation_are_not_attempts() {
        let mut core = ForwarderCore::default();
        core.retire_generation(2);
        core.on_result((1, 0, 1), 2, Err("purged".into()), false);
        core.on_result((1, 0, 2), 3, Err("real".into()), false);
        let got = outcomes(&mut core);
        assert!(matches!(got[0].1, ForwardOutcome::NotAttempted(_)));
        assert_eq!(got[1].1, ForwardOutcome::Failed("real".into()));
        for g in 10..200 {
            core.retire_generation(g);
        }
        assert!(core.collateral_generations.len() <= 64);
    }

    #[test]
    fn success_is_acked_with_the_external_offset() {
        let mut core = ForwarderCore::default();
        core.on_result((1, 0, 1), 1, Ok(42), false);
        assert_eq!(
            outcomes(&mut core),
            vec![((1, 0, 1), ForwardOutcome::Delivered(42))]
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
            counting_hold(vec![(1, 0, 1), (1, 0, 2)], Duration::from_secs(5), &n),
            now,
        );
        core.on_result((1, 0, 1), 1, Ok(0), false);
        assert_eq!(n.load(Ordering::SeqCst), 0);
        // A failure does not release it: the retry may still land in time.
        core.on_result((1, 0, 2), 1, Err("transient".into()), false);
        assert_eq!(n.load(Ordering::SeqCst), 0);
        core.on_result((1, 0, 2), 1, Ok(1), false);
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert_eq!(core.held(), 0);
    }

    #[test]
    fn hold_is_released_at_its_timeout() {
        let mut core = ForwarderCore::default();
        let n = Arc::new(AtomicUsize::new(0));
        let now = Instant::now();
        core.add_hold(
            counting_hold(vec![(1, 0, 1)], Duration::from_millis(100), &n),
            now,
        );
        assert_eq!(core.expire_holds(now + Duration::from_millis(50)), 0);
        assert_eq!(core.expire_holds(now + Duration::from_millis(100)), 1);
        assert_eq!(n.load(Ordering::SeqCst), 1);
        // A late delivery report finds nothing to release.
        core.on_result((1, 0, 1), 1, Ok(0), false);
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn two_holds_on_one_record_are_both_released() {
        let mut core = ForwarderCore::default();
        let n = Arc::new(AtomicUsize::new(0));
        let now = Instant::now();
        core.add_hold(
            counting_hold(vec![(1, 0, 1)], Duration::from_secs(5), &n),
            now,
        );
        core.add_hold(
            counting_hold(vec![(1, 0, 1)], Duration::from_secs(5), &n),
            now,
        );
        core.on_result((1, 0, 1), 1, Ok(0), false);
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
        core.on_result((1, 0, 1), 1, Ok(1), false);
        core.on_result((1, 0, 2), 1, Ok(2), false);
        assert!(core.flush_acks(&tx));
        assert_eq!(core.acks.len(), 1, "second ack kept, not dropped");
        assert_eq!(rx.try_recv().unwrap().local_offset, 1);
        assert!(core.flush_acks(&tx));
        assert_eq!(rx.try_recv().unwrap().local_offset, 2);
        drop(rx);
        core.on_result((1, 0, 3), 1, Ok(3), false);
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
