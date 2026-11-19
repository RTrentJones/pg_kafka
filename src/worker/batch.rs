//! The request batch executor (group commit) and the RB-1 response barrier.

use pgrx::prelude::*;
use std::time::{Duration, Instant};

use super::schema::reject_request;
use super::txn::{log_txn_error, run_request_in_subtransaction, worker_transaction};
use crate::kafka::constants::*;
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::{KafkaStore, PostgresStore};
use crate::pg_warning;

/// Answer SASL credential lookups: `first` plus whatever else is queued, in one
/// transaction (each lookup in its own subtransaction, so one failure cannot affect the
/// others). The network thread waits on each reply.
pub(super) fn serve_credential_requests(
    first: crate::kafka::auth::CredentialRequest,
    rx: &crossbeam_channel::Receiver<crate::kafka::auth::CredentialRequest>,
) {
    let mut batch = vec![first];
    while batch.len() < 64 {
        match rx.try_recv() {
            Ok(r) => batch.push(r),
            Err(_) => break,
        }
    }
    let mut batch = Some(batch);
    let outcome = worker_transaction(|| {
        for request in batch.take().unwrap_or_default() {
            let mut result = Err("credential lookup failed".to_string());
            let outcome = run_request_in_subtransaction(|| {
                result = crate::kafka::auth::credentials::lookup_credential(
                    &request.username,
                    request.mechanism,
                );
            });
            if let Err(panic_msg) = outcome {
                pg_warning!("SASL credential lookup rolled back: {}", panic_msg);
                result = Err("credential lookup failed".to_string());
            } else if let Err(e) = &result {
                pg_warning!("SASL credential lookup failed: {}", e);
            }
            // The receiver may have given up (timeout / disconnect); nothing to do then.
            let _ = request.reply.send(result);
        }
    });
    if let Err(e) = outcome {
        log_txn_error("SASL credential lookup", &e);
        // Not run (schema missing, lock timeout): fail the lookups, do not leave the
        // network thread waiting.
        for request in batch.take().unwrap_or_default() {
            let _ = request
                .reply
                .send(Err("credential lookup unavailable".to_string()));
        }
    }
}

/// Tell the network thread that transactions ended, so parked read_committed fetches
/// re-check their last stable offset now. Best effort: a full channel only means the
/// long-poll safety net picks it up.
pub(super) fn notify_transaction_ended(
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
) {
    let _ = notify_tx.try_send(crate::kafka::InternalNotification::TransactionEnded);
}

/// Whether a committed batch ended (or may have ended) transactions other than through
/// EndTxn, which notifies by itself: InitProducerId fences and aborts the previous
/// epoch's open transaction.
pub(super) fn batch_may_end_transactions(api_keys: &[i16]) -> bool {
    api_keys.contains(&API_KEY_INIT_PRODUCER_ID)
}

/// Most requests one top-level transaction carries (group commit).
///
/// Each request runs in its own subtransaction, and every one that writes is
/// assigned a subtransaction XID. 64 is `PGPROC_MAX_CACHED_SUBXIDS`, so a batch
/// never overflows the worker's subxid cache (an overflowed cache forces other
/// backends' snapshot checks into `pg_subtrans` lookups until it commits).
pub(super) const MAX_GROUP_COMMIT_REQUESTS: usize = 64;

/// A batch stops taking more requests once it has run this long, so group
/// commit delays the first request's response by at most about this much (plus
/// the request still running and the commit).
pub(super) const GROUP_COMMIT_TIME_BUDGET: Duration = Duration::from_millis(50);

/// Everything `process_request` needs, borrowed once by the main loop.
pub(super) struct RequestEnv<'a> {
    pub(super) coordinator: &'a std::sync::Arc<crate::kafka::GroupCoordinator>,
    pub(super) broker_metadata: &'a crate::kafka::BrokerMetadata,
    pub(super) default_partitions: i32,
    pub(super) notify_tx: &'a crossbeam_channel::Sender<crate::kafka::InternalNotification>,
    pub(super) compression: kafka_protocol::records::Compression,
    pub(super) shadow_store: &'a std::sync::Arc<ShadowStore<PostgresStore>>,
    pub(super) log_timing: bool,
}

/// Whether a group-commit batch should take another already-queued request.
///
/// It stops at the request cap or at the time budget. A waiting Heartbeat no longer
/// ends it (wave 3): heartbeats are answered between the batch's requests instead
/// ([`serve_liveness_in_batch`]), so liveness traffic still waits behind at most one
/// in-flight request, and many heartbeating consumers no longer shrink produce
/// batches to one request each.
pub(super) fn batch_has_room(taken: usize, elapsed: Duration) -> bool {
    taken < MAX_GROUP_COMMIT_REQUESTS && elapsed < GROUP_COMMIT_TIME_BUDGET
}

/// Answer every Heartbeat waiting on the liveness lane from inside a running batch.
///
/// A Heartbeat only reads and updates the in-memory coordinator (no SPI, nothing to
/// commit), so its response does not need the RB-1 commit barrier: it runs in its
/// own subtransaction like any request (an empty one, no XID) and its response is
/// released at once. Returns how many it served. `serve` runs one heartbeat (the
/// worker passes [`serve_heartbeat_now`]; unit tests pass a fake).
pub(super) fn serve_liveness_in_batch(
    liveness: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    mut serve: impl FnMut(crate::kafka::KafkaRequest),
) -> usize {
    // At most what was waiting on entry (and never more than one batch's worth), so a
    // stream of heartbeats cannot keep the batch from its own work.
    let limit = liveness.len().min(MAX_GROUP_COMMIT_REQUESTS);
    let mut served = 0;
    while served < limit {
        match liveness.try_recv() {
            Ok(request) => serve(request),
            Err(_) => break,
        }
        served += 1;
    }
    served
}

/// Run one liveness-lane request inside the current transaction and release its
/// response immediately (see [`serve_liveness_in_batch`]).
pub(super) fn serve_heartbeat_now(mut request: crate::kafka::KafkaRequest, env: &RequestEnv) {
    let buffered = BufferedResponse::intercept(&mut request);
    let _ = run_one_request(request, env);
    buffered.release();
}

/// The RB-1 response buffer for one request in a batch.
///
/// ## RB-1: response-after-commit barrier
///
/// Invariant: **no response may reach the client before
/// `BackgroundWorker::transaction` has returned** (i.e. before the commit is
/// durable and visible to other Postgres connections). Handlers still "send"
/// their response as always, but the request's `response_tx` is swapped for a
/// same-typed buffer channel before dispatch; the buffered response is
/// forwarded to the real client sender only after the transaction commits.
/// Without this barrier an acked produce/offset-commit could be lost on a
/// crash between response send and commit (acks>=1 durability violation), and
/// a client that received an OK could race a separate Postgres connection into
/// pre-commit state. With group commit the barrier covers the whole batch: no
/// response of any request in it is released before the one commit.
///
/// This relies on the writer task queueing response slots at parse time
/// (listener.rs), so deferring the send cannot reorder responses. If that
/// ordering scheme ever changes, re-review this barrier.
pub(super) struct BufferedResponse {
    client_tx: tokio::sync::mpsc::UnboundedSender<crate::kafka::KafkaResponse>,
    buffer_rx: tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    api_version: i16,
    correlation_id: i32,
    /// What the request's error response echoes (its topics/partitions/groups), for
    /// a commit failure.
    echo: crate::kafka::api::Echo,
    /// False for an acks=0 produce: the client expects no response frame at all,
    /// not even an error.
    expects_response: bool,
    /// Per-request metrics: started when the request is taken into the batch,
    /// recorded when this value is dropped (after release or fail), so latency
    /// covers the handler and the batch commit.
    metrics: crate::kafka::metrics::RequestTimer<'static>,
    /// The request's slot in the ShadowStore (`begin_request`), under which its
    /// sync-mode shadow rows are reported after the commit.
    shadow_slot: u64,
}

impl BufferedResponse {
    /// Swap the request's client sender for a buffer the handler writes into.
    /// Both ends of the buffer live on this one DB thread — tokio's unbounded
    /// send/try_recv are plain synchronous calls, so no runtime or extra
    /// synchronization is involved.
    fn intercept(request: &mut crate::kafka::KafkaRequest) -> Self {
        let api_key = request.header.api_key;
        let (buffer_tx, buffer_rx) = tokio::sync::mpsc::unbounded_channel();
        let client_tx = request.swap_response_tx(buffer_tx);
        BufferedResponse {
            client_tx,
            buffer_rx,
            api_version: request.header.api_version,
            correlation_id: request.header.correlation_id,
            echo: request.body.echo(),
            expects_response: request.body.expects_response(),
            metrics: crate::kafka::metrics::RequestTimer::start(api_key),
            shadow_slot: 0,
        }
    }

    /// The batch committed and the response must wait for the external
    /// cluster (a sync-mode shadow produce): take the buffered response(s) out
    /// into a release the shadow forwarder runs on the network thread (no pgrx
    /// in it), after the commit like `release` (RB-1).
    fn into_held(mut self) -> crate::kafka::shadow::HeldRelease {
        let mut responses = Vec::new();
        while let Ok(response) = self.buffer_rx.try_recv() {
            self.metrics.observe(&response);
            responses.push(response);
        }
        let client_tx = self.client_tx;
        let metrics = self.metrics;
        crate::kafka::shadow::HeldRelease::new(move || {
            for response in responses {
                let _ = client_tx.send(response);
            }
            drop(metrics);
        })
    }

    /// The batch committed: forward the buffered response(s) to the client, in
    /// order. Today a request yields at most one response, but the loop keeps a
    /// future multi-response request from silently dropping frames.
    fn release(mut self) {
        while let Ok(response) = self.buffer_rx.try_recv() {
            self.metrics.observe(&response);
            if self.client_tx.send(response).is_err() {
                pg_warning!(
                    "Client disconnected before response flush (correlation_id={})",
                    self.correlation_id
                );
            }
        }
    }

    /// The batch's commit failed. Any buffered response asserts success for work
    /// that did NOT commit — sending it would recreate the lost-ack bug at
    /// commit-failure granularity. Discard it and send an API-typed error frame
    /// instead (pure Rust construction + channel send; safe before Postgres
    /// error-state cleanup). An acks=0 produce gets nothing.
    fn fail(mut self, error_code: i16) {
        while self.buffer_rx.try_recv().is_ok() {}
        self.metrics.mark_error();
        if !self.expects_response {
            // wave3 net: an acks=0 produce whose writes were lost closes the
            // connection, as any acks=0 error does (Kafka), so the client notices.
            let _ = self.client_tx.send(crate::kafka::KafkaResponse::new(
                -1,
                -1,
                crate::kafka::api::ResponseBody::CloseConnection,
            ));
            return;
        }
        // The API's own error response, echoing the request's topics/partitions/groups
        // so an API without a top-level error code still reports the failure.
        let error_response = crate::kafka::KafkaResponse::new(
            self.correlation_id,
            self.api_version,
            self.echo.error_response(self.api_version, error_code),
        );
        let _ = self.client_tx.send(error_response);
    }
}

/// Run one request of a batch in its own subtransaction (so a handler error or
/// panic rolls back only that request) and log a caught panic.
pub(super) fn run_one_request(
    request: crate::kafka::KafkaRequest,
    env: &RequestEnv,
) -> Result<RequestOutcome, String> {
    let result = run_request_in_subtransaction(|| {
        // Each request reads from its own snapshot. pgrx runs read-only SPI statements
        // (the transaction has not written yet) on the active snapshot, which
        // `BackgroundWorker::transaction` pushes once when the batch starts; without this,
        // a request that joins a batch late would not see rows other sessions committed
        // since (e.g. a topic created by SQL just before the client asked for it). In READ
        // COMMITTED, GetTransactionSnapshot takes a fresh snapshot. A panic unwinds to the
        // subtransaction abort, which pops snapshots pushed inside it.
        unsafe { pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot()) };
        let outcome = process_request(
            request,
            env.coordinator,
            env.broker_metadata,
            env.default_partitions,
            env.notify_tx,
            env.compression,
            env.shadow_store,
        );
        unsafe { pg_sys::PopActiveSnapshot() };
        outcome
    });
    // A reported rollback (handler error) already logged itself and sent its
    // response; only a real panic is worth a warning here.
    if let Err(panic_msg) = &result {
        pg_warning!(
            "Handler panic caught, request rolled back (worker survived): {}",
            panic_msg
        );
    }
    result
}

/// RB-1 test hook: widen the response-send→commit window so the E2E barrier
/// test can prove the ack implies a visible commit. Produce-gated so heartbeats
/// (DR-12 lane) and unrelated traffic are unaffected while a test has the GUC
/// set. Applied once per batch, just before its commit.
pub(super) fn apply_test_pre_commit_delay(batch_has_produce: bool) {
    if batch_has_produce {
        let delay_ms = crate::config::TEST_PRE_COMMIT_DELAY_MS.get();
        if delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(delay_ms as u64));
        }
    }
}

/// The batch's transaction committed: release every buffered response, in
/// arrival order.
pub(super) fn after_batch_commit(pending: Vec<BufferedResponse>) {
    for response in pending {
        response.release();
    }
}

/// The batch's commit failed: every request in it gets an API-typed error.
pub(super) fn after_batch_commit_failure(pending: Vec<BufferedResponse>, error_code: i16) {
    for response in pending {
        response.fail(error_code);
    }
}

/// Execute `first` plus every request already queued behind it on `source`
/// (no waiting for more) in ONE top-level transaction: group commit.
///
/// Each request used to be its own top-level transaction and therefore its own
/// WAL flush, which capped produce throughput at the fsync rate (~220
/// requests/s). Now a batch pays one flush. Each request still runs in its own
/// subtransaction, so a handler error or panic rolls back only that request; the
/// responses of all requests are buffered and released only after the single
/// commit (RB-1, see [`BufferedResponse`]). A batch takes at most
/// `MAX_GROUP_COMMIT_REQUESTS` requests and stops early on
/// `GROUP_COMMIT_TIME_BUDGET` or when `yield_to` (the liveness lane) has work.
///
/// Used for both lanes: the DR-12 liveness lane passes `yield_to = None` (a
/// Heartbeat does no SPI, but taking the same guarded path keeps one code path
/// and costs only an empty transaction).
pub(super) fn execute_batch(
    first: crate::kafka::KafkaRequest,
    source: &crossbeam_channel::Receiver<crate::kafka::KafkaRequest>,
    yield_to: Option<&crossbeam_channel::Receiver<crate::kafka::KafkaRequest>>,
    env: &RequestEnv,
) {
    let batch_start = Instant::now();
    let mut pending: Vec<BufferedResponse> = Vec::new();
    let mut api_keys: Vec<i16> = Vec::new();
    // Time spent inside handlers (timing instrumentation, pg_kafka.log_timing).
    let mut handler_time = Duration::ZERO;
    let mut first = Some(first);

    // worker_transaction takes the schema locks first; a Postgres ERROR outside the
    // requests' subtransactions (a lock timeout there, or the COMMIT itself) rolls the
    // whole batch back and is reported as Err, and the worker carries on.
    let txn_result = worker_transaction(|| {
        let mut next = first.take();
        while let Some(mut request) = next.take() {
            api_keys.push(request.header.api_key);
            let mut buffered = BufferedResponse::intercept(&mut request);
            buffered.shadow_slot = env.shadow_store.begin_request();
            pending.push(buffered);

            let handler_start = Instant::now();
            let _ = run_one_request(request, env);
            handler_time += handler_start.elapsed();

            // wave3 worker: heartbeats that arrived meanwhile are answered now, and
            // the batch goes on taking queued work.
            if let Some(liveness) = yield_to {
                serve_liveness_in_batch(liveness, |hb| serve_heartbeat_now(hb, env));
            }
            if batch_has_room(pending.len(), batch_start.elapsed()) {
                next = source.try_recv().ok();
            }
        }
        apply_test_pre_commit_delay(api_keys.contains(&API_KEY_PRODUCE));
    });

    let batch_size = pending.len();
    match txn_result {
        Ok(()) => {
            // A sync-mode shadow produce's response waits for the external ack
            // (bounded by pg_kafka.shadow_sync_timeout_ms) on the network thread;
            // the DB thread does not wait (wave 3). Everything else is released now.
            let mut holds = env.shadow_store.take_sync_holds();
            let mut release_now = Vec::with_capacity(pending.len());
            for response in pending {
                match holds.remove(&response.shadow_slot) {
                    Some(keys) if response.expects_response => {
                        env.shadow_store.hold_response(keys, response.into_held())
                    }
                    _ => release_now.push(response),
                }
            }
            after_batch_commit(release_now);
            if batch_may_end_transactions(&api_keys) {
                notify_transaction_ended(env.notify_tx);
            }
        }
        Err(err) => {
            // Nothing of the batch committed. Every request gets an API-typed error
            // (a retriable one) instead of its buffered success; a request the batch
            // never started (the schema locks failed first) is rejected the same way.
            log_txn_error("Request batch", &err);
            // Sync holds of rolled-back requests are void (their outbox rows are gone).
            drop(env.shadow_store.take_sync_holds());
            let code = err.kafka_error_code();
            after_batch_commit_failure(pending, code);
            if let Some(request) = first.take() {
                reject_request(request, code);
            }
        }
    }

    if env.log_timing {
        let total = batch_start.elapsed();
        let tx_overhead = total.saturating_sub(handler_time);
        log!(
            "TIMING: batch={} total={}us handler={}us tx_overhead={}us ({}%)",
            batch_size,
            total.as_micros(),
            handler_time.as_micros(),
            tx_overhead.as_micros(),
            if total.as_micros() > 0 {
                (tx_overhead.as_micros() * 100) / total.as_micros()
            } else {
                0
            }
        );
    }
}

/// Run one request on the DB thread: build its handler context and hand it to the
/// API's [`crate::kafka::api::KafkaApi::handle`] through the registry.
///
/// # Testing
/// This function is public to allow unit testing, but is not part of the
/// public API and should not be called directly by external code.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn process_request(
    request: crate::kafka::KafkaRequest,
    coordinator: &std::sync::Arc<crate::kafka::GroupCoordinator>,
    broker_metadata: &crate::kafka::BrokerMetadata,
    default_partitions: i32,
    notify_tx: &crossbeam_channel::Sender<crate::kafka::InternalNotification>,
    compression: kafka_protocol::records::Compression,
    shadow_store: &std::sync::Arc<ShadowStore<PostgresStore>>,
) -> RequestOutcome {
    // ShadowStore wraps PostgresStore and handles shadow mode internally.
    // It checks the GUC at runtime to decide whether to forward.
    let store: &dyn KafkaStore = shadow_store.as_ref();
    let env = crate::kafka::api::ApiEnv {
        ctx: crate::kafka::HandlerContext::new(
            store,
            coordinator,
            broker_metadata,
            default_partitions,
            compression,
        ),
        notify_tx,
    };
    crate::kafka::api::dispatch(request, &env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::{
        ApiVersionsRequestData, ProduceRequestData, RequestBody, RequestHeader, ResponseBody,
    };
    use crate::worker::txn::ERROR_KAFKA_STORAGE_ERROR;

    #[test]
    fn test_batch_has_room_stops_at_cap_and_budget_only() {
        assert!(batch_has_room(1, Duration::ZERO));
        assert!(batch_has_room(
            MAX_GROUP_COMMIT_REQUESTS - 1,
            Duration::from_millis(1)
        ));
        // Request cap.
        assert!(!batch_has_room(MAX_GROUP_COMMIT_REQUESTS, Duration::ZERO));
        // Time budget.
        assert!(!batch_has_room(1, GROUP_COMMIT_TIME_BUDGET));
        // The cap keeps a batch within the subxid cache.
        const { assert!(MAX_GROUP_COMMIT_REQUESTS <= 64) };
    }

    /// wave3 worker (item 7): heartbeats waiting on the liveness lane are served from
    /// inside the batch, and the batch keeps taking main-lane work. Before, the first
    /// waiting heartbeat ended the batch (`batch_has_room(.., liveness_waiting=true)`
    /// was false), so a steady heartbeat stream cut every batch to one request.
    #[test]
    fn test_heartbeats_are_served_inside_a_batch_without_ending_it() {
        let (main_tx, main_rx) = crossbeam_channel::unbounded();
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 0..10 {
            main_tx.send(api_versions_request(i).0).unwrap();
            // A heartbeat arrives behind every main-lane request.
            hb_tx.send(api_versions_request(100 + i).0).unwrap();
        }
        let mut batch = Vec::new();
        let mut heartbeats = Vec::new();
        let start = Instant::now();
        let mut next = main_rx.try_recv().ok();
        while let Some(r) = next.take() {
            batch.push(r.header.correlation_id);
            serve_liveness_in_batch(&hb_rx, |hb| heartbeats.push(hb.header.correlation_id));
            if batch_has_room(batch.len(), start.elapsed()) {
                next = main_rx.try_recv().ok();
            }
        }
        assert_eq!(batch, (0..10).collect::<Vec<_>>(), "one batch took all ten");
        assert_eq!(
            heartbeats,
            (100..110).collect::<Vec<_>>(),
            "every heartbeat served"
        );
        assert!(hb_rx.is_empty());
    }

    /// A flood of heartbeats is served at most one batch's worth per slot, so the batch
    /// still gets to its own requests.
    #[test]
    fn test_heartbeat_service_per_slot_is_bounded() {
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 0..(3 * MAX_GROUP_COMMIT_REQUESTS as i32) {
            hb_tx.send(api_versions_request(i).0).unwrap();
        }
        let served = serve_liveness_in_batch(&hb_rx, |_| {});
        assert_eq!(served, MAX_GROUP_COMMIT_REQUESTS);
        assert_eq!(hb_rx.len(), 2 * MAX_GROUP_COMMIT_REQUESTS);
    }

    #[test]
    fn test_init_producer_id_batches_notify_transaction_end() {
        assert!(batch_may_end_transactions(&[
            API_KEY_PRODUCE,
            API_KEY_INIT_PRODUCER_ID
        ]));
        assert!(!batch_may_end_transactions(&[
            API_KEY_PRODUCE,
            API_KEY_FETCH
        ]));
        assert!(!batch_may_end_transactions(&[]));
    }

    fn request(
        api_key: i16,
        correlation_id: i32,
        body: RequestBody,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            crate::kafka::KafkaRequest {
                header: RequestHeader {
                    api_key,
                    api_version: 7,
                    correlation_id,
                    client_id: None,
                },
                body,
                response_tx: tx,
            },
            rx,
        )
    }

    fn api_versions_request(
        correlation_id: i32,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        let (mut r, rx) = request(
            API_KEY_API_VERSIONS,
            correlation_id,
            RequestBody::ApiVersions(ApiVersionsRequestData),
        );
        r.header.api_version = 3;
        (r, rx)
    }

    fn produce_request(
        correlation_id: i32,
        acks: i16,
    ) -> (
        crate::kafka::KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<crate::kafka::KafkaResponse>,
    ) {
        request(
            API_KEY_PRODUCE,
            correlation_id,
            RequestBody::Produce(ProduceRequestData {
                acks,
                timeout_ms: 1000,
                topic_data: vec![],
                transactional_id: None,
            }),
        )
    }

    /// What a handler does: send its (success) response on the request's (swapped)
    /// response channel.
    fn handler_sends(request: crate::kafka::KafkaRequest, correlation_id: i32) {
        request
            .response_tx
            .send(crate::kafka::KafkaResponse::new(
                correlation_id,
                0,
                ResponseBody::UnknownApi { error_code: 0 },
            ))
            .unwrap();
    }

    #[test]
    fn test_buffered_responses_are_held_until_commit_then_released_in_order() {
        // RB-1 with group commit: nothing reaches a client before the batch's
        // commit; after it, every request's response is released.
        let (mut a, mut a_rx) = api_versions_request(1);
        let (mut b, mut b_rx) = api_versions_request(2);
        let pending = vec![
            BufferedResponse::intercept(&mut a),
            BufferedResponse::intercept(&mut b),
        ];
        handler_sends(a, 1);
        handler_sends(b, 2);
        assert!(a_rx.try_recv().is_err(), "no response before commit");
        assert!(b_rx.try_recv().is_err(), "no response before commit");

        after_batch_commit(pending);
        for (rx, expected) in [(&mut a_rx, 1), (&mut b_rx, 2)] {
            let response = rx.try_recv().expect("released after commit");
            assert_eq!(response.correlation_id, expected);
            assert!(
                matches!(response.body, ResponseBody::UnknownApi { error_code: 0 }),
                "the handler's own (success) response"
            );
        }
    }

    #[test]
    fn test_commit_failure_replaces_every_response_with_an_error() {
        let (mut a, mut a_rx) = api_versions_request(11);
        let (mut p, mut p_rx) = produce_request(12, 1);
        let pending = vec![
            BufferedResponse::intercept(&mut a),
            BufferedResponse::intercept(&mut p),
        ];
        handler_sends(a, 11);
        handler_sends(p, 12);

        after_batch_commit_failure(pending, ERROR_KAFKA_STORAGE_ERROR);
        // Each request gets its own API's error response — never the buffered success.
        let response = a_rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 11);
        match response.body {
            ResponseBody::ApiVersions(r) => assert_eq!(r.error_code, ERROR_KAFKA_STORAGE_ERROR),
            other => panic!("expected an ApiVersions error, got {:?}", other),
        }
        assert!(a_rx.try_recv().is_err(), "the buffered success is dropped");
        // Produce (acks=1): a Produce-typed error.
        let response = p_rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 12);
        assert!(matches!(response.body, ResponseBody::Produce(_)));
        assert!(p_rx.try_recv().is_err());
    }

    #[test]
    fn test_commit_failure_sends_nothing_to_acks0_produce() {
        // An acks=0 client expects no response frame at all; an error frame would
        // be read as the answer to its next request. wave3 net: the connection gets
        // the close instruction instead (Kafka closes it on any acks=0 error).
        let (mut p, mut p_rx) = produce_request(21, 0);
        let pending = vec![BufferedResponse::intercept(&mut p)];
        drop(p); // the acks=0 path sends nothing
        after_batch_commit_failure(pending, ERROR_KAFKA_STORAGE_ERROR);
        match p_rx.try_recv() {
            Ok(r) => assert!(matches!(
                r.body,
                crate::kafka::api::ResponseBody::CloseConnection
            )),
            Err(e) => panic!("expected the close instruction, got {e:?}"),
        }
        assert!(p_rx.try_recv().is_err(), "nothing else");
    }
}
