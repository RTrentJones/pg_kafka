//! The request batch executor: group commit of everything already queued, each request
//! in its own subtransaction, the RB-1 barrier that holds every response until the commit,
//! and the SASL credential lookups the network thread asks for.

use crossbeam_channel::{Receiver, Sender};
use pgrx::prelude::*;
use std::time::{Duration, Instant};

use super::txn::{log_txn_error, run_request_in_subtransaction, worker_transaction};
use crate::kafka::api::ResponseBody;
use crate::kafka::auth::CredentialRequest;
use crate::kafka::constants::{API_KEY_INIT_PRODUCER_ID, API_KEY_PRODUCE};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::shadow::ShadowStore;
use crate::kafka::{
    BrokerMetadata, GroupCoordinator, InternalNotification, KafkaRequest, KafkaResponse,
    KafkaStore, PostgresStore,
};
use crate::pg_warning;

/// Most requests one top-level transaction carries (group commit).
///
/// Each request runs in its own subtransaction, and every one that writes is
/// assigned a subtransaction XID. 64 is `PGPROC_MAX_CACHED_SUBXIDS`, so a batch
/// never overflows the worker's subxid cache (an overflowed cache forces other
/// backends' snapshot checks into `pg_subtrans` lookups until it commits).
const MAX_GROUP_COMMIT_REQUESTS: usize = 64;

/// A batch stops taking more requests once it has run this long, so group
/// commit delays the first request's response by at most about this much (plus
/// the request still running and the commit).
const GROUP_COMMIT_TIME_BUDGET: Duration = Duration::from_millis(50);

/// Most SASL credential lookups answered in one transaction.
const MAX_CREDENTIAL_LOOKUPS: usize = 64;

/// Everything a request needs from the worker, borrowed once by the main loop.
pub(super) struct RequestEnv<'a> {
    pub(super) coordinator: &'a GroupCoordinator,
    pub(super) broker_metadata: &'a BrokerMetadata,
    pub(super) default_partitions: i32,
    pub(super) notify_tx: &'a Sender<InternalNotification>,
    pub(super) compression: kafka_protocol::records::Compression,
    /// The store every handler uses: it wraps PostgresStore and decides per request,
    /// from the shadow GUCs, whether to forward.
    pub(super) shadow_store: &'a ShadowStore<PostgresStore>,
    /// `pg_kafka.log_timing`: log each batch's handler and transaction time.
    pub(super) log_timing: bool,
}

/// Answer a request that will not run (schema missing, worker stopping) with its API's
/// error response, on its own sender. An acks=0 produce gets nothing.
pub(super) fn reject_request(request: KafkaRequest, error_code: i16) {
    let KafkaRequest {
        header,
        body,
        response_tx,
    } = request;
    if !body.expects_response() {
        return;
    }
    let response = KafkaResponse::new(
        header.correlation_id,
        header.api_version,
        body.echo().error_response(header.api_version, error_code),
    );
    let _ = response_tx.send(response);
}

/// Answer SASL credential lookups: `first` plus whatever else is queued, in one
/// transaction (each lookup in its own subtransaction, so one failure cannot affect the
/// others). The network thread waits on each reply.
pub(super) fn serve_credential_requests(
    first: CredentialRequest,
    rx: &Receiver<CredentialRequest>,
) {
    let mut batch = vec![first];
    batch.extend(rx.try_iter().take(MAX_CREDENTIAL_LOOKUPS - 1));
    // Taken by the transaction once it runs, so a transaction that never ran leaves the
    // lookups here to be failed below.
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
                pg_warning!("SASL credential lookup rolled back: {panic_msg}");
                result = Err("credential lookup failed".to_string());
            } else if let Err(e) = &result {
                pg_warning!("SASL credential lookup failed: {e}");
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
pub(super) fn notify_transaction_ended(notify_tx: &Sender<InternalNotification>) {
    let _ = notify_tx.try_send(InternalNotification::TransactionEnded);
}

/// Whether a committed batch ended (or may have ended) transactions other than through
/// EndTxn, which notifies by itself: InitProducerId fences and aborts the previous
/// epoch's open transaction.
fn batch_may_end_transactions(api_keys: &[i16]) -> bool {
    api_keys.contains(&API_KEY_INIT_PRODUCER_ID)
}

/// Whether a group-commit batch should take another already-queued request: it stops
/// at the request cap or at the time budget. Heartbeats waiting meanwhile do not end it;
/// they are answered between the batch's requests ([`serve_liveness_in_batch`]).
fn batch_has_room(taken: usize, elapsed: Duration) -> bool {
    taken < MAX_GROUP_COMMIT_REQUESTS && elapsed < GROUP_COMMIT_TIME_BUDGET
}

/// Answer every Heartbeat waiting on the liveness lane from inside a running batch.
///
/// A Heartbeat only reads and updates the in-memory coordinator (no SPI, nothing to
/// commit), so its response does not need the RB-1 commit barrier: it runs in its
/// own subtransaction like any request (an empty one, no XID) and its response is
/// released at once. Returns how many it served. `serve` runs one heartbeat.
fn serve_liveness_in_batch(
    liveness: &Receiver<KafkaRequest>,
    serve: impl FnMut(KafkaRequest),
) -> usize {
    // At most what was waiting on entry (and never more than one batch's worth), so a
    // stream of heartbeats cannot keep the batch from its own work.
    let limit = liveness.len().min(MAX_GROUP_COMMIT_REQUESTS);
    liveness.try_iter().take(limit).map(serve).count()
}

/// The request loop of one batch: run `first`, then every request already queued on
/// `source` while the batch has room ([`batch_has_room`]), answering the heartbeats
/// waiting on `liveness` (if given) with `serve_heartbeat` after each request. `run`
/// executes one request. Returns how many requests ran.
fn drive_batch(
    first: KafkaRequest,
    source: &Receiver<KafkaRequest>,
    liveness: Option<&Receiver<KafkaRequest>>,
    mut run: impl FnMut(KafkaRequest),
    mut serve_heartbeat: impl FnMut(KafkaRequest),
) -> usize {
    let start = Instant::now();
    let mut taken = 0;
    let mut next = Some(first);
    while let Some(request) = next.take() {
        taken += 1;
        run(request);
        if let Some(liveness) = liveness {
            serve_liveness_in_batch(liveness, &mut serve_heartbeat);
        }
        if batch_has_room(taken, start.elapsed()) {
            next = source.try_recv().ok();
        }
    }
    taken
}

/// Run one liveness-lane request inside the current transaction and release its
/// response immediately (see [`serve_liveness_in_batch`]).
fn serve_heartbeat_now(mut request: KafkaRequest, env: &RequestEnv) {
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
struct BufferedResponse {
    client_tx: tokio::sync::mpsc::UnboundedSender<KafkaResponse>,
    buffer_rx: tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
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
    /// Both ends of the buffer live on this one DB thread: tokio's unbounded
    /// send/try_recv are plain synchronous calls, so no runtime or extra
    /// synchronization is involved.
    fn intercept(request: &mut KafkaRequest) -> Self {
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
    /// order. A request yields at most one response; the loop keeps a multi-response
    /// request from silently dropping frames.
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
            // An acks=0 produce whose writes were lost closes the connection, as any
            // acks=0 error does in Kafka, so the client notices.
            let _ = self
                .client_tx
                .send(KafkaResponse::new(-1, -1, ResponseBody::CloseConnection));
            return;
        }
        // The API's own error response, echoing the request's topics/partitions/groups
        // so an API without a top-level error code still reports the failure.
        let error_response = KafkaResponse::new(
            self.correlation_id,
            self.api_version,
            self.echo.error_response(self.api_version, error_code),
        );
        let _ = self.client_tx.send(error_response);
    }
}

/// Run one request of a batch in its own subtransaction (so a handler error or
/// panic rolls back only that request) and log a caught panic.
fn run_one_request(request: KafkaRequest, env: &RequestEnv) -> Result<RequestOutcome, String> {
    let result = run_request_in_subtransaction(|| {
        // Each request reads from its own snapshot. pgrx runs read-only SPI statements
        // (the transaction has not written yet) on the active snapshot, which
        // `BackgroundWorker::transaction` pushes once when the batch starts; without this,
        // a request that joins a batch late would not see rows other sessions committed
        // since (e.g. a topic created by SQL just before the client asked for it). In READ
        // COMMITTED, GetTransactionSnapshot takes a fresh snapshot. A panic unwinds to the
        // subtransaction abort, which pops snapshots pushed inside it.
        // SAFETY: inside the batch's transaction, on the main thread.
        unsafe { pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot()) };
        let outcome = process_request(request, env);
        // SAFETY: pops the snapshot pushed above (process_request returned normally).
        unsafe { pg_sys::PopActiveSnapshot() };
        outcome
    });
    // A reported rollback (handler error) already logged itself and sent its
    // response; only a real panic is worth a warning here.
    if let Err(panic_msg) = &result {
        pg_warning!("Handler panic caught, request rolled back (worker survived): {panic_msg}");
    }
    result
}

/// Run one request on the DB thread: build its handler context and hand it to the
/// API's [`crate::kafka::api::KafkaApi::handle`] through the registry.
fn process_request(request: KafkaRequest, env: &RequestEnv) -> RequestOutcome {
    let store: &dyn KafkaStore = env.shadow_store;
    let api_env = crate::kafka::api::ApiEnv {
        ctx: crate::kafka::HandlerContext::new(
            store,
            env.coordinator,
            env.broker_metadata,
            env.default_partitions,
            env.compression,
        ),
        notify_tx: env.notify_tx,
    };
    crate::kafka::api::dispatch(request, &api_env)
}

/// RB-1 test hook: widen the response-send→commit window so the E2E barrier
/// test can prove the ack implies a visible commit. Produce-gated so heartbeats
/// (DR-12 lane) and unrelated traffic are unaffected while a test has the GUC
/// set. Applied once per batch, just before its commit.
fn apply_test_pre_commit_delay(batch_has_produce: bool) {
    if batch_has_produce {
        let delay = crate::config::guc_millis(crate::config::TEST_PRE_COMMIT_DELAY_MS.get());
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }
}

/// The batch's transaction committed: release every buffered response, in
/// arrival order.
fn after_batch_commit(pending: Vec<BufferedResponse>) {
    for response in pending {
        response.release();
    }
}

/// The batch's commit failed: every request in it gets an API-typed error.
fn after_batch_commit_failure(pending: Vec<BufferedResponse>, error_code: i16) {
    for response in pending {
        response.fail(error_code);
    }
}

/// The batch committed. A sync-mode shadow produce's response waits for the external
/// ack (bounded by `pg_kafka.shadow_sync_timeout_ms`) on the network thread, so the DB
/// thread never waits for it; everything else is released now.
fn release_committed(pending: Vec<BufferedResponse>, shadow_store: &ShadowStore<PostgresStore>) {
    let mut holds = shadow_store.take_sync_holds();
    let mut release_now = Vec::with_capacity(pending.len());
    for response in pending {
        match holds.remove(&response.shadow_slot) {
            Some(keys) if response.expects_response => {
                shadow_store.hold_response(keys, response.into_held());
            }
            _ => release_now.push(response),
        }
    }
    after_batch_commit(release_now);
}

/// Execute `first` plus every request already queued behind it on `source`
/// (no waiting for more) in ONE top-level transaction: group commit.
///
/// One top-level transaction per request would pay one WAL flush each, capping produce
/// throughput at the fsync rate; a batch pays one flush. Each request still runs in its
/// own subtransaction, so a handler error or panic rolls back only that request; the
/// responses of all requests are buffered and released only after the single
/// commit (RB-1, see [`BufferedResponse`]). A batch takes at most
/// [`MAX_GROUP_COMMIT_REQUESTS`] requests and stops early at
/// [`GROUP_COMMIT_TIME_BUDGET`]; heartbeats waiting on `liveness` are answered between
/// its requests.
///
/// Used for both lanes: the DR-12 liveness lane passes `liveness = None` (a Heartbeat
/// does no SPI, but taking the same guarded path keeps one code path and costs only an
/// empty transaction).
pub(super) fn execute_batch(
    first: KafkaRequest,
    source: &Receiver<KafkaRequest>,
    liveness: Option<&Receiver<KafkaRequest>>,
    env: &RequestEnv,
) {
    let batch_start = Instant::now();
    let mut pending: Vec<BufferedResponse> = Vec::new();
    let mut api_keys: Vec<i16> = Vec::new();
    // Time spent inside handlers (timing instrumentation, pg_kafka.log_timing).
    let mut handler_time = Duration::ZERO;
    // Taken by the transaction once it runs: a batch whose schema locks failed first
    // still has its first request here, to be rejected below.
    let mut first = Some(first);

    // worker_transaction takes the schema locks first; a Postgres ERROR outside the
    // requests' subtransactions (a lock timeout there, or the COMMIT itself) rolls the
    // whole batch back and is reported as Err, and the worker carries on.
    let txn_result = worker_transaction(|| {
        let Some(first) = first.take() else { return };
        drive_batch(
            first,
            source,
            liveness,
            |mut request| {
                api_keys.push(request.header.api_key);
                let mut buffered = BufferedResponse::intercept(&mut request);
                buffered.shadow_slot = env.shadow_store.begin_request();
                pending.push(buffered);

                let handler_start = Instant::now();
                let _ = run_one_request(request, env);
                handler_time += handler_start.elapsed();
            },
            |heartbeat| serve_heartbeat_now(heartbeat, env),
        );
        apply_test_pre_commit_delay(api_keys.contains(&API_KEY_PRODUCE));
    });

    let batch_size = pending.len();
    match txn_result {
        Ok(()) => {
            release_committed(pending, env.shadow_store);
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
        log_batch_timing(batch_size, batch_start.elapsed(), handler_time);
    }
}

/// The `pg_kafka.log_timing` line of one batch.
fn log_batch_timing(batch_size: usize, total: Duration, handler_time: Duration) {
    let tx_overhead = total.saturating_sub(handler_time);
    let overhead_pct = (tx_overhead.as_micros() * 100)
        .checked_div(total.as_micros())
        .unwrap_or(0);
    log!(
        "TIMING: batch={} total={}us handler={}us tx_overhead={}us ({}%)",
        batch_size,
        total.as_micros(),
        handler_time.as_micros(),
        tx_overhead.as_micros(),
        overhead_pct
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::{
        ApiVersionsRequestData, ProduceRequestData, RequestBody, RequestHeader,
    };
    use crate::kafka::constants::{API_KEY_API_VERSIONS, API_KEY_FETCH};
    use crate::kafka::ERROR_KAFKA_STORAGE_ERROR;

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

    /// Heartbeats waiting on the liveness lane are served from inside the batch, and
    /// the batch keeps taking main-lane work: a steady heartbeat stream must not cut
    /// every batch down to one request.
    #[test]
    fn test_heartbeats_are_served_inside_a_batch_without_ending_it() {
        let (main_tx, main_rx) = crossbeam_channel::unbounded();
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 1..10 {
            main_tx.send(api_versions_request(i).0).unwrap();
        }
        for i in 0..10 {
            // A heartbeat arrives behind every main-lane request.
            hb_tx.send(api_versions_request(100 + i).0).unwrap();
        }
        let mut batch = Vec::new();
        let mut heartbeats = Vec::new();
        let taken = drive_batch(
            api_versions_request(0).0,
            &main_rx,
            Some(&hb_rx),
            |r| batch.push(r.header.correlation_id),
            |hb| heartbeats.push(hb.header.correlation_id),
        );
        assert_eq!(taken, 10);
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
    fn test_liveness_batch_does_not_serve_heartbeats_inside_itself() {
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 1..5 {
            hb_tx.send(api_versions_request(i).0).unwrap();
        }
        let mut served = Vec::new();
        let taken = drive_batch(
            api_versions_request(0).0,
            &hb_rx,
            None,
            |r| served.push(r.header.correlation_id),
            |_| panic!("no liveness lane to serve"),
        );
        assert_eq!(taken, 5);
        assert_eq!(served, (0..5).collect::<Vec<_>>());
    }

    #[test]
    fn test_batch_stops_at_the_request_cap() {
        let (tx, rx) = crossbeam_channel::unbounded();
        for i in 1..=MAX_GROUP_COMMIT_REQUESTS {
            tx.send(api_versions_request(i32::try_from(i).unwrap()).0)
                .unwrap();
        }
        let taken = drive_batch(api_versions_request(0).0, &rx, None, |_| {}, |_| {});
        assert_eq!(taken, MAX_GROUP_COMMIT_REQUESTS);
        assert_eq!(
            rx.len(),
            1,
            "the request past the cap waits for the next batch"
        );
    }

    #[test]
    fn test_heartbeat_service_per_slot_is_bounded() {
        let (hb_tx, hb_rx) = crossbeam_channel::unbounded();
        for i in 0..i32::try_from(3 * MAX_GROUP_COMMIT_REQUESTS).unwrap() {
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
        KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            KafkaRequest {
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
        KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
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
        KafkaRequest,
        tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
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
    fn handler_sends(request: &KafkaRequest, correlation_id: i32) {
        request
            .response_tx
            .send(KafkaResponse::new(
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
        handler_sends(&a, 1);
        handler_sends(&b, 2);
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
        handler_sends(&a, 11);
        handler_sends(&p, 12);

        after_batch_commit_failure(pending, ERROR_KAFKA_STORAGE_ERROR);
        // Each request gets its own API's error response, never the buffered success.
        let response = a_rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 11);
        match response.body {
            ResponseBody::ApiVersions(r) => assert_eq!(r.error_code, ERROR_KAFKA_STORAGE_ERROR),
            other => panic!("expected an ApiVersions error, got {other:?}"),
        }
        assert!(a_rx.try_recv().is_err(), "the buffered success is dropped");
        // Produce (acks=1): a Produce-typed error.
        let response = p_rx.try_recv().expect("an error response");
        assert_eq!(response.correlation_id, 12);
        assert!(matches!(response.body, ResponseBody::Produce(_)));
        assert!(p_rx.try_recv().is_err());
    }

    #[test]
    fn test_commit_failure_closes_the_connection_of_an_acks0_produce() {
        // An acks=0 client expects no response frame at all; an error frame would
        // be read as the answer to its next request. The connection gets the close
        // instruction instead (Kafka closes it on any acks=0 error).
        let (mut p, mut p_rx) = produce_request(21, 0);
        let pending = vec![BufferedResponse::intercept(&mut p)];
        drop(p); // the acks=0 path sends nothing
        after_batch_commit_failure(pending, ERROR_KAFKA_STORAGE_ERROR);
        match p_rx.try_recv() {
            Ok(r) => assert!(matches!(r.body, ResponseBody::CloseConnection)),
            Err(e) => panic!("expected the close instruction, got {e:?}"),
        }
        assert!(p_rx.try_recv().is_err(), "nothing else");
    }
}
