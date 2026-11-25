//! One client connection: the read loop, its deadlines, and how the connection closes.
//!
//! The socket is split into a read half, served by [`ReadLoop`] on the connection's
//! task, and a write half owned by the writer task ([`super::writer`]), so requests are
//! read and dispatched while earlier ones are still being answered (pipelining). The
//! reader never waits for the DB thread.
//!
//! Every request takes an in-flight permit before its frame is read, released only once
//! its response is on the socket: at most [`MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION`]
//! requests per connection are read but unanswered. A client that pipelines without
//! reading responses fills its TCP receive window, the writer blocks, permits stop
//! coming back and the reader stops reading: the raw request bytes a connection holds are
//! bounded (what a request decodes to is bounded by `protocol::decode_meter` and
//! `protocol::decode_budget`, and given back once the DB thread has run it) and the
//! backpressure reaches the client through TCP. What its in-flight requests' responses hold
//! is bounded by the connection's response budget (`super::response_budget`: a Fetch reserves
//! its `max_bytes` before it runs, and the reader reads nothing while unwritten responses hold
//! more than the budget), and only until the idle timeout: a connection whose responses wait
//! for its client to read them counts as idle, as Kafka's idle expiry closes a muted
//! connection whose response does not go out. A request that decodes off the I/O threads or waits for room
//! to decode stops the reader until it has decoded, so a connection waits for one such
//! decode at a time.

use bytes::BytesMut;
use futures::stream::{Stream, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, warn};

use super::dispatch::{Dispatcher, Route};
use super::is_peer_disconnect;
use super::response_budget::{ReplyCharge, ResponseBudget};
use super::sasl::ConnectionAuth;
use super::writer::{self, ResponseSlot, WriterContext};
use crate::kafka::api::{RequestBody, ResponseBody};
use crate::kafka::auth::session::{is_sasl_api, peek_api_key};
use crate::kafka::constants::{
    MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_LONG_POLLS_PER_CONNECTION, MAX_REQUEST_SIZE,
};
use crate::kafka::handlers;
use crate::kafka::messages::{KafkaRequest, KafkaResponse};
use crate::kafka::metrics::RequestTimer;
use crate::kafka::protocol;
use crate::kafka::protocol::framing::{FrameClock, FrameCodec, FrameSink, PRE_AUTH_MAX_FRAME};

/// How long a client has, from the moment its TCP connection is accepted, to finish
/// everything before it is authenticated: the TLS handshake (SSL / SASL_SSL) and, on a
/// SASL listener, SASL authentication, including any wait for a credential lookup or a
/// password check. One deadline from accept, so SASL_SSL gets 10 s in total.
pub(super) const PRE_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a connection that is being closed (bad frame, SASL failure, timeout, acks=0
/// error) waits for the responses of the requests it read before, so that they are
/// delivered first as Kafka's serial reader guarantees (a committed produce is answered).
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Grace for the writer when the peer disconnected (nothing can be delivered).
const PEER_GONE_GRACE: Duration = Duration::from_millis(100);

/// The frame-size limit once a connection may send any request.
const AUTHENTICATED_MAX_FRAME: usize = MAX_REQUEST_SIZE;

/// How often a read loop that waits (for a frame, or for an in-flight slot) re-checks
/// whether the connection has become idle, at most: a writer may start or stop waiting for
/// its client meanwhile.
const IDLE_RECHECK: Duration = Duration::from_secs(1);

/// Why a connection's read loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Close {
    /// EOF or reset: the peer went away.
    PeerGone,
    /// The writer task exited (write/encode error, acks=0 close request).
    WriterGone,
    /// A frame the broker closes the connection for (malformed, refused, SASL failure).
    Protocol,
    /// Pre-auth, per-frame read or idle deadline.
    Timeout,
    /// The idle deadline passed while responses waited for the client to read them (the writer
    /// writing one, or responses ready behind an earlier request): nothing more can be
    /// delivered, so the writer is not waited for.
    Stalled,
}

/// When a connection last read a frame or moved response bytes, and whether a response is
/// going out (the idle timeout).
pub(super) struct ActivityClock {
    epoch: Instant,
    last_ms: AtomicU64,
    sending: AtomicBool,
}

impl ActivityClock {
    pub(super) fn new() -> Self {
        ActivityClock {
            epoch: Instant::now(),
            last_ms: AtomicU64::new(0),
            sending: AtomicBool::new(false),
        }
    }

    pub(super) fn touch(&self) {
        let ms = u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last_ms.store(ms, Ordering::Relaxed);
    }

    pub(super) fn last(&self) -> Instant {
        self.epoch + Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }

    /// A response starts going out: from now the connection waits for its client, and the
    /// idle timeout counts from the last byte written.
    pub(super) fn start_sending(&self) {
        self.touch();
        self.sending.store(true, Ordering::Relaxed);
    }

    /// The response is on the socket.
    pub(super) fn stop_sending(&self) {
        self.sending.store(false, Ordering::Relaxed);
    }

    /// Whether a response is going out (the writer waits for the client to read).
    fn sending(&self) -> bool {
        self.sending.load(Ordering::Relaxed)
    }
}

/// A connection's TCP socket, touching its [`ActivityClock`] whenever bytes are written to
/// it, so a large response that a slow client reads keeps its connection alive and a client
/// that reads nothing lets it go idle (as Kafka's idle expiry counts every write). On a TLS
/// listener it sits under the TLS stream: the records rustls buffers (up to 64 KiB) reach the
/// socket on a later write or on flush, and each of those writes counts too.
pub(super) struct Tracked<S> {
    inner: S,
    activity: Arc<ActivityClock>,
}

impl<S> Tracked<S> {
    /// `inner`, touching `activity` on every write.
    pub(super) fn new(inner: S, activity: Arc<ActivityClock>) -> Self {
        Tracked { inner, activity }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Tracked<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Tracked<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let written = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if matches!(written, std::task::Poll::Ready(Ok(n)) if n > 0) {
            self.activity.touch();
        }
        written
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let written = std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if matches!(written, std::task::Poll::Ready(Ok(n)) if n > 0) {
            self.activity.touch();
        }
        written
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Serve one client connection until it closes. `activity` is the clock the connection's
/// [`Tracked`] TCP socket (under `socket`'s TLS, if any) touches.
///
/// Runs on the network thread and MUST NOT call pgrx functions.
pub(super) async fn handle_connection<S>(
    socket: S,
    activity: Arc<ActivityClock>,
    dispatcher: Dispatcher,
    auth: ConnectionAuth,
    pre_auth_deadline: tokio::time::Instant,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    debug!("New connection established, starting pipelined handle loop");

    // The frame codec charges the global budget only for bytes actually buffered of a
    // large incomplete frame and records when that frame started arriving, for
    // the per-frame read deadline. Before SASL authentication frames are small.
    let frame_limit = Arc::new(AtomicUsize::new(
        if auth.sasl.is_authenticated() || !auth.security.protocol.uses_sasl() {
            AUTHENTICATED_MAX_FRAME
        } else {
            PRE_AUTH_MAX_FRAME
        },
    ));
    let clock = Arc::new(FrameClock::new());
    let (read_half, write_half) = tokio::io::split(socket);
    let reader = FramedRead::new(
        read_half,
        FrameCodec::new(Arc::clone(&frame_limit), Arc::clone(&clock)),
    );
    let sink = FrameSink::new(write_half);

    // What the connection's unwritten responses may hold: one full Fetch response.
    let budget =
        ResponseBudget::new(usize::try_from(dispatcher.fetch_max_bytes()).unwrap_or(usize::MAX));
    let (order_tx, slots) = tokio::sync::mpsc::unbounded_channel();
    let (writer_gone_tx, writer_gone) = oneshot::channel();
    let writer = writer::spawn(
        sink,
        WriterContext {
            slots,
            advertised: auth.security.advertised.clone(),
            activity: Arc::clone(&activity),
            budget: Arc::clone(&budget),
            peer: auth.peer.clone(),
            gone: writer_gone_tx,
        },
    );

    let mut read_loop = ReadLoop {
        reader,
        auth,
        dispatcher,
        pre_auth_deadline,
        frame_limit,
        clock,
        activity,
        in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)),
        // Each long-poll fetch spawns a detached task: cap them per connection (see
        // `Dispatcher::dispatch`).
        long_poll_permits: Arc::new(Semaphore::new(MAX_LONG_POLLS_PER_CONNECTION)),
        budget,
        next_seq: 0,
        order_tx,
        writer_gone,
    };
    let close = read_loop.run().await;
    // The writer exits once it has drained the slots of the requests already read.
    drop(read_loop.order_tx);
    let auth = read_loop.auth;
    drain_writer(writer, close, &auth.peer).await;

    if auth.log_connections {
        info!(
            "Kafka connection from {} ({}) closed ({close:?})",
            auth.peer, auth.principal
        );
    }
    debug!("Connection handler exiting");
}

/// Let the writer deliver the responses of the requests read before the connection
/// closed: Kafka reads a connection serially, so every request read before the one that
/// closes it is answered first (bounded; a committed acks=1 produce must be answered).
/// When the peer is gone the writer is aborted after a short grace: an in-flight long
/// poll could otherwise hold the task (and the half-open socket) for up to max_wait_ms.
async fn drain_writer(mut writer: tokio::task::JoinHandle<()>, close: Close, peer: &str) {
    let grace = match close {
        Close::PeerGone => PEER_GONE_GRACE,
        Close::WriterGone | Close::Stalled => Duration::ZERO,
        Close::Protocol | Close::Timeout => CLOSE_DRAIN_TIMEOUT,
    };
    if tokio::time::timeout(grace, &mut writer).await.is_err() {
        if grace >= CLOSE_DRAIN_TIMEOUT {
            warn!("Closing connection from {peer}: responses still pending after {grace:?}");
        }
        writer.abort();
        let _ = writer.await;
    }
}

/// Log that a client missed the pre-authentication deadline.
fn pre_auth_timeout(peer: &str) -> Close {
    warn!(
        "Closing connection from {peer}: SASL authentication not completed within {PRE_AUTH_TIMEOUT:?}"
    );
    Close::Timeout
}

fn is_api_versions(request: &KafkaRequest) -> bool {
    matches!(request.body, RequestBody::ApiVersions(_))
}

/// Answer an ApiVersions request on this thread, counted in kafka.api_metrics like a DB
/// thread request. The response is static, so it equals the DB thread's answer.
fn answer_api_versions(request: &KafkaRequest) {
    let mut timer = RequestTimer::start(request.header.api_key);
    let response = KafkaResponse::new(
        request.header.correlation_id,
        request.header.api_version,
        ResponseBody::ApiVersions(handlers::handle_api_versions()),
    );
    timer.observe(&response);
    // A closed slot means the connection is gone; there is nobody to answer.
    let _ = request.response_tx.send(response);
}

/// The read side of one connection.
struct ReadLoop<R> {
    reader: R,
    auth: ConnectionAuth,
    dispatcher: Dispatcher,
    pre_auth_deadline: tokio::time::Instant,
    /// The codec's frame-size limit, raised once SASL authentication succeeds.
    frame_limit: Arc<AtomicUsize>,
    clock: Arc<FrameClock>,
    activity: Arc<ActivityClock>,
    in_flight: Arc<Semaphore>,
    long_poll_permits: Arc<Semaphore>,
    /// What the connection's unwritten responses hold.
    budget: Arc<ResponseBudget>,
    /// The request-order number of the next slot queued to the writer.
    next_seq: u64,
    order_tx: UnboundedSender<ResponseSlot>,
    writer_gone: oneshot::Receiver<()>,
}

impl<R> ReadLoop<R>
where
    R: Stream<Item = std::io::Result<BytesMut>> + Unpin,
{
    /// Read and dispatch requests until the connection must close.
    async fn run(&mut self) -> Close {
        loop {
            if let Err(close) = self.serve_next().await {
                return close;
            }
        }
    }

    /// Read one frame and hand it on: SASL frames to the connection's SASL session,
    /// anything else (once admitted) parsed to the dispatcher.
    async fn serve_next(&mut self) -> Result<(), Close> {
        let permit = self.acquire_slot().await?;
        // Time spent not reading (backpressure) does not count against a frame the
        // client had started.
        self.clock.restart_if_pending();
        let frame = self.next_frame().await?;
        self.activity.touch();
        debug!("Received frame of {} bytes", frame.len());

        // Each request's response slot is queued to the writer BEFORE the request is
        // dispatched, so the writer's output order is fixed by request arrival order.
        let (response_tx, responses) = ResponseSlot::channel();

        // On a SASL listener nothing but ApiVersions and the two SASL APIs is processed
        // before authentication succeeds; anything else closes the connection, as Kafka
        // does.
        let api_key = peek_api_key(&frame);
        if let Some(key) = api_key.filter(|k| is_sasl_api(*k)) {
            self.queue_slot(responses, permit)?;
            return self.serve_sasl(key, frame, response_tx).await;
        }
        let pre_auth = !self.auth.sasl.admits(-1);
        if !self.auth.sasl.admits(api_key.unwrap_or(-1)) {
            warn!(
                "Closing connection from {} on {}: request api_key={api_key:?} before SASL authentication",
                self.auth.peer,
                self.auth.security.describe()
            );
            return Err(Close::Protocol);
        }

        // A request that decodes off the I/O threads, waits for room to decode or for room
        // to hold what it decoded waits here, holding no thread: this connection reads
        // nothing more meanwhile, so it waits for one decode at a time and its requests
        // reach the DB thread in the order they were sent.
        let decoded = tokio::select! {
            biased;
            _ = &mut self.writer_gone => return Err(Close::WriterGone),
            decoded = protocol::decode_request(frame, response_tx) => decoded,
        };
        let reply = self.queue_slot(responses, permit)?;
        match decoded {
            // Before SASL authentication the network thread answers ApiVersions itself,
            // as Kafka's authenticator does: an anonymous peer never reaches the one DB
            // thread, so it cannot delay authenticated clients' requests or the
            // credential lookups other connections wait for under their pre-auth deadline.
            Ok(Some(request)) if pre_auth && is_api_versions(&request) => {
                answer_api_versions(&request);
                Ok(())
            }
            Ok(Some(request)) => match self.dispatcher.route(request, &self.long_poll_permits) {
                Route::LongPoll(job) => {
                    self.dispatcher.spawn_long_poll(job, reply);
                    Ok(())
                }
                Route::Lane(request) => {
                    // A Fetch reserves its max_bytes of the response budget before the DB
                    // thread reads its records; this connection reads nothing more until it
                    // has room.
                    if let RequestBody::Fetch(fetch) = &request.body {
                        let charge = self.wait_for_room(&reply, fetch.max_bytes).await?;
                        reply.hold(charge);
                    }
                    if self.dispatcher.send(request).await {
                        Ok(())
                    } else {
                        error!("Worker channel closed");
                        Err(Close::Protocol)
                    }
                }
            },
            // A request the broker answers with an error (already queued on
            // `response_tx`); the connection stays open.
            Ok(None) => Ok(()),
            Err(e) => {
                warn!("Framing/Parse error from {}: {e}", self.auth.peer);
                Err(Close::Protocol)
            }
        }
    }

    /// Queue a request's response slot to the writer; the reply charge is what its responses
    /// hold of the connection's response budget.
    fn queue_slot(
        &mut self,
        responses: tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
        permit: OwnedSemaphorePermit,
    ) -> Result<ReplyCharge, Close> {
        let reply = ReplyCharge::new(Arc::clone(&self.budget), self.next_seq);
        self.next_seq += 1;
        if self
            .order_tx
            .send(ResponseSlot::new(responses, reply.slot(), permit))
            .is_err()
        {
            warn!("Writer task gone, closing connection");
            return Err(Close::WriterGone);
        }
        Ok(reply)
    }

    /// Reserve a Fetch's `max_bytes` of the response budget, waiting for room under the idle
    /// timeout: earlier responses make room as the client reads them.
    async fn wait_for_room(
        &mut self,
        reply: &ReplyCharge,
        max_bytes: i32,
    ) -> Result<super::response_budget::ResponseCharge, Close> {
        loop {
            // The request's own slot is queued: every permit out is a request read and not
            // yet written.
            let outstanding =
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION - self.in_flight.available_permits();
            let wake = self.idle_wake(tokio::time::Instant::now(), outstanding, false)?;
            tokio::select! {
                biased;
                _ = &mut self.writer_gone => return Err(Close::WriterGone),
                charge = reply.reserve(max_bytes) => return Ok(charge),
                () = tokio::time::sleep_until(wake) => {}
            }
        }
    }

    /// Wait for an in-flight slot, and for the unwritten responses to hold no more than the
    /// response budget, before reading the next frame. A client that pipelines without
    /// reading its responses is bound here by the pre-auth deadline while unauthenticated,
    /// and by the idle timeout once its responses wait for it to read them.
    async fn acquire_slot(&mut self) -> Result<OwnedSemaphorePermit, Close> {
        let permit = self.acquire_in_flight().await?;
        let pre_auth = !self.auth.sasl.admits(-1);
        while self.budget.over_limit() {
            let wake = self.idle_wake(tokio::time::Instant::now(), self.outstanding(), false)?;
            tokio::select! {
                biased;
                _ = &mut self.writer_gone => return Err(Close::WriterGone),
                () = self.budget.under_limit() => {}
                () = tokio::time::sleep_until(self.pre_auth_deadline), if pre_auth => {
                    return Err(pre_auth_timeout(&self.auth.peer));
                }
                () = tokio::time::sleep_until(wake) => {}
            }
        }
        Ok(permit)
    }

    /// Wait for an in-flight slot.
    async fn acquire_in_flight(&mut self) -> Result<OwnedSemaphorePermit, Close> {
        let pre_auth = !self.auth.sasl.admits(-1);
        let acquire = Arc::clone(&self.in_flight).acquire_owned();
        tokio::pin!(acquire);
        loop {
            // The permits that are out are held by the requests read and not yet answered.
            // Once the idle deadline has passed, the next turn's check closes the connection.
            let outstanding =
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION - self.in_flight.available_permits();
            let wake = self.idle_wake(tokio::time::Instant::now(), outstanding, false)?;
            tokio::select! {
                biased;
                _ = &mut self.writer_gone => return Err(Close::WriterGone),
                // The semaphore is never closed, so acquiring fails only if that changes.
                permit = &mut acquire => return permit.map_err(|_| Close::Protocol),
                () = tokio::time::sleep_until(self.pre_auth_deadline), if pre_auth => {
                    return Err(pre_auth_timeout(&self.auth.peer));
                }
                () = tokio::time::sleep_until(wake) => {}
            }
        }
    }

    /// When to look at the idle deadline again from `now` (at the latest the deadline
    /// itself), or `Err` once it has passed. A connection is idle when nothing was read or
    /// written for `connections_max_idle`, no frame is arriving, and either nothing is
    /// outstanding or responses wait for the client to read them: the writer is writing one,
    /// or responses are ready while the writer waits on an earlier request (a long poll at the
    /// head of a client that stops reading). A request still being answered with nothing
    /// ready behind it (a long poll, a JoinGroup waiting for its group) does not make its
    /// connection idle. `outstanding` counts the requests read and not yet written;
    /// `reading_frame` is set when the caller waits for a frame and the per-frame deadline
    /// governs a partial one.
    fn idle_wake(
        &self,
        now: tokio::time::Instant,
        outstanding: usize,
        reading_frame: bool,
    ) -> Result<tokio::time::Instant, Close> {
        let limit = self.auth.security.limits.connections_max_idle;
        let recheck = now + limit.min(IDLE_RECHECK);
        let waits_for_client = self.activity.sending() || self.budget.ready() > 0;
        let frame_pending = reading_frame && self.clock.pending_since().is_some();
        if frame_pending || (outstanding > 0 && !waits_for_client) {
            return Ok(recheck);
        }
        let deadline = tokio::time::Instant::from_std(self.activity.last()) + limit;
        if now >= deadline {
            if waits_for_client {
                debug!(
                    "Closing connection from {}: its responses unread for {limit:?}",
                    self.auth.peer
                );
                return Err(Close::Stalled);
            }
            debug!(
                "Closing connection from {}: idle for {limit:?}",
                self.auth.peer
            );
            return Err(Close::Timeout);
        }
        // The writer may start or stop waiting before the deadline.
        Ok(deadline.min(recheck))
    }

    /// Requests read whose responses are not written yet (the caller holds one permit
    /// itself).
    fn outstanding(&self) -> usize {
        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION - 1 - self.in_flight.available_permits()
    }

    /// Wait for the next frame under three deadlines: the pre-auth deadline (SASL
    /// listeners, until authenticated), the per-frame read deadline (from the first
    /// byte of a frame) and the idle timeout ([`Self::idle_wake`]).
    async fn next_frame(&mut self) -> Result<BytesMut, Close> {
        let limits = self.auth.security.limits;
        loop {
            let now = tokio::time::Instant::now();
            let mut wake = now + limits.request_read_timeout;
            if !self.auth.sasl.admits(-1) {
                if now >= self.pre_auth_deadline {
                    return Err(pre_auth_timeout(&self.auth.peer));
                }
                wake = wake.min(self.pre_auth_deadline);
            }
            let pending_frame = self.clock.pending_since();
            if let Some(started) = pending_frame {
                let deadline =
                    tokio::time::Instant::from_std(started) + limits.request_read_timeout;
                if now >= deadline {
                    warn!(
                        "Closing connection from {}: request not received completely within {:?}",
                        self.auth.peer, limits.request_read_timeout
                    );
                    return Err(Close::Timeout);
                }
                wake = wake.min(deadline);
            }
            wake = wake.min(self.idle_wake(now, self.outstanding(), true)?);
            let read = tokio::select! {
                biased;
                _ = &mut self.writer_gone => return Err(Close::WriterGone),
                read = tokio::time::timeout_at(wake, self.reader.next()) => read,
            };
            if let Ok(next) = read {
                return self.frame_or_close(next);
            }
        }
    }

    /// The frame the codec produced, or why the connection closes.
    fn frame_or_close(&self, next: Option<std::io::Result<BytesMut>>) -> Result<BytesMut, Close> {
        match next {
            None => Err(Close::PeerGone),
            Some(Ok(frame)) => Ok(frame),
            Some(Err(e)) if is_peer_disconnect(&e) => {
                debug!("Client disconnected mid-frame: {e}");
                Err(Close::PeerGone)
            }
            Some(Err(e)) => {
                warn!("Error reading frame from {}: {e}", self.auth.peer);
                Err(Close::Protocol)
            }
        }
    }

    /// Answer a SaslHandshake / SaslAuthenticate frame on this thread. Until the
    /// connection is authenticated the pre-auth deadline also covers the wait for the
    /// credential lookup, the lookup rate limit and the password-check slots.
    async fn serve_sasl(
        &mut self,
        api_key: i16,
        frame: BytesMut,
        response_tx: UnboundedSender<KafkaResponse>,
    ) -> Result<(), Close> {
        debug!("SASL request api_key={api_key} from {}", self.auth.peer);
        let keep_open = if self.auth.sasl.is_authenticated() {
            self.auth.handle_sasl_frame(frame, response_tx).await
        } else {
            let answer = self.auth.handle_sasl_frame(frame, response_tx);
            match tokio::time::timeout_at(self.pre_auth_deadline, answer).await {
                Ok(keep_open) => keep_open,
                Err(_) => return Err(pre_auth_timeout(&self.auth.peer)),
            }
        };
        if !keep_open {
            return Err(Close::Protocol);
        }
        if self.auth.sasl.is_authenticated() {
            self.frame_limit
                .store(AUTHENTICATED_MAX_FRAME, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::accept::ListenerSecurity;
    use super::*;
    use crate::kafka::api::ResponseBody;
    use crate::kafka::auth::{ChannelLookup, ConnectionPrincipal};
    use crate::kafka::conn_limits::NetworkLimits;
    use crate::kafka::messages::{KafkaRequest, RequestLanes};
    use crate::kafka::pending_fetches::PendingFetchRegistry;

    /// An ApiVersions v0 request frame ([size][api_key][api_version][corr][client_id=null]).
    fn api_versions_v0_frame(correlation_id: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&18i16.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&correlation_id.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        let len = i32::try_from(body.len()).unwrap();
        let mut frame = len.to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_bounds_in_flight_requests() {
        use tokio::io::AsyncWriteExt;

        // A client pipelines CAP + 20 requests. The fake DB thread holds every request
        // (unanswered), so no response slot completes: the reader must stop after exactly CAP
        // requests instead of reading (and buffering) all of them. Releasing the held requests
        // frees the slots and the rest flow through.
        const EXTRA: usize = 20;
        let total = MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + EXTRA;

        let (addr, main_rx) = serve_one(NetworkLimits::default(), 10_000).await;

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = Vec::new();
        for i in 0..total {
            bytes.extend_from_slice(&api_versions_v0_frame(i32::try_from(i).unwrap()));
        }
        client.write_all(&bytes).await.unwrap();
        client.flush().await.unwrap();

        // Collect what reaches the "DB thread" while holding every request.
        let mut held = Vec::new();
        let collect_until = Instant::now() + Duration::from_millis(500);
        while Instant::now() < collect_until {
            while let Ok(req) = main_rx.try_recv() {
                held.push(req);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            held.len(),
            MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            "the reader must stop at the in-flight cap"
        );

        // Dropping the requests closes their response slots (no response, like acks=0), which
        // returns the permits; the remaining requests are then read.
        let first_batch = held.len();
        held.clear();
        let mut released = 0usize;
        let deadline = Instant::now() + Duration::from_secs(5);
        while first_batch + released < total && Instant::now() < deadline {
            while let Ok(req) = main_rx.try_recv() {
                drop(req);
                released += 1;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            first_batch + released,
            total,
            "all requests flow once slots free"
        );
    }

    // ===== Connection lifecycle =====

    /// Serve one connection on an ephemeral port with `limits`; requests go to the
    /// returned receiver (the fake DB thread), whose main lane holds `capacity`.
    async fn serve_one(
        limits: NetworkLimits,
        capacity: usize,
    ) -> (
        std::net::SocketAddr,
        crossbeam_channel::Receiver<KafkaRequest>,
    ) {
        let security = ListenerSecurity {
            limits,
            ..ListenerSecurity::plaintext("127.0.0.1:0")
        };
        serve_one_on(security, capacity).await
    }

    /// [`serve_one`] on a listener with the given security.
    async fn serve_one_on(
        security: ListenerSecurity,
        capacity: usize,
    ) -> (
        std::net::SocketAddr,
        crossbeam_channel::Receiver<KafkaRequest>,
    ) {
        let (addr, rx, _task) = serve_one_task(security, capacity).await;
        (addr, rx)
    }

    /// [`serve_one_on`], with the task serving the connection (it ends when the connection
    /// has closed).
    async fn serve_one_task(
        security: ListenerSecurity,
        capacity: usize,
    ) -> (
        std::net::SocketAddr,
        crossbeam_channel::Receiver<KafkaRequest>,
        tokio::task::JoinHandle<()>,
    ) {
        serve_one_with(security, capacity, crate::config::Config::load()).await
    }

    /// [`serve_one_task`] with the given configuration snapshot.
    async fn serve_one_with(
        security: ListenerSecurity,
        capacity: usize,
        config: crate::config::Config,
    ) -> (
        std::net::SocketAddr,
        crossbeam_channel::Receiver<KafkaRequest>,
        tokio::task::JoinHandle<()>,
    ) {
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(capacity);
        let (liveness_tx, _liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let task = tokio::spawn(async move {
            let (socket, _) = server.accept().await.unwrap();
            let (cred_tx, _cred_rx) = crossbeam_channel::bounded(1);
            let auth = ConnectionAuth::new(
                "test".into(),
                Arc::new(security),
                ConnectionPrincipal::anonymous(),
                ChannelLookup(cred_tx),
                false,
                None,
            );
            let dispatcher = Dispatcher {
                request_lanes: RequestLanes {
                    main: main_tx,
                    liveness: liveness_tx,
                },
                registry: Arc::new(PendingFetchRegistry::new()),
                long_poll_interval: None,
                runtime_context: Arc::new(crate::kafka::RuntimeContext::new(config)),
            };
            let activity = Arc::new(ActivityClock::new());
            handle_connection(
                Tracked::new(socket, Arc::clone(&activity)),
                activity,
                dispatcher,
                auth,
                tokio::time::Instant::now() + PRE_AUTH_TIMEOUT,
            )
            .await;
        });
        (addr, main_rx, task)
    }

    async fn recv_request(rx: &crossbeam_channel::Receiver<KafkaRequest>) -> KafkaRequest {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(r) = rx.try_recv() {
                return r;
            }
            assert!(Instant::now() < deadline, "no request reached the worker");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Bytes until EOF (or `None` if the connection stays open for `within`).
    async fn read_until_eof(
        client: &mut tokio::net::TcpStream,
        within: Duration,
    ) -> Option<(Vec<u8>, Duration)> {
        use tokio::io::AsyncReadExt;
        let started = Instant::now();
        let mut out = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let left = within.checked_sub(started.elapsed())?;
            match tokio::time::timeout(left, client.read(&mut buf)).await {
                Ok(Ok(0) | Err(_)) => return Some((out, started.elapsed())),
                Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
                Err(_) => return None,
            }
        }
    }

    fn api_versions_response(correlation_id: i32) -> KafkaResponse {
        KafkaResponse::new(
            correlation_id,
            0,
            ResponseBody::ApiVersions(kafka_protocol::messages::ApiVersionsResponse::default()),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_instruction_ends_the_connection_after_earlier_responses() {
        // The writer exiting (here on the acks=0 CloseConnection instruction)
        // ends the connection; the earlier request's response is written first.
        use tokio::io::AsyncWriteExt;
        let (addr, rx) = serve_one(NetworkLimits::default(), 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = api_versions_v0_frame(1);
        bytes.extend_from_slice(&api_versions_v0_frame(2));
        client.write_all(&bytes).await.unwrap();
        let first = recv_request(&rx).await;
        let second = recv_request(&rx).await;
        let _ = first.response_tx.send(api_versions_response(1));
        let _ = second
            .response_tx
            .send(KafkaResponse::new(-1, -1, ResponseBody::CloseConnection));
        // The worker drops a request once answered, closing its response slot.
        drop((first, second));
        let (bytes, _) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("the connection must close");
        assert!(bytes.len() > 8, "the first response is delivered");
        assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_response_delivered_before_a_bad_frame_closes() {
        // A bad frame closes the connection only after the responses of the requests
        // read before it are written, however long they take (a long poll, a slow commit).
        use tokio::io::AsyncWriteExt;
        let (addr, rx) = serve_one(NetworkLimits::default(), 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = api_versions_v0_frame(7);
        bytes.extend_from_slice(&[0, 0, 0, 3, 0, 0x12, 0]); // truncated header
        client.write_all(&bytes).await.unwrap();
        let request = recv_request(&rx).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _ = request.response_tx.send(api_versions_response(7));
        drop(request);
        let (bytes, _) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("the connection must close");
        assert!(
            bytes.len() > 8,
            "the response is delivered before the close"
        );
        assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_idle_timeout_waits_for_outstanding_requests() {
        use tokio::io::AsyncWriteExt;
        let limits = NetworkLimits {
            connections_max_idle: Duration::from_millis(300),
            ..Default::default()
        };
        // Idle with nothing outstanding: closed after ~300 ms.
        let (addr, _rx) = serve_one(limits, 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("an idle connection must close");
        assert!(after >= Duration::from_millis(250), "{after:?}");

        // A request outstanding for 800 ms keeps the connection open past the timeout.
        let (addr, rx) = serve_one(limits, 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(&api_versions_v0_frame(3)).await.unwrap();
        let request = recv_request(&rx).await;
        tokio::time::sleep(Duration::from_millis(800)).await;
        let _ = request.response_tx.send(api_versions_response(3));
        drop(request);
        let (bytes, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("idle again after the response: closes");
        assert_eq!(i32::from_be_bytes(bytes[4..8].try_into().unwrap()), 3);
        // Counted from the response (its write is activity), not from the request.
        assert!(after >= Duration::from_millis(250), "{after:?}");
    }

    /// A Fetch response of `bytes` record bytes: more than the socket buffers between the
    /// writer and a client that reads nothing hold.
    fn large_response(correlation_id: i32, bytes: usize) -> KafkaResponse {
        use kafka_protocol::messages::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        };
        let mut partition = PartitionData::default();
        partition.records = Some(bytes::Bytes::from(vec![0u8; bytes]));
        let mut topic = FetchableTopicResponse::default();
        topic.partitions = vec![partition];
        let mut response = FetchResponse::default();
        response.responses = vec![topic];
        KafkaResponse::new(correlation_id, 4, ResponseBody::Fetch(response))
    }

    /// A client whose receive buffer is as small as the kernel allows.
    async fn small_window_client(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.connect(addr).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_a_writer_waiting_for_its_client_counts_as_idle() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let idle = Duration::from_millis(300);
        let limits = NetworkLimits {
            connections_max_idle: idle,
            ..Default::default()
        };
        let security = || ListenerSecurity {
            limits,
            ..ListenerSecurity::plaintext("127.0.0.1:0")
        };

        // A client that reads nothing: the writer stalls on an 8 MiB response and the
        // connection closes once nothing has been written for the idle timeout, though its
        // request is still outstanding (its response is not on the socket).
        let (addr, rx, task) = serve_one_task(security(), 1000).await;
        let mut client = small_window_client(addr).await;
        client.write_all(&api_versions_v0_frame(1)).await.unwrap();
        let request = recv_request(&rx).await;
        let answered = Instant::now();
        let _ = request.response_tx.send(large_response(1, 8 << 20));
        drop(request);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("a connection whose responses are unread must close")
            .unwrap();
        let after = answered.elapsed();
        assert!(after >= Duration::from_millis(250), "{after:?}");

        // A client that reads a 32 MiB response slowly, 1 MiB at a time with 50 ms pauses
        // (more than the idle timeout in all, while the writer is still writing), gets all of
        // it: every byte written is activity, so its connection is not closed mid-response,
        // which would cut the response short. (The kernel wakes a writer once a part of its
        // send buffer has drained, so progress comes in steps of that part, as in Kafka.)
        let (addr, rx, _task) = serve_one_task(security(), 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(&api_versions_v0_frame(2)).await.unwrap();
        let request = recv_request(&rx).await;
        let _ = request.response_tx.send(large_response(2, 32 << 20));
        drop(request);
        let mut len = [0u8; 4];
        client.read_exact(&mut len).await.unwrap();
        let mut left = usize::try_from(u32::from_be_bytes(len)).unwrap();
        let started = Instant::now();
        let mut buf = vec![0u8; 1 << 20];
        while left > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let chunk = left.min(1 << 20);
            client
                .read_exact(&mut buf[..chunk])
                .await
                .expect("the slow reader gets the whole response");
            left -= chunk;
        }
        assert!(
            started.elapsed() > idle * 3,
            "the slow read must outlast the idle timeout"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_responses_waiting_behind_an_unanswered_request_count_as_idle() {
        // A client that reads nothing sends a request that stays unanswered (a long poll at
        // the head) and one answered with 8 MiB behind it: the answered response waits for the
        // head, then for the client. Nothing was read or written for the idle timeout and a
        // response waits unread: the connection closes, though its head is still outstanding.
        use tokio::io::AsyncWriteExt;
        let idle = Duration::from_millis(300);
        let security = ListenerSecurity {
            limits: NetworkLimits {
                connections_max_idle: idle,
                ..Default::default()
            },
            ..ListenerSecurity::plaintext("127.0.0.1:0")
        };
        let (addr, rx, task) = serve_one_task(security, 1000).await;
        let mut client = small_window_client(addr).await;
        let mut bytes = api_versions_v0_frame(1);
        bytes.extend_from_slice(&api_versions_v0_frame(2));
        client.write_all(&bytes).await.unwrap();
        let head = recv_request(&rx).await;
        let behind = recv_request(&rx).await;
        let answered = Instant::now();
        let _ = behind.response_tx.send(large_response(2, 8 << 20));
        drop(behind);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("a connection whose ready responses wait unread must close")
            .unwrap();
        let after = answered.elapsed();
        assert!(after >= Duration::from_millis(250), "{after:?}");
        assert!(
            head.response_tx.is_closed(),
            "the unanswered head's slot is gone with the connection"
        );
    }

    /// A Fetch v4 request frame of no partitions asking for `max_bytes`, without waiting.
    fn fetch_v4_frame(correlation_id: i32, max_bytes: i32) -> Vec<u8> {
        use kafka_protocol::messages::{BrokerId, FetchRequest, RequestHeader};
        use kafka_protocol::protocol::Encodable;
        let mut body = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(crate::kafka::constants::API_KEY_FETCH)
            .with_request_api_version(4)
            .with_correlation_id(correlation_id)
            .encode(&mut body, 1)
            .unwrap();
        FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(0)
            .with_max_bytes(max_bytes)
            .encode(&mut body, 4)
            .unwrap();
        let mut frame = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unread_fetch_responses_stop_the_reader_at_the_response_budget() {
        // With pg_kafka.fetch_max_bytes (the response budget) at 8 MiB, a client that pipelines
        // 30 Fetches of 4 MiB and reads nothing gets two read (their reservations fill the
        // budget), and one or two more once the kernel's socket buffers have taken a response,
        // and no more while the responses wait unread: the rest stay in the socket, and the
        // responses the connection holds stay within the budget instead of 30 of them.
        use tokio::io::AsyncWriteExt;
        let mut config = crate::config::Config::load();
        config.fetch_max_bytes = 8 << 20;
        let security = ListenerSecurity::plaintext("127.0.0.1:0");
        let (addr, rx, _task) = serve_one_with(security, 1000, config).await;
        let mut client = small_window_client(addr).await;
        let mut bytes = Vec::new();
        for i in 0..30 {
            bytes.extend_from_slice(&fetch_v4_frame(i, 4 << 20));
        }
        client.write_all(&bytes).await.unwrap();
        let mut read = 0;
        let until = Instant::now() + Duration::from_secs(1);
        while Instant::now() < until {
            while let Ok(request) = rx.try_recv() {
                let RequestBody::Fetch(fetch) = &request.body else {
                    panic!("a Fetch");
                };
                let size = usize::try_from(fetch.max_bytes).unwrap();
                let id = request.header.correlation_id;
                let _ = request.response_tx.send(large_response(id, size));
                read += 1;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            read <= 6,
            "{read} of 30 Fetches read although their unread responses fill the budget"
        );
        drop(client);
    }

    /// Read one response frame: (correlation id, body after the v0 header).
    async fn read_response(client: &mut tokio::net::TcpStream) -> (i32, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        let mut len = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(3), client.read_exact(&mut len))
            .await
            .expect("a response within 3 s")
            .unwrap();
        let mut body = vec![0u8; usize::try_from(u32::from_be_bytes(len)).unwrap()];
        client.read_exact(&mut body).await.unwrap();
        let correlation_id = i32::from_be_bytes(body[..4].try_into().unwrap());
        (correlation_id, body[4..].to_vec())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pre_auth_api_versions_answered_without_the_db_thread() {
        // On a SASL listener an unauthenticated peer's ApiVersions never reaches the DB
        // thread: the network thread answers it with the full key list.
        use kafka_protocol::messages::ApiVersionsResponse;
        use kafka_protocol::protocol::Decodable;
        use tokio::io::AsyncWriteExt;
        let security = ListenerSecurity {
            protocol: crate::kafka::auth::SecurityProtocol::SaslPlaintext,
            sasl_mechanisms: Arc::new(vec![crate::kafka::auth::SaslMechanism::Plain]),
            ..ListenerSecurity::plaintext("127.0.0.1:0")
        };
        let (addr, rx) = serve_one_on(security, 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = api_versions_v0_frame(11);
        bytes.extend_from_slice(&api_versions_v0_frame(12));
        client.write_all(&bytes).await.unwrap();
        for expected in [11, 12] {
            let (correlation_id, body) = read_response(&mut client).await;
            assert_eq!(correlation_id, expected);
            let response = ApiVersionsResponse::decode(&mut bytes::Bytes::from(body), 0).unwrap();
            assert_eq!(response.error_code, 0);
            assert_eq!(
                response.api_keys.len(),
                crate::kafka::api::APIS.len(),
                "every API is advertised"
            );
        }
        assert!(
            rx.try_recv().is_err(),
            "no pre-auth request reaches the DB thread"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_partial_frame_read_deadline() {
        // A frame that never completes closes the connection at the read deadline.
        use tokio::io::AsyncWriteExt;
        let limits = NetworkLimits {
            request_read_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        let (addr, _rx) = serve_one(limits, 1000).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(&1000u32.to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2, 3]).await.unwrap();
        let (_, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("a stalled frame must close the connection");
        assert!(after >= Duration::from_millis(250), "{after:?}");
    }
}
