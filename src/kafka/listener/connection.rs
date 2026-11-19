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
//! coming back and the reader stops reading: the connection's memory is bounded and the
//! backpressure reaches the client through TCP.

use bytes::BytesMut;
use futures::stream::{Stream, StreamExt};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::dispatch::Dispatcher;
use super::is_peer_disconnect;
use super::sasl::ConnectionAuth;
use super::writer::{self, ResponseSlot, WriterContext};
use crate::kafka::auth::session::{is_sasl_api, peek_api_key};
use crate::kafka::constants::{
    MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_LONG_POLLS_PER_CONNECTION, MAX_REQUEST_SIZE,
};
use crate::kafka::messages::KafkaResponse;
use crate::kafka::protocol;
use crate::kafka::protocol::framing::{FrameClock, FrameCodec, PRE_AUTH_MAX_FRAME};

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
}

/// When a connection last read a frame or wrote a response (the idle timeout).
pub(super) struct ActivityClock {
    epoch: Instant,
    last_ms: AtomicU64,
}

impl ActivityClock {
    fn new() -> Self {
        ActivityClock {
            epoch: Instant::now(),
            last_ms: AtomicU64::new(0),
        }
    }

    pub(super) fn touch(&self) {
        let ms = u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last_ms.store(ms, Ordering::Relaxed);
    }

    fn last(&self) -> Instant {
        self.epoch + Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }
}

/// Serve one client connection until it closes.
///
/// Runs on the network thread and MUST NOT call pgrx functions.
pub(super) async fn handle_connection<S>(
    socket: S,
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
    let framed = Framed::new(
        socket,
        FrameCodec::new(Arc::clone(&frame_limit), Arc::clone(&clock)),
    );
    let (sink, reader) = framed.split();

    let (order_tx, slots) = tokio::sync::mpsc::unbounded_channel();
    let activity = Arc::new(ActivityClock::new());
    let (writer_gone_tx, writer_gone) = oneshot::channel();
    let writer = writer::spawn(
        sink,
        WriterContext {
            slots,
            advertised: auth.security.advertised.clone(),
            activity: Arc::clone(&activity),
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
        Close::WriterGone => Duration::ZERO,
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

        // Queue this request's response slot to the writer BEFORE dispatching, so the
        // writer's output order is fixed by request arrival order.
        let (response_tx, slot) = ResponseSlot::open(permit);
        if self.order_tx.send(slot).is_err() {
            warn!("Writer task gone, closing connection");
            return Err(Close::WriterGone);
        }

        // On a SASL listener nothing but ApiVersions and the two SASL APIs is processed
        // before authentication succeeds; anything else closes the connection, as Kafka
        // does.
        let api_key = peek_api_key(&frame);
        if let Some(key) = api_key.filter(|k| is_sasl_api(*k)) {
            return self.serve_sasl(key, frame, response_tx).await;
        }
        if !self.auth.sasl.admits(api_key.unwrap_or(-1)) {
            warn!(
                "Closing connection from {} on {}: request api_key={api_key:?} before SASL authentication",
                self.auth.peer,
                self.auth.security.describe()
            );
            return Err(Close::Protocol);
        }

        match protocol::parse_request(frame, response_tx) {
            Ok(Some(request)) => {
                if self
                    .dispatcher
                    .dispatch(request, &self.long_poll_permits)
                    .await
                {
                    Ok(())
                } else {
                    error!("Worker channel closed");
                    Err(Close::Protocol)
                }
            }
            // A request the broker answers with an error (already queued on
            // `response_tx`); the connection stays open.
            Ok(None) => Ok(()),
            Err(e) => {
                warn!("Framing/Parse error from {}: {e}", self.auth.peer);
                Err(Close::Protocol)
            }
        }
    }

    /// Wait for an in-flight slot before reading the next frame. An unauthenticated
    /// client that pipelines without reading its responses is still bound by the
    /// pre-auth deadline here.
    async fn acquire_slot(&mut self) -> Result<OwnedSemaphorePermit, Close> {
        let pre_auth = !self.auth.sasl.admits(-1);
        tokio::select! {
            biased;
            _ = &mut self.writer_gone => Err(Close::WriterGone),
            // The semaphore is never closed, so acquiring fails only if that changes.
            permit = Arc::clone(&self.in_flight).acquire_owned() => {
                permit.map_err(|_| Close::Protocol)
            }
            () = tokio::time::sleep_until(self.pre_auth_deadline), if pre_auth => {
                Err(pre_auth_timeout(&self.auth.peer))
            }
        }
    }

    /// Requests read whose responses are not written yet (the caller holds one permit
    /// itself).
    fn outstanding(&self) -> usize {
        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION - 1 - self.in_flight.available_permits()
    }

    /// Wait for the next frame under three deadlines: the pre-auth deadline (SASL
    /// listeners, until authenticated), the per-frame read deadline (from the first
    /// byte of a frame) and the idle timeout (nothing read, nothing written, nothing
    /// outstanding).
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
            if self.outstanding() == 0 && pending_frame.is_none() {
                let deadline = tokio::time::Instant::from_std(self.activity.last())
                    + limits.connections_max_idle;
                if now >= deadline {
                    debug!(
                        "Closing connection from {}: idle for {:?}",
                        self.auth.peer, limits.connections_max_idle
                    );
                    return Err(Close::Timeout);
                }
                wake = wake.min(deadline);
            } else {
                // Re-check once the outstanding requests may have been answered.
                wake = wake.min(now + limits.connections_max_idle);
            }
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
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(capacity);
        let (liveness_tx, _liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        tokio::spawn(async move {
            let (socket, _) = server.accept().await.unwrap();
            let (cred_tx, _cred_rx) = crossbeam_channel::bounded(1);
            let security = ListenerSecurity {
                limits,
                ..ListenerSecurity::plaintext("127.0.0.1:0")
            };
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
            };
            handle_connection(
                socket,
                dispatcher,
                auth,
                tokio::time::Instant::now() + PRE_AUTH_TIMEOUT,
            )
            .await;
        });
        (addr, main_rx)
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
