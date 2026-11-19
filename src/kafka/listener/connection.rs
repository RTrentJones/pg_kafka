//! One client connection: the read loop, the response writer and how it closes.

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::dispatch::dispatch_request;
use super::is_peer_disconnect;
use super::sasl::{handle_sasl_frame, ConnectionAuth};
use crate::kafka::api::ResponseBody;
use crate::kafka::conn_limits::NetworkLimits;
use crate::kafka::constants::{
    MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_LONG_POLLS_PER_CONNECTION,
};
use crate::kafka::messages::KafkaResponse;
use crate::kafka::pending_fetches::PendingFetchRegistry;
use crate::kafka::protocol;
use crate::kafka::protocol::framing::{FrameClock, FrameCodec, PRE_AUTH_MAX_FRAME};

/// wave3 net: how long a client has, from the moment its TCP connection is accepted, to
/// finish everything before it is authenticated: the TLS handshake (SSL / SASL_SSL) and,
/// on a SASL listener, SASL authentication, including any wait for a credential lookup
/// or a password check. One deadline from accept, so SASL_SSL gets 10 s in total.
pub(super) const PRE_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// wave3 net: how long a connection that is being closed (bad frame, SASL failure,
/// timeout, acks=0 error) waits for the responses of the requests it read before, so
/// that they are delivered first as Kafka's serial reader guarantees (a committed
/// produce is answered). Peers that went away get only a short grace.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Grace for the writer when the peer disconnected (nothing can be delivered).
const PEER_GONE_GRACE: Duration = Duration::from_millis(100);

/// Handle a single client connection with request pipelining support
///
/// This function supports Kafka request pipelining by splitting the socket into
/// separate reader and writer halves that operate concurrently:
/// - Reader task: Continuously reads requests and dispatches them to the worker
/// - Writer task: Continuously waits for responses and sends them back
///
/// For FetchRequest with long polling enabled, a separate task is spawned to
/// handle the wait/retry logic without blocking other requests.
///
/// ## Ordering Guarantee
/// Kafka requires: if Request A comes before Request B, Response A must come before Response B.
/// Clients that pipeline requests (the official Java client enforces strict per-connection
/// FIFO and kills the connection on a correlation-id mismatch) depend on this.
///
/// The reader creates a fresh response channel per request and queues the receiving ends,
/// in request order, to the writer task. The writer drains one request's responses before
/// moving to the next, so responses hit the socket in request order even when a long-poll
/// fetch resolves in a detached task long after later requests completed. A request that
/// produces no response (acks=0 produce) closes its channel without sending and the writer
/// skips it. A long-poll fetch therefore blocks later responses on the same connection
/// until it resolves — the same behavior as a real Kafka broker, which mutes a connection
/// while a request waits in purgatory.
///
/// # Thread Safety
/// This function runs in the network thread and MUST NOT call pgrx functions.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_connection<S>(
    socket: S,
    request_lanes: crate::kafka::RequestLanes,
    registry: Arc<PendingFetchRegistry>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
    mut auth: ConnectionAuth,
    limits: NetworkLimits,
    pre_auth_deadline: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    debug!("New connection established, starting pipelined handle loop");

    // Frame codec (protocol::framing): charges the global budget only for bytes actually
    // buffered of a large incomplete frame (RV-9, wave3 net item 1) and records when that
    // frame started arriving, for the per-frame read deadline.
    let frame_limit = Arc::new(AtomicUsize::new(
        if auth.sasl.is_authenticated() || !auth.security.protocol.uses_sasl() {
            crate::kafka::constants::MAX_REQUEST_SIZE as usize
        } else {
            PRE_AUTH_MAX_FRAME
        },
    ));
    let clock = Arc::new(FrameClock::new());
    let framed = Framed::new(socket, FrameCodec::new(frame_limit.clone(), clock.clone()));

    // Split the framed socket into independent reader and writer halves
    // This enables concurrent reading and writing for pipelining support
    let (mut writer, mut reader) = framed.split();

    // Per-request response ordering: each request gets its own response
    // channel, and the receiving ends are queued to the writer in request
    // order. The writer fully drains one request's channel (closed when the
    // request's sender is dropped) before moving to the next, guaranteeing
    // responses are written in request order.
    //
    // Each queued slot carries an in-flight permit that the writer drops only
    // after the slot's response has been written to the socket. The reader
    // acquires a permit before reading the next frame, so at most
    // MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION requests per connection are read but
    // unanswered. A client that pipelines without reading responses fills its
    // TCP receive window, the writer blocks, permits stop coming back, and the
    // reader stops reading: the connection's memory is bounded and the
    // backpressure reaches the client through TCP.
    type ResponseSlot = (
        tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
        tokio::sync::OwnedSemaphorePermit,
    );
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel::<ResponseSlot>();
    let in_flight = Arc::new(tokio::sync::Semaphore::new(
        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
    ));

    // RV-10 follow-up: each long-poll fetch spawns a detached task, previously
    // bounded only by MAX_CONNECTIONS × the client's pipelining rate. Cap the
    // concurrent long-poll tasks per connection; a fetch over the cap degrades
    // gracefully to the immediate (non-waiting) fetch path — a valid Kafka
    // response (possibly empty), never an error or a dropped request.
    let long_poll_permits = Arc::new(tokio::sync::Semaphore::new(MAX_LONG_POLLS_PER_CONNECTION));

    // An additional listener advertises its own host:port in Metadata/FindCoordinator,
    // so a client that bootstrapped on it stays on it (Kafka's per-listener
    // advertised.listeners).
    let advertised = auth.security.advertised.clone();

    // wave3 net: last time a frame was read or a response written (idle timeout).
    let activity = Arc::new(ActivityClock::new());
    let writer_activity = activity.clone();
    // wave3 net (item 2): the writer owns `writer_gone_tx`; however it exits (an encode
    // or write error, a panic, an acks=0 close request, all slots done), the sender is
    // dropped and the reader closes the connection instead of reading requests whose
    // responses can never be written.
    let (writer_gone_tx, mut writer_gone_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn the Writer Task
    // This task sits and waits for responses from the DB.
    // It runs completely independently of the reader.
    let peer_for_writer = auth.peer.clone();
    let mut writer_handle: tokio::task::JoinHandle<Result<(), String>> = tokio::spawn(async move {
        let _writer_gone = writer_gone_tx;
        while let Some((mut slot, _in_flight_permit)) = order_rx.recv().await {
            // A request yields at most one response; one that yields none
            // (acks=0 produce) closes the channel without sending and is
            // skipped here.
            while let Some(mut response) = slot.recv().await {
                if matches!(response.body, ResponseBody::CloseConnection) {
                    // wave3 net (item 4): an acks=0 produce hit a partition error;
                    // Kafka closes the connection (after the request's writes
                    // committed) so the client refreshes its metadata. Earlier
                    // responses are already on the socket.
                    debug!(
                        "Closing connection from {} after an acks=0 produce error",
                        peer_for_writer
                    );
                    let _ = writer.close().await;
                    return Ok(());
                }
                debug!("Writer received response, encoding...");
                if let Some((host, port)) = &advertised {
                    rewrite_advertised(&mut response, host, *port);
                }
                let response_bytes = match protocol::encode_response(response) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!("Failed to encode response: {}", e);
                        return Err(format!("Encoding error: {}", e));
                    }
                };

                debug!(
                    "Sending {} byte response to client...",
                    response_bytes.len()
                );
                if let Err(e) = writer.send(response_bytes.freeze()).await {
                    if is_peer_disconnect(&e) {
                        debug!("Client went away while sending response: {}", e);
                    } else {
                        warn!("Failed to send response to client: {}", e);
                    }
                    return Err(format!("Write error: {}", e));
                }
                writer_activity.touch();
                debug!("Response sent to client successfully");
            }
            // `_in_flight_permit` drops here: this request's response (if any) is
            // on the socket, so the reader may read one more frame.
        }
        Ok(())
    });

    // Run the Reader Loop (Main Task)
    // This loop ONLY reads and dispatches. It never waits for the DB response.
    let close = 'conn: loop {
        // Backpressure: wait for an in-flight slot before reading the next frame.
        // The semaphore is never closed, so acquire only fails if that changes.
        // An unauthenticated client that pipelines without reading its responses is still
        // bound by the pre-auth deadline here.
        let pre_auth = !auth.sasl.admits(-1);
        let in_flight_permit = tokio::select! {
            biased;
            _ = &mut writer_gone_rx => break 'conn Close::WriterGone,
            permit = in_flight.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break 'conn Close::Protocol,
            },
            _ = tokio::time::sleep_until(pre_auth_deadline), if pre_auth => {
                warn!(
                    "Closing connection from {}: SASL authentication not completed within {:?}",
                    auth.peer, PRE_AUTH_TIMEOUT
                );
                break 'conn Close::Timeout;
            }
        };
        // Time spent not reading (backpressure) does not count against a frame the
        // client had started.
        clock.restart_if_pending();

        // wave3 net (items 1, 8): wait for the next frame under three deadlines: the
        // pre-auth deadline (SASL listeners, until authenticated), the per-frame read
        // deadline (from the first byte of a frame) and the idle timeout (nothing read,
        // nothing written, nothing outstanding).
        let next = loop {
            let now = tokio::time::Instant::now();
            let mut wake = now + limits.request_read_timeout;
            if !auth.sasl.admits(-1) {
                if now >= pre_auth_deadline {
                    warn!(
                        "Closing connection from {}: SASL authentication not completed within {:?}",
                        auth.peer, PRE_AUTH_TIMEOUT
                    );
                    break 'conn Close::Timeout;
                }
                wake = wake.min(pre_auth_deadline);
            }
            let pending_frame = clock.pending_since();
            if let Some(started) = pending_frame {
                let deadline =
                    tokio::time::Instant::from_std(started) + limits.request_read_timeout;
                if now >= deadline {
                    warn!(
                        "Closing connection from {}: request not received completely within {:?}",
                        auth.peer, limits.request_read_timeout
                    );
                    break 'conn Close::Timeout;
                }
                wake = wake.min(deadline);
            }
            // We hold one permit ourselves; every other missing one is a request whose
            // response is not written yet.
            let outstanding =
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION - 1 - in_flight.available_permits();
            if outstanding == 0 && pending_frame.is_none() {
                let deadline =
                    tokio::time::Instant::from_std(activity.last()) + limits.connections_max_idle;
                if now >= deadline {
                    debug!(
                        "Closing connection from {}: idle for {:?}",
                        auth.peer, limits.connections_max_idle
                    );
                    break 'conn Close::Timeout;
                }
                wake = wake.min(deadline);
            } else {
                // Re-check once the outstanding requests may have been answered.
                wake = wake.min(now + limits.connections_max_idle);
            }
            tokio::select! {
                biased;
                _ = &mut writer_gone_rx => break 'conn Close::WriterGone,
                read = tokio::time::timeout_at(wake, reader.next()) => match read {
                    Ok(next) => break next,
                    Err(_) => continue,
                },
            }
        };
        let frame = match next {
            None => break 'conn Close::PeerGone,
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                if is_peer_disconnect(&e) {
                    debug!("Client disconnected mid-frame: {}", e);
                    break 'conn Close::PeerGone;
                }
                warn!("Error reading frame from {}: {}", auth.peer, e);
                break 'conn Close::Protocol;
            }
        };
        activity.touch();

        debug!("Received frame of {} bytes", frame.len());

        // Create this request's response channel and queue its receiver to
        // the writer BEFORE dispatching, so the writer's output order is
        // fixed by request arrival order.
        let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();
        if order_tx.send((response_rx, in_flight_permit)).is_err() {
            warn!("Writer task gone, closing connection");
            break 'conn Close::WriterGone;
        }

        // Listener security: SaslHandshake / SaslAuthenticate are answered here, on
        // the network thread, from the connection's SASL state. On a SASL listener
        // nothing but ApiVersions and those two is processed before authentication
        // succeeds; anything else closes the connection, as Kafka does.
        let api_key = crate::kafka::auth::session::peek_api_key(&frame);
        if let Some(key) = api_key.filter(|k| crate::kafka::auth::session::is_sasl_api(*k)) {
            debug!("SASL request api_key={} from {}", key, auth.peer);
            // wave3 net (item 8): the pre-auth deadline also covers the wait for the
            // credential lookup, the lookup rate limit and the password-check slots.
            let keep_open = if auth.sasl.is_authenticated() {
                handle_sasl_frame(&mut auth, frame, response_tx).await
            } else {
                match tokio::time::timeout_at(
                    pre_auth_deadline,
                    handle_sasl_frame(&mut auth, frame, response_tx),
                )
                .await
                {
                    Ok(keep_open) => keep_open,
                    Err(_) => {
                        warn!(
                            "Closing connection from {}: SASL authentication not completed within {:?}",
                            auth.peer, PRE_AUTH_TIMEOUT
                        );
                        break 'conn Close::Timeout;
                    }
                }
            };
            if keep_open {
                if auth.sasl.is_authenticated() {
                    frame_limit.store(
                        crate::kafka::constants::MAX_REQUEST_SIZE as usize,
                        Ordering::Relaxed,
                    );
                }
                continue;
            }
            break 'conn Close::Protocol;
        }
        if !auth.sasl.admits(api_key.unwrap_or(-1)) {
            warn!(
                "Closing connection from {} on {}://{}: request api_key={:?} before SASL authentication",
                auth.peer,
                auth.security.protocol.name(),
                auth.security.bind_addr,
                api_key
            );
            break 'conn Close::Protocol;
        }

        match protocol::parse_request(frame, response_tx) {
            Ok(Some(request)) => {
                if !dispatch_request(
                    request,
                    &request_lanes,
                    &registry,
                    &long_poll_permits,
                    enable_long_polling,
                    poll_interval_ms,
                )
                .await
                {
                    error!("Worker channel closed");
                    break 'conn Close::Protocol;
                }
            }
            Ok(None) => {
                // Parse error - error response already queued via response_tx
                // Continue reading more requests (don't break connection)
                debug!("Parse returned None, continuing...");
            }
            Err(e) => {
                warn!("Framing/Parse error from {}: {}", auth.peer, e);
                break 'conn Close::Protocol;
            }
        }
    };

    // Cleanup: Drop order_tx so the writer exits once it has drained the
    // response slots of requests that were already dispatched
    drop(order_tx);

    // wave3 net (item 6): Kafka reads a connection serially, so every request read
    // before the one that closes the connection is answered first. Give the writer the
    // time to deliver them (bounded; a committed acks=1 produce must be answered),
    // unless the peer is gone, then abort it: an in-flight long poll could otherwise
    // hold the task (and the half-open socket) alive for up to max_wait_ms.
    let grace = match close {
        Close::PeerGone => PEER_GONE_GRACE,
        Close::WriterGone => Duration::ZERO,
        Close::Protocol | Close::Timeout => CLOSE_DRAIN_TIMEOUT,
    };
    if tokio::time::timeout(grace, &mut writer_handle)
        .await
        .is_err()
    {
        if grace >= CLOSE_DRAIN_TIMEOUT {
            warn!(
                "Closing connection from {}: responses still pending after {:?}",
                auth.peer, grace
            );
        }
        writer_handle.abort();
        let _ = writer_handle.await;
    }

    if auth.log_connections {
        info!(
            "Kafka connection from {} ({}) closed ({:?})",
            auth.peer, auth.principal, close
        );
    }
    debug!("Connection handler exiting");
    Ok(())
}

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

/// wave3 net: when a connection last read a frame or wrote a response.
struct ActivityClock {
    epoch: Instant,
    last_ms: std::sync::atomic::AtomicU64,
}

impl ActivityClock {
    fn new() -> Self {
        ActivityClock {
            epoch: Instant::now(),
            last_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        self.last_ms
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn last(&self) -> Instant {
        self.epoch + Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }
}

/// Point Metadata brokers and FindCoordinator coordinators at this listener's
/// advertised address (single-node broker: every broker entry is this node).
fn rewrite_advertised(response: &mut KafkaResponse, host: &str, port: i32) {
    use kafka_protocol::protocol::StrBytes;
    match &mut response.body {
        ResponseBody::Metadata(response) => {
            for broker in response.brokers.iter_mut() {
                broker.host = StrBytes::from_string(host.to_string());
                broker.port = port;
            }
        }
        ResponseBody::FindCoordinator(response) => {
            if response.error_code == 0 && !response.host.is_empty() {
                response.host = StrBytes::from_string(host.to_string());
                response.port = port;
            }
            for c in response.coordinators.iter_mut() {
                if c.error_code == 0 && !c.host.is_empty() {
                    c.host = StrBytes::from_string(host.to_string());
                    c.port = port;
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::super::accept::ListenerSecurity;
    use super::*;
    use crate::kafka::auth::{ChannelLookup, ConnectionPrincipal};
    use crate::kafka::messages::KafkaRequest;

    /// An ApiVersions v0 request frame ([size][api_key][api_version][corr][client_id=null]).
    fn api_versions_v0_frame(correlation_id: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&18i16.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&correlation_id.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        let mut frame = (body.len() as i32).to_be_bytes().to_vec();
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

        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(10_000);
        let (liveness_tx, _liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        let lanes = crate::kafka::RequestLanes {
            main: main_tx,
            liveness: liveness_tx,
        };
        tokio::spawn(async move {
            let (socket, _) = server.accept().await.unwrap();
            let (cred_tx, _cred_rx) = crossbeam_channel::bounded(1);
            let auth = ConnectionAuth::new(
                "test".into(),
                Arc::new(ListenerSecurity::plaintext("127.0.0.1:0")),
                ConnectionPrincipal::anonymous(),
                ChannelLookup(cred_tx),
                false,
            );
            let _ = handle_connection(
                socket,
                lanes,
                Arc::new(PendingFetchRegistry::new()),
                false,
                1000,
                auth,
                NetworkLimits::default(),
                tokio::time::Instant::now() + PRE_AUTH_TIMEOUT,
            )
            .await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut bytes = Vec::new();
        for i in 0..total {
            bytes.extend_from_slice(&api_versions_v0_frame(i as i32));
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

    // ===== wave3 net: connection lifecycle =====

    /// Serve one connection on an ephemeral port with `limits`; requests go to the
    /// returned receiver (the fake DB thread).
    async fn serve_one(
        limits: NetworkLimits,
    ) -> (
        std::net::SocketAddr,
        crossbeam_channel::Receiver<KafkaRequest>,
    ) {
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (main_tx, main_rx) = crossbeam_channel::bounded::<KafkaRequest>(1000);
        let (liveness_tx, _liveness_rx) = crossbeam_channel::bounded::<KafkaRequest>(16);
        tokio::spawn(async move {
            let (socket, _) = server.accept().await.unwrap();
            let (cred_tx, _cred_rx) = crossbeam_channel::bounded(1);
            let auth = ConnectionAuth::new(
                "test".into(),
                Arc::new(ListenerSecurity::plaintext("127.0.0.1:0")),
                ConnectionPrincipal::anonymous(),
                ChannelLookup(cred_tx),
                false,
            );
            let _ = handle_connection(
                socket,
                crate::kafka::RequestLanes {
                    main: main_tx,
                    liveness: liveness_tx,
                },
                Arc::new(PendingFetchRegistry::new()),
                false,
                1000,
                auth,
                limits,
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
                Ok(Ok(0)) | Ok(Err(_)) => return Some((out, started.elapsed())),
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
        // Item 2/4: the writer exiting (here on the acks=0 CloseConnection instruction)
        // ends the connection; the earlier request's response is written first.
        use tokio::io::AsyncWriteExt;
        let (addr, rx) = serve_one(NetworkLimits::default()).await;
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
        // Item 6: the old code gave the writer 100 ms after a bad frame and then aborted
        // it, so a response that took longer (a long poll, a slow commit) was lost.
        use tokio::io::AsyncWriteExt;
        let (addr, rx) = serve_one(NetworkLimits::default()).await;
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
        let (addr, _rx) = serve_one(limits).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("an idle connection must close");
        assert!(after >= Duration::from_millis(250), "{after:?}");

        // A request outstanding for 800 ms keeps the connection open past the timeout.
        let (addr, rx) = serve_one(limits).await;
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
        // Item 1: a frame that never completes used to be waited for forever.
        use tokio::io::AsyncWriteExt;
        let limits = NetworkLimits {
            request_read_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        let (addr, _rx) = serve_one(limits).await;
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client.write_all(&1000u32.to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2, 3]).await.unwrap();
        let (_, after) = read_until_eof(&mut client, Duration::from_secs(3))
            .await
            .expect("a stalled frame must close the connection");
        assert!(after >= Duration::from_millis(250), "{after:?}");
    }
}
