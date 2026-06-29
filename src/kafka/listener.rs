// TCP listener module for pg_kafka
//
// This module implements the TCP listener that binds to the configured port
// and accepts Kafka client connections. In Step 3, we parse Kafka protocol requests.
//
// ## Thread Safety
//
// This module runs in a SPAWNED THREAD separate from the main Postgres BGWorker.
// It MUST NOT call any pgrx functions (pg_log!, pgrx::error!, SPI, etc.).
// All logging uses the `tracing` crate which is thread-safe.
//
// Architecture: The listener runs a multi-threaded tokio runtime and sends
// parsed Kafka requests to the main thread via a crossbeam bounded channel.
//
// ## Long Polling Support
//
// When enabled, FetchRequest handlers will wait up to max_wait_ms for data if
// min_bytes threshold is not met. This is implemented via:
// - PendingFetchRegistry: tracks waiting fetch requests
// - InternalNotification: signals from DB thread when new data arrives
// - Per-connection long poll handlers that manage the wait/retry logic

use crossbeam_channel::{Receiver, Sender};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};

use super::messages::{KafkaRequest, KafkaResponse, TopicFetchData};
use super::notifications::InternalNotification;
use super::pending_fetches::PendingFetchRegistry;
use super::protocol;
use super::shadow::{ForwardRequest, ShadowConfig, ShadowProducer};

/// Shadow forwarder task for async forwarding
///
/// Receives ForwardRequest messages from the DB thread via crossbeam channel
/// and forwards them to external Kafka asynchronously (fire-and-forget).
///
/// This task uses RuntimeContext to check configuration dynamically, allowing
/// shadow mode to be enabled/disabled at runtime via ALTER SYSTEM + pg_reload_conf().
/// The producer is lazily created when shadow mode is first enabled and recreated
/// if the bootstrap servers change.
async fn run_shadow_forwarder(
    forward_rx: Receiver<ForwardRequest>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
) {
    info!("Shadow forwarder task started (dynamic config mode)");

    // Track current producer and config for change detection
    let mut producer: Option<Arc<ShadowProducer>> = None;
    let mut current_bootstrap_servers: String = String::new();

    loop {
        // Use spawn_blocking to receive from crossbeam channel in async context
        let rx = forward_rx.clone();
        let result =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_millis(100))).await;

        match result {
            Ok(Ok(req)) => {
                // Check current config from RuntimeContext (dynamic reload support)
                let config = runtime_context.config();

                // Skip if shadow mode is not enabled or not configured
                if !config.shadow_mode_enabled || config.shadow_bootstrap_servers.is_empty() {
                    debug!(
                        "Shadow mode disabled or not configured, dropping forward request for {}[{}]",
                        req.topic_name, req.partition_id
                    );
                    continue;
                }

                // Check if config changed - recreate producer if bootstrap servers changed
                if config.shadow_bootstrap_servers != current_bootstrap_servers {
                    info!(
                        "Shadow config changed: bootstrap_servers '{}' -> '{}'",
                        current_bootstrap_servers, config.shadow_bootstrap_servers
                    );

                    let shadow_config = Arc::new(ShadowConfig::from_config(&config));
                    match ShadowProducer::new(shadow_config) {
                        Ok(p) => {
                            current_bootstrap_servers = config.shadow_bootstrap_servers.clone();
                            producer = Some(Arc::new(p));
                            info!(
                                "Shadow producer created/recreated for bootstrap_servers='{}'",
                                current_bootstrap_servers
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to create shadow producer for '{}': {:?}",
                                config.shadow_bootstrap_servers, e
                            );
                            // Clear producer so we retry on next config check
                            producer = None;
                            current_bootstrap_servers.clear();
                            continue;
                        }
                    }
                }

                // Get producer (should exist if we passed the config check)
                let p = match &producer {
                    Some(p) => p.clone(),
                    None => {
                        warn!("No producer available, dropping forward request");
                        continue;
                    }
                };

                // Fire-and-forget: log failure but continue
                let topic = req.topic_name.clone();
                let partition = req.partition_id;
                let offset = req.local_offset;

                if let Err(e) = p
                    .send_async(
                        &req.topic_name,
                        Some(req.partition_id),
                        req.key.as_deref(),
                        req.value.as_deref(),
                    )
                    .await
                {
                    warn!(
                        "Async forward failed for {}[{}] offset {}: {:?}",
                        topic, partition, offset, e
                    );
                    // Note: No retry queue - this is intentional fire-and-forget
                } else {
                    debug!(
                        "Async forward succeeded for {}[{}] offset {}",
                        topic, partition, offset
                    );
                }
            }
            Ok(Err(crossbeam_channel::RecvTimeoutError::Timeout)) => {
                // Normal timeout, continue
            }
            Ok(Err(crossbeam_channel::RecvTimeoutError::Disconnected)) => {
                info!("Forward channel disconnected, stopping shadow forwarder");
                break;
            }
            Err(_) => {
                // spawn_blocking panicked
                error!("Shadow forwarder recv task panicked");
                break;
            }
        }
    }

    info!("Shadow forwarder task stopped");
}

/// Try to admit a new connection under the concurrent-connection cap (SEC-3).
///
/// Returns a permit to hold for the connection's lifetime, or `None` if the cap has been
/// reached (the caller must then close the new socket). Dropping the returned permit — which the
/// connection task does on exit — frees the slot for a future connection. `try_acquire` is
/// non-blocking so the accept loop keeps servicing shutdown checks while at capacity.
fn try_admit_connection(
    limiter: &Arc<tokio::sync::Semaphore>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    Arc::clone(limiter).try_acquire_owned().ok()
}

/// Run the TCP listener
///
/// This is the main entry point for the Kafka protocol listener.
/// It accepts a pre-bound TcpListener and spawns a new async task
/// for each incoming connection.
///
/// # Arguments
/// * `listener` - Pre-bound TcpListener (bound in worker.rs)
/// * `request_tx` - Channel sender for forwarding parsed requests to main thread
/// * `notify_rx` - Channel receiver for notifications from DB thread (long polling)
/// * `forward_rx` - Channel receiver for async shadow forwarding requests
/// * `runtime_context` - Runtime context for accessing configuration
/// * `shutdown_rx` - Channel receiver for shutdown signal from main thread
///
/// # Thread Safety
/// This function runs in a spawned thread and MUST NOT call pgrx functions.
pub async fn run(
    listener: TcpListener,
    request_tx: Sender<KafkaRequest>,
    notify_rx: Receiver<InternalNotification>,
    forward_rx: Receiver<ForwardRequest>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Extract config values from RuntimeContext
    let config = runtime_context.config();
    let enable_long_polling = config.enable_long_polling;
    let log_connections = config.log_connections;
    let fetch_poll_interval_ms = config.fetch_poll_interval_ms;

    // Note: Shadow config is now checked dynamically in run_shadow_forwarder
    // to support runtime enable/disable via ALTER SYSTEM + pg_reload_conf()

    info!(
        "pg_kafka TCP listener started (long_polling={}, log_connections={})",
        enable_long_polling, log_connections
    );

    // Create the pending fetch registry for long polling
    let registry = Arc::new(PendingFetchRegistry::new());

    // Spawn the notification receiver task
    // This task receives notifications from the DB thread and wakes waiting fetch handlers
    let registry_for_notifier = registry.clone();
    let notify_handle = tokio::spawn(async move {
        loop {
            // Use spawn_blocking to receive from crossbeam channel in async context
            let rx = notify_rx.clone();
            let result =
                tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_millis(100)))
                    .await;

            match result {
                Ok(Ok(notification)) => match notification {
                    InternalNotification::NewMessages {
                        topic_name,
                        topic_id,
                        partition_id,
                        high_watermark,
                    } => {
                        debug!(
                            "Notification: new messages for topic={} (id={}), partition_id={}, hwm={}",
                            topic_name, topic_id, partition_id, high_watermark
                        );
                        registry_for_notifier
                            .notify_new_data(&topic_name, partition_id, high_watermark)
                            .await;
                    }
                },
                Ok(Err(crossbeam_channel::RecvTimeoutError::Timeout)) => {
                    // Normal timeout, continue
                }
                Ok(Err(crossbeam_channel::RecvTimeoutError::Disconnected)) => {
                    info!("Notification channel disconnected, stopping notifier task");
                    break;
                }
                Err(_) => {
                    // spawn_blocking panicked
                    error!("Notification receiver task panicked");
                    break;
                }
            }
        }
    });

    // Spawn the shadow forwarder task (async forwarding from DB thread)
    // This task receives ForwardRequest messages and sends them to external Kafka.
    // Always spawned - config is checked dynamically to support runtime enable/disable.
    let runtime_context_for_forwarder = runtime_context.clone();
    let _forward_handle = tokio::spawn(async move {
        run_shadow_forwarder(forward_rx, runtime_context_for_forwarder).await;
    });

    // SEC-3: bound concurrent client connections to prevent fd/memory exhaustion. Each
    // connection task can buffer up to one MAX_REQUEST_SIZE frame, so unbounded connections
    // are a DoS vector. A permit is held for the connection's lifetime and freed on close.
    let conn_limiter = Arc::new(tokio::sync::Semaphore::new(
        crate::kafka::constants::MAX_CONNECTIONS,
    ));

    // Accept loop: wait for new connections OR shutdown signal
    loop {
        // Check for shutdown signal (non-blocking)
        match shutdown_rx.try_recv() {
            Ok(()) => {
                info!("TCP listener received shutdown signal");
                break;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                // No shutdown signal, continue
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                info!("Shutdown channel disconnected, exiting");
                break;
            }
        }

        // Accept with timeout to allow periodic shutdown checks
        let accept_result =
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept()).await;

        match accept_result {
            Ok(Ok((socket, addr))) => {
                // SEC-3: enforce the concurrent-connection cap. try_acquire is non-blocking so
                // the accept loop keeps servicing shutdown checks; at the cap we close the new
                // socket rather than buffer an unbounded number of connections.
                let permit = match try_admit_connection(&conn_limiter) {
                    Some(permit) => permit,
                    None => {
                        if log_connections {
                            warn!(
                                "Connection limit ({}) reached; rejecting {}",
                                crate::kafka::constants::MAX_CONNECTIONS,
                                addr
                            );
                        }
                        drop(socket);
                        continue;
                    }
                };

                // Log connections if enabled
                if log_connections {
                    info!("Kafka client connected from {}", addr);
                }

                // Clone resources for this connection
                let conn_request_tx = request_tx.clone();
                let conn_registry = registry.clone();
                let poll_interval = fetch_poll_interval_ms;
                let long_poll_enabled = enable_long_polling;
                let should_log = log_connections;

                // Spawn a new async task to handle this connection
                // Use tokio::spawn (not spawn_local) since we're in multi-thread runtime
                tokio::spawn(async move {
                    // Hold the connection permit for the task's lifetime; dropping it on exit
                    // frees a slot for a new connection (SEC-3).
                    let _permit = permit;
                    if let Err(e) = handle_connection(
                        socket,
                        conn_request_tx,
                        conn_registry,
                        long_poll_enabled,
                        poll_interval,
                        should_log,
                    )
                    .await
                    {
                        if should_log {
                            warn!("Error handling connection from {}: {}", addr, e);
                        }
                    } else if should_log {
                        info!("Kafka client disconnected from {}", addr);
                    }
                });
            }
            Ok(Err(e)) => {
                warn!("Error accepting connection: {}", e);
            }
            Err(_) => {
                // Timeout - normal, continue to check shutdown signal
            }
        }
    }

    // Cleanup: abort the notifier task
    notify_handle.abort();
    let _ = notify_handle.await;

    Ok(())
}

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
async fn handle_connection(
    socket: TcpStream,
    request_tx: Sender<KafkaRequest>,
    registry: Arc<PendingFetchRegistry>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
    _log_connections: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!("New connection established, starting pipelined handle loop");

    // Wrap socket in LengthDelimitedCodec for automatic size-prefix framing
    // Kafka uses big-endian 4-byte size prefix
    let framed = Framed::new(
        socket,
        LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(super::constants::MAX_REQUEST_SIZE as usize)
            .new_codec(),
    );

    // Split the framed socket into independent reader and writer halves
    // This enables concurrent reading and writing for pipelining support
    let (mut writer, mut reader) = framed.split();

    // Per-request response ordering: each request gets its own response
    // channel, and the receiving ends are queued to the writer in request
    // order. The writer fully drains one request's channel (closed when the
    // request's sender is dropped) before moving to the next, guaranteeing
    // responses are written in request order.
    type ResponseSlot = tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>;
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel::<ResponseSlot>();

    // Spawn the Writer Task
    // This task sits and waits for responses from the DB.
    // It runs completely independently of the reader.
    let writer_handle: tokio::task::JoinHandle<Result<(), String>> = tokio::spawn(async move {
        while let Some(mut slot) = order_rx.recv().await {
            // A request yields at most one response; one that yields none
            // (acks=0 produce) closes the channel without sending and is
            // skipped here.
            while let Some(response) = slot.recv().await {
                debug!("Writer received response, encoding...");
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
                    warn!("Failed to send response to client: {}", e);
                    return Err(format!("Write error: {}", e));
                }
                debug!("Response sent to client successfully");
            }
        }
        Ok(())
    });

    // Run the Reader Loop (Main Task)
    // This loop ONLY reads and dispatches. It never waits for the DB response.
    while let Some(frame_result) = reader.next().await {
        let frame = match frame_result {
            Ok(f) => f,
            Err(e) => {
                warn!("Error reading frame: {}", e);
                break;
            }
        };

        debug!("Received frame of {} bytes", frame.len());

        // Create this request's response channel and queue its receiver to
        // the writer BEFORE dispatching, so the writer's output order is
        // fixed by request arrival order.
        let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();
        if order_tx.send(response_rx).is_err() {
            warn!("Writer task gone, closing connection");
            break;
        }

        match protocol::parse_request(frame, response_tx) {
            Ok(Some(request)) => {
                debug!("Request parsed successfully, dispatching to worker...");

                // Check if this is a Fetch request that should use long polling
                let should_long_poll = enable_long_polling
                    && matches!(
                        &request,
                        KafkaRequest::Fetch { max_wait_ms, .. } if *max_wait_ms > 0
                    );

                if should_long_poll {
                    // Destructure for long polling - we know it's a Fetch with max_wait_ms > 0
                    if let KafkaRequest::Fetch {
                        correlation_id,
                        client_id,
                        api_version,
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation_level,
                        topic_data,
                        response_tx: original_response_tx,
                    } = request
                    {
                        debug!(
                            "Fetch request with long polling: max_wait_ms={}, min_bytes={}, isolation_level={}",
                            max_wait_ms, min_bytes, isolation_level
                        );

                        let long_poll_registry = registry.clone();
                        let long_poll_request_tx = request_tx.clone();
                        let poll_interval = poll_interval_ms;

                        tokio::spawn(async move {
                            if let Err(e) = handle_fetch_long_poll(
                                correlation_id,
                                client_id,
                                api_version,
                                max_wait_ms,
                                min_bytes,
                                max_bytes,
                                isolation_level,
                                topic_data,
                                long_poll_request_tx,
                                original_response_tx,
                                long_poll_registry,
                                poll_interval,
                            )
                            .await
                            {
                                warn!("Long poll handler error: {}", e);
                            }
                        });

                        // Don't send to worker directly - the long poll handler will do it
                        continue;
                    }
                }

                // Normal path - send to worker directly (SEC-4: non-blocking handoff)
                if !send_with_backpressure(&request_tx, request).await {
                    error!("Worker channel closed");
                    break;
                }
            }
            Ok(None) => {
                // Parse error - error response already queued via response_tx
                // Continue reading more requests (don't break connection)
                debug!("Parse returned None, continuing...");
            }
            Err(e) => {
                warn!("Framing/Parse error: {}", e);
                break;
            }
        }
    }

    // Cleanup: Drop order_tx so the writer exits once it has drained the
    // response slots of requests that were already dispatched
    drop(order_tx);

    // Wait briefly for writer to finish sending any pending responses, then
    // abort it: an in-flight long poll could otherwise hold the task (and
    // the half-open socket) alive for up to max_wait_ms after disconnect
    let mut writer_handle = writer_handle;
    if tokio::time::timeout(std::time::Duration::from_millis(100), &mut writer_handle)
        .await
        .is_err()
    {
        writer_handle.abort();
        let _ = writer_handle.await;
    }

    debug!("Connection handler exiting");
    Ok(())
}

/// SEC-4: hand a request to the DB thread without blocking the async runtime.
///
/// The request channel is a bounded crossbeam channel; a plain `Sender::send` *blocks the calling
/// thread* when the channel is full (DB thread behind), which on the tokio runtime parks a whole
/// worker thread and starves every other connection scheduled on it. Instead we `try_send` and, on a
/// full channel, `await` a short sleep and retry — yielding the worker to other tasks. This preserves
/// the bounded channel's backpressure (we never drop or reorder the request, we wait for room) while
/// keeping the runtime responsive. The normal, not-full case takes the first `try_send` with no sleep.
///
/// Returns `true` once the item is enqueued, or `false` if the channel is disconnected (DB thread
/// gone) — the caller tears down the connection in that case.
async fn send_with_backpressure<T>(tx: &Sender<T>, mut item: T) -> bool {
    loop {
        match tx.try_send(item) {
            Ok(()) => return true,
            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                item = returned;
                // Yield the worker thread instead of blocking it, then retry.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
        }
    }
}

/// Handle a FetchRequest with long polling support
///
/// This function implements the long polling logic:
/// 1. Send fetch to worker
/// 2. Check if response has enough data (>= min_bytes)
/// 3. If not enough data and timeout not reached:
///    - Register with registry for notifications
///    - Wait for notification or poll interval
///    - Re-send fetch
/// 4. Forward final response to writer
#[allow(clippy::too_many_arguments)]
async fn handle_fetch_long_poll(
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topic_data: Vec<TopicFetchData>,
    request_tx: Sender<KafkaRequest>,
    final_response_tx: UnboundedSender<KafkaResponse>,
    registry: Arc<PendingFetchRegistry>,
    poll_interval_ms: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms as u64);
    let poll_interval = Duration::from_millis(poll_interval_ms as u64);

    loop {
        // Create a response channel for this fetch attempt
        let (inner_tx, mut inner_rx) = tokio::sync::mpsc::unbounded_channel();

        // Reconstruct the Fetch request with our response channel
        let fetch_request = KafkaRequest::Fetch {
            correlation_id,
            client_id: client_id.clone(),
            api_version,
            max_wait_ms: 0, // Don't long poll again in worker
            min_bytes: 0,   // Accept any amount of data
            max_bytes,
            isolation_level,
            topic_data: topic_data.clone(),
            response_tx: inner_tx,
        };

        // Send to worker (SEC-4: non-blocking handoff)
        if !send_with_backpressure(&request_tx, fetch_request).await {
            error!("Worker channel closed during long poll");
            return Err("Worker channel closed".into());
        }

        // Wait for response
        let response = match inner_rx.recv().await {
            Some(resp) => resp,
            None => {
                error!("Response channel closed during long poll");
                return Err("Response channel closed".into());
            }
        };

        // Check if we have enough data
        let total_bytes = calculate_response_bytes(&response);
        debug!(
            "Long poll fetch returned {} bytes (min_bytes={})",
            total_bytes, min_bytes
        );

        if total_bytes >= min_bytes as usize || min_bytes <= 0 {
            // Enough data - forward to client
            debug!("Long poll: min_bytes threshold met, returning response");
            let _ = final_response_tx.send(response);
            return Ok(());
        }

        // Check timeout
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // Timeout reached - return whatever we have
            debug!("Long poll: timeout reached, returning response");
            let _ = final_response_tx.send(response);
            return Ok(());
        }

        // Not enough data and not timed out. QA-5: register every requested topic-partition on one
        // shared Notify, then wait for either a produce notification (low latency) or the poll
        // interval. The poll interval is retained as a correctness fallback — if a wake-up is ever
        // missed (e.g. a produce lands in the brief window between this fetch and registration), the
        // behaviour degrades to exactly the previous fixed-interval polling rather than stalling.
        let wait_time = remaining.min(poll_interval);
        let notify = Arc::new(tokio::sync::Notify::new());
        for topic in &topic_data {
            for partition in &topic.partitions {
                registry
                    .register(
                        &topic.name,
                        partition.partition_index,
                        partition.fetch_offset,
                        notify.clone(),
                    )
                    .await;
            }
        }

        debug!(
            "Long poll: waiting up to {:?} for new-data notification (remaining: {:?})",
            wait_time, remaining
        );

        tokio::select! {
            _ = notify.notified() => {
                debug!("Long poll: woken by new-data notification");
            }
            _ = tokio::time::sleep(wait_time) => {
                debug!("Long poll: poll interval elapsed, re-fetching");
            }
        }

        // Clean up our registrations before the next iteration (or return).
        for topic in &topic_data {
            for partition in &topic.partitions {
                registry
                    .unregister(&topic.name, partition.partition_index, &notify)
                    .await;
            }
        }
    }
}

/// Calculate the total bytes in a FetchResponse
fn calculate_response_bytes(response: &KafkaResponse) -> usize {
    match response {
        KafkaResponse::Fetch { response, .. } => response
            .responses
            .iter()
            .flat_map(|t| t.partitions.iter())
            .filter_map(|p| p.records.as_ref())
            .map(|r| r.len())
            .sum(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_admit_connection_enforces_cap_and_frees_on_drop() {
        // SEC-3: admission must reject once the concurrent-connection cap is reached, and free a
        // slot when a permit (held for a connection's lifetime) is dropped. This is the policy the
        // accept loop relies on to bound fd/memory usage.
        let limiter = Arc::new(tokio::sync::Semaphore::new(2));
        let p1 = try_admit_connection(&limiter).expect("1st connection admitted under the cap");
        let _p2 = try_admit_connection(&limiter).expect("2nd connection admitted under the cap");
        assert!(
            try_admit_connection(&limiter).is_none(),
            "a 3rd connection must be rejected at the cap of 2"
        );
        drop(p1); // a connection closes, dropping its permit
        assert!(
            try_admit_connection(&limiter).is_some(),
            "closing a connection must free a slot for a new one"
        );
    }

    #[tokio::test]
    async fn test_send_with_backpressure_succeeds_once_room_frees() {
        // SEC-4: a full bounded channel must not make the send hang forever or drop the item — it
        // waits (yielding the runtime) until a slot frees, then enqueues the item in order.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        tx.try_send(1).unwrap(); // fill to capacity

        let tx2 = tx.clone();
        let send = tokio::spawn(async move { send_with_backpressure(&tx2, 2).await });

        // Let the sender spin a few backoff iterations, then free a slot.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(rx.recv().unwrap(), 1, "first item drains");

        let join = tokio::time::timeout(Duration::from_millis(500), send).await;
        assert!(join.is_ok(), "send task should finish, not hang");
        let sent = join.unwrap().expect("send task should not panic");
        assert!(sent, "send should report success once room frees");
        assert_eq!(
            rx.recv().unwrap(),
            2,
            "the backpressured item is enqueued in order"
        );
    }

    #[tokio::test]
    async fn test_send_with_backpressure_reports_disconnect() {
        // SEC-4: if the DB thread is gone (receiver dropped), the send reports failure so the caller
        // tears down the connection instead of looping forever.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        drop(rx);
        assert!(
            !send_with_backpressure(&tx, 1).await,
            "a disconnected channel must report failure"
        );
    }
}
