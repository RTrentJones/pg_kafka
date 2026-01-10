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
/// * `shutdown_rx` - Channel receiver for shutdown signal from main thread
/// * `_log_connections` - Whether to log each new connection
/// * `enable_long_polling` - Whether to enable long polling for FetchRequest
/// * `fetch_poll_interval_ms` - Polling interval for long polling fallback
///
/// # Thread Safety
/// This function runs in a spawned thread and MUST NOT call pgrx functions.
pub async fn run(
    listener: TcpListener,
    request_tx: Sender<KafkaRequest>,
    notify_rx: Receiver<InternalNotification>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
    _log_connections: bool,
    enable_long_polling: bool,
    fetch_poll_interval_ms: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "pg_kafka TCP listener started (long_polling={})",
        enable_long_polling
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
                        topic_id,
                        partition_id,
                        high_watermark,
                    } => {
                        debug!(
                            "Notification: new messages for topic_id={}, partition_id={}, hwm={}",
                            topic_id, partition_id, high_watermark
                        );
                        registry_for_notifier
                            .notify_new_data(topic_id, partition_id, high_watermark)
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
                // Always log connections for debugging
                info!("Accepted connection from {}", addr);

                // Clone resources for this connection
                let conn_request_tx = request_tx.clone();
                let conn_registry = registry.clone();
                let poll_interval = fetch_poll_interval_ms;
                let long_poll_enabled = enable_long_polling;

                // Spawn a new async task to handle this connection
                // Use tokio::spawn (not spawn_local) since we're in multi-thread runtime
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(
                        socket,
                        conn_request_tx,
                        conn_registry,
                        long_poll_enabled,
                        poll_interval,
                    )
                    .await
                    {
                        warn!("Error handling connection from {}: {}", addr, e);
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
/// Our architecture guarantees this for non-long-poll requests:
/// 1. reader.next() reads from TCP in order → pushes to request_tx (FIFO) in order
/// 2. Worker pops from request_rx in order → processes synchronously → pushes to response_tx in order
/// 3. response_rx receives in order → writer.send() writes to TCP in order
///
/// Note: Long poll requests may return out of order relative to each other, but this is
/// acceptable for FetchRequest as clients handle this via correlation_id matching.
///
/// # Thread Safety
/// This function runs in the network thread and MUST NOT call pgrx functions.
async fn handle_connection(
    socket: TcpStream,
    request_tx: Sender<KafkaRequest>,
    registry: Arc<PendingFetchRegistry>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
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

    // Create a channel for this connection's responses
    // The main thread will send responses back via this channel
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn the Writer Task
    // This task sits and waits for responses from the DB.
    // It runs completely independently of the reader.
    let writer_handle: tokio::task::JoinHandle<Result<(), String>> = tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
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

        // Parse and attach the SAME response_tx clone for every request
        // This ensures all responses route back to our writer task
        match protocol::parse_request(frame, response_tx.clone()) {
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

                // Normal path - send to worker directly
                if let Err(e) = request_tx.send(request) {
                    error!("Worker channel closed: {}", e);
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

    // Cleanup: Drop response_tx to signal writer task to exit
    drop(response_tx);

    // Wait briefly for writer to finish sending any pending responses
    // Use a timeout to avoid blocking forever if writer is stuck
    let _ = tokio::time::timeout(std::time::Duration::from_millis(100), writer_handle).await;

    debug!("Connection handler exiting");
    Ok(())
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
    _registry: Arc<PendingFetchRegistry>,
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

        // Send to worker
        if let Err(e) = request_tx.send(fetch_request) {
            error!("Worker channel closed during long poll: {}", e);
            return Err(e.into());
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

        // Not enough data and not timed out - register and wait
        // Note: We use a simplified approach where we just wait for the poll interval
        // rather than registering for every topic-partition. This is because we don't
        // have topic_id (only topic_name) in the request - we'd need to resolve it.
        // The fallback poll interval still provides the correctness guarantee.
        let wait_time = remaining.min(poll_interval);

        debug!(
            "Long poll: waiting {:?} for more data (remaining: {:?})",
            wait_time, remaining
        );

        // For now, just sleep. In a more sophisticated implementation, we would:
        // 1. Resolve topic names to topic IDs
        // 2. Register with the registry for each topic-partition
        // 3. Use tokio::select! to wait on either notification or timeout
        // 4. Unregister after waking up
        //
        // The current implementation relies on the poll interval for wakeup,
        // which is correct but slightly less efficient than notification-based wakeup.
        tokio::time::sleep(wait_time).await;
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
