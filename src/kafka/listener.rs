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

use crossbeam_channel::Sender;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};

use super::messages::KafkaRequest;
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
/// * `shutdown_rx` - Channel receiver for shutdown signal from main thread
/// * `log_connections` - Whether to log each new connection
///
/// # Thread Safety
/// This function runs in a spawned thread and MUST NOT call pgrx functions.
pub async fn run(
    listener: TcpListener,
    request_tx: Sender<KafkaRequest>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
    _log_connections: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("pg_kafka TCP listener started");

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

                // Clone the sender for this connection
                let conn_request_tx = request_tx.clone();

                // Spawn a new async task to handle this connection
                // Use tokio::spawn (not spawn_local) since we're in multi-thread runtime
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, conn_request_tx).await {
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

    Ok(())
}

/// Handle a single client connection with request pipelining support
///
/// This function supports Kafka request pipelining by splitting the socket into
/// separate reader and writer halves that operate concurrently:
/// - Reader task: Continuously reads requests and dispatches them to the worker
/// - Writer task: Continuously waits for responses and sends them back
///
/// This allows clients to send multiple requests without waiting for responses,
/// which is required by rdkafka's AdminClient implementation.
///
/// ## Ordering Guarantee
/// Kafka requires: if Request A comes before Request B, Response A must come before Response B.
/// Our architecture guarantees this:
/// 1. reader.next() reads from TCP in order → pushes to request_tx (FIFO) in order
/// 2. Worker pops from request_rx in order → processes synchronously → pushes to response_tx in order
/// 3. response_rx receives in order → writer.send() writes to TCP in order
///
/// # Thread Safety
/// This function runs in the network thread and MUST NOT call pgrx functions.
async fn handle_connection(
    socket: TcpStream,
    request_tx: Sender<KafkaRequest>,
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
