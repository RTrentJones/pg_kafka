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
    log_connections: bool,
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
                // Log connection if configured
                if log_connections {
                    info!("Accepted connection from {}", addr);
                }

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

/// Handle a single client connection
///
/// Step 3: Parse Kafka protocol requests and send responses
///
/// Flow:
/// 1. Read request from socket (using LengthDelimitedCodec for automatic framing)
/// 2. Parse request and send to main thread via channel
/// 3. Wait for response from main thread
/// 4. Encode and send response back to client
/// 5. Repeat until client disconnects
///
/// # Thread Safety
/// This function runs in the network thread and MUST NOT call pgrx functions.
async fn handle_connection(
    socket: TcpStream,
    request_tx: Sender<KafkaRequest>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!("New connection established, starting handle loop");

    // Wrap socket in LengthDelimitedCodec for automatic size-prefix framing
    // Kafka uses big-endian 4-byte size prefix
    let mut framed = Framed::new(
        socket,
        LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(super::constants::MAX_REQUEST_SIZE as usize)
            .new_codec(),
    );

    // Create a channel for this specific connection's responses
    // The main thread will send responses back to us via this channel
    // Using tokio unbounded channel for async-friendly receive
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel();

    // Connection loop: handle multiple requests on the same connection
    loop {
        // Read the next frame from the socket
        // LengthDelimitedCodec automatically handles size prefix
        debug!("Waiting for next frame from client...");
        let frame = match framed.next().await {
            Some(Ok(bytes)) => bytes,
            Some(Err(e)) => {
                warn!("Error reading frame: {}", e);
                break;
            }
            None => {
                // Client closed connection gracefully
                debug!("Client closed connection (stream ended)");
                break;
            }
        };

        debug!("Received frame of {} bytes", frame.len());

        // Parse the request from the frame bytes
        match protocol::parse_request(frame, response_tx.clone()) {
            Ok(Some(request)) => {
                debug!("Request parsed successfully, sending to main worker thread...");
                // Send the parsed request to the main worker thread for processing
                // Use send() which blocks if channel is full (backpressure)
                if let Err(e) = request_tx.send(request) {
                    error!("Failed to send request to worker thread: {}", e);
                    break;
                }
                debug!("Request sent to worker thread successfully");

                // Wait for the response from the main thread
                // CRITICAL: Using tokio channel's async .recv() instead of blocking
                // This allows the tokio runtime to continue processing other tasks
                debug!("Waiting for response from main thread...");
                match response_rx.recv().await {
                    Some(response) => {
                        debug!("Received response from main thread, encoding...");
                        // Encode the response
                        match protocol::encode_response(response) {
                            Ok(response_bytes) => {
                                debug!(
                                    "Sending {} byte response to client...",
                                    response_bytes.len()
                                );
                                // Send the response frame
                                // LengthDelimitedCodec automatically adds size prefix
                                if let Err(e) = framed.send(response_bytes.freeze()).await {
                                    warn!("Failed to send response to client: {}", e);
                                    break;
                                }
                                debug!("Response sent to client successfully");
                            }
                            Err(e) => {
                                error!("Failed to encode response: {}", e);
                                break;
                            }
                        }
                    }
                    None => {
                        warn!("Response channel closed by worker thread");
                        break;
                    }
                }
            }
            Ok(None) => {
                // Parse error with error response already sent
                break;
            }
            Err(e) => {
                warn!("Error parsing request: {}", e);
                break;
            }
        }
    }

    Ok(())
}
