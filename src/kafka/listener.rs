// TCP listener module for pg_kafka
//
// This module implements the TCP listener that binds to the configured port
// and accepts Kafka client connections. In Step 3, we parse Kafka protocol requests.
//
// Architecture note: The listener runs inside a tokio runtime, which is embedded
// in the pgrx background worker. This allows us to handle thousands of concurrent
// connections efficiently without blocking the Postgres main loop.

use tokio::net::{TcpListener, TcpStream};

use super::messages;
use super::protocol;

// Import conditional logging macros for test isolation
use crate::{pg_log, pg_warning};

/// Run the TCP listener
///
/// This is the main entry point for the Kafka protocol listener.
/// It binds to the configured host and port and spawns a new async task
/// for each incoming connection.
///
/// The shutdown_rx channel is used to signal when the worker should stop.
/// This allows graceful shutdown when Postgres sends SIGTERM.
///
/// In Step 3, we parse Kafka protocol requests (ApiVersions) and send responses
/// via the message queue to the background worker's main thread.
pub async fn run(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    host: &str,
    port: i32,
    log_connections: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Bind to configured host and port
    let bind_addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&bind_addr).await?;

    pg_log!("pg_kafka TCP listener bound to {}", bind_addr);

    // Accept loop: wait for new connections OR shutdown signal
    loop {
        tokio::select! {
            // Wait for new connection
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        // Log connection if configured
                        if log_connections {
                            pg_log!("Accepted connection from {}", addr);
                        }

                        // Spawn a new async task to handle this connection
                        // Use task_local::spawn_local since we're in a LocalSet (single-threaded)
                        tokio::task::spawn_local(async move {
                            if let Err(e) = handle_connection(socket).await {
                                pg_warning!("Error handling connection: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        pg_warning!("Error accepting connection: {}", e);
                    }
                }
            }
            // Wait for shutdown signal
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    pg_log!("TCP listener received shutdown signal");
                    break;
                }
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
/// 1. Read request from socket
/// 2. Parse request and send to main thread via queue
/// 3. Wait for response from main thread
/// 4. Encode and send response back to client
/// 5. Repeat until client disconnects
async fn handle_connection(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    pg_log!("New connection established, starting handle loop");

    // Create a channel for this specific connection's responses
    // The main thread will send responses back to us via this channel
    // Using tokio unbounded channel for async-friendly receive
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel();

    // Get the global request sender
    let request_tx = messages::request_sender();

    // Connection loop: handle multiple requests on the same connection
    loop {
        // Parse the next request from the socket
        pg_log!("Calling parse_request...");
        match protocol::parse_request(&mut socket, response_tx.clone()).await {
            Ok(Some(request)) => {
                pg_log!("Request parsed successfully, sending to main worker thread...");
                // Send the parsed request to the main worker thread for processing
                if let Err(e) = request_tx.send(request) {
                    pg_warning!("Failed to send request to worker thread: {}", e);
                    break;
                }
                pg_log!("Request sent to worker thread successfully via request_tx");


                // Wait for the response from the main thread
                // CRITICAL: Using tokio channel's async .recv() instead of blocking crossbeam recv()
                // This allows the tokio runtime to continue processing other tasks
                pg_log!("Waiting for response from main thread via response_rx...");
                match response_rx.recv().await {
                    Some(response) => {
                        pg_log!("Received response from main thread, sending to client...");
                        // Encode and send the response back to the client
                        if let Err(e) = protocol::send_response(&mut socket, response).await {
                            pg_warning!("Failed to send response to client: {}", e);
                            break;
                        }
                        pg_log!("Response sent to client successfully");
                    }
                    None => {
                        pg_warning!("Response channel closed by worker thread");
                        break;
                    }
                }
            }
            Ok(None) => {
                // Client closed the connection gracefully
                break;
            }
            Err(e) => {
                pg_warning!("Error parsing request: {}", e);
                break;
            }
        }
    }

    Ok(())
}
