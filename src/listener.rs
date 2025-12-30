// TCP listener module for pg_kafka
//
// This module implements the TCP listener that binds to the configured port
// and accepts Kafka client connections. In Step 2, we just send a hello message.
//
// Architecture note: The listener runs inside a tokio runtime, which is embedded
// in the pgrx background worker. This allows us to handle thousands of concurrent
// connections efficiently without blocking the Postgres main loop.

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// Run the TCP listener
///
/// This is the main entry point for the Kafka protocol listener.
/// It binds to the configured host and port and spawns a new async task
/// for each incoming connection.
///
/// The shutdown_rx channel is used to signal when the worker should stop.
/// This allows graceful shutdown when Postgres sends SIGTERM.
///
/// In Step 2, we just send "Hello from pg_kafka\n" and close the connection.
/// In Step 3, we'll parse actual Kafka protocol requests and send responses.
pub async fn run(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    host: &str,
    port: i32,
    log_connections: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Bind to configured host and port
    let bind_addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&bind_addr).await?;

    pgrx::log!("pg_kafka TCP listener bound to {}", bind_addr);

    // Accept loop: wait for new connections OR shutdown signal
    loop {
        tokio::select! {
            // Wait for new connection
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        // Log connection if configured
                        if log_connections {
                            pgrx::log!("Accepted connection from {}", addr);
                        }

                        // Spawn a new async task to handle this connection
                        // tokio::spawn creates a green thread that runs concurrently
                        // with other tasks in the same OS thread pool
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(socket).await {
                                pgrx::warning!("Error handling connection: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        pgrx::warning!("Error accepting connection: {}", e);
                    }
                }
            }
            // Wait for shutdown signal
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    pgrx::log!("TCP listener received shutdown signal");
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Handle a single client connection
///
/// In Step 2: Just send hello message and close
/// In Step 3: Parse Kafka protocol requests and send responses
async fn handle_connection(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    // Send hello message
    socket.write_all(b"Hello from pg_kafka\n").await?;

    // Flush to ensure data is sent before closing
    socket.flush().await?;

    // Socket automatically closes when it goes out of scope
    Ok(())
}
