//! The Kafka network edge (network thread): it accepts client connections on every
//! listener, reads and answers Kafka frames, and hands parsed requests to the DB thread.
//!
//! It runs on its own thread inside a multi-threaded tokio runtime and MUST NOT call any
//! pgrx function (logging, SPI, ...); it logs through `tracing`. Requests cross to the
//! background worker over bounded crossbeam channels.
//!
//! * [`thread`]: logging and the signal mask of the network thread.
//! * [`accept`]: the accept loops and connection admission (connection caps, TLS).
//! * [`connection`]: one connection's read loop, response writer and close handling.
//! * [`sasl`]: per-connection authentication state and the SASL frames.
//! * [`dispatch`]: handing requests to the DB thread.
//! * [`long_poll`]: Fetch long polling.

mod accept;
mod connection;
mod dispatch;
mod long_poll;
mod sasl;
mod thread;

pub use accept::ListenerSecurity;
pub use thread::{block_signals_on_this_thread, init_network_tracing};

use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use super::auth::{ChannelLookup, CredentialRequest};
use super::conn_limits::IpTracker;
use super::notifications::InternalNotification;
use super::pending_fetches::PendingFetchRegistry;
use super::shadow::{ForwardAck, ForwardRequest};
use accept::{accept_loop, AcceptContext};

/// Whether an I/O error is just the peer going away (reset / broken pipe /
/// aborted / EOF mid-frame). Those are routine for a broker and logged at
/// debug; anything else (oversized frame, exhausted budget, ...) is a warning.
fn is_peer_disconnect(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionReset | ConnectionAborted | BrokenPipe | UnexpectedEof | NotConnected
    )
}

/// Run the TCP listener
///
/// This is the main entry point for the Kafka protocol listener.
/// It accepts the pre-bound TcpListeners (the primary one first) and spawns a new
/// async task for each incoming connection.
///
/// # Arguments
/// * `listeners` - Pre-bound TcpListeners (bound in worker.rs) and their security
/// * `request_lanes` - Channel senders (main + DR-12 liveness lanes) to the DB thread
/// * `credential_tx` - Channel to the DB thread for SASL credential lookups
/// * `notify_rx` - Channel receiver for notifications from DB thread (long polling)
/// * `forward_rx` - Channel receiver for async shadow forwarding requests
/// * `runtime_context` - Runtime context for accessing configuration
/// * `shutdown_rx` - Channel receiver for shutdown signal from main thread
///
/// # Thread Safety
/// This function runs in a spawned thread and MUST NOT call pgrx functions.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    listeners: Vec<(TcpListener, Arc<ListenerSecurity>)>,
    request_lanes: crate::kafka::RequestLanes,
    credential_tx: Sender<CredentialRequest>,
    notify_rx: Receiver<InternalNotification>,
    forward_rx: Receiver<ForwardRequest>,
    forward_ack_tx: Sender<ForwardAck>,
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
                    InternalNotification::TransactionEnded => {
                        debug!("Notification: transaction ended, waking read_committed waiters");
                        registry_for_notifier.notify_transaction_ended().await;
                    }
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
        super::shadow::forwarder_task::run_shadow_forwarder(
            forward_rx,
            forward_ack_tx,
            runtime_context_for_forwarder,
        )
        .await;
    });

    // SEC-3 / wave3 net: bound concurrent client connections to prevent fd/memory
    // exhaustion. Each listener has its own cap (pg_kafka.max_connections, see
    // accept_loop) and each IP address one across all listeners
    // (pg_kafka.max_connections_per_ip). Every listener carries the same limits.
    let max_per_ip = listeners
        .first()
        .map(|(_, s)| s.limits.max_connections_per_ip)
        .unwrap_or(usize::MAX);
    let accept_context = AcceptContext {
        request_lanes,
        registry,
        ip_tracker: IpTracker::new(max_per_ip),
        lookup: ChannelLookup(credential_tx),
        enable_long_polling,
        fetch_poll_interval_ms,
        log_connections,
    };

    // One accept loop per listener (the primary listener plus
    // pg_kafka.additional_listeners).
    let accept_tasks: Vec<_> = listeners
        .into_iter()
        .map(|(listener, security)| {
            info!(
                "pg_kafka listener {}://{} accepting connections",
                security.protocol.name(),
                security.bind_addr
            );
            tokio::spawn(accept_loop(listener, security, accept_context.clone()))
        })
        .collect();

    // Wait for the shutdown signal.
    loop {
        match shutdown_rx.try_recv() {
            Ok(()) => {
                info!("TCP listener received shutdown signal");
                break;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                info!("Shutdown channel disconnected, exiting");
                break;
            }
        }
    }

    // Cleanup: stop accepting and abort the notifier task
    for task in &accept_tasks {
        task.abort();
    }
    notify_handle.abort();
    let _ = notify_handle.await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_peer_disconnect_classification() {
        use std::io::{Error, ErrorKind};
        assert!(is_peer_disconnect(&Error::from(ErrorKind::ConnectionReset)));
        assert!(is_peer_disconnect(&Error::from(ErrorKind::BrokenPipe)));
        assert!(is_peer_disconnect(&Error::from(ErrorKind::UnexpectedEof)));
        assert!(!is_peer_disconnect(&Error::other(
            "aggregate request reservation budget exhausted"
        )));
        assert!(!is_peer_disconnect(&Error::from(ErrorKind::InvalidData)));
    }
}
