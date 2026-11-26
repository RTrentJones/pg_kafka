//! The Kafka network edge (network thread): it accepts client connections on every
//! listener, reads and answers Kafka frames, and hands parsed requests to the DB thread.
//!
//! It runs on its own thread inside a multi-threaded tokio runtime and MUST NOT call any
//! pgrx function (logging, SPI, ...); it logs through `tracing`. Requests cross to the
//! background worker over bounded crossbeam channels; responses come back on per-request
//! tokio channels.
//!
//! * `thread`: logging and the signal mask of the network thread.
//! * `accept`: the accept loops and connection admission (connection caps).
//! * `tls`: the TLS handshake and the client-certificate principal.
//! * `connection`: one connection's read loop and how it closes.
//! * `writer`: the task that writes a connection's responses in request order.
//! * `response_budget`: what a connection's unwritten responses may hold.
//! * `sasl`: per-connection authentication state and the SASL frames.
//! * `dispatch`: handing requests to the DB thread.
//! * `long_poll`: Fetch long polling.

mod accept;
mod connection;
mod dispatch;
mod long_poll;
mod response_budget;
mod sasl;
mod thread;
mod tls;
mod writer;

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
use dispatch::Dispatcher;

/// Whether an I/O error is just the peer going away (reset / broken pipe /
/// aborted / EOF mid-frame). Those are routine for a broker and logged at
/// debug; anything else (oversized frame, exhausted budget, ...) is a warning.
fn is_peer_disconnect(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        e.kind(),
        ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
            | ErrorKind::NotConnected
    )
}

/// How long a blocking receive from the notification channel waits before the
/// notifier task checks again (so it sees an aborted runtime promptly).
const NOTIFY_RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// How often [`run`] checks for the worker's shutdown signal.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The network thread's ends of the channels it shares with the DB thread.
pub struct NetworkChannels {
    /// Where parsed requests go (main and liveness lanes).
    pub request_lanes: crate::kafka::RequestLanes,
    /// SASL credential lookups, answered by the DB thread.
    pub credential_tx: Sender<CredentialRequest>,
    /// The DB thread's long-poll wake-ups.
    pub notify_rx: Receiver<InternalNotification>,
    /// Shadow-mode records to forward to the external Kafka.
    pub forward_rx: Receiver<ForwardRequest>,
    /// One ack per forward attempt, back to the DB thread.
    pub forward_ack_tx: Sender<ForwardAck>,
}

/// Run the Kafka listeners until the worker signals shutdown.
///
/// `listeners` are the pre-bound sockets (the primary listener first) with their
/// security. Besides one accept loop per listener, this spawns the task that turns the
/// DB thread's [`InternalNotification`]s into long-poll wake-ups and the shadow-mode
/// forwarder (always spawned: it reads the shadow configuration on the fly, so shadow
/// mode can be enabled with `pg_reload_conf()`).
///
/// Runs on the network thread and MUST NOT call pgrx functions. Errors of individual
/// connections are logged and end only that connection.
pub async fn run(
    listeners: Vec<(TcpListener, Arc<ListenerSecurity>)>,
    channels: NetworkChannels,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) {
    let NetworkChannels {
        request_lanes,
        credential_tx,
        notify_rx,
        forward_rx,
        forward_ack_tx,
    } = channels;
    let config = runtime_context.config();
    info!(
        "pg_kafka TCP listener started (long_polling={}, log_connections={})",
        config.enable_long_polling, config.log_connections
    );

    let registry = Arc::new(PendingFetchRegistry::new());
    let notifier = spawn_notifier(notify_rx, Arc::clone(&registry));

    // Detached: the forwarder ends when the DB thread drops its end of `forward_rx`.
    tokio::spawn(super::shadow::forwarder_task::run_shadow_forwarder(
        forward_rx,
        forward_ack_tx,
        Arc::clone(&runtime_context),
    ));

    // Each listener has its own connection cap (see `accept_loop`); the per-IP cap is
    // shared by all of them. Every listener carries the same limits.
    let max_per_ip = listeners
        .first()
        .map_or(usize::MAX, |(_, s)| s.limits.max_connections_per_ip);
    let accept_context = AcceptContext {
        dispatcher: Dispatcher {
            request_lanes,
            registry,
            long_poll_interval: config
                .enable_long_polling
                .then(|| long_poll::poll_interval(config.fetch_poll_interval_ms)),
            runtime_context: Arc::clone(&runtime_context),
        },
        ip_tracker: IpTracker::new(max_per_ip),
        lookup: ChannelLookup(credential_tx),
        log_connections: config.log_connections,
    };

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

    wait_for_shutdown(&shutdown_rx).await;

    for task in &accept_tasks {
        task.abort();
    }
    notifier.abort();
    let _ = notifier.await;
}

/// Wait until the worker sends the shutdown signal or drops its sender.
async fn wait_for_shutdown(shutdown_rx: &std::sync::mpsc::Receiver<()>) {
    loop {
        match shutdown_rx.try_recv() {
            Ok(()) => {
                info!("TCP listener received shutdown signal");
                return;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                info!("Shutdown channel disconnected, exiting");
                return;
            }
        }
    }
}

/// Spawn the task that receives the DB thread's notifications and wakes the long
/// polls waiting on them. The crossbeam receive blocks, so it runs on the blocking pool.
fn spawn_notifier(
    notify_rx: Receiver<InternalNotification>,
    registry: Arc<PendingFetchRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let rx = notify_rx.clone();
            let received =
                tokio::task::spawn_blocking(move || rx.recv_timeout(NOTIFY_RECV_TIMEOUT)).await;
            match received {
                Ok(Ok(notification)) => wake_waiters(&registry, notification).await,
                Ok(Err(crossbeam_channel::RecvTimeoutError::Timeout)) => {}
                Ok(Err(crossbeam_channel::RecvTimeoutError::Disconnected)) => {
                    info!("Notification channel disconnected, stopping notifier task");
                    return;
                }
                Err(_) => {
                    error!("Notification receiver task panicked");
                    return;
                }
            }
            // A notification dropped on the full channel named partitions no long poll
            // learns of: wake them all coarsely (at most once per receive, so at most every
            // NOTIFY_RECV_TIMEOUT while the channel keeps overflowing).
            if crate::kafka::notifications::take_dropped() {
                debug!("Notifications were dropped; waking every long poll");
                registry.notify_coarse().await;
            }
        }
    })
}

/// Wake the long polls one notification concerns.
async fn wake_waiters(registry: &PendingFetchRegistry, notification: InternalNotification) {
    match notification {
        InternalNotification::TransactionEnded => {
            debug!("Notification: transaction ended, waking read_committed waiters");
            registry.notify_transaction_ended().await;
        }
        InternalNotification::NewMessages {
            topic_name,
            topic_id,
            partition_id,
            high_watermark,
        } => {
            debug!(
                "Notification: new messages for topic={topic_name} (id={topic_id}), \
                 partition_id={partition_id}, hwm={high_watermark}"
            );
            registry
                .notify_new_data(&topic_name, partition_id, high_watermark)
                .await;
        }
        InternalNotification::LogStartMoved {
            topic_name,
            partition_id,
        } => {
            registry
                .notify_partition_changed(&topic_name, partition_id)
                .await;
        }
        InternalNotification::TopicDeleted { topic_name } => {
            registry.notify_topic_changed(&topic_name).await;
        }
        InternalNotification::RecordsExpired => registry.notify_coarse().await,
    }
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
