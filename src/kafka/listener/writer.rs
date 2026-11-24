//! The task that writes one connection's responses, in request order.
//!
//! The reader creates a fresh response channel per request and queues its receiving
//! end ([`ResponseSlot`]), in request order, to the writer. The writer drains one
//! request's channel (closed when the request's sender is dropped) before moving to the
//! next, so responses hit the socket in request order even when a long-poll fetch
//! resolves in a detached task long after later requests completed. Kafka requires this
//! (the Java client kills the connection on a correlation-id mismatch). A request that
//! yields no response (acks=0 produce) closes its channel without sending and is
//! skipped. A long-poll fetch therefore holds back later responses on the same
//! connection until it resolves, as a Kafka broker mutes a connection while a request
//! waits in purgatory.

use bytes::Bytes;
use futures::sink::{Sink, SinkExt};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, OwnedSemaphorePermit};
use tracing::{debug, error, warn};

use super::connection::ActivityClock;
use super::is_peer_disconnect;
use crate::kafka::api::ResponseBody;
use crate::kafka::messages::KafkaResponse;
use crate::kafka::protocol;
use crate::kafka::protocol::DecodedCharge;

/// The responses of one request, queued to the writer in request order.
pub(super) struct ResponseSlot {
    responses: UnboundedReceiver<KafkaResponse>,
    /// Dropped only once the slot's response is on the socket, so the reader reads at
    /// most `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` requests ahead of the socket.
    _in_flight: OwnedSemaphorePermit,
    /// The decoded request's share of the decoded-request budget, held until its response
    /// is on the socket (the request, then its response, hold the memory until then).
    _decoded: DecodedCharge,
}

impl ResponseSlot {
    /// A fresh response channel for one request: the sender for the request, and the
    /// receiver [`ResponseSlot::new`] makes the writer's slot of.
    pub(super) fn channel() -> (
        UnboundedSender<KafkaResponse>,
        UnboundedReceiver<KafkaResponse>,
    ) {
        tokio::sync::mpsc::unbounded_channel()
    }

    /// The writer's slot of a request's response channel.
    pub(super) fn new(
        responses: UnboundedReceiver<KafkaResponse>,
        in_flight: OwnedSemaphorePermit,
        decoded: DecodedCharge,
    ) -> Self {
        ResponseSlot {
            responses,
            _in_flight: in_flight,
            _decoded: decoded,
        }
    }
}

/// What the writer task needs besides the socket.
pub(super) struct WriterContext {
    /// The response slots, in request order.
    pub(super) slots: UnboundedReceiver<ResponseSlot>,
    /// The listener's advertised address, written into Metadata / FindCoordinator.
    pub(super) advertised: Option<(String, i32)>,
    /// Touched after every response written (idle timeout).
    pub(super) activity: Arc<ActivityClock>,
    /// Peer address (for logs).
    pub(super) peer: String,
    /// Dropped when the writer exits, however it exits (an encode or write error, a
    /// panic, an acks=0 close request, all slots done), so the reader closes the
    /// connection instead of reading requests whose responses can never be written.
    pub(super) gone: oneshot::Sender<()>,
}

/// Spawn the writer task over the socket's write half.
pub(super) fn spawn<W>(sink: W, ctx: WriterContext) -> tokio::task::JoinHandle<()>
where
    W: Sink<Bytes, Error = std::io::Error> + Unpin + Send + 'static,
{
    tokio::spawn(write_responses(sink, ctx))
}

async fn write_responses<W>(mut sink: W, ctx: WriterContext)
where
    W: Sink<Bytes, Error = std::io::Error> + Unpin,
{
    let WriterContext {
        mut slots,
        advertised,
        activity,
        peer,
        gone: _gone,
    } = ctx;
    while let Some(mut slot) = slots.recv().await {
        while let Some(mut response) = slot.responses.recv().await {
            if matches!(response.body, ResponseBody::CloseConnection) {
                // An acks=0 produce hit a partition error: Kafka closes the connection
                // (after the request's writes committed) so the client refreshes its
                // metadata. Earlier responses are already on the socket.
                debug!(
                    "Closing connection from {} after an acks=0 produce error",
                    peer
                );
                let _ = sink.close().await;
                return;
            }
            if let Some((host, port)) = &advertised {
                rewrite_advertised(&mut response, host, *port);
            }
            let bytes = match protocol::encode_response(response) {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!("Failed to encode response: {e}");
                    return;
                }
            };
            debug!("Sending {} byte response to client...", bytes.len());
            if let Err(e) = sink.send(bytes.freeze()).await {
                if is_peer_disconnect(&e) {
                    debug!("Client went away while sending response: {e}");
                } else {
                    warn!("Failed to send response to client: {e}");
                }
                return;
            }
            activity.touch();
        }
        // `slot` drops here: its response (if any) is on the socket, so the reader may
        // read one more frame.
    }
}

/// Point Metadata brokers and FindCoordinator coordinators at this listener's
/// advertised address (single-node broker: every broker entry is this node), so a
/// client that bootstrapped on an additional listener stays on it (Kafka's
/// per-listener `advertised.listeners`).
fn rewrite_advertised(response: &mut KafkaResponse, host: &str, port: i32) {
    use kafka_protocol::protocol::StrBytes;
    match &mut response.body {
        ResponseBody::Metadata(response) => {
            for broker in &mut response.brokers {
                broker.host = StrBytes::from_string(host.to_string());
                broker.port = port;
            }
        }
        ResponseBody::FindCoordinator(response) => {
            if response.error_code == 0 && !response.host.is_empty() {
                response.host = StrBytes::from_string(host.to_string());
                response.port = port;
            }
            for c in &mut response.coordinators {
                if c.error_code == 0 && !c.host.is_empty() {
                    c.host = StrBytes::from_string(host.to_string());
                    c.port = port;
                }
            }
        }
        _ => {}
    }
}
