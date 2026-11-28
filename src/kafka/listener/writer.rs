//! The task that writes one connection's responses, in request order.
//!
//! The reader creates a fresh response channel per request and queues its receiving
//! end ([`ResponseSlot`]), in request order, to the writer. The writer writes one
//! request's responses (its channel is closed when the request's sender is dropped) before
//! moving to the next, so responses hit the socket in request order even when a long-poll
//! fetch resolves in a detached task long after later requests completed. Kafka requires this
//! (the Java client kills the connection on a correlation-id mismatch). A request that
//! yields no response (acks=0 produce) closes its channel without sending and is
//! skipped. A long-poll fetch therefore holds back later responses on the same
//! connection until it resolves, as a Kafka broker mutes a connection while a request
//! waits in purgatory.
//!
//! The writer takes every response as soon as it arrives, whichever request it answers, and
//! encodes it: what it holds counts against the connection's [`ResponseBudget`] at its encoded
//! size until it is written, so the reader stops reading while responses the client has not
//! read hold more than the budget.

use bytes::Bytes;
use futures::sink::{Sink, SinkExt};
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, OwnedSemaphorePermit};
use tracing::{debug, error, warn};

use super::connection::ActivityClock;
use super::is_peer_disconnect;
use super::response_budget::{ResponseBudget, SlotCharge};
use crate::kafka::api::ResponseBody;
use crate::kafka::messages::KafkaResponse;
use crate::kafka::protocol;

/// The responses of one request, queued to the writer in request order.
pub(super) struct ResponseSlot {
    responses: UnboundedReceiver<KafkaResponse>,
    /// What the request's responses hold of the connection's [`ResponseBudget`]: a Fetch's
    /// reservation until its response arrives, then the encoded responses until written.
    charge: SlotCharge,
    /// Dropped only once the slot's responses are on the socket, so the reader reads at
    /// most `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` requests ahead of the socket. The
    /// responses of those requests hold at most the connection's response budget (plus the
    /// head's response, see [`ResponseBudget`]), never the decoded requests they answer
    /// (`KafkaRequest::decode_charge` is given back once the request ran), and only until
    /// `pg_kafka.connections_max_idle_ms` closes a connection whose client does not read
    /// them (`ReadLoop::idle_wake`).
    _in_flight: OwnedSemaphorePermit,
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
        charge: SlotCharge,
        in_flight: OwnedSemaphorePermit,
    ) -> Self {
        ResponseSlot {
            responses,
            charge,
            _in_flight: in_flight,
        }
    }
}

/// What the writer task needs besides the socket.
pub(super) struct WriterContext {
    /// The response slots, in request order.
    pub(super) slots: UnboundedReceiver<ResponseSlot>,
    /// The listener's advertised address, written into Metadata / FindCoordinator.
    pub(super) advertised: Option<(String, i32)>,
    /// Told when a response starts and stops going out (the bytes written touch it through
    /// the socket: idle timeout).
    pub(super) activity: Arc<ActivityClock>,
    /// What the connection's unwritten responses hold.
    pub(super) budget: Arc<ResponseBudget>,
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

/// What the writer has for the socket.
enum Outgoing {
    /// An encoded response.
    Frame(Bytes),
    /// An acks=0 produce hit a partition error: Kafka closes the connection (after the
    /// request's writes committed) so the client refreshes its metadata.
    Close,
}

/// One request's slot and the responses that arrived on it.
struct Queued {
    slot: ResponseSlot,
    /// The request's sender is not dropped yet: more responses may come.
    open: bool,
    ready: VecDeque<Outgoing>,
}

/// The slots the writer has taken, in request order, and the responses that arrived on them.
struct Queue {
    slots: UnboundedReceiver<ResponseSlot>,
    /// The reader may queue more slots.
    accepting: bool,
    queued: VecDeque<Queued>,
    advertised: Option<(String, i32)>,
    budget: Arc<ResponseBudget>,
}

/// A response could not be encoded: the writer exits.
struct EncodeFailed;

impl Queue {
    /// Take every slot the reader queued and every response that arrived on any slot,
    /// encoding and counting each. Ready once anything changed, pending otherwise.
    fn poll_arrivals(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), EncodeFailed>> {
        let mut changed = false;
        while self.accepting {
            match self.slots.poll_recv(cx) {
                Poll::Ready(Some(slot)) => {
                    self.queued.push_back(Queued {
                        slot,
                        open: true,
                        ready: VecDeque::new(),
                    });
                    changed = true;
                }
                Poll::Ready(None) => {
                    self.accepting = false;
                    changed = true;
                }
                Poll::Pending => break,
            }
        }
        for queued in &mut self.queued {
            while queued.open {
                match queued.slot.responses.poll_recv(cx) {
                    Poll::Ready(Some(response)) => {
                        changed = true;
                        if matches!(response.body, ResponseBody::CloseConnection) {
                            queued.ready.push_back(Outgoing::Close);
                            queued.open = false;
                            break;
                        }
                        let bytes = encode(response, self.advertised.as_ref())?;
                        queued.slot.charge.arrived(&self.budget, bytes.len());
                        self.budget.add_ready();
                        queued.ready.push_back(Outgoing::Frame(bytes));
                    }
                    Poll::Ready(None) => {
                        queued.open = false;
                        changed = true;
                    }
                    Poll::Pending => break,
                }
            }
        }
        if changed {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

/// Encode a response, pointed at the listener's advertised address.
fn encode(
    mut response: KafkaResponse,
    advertised: Option<&(String, i32)>,
) -> Result<Bytes, EncodeFailed> {
    if let Some((host, port)) = advertised {
        rewrite_advertised(&mut response, host, *port);
    }
    match protocol::encode_response(response) {
        Ok(bytes) => Ok(bytes.freeze()),
        Err(e) => {
            error!("Failed to encode response: {e}");
            Err(EncodeFailed)
        }
    }
}

async fn write_responses<W>(mut sink: W, ctx: WriterContext)
where
    W: Sink<Bytes, Error = std::io::Error> + Unpin,
{
    let WriterContext {
        slots,
        advertised,
        activity,
        budget,
        peer,
        gone: _gone,
    } = ctx;
    let mut queue = Queue {
        slots,
        accepting: true,
        queued: VecDeque::new(),
        advertised,
        budget: Arc::clone(&budget),
    };
    loop {
        let next = match queue.queued.front_mut() {
            Some(head) => match head.ready.pop_front() {
                Some(out) => Some(out),
                None if !head.open => {
                    // Every response of the head request is on the socket: the reader may
                    // read one more frame.
                    queue.queued.pop_front();
                    budget.head_written();
                    continue;
                }
                None => None,
            },
            None if !queue.accepting => return,
            None => None,
        };
        let Some(out) = next else {
            if std::future::poll_fn(|cx| queue.poll_arrivals(cx))
                .await
                .is_err()
            {
                return;
            }
            continue;
        };
        let bytes = match out {
            Outgoing::Close => {
                // Earlier responses are already on the socket.
                debug!(
                    "Closing connection from {} after an acks=0 produce error",
                    peer
                );
                let _ = sink.close().await;
                return;
            }
            Outgoing::Frame(bytes) => bytes,
        };
        budget.remove_ready();
        let len = bytes.len();
        debug!("Sending {len} byte response to client...");
        // While the response goes out the connection waits for its client: if the client
        // stops reading, nothing more is written and the idle timeout closes it. Responses
        // that arrive meanwhile are taken and counted.
        activity.start_sending();
        let sent = {
            let send = sink.send(bytes);
            tokio::pin!(send);
            loop {
                tokio::select! {
                    biased;
                    sent = &mut send => break sent,
                    arrived = std::future::poll_fn(|cx| queue.poll_arrivals(cx)) => {
                        if arrived.is_err() {
                            return;
                        }
                    }
                }
            }
        };
        activity.stop_sending();
        if let Some(head) = queue.queued.front() {
            head.slot.charge.written(len);
        }
        if let Err(e) = sent {
            if is_peer_disconnect(&e) {
                debug!("Client went away while sending response: {e}");
            } else {
                warn!("Failed to send response to client: {e}");
            }
            return;
        }
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
