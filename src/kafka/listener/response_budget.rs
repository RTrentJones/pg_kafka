//! What one connection's responses may hold before they are on the socket.
//!
//! Kafka reads a connection's next request only once the previous response is sent, so a
//! connection holds at most one response. pg_kafka reads up to
//! `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` requests ahead (pipelining); this budget bounds the
//! response bytes those requests may hold together, whether or not the client reads them:
//!
//! * a Fetch reserves its `max_bytes` (at least one byte, at most the budget) before it runs:
//!   before the DB thread reads its records, or before each attempt of a long poll. Only a
//!   Fetch that reserves while the connection holds nothing else and waits for no other
//!   response (the usual consumer, one Fetch at a time) may return a first record larger than
//!   its limits, as Kafka returns the first record batch; any other Fetch returns at most its
//!   `max_bytes` of records;
//! * the size of any other response is known only once it arrives, so at most
//!   [`MAX_UNANSWERED`] such requests may wait for their response at once (Heartbeats, whose
//!   responses are a few bytes, do not count);
//! * the writer replaces a reservation by the encoded size of the response once it arrives,
//!   counts every other response the same way, and gives the bytes back once written;
//! * the reader reads no further request while the connection holds more than the budget or
//!   [`MAX_UNANSWERED`] requests wait for their response.
//!
//! A reservation that does not fit waits, except for the oldest request whose response is not
//! written yet (the head): everything before it is on the socket, so it always proceeds, as
//! Kafka's one response does, and nothing waits on a response that cannot be written. What a
//! connection holds is therefore at most the budget, plus the head's reservation, plus the
//! responses of the at most [`MAX_UNANSWERED`] requests other than Fetch read while it was
//! within the budget, plus one first record larger than its Fetch's limits. Every Fetch
//! response also carries its partitions' entries (some 40 bytes each) beside the records its
//! `max_bytes` counts, as in Kafka.

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// How many requests other than Fetch and Heartbeat may wait for their response at once per
/// connection (see the module docs): enough for a producer's usual five in-flight requests
/// and a few more, while a client that pipelines requests with large responses (Metadata of
/// every topic, say) and reads none holds a few of them, not one per in-flight request.
pub(super) const MAX_UNANSWERED: usize = 8;

/// One connection's response budget (see the module docs).
pub(super) struct ResponseBudget {
    /// The budget, in bytes.
    limit: usize,
    /// Bytes reserved or held by responses not yet written.
    held: Mutex<usize>,
    /// How many requests, in request order, have all their responses on the socket: the
    /// request with this sequence number is the head.
    written: AtomicU64,
    /// Encoded responses waiting for the socket (the idle rule: a connection whose responses
    /// wait unwritten is waiting for its client).
    ready: AtomicUsize,
    /// Requests whose response size is not known before it arrives (any but Fetch and
    /// Heartbeat) that wait for it.
    unanswered: AtomicUsize,
    /// Told whenever bytes are given back, the head moves or a response arrives.
    changed: Notify,
}

impl ResponseBudget {
    /// A budget of `limit` bytes (at least one).
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(ResponseBudget {
            limit: limit.max(1),
            held: Mutex::new(0),
            written: AtomicU64::new(0),
            ready: AtomicUsize::new(0),
            unanswered: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }

    /// Bytes reserved or held by responses not yet written.
    pub(super) fn held(&self) -> usize {
        *self.held.lock()
    }

    /// Whether the connection holds more than its budget.
    pub(super) fn over_limit(&self) -> bool {
        self.held() > self.limit
    }

    /// Whether the reader may read another request: the connection holds no more than its
    /// budget and fewer than [`MAX_UNANSWERED`] requests wait for their response.
    pub(super) fn may_read(&self) -> bool {
        !self.over_limit() && self.unanswered.load(Ordering::Acquire) < MAX_UNANSWERED
    }

    /// Whether request `seq` is the head: every response of the requests before it is written.
    fn is_head(&self, seq: u64) -> bool {
        seq <= self.written.load(Ordering::Acquire)
    }

    /// Reserve `bytes` (at most the budget) for the response of request `seq`, if they fit or
    /// `seq` is the head.
    /// A reservation is at least one byte, so a connection that holds nothing has no
    /// reservation out either.
    pub(super) fn try_reserve(self: &Arc<Self>, seq: u64, bytes: usize) -> Option<ResponseCharge> {
        let bytes = bytes.clamp(1, self.limit);
        let mut held = self.held.lock();
        if *held + bytes <= self.limit || self.is_head(seq) {
            let alone = *held == 0 && self.unanswered.load(Ordering::Acquire) == 0;
            *held += bytes;
            return Some(ResponseCharge {
                budget: Arc::clone(self),
                bytes,
                alone,
            });
        }
        None
    }

    /// [`Self::try_reserve`], waiting until the bytes fit or `seq` is the head.
    pub(super) async fn reserve(self: &Arc<Self>, seq: u64, bytes: usize) -> ResponseCharge {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(charge) = self.try_reserve(seq, bytes) {
                return charge;
            }
            changed.await;
        }
    }

    /// Wait until [`Self::may_read`].
    pub(super) async fn readable(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.may_read() {
                return;
            }
            changed.await;
        }
    }

    /// A request whose response size is unknown until it arrives now waits for it.
    fn awaiting(&self) {
        self.unanswered.fetch_add(1, Ordering::AcqRel);
    }

    /// Such a request's response arrived, or the request will not be answered.
    fn answered(&self) {
        self.unanswered.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }

    /// Count `bytes` of a response that has arrived, whether or not they fit.
    fn hold(self: &Arc<Self>, bytes: usize) -> ResponseCharge {
        *self.held.lock() += bytes;
        ResponseCharge {
            budget: Arc::clone(self),
            bytes,
            alone: false,
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        {
            let mut held = self.held.lock();
            *held = held.saturating_sub(bytes);
        }
        self.changed.notify_waiters();
    }

    /// Every response of the head request is written: the next request is the head.
    pub(super) fn head_written(&self) {
        self.written.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }

    /// Encoded responses waiting for the socket.
    pub(super) fn ready(&self) -> usize {
        self.ready.load(Ordering::Relaxed)
    }

    pub(super) fn add_ready(&self) {
        self.ready.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn remove_ready(&self) {
        self.ready.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Bytes of a [`ResponseBudget`] held for one request's response, given back when dropped.
pub(super) struct ResponseCharge {
    budget: Arc<ResponseBudget>,
    bytes: usize,
    /// A reservation made while the connection held nothing else and no request waited for a
    /// response of unknown size.
    alone: bool,
}

impl ResponseCharge {
    /// The bytes held.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Hold `bytes` instead: fewer gives the difference back, more is counted whether or not
    /// it fits (the response exists already).
    pub(super) fn resize(&mut self, bytes: usize) {
        if bytes < self.bytes {
            self.budget.release(self.bytes - bytes);
        } else {
            *self.budget.held.lock() += bytes - self.bytes;
        }
        self.bytes = bytes;
    }
}

impl Drop for ResponseCharge {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// What one request's responses hold of the budget, shared between the request's side (the
/// reader's reservation of an immediate Fetch, a long poll's reservation) and the writer, which
/// replaces a reservation by the encoded size once the response arrives.
#[derive(Clone, Default)]
pub(super) struct SlotCharge(Arc<Mutex<SlotChargeState>>);

#[derive(Default)]
struct SlotChargeState {
    charge: Option<ResponseCharge>,
    /// The charge is a reservation no response has replaced yet.
    reserved: bool,
    /// The budget counts the request as waiting for a response of unknown size.
    awaiting: Option<Arc<ResponseBudget>>,
}

impl SlotChargeState {
    /// The request no longer waits for a response of unknown size.
    fn answered(&mut self) {
        if let Some(budget) = self.awaiting.take() {
            budget.answered();
        }
    }
}

impl Drop for SlotChargeState {
    fn drop(&mut self) {
        // A request that is never answered (an acks=0 Produce, a closed connection) stops
        // counting once its slot is gone.
        self.answered();
    }
}

impl SlotCharge {
    /// Hold `charge`, a reservation, for the request's response.
    pub(super) fn reserve(&self, charge: ResponseCharge) {
        let mut state = self.0.lock();
        state.charge = Some(charge);
        state.reserved = true;
    }

    /// A response of `bytes` encoded bytes arrived: the first replaces the reservation, later
    /// ones add to it.
    pub(super) fn arrived(&self, budget: &Arc<ResponseBudget>, bytes: usize) {
        let mut state = self.0.lock();
        let reserved = std::mem::take(&mut state.reserved);
        let was_awaiting = state.awaiting.is_some();
        match &mut state.charge {
            Some(charge) if reserved => charge.resize(bytes),
            Some(charge) => charge.resize(charge.bytes() + bytes),
            None => state.charge = Some(budget.hold(bytes)),
        }
        // Held before the request stops counting as unanswered, so the reader never sees the
        // response counted by neither.
        if was_awaiting {
            state.answered();
        }
    }

    /// A response of `bytes` encoded bytes is on the socket.
    pub(super) fn written(&self, bytes: usize) {
        if let Some(charge) = &mut self.0.lock().charge {
            charge.resize(charge.bytes().saturating_sub(bytes));
        }
    }
}

/// What one request's response may hold of its connection's budget: the budget, the
/// request's place in request order and its slot's charge.
pub(super) struct ReplyCharge {
    budget: Arc<ResponseBudget>,
    seq: u64,
    slot: SlotCharge,
}

impl ReplyCharge {
    /// The reply of request `seq` (its slot's charge starts empty).
    pub(super) fn new(budget: Arc<ResponseBudget>, seq: u64) -> Self {
        ReplyCharge {
            budget,
            seq,
            slot: SlotCharge::default(),
        }
    }

    /// The slot's charge, for the writer.
    pub(super) fn slot(&self) -> SlotCharge {
        self.slot.clone()
    }

    /// Reserve `max_bytes` for a Fetch response, waiting for room (see [`ResponseBudget`]).
    /// Also returns whether the connection held nothing else when the bytes were reserved
    /// and no request waited for a response of unknown size: only such a Fetch may return a
    /// first record larger than its limits, so that record is all the connection holds
    /// beyond the budget for it.
    pub(super) async fn reserve(&self, max_bytes: i32) -> (ResponseCharge, bool) {
        let charge = self
            .budget
            .reserve(self.seq, usize::try_from(max_bytes).unwrap_or(0))
            .await;
        let alone = charge.alone;
        (charge, alone)
    }

    /// Count the request as waiting for a response whose size is unknown until it arrives
    /// (anything but a Fetch or a Heartbeat), until it arrives or the slot is gone.
    pub(super) fn awaits_response(&self) {
        let mut state = self.slot.0.lock();
        if state.awaiting.is_none() {
            self.budget.awaiting();
            state.awaiting = Some(Arc::clone(&self.budget));
        }
    }

    /// Hand a reservation to the slot just before the response is sent: the writer replaces
    /// it by the response's encoded size.
    pub(super) fn hold(&self, charge: ResponseCharge) {
        self.slot.reserve(charge);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_reservations_fit_the_budget_or_wait_unless_head() {
        let budget = ResponseBudget::new(100);
        // Request 0 is the head: it always gets its bytes, at most the budget.
        let head = budget
            .try_reserve(0, 1000)
            .expect("the head always proceeds");
        assert_eq!(head.bytes(), 100);
        // Request 1 does not fit while the head holds the budget.
        assert!(budget.try_reserve(1, 1).is_none());
        drop(head);
        let second = budget.try_reserve(1, 60).expect("fits once given back");
        assert!(budget.try_reserve(2, 60).is_none(), "60 + 60 > 100");
        let third = budget.try_reserve(2, 40).expect("60 + 40 fits");
        assert_eq!(budget.held(), 100);
        drop((second, third));
        assert_eq!(budget.held(), 0);
    }

    #[test]
    fn test_arrival_replaces_a_reservation_and_writing_gives_it_back() {
        let budget = ResponseBudget::new(1000);
        let slot = SlotCharge::default();
        slot.reserve(budget.try_reserve(0, 800).unwrap());
        assert_eq!(budget.held(), 800);
        // The response is smaller than its max_bytes: the rest is given back at once.
        slot.arrived(&budget, 50);
        assert_eq!(budget.held(), 50);
        // A second response of the same request adds to it.
        slot.arrived(&budget, 20);
        assert_eq!(budget.held(), 70);
        slot.written(50);
        assert_eq!(budget.held(), 20);
        // A response nobody reserved for (not a Fetch) is counted as it arrives, even past
        // the budget.
        let other = SlotCharge::default();
        other.arrived(&budget, 5000);
        assert_eq!(budget.held(), 5020);
        assert!(budget.over_limit());
        drop(other);
        drop(slot);
        assert_eq!(budget.held(), 0);
        assert!(!budget.over_limit());
    }

    #[tokio::test]
    async fn test_a_waiting_reservation_proceeds_once_it_is_the_head() {
        let budget = ResponseBudget::new(100);
        let first = SlotCharge::default();
        first.arrived(&budget, 100);
        let waiting = tokio::spawn({
            let budget = Arc::clone(&budget);
            async move { budget.reserve(1, 100).await.bytes() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "request 1 waits for room");
        // Request 0's response is written: request 1 is the head (and room came free).
        first.written(100);
        budget.head_written();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap(),
            100
        );

        // The head proceeds even when the budget is held by later responses it precedes.
        let budget = ResponseBudget::new(100);
        let later = SlotCharge::default();
        later.arrived(&budget, 100);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), budget.reserve(0, 100))
                .await
                .expect("the head never waits")
                .bytes(),
            100
        );
    }

    #[tokio::test]
    async fn test_readable_waits_for_written_bytes() {
        let budget = ResponseBudget::new(100);
        let slot = SlotCharge::default();
        slot.arrived(&budget, 150);
        let wait = tokio::spawn({
            let budget = Arc::clone(&budget);
            async move { budget.readable().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait.is_finished());
        slot.written(100);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
    }

    /// Requests whose response size is unknown stop the reader at `MAX_UNANSWERED`, however
    /// little the connection holds, until one is answered: a response arriving or a slot
    /// dropped unanswered (an acks=0 Produce) gives the place back, and a response counts in
    /// what is held before its request stops counting as unanswered.
    #[tokio::test]
    async fn test_unanswered_requests_stop_the_reader() {
        let budget = ResponseBudget::new(1 << 20);
        let replies: Vec<ReplyCharge> = (0..u64::try_from(MAX_UNANSWERED).unwrap())
            .map(|seq| ReplyCharge::new(Arc::clone(&budget), seq))
            .collect();
        for (i, reply) in replies.iter().enumerate() {
            assert!(budget.may_read(), "{i} unanswered");
            reply.awaits_response();
            // Marking twice counts once.
            reply.awaits_response();
        }
        assert!(!budget.may_read());
        assert_eq!(budget.held(), 0, "nothing is held: only the count stops it");
        let wait = tokio::spawn({
            let budget = Arc::clone(&budget);
            async move { budget.readable().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait.is_finished());
        // The first response arrives: counted, and its request answered.
        let slot = replies[0].slot();
        slot.arrived(&budget, 3000);
        assert_eq!(budget.held(), 3000);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
        // A later response of the same request changes nothing in the count.
        slot.arrived(&budget, 10);
        assert!(budget.may_read());
        replies[1].awaits_response();
        replies[0].awaits_response();
        assert!(!budget.may_read(), "marked again, it counts again");
        // Slots dropped without a response give their places back.
        drop(slot);
        let mut replies = replies.into_iter();
        drop(replies.next());
        assert!(budget.may_read());
        drop(replies.collect::<Vec<_>>());
        assert_eq!(budget.unanswered.load(Ordering::Acquire), 0);
    }

    /// A reservation reports whether the connection held nothing else and no request
    /// waited for a response of unknown size; a reservation of no bytes counts one.
    #[tokio::test]
    async fn test_a_reservation_reports_whether_it_is_alone() {
        let budget = ResponseBudget::new(100);
        let (charge, alone) = ReplyCharge::new(Arc::clone(&budget), 0).reserve(0).await;
        assert!(alone);
        assert_eq!(charge.bytes(), 1);
        let (later, alone) = ReplyCharge::new(Arc::clone(&budget), 1).reserve(10).await;
        assert!(!alone, "the first reservation is out");
        drop((charge, later));
        let waiting = ReplyCharge::new(Arc::clone(&budget), 2);
        waiting.awaits_response();
        let (_charge, alone) = ReplyCharge::new(Arc::clone(&budget), 3).reserve(10).await;
        assert!(!alone, "a request waits for its response");
    }
}
