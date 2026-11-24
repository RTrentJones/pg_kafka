//! What one connection's responses may hold before they are on the socket.
//!
//! Kafka reads a connection's next request only once the previous response is sent, so a
//! connection holds at most one response. pg_kafka reads up to
//! `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` requests ahead (pipelining); this budget bounds the
//! response bytes those requests may hold together, whether or not the client reads them:
//!
//! * a Fetch reserves its `max_bytes` (at most the budget) before it runs: before the DB thread
//!   reads its records, or before each attempt of a long poll;
//! * the writer replaces a reservation by the encoded size of the response once it arrives,
//!   counts every other response the same way, and gives the bytes back once written;
//! * the reader reads no further request while the connection holds more than the budget.
//!
//! A reservation that does not fit waits, except for the oldest request whose response is not
//! written yet (the head): everything before it is on the socket, so it always proceeds, as
//! Kafka's one response does, and nothing waits on a response that cannot be written. What a
//! connection holds is therefore at most the budget plus the head's response (and the first
//! record of a partition that is larger than the request's `max_bytes`, which Kafka returns too).

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

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
    /// Told whenever bytes are given back or the head moves.
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

    /// Whether request `seq` is the head: every response of the requests before it is written.
    fn is_head(&self, seq: u64) -> bool {
        seq <= self.written.load(Ordering::Acquire)
    }

    /// Reserve `bytes` (at most the budget) for the response of request `seq`, if they fit or
    /// `seq` is the head.
    pub(super) fn try_reserve(self: &Arc<Self>, seq: u64, bytes: usize) -> Option<ResponseCharge> {
        let bytes = bytes.min(self.limit);
        let mut held = self.held.lock();
        if *held + bytes <= self.limit || self.is_head(seq) {
            *held += bytes;
            return Some(ResponseCharge {
                budget: Arc::clone(self),
                bytes,
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

    /// Wait until the connection holds no more than its budget.
    pub(super) async fn under_limit(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.over_limit() {
                return;
            }
            changed.await;
        }
    }

    /// Count `bytes` of a response that has arrived, whether or not they fit.
    fn hold(self: &Arc<Self>, bytes: usize) -> ResponseCharge {
        *self.held.lock() += bytes;
        ResponseCharge {
            budget: Arc::clone(self),
            bytes,
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
        match &mut state.charge {
            Some(charge) if reserved => charge.resize(bytes),
            Some(charge) => charge.resize(charge.bytes() + bytes),
            None => state.charge = Some(budget.hold(bytes)),
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
    pub(super) async fn reserve(&self, max_bytes: i32) -> ResponseCharge {
        self.budget
            .reserve(self.seq, usize::try_from(max_bytes).unwrap_or(0))
            .await
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
    async fn test_under_limit_waits_for_written_bytes() {
        let budget = ResponseBudget::new(100);
        let slot = SlotCharge::default();
        slot.arrived(&budget, 150);
        let wait = tokio::spawn({
            let budget = Arc::clone(&budget);
            async move { budget.under_limit().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait.is_finished());
        slot.written(100);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
    }
}
