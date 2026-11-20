//! How much memory the records of Produce requests may take once decoded.
//!
//! A frame's size says little about what its records take: a gzip or zstd batch of zeros
//! expands about a thousandfold, and every record, however small on the wire (7 bytes),
//! becomes a few hundred bytes of decoded structure. The memory of a decode is therefore
//! charged to a [`DecodeBudget`] before it is allocated, never checked afterwards: the
//! records and headers at a fixed cost each, the decoded bytes as the decompressors produce
//! them (they write through the budget and stop at its limit; see
//! `super::recordbatch`):
//!
//! * per request, at most [`MAX_DECODED_REQUEST_BYTES`] across all its partitions; beyond
//!   that the request is answered MESSAGE_TOO_LARGE;
//! * across the process, decoded requests that the DB thread has not run yet hold at most
//!   [`DECODED_BUDGET_BYTES`] ([`DECODED_REMAINING`]); a request that would exceed it closes
//!   its connection, as an exhausted frame budget does, and the client retries. The decoded
//!   request keeps its share ([`DecodedCharge`]) until it is dropped after its handler ran.
//!
//! What a decode holds at its peak (the decompressed section and the records' copies of
//! it) is at most twice its charge; once the section is dropped it holds no more than its
//! charge.
//!
//! Decoding is also kept off the two tokio I/O threads once it is large
//! ([`run_large_decode`]): at most [`LARGE_DECODE_SLOTS`] such decodes run at once, each on a
//! thread of its own, and at most [`LARGE_DECODE_WAITERS`] more wait for one; a request
//! beyond that closes its connection, as an exhausted decode budget does.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::cell::Cell;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex, PoisonError};

use super::framing::{charge_frame_budget, release_frame_budget};
use crate::kafka::constants::MAX_REQUEST_SIZE;

/// The most memory one Produce request's records may take decoded, summed over its
/// partitions: the size of the largest request frame, so a compressed request may decode to
/// no more than an uncompressed one of large records.
pub const MAX_DECODED_REQUEST_BYTES: usize = MAX_REQUEST_SIZE;

/// The most decoded record memory all requests waiting for the DB thread may hold together.
pub const DECODED_BUDGET_BYTES: usize = 512 * 1024 * 1024;

const _: () = assert!(MAX_DECODED_REQUEST_BYTES <= DECODED_BUDGET_BYTES);

/// Remaining process-wide decoded budget, in bytes.
pub static DECODED_REMAINING: AtomicUsize = AtomicUsize::new(DECODED_BUDGET_BYTES);

/// Decodes taking up to this much memory run inline on the I/O thread (the cost of moving
/// them elsewhere would exceed the work); a larger request decodes through
/// [`run_large_decode`].
pub const INLINE_DECODE_BYTES: usize = 1024 * 1024;

/// How many large decodes may run at once.
pub const LARGE_DECODE_SLOTS: usize = 2;

/// Which limit a decode ran into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exhausted {
    /// The budget's own limit (the request limit, or the inline limit).
    Limit,
    /// The process-wide budget.
    Global,
}

/// The decoded-memory allowance of one request while it decodes, in bytes. Memory is
/// charged before it is allocated; whatever is charged is given back when the budget is
/// dropped, unless it is handed to the decoded request with [`DecodeBudget::into_charge`].
#[derive(Debug)]
pub struct DecodeBudget {
    global: &'static AtomicUsize,
    limit: usize,
    charged: Cell<usize>,
    exhausted: Cell<Option<Exhausted>>,
}

impl DecodeBudget {
    /// A budget of `limit` bytes charging the process-wide budget.
    pub fn new(limit: usize) -> Self {
        Self::with_global(limit, &DECODED_REMAINING)
    }

    /// A budget of `limit` bytes charging `global` (tests use their own).
    pub fn with_global(limit: usize, global: &'static AtomicUsize) -> Self {
        Self {
            global,
            limit,
            charged: Cell::new(0),
            exhausted: Cell::new(None),
        }
    }

    /// Charge `n` more bytes. Fails, charging nothing, when that would pass the
    /// budget's limit or the process-wide budget; [`DecodeBudget::exhausted`] then says
    /// which.
    pub fn take(&self, n: usize) -> Result<(), Exhausted> {
        let total = self.charged.get().saturating_add(n);
        let result = if total > self.limit {
            Err(Exhausted::Limit)
        } else if !charge_frame_budget(self.global, n) {
            Err(Exhausted::Global)
        } else {
            self.charged.set(total);
            Ok(())
        };
        if let Err(which) = result {
            self.exhausted.set(Some(which));
        }
        result
    }

    /// The limit a [`DecodeBudget::take`] ran into, if one did.
    pub fn exhausted(&self) -> Option<Exhausted> {
        self.exhausted.get()
    }

    /// Give back `n` of the bytes charged (memory a decode no longer holds).
    pub fn release(&self, n: usize) {
        let n = n.min(self.charged.get());
        self.charged.set(self.charged.get() - n);
        release_frame_budget(self.global, n);
    }

    /// Bytes charged so far.
    pub fn charged(&self) -> usize {
        self.charged.get()
    }

    /// Hand what is charged to the decoded request, which gives it back when dropped.
    pub fn into_charge(self) -> DecodedCharge {
        DecodedCharge {
            global: self.global,
            bytes: self.charged.replace(0),
        }
    }
}

impl Drop for DecodeBudget {
    fn drop(&mut self) {
        release_frame_budget(self.global, self.charged.get());
    }
}

/// The decoded memory a request holds of the process-wide budget, given back when it is
/// dropped (after the DB thread ran the request, or when the request is discarded).
#[derive(Debug)]
pub struct DecodedCharge {
    global: &'static AtomicUsize,
    bytes: usize,
}

impl DecodedCharge {
    /// Bytes held.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Default for DecodedCharge {
    /// Holds nothing.
    fn default() -> Self {
        Self {
            global: &DECODED_REMAINING,
            bytes: 0,
        }
    }
}

impl Drop for DecodedCharge {
    fn drop(&mut self) {
        release_frame_budget(self.global, self.bytes);
    }
}

/// How many requests may wait for a large-decode slot. Each waits on a thread of its own
/// (a tokio blocking-pool thread), so the bound keeps hostile pipelining from taking that
/// pool's threads; a request beyond it is turned away at once ([`run_large_decode`]).
pub const LARGE_DECODE_WAITERS: usize = 8;

/// A counting semaphore for blocking threads with a bounded queue: `slots` decodes at
/// once, at most `waiters` more waiting.
pub(crate) struct Slots {
    state: Mutex<SlotState>,
    freed: Condvar,
    waiters: usize,
}

struct SlotState {
    free: usize,
    waiting: usize,
}

/// The large decodes of the network thread.
static SLOTS: Slots = Slots::new(LARGE_DECODE_SLOTS, LARGE_DECODE_WAITERS);

/// One of a [`Slots`], given back on drop.
struct Slot(&'static Slots);

impl Slots {
    pub(crate) const fn new(slots: usize, waiters: usize) -> Self {
        Self {
            state: Mutex::new(SlotState {
                free: slots,
                waiting: 0,
            }),
            freed: Condvar::new(),
            waiters,
        }
    }

    /// A slot, waiting for one if fewer than `waiters` already wait; `None` otherwise.
    fn acquire(&'static self) -> Option<Slot> {
        // The counters stay consistent even if a holder panicked, so a poisoned lock is
        // used as is.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.free == 0 {
            if state.waiting >= self.waiters {
                return None;
            }
            state.waiting += 1;
            while state.free == 0 {
                state = self
                    .freed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            state.waiting -= 1;
        }
        state.free -= 1;
        Some(Slot(self))
    }

    /// Run `decode` under one of these slots, or return `None` at once when every slot is
    /// taken and the queue is full. See [`run_large_decode`].
    pub(crate) fn run<R>(&'static self, decode: impl FnOnce() -> R) -> Option<R> {
        let run = || {
            let _slot = self.acquire()?;
            Some(decode())
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(run)
            }
            _ => run(),
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.free += 1;
        self.0.freed.notify_one();
    }
}

/// Run a large decode: under one of [`LARGE_DECODE_SLOTS`], and, on the multi-threaded
/// network runtime, through `block_in_place`, so the I/O thread's other connections move to
/// another thread while it runs (and while it waits for a slot). At most
/// [`LARGE_DECODE_WAITERS`] wait; with the queue full it returns `None` at once, and the
/// request is turned away like one that finds the decode budget exhausted. Elsewhere (unit
/// tests, a current-thread runtime) it runs in place.
pub fn run_large_decode<R>(decode: impl FnOnce() -> R) -> Option<R> {
    SLOTS.run(decode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn take_stops_at_the_limit_and_charges_nothing_past_it() {
        static GLOBAL: AtomicUsize = AtomicUsize::new(1000);
        let budget = DecodeBudget::with_global(100, &GLOBAL);
        assert_eq!(budget.take(60), Ok(()));
        assert_eq!(budget.take(41), Err(Exhausted::Limit));
        assert_eq!(budget.exhausted(), Some(Exhausted::Limit));
        assert_eq!(budget.charged(), 60);
        assert_eq!(GLOBAL.load(Ordering::Relaxed), 940);
        drop(budget);
        assert_eq!(
            GLOBAL.load(Ordering::Relaxed),
            1000,
            "dropped budget gives back"
        );
    }

    #[test]
    fn take_stops_at_the_global_budget() {
        static GLOBAL: AtomicUsize = AtomicUsize::new(50);
        let budget = DecodeBudget::with_global(100, &GLOBAL);
        assert_eq!(budget.take(40), Ok(()));
        assert_eq!(budget.take(20), Err(Exhausted::Global));
        assert_eq!(budget.exhausted(), Some(Exhausted::Global));
        assert_eq!(GLOBAL.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn the_charge_is_held_until_the_decoded_request_is_dropped() {
        static GLOBAL: AtomicUsize = AtomicUsize::new(1000);
        let budget = DecodeBudget::with_global(1000, &GLOBAL);
        budget.take(300).unwrap();
        let charge = budget.into_charge();
        assert_eq!(charge.bytes(), 300);
        assert_eq!(GLOBAL.load(Ordering::Relaxed), 700);
        drop(charge);
        assert_eq!(GLOBAL.load(Ordering::Relaxed), 1000);
        drop(DecodedCharge::default());
    }

    #[test]
    fn large_decodes_run_at_most_slots_at_a_time() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;
        static SLOTS: Slots = Slots::new(LARGE_DECODE_SLOTS, 8);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
                std::thread::spawn(move || {
                    SLOTS
                        .run(|| {
                            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_millis(20));
                            running.fetch_sub(1, Ordering::SeqCst);
                        })
                        .expect("eight callers fit two slots and six waiters");
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= LARGE_DECODE_SLOTS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn large_decodes_leave_the_io_threads_free() {
        static SLOTS: Slots = Slots::new(LARGE_DECODE_SLOTS, LARGE_DECODE_WAITERS);
        // Two decodes occupying both worker threads' cores must not stall a timer task.
        let ticker = tokio::spawn(async {
            let start = std::time::Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            start.elapsed()
        });
        let busy: Vec<_> = (0..2)
            .map(|_| {
                tokio::spawn(async {
                    SLOTS.run(|| std::thread::sleep(std::time::Duration::from_millis(400)));
                })
            })
            .collect();
        let waited = ticker.await.unwrap();
        for b in busy {
            b.await.unwrap();
        }
        assert!(
            waited < std::time::Duration::from_millis(300),
            "timer waited {waited:?}"
        );
    }

    #[test]
    fn runs_in_place_on_a_current_thread_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(rt.block_on(async { run_large_decode(|| 7) }), Some(7));
    }

    #[test]
    fn waiting_for_a_large_decode_slot_is_bounded() {
        // Every caller waiting for a slot holds a thread (a blocking-pool thread on the
        // network runtime), so at most `waiters` may wait; the rest are turned away at
        // once.
        use std::sync::Arc;
        use std::time::Duration;
        static SLOTS: Slots = Slots::new(2, 8);
        let inside = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..24)
            .map(|_| {
                let inside = Arc::clone(&inside);
                std::thread::spawn(move || {
                    inside.fetch_add(1, Ordering::SeqCst);
                    let ran = SLOTS.run(|| std::thread::sleep(Duration::from_millis(100)));
                    inside.fetch_sub(1, Ordering::SeqCst);
                    ran.is_some()
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        let held_at_once = inside.load(Ordering::SeqCst);
        let ran = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|&ran| ran)
            .count();
        assert!(
            held_at_once <= 2 + 8,
            "{held_at_once} callers held in the slots at once"
        );
        assert_eq!(
            ran, 10,
            "two ran at once and eight waited; the rest were turned away"
        );
        // With the queue drained, a caller gets a slot again.
        assert_eq!(SLOTS.run(|| 1), Some(1));
    }
}
