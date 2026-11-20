//! How much memory the records of Produce requests may take once decoded.
//!
//! A frame's size says little about its records: a gzip or zstd batch of zeros expands about
//! a thousandfold, so an 800 KB request can decode to gigabytes. The decoded bytes are
//! therefore bounded while they are produced (every decompressor writes through a
//! [`DecodeBudget`] and stops at the limit), never checked afterwards:
//!
//! * per request, at most [`MAX_DECODED_REQUEST_BYTES`] across all its partitions (the
//!   largest request an uncompressed client may send); beyond that the request is answered
//!   MESSAGE_TOO_LARGE;
//! * across the process, decoded requests that the DB thread has not run yet hold at most
//!   [`DECODED_BUDGET_BYTES`] ([`DECODED_REMAINING`]); a request that would exceed it closes
//!   its connection, as an exhausted frame budget does, and the client retries. The decoded
//!   request keeps its share ([`DecodedCharge`]) until it is dropped after its handler ran.
//!
//! Decoding is also kept off the two tokio I/O threads once it is large
//! ([`run_large_decode`]): at most [`LARGE_DECODE_SLOTS`] such decodes run at once, each on a
//! thread of its own, so they bound the transient copies too (a decompressed buffer and the
//! records copied out of it, at most twice the request limit per decode).
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::cell::Cell;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex, PoisonError};

use super::framing::{charge_frame_budget, release_frame_budget};
use crate::kafka::constants::MAX_REQUEST_SIZE;

/// The most bytes one Produce request's records may decode to, summed over its partitions.
/// A compressed request may decode to no more than an uncompressed one may be.
pub const MAX_DECODED_REQUEST_BYTES: usize = MAX_REQUEST_SIZE;

/// The most decoded record bytes all requests waiting for the DB thread may hold together.
pub const DECODED_BUDGET_BYTES: usize = 512 * 1024 * 1024;

const _: () = assert!(MAX_DECODED_REQUEST_BYTES <= DECODED_BUDGET_BYTES);

/// Remaining process-wide decoded budget, in bytes.
pub static DECODED_REMAINING: AtomicUsize = AtomicUsize::new(DECODED_BUDGET_BYTES);

/// Decodes up to this many bytes run inline on the I/O thread (the cost of moving them
/// elsewhere would exceed the work); a larger request decodes through [`run_large_decode`].
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

/// The decoded-bytes allowance of one request while it decodes. Bytes are charged as they
/// are produced; whatever is charged is given back when the budget is dropped, unless it
/// is handed to the decoded request with [`DecodeBudget::into_charge`].
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

    /// Charge `n` more decoded bytes. Fails, charging nothing, when that would pass the
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

/// The decoded bytes a request holds of the process-wide budget, given back when it is
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

/// A counting semaphore for blocking threads: [`LARGE_DECODE_SLOTS`] large decodes at once.
struct Slots {
    free: Mutex<usize>,
    freed: Condvar,
}

static SLOTS: Slots = Slots {
    free: Mutex::new(LARGE_DECODE_SLOTS),
    freed: Condvar::new(),
};

/// One of [`SLOTS`], given back on drop.
struct Slot(&'static Slots);

impl Slots {
    fn acquire(&'static self) -> Slot {
        // The counter stays consistent even if a holder panicked, so a poisoned lock is used
        // as is.
        let mut free = self.free.lock().unwrap_or_else(PoisonError::into_inner);
        while *free == 0 {
            free = self
                .freed
                .wait(free)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *free -= 1;
        Slot(self)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut free = self.0.free.lock().unwrap_or_else(PoisonError::into_inner);
        *free += 1;
        self.0.freed.notify_one();
    }
}

/// Run a large decode: under one of [`LARGE_DECODE_SLOTS`], and, on the multi-threaded
/// network runtime, through `block_in_place`, so the I/O thread's other connections move to
/// another thread while it runs (and while it waits for a slot). Elsewhere (unit tests, a
/// current-thread runtime) it runs in place.
pub fn run_large_decode<R>(decode: impl FnOnce() -> R) -> R {
    let run = || {
        let _slot = SLOTS.acquire();
        decode()
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(run)
        }
        _ => run(),
    }
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
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
                std::thread::spawn(move || {
                    run_large_decode(|| {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        running.fetch_sub(1, Ordering::SeqCst);
                    });
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
        // Two decodes occupying both worker threads' cores must not stall a timer task.
        let ticker = tokio::spawn(async {
            let start = std::time::Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            start.elapsed()
        });
        let busy: Vec<_> = (0..2)
            .map(|_| {
                tokio::spawn(async {
                    run_large_decode(|| std::thread::sleep(std::time::Duration::from_millis(400)));
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
        assert_eq!(rt.block_on(async { run_large_decode(|| 7) }), 7);
    }
}
