//! How much memory the records of Produce requests may take once decoded, and how a request
//! waits for room to decode.
//!
//! A frame's size says little about what its records take: a gzip or zstd batch of zeros
//! expands about a thousandfold, and every record, however small on the wire (7 bytes),
//! becomes a few hundred bytes of decoded structure. The memory of a decode is therefore
//! charged to a [`DecodeBudget`] before it is allocated, never checked afterwards: the
//! records and headers at a fixed cost each, the decoded bytes as the decompressors produce
//! them (they write through the budget and stop at its limit; see
//! `super::recordbatch`):
//!
//! * per request, at most [`MAX_DECODED_REQUEST_BYTES`] across all its partitions; a
//!   partition that does not fit in what its request may still hold is answered
//!   MESSAGE_TOO_LARGE on its own, and what its decode charged is given back, as is the
//!   charge of any partition refused after it began to decode;
//! * across the process, decoded requests that the DB thread has not run yet hold at most
//!   [`DECODED_BUDGET_BYTES`] ([`DECODED`]). The decoded request keeps its share
//!   ([`DecodedCharge`]) until it is dropped after its handler ran.
//!
//! What a decode holds at any moment (the decompressed section as it grows, then the section
//! and the records' copies of it) is at most twice what it has charged by then: the
//! section's buffer is charged by its capacity before it grows, and shrunk to its bytes
//! once decoded, the spare capacity given back. Measured for every codec, at most 2.0
//! times, besides the decompressors' fixed state (`recordbatch`'s
//! `a_decode_holds_at_most_twice_its_charge`). The charge a decoded request keeps (its
//! section's bytes) may be half the most it charged while the section grew; once the
//! section is dropped the request holds no more than that charge.
//!
//! Decoding is also kept off the two tokio I/O threads once it is large: at most
//! [`LARGE_DECODE_SLOTS`] such decodes run at once, each on a blocking-pool thread
//! ([`LargeDecodes`]).
//!
//! A request that finds every slot taken, or the process-wide budget exhausted, waits for
//! one without holding a thread ([`LargeDecodes::run`], [`DecodePool::wait_for`]), as Kafka
//! holds a request it has no memory for (`queued.max.request.bytes`) instead of failing it.
//! The wait is bounded ([`decode_wait`]); past it the request is answered
//! REQUEST_TIMED_OUT, which clients retry. Slots are granted in arrival order, and a
//! connection waits for at most one at a time (its read loop waits with it), so one
//! connection cannot take more than its turn. While it waits, a request's frame is charged
//! to the frame budget as an incomplete frame is ([`HeldFrame`]), so the raw bytes waiting
//! stay bounded as the bytes still arriving are.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, Semaphore};
use tokio::time::Instant;

use super::framing::{
    charge_frame_budget, release_frame_budget, FRAME_RESERVE_REMAINING, UNMETERED_FRAME_BYTES,
};
use crate::kafka::constants::MAX_REQUEST_SIZE;

/// The most memory one Produce request's records may take decoded, summed over its
/// partitions: the size of the largest request frame, so a compressed request may decode to
/// no more than an uncompressed one of large records.
pub const MAX_DECODED_REQUEST_BYTES: usize = MAX_REQUEST_SIZE;

/// The most decoded record memory all requests waiting for the DB thread may hold together.
pub const DECODED_BUDGET_BYTES: usize = 512 * 1024 * 1024;

const _: () = assert!(MAX_DECODED_REQUEST_BYTES <= DECODED_BUDGET_BYTES);

/// Decodes taking up to this much memory run inline on the I/O thread (the cost of moving
/// them elsewhere would exceed the work); a larger request decodes through
/// [`LargeDecodes`]. Twice the clients' default request size limit (1 MiB: Java's
/// `max.request.size`, librdkafka's `message.max.bytes`), so a request of that size decodes
/// inline unless its records are under some 200 bytes or it is compressed.
pub const INLINE_DECODE_BYTES: usize = 2 * 1024 * 1024;

/// How many large decodes may run at once.
pub const LARGE_DECODE_SLOTS: usize = 2;

/// The longest a request waits for a large-decode slot or for decode budget.
pub const MAX_DECODE_WAIT: Duration = Duration::from_secs(10);

/// How long a Produce request whose `timeout_ms` is `timeout_ms` may wait to decode: half
/// its timeout, at most [`MAX_DECODE_WAIT`]. Java and librdkafka send their
/// `request.timeout.ms` (30 s by default) as the Produce timeout and drop the connection
/// when it passes without a response, so the REQUEST_TIMED_OUT a wait ends in must reach
/// them well before; a client asking for a short timeout gets its answer quickly. A
/// timeout of 0 or less (no deadline asked for) waits the longest.
pub fn decode_wait(timeout_ms: i32) -> Duration {
    match u64::try_from(timeout_ms) {
        Ok(ms) if ms > 0 => MAX_DECODE_WAIT.min(Duration::from_millis(ms / 2)),
        _ => MAX_DECODE_WAIT,
    }
}

/// Which limit a decode ran into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exhausted {
    /// The budget's own limit (the request limit, or the inline limit).
    Limit,
    /// The process-wide budget.
    Global,
}

/// A process-wide budget of decoded bytes, with a wake-up for those waiting for some of it.
#[derive(Debug)]
pub struct DecodePool {
    remaining: AtomicUsize,
    freed: Notify,
}

/// The decoded records of all requests waiting for the DB thread.
pub static DECODED: DecodePool = DecodePool::new(DECODED_BUDGET_BYTES);

impl DecodePool {
    /// A pool of `bytes`.
    pub const fn new(bytes: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(bytes),
            freed: Notify::const_new(),
        }
    }

    /// Bytes not charged.
    pub fn remaining(&self) -> usize {
        self.remaining.load(Ordering::Relaxed)
    }

    fn take(&self, n: usize) -> bool {
        charge_frame_budget(&self.remaining, n)
    }

    fn give_back(&self, n: usize) {
        if n > 0 {
            release_frame_budget(&self.remaining, n);
            self.freed.notify_waiters();
        }
    }

    /// Wait until at least `wanted` bytes are free; false when `deadline` passes first.
    /// Waiters are not queued: whoever finds the bytes free first takes them, and a waiter
    /// for many bytes may be passed by smaller decodes until its deadline.
    pub async fn wait_for(&self, wanted: usize, deadline: Instant) -> bool {
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            // Registered before the check, so bytes given back after it wake this waiter.
            freed.as_mut().enable();
            if self.remaining() >= wanted {
                return true;
            }
            if tokio::time::timeout_at(deadline, freed).await.is_err() {
                return false;
            }
        }
    }
}

/// The decoded-memory allowance of one request while it decodes, in bytes. Memory is
/// charged before it is allocated; whatever is charged is given back when the budget is
/// dropped, unless it is handed to the decoded request with [`DecodeBudget::into_charge`].
#[derive(Debug)]
pub struct DecodeBudget {
    pool: &'static DecodePool,
    limit: usize,
    charged: Cell<usize>,
    most: Cell<usize>,
    exhausted: Cell<Option<Exhausted>>,
    wanted: Cell<usize>,
}

impl DecodeBudget {
    /// A budget of `limit` bytes charging the process-wide budget.
    pub fn new(limit: usize) -> Self {
        Self::with_pool(limit, &DECODED)
    }

    /// A budget of `limit` bytes charging `pool` (tests use their own).
    pub fn with_pool(limit: usize, pool: &'static DecodePool) -> Self {
        Self {
            pool,
            limit,
            charged: Cell::new(0),
            most: Cell::new(0),
            exhausted: Cell::new(None),
            wanted: Cell::new(0),
        }
    }

    /// Charge `n` more bytes. Fails, charging nothing, when that would pass the
    /// budget's limit or the process-wide budget; [`DecodeBudget::exhausted`] then says
    /// which.
    pub fn take(&self, n: usize) -> Result<(), Exhausted> {
        let total = self.charged.get().saturating_add(n);
        let result = if total > self.limit {
            Err(Exhausted::Limit)
        } else if !self.pool.take(n) {
            self.wanted.set(total);
            Err(Exhausted::Global)
        } else {
            self.charged.set(total);
            self.most.set(self.most.get().max(total));
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

    /// How much this decode had asked for in all when the process-wide budget refused it:
    /// the least a retry needs free.
    pub fn wanted(&self) -> usize {
        self.wanted.get()
    }

    /// Give back `n` of the bytes charged (memory a decode no longer holds).
    pub fn release(&self, n: usize) {
        let n = n.min(self.charged.get());
        self.charged.set(self.charged.get() - n);
        self.pool.give_back(n);
    }

    /// Give back everything charged since [`Self::charged`] was `mark`, and forget which
    /// limit a charge ran into since: the decode of a refused partition, whose memory is
    /// freed, so that it does not count against the partitions after it.
    pub fn rewind(&self, mark: usize) {
        self.release(self.charged.get().saturating_sub(mark));
        self.exhausted.set(None);
    }

    /// Bytes this budget may still charge before its limit (the process-wide budget may
    /// hold fewer).
    pub fn room(&self) -> usize {
        self.limit.saturating_sub(self.charged.get())
    }

    /// Bytes charged so far.
    pub fn charged(&self) -> usize {
        self.charged.get()
    }

    /// The most bytes charged at once so far (what is given back lowers [`Self::charged`],
    /// not this).
    pub fn most_charged(&self) -> usize {
        self.most.get()
    }

    /// Hand what is charged to the decoded request, which gives it back when dropped.
    pub fn into_charge(self) -> DecodedCharge {
        DecodedCharge {
            pool: self.pool,
            bytes: self.charged.replace(0),
        }
    }
}

impl Drop for DecodeBudget {
    fn drop(&mut self) {
        self.pool.give_back(self.charged.get());
    }
}

/// The decoded memory a request holds of the process-wide budget, given back when it is
/// dropped (after the DB thread ran the request, or when the request is discarded).
#[derive(Debug)]
pub struct DecodedCharge {
    pool: &'static DecodePool,
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
            pool: &DECODED,
            bytes: 0,
        }
    }
}

impl Drop for DecodedCharge {
    fn drop(&mut self) {
        self.pool.give_back(self.bytes);
    }
}

/// The raw bytes of a request frame held while the request waits to decode, charged to
/// the frame budget like the bytes of an incomplete frame (frames up to
/// [`UNMETERED_FRAME_BYTES`] are not, as there), and given back on drop.
#[derive(Debug)]
pub struct HeldFrame {
    budget: &'static AtomicUsize,
    bytes: usize,
}

impl HeldFrame {
    /// Charge a frame of `bytes` to the frame budget; `None` when the budget lacks them.
    pub fn charge(bytes: usize) -> Option<Self> {
        Self::charge_to(bytes, &FRAME_RESERVE_REMAINING)
    }

    fn charge_to(bytes: usize, budget: &'static AtomicUsize) -> Option<Self> {
        let bytes = if bytes > UNMETERED_FRAME_BYTES {
            bytes
        } else {
            0
        };
        // Built only once charged: dropping it gives the bytes back.
        charge_frame_budget(budget, bytes).then(|| Self { budget, bytes })
    }
}

impl Drop for HeldFrame {
    fn drop(&mut self) {
        release_frame_budget(self.budget, self.bytes);
    }
}

/// Why [`LargeDecodes::run`] did not return the decode's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotRun {
    /// No slot came free before the deadline.
    TimedOut,
    /// The decode panicked.
    Panicked,
}

/// The large decodes of the network thread: at most `slots` at once, each on a
/// blocking-pool thread, granted in the order they were asked for.
#[derive(Debug)]
pub struct LargeDecodes {
    slots: Semaphore,
}

/// The large decodes of the network thread.
pub static LARGE_DECODES: LargeDecodes = LargeDecodes::new(LARGE_DECODE_SLOTS);

impl LargeDecodes {
    /// At most `slots` decodes at once.
    pub const fn new(slots: usize) -> Self {
        Self {
            slots: Semaphore::const_new(slots),
        }
    }

    /// Run `decode` on a blocking-pool thread once a slot is free, waiting for one until
    /// `deadline` without holding a thread. The slot is held by the decode itself, so a
    /// caller that stops waiting (its connection closed) does not free it before the
    /// decode ends.
    pub async fn run<R: Send + 'static>(
        &'static self,
        deadline: Instant,
        decode: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, NotRun> {
        // The semaphore is never closed, so only the deadline fails the acquire.
        let Ok(Ok(permit)) = tokio::time::timeout_at(deadline, self.slots.acquire()).await else {
            return Err(NotRun::TimedOut);
        };
        tokio::task::spawn_blocking(move || {
            let _slot = permit;
            decode()
        })
        .await
        .map_err(|_| NotRun::Panicked)
    }
}

#[cfg(test)]
mod tests;
