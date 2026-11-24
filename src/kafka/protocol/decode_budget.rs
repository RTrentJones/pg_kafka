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
//! * per request, the decoding work, counted as everything it ever charged (what is given
//!   back still counts), stops at a limit of its own ([`DecodeBudget::limit_work`]): what a
//!   refused partition decoded is freed, but the time spent decoding it is not, so a request
//!   of many partitions that each decode much and are then refused cannot decode without
//!   end;
//! * per request, the decoding thread's CPU time ([`DecodeBudget::limit_time`]), checked
//!   between chunks of compressed input and between partitions: what a decode charges
//!   bounds the work its
//!   output takes, not the work its input takes, and input can cost much while decoding to
//!   nothing (gzip of empty dynamic-Huffman blocks, some 200 ns per byte, or snappy and
//!   zstd framing of empty blocks or frames);
//! * across the process, decoded requests that the DB thread has not run yet hold at most
//!   [`DECODED_BUDGET_BYTES`] ([`DECODED`]). The decoded request keeps its share
//!   ([`DecodedCharge`]) until it is dropped after its handler ran.
//!
//! The rest of a request's decode (its arrays, strings, the handler's input built from them,
//! for every API including Produce's own structure) is metered as it allocates
//! (`super::decode_meter`): at most [`INLINE_REQUEST_DECODE_BYTES`] on an I/O thread, then
//! [`MAX_REQUEST_DECODE_BYTES`] under a large-decode slot, past which the connection closes.
//! What the decoded request holds is charged to [`DECODED_REQUESTS`] and kept until its
//! response is written (`super::decode_request`).
//!
//! What a decode holds at any moment (the decompressed section as it grows, then the section
//! and the records' copies of it) is at most twice what it has charged by then: the
//! section's buffer is charged by its capacity before it grows, and shrunk to its bytes
//! once decoded, the spare capacity given back. Measured for every codec, at most 2.0
//! times, besides the decompressors' fixed state (`recordbatch`'s
//! `a_decode_holds_at_most_twice_its_charge`). The measurement counts every allocation of
//! the decode, the decompressors' own included: gzip and snappy decode in Rust, zstd's
//! contexts allocate through Rust's allocator (its whole window, which is charged as it
//! fills), and lz4's block buffers, which its C library allocates with `malloc`, are
//! measured through lz4's allocator hook and added. lz4's buffers are fixed by the frame's
//! block size and not charged: some 128 KiB at Kafka's default 64 KiB blocks, at most about
//! 8.1 MiB at 4 MiB blocks, per decode running
//! (`lz4_block_buffers_are_fixed_by_the_frame_header`). The charge a decoded request keeps (its
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

/// A decode that fits its memory limit charges each byte it keeps at most three times
/// over its course (its output buffer's growth, a zstd window charged as it fills, then the
/// records), so a work limit of three times the memory limit refuses only what a request's
/// refused partitions decoded besides.
const WORK_PER_KEPT_BYTE: usize = 3;

/// The most decoding work (everything charged, given back or not) a decode on an I/O thread
/// may do; past it the request decodes again under a large-decode slot. Some 50 ms of
/// decoding gzip-compressed 1 KB records of hex digits (compressed about 1.75:1, they
/// decode at some 170 MB/s in a release build), so a decode on an I/O thread usually stops
/// at [`INLINE_DECODE_TIME`] first; a 1 MB request of them decodes in about 5 ms.
pub const INLINE_DECODE_WORK_BYTES: usize = WORK_PER_KEPT_BYTE * INLINE_DECODE_BYTES;

/// The most decoding work a request may do under a large-decode slot: what a request that
/// fits may take, and as much again for partitions refused after they decoded (one refused
/// at the request limit, or many smaller). Past it the request's other partitions are
/// refused MESSAGE_TOO_LARGE without decoding (a request at the request limit, 85 MB of
/// gzip-compressed 1 KB records of hex digits, decodes in about 0.5 s in a release build).
/// Input that decodes to little is bounded by [`SLOT_DECODE_TIME`] instead.
pub const SLOT_DECODE_WORK_BYTES: usize = (WORK_PER_KEPT_BYTE + 1) * MAX_DECODED_REQUEST_BYTES;

/// The most CPU time a decode on an I/O thread may take; past it the request decodes again
/// under a large-decode slot. The work limit bounds what a decode charges, not what its
/// input costs, which may decode to nothing: this bounds that too, checked between chunks
/// of input ([`DecodeBudget::check_time`]). An I/O thread decodes for at most this and one
/// chunk (some 3 ms of the slowest input); a 1 MB request of gzip-compressed 1 KB records
/// decodes in about 5 ms.
pub const INLINE_DECODE_TIME: Duration = Duration::from_millis(20);

/// The most CPU time a decode under a large-decode slot may take; past it the partition
/// decoding and those after it are refused REQUEST_TIMED_OUT (UNKNOWN_TOPIC_OR_PARTITION
/// for those of an unknown topic), so no request decodes for longer than this and one chunk
/// of input: some four times what a request at the request limit takes (0.5 s for 85 MB of
/// gzip-compressed 1 KB records). Counted in CPU time, a busy machine stretches the slot's
/// hold in wall-clock time but refuses no legitimate request.
pub const SLOT_DECODE_TIME: Duration = Duration::from_secs(2);

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
    /// The request's decoding work ([`DecodeBudget::limit_work`]).
    Work,
    /// The process-wide budget.
    Global,
    /// The decode's time ([`DecodeBudget::limit_time`]).
    Time,
}

/// A process-wide budget of decoded bytes, with a wake-up for those waiting for some of it.
#[derive(Debug)]
pub struct DecodePool {
    remaining: AtomicUsize,
    freed: Notify,
}

/// The decoded records of all requests waiting for the DB thread.
pub static DECODED: DecodePool = DecodePool::new(DECODED_BUDGET_BYTES);

/// The most memory one request's decode may hold, besides a Produce request's records
/// (`super::decode_meter`); a request whose decode would hold more closes the connection,
/// as Kafka closes it for a frame over `socket.request.max.bytes`. 64 MiB: what a Metadata
/// request of some 300,000 topic names of 50 bytes, a Fetch of 500,000 partitions or
/// an OffsetCommit of 400,000 partitions decodes to, all far beyond what Kafka's clients
/// send (a group's assignment, which a SyncGroup carries, must fit in one
/// `__consumer_offsets` record, 1 MiB by default).
pub const MAX_REQUEST_DECODE_BYTES: usize = 64 * 1024 * 1024;

/// The most memory a request's decode may hold on an I/O thread; a larger decode is
/// stopped and runs again under a large-decode slot ([`LargeDecodes`]), with
/// [`MAX_REQUEST_DECODE_BYTES`]. Some 15,000 Metadata topics or 60,000 Fetch partitions.
pub const INLINE_REQUEST_DECODE_BYTES: usize = INLINE_DECODE_BYTES;

/// The most memory decoded requests (besides Produce records) may hold together, from their
/// decode until their response is written.
pub const DECODED_REQUESTS_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// A decoded request holding at most this much may take the last
/// [`CONTROL_RESERVE_BYTES`] of [`DECODED_REQUESTS`]: Heartbeats, ApiVersions, OffsetCommits
/// and the other small requests are not locked out by a few large ones.
pub const SMALL_REQUEST_BYTES: usize = 64 * 1024;

/// What larger decoded requests leave of [`DECODED_REQUESTS`] for small ones.
pub const CONTROL_RESERVE_BYTES: usize = 32 * 1024 * 1024;

const _: () = assert!(
    INLINE_REQUEST_DECODE_BYTES <= MAX_REQUEST_DECODE_BYTES
        && MAX_REQUEST_DECODE_BYTES + CONTROL_RESERVE_BYTES <= DECODED_REQUESTS_BUDGET_BYTES
        && SMALL_REQUEST_BYTES <= CONTROL_RESERVE_BYTES
);

/// The decoded requests (all but the records of Produce requests) from their decode until
/// their response is written.
pub static DECODED_REQUESTS: DecodePool = DecodePool::new(DECODED_REQUESTS_BUDGET_BYTES);

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

    /// Charge `n` bytes, leaving at least `keep` bytes free; `None`, charging nothing, when
    /// the pool lacks them.
    pub fn charge_leaving(&'static self, n: usize, keep: usize) -> Option<DecodedCharge> {
        let mut remaining = self.remaining.load(Ordering::Relaxed);
        loop {
            if n.saturating_add(keep) > remaining {
                return None;
            }
            match self.remaining.compare_exchange_weak(
                remaining,
                remaining - n,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(DecodedCharge {
                        pool: self,
                        bytes: n,
                    })
                }
                Err(actual) => remaining = actual,
            }
        }
    }

    /// Charge a decoded request of `n` bytes to this pool: a small one
    /// ([`SMALL_REQUEST_BYTES`]) may take every byte left, a larger one must leave
    /// [`CONTROL_RESERVE_BYTES`]. `None` when it does not fit now.
    pub fn charge_request(&'static self, n: usize) -> Option<DecodedCharge> {
        let keep = if n <= SMALL_REQUEST_BYTES {
            0
        } else {
            CONTROL_RESERVE_BYTES
        };
        self.charge_leaving(n, keep)
    }

    /// What [`Self::charge_request`] needs free to charge `n` bytes.
    pub fn request_room(n: usize) -> usize {
        if n <= SMALL_REQUEST_BYTES {
            n
        } else {
            n.saturating_add(CONTROL_RESERVE_BYTES)
        }
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
    work: Cell<usize>,
    work_limit: usize,
    work_exhausted: Cell<bool>,
    clock: Option<CpuClock>,
    out_of_time: Cell<bool>,
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
            work: Cell::new(0),
            work_limit: usize::MAX,
            work_exhausted: Cell::new(false),
            clock: None,
            out_of_time: Cell::new(false),
        }
    }

    /// Limit the decoding work to `bytes`: everything ever charged, whether given back
    /// since or not. Unlimited by default.
    #[must_use]
    pub fn limit_work(mut self, bytes: usize) -> Self {
        self.work_limit = bytes;
        self
    }

    /// Limit the decode to `limit` of the calling thread's CPU time from now:
    /// [`Self::check_time`] fails once the thread has run that long on the CPU (time it
    /// waits for one does not count). The decode runs on the thread that sets the limit.
    /// Unlimited by default.
    #[must_use]
    pub fn limit_time(mut self, limit: Duration) -> Self {
        self.clock = Some(CpuClock::start(limit));
        self
    }

    /// Whether the decode may go on, checked between chunks of its input and between
    /// partitions: fails once its time limit has passed, and from then on
    /// ([`DecodeBudget::exhausted`] is then [`Exhausted::Time`]). What a decode charges
    /// bounds only the work its output takes; this bounds the work its input takes, which
    /// may decode to nothing (a gzip stream of empty blocks costs some 200 ns per byte).
    pub fn check_time(&self) -> Result<(), Exhausted> {
        let passed = self.out_of_time.get() || self.clock.as_ref().is_some_and(CpuClock::passed);
        if passed {
            self.out_of_time.set(true);
            self.exhausted.set(Some(Exhausted::Time));
            return Err(Exhausted::Time);
        }
        Ok(())
    }

    /// Whether the decode's time limit has passed: the request may decode nothing more.
    pub fn out_of_time(&self) -> bool {
        self.out_of_time.get()
    }

    /// Charge `n` more bytes. Fails, charging nothing, when that would pass the
    /// budget's limit, its work limit or the process-wide budget;
    /// [`DecodeBudget::exhausted`] then says which.
    pub fn take(&self, n: usize) -> Result<(), Exhausted> {
        let total = self.charged.get().saturating_add(n);
        let work = self.work.get().saturating_add(n);
        let result = if total > self.limit {
            Err(Exhausted::Limit)
        } else if work > self.work_limit {
            self.work_exhausted.set(true);
            Err(Exhausted::Work)
        } else if !self.pool.take(n) {
            self.wanted.set(total);
            Err(Exhausted::Global)
        } else {
            self.charged.set(total);
            self.work.set(work);
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
    /// freed, so that it does not count against the partitions after it. Its work still
    /// counts ([`Self::work_exhausted`] and [`Self::out_of_time`] stay set).
    pub fn rewind(&self, mark: usize) {
        self.release(self.charged.get().saturating_sub(mark));
        self.exhausted.set(None);
    }

    /// Bytes this budget may still charge before its limit or its work limit (the
    /// process-wide budget may hold fewer).
    pub fn room(&self) -> usize {
        self.limit
            .saturating_sub(self.charged.get())
            .min(self.work_limit.saturating_sub(self.work.get()))
    }

    /// Whether a charge ran into the work limit: the request may decode nothing more.
    pub fn work_exhausted(&self) -> bool {
        self.work_exhausted.get()
    }

    /// The decoding work so far: every byte ever charged.
    pub fn work(&self) -> usize {
        self.work.get()
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

/// The CPU time a decode may spend, on the thread that runs it.
///
/// A decode's time limit bounds the work its input costs, so it counts the decoding thread's
/// CPU time (`CLOCK_THREAD_CPUTIME_ID`), not wall-clock time: on a busy machine (other
/// connections' decodes, the DB thread, other processes on the same CPUs) a legitimate
/// request takes several times its CPU time in wall-clock time, and refusing it then would
/// refuse its every retry too. A request that costs much to decode still costs as much CPU,
/// busy machine or not. Counting the compressed input instead would bound the work only as
/// well as a cost per byte guessed for each codec and machine fits the slowest input.
///
/// The thread's CPU time is read with a system call (some 0.3 us), so it is read only once
/// the wall clock says the limit may have passed: a thread runs on the CPU no faster than
/// the wall clock goes, so until `limit - spent` has passed in wall-clock time since the last
/// reading, the limit cannot have passed. On an idle machine that is one reading at the end
/// of the limit, under contention a few more, at most one per [`MIN_CPU_CLOCK_READ`].
#[derive(Debug)]
struct CpuClock {
    /// The thread's CPU time when the limit was set.
    start: Duration,
    limit: Duration,
    /// Before this the limit cannot have passed.
    next_read: Cell<std::time::Instant>,
    /// The CPU time cannot be read: the limit counts wall-clock time from `next_read`'s
    /// first value.
    wall_clock: bool,
}

/// The least wall-clock time between two readings of a decode's CPU time: the most it may
/// run past its limit.
const MIN_CPU_CLOCK_READ: Duration = Duration::from_millis(1);

impl CpuClock {
    fn start(limit: Duration) -> Self {
        let now = std::time::Instant::now();
        let start = thread_cpu_time();
        Self {
            start: start.unwrap_or_default(),
            limit,
            next_read: Cell::new(now + limit),
            wall_clock: start.is_none(),
        }
    }

    /// Whether the thread has spent the limit since the clock started.
    fn passed(&self) -> bool {
        let now = std::time::Instant::now();
        if now < self.next_read.get() {
            return false;
        }
        let Some(cpu) = thread_cpu_time().filter(|_| !self.wall_clock) else {
            return true;
        };
        let spent = cpu.saturating_sub(self.start);
        match self.limit.checked_sub(spent) {
            Some(left) if !left.is_zero() => {
                self.next_read.set(now + left.max(MIN_CPU_CLOCK_READ));
                false
            }
            _ => true,
        }
    }
}

/// The CPU time the calling thread has run, or `None` where it cannot be read.
#[cfg(unix)]
fn thread_cpu_time() -> Option<Duration> {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable timespec for the duration of the call, which only
    // writes it; CLOCK_THREAD_CPUTIME_ID needs no other state.
    let read = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &raw mut now) };
    if read != 0 {
        return None;
    }
    Some(Duration::new(
        u64::try_from(now.tv_sec).ok()?,
        u32::try_from(now.tv_nsec).ok()?,
    ))
}

/// The CPU time the calling thread has run, or `None` where it cannot be read.
#[cfg(not(unix))]
fn thread_cpu_time() -> Option<Duration> {
    None
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
