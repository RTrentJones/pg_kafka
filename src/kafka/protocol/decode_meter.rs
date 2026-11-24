//! What decoding a request allocates, measured as it allocates, and the decode stopped
//! once that passes a limit.
//!
//! A request frame's size says little about the memory its decode creates: every array
//! entry becomes a struct (and often an owned copy of its strings), whatever it takes on
//! the wire. A Metadata v9 topic entry of an empty name is 2 bytes on the wire and some
//! 90 bytes decoded, a DeleteGroups v2 group name 1 byte and some 60; with the growth of
//! the vectors that hold them, a decode holds up to some 100 times its frame (measured for
//! every API in this module's tests). A 100 MB frame of such entries would decode to
//! several GB, and an allocation failure aborts the process: `catch_unwind` cannot
//! contain it, and the postmaster goes through crash recovery.
//!
//! So every request decodes under a [`metered`] scope, which counts the heap memory the
//! decoding thread holds (the crate's global allocator counts every thread's live bytes,
//! [`CountingAllocator`]), and reads its body through a [`MeteredBuf`], which reports the
//! buffer exhausted once the decode holds more than its limit. Each entry of a request
//! reads at least one byte, so the decoder fails at the next entry it reads (the
//! kafka-protocol decoders read with `try_get_*` and return an error on an exhausted
//! buffer; a decoder that panics instead is caught as for a truncated frame). What a decode
//! holds at once is then at most its limit plus what it allocated since its last read:
//! one entry, one array's growth (a vector of at most the limit, grown to twice its size)
//! or one capacity hint (the fork caps those at 16 Ki entries), and the conversion of the
//! decoded wire request into the handler's input, measured at the end. The tests measure
//! it, for the APIs whose entries expand the most, at under three times the limit.
//!
//! What a decode keeps (its `held` bytes) is what the decoded request holds until it is
//! dropped. The memory a Produce request's records decode to is charged on its own, to
//! `super::decode_budget::DECODED`, and decodes [`unmetered`].
//!
//! The counters are per thread: the allocator adds what a thread allocates to that thread's
//! count and subtracts what it frees, so memory freed by another thread than the one that
//! allocated it skews both counts. A decode runs on one thread and frees on it what it
//! allocates and does not keep; a decoded request freed on the DB thread only lowers the
//! DB thread's count, which nothing reads.
//!
//! Runs on the network thread, so it logs through `tracing`, never pgrx.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ops::Range;

use bytes::{Buf, Bytes};
use kafka_protocol::protocol::buf::{ByteBuf, NotEnoughBytesError};

thread_local! {
    /// Bytes this thread allocated and has not freed.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// The running metered decode on this thread: the live bytes it started from (raised by
    /// what [`unmetered`] sections keep) and its limit.
    static METER: Cell<Option<(isize, isize)>> = const { Cell::new(None) };
    /// A [`MeteredBuf`] stopped the running metered decode.
    static STOPPED: Cell<bool> = const { Cell::new(false) };
    /// The most `LIVE` reached since [`heap_peak`](crate::testing::heap_peak) reset it.
    #[cfg(test)]
    pub(crate) static PEAK: Cell<isize> = const { Cell::new(0) };
    /// Bytes this thread ever allocated, freed or not.
    #[cfg(test)]
    pub(crate) static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

/// Bytes this thread allocated and has not freed (it may be negative: memory another thread
/// allocated and this one freed).
pub fn live_bytes() -> isize {
    LIVE.with(Cell::get)
}

fn grow(bytes: usize) {
    // `try_with`: a thread being torn down has no counters left, and allocating then is
    // not measured.
    #[cfg(test)]
    let _ = ALLOCATED.try_with(|allocated| allocated.set(allocated.get().saturating_add(bytes)));
    let bytes = isize::try_from(bytes).unwrap_or(isize::MAX);
    let _ = LIVE.try_with(|live| {
        let now = live.get().saturating_add(bytes);
        live.set(now);
        #[cfg(test)]
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

fn shrink(bytes: usize) {
    let bytes = isize::try_from(bytes).unwrap_or(isize::MAX);
    let _ = LIVE.try_with(|live| live.set(live.get().saturating_sub(bytes)));
}

/// The system allocator, counting the bytes each thread holds (see the module docs). The
/// count costs two thread-local accesses per allocation.
pub struct CountingAllocator;

// SAFETY: every call is forwarded unchanged to `System`, which upholds `GlobalAlloc`'s
// contract; the counting only reads and writes const-initialized thread-local cells, which
// never allocate and register no destructor.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's `layout` is passed on as it came.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as in `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's pointer and layout are those of an allocation of `System`.
        unsafe { System.dealloc(ptr, layout) };
        shrink(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Counted as a move (both blocks held at once, as when the block cannot grow in
        // place), the most a reallocation holds.
        grow(new_size);
        // SAFETY: the caller's arguments are passed on as they came.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if new.is_null() {
            shrink(new_size);
        } else {
            shrink(layout.size());
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// What [`metered`] measured of a decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metering {
    /// The bytes the decode allocated and still holds (what it returned holds), besides
    /// what its [`unmetered`] sections keep.
    pub held: usize,
    /// The decode passed its limit: a [`MeteredBuf`] stopped it, or it holds more than the
    /// limit at its end. Its result is then not to be used.
    pub over: bool,
}

/// Restores the meter that ran before a [`metered`] scope, also when the decode panics.
struct MeterScope {
    outer: Option<(isize, isize)>,
    outer_stopped: bool,
}

impl Drop for MeterScope {
    fn drop(&mut self) {
        METER.with(|m| m.set(self.outer));
        STOPPED.with(|s| s.set(self.outer_stopped));
    }
}

/// Run the decode `decode` on this thread under a limit of `limit` bytes of heap memory:
/// every [`MeteredBuf`] it reads through reports itself exhausted once the thread holds more
/// than `limit` bytes beyond what it held when the scope started (see the module docs).
/// Returns the decode's result with what it held at its end. A scope inside another counts
/// toward both.
pub fn metered<R>(limit: usize, decode: impl FnOnce() -> R) -> (R, Metering) {
    let limit = isize::try_from(limit).unwrap_or(isize::MAX);
    let scope = MeterScope {
        outer: METER.with(|m| m.replace(Some((live_bytes(), limit)))),
        outer_stopped: STOPPED.with(|s| s.replace(false)),
    };
    let result = decode();
    let (base, _) = METER.with(Cell::get).expect("set by this scope");
    let held = live_bytes().saturating_sub(base);
    let over = STOPPED.with(Cell::get) || held > limit;
    drop(scope);
    let held = usize::try_from(held).unwrap_or(0);
    (result, Metering { held, over })
}

/// Run `f` inside a [`metered`] decode without counting what it keeps toward the decode
/// (memory charged elsewhere: the records of a Produce request). What `f` allocates and
/// frees again still counts while it runs. Outside a metered decode it only runs `f`.
pub fn unmetered<R>(f: impl FnOnce() -> R) -> R {
    let before = live_bytes();
    let result = f();
    let kept = live_bytes().saturating_sub(before);
    METER.with(|m| {
        if let Some((base, limit)) = m.get() {
            m.set(Some((base.saturating_add(kept), limit)));
        }
    });
    result
}

/// Whether the running metered decode on this thread holds more than its limit.
pub fn over_limit() -> bool {
    METER
        .with(Cell::get)
        .is_some_and(|(base, limit)| live_bytes().saturating_sub(base) > limit)
}

/// A buffer a request body decodes from, which reports itself exhausted (no bytes left)
/// once the [`metered`] decode reading it holds more than its limit, and from then on.
pub struct MeteredBuf<B> {
    inner: B,
    stopped: Cell<bool>,
}

impl<B: Buf> MeteredBuf<B> {
    /// Read `inner` under the running meter (none: never stopped).
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            stopped: Cell::new(false),
        }
    }

    /// Whether the decode may read on; once it may not, it never may again.
    fn readable(&self) -> bool {
        if !self.stopped.get() && over_limit() {
            self.stopped.set(true);
            STOPPED.with(|s| s.set(true));
        }
        !self.stopped.get()
    }

    /// Whether this buffer stopped its decode.
    pub fn stopped(&self) -> bool {
        self.stopped.get()
    }

    /// The buffer read.
    pub fn into_inner(self) -> B {
        self.inner
    }
}

impl<B: Buf> Buf for MeteredBuf<B> {
    fn remaining(&self) -> usize {
        if self.readable() {
            self.inner.remaining()
        } else {
            0
        }
    }

    fn chunk(&self) -> &[u8] {
        if self.readable() {
            self.inner.chunk()
        } else {
            &[]
        }
    }

    // A decoder learns the buffer is exhausted from `remaining` and `chunk`; a read it
    // makes without asking still reads the frame (and so still makes progress through it).
    fn advance(&mut self, cnt: usize) {
        self.inner.advance(cnt);
    }
}

impl<B: ByteBuf> ByteBuf for MeteredBuf<B> {
    fn peek_bytes(&mut self, r: Range<usize>) -> Bytes {
        self.inner.peek_bytes(r)
    }

    fn get_bytes(&mut self, size: usize) -> Bytes {
        self.inner.get_bytes(size)
    }

    fn try_peek_bytes(&mut self, r: Range<usize>) -> Result<Bytes, NotEnoughBytesError> {
        if self.readable() {
            self.inner.try_peek_bytes(r)
        } else {
            Err(NotEnoughBytesError)
        }
    }

    fn try_get_bytes(&mut self, size: usize) -> Result<Bytes, NotEnoughBytesError> {
        if self.readable() {
            self.inner.try_get_bytes(size)
        } else {
            Err(NotEnoughBytesError)
        }
    }
}

#[cfg(test)]
mod tests;
