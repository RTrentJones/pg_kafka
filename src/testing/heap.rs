//! The unit tests' global allocator: the system allocator, counting what each thread holds,
//! so a test can measure the heap a closure takes ([`heap_peak`]) while other tests run on
//! other threads.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Bytes this thread allocated and has not freed (memory freed by another thread than
    /// the one that allocated it skews both threads' counts; a measured closure frees what
    /// it allocates on its own thread).
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// The most `LIVE` reached since [`heap_peak`] reset it.
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn grow(bytes: usize) {
    let bytes = isize::try_from(bytes).unwrap_or(isize::MAX);
    // `try_with`: a thread being torn down has no counters left, and allocating then is
    // not measured.
    let _ = LIVE.try_with(|live| {
        let now = live.get().saturating_add(bytes);
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

fn shrink(bytes: usize) {
    let bytes = isize::try_from(bytes).unwrap_or(isize::MAX);
    let _ = LIVE.try_with(|live| live.set(live.get().saturating_sub(bytes)));
}

/// The system allocator, counting per thread.
pub struct CountingAllocator;

// SAFETY: every call is forwarded unchanged to `System`, which upholds `GlobalAlloc`'s
// contract; the counting only reads and writes thread-local cells, which never allocate.
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

/// Run `f` on this thread and return its result with the most heap memory, in bytes, that
/// it held at once beyond what the thread held before (what it returns included).
pub fn heap_peak<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = LIVE.with(Cell::get);
    PEAK.with(|peak| peak.set(before));
    let result = f();
    let peak = PEAK.with(Cell::get);
    (result, usize::try_from(peak - before).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::heap_peak;

    #[test]
    fn heap_peak_counts_what_a_closure_holds_at_once() {
        let (kept, peak) = heap_peak(|| {
            drop(vec![0u8; 1 << 20]);
            vec![0u8; 1000]
        });
        assert_eq!(kept.len(), 1000);
        assert!((1 << 20..(1 << 20) + 1000).contains(&peak), "{peak}");
        let ((), peak) = heap_peak(|| {
            let mut v = vec![0u8; 1000];
            v.reserve_exact(3000);
        });
        assert_eq!(peak, 1000 + 4000, "a reallocation counts both blocks");
    }
}
