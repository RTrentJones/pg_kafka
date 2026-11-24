//! The heap a closure takes, measured with the crate's global allocator
//! (`kafka::protocol::decode_meter::CountingAllocator`, which counts what each thread holds), so
//! a test can measure it while other tests run on other threads.

use std::cell::Cell;

use crate::kafka::protocol::decode_meter::{live_bytes, ALLOCATED, PEAK};

/// Run `f` on this thread and return its result with the most heap memory, in bytes, that
/// it held at once beyond what the thread held before (what it returns included).
pub fn heap_peak<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = live_bytes();
    PEAK.with(|peak| peak.set(before));
    let result = f();
    let peak = PEAK.with(Cell::get);
    (result, usize::try_from(peak - before).unwrap_or(0))
}

/// Run `f` on this thread and return its result with the bytes it allocated in all, freed
/// or not: what it copied, for tests that bound copying that holds nothing for long.
pub fn heap_allocated<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCATED.with(Cell::get);
    let result = f();
    (result, ALLOCATED.with(Cell::get).saturating_sub(before))
}

#[cfg(test)]
mod tests {
    use super::{heap_allocated, heap_peak};

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

    #[test]
    fn heap_allocated_counts_what_a_closure_freed_too() {
        let ((), allocated) = heap_allocated(|| {
            for _ in 0..10 {
                drop(vec![0u8; 1000]);
            }
        });
        assert_eq!(allocated, 10_000);
    }
}
