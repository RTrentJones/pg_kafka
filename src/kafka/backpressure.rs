// Byte-weighted backpressure for the request channel
//
// This module implements memory-safe backpressure by tracking the total
// bytes of pending requests in the channel. When the byte limit is exceeded,
// producers (the network thread) should pause accepting new requests.
//
// ## Architecture
//
// The `ByteTracker` is shared between:
// - Network thread (listener.rs): Adds bytes when sending requests
// - Database thread (worker.rs): Subtracts bytes when processing requests
//
// ## Thread Safety
//
// Uses AtomicUsize for lock-free concurrent access from both threads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default maximum pending bytes (100 MB)
///
/// This value is chosen to:
/// - Allow high throughput (100MB ≈ 100 large Kafka batches)
/// - Prevent OOM even under heavy load
/// - Be configurable via GUC in the future
pub const DEFAULT_MAX_PENDING_BYTES: usize = 100 * 1024 * 1024;

/// Tracks the total bytes of pending requests in the channel.
///
/// This provides byte-weighted backpressure to prevent OOM from large
/// message batches filling the bounded channel.
#[derive(Debug)]
pub struct ByteTracker {
    /// Current pending bytes (atomically updated)
    pending_bytes: AtomicUsize,
    /// Maximum allowed pending bytes
    max_bytes: usize,
}

impl ByteTracker {
    /// Create a new ByteTracker with the specified maximum bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            pending_bytes: AtomicUsize::new(0),
            max_bytes,
        }
    }

    /// Create a new ByteTracker with the default maximum (100 MB).
    pub fn with_default_max() -> Self {
        Self::new(DEFAULT_MAX_PENDING_BYTES)
    }

    /// Add bytes when a request is queued.
    ///
    /// Returns the new total pending bytes.
    pub fn add(&self, bytes: usize) -> usize {
        self.pending_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes
    }

    /// Subtract bytes when a request is processed.
    ///
    /// Uses saturating subtraction to prevent underflow in edge cases.
    pub fn subtract(&self, bytes: usize) {
        let _ = self
            .pending_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    /// Check if adding the specified bytes would exceed the limit.
    ///
    /// This is a non-blocking check used to decide whether to accept
    /// a new request or apply backpressure.
    pub fn would_exceed_limit(&self, additional_bytes: usize) -> bool {
        let current = self.pending_bytes.load(Ordering::Relaxed);
        current + additional_bytes > self.max_bytes
    }

    /// Get the current pending bytes.
    pub fn current(&self) -> usize {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    /// Get the maximum allowed bytes.
    pub fn max(&self) -> usize {
        self.max_bytes
    }

    /// Get the current utilization as a percentage (0-100).
    pub fn utilization_percent(&self) -> u8 {
        let current = self.current();
        if self.max_bytes == 0 {
            return 100;
        }
        ((current * 100) / self.max_bytes).min(100) as u8
    }
}

/// Shared byte tracker wrapped in Arc for thread-safe sharing.
pub type SharedByteTracker = Arc<ByteTracker>;

/// Create a new shared byte tracker.
pub fn new_shared_tracker(max_bytes: usize) -> SharedByteTracker {
    Arc::new(ByteTracker::new(max_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_tracker_add_subtract() {
        let tracker = ByteTracker::new(1000);

        // Add bytes
        assert_eq!(tracker.add(100), 100);
        assert_eq!(tracker.add(200), 300);
        assert_eq!(tracker.current(), 300);

        // Subtract bytes
        tracker.subtract(150);
        assert_eq!(tracker.current(), 150);

        // Subtract more than available (saturating)
        tracker.subtract(200);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_would_exceed_limit() {
        let tracker = ByteTracker::new(1000);

        assert!(!tracker.would_exceed_limit(500));
        assert!(!tracker.would_exceed_limit(1000));
        assert!(tracker.would_exceed_limit(1001));

        tracker.add(600);
        assert!(!tracker.would_exceed_limit(400));
        assert!(tracker.would_exceed_limit(401));
    }

    #[test]
    fn test_utilization_percent() {
        let tracker = ByteTracker::new(1000);

        assert_eq!(tracker.utilization_percent(), 0);

        tracker.add(500);
        assert_eq!(tracker.utilization_percent(), 50);

        tracker.add(500);
        assert_eq!(tracker.utilization_percent(), 100);

        // Can't exceed 100%
        tracker.add(500);
        assert_eq!(tracker.utilization_percent(), 100);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let tracker = Arc::new(ByteTracker::new(10_000_000));

        // Spawn threads that add bytes
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let t = Arc::clone(&tracker);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        t.add(100);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // 10 threads × 1000 iterations × 100 bytes = 1,000,000
        assert_eq!(tracker.current(), 1_000_000);
    }

    #[test]
    fn test_with_default_max() {
        let tracker = ByteTracker::with_default_max();
        assert_eq!(tracker.max(), DEFAULT_MAX_PENDING_BYTES);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_max_getter() {
        let tracker = ByteTracker::new(12345);
        assert_eq!(tracker.max(), 12345);
    }

    #[test]
    fn test_new_shared_tracker() {
        let tracker = new_shared_tracker(5000);
        assert_eq!(tracker.max(), 5000);
        assert_eq!(tracker.current(), 0);

        // Test that it's actually shared (Arc)
        let tracker2 = Arc::clone(&tracker);
        tracker.add(100);
        assert_eq!(tracker2.current(), 100);
    }

    #[test]
    fn test_utilization_percent_zero_max() {
        let tracker = ByteTracker::new(0);
        // With max_bytes = 0, utilization should be 100%
        assert_eq!(tracker.utilization_percent(), 100);
    }

    #[test]
    fn test_debug_format() {
        let tracker = ByteTracker::new(1000);
        tracker.add(500);
        let debug_str = format!("{:?}", tracker);
        assert!(debug_str.contains("ByteTracker"));
    }
}
