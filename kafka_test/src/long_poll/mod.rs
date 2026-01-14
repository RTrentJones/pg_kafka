//! Long polling tests for FetchRequest
//!
//! These tests verify the Phase 8 long polling implementation:
//! - max_wait_ms: Consumer waits for data or timeout
//! - min_bytes: Consumer waits until threshold is met
//! - Producer wakeup: Waiting consumers wake when data arrives
//! - Edge cases: Timeout precision, disconnect handling, multiple waiters

mod edge_cases;
mod timeout;
mod wakeup;

pub use edge_cases::{
    test_long_poll_auto_commit_interval, test_long_poll_consumer_disconnect,
    test_long_poll_min_bytes_threshold, test_long_poll_multiple_consumers_same_partition,
    test_long_poll_timeout_precision,
};
pub use timeout::{test_long_poll_immediate_return, test_long_poll_timeout};
pub use wakeup::{test_long_poll_multiple_waiters, test_long_poll_producer_wakeup};
