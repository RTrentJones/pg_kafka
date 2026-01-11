//! Long polling tests for FetchRequest
//!
//! These tests verify the Phase 8 long polling implementation:
//! - max_wait_ms: Consumer waits for data or timeout
//! - min_bytes: Consumer waits until threshold is met
//! - Producer wakeup: Waiting consumers wake when data arrives

mod timeout;
mod wakeup;

pub use timeout::{test_long_poll_immediate_return, test_long_poll_timeout};
pub use wakeup::{test_long_poll_multiple_waiters, test_long_poll_producer_wakeup};
