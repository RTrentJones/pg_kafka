//! Long polling tests for FetchRequest
//!
//! These tests verify the Phase 8 long polling implementation:
//! - max_wait_ms: Consumer waits for data or timeout
//! - min_bytes: Consumer waits until threshold is met
//! - Producer wakeup: Waiting consumers wake when data arrives
//! - Edge cases: Timeout precision, disconnect handling, multiple waiters, auto-commit

mod edge_cases;
mod task_cap;
mod timeout;
mod transaction_end;
mod wakeup;

/// The `long_poll` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["long_poll";
        timeout::test_long_poll_immediate_return: sequential, // Timing-sensitive
        timeout::test_long_poll_of_no_partitions_returns_at_once: sequential, // Timing-sensitive
        task_cap::test_long_poll_per_connection_task_cap: sequential, // Floods one connection; sensitive to concurrent produce wakeups
        timeout::test_long_poll_timeout: sequential, // Timing-sensitive
        wakeup::test_long_poll_consumer_receives_before_max_wait: sequential, // Tests timing-sensitive behavior
        wakeup::test_long_poll_sequential_produce_consume: sequential, // Tests timing-sensitive behavior
        edge_cases::test_long_poll_min_bytes_threshold: sequential, // Timing-sensitive
        edge_cases::test_long_poll_timeout_precision: sequential, // Timing-sensitive
        edge_cases::test_long_poll_consumer_disconnect,
        edge_cases::test_long_poll_multiple_consumers_same_partition: sequential, // Multiple consumers
        edge_cases::test_long_poll_auto_commit_interval: sequential, // Timing-sensitive
        transaction_end::test_transaction_end_wakes_parked_read_committed_fetch: sequential,
    ]
}
