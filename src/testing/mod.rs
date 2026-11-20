//! Test doubles and fixtures for unit tests that cannot use the PostgreSQL runtime.
//!
//! - [`mocks::MockKafkaStore`]: a storage double with mockall expectations.
//! - [`TestEnv`]: the coordinator and broker metadata a [`HandlerContext`] borrows.
//! - [`cpu_time`]: how long a closure ran on the CPU, for tests that bound a cost.

#![cfg(test)]

pub mod mocks;

use crate::kafka::storage::KafkaStore;
use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
use kafka_protocol::records::Compression;
use std::time::{Duration, Instant};

/// Run `f` and return its result with the time the calling thread spent on the CPU.
///
/// Tests that bound an algorithm's cost measure this instead of wall-clock time, which
/// grows with whatever else the machine runs. On Linux it is the thread's run time from
/// `/proc/thread-self/schedstat`; elsewhere it falls back to wall-clock time.
pub fn cpu_time<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let before = thread_cpu();
    let started = Instant::now();
    let result = f();
    let spent = match (before, thread_cpu()) {
        (Some(before), Some(after)) => after.saturating_sub(before),
        _ => started.elapsed(),
    };
    (result, spent)
}

fn thread_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    let nanos = stat.split_whitespace().next()?.parse().ok()?;
    Some(Duration::from_nanos(nanos))
}

/// What a handler test needs besides its store: a fresh coordinator and broker
/// metadata for `localhost:9092`. Build it after the store's expectations are set and
/// borrow a context from it with [`TestEnv::ctx`].
pub struct TestEnv {
    pub coordinator: GroupCoordinator,
    pub broker: BrokerMetadata,
}

impl TestEnv {
    pub fn new() -> Self {
        Self {
            coordinator: GroupCoordinator::new(),
            broker: BrokerMetadata::new("localhost".to_string(), 9092),
        }
    }

    /// A context over `store` with one default partition and no compression.
    pub fn ctx<'a>(&'a self, store: &'a dyn KafkaStore) -> HandlerContext<'a> {
        HandlerContext::new(store, &self.coordinator, &self.broker, 1, Compression::None)
    }
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}
