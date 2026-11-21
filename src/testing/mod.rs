//! Test doubles and fixtures for unit tests that cannot use the PostgreSQL runtime.
//!
//! - [`mocks::MockKafkaStore`]: a storage double with mockall expectations.
//! - [`expect_topics`]: answer the store's batched topic lookups from a per-name function.
//! - [`TestEnv`]: the coordinator and broker metadata a [`HandlerContext`] borrows.
//! - [`cpu_time`]: how long a closure ran on the CPU, for tests that bound a cost.
//! - [`growth`]: how a closure's CPU time grows with its input, for tests that bound a
//!   cost's growth.
//! - [`heap_peak`]: the most heap memory a closure held at once, for tests that bound
//!   what a decode holds.

#![cfg(test)]

pub mod heap;
pub mod mocks;

pub use heap::heap_peak;

use crate::kafka::error::Result;
use crate::kafka::storage::{KafkaStore, TopicMetadata};
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

/// How much longer `run(4 * n)` takes on the CPU than `run(n)`, each the best of three runs:
/// about 4 for work linear in its size, about 16 for quadratic work. Tests that bound how a
/// cost grows assert it stays well under 16.
pub fn growth(n: usize, mut run: impl FnMut(usize)) -> f64 {
    let mut best = |size: usize| {
        (0..3)
            .map(|_| cpu_time(|| run(size)).1)
            .min()
            .unwrap_or_default()
            .max(Duration::from_micros(1))
    };
    let small = best(n);
    let large = best(4 * n);
    large.as_secs_f64() / small.as_secs_f64()
}

fn thread_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    let nanos = stat.split_whitespace().next()?.parse().ok()?;
    Some(Duration::from_nanos(nanos))
}

/// Answer `store`'s lookups of named topics (`get_topic_metadata(Some(..))`, one statement
/// for all the names) from `topic`: a name's `(topic id, partition count)`, `None` for a
/// topic that does not exist. An `Err` for any name fails the whole lookup, as the
/// statement would.
pub fn expect_topics(
    store: &mut mocks::MockKafkaStore,
    topic: impl Fn(&str) -> Result<Option<(i32, i32)>> + Send + 'static,
) {
    store.expect_get_topic_metadata().returning(move |names| {
        let mut found = Vec::new();
        for name in names.unwrap_or_default() {
            if let Some((id, partition_count)) = topic(name)? {
                found.push(TopicMetadata {
                    id,
                    name: name.clone(),
                    partition_count,
                });
            }
        }
        Ok(found)
    });
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
