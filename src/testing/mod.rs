//! Test doubles and fixtures for unit tests that cannot use the PostgreSQL runtime.
//!
//! - [`mocks::MockKafkaStore`]: a storage double with mockall expectations.
//! - [`TestEnv`]: the coordinator and broker metadata a [`HandlerContext`] borrows.

#![cfg(test)]

pub mod mocks;

use crate::kafka::storage::KafkaStore;
use crate::kafka::{BrokerMetadata, GroupCoordinator, HandlerContext};
use kafka_protocol::records::Compression;

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
