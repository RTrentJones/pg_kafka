//! pg_kafka E2E test suite.
//!
//! The tests run against a live PostgreSQL with the extension loaded, through real
//! Kafka clients (rdkafka) and, where rdkafka hides what a test asserts on, the raw
//! protocol ([`wire`]). Each category is a module whose `tests()` lists its tests
//! ([`registry`]); [`all_tests`] joins them for the runner in `main.rs`.
//!
//! Shared infrastructure: [`common`] (client factories, result types), [`setup`]
//! ([`TestContext`]: unique names, cleanup), [`fixtures`] (builders, waits),
//! [`assertions`] (database assertions), [`members`] (bounded closes of group
//! consumers), [`error_codes`] and [`wire`].

// Infrastructure modules
pub mod assertions;
pub mod common;
pub mod error_codes;
pub mod fixtures;
pub mod members;
pub mod registry;
pub mod setup;
pub mod wire;

// Test modules
pub mod admin;
pub mod auth;
pub mod compression;
pub mod concurrent;
pub mod consumer;
pub mod consumer_group;
pub mod edge_cases;
pub mod error_paths;
pub mod idempotent;
pub mod long_poll;
pub mod metadata;
pub mod negative;
pub mod network;
pub mod offset_management;
pub mod part;
pub mod partition;
pub mod performance;
pub mod producer;
pub mod protocol;
pub mod shadow;
pub mod transaction;
pub mod worker;

// Re-export infrastructure
pub use assertions::*;
pub use fixtures::*;
pub use setup::TestContext;

/// Every registered test, category by category, in run order.
pub fn all_tests() -> Vec<registry::TestDef> {
    [
        admin::tests(),
        producer::tests(),
        consumer::tests(),
        offset_management::tests(),
        consumer_group::tests(),
        partition::tests(),
        error_paths::tests(),
        edge_cases::tests(),
        concurrent::tests(),
        negative::tests(),
        performance::tests(),
        long_poll::tests(),
        compression::tests(),
        idempotent::tests(),
        transaction::tests(),
        shadow::tests(),
        metadata::tests(),
        protocol::tests(),
        part::tests(),
        auth::tests(),
        network::tests(),
        worker::tests(),
    ]
    .into_iter()
    .flatten()
    .collect()
}
