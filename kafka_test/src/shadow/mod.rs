//! Shadow mode E2E tests (Phase 11)
//!
//! This module contains end-to-end tests for pg_kafka's shadow mode functionality,
//! which enables forwarding messages to an external Kafka cluster.
//!
//! ## Test Categories
//!
//! - **Basic Forwarding**: DualWrite/ExternalOnly modes with sync/async
//! - **Percentage Routing**: 0%, 50%, 100% routing with murmur2 hash
//! - **Dial-Up Tests**: High-volume tests (500 messages) at various percentages
//! - **Topic Mapping**: External topic name mapping
//! - **Transaction Integration**: Commit/abort behavior with shadow mode
//! - **Error Handling**: External Kafka down/recovery
//! - **Replay**: Historical message replay
//!
//! ## Requirements
//!
//! These tests require an external Kafka broker running on port 9093.
//! Use `docker-compose up -d` to start both pg_kafka and external Kafka.

pub mod assertions;
pub mod basic_forwarding;
pub mod dialup;
pub mod error_handling;
pub mod external_client;
pub mod helpers;
pub mod percentage_routing;
pub mod replay;
pub mod topic_mapping;
pub mod transaction_integration;

// Re-export test functions for registration in main.rs
pub use basic_forwarding::{
    test_dual_write_async, test_dual_write_sync, test_external_only_mode, test_local_only_mode,
};
pub use dialup::{
    test_dialup_0_percent, test_dialup_100_percent, test_dialup_10_percent, test_dialup_25_percent,
    test_dialup_50_percent, test_dialup_75_percent,
};
pub use error_handling::{test_dual_write_external_down, test_external_only_fallback};
pub use percentage_routing::{
    test_deterministic_routing, test_fifty_percent_forwarding, test_hundred_percent_forwarding,
    test_zero_percent_forwarding,
};
pub use replay::test_replay_historical_messages;
pub use topic_mapping::test_topic_name_mapping;
pub use transaction_integration::{
    test_aborted_transaction_not_forwarded, test_committed_transaction_forwarded,
};
