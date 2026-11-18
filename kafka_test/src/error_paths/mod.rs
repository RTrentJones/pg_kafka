//! Error path E2E tests
//!
//! Tests for error conditions and edge cases in Kafka protocol handling.
//! These tests verify that pg_kafka returns appropriate error codes
//! when given invalid inputs.

pub(crate) mod bug_hunt;
mod consumer_errors;
mod coordinator_errors;
mod fetch_errors;
mod lifecycle_tests;
mod listener_tests;
mod metrics_tests;
mod offset_errors;
mod producer_errors;
mod review_regressions;
mod storage_tests;

pub use bug_hunt::*;
pub use consumer_errors::*;
pub use coordinator_errors::*;
pub use fetch_errors::*;
pub use lifecycle_tests::*;
pub use listener_tests::*;
pub use metrics_tests::*;
pub use offset_errors::*;
pub use producer_errors::*;
pub use review_regressions::*;
pub use storage_tests::*;
