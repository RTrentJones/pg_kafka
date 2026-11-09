//! Error path E2E tests
//!
//! Tests for error conditions and edge cases in Kafka protocol handling.
//! These tests verify that pg_kafka returns appropriate error codes
//! when given invalid inputs.

pub(crate) mod bug_hunt;
mod consumer_errors;
mod coordinator_errors;
mod fetch_errors;
mod offset_errors;
mod producer_errors;
mod review_regressions;

pub use bug_hunt::*;
pub use consumer_errors::*;
pub use coordinator_errors::*;
pub use fetch_errors::*;
pub use offset_errors::*;
pub use producer_errors::*;
pub use review_regressions::*;
