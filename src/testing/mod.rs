//! Testing utilities for pg_kafka
//!
//! This module provides test infrastructure that works around pgrx's requirement
//! for the PostgreSQL runtime. It's only compiled when running tests.
//!
//! # Organization
//! - `mocks.rs` - Mock implementations of pgrx-dependent types
//! - `helpers.rs` - Test helper functions for creating requests/responses
//!
//! # Logging Macros
//! The conditional logging macros (`pg_log!`, `pg_warning!`) are defined in src/lib.rs
//! and are available throughout the codebase.

#![cfg(test)]

pub mod helpers;
pub mod mocks;

// Re-export commonly used items
pub use helpers::{mock_api_versions_request, mock_metadata_request};
pub use mocks::mock_config;
