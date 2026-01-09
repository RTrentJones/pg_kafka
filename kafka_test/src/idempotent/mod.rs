//! Idempotent Producer E2E tests (Phase 9)
//!
//! Tests for idempotent producer functionality including:
//! - InitProducerId API
//! - Idempotent message delivery
//! - Producer ID and sequence verification in database

mod basic;

pub use basic::test_idempotent_producer_basic;
