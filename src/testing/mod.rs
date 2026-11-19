//! Test doubles for unit tests that cannot use the PostgreSQL runtime.
//!
//! - [`mocks::MockKafkaStore`]: a storage double with mockall expectations.

#![cfg(test)]

pub mod mocks;
