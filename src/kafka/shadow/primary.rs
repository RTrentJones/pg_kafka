// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Primary detection for shadow mode
//!
//! This module provides utilities to detect whether the current PostgreSQL
//! instance is a primary (leader) or standby (replica). Shadow mode only
//! forwards messages when running on the primary to prevent duplicates.
//!
//! ## How it works
//!
//! PostgreSQL has built-in replication awareness via `pg_is_in_recovery()`:
//! - On primary: returns `false` (not in recovery mode)
//! - On standby: returns `true` (in recovery, replaying WAL)
//!
//! Standbys are read-only and don't receive write requests, so they naturally
//! don't need to forward. This is simpler than custom leader election.

/// Check if the current PostgreSQL instance is a primary (not a standby)
///
/// Returns `true` if this is a primary instance that should forward shadow messages.
/// Returns `false` if this is a standby/replica that should skip forwarding.
///
/// # Implementation
///
/// Uses PostgreSQL's `pg_is_in_recovery()` function:
/// - Primary (not in recovery) → returns `true` (is_primary = true)
/// - Standby (in recovery) → returns `false` (is_primary = false)
///
/// # Note
///
/// This function requires a database connection via SPI. It should be called
/// from the background worker thread that has an active SPI connection.
#[cfg(not(test))]
pub fn is_primary() -> bool {
    use pgrx::prelude::*;

    // pg_is_in_recovery() returns TRUE on standby, FALSE on primary
    // So we negate it: is_primary = NOT pg_is_in_recovery()
    let result = Spi::get_one::<bool>("SELECT NOT pg_is_in_recovery()");

    match result {
        Ok(Some(is_primary)) => is_primary,
        Ok(None) => {
            // NULL result is unexpected, assume primary to be safe
            tracing::warn!("pg_is_in_recovery() returned NULL, assuming primary");
            true
        }
        Err(e) => {
            // Error checking recovery state, assume primary to avoid silent failures
            tracing::error!(
                "Failed to check pg_is_in_recovery(): {:?}, assuming primary",
                e
            );
            true
        }
    }
}

/// Test version that always returns true (is primary)
#[cfg(test)]
pub fn is_primary() -> bool {
    true
}

/// Cached primary status to avoid repeated SPI calls
///
/// The recovery state doesn't change during a session (a standby doesn't
/// suddenly become primary), so we can cache the result.
pub struct PrimaryStatus {
    /// Cached result of is_primary() check
    is_primary: Option<bool>,
}

impl PrimaryStatus {
    /// Create a new uncached primary status
    pub const fn new() -> Self {
        Self { is_primary: None }
    }

    /// Check if this is a primary, caching the result
    ///
    /// First call performs the SPI query, subsequent calls return cached value.
    pub fn check(&mut self) -> bool {
        if let Some(cached) = self.is_primary {
            return cached;
        }

        let result = is_primary();
        self.is_primary = Some(result);
        result
    }

    /// Force a refresh of the primary status
    ///
    /// Useful if the application suspects a failover occurred.
    pub fn refresh(&mut self) -> bool {
        self.is_primary = None;
        self.check()
    }

    /// Check if status has been cached
    pub fn is_cached(&self) -> bool {
        self.is_primary.is_some()
    }
}

impl Default for PrimaryStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_primary_mock() {
        // In test mode, is_primary() always returns true
        assert!(is_primary());
    }

    #[test]
    fn test_primary_status_caching() {
        let mut status = PrimaryStatus::new();

        // Initially not cached
        assert!(!status.is_cached());

        // First check caches the result
        let result1 = status.check();
        assert!(status.is_cached());

        // Second check returns cached value
        let result2 = status.check();
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_primary_status_refresh() {
        let mut status = PrimaryStatus::new();

        // Cache a value
        status.check();
        assert!(status.is_cached());

        // Refresh clears and re-checks
        status.refresh();
        assert!(status.is_cached()); // Re-cached after refresh
    }

    #[test]
    fn test_primary_status_default() {
        let status = PrimaryStatus::default();
        assert!(!status.is_cached());
    }
}
