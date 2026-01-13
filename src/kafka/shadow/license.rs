// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Shadow mode license validation
//!
//! This module provides license key validation for Shadow Mode commercial licensing.
//! Shadow Mode is a paid feature for production use, with evaluation mode available
//! for development and testing.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// License validation result
#[derive(Debug, Clone, PartialEq)]
pub enum LicenseStatus {
    /// Valid commercial license
    Valid,
    /// Evaluation mode
    Evaluation,
    /// No license key provided
    Unlicensed,
    /// Invalid license key format
    Invalid(String),
}

/// License validator for Shadow Mode with rate-limited warnings
pub struct LicenseValidator {
    status: LicenseStatus,
    /// Epoch seconds of last warning (0 = never warned)
    last_warning_epoch: AtomicU64,
}

impl LicenseValidator {
    /// Warning interval: 1 hour
    const WARNING_INTERVAL_SECS: u64 = 3600;

    /// Create new validator from license key
    pub fn new(license_key: &str) -> Self {
        let status = Self::validate_key(license_key);
        Self {
            status,
            last_warning_epoch: AtomicU64::new(0),
        }
    }

    /// Validate license key format
    fn validate_key(key: &str) -> LicenseStatus {
        if key.is_empty() {
            return LicenseStatus::Unlicensed;
        }

        // Special "eval" key for evaluation
        if key == "eval" || key == "evaluation" {
            return LicenseStatus::Evaluation;
        }

        // Format: "sponsor_id:signature"
        // MVP: Just check format. Future: ed25519 signature verification
        if let Some((sponsor_id, signature)) = key.split_once(':') {
            if !sponsor_id.is_empty() && !signature.is_empty() {
                return LicenseStatus::Valid;
            }
        }

        LicenseStatus::Invalid("Invalid format. Expected 'sponsor_id:signature'".to_string())
    }

    /// Check license and emit rate-limited warnings
    /// Uses pgrx::warning! macro for PostgreSQL log integration
    pub fn check_and_warn(&self) {
        // Get current epoch seconds
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let last = self.last_warning_epoch.load(Ordering::Relaxed);
        let should_warn = last == 0 || now.saturating_sub(last) >= Self::WARNING_INTERVAL_SECS;

        if !should_warn {
            return;
        }

        // Try to update atomically (CAS to prevent duplicate warnings)
        if self
            .last_warning_epoch
            .compare_exchange(last, now, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return; // Another thread beat us
        }

        match &self.status {
            LicenseStatus::Valid => {} // Silent
            LicenseStatus::Evaluation => {
                pgrx::warning!(
                    "Shadow Mode running in EVALUATION mode. \
                     Production use requires a license: https://github.com/sponsors/RTrentJones"
                );
            }
            LicenseStatus::Unlicensed => {
                pgrx::warning!(
                    "SHADOW MODE ACTIVE WITHOUT LICENSE - \
                     This is a paid feature for production use. \
                     Set pg_kafka.shadow_license_key = 'eval' for evaluation, \
                     or obtain a license: https://github.com/sponsors/RTrentJones"
                );
            }
            LicenseStatus::Invalid(reason) => {
                pgrx::warning!(
                    "Invalid Shadow Mode license key: {}. \
                     Visit https://github.com/sponsors/RTrentJones for licensing.",
                    reason
                );
            }
        }
    }

    /// Get current license status
    pub fn status(&self) -> &LicenseStatus {
        &self.status
    }

    /// Check if licensed (Valid or Evaluation)
    #[allow(dead_code)]
    pub fn is_licensed(&self) -> bool {
        matches!(
            self.status,
            LicenseStatus::Valid | LicenseStatus::Evaluation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_key() {
        let v = LicenseValidator::new("");
        assert_eq!(v.status(), &LicenseStatus::Unlicensed);
        assert!(!v.is_licensed());
    }

    #[test]
    fn test_eval_key() {
        let v = LicenseValidator::new("eval");
        assert_eq!(v.status(), &LicenseStatus::Evaluation);
        assert!(v.is_licensed());
    }

    #[test]
    fn test_evaluation_key() {
        let v = LicenseValidator::new("evaluation");
        assert_eq!(v.status(), &LicenseStatus::Evaluation);
    }

    #[test]
    fn test_valid_format() {
        let v = LicenseValidator::new("sponsor_12345:abc123def456");
        assert_eq!(v.status(), &LicenseStatus::Valid);
        assert!(v.is_licensed());
    }

    #[test]
    fn test_invalid_format_no_colon() {
        let v = LicenseValidator::new("invalid-key");
        assert!(matches!(v.status(), LicenseStatus::Invalid(_)));
        assert!(!v.is_licensed());
    }

    #[test]
    fn test_invalid_format_empty_parts() {
        let v = LicenseValidator::new(":signature");
        assert!(matches!(v.status(), LicenseStatus::Invalid(_)));

        let v2 = LicenseValidator::new("sponsor:");
        assert!(matches!(v2.status(), LicenseStatus::Invalid(_)));
    }
}
