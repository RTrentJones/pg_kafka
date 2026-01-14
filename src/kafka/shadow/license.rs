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

    // ========== Additional Coverage Tests ==========

    #[test]
    fn test_license_status_clone() {
        let status = LicenseStatus::Valid;
        let cloned = status.clone();
        assert_eq!(cloned, LicenseStatus::Valid);

        let eval = LicenseStatus::Evaluation;
        assert_eq!(eval.clone(), LicenseStatus::Evaluation);

        let unlicensed = LicenseStatus::Unlicensed;
        assert_eq!(unlicensed.clone(), LicenseStatus::Unlicensed);

        let invalid = LicenseStatus::Invalid("reason".to_string());
        let invalid_cloned = invalid.clone();
        assert!(matches!(invalid_cloned, LicenseStatus::Invalid(ref s) if s == "reason"));
    }

    #[test]
    fn test_license_status_debug_format() {
        let valid = LicenseStatus::Valid;
        assert!(format!("{:?}", valid).contains("Valid"));

        let eval = LicenseStatus::Evaluation;
        assert!(format!("{:?}", eval).contains("Evaluation"));

        let unlicensed = LicenseStatus::Unlicensed;
        assert!(format!("{:?}", unlicensed).contains("Unlicensed"));

        let invalid = LicenseStatus::Invalid("test reason".to_string());
        assert!(format!("{:?}", invalid).contains("Invalid"));
        assert!(format!("{:?}", invalid).contains("test reason"));
    }

    #[test]
    fn test_license_status_equality() {
        assert_eq!(LicenseStatus::Valid, LicenseStatus::Valid);
        assert_eq!(LicenseStatus::Evaluation, LicenseStatus::Evaluation);
        assert_eq!(LicenseStatus::Unlicensed, LicenseStatus::Unlicensed);
        assert_eq!(
            LicenseStatus::Invalid("a".to_string()),
            LicenseStatus::Invalid("a".to_string())
        );
        assert_ne!(
            LicenseStatus::Invalid("a".to_string()),
            LicenseStatus::Invalid("b".to_string())
        );
        assert_ne!(LicenseStatus::Valid, LicenseStatus::Evaluation);
    }

    #[test]
    fn test_validator_warning_interval_constant() {
        // Verify the warning interval is 1 hour
        assert_eq!(LicenseValidator::WARNING_INTERVAL_SECS, 3600);
    }

    #[test]
    fn test_validator_last_warning_starts_at_zero() {
        let v = LicenseValidator::new("eval");
        // The last_warning_epoch should start at 0 (never warned)
        assert_eq!(
            v.last_warning_epoch
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_valid_license_various_formats() {
        // Any format with non-empty sponsor_id and signature should be valid
        let v1 = LicenseValidator::new("a:b");
        assert_eq!(v1.status(), &LicenseStatus::Valid);

        let v2 = LicenseValidator::new("sponsor-123:sig-abc-def-456");
        assert_eq!(v2.status(), &LicenseStatus::Valid);

        let v3 = LicenseValidator::new("user@company.com:signature123");
        assert_eq!(v3.status(), &LicenseStatus::Valid);
    }

    #[test]
    fn test_invalid_license_reason_message() {
        let v = LicenseValidator::new("invalid");
        if let LicenseStatus::Invalid(reason) = v.status() {
            assert!(reason.contains("sponsor_id:signature"));
        } else {
            panic!("Expected Invalid status");
        }
    }

    #[test]
    fn test_is_licensed_returns_true_for_valid() {
        let v = LicenseValidator::new("sponsor:sig");
        assert!(v.is_licensed());
    }

    #[test]
    fn test_is_licensed_returns_true_for_evaluation() {
        let v = LicenseValidator::new("eval");
        assert!(v.is_licensed());

        let v2 = LicenseValidator::new("evaluation");
        assert!(v2.is_licensed());
    }

    #[test]
    fn test_is_licensed_returns_false_for_unlicensed() {
        let v = LicenseValidator::new("");
        assert!(!v.is_licensed());
    }

    #[test]
    fn test_is_licensed_returns_false_for_invalid() {
        let v = LicenseValidator::new("bad-format");
        assert!(!v.is_licensed());
    }
}
