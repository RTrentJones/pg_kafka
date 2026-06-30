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
//! Shadow Mode is a paid feature for production use, with evaluation mode
//! available for development and testing.
//!
//! ## This is an honor-system "freemium" nag, not an enforcement gate
//!
//! By design, Shadow Mode is **never blocked** — it logs and warns, but always
//! forwards. The license key is a registered-sponsor marker, not a cryptographic
//! credential: the code performs a **format check only** and does not (and is not
//! intended to) verify a signature. The deterrent is the recurring log warning and
//! the compliance/audit exposure it creates for an organization large enough to
//! care, not a technical lockout. Do not add a signing/enforcement gate here.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// License validation result
#[derive(Debug, Clone, PartialEq)]
pub enum LicenseStatus {
    /// A registered sponsor key (`sponsor_id:token`) was supplied. This is a
    /// well-formed marker on the honor system — its format is checked but no
    /// signature is verified, so it silences the nag without unlocking anything
    /// gated (nothing is gated). See the module docs.
    Registered,
    /// Evaluation mode
    Evaluation,
    /// No license key provided
    Unlicensed,
    /// Malformed license key (wrong shape for a `sponsor_id:token` marker)
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

    /// Classify a license key by shape.
    ///
    /// This is a deliberate **format check only** — no signature is verified
    /// (see the module docs on the freemium honor system). Any non-empty
    /// `sponsor_id:token` pair is accepted as a registered-sponsor marker.
    fn validate_key(key: &str) -> LicenseStatus {
        if key.is_empty() {
            return LicenseStatus::Unlicensed;
        }

        // Special "eval" key for evaluation
        if key == "eval" || key == "evaluation" {
            return LicenseStatus::Evaluation;
        }

        // Shape: "sponsor_id:token" — both halves non-empty. Intentionally not
        // cryptographically verified; this only silences the recurring nag.
        if let Some((sponsor_id, token)) = key.split_once(':') {
            if !sponsor_id.is_empty() && !token.is_empty() {
                return LicenseStatus::Registered;
            }
        }

        LicenseStatus::Invalid("Invalid format. Expected 'sponsor_id:token'".to_string())
    }

    /// Check license and emit rate-limited warnings
    /// Uses crate::pg_warning! macro for PostgreSQL log integration
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
            LicenseStatus::Registered => {} // Silent — registered sponsor marker
            LicenseStatus::Evaluation => {
                crate::pg_warning!(
                    "Shadow Mode running in EVALUATION mode. \
                     Production use requires a license: https://github.com/sponsors/RTrentJones"
                );
            }
            LicenseStatus::Unlicensed => {
                crate::pg_warning!(
                    "SHADOW MODE ACTIVE WITHOUT LICENSE - \
                     This is a paid feature for production use. \
                     Set pg_kafka.shadow_license_key = 'eval' for evaluation, \
                     or obtain a license: https://github.com/sponsors/RTrentJones"
                );
            }
            LicenseStatus::Invalid(reason) => {
                crate::pg_warning!(
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
            LicenseStatus::Registered | LicenseStatus::Evaluation
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
        assert_eq!(v.status(), &LicenseStatus::Registered);
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
        let status = LicenseStatus::Registered;
        let cloned = status.clone();
        assert_eq!(cloned, LicenseStatus::Registered);

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
        let registered = LicenseStatus::Registered;
        assert!(format!("{:?}", registered).contains("Registered"));

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
        assert_eq!(LicenseStatus::Registered, LicenseStatus::Registered);
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
        assert_ne!(LicenseStatus::Registered, LicenseStatus::Evaluation);
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
        assert_eq!(v1.status(), &LicenseStatus::Registered);

        let v2 = LicenseValidator::new("sponsor-123:sig-abc-def-456");
        assert_eq!(v2.status(), &LicenseStatus::Registered);

        let v3 = LicenseValidator::new("user@company.com:signature123");
        assert_eq!(v3.status(), &LicenseStatus::Registered);
    }

    #[test]
    fn test_invalid_license_reason_message() {
        let v = LicenseValidator::new("invalid");
        if let LicenseStatus::Invalid(reason) = v.status() {
            assert!(reason.contains("sponsor_id:token"));
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

    #[test]
    fn test_registered_key_check_is_silent() {
        // A registered-sponsor marker takes the silent arm of check_and_warn:
        // it must not emit a warning (and must not panic). This is the whole
        // point of the freemium honor system — a well-formed key quiets the nag.
        let v = LicenseValidator::new("sponsor:token");
        assert_eq!(v.status(), &LicenseStatus::Registered);
        // Exercises the `Registered => {}` arm; the other arms call pg_warning!,
        // which requires the Postgres runtime, so only the silent path is safe
        // to drive from a plain unit test.
        v.check_and_warn();
    }
}
