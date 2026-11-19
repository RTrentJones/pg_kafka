// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Errors of shadow mode's own operations (producer creation, configuration,
//! the outbox tick's database work). Forward failures are not errors here:
//! they travel as [`super::ForwardOutcome`]s and end up in the outbox row.

use std::fmt;

/// Result type for shadow mode operations.
pub type ShadowResult<T> = Result<T, ShadowError>;

/// Errors that can occur during shadow mode operations.
#[derive(Debug)]
pub enum ShadowError {
    /// Shadow mode is not enabled.
    NotEnabled,
    /// Shadow mode is not properly configured (missing bootstrap servers, etc.).
    NotConfigured(String),
    /// rdkafka could not create the producer.
    ProducerError(String),
    /// An invalid shadow GUC value.
    ConfigError(String),
    /// A shadow SQL statement failed.
    DatabaseError(String),
}

impl fmt::Display for ShadowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShadowError::NotEnabled => write!(f, "Shadow mode is not enabled"),
            ShadowError::NotConfigured(msg) => write!(f, "Shadow mode not configured: {msg}"),
            ShadowError::ProducerError(msg) => write!(f, "Kafka producer error: {msg}"),
            ShadowError::ConfigError(msg) => write!(f, "Configuration error: {msg}"),
            ShadowError::DatabaseError(msg) => write!(f, "Database error: {msg}"),
        }
    }
}

impl std::error::Error for ShadowError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shadow_error_display() {
        for (err, expected) in [
            (ShadowError::NotEnabled, "Shadow mode is not enabled"),
            (
                ShadowError::NotConfigured("missing bootstrap servers".to_string()),
                "Shadow mode not configured: missing bootstrap servers",
            ),
            (
                ShadowError::ProducerError("queue full".to_string()),
                "Kafka producer error: queue full",
            ),
            (
                ShadowError::ConfigError("invalid protocol".to_string()),
                "Configuration error: invalid protocol",
            ),
            (
                ShadowError::DatabaseError("connection lost".to_string()),
                "Database error: connection lost",
            ),
        ] {
            assert_eq!(err.to_string(), expected);
            // ShadowError is a std error, so `?` can box it.
            let boxed: Box<dyn std::error::Error> = Box::new(err);
            assert_eq!(boxed.to_string(), expected);
        }
    }
}
