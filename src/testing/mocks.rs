//! Mock implementations for pgrx-dependent types
//!
//! These mocks allow testing without the PostgreSQL runtime.

use crate::kafka::constants::{DEFAULT_KAFKA_PORT, DEFAULT_SHUTDOWN_TIMEOUT_MS, TEST_HOST};

/// Returns a mock Config with default test values
///
/// Avoids calling pgrx GUC functions which require Postgres runtime
pub fn mock_config() -> crate::config::Config {
    crate::config::Config {
        port: DEFAULT_KAFKA_PORT,
        host: TEST_HOST.to_string(),
        log_connections: false,
        shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT_MS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_config_defaults() {
        use crate::kafka::constants::{DEFAULT_KAFKA_PORT, DEFAULT_SHUTDOWN_TIMEOUT_MS, TEST_HOST};

        let config = mock_config();
        assert_eq!(config.port, DEFAULT_KAFKA_PORT);
        assert_eq!(config.host, TEST_HOST);
        assert!(!config.log_connections);
        assert_eq!(config.shutdown_timeout_ms, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }
}
