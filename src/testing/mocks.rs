//! Mock implementations for pgrx-dependent types
//!
//! These mocks allow testing without the PostgreSQL runtime.

use crate::kafka::constants::{DEFAULT_KAFKA_PORT, DEFAULT_SHUTDOWN_TIMEOUT_MS, TEST_HOST};
use crate::kafka::error::Result;
use crate::kafka::messages::Record;
use crate::kafka::storage::{CommittedOffset, FetchedMessage, KafkaStore, TopicMetadata};
use mockall::mock;
use mockall::predicate::*;

/// Mock implementation of KafkaStore trait using mockall
///
/// This auto-generated mock allows setting expectations on storage operations
/// without requiring a real database connection. Use this in handler tests
/// to isolate protocol logic from database concerns.
///
/// # Examples
///
/// ```no_run
/// use pg_kafka::testing::mocks::MockKafkaStore;
/// use mockall::predicate::*;
///
/// let mut mock = MockKafkaStore::new();
/// mock.expect_get_or_create_topic()
///     .with(eq("test-topic"))
///     .returning(|_| Ok(1));
/// ```
mock! {
    pub KafkaStore {}

    impl KafkaStore for KafkaStore {
        fn get_or_create_topic<'a>(&self, name: &'a str) -> Result<i32>;
        fn get_topic_metadata<'a>(&self, names: Option<&'a [String]>) -> Result<Vec<TopicMetadata>>;
        fn insert_records<'a>(&self, topic_id: i32, partition_id: i32, records: &'a [Record]) -> Result<i64>;
        fn fetch_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            fetch_offset: i64,
            max_bytes: i32,
        ) -> Result<Vec<FetchedMessage>>;
        fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
        fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
        fn commit_offset<'a>(
            &self,
            group_id: &'a str,
            topic_id: i32,
            partition_id: i32,
            offset: i64,
            metadata: Option<&'a str>,
        ) -> Result<()>;
        fn fetch_offset<'a>(
            &self,
            group_id: &'a str,
            topic_id: i32,
            partition_id: i32,
        ) -> Result<Option<CommittedOffset>>;
        fn fetch_all_offsets<'a>(&self, group_id: &'a str) -> Result<Vec<(String, i32, CommittedOffset)>>;
    }
}

// Note: Helper constructors for MockKafkaStore are intentionally simple
// to avoid lifetime issues with mockall predicates. Tests should set up
// expectations directly using expect_* methods.

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

    #[test]
    fn test_mock_store_creation() {
        // Verify MockKafkaStore can be instantiated
        let mut mock = MockKafkaStore::new();

        // Set up a simple expectation
        mock.expect_get_high_watermark().returning(|_, _| Ok(10));

        let result = mock.get_high_watermark(1, 0);
        assert_eq!(result.unwrap(), 10);
    }

    #[test]
    fn test_mock_store_with_expectation() {
        let mut mock = MockKafkaStore::new();

        // Test that expectations work correctly
        mock.expect_get_or_create_topic()
            .times(1)
            .returning(|_| Ok(1));

        let result = mock.get_or_create_topic("test-topic");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }
}
