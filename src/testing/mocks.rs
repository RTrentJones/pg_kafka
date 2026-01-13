//! Mock implementations for pgrx-dependent types
//!
//! These mocks allow testing without the PostgreSQL runtime.

use crate::kafka::constants::{
    DEFAULT_COMPRESSION_TYPE, DEFAULT_DATABASE, DEFAULT_KAFKA_PORT, DEFAULT_SHUTDOWN_TIMEOUT_MS,
    DEFAULT_TOPIC_PARTITIONS, TEST_HOST,
};
use crate::kafka::error::Result;
use crate::kafka::messages::Record;
use crate::kafka::storage::{
    CommittedOffset, FetchedMessage, IsolationLevel, KafkaStore, TopicMetadata, TransactionState,
};
use mockall::mock;
use mockall::predicate::*;
use std::time::Duration;

// MockKafkaStore: Auto-generated mock for KafkaStore trait.
// This allows setting expectations on storage operations without requiring
// a real database connection. Use this in handler tests to isolate protocol
// logic from database concerns.
mock! {
    pub KafkaStore {}

    impl KafkaStore for KafkaStore {
        fn get_or_create_topic<'a>(&self, name: &'a str, default_partitions: i32) -> Result<(i32, i32)>;
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
        // Admin Topic Operations (Phase 6)
        fn topic_exists<'a>(&self, name: &'a str) -> Result<bool>;
        fn create_topic<'a>(&self, name: &'a str, partition_count: i32) -> Result<i32>;
        fn get_topic_id<'a>(&self, name: &'a str) -> Result<Option<i32>>;
        fn delete_topic(&self, topic_id: i32) -> Result<()>;
        fn get_topic_partition_count<'a>(&self, name: &'a str) -> Result<Option<i32>>;
        fn set_topic_partition_count<'a>(&self, name: &'a str, partition_count: i32) -> Result<()>;
        // Admin Consumer Group Operations (Phase 6)
        fn delete_consumer_group_offsets<'a>(&self, group_id: &'a str) -> Result<()>;
        // Idempotent Producer Operations (Phase 9)
        fn allocate_producer_id<'a>(
            &self,
            client_id: Option<&'a str>,
            transactional_id: Option<&'a str>,
        ) -> Result<(i64, i16)>;
        fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>>;
        fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16>;
        fn check_and_update_sequence(
            &self,
            producer_id: i64,
            producer_epoch: i16,
            topic_id: i32,
            partition_id: i32,
            base_sequence: i32,
            record_count: i32,
        ) -> Result<bool>;
        // Transaction Operations (Phase 10)
        fn get_or_create_transactional_producer<'a>(
            &self,
            transactional_id: &'a str,
            transaction_timeout_ms: i32,
            client_id: Option<&'a str>,
        ) -> Result<(i64, i16)>;
        fn begin_transaction<'a>(
            &self,
            transactional_id: &'a str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> Result<()>;
        fn validate_transaction<'a>(
            &self,
            transactional_id: &'a str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> Result<()>;
        fn insert_transactional_records<'a>(
            &self,
            topic_id: i32,
            partition_id: i32,
            records: &'a [Record],
            producer_id: i64,
            producer_epoch: i16,
        ) -> Result<i64>;
        fn store_txn_pending_offset<'a>(
            &self,
            transactional_id: &'a str,
            group_id: &'a str,
            topic_id: i32,
            partition_id: i32,
            offset: i64,
            metadata: Option<&'a str>,
        ) -> Result<()>;
        fn commit_transaction<'a>(
            &self,
            transactional_id: &'a str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> Result<()>;
        fn abort_transaction<'a>(
            &self,
            transactional_id: &'a str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> Result<()>;
        fn get_transaction_state<'a>(&self, transactional_id: &'a str) -> Result<Option<TransactionState>>;
        fn fetch_records_with_isolation(
            &self,
            topic_id: i32,
            partition_id: i32,
            fetch_offset: i64,
            max_bytes: i32,
            isolation_level: IsolationLevel,
        ) -> Result<Vec<FetchedMessage>>;
        fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
        fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>>;
        fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64>;
    }
}

// Note: Helper constructors for MockKafkaStore are intentionally simple
// to avoid lifetime issues with mockall predicates. Tests should set up
// expectations directly using expect_* methods.

/// Returns a mock Config with default test values
///
/// Avoids calling pgrx GUC functions which require Postgres runtime
pub fn mock_config() -> crate::config::Config {
    use crate::kafka::constants::{
        DEFAULT_FETCH_POLL_INTERVAL_MS, DEFAULT_SHADOW_BATCH_SIZE,
        DEFAULT_SHADOW_BOOTSTRAP_SERVERS, DEFAULT_SHADOW_LINGER_MS, DEFAULT_SHADOW_MAX_RETRIES,
        DEFAULT_SHADOW_METRICS_ENABLED, DEFAULT_SHADOW_MODE_ENABLED, DEFAULT_SHADOW_OTEL_ENDPOINT,
        DEFAULT_SHADOW_RETRY_BACKOFF_MS, DEFAULT_SHADOW_SASL_MECHANISM,
        DEFAULT_SHADOW_SECURITY_PROTOCOL, DEFAULT_SHADOW_SYNC_MODE,
    };
    crate::config::Config {
        port: DEFAULT_KAFKA_PORT,
        host: TEST_HOST.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        log_connections: false,
        shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT_MS,
        default_partitions: DEFAULT_TOPIC_PARTITIONS,
        fetch_poll_interval_ms: DEFAULT_FETCH_POLL_INTERVAL_MS,
        enable_long_polling: true,
        compression_type: DEFAULT_COMPRESSION_TYPE.to_string(),
        log_timing: false,
        // Shadow mode configuration (Phase 11)
        shadow_mode_enabled: DEFAULT_SHADOW_MODE_ENABLED,
        shadow_bootstrap_servers: DEFAULT_SHADOW_BOOTSTRAP_SERVERS.to_string(),
        shadow_security_protocol: DEFAULT_SHADOW_SECURITY_PROTOCOL.to_string(),
        shadow_sasl_mechanism: DEFAULT_SHADOW_SASL_MECHANISM.to_string(),
        shadow_sasl_username: String::new(),
        shadow_sasl_password: String::new(),
        shadow_ssl_ca_location: String::new(),
        shadow_batch_size: DEFAULT_SHADOW_BATCH_SIZE,
        shadow_linger_ms: DEFAULT_SHADOW_LINGER_MS,
        shadow_retry_backoff_ms: DEFAULT_SHADOW_RETRY_BACKOFF_MS,
        shadow_max_retries: DEFAULT_SHADOW_MAX_RETRIES,
        shadow_default_sync_mode: DEFAULT_SHADOW_SYNC_MODE.to_string(),
        shadow_metrics_enabled: DEFAULT_SHADOW_METRICS_ENABLED,
        shadow_otel_endpoint: DEFAULT_SHADOW_OTEL_ENDPOINT.to_string(),
        // Tests run in eval mode by default (Commercial License)
        shadow_license_key: "eval".to_string(),
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
        // Returns (topic_id, partition_count)
        mock.expect_get_or_create_topic()
            .times(1)
            .returning(|_, _| Ok((1, 1)));

        let result = mock.get_or_create_topic("test-topic", 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (1, 1));
    }

    #[test]
    fn test_mock_config_has_default_partitions() {
        let config = mock_config();
        assert_eq!(config.default_partitions, DEFAULT_TOPIC_PARTITIONS);
    }
}
