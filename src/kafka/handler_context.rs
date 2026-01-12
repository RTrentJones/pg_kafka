//! Handler context for unified parameter passing
//!
//! This module provides HandlerContext, a struct that groups common parameters
//! passed to Kafka protocol handlers. This standardizes handler signatures and
//! makes it easier to add new context without changing every handler signature.
//!
//! ## Motivation
//!
//! Previously, handlers had inconsistent parameter ordering and varying numbers
//! of parameters (3-7 params). Adding new context (like logging or metrics)
//! required updating every handler signature and call site.
//!
//! HandlerContext provides:
//! - **Consistency:** All handlers accept `&HandlerContext` as first parameter
//! - **Extensibility:** Add new context fields without touching handler signatures
//! - **Clarity:** Separates "shared context" from "request-specific data"

use crossbeam_channel::Sender;
use kafka_protocol::records::Compression;

use crate::kafka::broker_metadata::BrokerMetadata;
use crate::kafka::coordinator::GroupCoordinator;
use crate::kafka::notifications::InternalNotification;
use crate::kafka::storage::KafkaStore;

/// Shared context passed to all Kafka protocol handlers
///
/// Contains resources and configuration that handlers need but don't own.
/// All fields are references or lightweight copies to make cloning cheap.
///
/// # Lifetime
///
/// The `'a` lifetime ensures that HandlerContext doesn't outlive the resources
/// it references (store, coordinator, etc.). In practice, HandlerContext is
/// created per-request and lives only for the duration of handler execution.
///
/// # Usage
///
/// ```rust,ignore
/// let ctx = HandlerContext::new(
///     store,
///     &coordinator,
///     &broker_metadata,
///     default_partitions,
///     compression,
/// );
///
/// // Pass to handler
/// let response = handlers::handle_produce(&ctx, topic_data, None, None)?;
/// ```
pub struct HandlerContext<'a> {
    /// Storage backend for reading/writing Kafka data
    ///
    /// Uses trait object to support different implementations (PostgresStore,
    /// ShadowStore, MockStore in tests).
    pub store: &'a dyn KafkaStore,

    /// Consumer group coordinator for managing group state
    ///
    /// Handles group membership, partition assignment, and rebalancing.
    /// Shared via Arc across requests.
    pub coordinator: &'a GroupCoordinator,

    /// Broker metadata for Metadata/FindCoordinator responses
    ///
    /// Contains advertised host/port that clients use to connect.
    /// Uses Arc<String> internally for zero-cost cloning.
    pub broker: &'a BrokerMetadata,

    /// Default number of partitions for auto-created topics
    ///
    /// Used when a client produces to a non-existent topic and we need to
    /// create it automatically.
    pub default_partitions: i32,

    /// Compression algorithm for outbound messages
    ///
    /// Applied to RecordBatch data in FetchResponse. Clients specify this
    /// via configuration (e.g., pg_kafka.compression_type GUC).
    pub compression: Compression,

    /// Optional notification channel for long polling support
    ///
    /// When set, ProduceRequest handlers send notifications to wake up waiting
    /// FetchRequest handlers. None if long polling is disabled.
    pub notifier: Option<&'a Sender<InternalNotification>>,
}

impl<'a> HandlerContext<'a> {
    /// Create a new handler context
    ///
    /// # Arguments
    ///
    /// * `store` - Storage backend (PostgresStore, ShadowStore, etc.)
    /// * `coordinator` - Consumer group coordinator
    /// * `broker` - Broker metadata for responses
    /// * `default_partitions` - Partition count for auto-created topics
    /// * `compression` - Compression for outbound messages
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let ctx = HandlerContext::new(
    ///     &shadow_store,
    ///     &coordinator,
    ///     &broker_metadata,
    ///     1, // default partitions
    ///     Compression::None,
    /// );
    /// ```
    pub fn new(
        store: &'a dyn KafkaStore,
        coordinator: &'a GroupCoordinator,
        broker: &'a BrokerMetadata,
        default_partitions: i32,
        compression: Compression,
    ) -> Self {
        Self {
            store,
            coordinator,
            broker,
            default_partitions,
            compression,
            notifier: None,
        }
    }

    /// Create a handler context with notification support
    ///
    /// Used when long polling is enabled. The notifier channel allows
    /// ProduceRequest handlers to wake up waiting FetchRequest handlers
    /// when new messages arrive.
    ///
    /// # Arguments
    ///
    /// * `store` - Storage backend
    /// * `coordinator` - Consumer group coordinator
    /// * `broker` - Broker metadata
    /// * `default_partitions` - Partition count for auto-created topics
    /// * `compression` - Compression for outbound messages
    /// * `notifier` - Channel for sending notifications to network thread
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let ctx = HandlerContext::with_notifier(
    ///     &shadow_store,
    ///     &coordinator,
    ///     &broker_metadata,
    ///     1,
    ///     Compression::None,
    ///     &notify_tx,
    /// );
    /// ```
    pub fn with_notifier(
        store: &'a dyn KafkaStore,
        coordinator: &'a GroupCoordinator,
        broker: &'a BrokerMetadata,
        default_partitions: i32,
        compression: Compression,
        notifier: &'a Sender<InternalNotification>,
    ) -> Self {
        Self {
            store,
            coordinator,
            broker,
            default_partitions,
            compression,
            notifier: Some(notifier),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Mock implementations for testing
    struct MockStore;
    impl KafkaStore for MockStore {
        fn get_or_create_topic(
            &self,
            _name: &str,
            _default_partitions: i32,
        ) -> crate::kafka::error::Result<(i32, i32)> {
            Ok((1, 1))
        }

        fn get_topic_metadata(
            &self,
            _names: Option<&[String]>,
        ) -> crate::kafka::error::Result<Vec<crate::kafka::storage::TopicMetadata>> {
            Ok(vec![])
        }

        fn insert_records(
            &self,
            _topic_id: i32,
            _partition_id: i32,
            _records: &[crate::kafka::messages::Record],
        ) -> crate::kafka::error::Result<i64> {
            Ok(0)
        }

        fn fetch_records(
            &self,
            _topic_id: i32,
            _partition_id: i32,
            _fetch_offset: i64,
            _max_bytes: i32,
        ) -> crate::kafka::error::Result<Vec<crate::kafka::storage::FetchedMessage>> {
            Ok(vec![])
        }

        fn get_high_watermark(
            &self,
            _topic_id: i32,
            _partition_id: i32,
        ) -> crate::kafka::error::Result<i64> {
            Ok(0)
        }

        fn get_earliest_offset(
            &self,
            _topic_id: i32,
            _partition_id: i32,
        ) -> crate::kafka::error::Result<i64> {
            Ok(0)
        }

        fn commit_offset(
            &self,
            _group_id: &str,
            _topic_id: i32,
            _partition_id: i32,
            _offset: i64,
            _metadata: Option<&str>,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn fetch_offset(
            &self,
            _group_id: &str,
            _topic_id: i32,
            _partition_id: i32,
        ) -> crate::kafka::error::Result<Option<crate::kafka::storage::CommittedOffset>> {
            Ok(None)
        }

        fn fetch_all_offsets(
            &self,
            _group_id: &str,
        ) -> crate::kafka::error::Result<Vec<(String, i32, crate::kafka::storage::CommittedOffset)>>
        {
            Ok(vec![])
        }

        fn topic_exists(&self, _name: &str) -> crate::kafka::error::Result<bool> {
            Ok(true)
        }

        fn create_topic(
            &self,
            _name: &str,
            _partition_count: i32,
        ) -> crate::kafka::error::Result<i32> {
            Ok(1)
        }

        fn get_topic_id(&self, _name: &str) -> crate::kafka::error::Result<Option<i32>> {
            Ok(Some(1))
        }

        fn delete_topic(&self, _topic_id: i32) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn get_topic_partition_count(
            &self,
            _name: &str,
        ) -> crate::kafka::error::Result<Option<i32>> {
            Ok(Some(1))
        }

        fn set_topic_partition_count(
            &self,
            _name: &str,
            _partition_count: i32,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn delete_consumer_group_offsets(
            &self,
            _group_id: &str,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn allocate_producer_id(
            &self,
            _client_id: Option<&str>,
            _transactional_id: Option<&str>,
        ) -> crate::kafka::error::Result<(i64, i16)> {
            Ok((1, 0))
        }

        fn get_producer_epoch(
            &self,
            _producer_id: i64,
        ) -> crate::kafka::error::Result<Option<i16>> {
            Ok(Some(0))
        }

        fn increment_producer_epoch(&self, _producer_id: i64) -> crate::kafka::error::Result<i16> {
            Ok(1)
        }

        fn check_and_update_sequence(
            &self,
            _producer_id: i64,
            _producer_epoch: i16,
            _topic_id: i32,
            _partition_id: i32,
            _base_sequence: i32,
            _record_count: i32,
        ) -> crate::kafka::error::Result<bool> {
            Ok(true)
        }

        fn get_or_create_transactional_producer(
            &self,
            _transactional_id: &str,
            _transaction_timeout_ms: i32,
            _client_id: Option<&str>,
        ) -> crate::kafka::error::Result<(i64, i16)> {
            Ok((1, 0))
        }

        fn begin_transaction(
            &self,
            _transactional_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn validate_transaction(
            &self,
            _transactional_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn insert_transactional_records(
            &self,
            _topic_id: i32,
            _partition_id: i32,
            _records: &[crate::kafka::messages::Record],
            _producer_id: i64,
            _producer_epoch: i16,
        ) -> crate::kafka::error::Result<i64> {
            Ok(0)
        }

        fn store_txn_pending_offset(
            &self,
            _transactional_id: &str,
            _group_id: &str,
            _topic_id: i32,
            _partition_id: i32,
            _offset: i64,
            _metadata: Option<&str>,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn commit_transaction(
            &self,
            _transactional_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn abort_transaction(
            &self,
            _transactional_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
        ) -> crate::kafka::error::Result<()> {
            Ok(())
        }

        fn get_transaction_state(
            &self,
            _transactional_id: &str,
        ) -> crate::kafka::error::Result<Option<crate::kafka::storage::TransactionState>> {
            Ok(None)
        }

        fn fetch_records_with_isolation(
            &self,
            _topic_id: i32,
            _partition_id: i32,
            _fetch_offset: i64,
            _max_bytes: i32,
            _isolation_level: crate::kafka::storage::IsolationLevel,
        ) -> crate::kafka::error::Result<Vec<crate::kafka::storage::FetchedMessage>> {
            Ok(vec![])
        }

        fn get_last_stable_offset(
            &self,
            _topic_id: i32,
            _partition_id: i32,
        ) -> crate::kafka::error::Result<i64> {
            Ok(0)
        }

        fn abort_timed_out_transactions(
            &self,
            _timeout: std::time::Duration,
        ) -> crate::kafka::error::Result<Vec<String>> {
            Ok(vec![])
        }

        fn cleanup_aborted_messages(
            &self,
            _older_than: std::time::Duration,
        ) -> crate::kafka::error::Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn test_handler_context_creation() {
        let mock_store = MockStore;
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);

        let ctx = HandlerContext::new(&mock_store, &coordinator, &broker, 1, Compression::None);

        assert_eq!(ctx.default_partitions, 1);
        assert_eq!(ctx.broker.host(), "localhost");
        assert_eq!(ctx.broker.port(), 9092);
        assert!(ctx.notifier.is_none());
    }

    #[test]
    fn test_handler_context_with_notifier() {
        let mock_store = MockStore;
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        let (notify_tx, _notify_rx) = crossbeam_channel::bounded::<InternalNotification>(100);

        let ctx = HandlerContext::with_notifier(
            &mock_store,
            &coordinator,
            &broker,
            1,
            Compression::None,
            &notify_tx,
        );

        assert_eq!(ctx.default_partitions, 1);
        assert!(ctx.notifier.is_some());
    }

    #[test]
    fn test_context_lifetime() {
        // Ensure context doesn't outlive its references
        let mock_store = MockStore;
        let coordinator = Arc::new(GroupCoordinator::new());
        let broker = BrokerMetadata::new("test".to_string(), 1234);

        {
            let ctx = HandlerContext::new(&mock_store, &coordinator, &broker, 1, Compression::None);
            assert_eq!(ctx.broker.port(), 1234);
        }
        // ctx is dropped, but coordinator and broker still exist
        assert_eq!(broker.port(), 1234);
    }
}
