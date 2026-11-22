//! `HandlerContext`: the store, coordinator and broker metadata every Kafka handler takes
//! as its first parameter, so shared context is added in one place rather than to every
//! handler signature, and request-specific data stays in the handler's own parameters.

use kafka_protocol::records::Compression;

use crate::kafka::broker_metadata::BrokerMetadata;
use crate::kafka::coordinator::GroupCoordinator;
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
    /// ShadowStore, MockKafkaStore in tests).
    pub store: &'a dyn KafkaStore,

    /// Consumer group coordinator for managing group state
    ///
    /// Handles group membership, partition assignment, and rebalancing.
    /// Shared via Arc across requests.
    pub coordinator: &'a GroupCoordinator,

    /// Broker metadata for Metadata/FindCoordinator responses
    ///
    /// Contains advertised host/port that clients use to connect.
    /// Holds the host as an `Arc<String>`, so cloning it is cheap.
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_handler_context_creation() {
        let mock_store = crate::testing::mocks::MockKafkaStore::new();
        let coordinator = GroupCoordinator::new();
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);

        let ctx = HandlerContext::new(&mock_store, &coordinator, &broker, 1, Compression::None);

        assert_eq!(ctx.default_partitions, 1);
        assert_eq!(ctx.broker.host(), "localhost");
        assert_eq!(ctx.broker.port(), 9092);
    }

    #[test]
    fn test_context_lifetime() {
        // Ensure context doesn't outlive its references
        let mock_store = crate::testing::mocks::MockKafkaStore::new();
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
