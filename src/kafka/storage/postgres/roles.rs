//! `PostgresStore`'s storage roles.
//!
//! The SQL lives in associated functions of `PostgresStore` (one submodule per role)
//! that carry the role methods' names; each role here forwards to them wholesale.
//! `PostgresStore::name(...)` resolves to the associated function, not back to the
//! trait method.

use super::PostgresStore;
use crate::kafka::storage::{
    LogStore, MaintenanceStore, OffsetStore, ProducerStore, TopicStore, TransactionStore,
};

impl TopicStore for PostgresStore {
    crate::kafka::storage::delegate_topic_store!(assoc PostgresStore);
}

impl LogStore for PostgresStore {
    crate::kafka::storage::delegate_log_store!(assoc PostgresStore);
}

impl OffsetStore for PostgresStore {
    crate::kafka::storage::delegate_offset_store!(assoc PostgresStore);
}

impl ProducerStore for PostgresStore {
    crate::kafka::storage::delegate_producer_store!(assoc PostgresStore);
}

impl TransactionStore for PostgresStore {
    crate::kafka::storage::delegate_transaction_store!(assoc PostgresStore);
}

impl MaintenanceStore for PostgresStore {
    crate::kafka::storage::delegate_maintenance_store!(assoc PostgresStore);
}
