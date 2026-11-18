//! `PostgresStore`'s storage roles.
//!
//! The SQL lives in one inherent `impl PostgresStore` block in `postgres.rs` whose methods
//! carry the role methods' names. Each role here forwards to them wholesale, so the role
//! split did not have to move any SQL. A child module can call those private inherent
//! methods; `PostgresStore::name(self, ...)` resolves to the inherent method, not back to
//! the trait.

use super::PostgresStore;
use crate::kafka::storage::{
    LogStore, MaintenanceStore, OffsetStore, ProducerStore, TopicStore, TransactionStore,
};

impl TopicStore for PostgresStore {
    crate::kafka::storage::delegate_topic_store!(PostgresStore);
}

impl LogStore for PostgresStore {
    crate::kafka::storage::delegate_log_store!(PostgresStore);
}

impl OffsetStore for PostgresStore {
    crate::kafka::storage::delegate_offset_store!(PostgresStore);
}

impl ProducerStore for PostgresStore {
    crate::kafka::storage::delegate_producer_store!(PostgresStore);
}

impl TransactionStore for PostgresStore {
    crate::kafka::storage::delegate_transaction_store!(PostgresStore);
}

impl MaintenanceStore for PostgresStore {
    crate::kafka::storage::delegate_maintenance_store!(PostgresStore);
}
