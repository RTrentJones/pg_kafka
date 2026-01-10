// Kafka protocol handlers
//
// This module contains pure protocol logic that is decoupled from the storage implementation.
// Each handler accepts a storage trait and returns protocol responses without knowing about SQL.
//
// Architecture:
// - Handlers accept &impl KafkaStore (dependency injection)
// - They focus on protocol logic: parsing requests, coordinating storage calls, building responses
// - They know nothing about SQL, SPI, or transactions
// - Transaction boundaries remain explicit in worker.rs
//
// Module organization:
// - helpers: Common utilities for topic resolution and error handling
// - metadata: ApiVersions and Metadata request handlers
// - produce: ProduceRequest handler
// - fetch: FetchRequest and ListOffsetsRequest handlers
// - consumer: OffsetCommit and OffsetFetch handlers
// - coordinator: Consumer group coordination (JoinGroup, SyncGroup, Heartbeat, etc.)
// - init_producer_id: InitProducerId handler for idempotent producers (Phase 9)
// - transaction: Transaction handlers (Phase 10)

mod admin;
mod consumer;
mod coordinator;
mod fetch;
mod helpers;
mod init_producer_id;
mod metadata;
mod produce;
mod transaction;

#[cfg(test)]
mod tests;

// Re-export all handlers
pub use admin::{
    handle_create_partitions, handle_create_topics, handle_delete_groups, handle_delete_topics,
};
pub use consumer::{handle_offset_commit, handle_offset_fetch};
pub use coordinator::{
    handle_describe_groups, handle_find_coordinator, handle_heartbeat, handle_join_group,
    handle_leave_group, handle_list_groups, handle_sync_group,
};
pub use fetch::{handle_fetch, handle_list_offsets};
pub use helpers::{resolve_topic_id, topic_resolution_error_code, TopicResolution};
pub use init_producer_id::handle_init_producer_id;
pub use metadata::{handle_api_versions, handle_metadata};
pub use produce::handle_produce;
pub use transaction::{
    handle_add_offsets_to_txn, handle_add_partitions_to_txn, handle_end_txn,
    handle_txn_offset_commit,
};
