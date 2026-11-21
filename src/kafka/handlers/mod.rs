//! Kafka protocol handlers: one `handle_*` function per API, taking `&HandlerContext`.
//!
//! A handler turns a decoded request into the kafka-protocol response (or a typed
//! `KafkaError`) through the storage role traits on `HandlerContext::store`
//! (`&dyn KafkaStore`) and the group coordinator; it knows nothing about SQL or SPI, and
//! the transaction around it belongs to the worker. The decode/encode pipeline around each
//! handler is its API's `KafkaApi` impl in `kafka::api`.

mod admin;
mod configs;
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

pub use admin::{
    handle_create_partitions, handle_create_topics, handle_delete_groups, handle_delete_topics,
};
pub use configs::{
    handle_delete_records, handle_describe_configs, handle_incremental_alter_configs,
};
pub use consumer::{handle_offset_commit, handle_offset_fetch};
pub use coordinator::{
    handle_describe_groups, handle_find_coordinator, handle_heartbeat, handle_join_group,
    handle_leave_group, handle_list_groups, handle_sync_group, normalize_join_group_response,
    JoinGroupReply, JoinGroupRequestData, SyncGroupReply,
};
pub use fetch::{handle_fetch, handle_fetch_with_max_bytes, handle_list_offsets};
pub use helpers::{resolve_topic_id, topic_resolution_error_code, TopicResolution};
pub use init_producer_id::{handle_init_producer_id, TRANSACTION_MAX_TIMEOUT_MS};
pub use metadata::{handle_api_versions, handle_metadata};
pub use produce::{answer_refused_partitions, handle_produce};
pub use transaction::{
    handle_add_offsets_to_txn, handle_add_partitions_to_txn, handle_end_txn,
    handle_txn_offset_commit, txn_offset_commit_error_response,
};
