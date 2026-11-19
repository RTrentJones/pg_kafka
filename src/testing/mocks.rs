//! `MockKafkaStore`: a mockall double of every storage role, so handler tests set
//! expectations on storage calls instead of needing a database.

pub use store::MockKafkaStore;

#[expect(
    clippy::ref_option_ref,
    reason = "mockall's generated matchers take `Option<&[String]>` arguments by reference"
)]
mod store {
    use crate::kafka::error::Result;
    use crate::kafka::messages::Record;
    use crate::kafka::storage::{
        CommittedOffset, FetchedMessage, IsolationLevel, LogStore, MaintenanceStore, OffsetStore,
        ProducerBatch, ProducerStore, SequenceCheck, TopicMetadata, TopicStore, TransactionState,
        TransactionStore,
    };
    use mockall::mock;
    use mockall::predicate::*;
    use std::time::Duration;

    mock! {
        pub KafkaStore {}

        impl TopicStore for KafkaStore {
            fn get_or_create_topic<'a>(&self, name: &'a str, default_partitions: i32) -> Result<(i32, i32)>;
            fn get_topic_metadata<'a>(&self, names: Option<&'a [String]>) -> Result<Vec<TopicMetadata>>;
            fn topic_exists<'a>(&self, name: &'a str) -> Result<bool>;
            fn create_topic<'a>(&self, name: &'a str, partition_count: i32) -> Result<i32>;
            fn get_topic_id<'a>(&self, name: &'a str) -> Result<Option<i32>>;
            fn delete_topic(&self, topic_id: i32) -> Result<()>;
            fn get_topic_partition_count<'a>(&self, name: &'a str) -> Result<Option<i32>>;
            fn set_topic_partition_count<'a>(&self, name: &'a str, partition_count: i32) -> Result<()>;
            fn get_topic_retention_ms(&self, topic_id: i32) -> Result<Option<i64>>;
            fn set_topic_retention_ms(&self, topic_id: i32, retention_ms: Option<i64>) -> Result<()>;
                fn find_colliding_topic<'a>(&self, name: &'a str) -> Result<Option<String>>;
        }

        impl LogStore for KafkaStore {
            // get_partition_bounds keeps the trait default, so expectations on the
            // single-value bound methods drive handler tests.
            fn insert_records<'a>(&self, topic_id: i32, partition_id: i32, records: &'a [Record]) -> Result<i64>;
            fn fetch_records(
                &self,
                topic_id: i32,
                partition_id: i32,
                fetch_offset: i64,
                max_bytes: i32,
            ) -> Result<Vec<FetchedMessage>>;
            fn fetch_records_with_isolation(
                &self,
                topic_id: i32,
                partition_id: i32,
                fetch_offset: i64,
                max_bytes: i32,
                isolation_level: IsolationLevel,
            ) -> Result<Vec<FetchedMessage>>;
            fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
            fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
            fn get_log_start_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
            fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
            fn get_offset_for_timestamp(
                &self,
                topic_id: i32,
                partition_id: i32,
                timestamp_ms: i64,
            ) -> Result<Option<(i64, i64)>>;
            fn delete_records_before(&self, topic_id: i32, partition_id: i32, before_offset: i64) -> Result<i64>;
        }

        impl OffsetStore for KafkaStore {
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
            fn delete_consumer_group_offsets<'a>(&self, group_id: &'a str) -> Result<()>;
                fn list_offset_groups(&self) -> Result<Vec<String>>;
            fn group_has_offsets<'a>(&self, group_id: &'a str) -> Result<bool>;
            fn pending_txn_offset_partitions<'a>(&self, group_id: &'a str) -> Result<Vec<(String, i32)>>;
        }

        impl ProducerStore for KafkaStore {
            fn allocate_producer_id<'a>(
                &self,
                client_id: Option<&'a str>,
                transactional_id: Option<&'a str>,
            ) -> Result<(i64, i16)>;
            fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>>;
            fn increment_producer_epoch(&self, producer_id: i64) -> Result<(i64, i16)>;
            fn check_and_update_sequence(
                &self,
                producer_id: i64,
                producer_epoch: i16,
                topic_id: i32,
                partition_id: i32,
                base_sequence: i32,
                record_count: i32,
            ) -> Result<SequenceCheck>;
            fn record_producer_batch(&self, batch: ProducerBatch) -> Result<()>;
            fn add_txn_partitions<'a, 'b>(
                &self,
                transactional_id: &'a str,
                partitions: &'b [(i32, i32)],
            ) -> Result<()>;
            fn txn_has_partition<'a>(
                &self,
                transactional_id: &'a str,
                topic_id: i32,
                partition_id: i32,
            ) -> Result<bool>;
            fn record_producer_sequence(
                &self,
                producer_id: i64,
                topic_id: i32,
                partition_id: i32,
                last_sequence: i32,
            ) -> Result<()>;
        }

        impl TransactionStore for KafkaStore {
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
            fn begin_or_continue_transaction<'a>(
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
        }

        impl MaintenanceStore for KafkaStore {
            fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>>;
            fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64>;
        }
    }
}
