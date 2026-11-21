//! Role delegation macros: an implementation that changes nothing about a role forwards
//! it wholesale with one of these instead of re-listing the role method by method.
//!
//! `delegate_x_store!(Trait, field)` forwards every method to `&self.field` (a wrapper
//! such as `ShadowStore`); `delegate_x_store!(assoc Type)` forwards to the associated
//! functions of `Type` with the same names and no receiver (`PostgresStore`, which is
//! stateless).

/// Implement every [`TopicStore`](super::TopicStore) method, inside an
/// `impl TopicStore for ...`: `delegate_topic_store!(Trait, field)` forwards to
/// `&self.field`, `delegate_topic_store!(assoc Type)` to `Type`'s associated functions of the
/// same names.
macro_rules! delegate_topic_store {
    (assoc $via:ident) => {
        fn get_or_create_topic(
            &self,
            name: &str,
            default_partitions: i32,
        ) -> $crate::kafka::error::Result<(i32, i32)> {
            $via::get_or_create_topic(name, default_partitions)
        }
        fn get_topic_metadata(
            &self,
            names: Option<&[String]>,
        ) -> $crate::kafka::error::Result<Vec<$crate::kafka::storage::TopicMetadata>> {
            $via::get_topic_metadata(names)
        }
        fn topic_exists(&self, name: &str) -> $crate::kafka::error::Result<bool> {
            $via::topic_exists(name)
        }
        fn create_topic(
            &self,
            name: &str,
            partition_count: i32,
        ) -> $crate::kafka::error::Result<i32> {
            $via::create_topic(name, partition_count)
        }
        fn get_topic_id(&self, name: &str) -> $crate::kafka::error::Result<Option<i32>> {
            $via::get_topic_id(name)
        }
        fn delete_topic(&self, topic_id: i32) -> $crate::kafka::error::Result<()> {
            $via::delete_topic(topic_id)
        }
        fn get_topic_partition_count(
            &self,
            name: &str,
        ) -> $crate::kafka::error::Result<Option<i32>> {
            $via::get_topic_partition_count(name)
        }
        fn set_topic_partition_count(
            &self,
            name: &str,
            partition_count: i32,
        ) -> $crate::kafka::error::Result<()> {
            $via::set_topic_partition_count(name, partition_count)
        }
        fn get_topic_retention_ms(
            &self,
            topic_id: i32,
        ) -> $crate::kafka::error::Result<Option<i64>> {
            $via::get_topic_retention_ms(topic_id)
        }
        fn set_topic_retention_ms(
            &self,
            topic_id: i32,
            retention_ms: Option<i64>,
        ) -> $crate::kafka::error::Result<()> {
            $via::set_topic_retention_ms(topic_id, retention_ms)
        }
        fn find_colliding_topic(&self, name: &str) -> $crate::kafka::error::Result<Option<String>> {
            $via::find_colliding_topic(name)
        }
    };
    ($via:ident, $field:ident) => {
        fn get_or_create_topic(
            &self,
            name: &str,
            default_partitions: i32,
        ) -> $crate::kafka::error::Result<(i32, i32)> {
            $via::get_or_create_topic(&self.$field, name, default_partitions)
        }
        fn get_topic_metadata(
            &self,
            names: Option<&[String]>,
        ) -> $crate::kafka::error::Result<Vec<$crate::kafka::storage::TopicMetadata>> {
            $via::get_topic_metadata(&self.$field, names)
        }
        fn topic_exists(&self, name: &str) -> $crate::kafka::error::Result<bool> {
            $via::topic_exists(&self.$field, name)
        }
        fn create_topic(
            &self,
            name: &str,
            partition_count: i32,
        ) -> $crate::kafka::error::Result<i32> {
            $via::create_topic(&self.$field, name, partition_count)
        }
        fn get_topic_id(&self, name: &str) -> $crate::kafka::error::Result<Option<i32>> {
            $via::get_topic_id(&self.$field, name)
        }
        fn delete_topic(&self, topic_id: i32) -> $crate::kafka::error::Result<()> {
            $via::delete_topic(&self.$field, topic_id)
        }
        fn get_topic_partition_count(
            &self,
            name: &str,
        ) -> $crate::kafka::error::Result<Option<i32>> {
            $via::get_topic_partition_count(&self.$field, name)
        }
        fn set_topic_partition_count(
            &self,
            name: &str,
            partition_count: i32,
        ) -> $crate::kafka::error::Result<()> {
            $via::set_topic_partition_count(&self.$field, name, partition_count)
        }
        fn get_topic_retention_ms(
            &self,
            topic_id: i32,
        ) -> $crate::kafka::error::Result<Option<i64>> {
            $via::get_topic_retention_ms(&self.$field, topic_id)
        }
        fn set_topic_retention_ms(
            &self,
            topic_id: i32,
            retention_ms: Option<i64>,
        ) -> $crate::kafka::error::Result<()> {
            $via::set_topic_retention_ms(&self.$field, topic_id, retention_ms)
        }
        fn find_colliding_topic(&self, name: &str) -> $crate::kafka::error::Result<Option<String>> {
            $via::find_colliding_topic(&self.$field, name)
        }
    };
}
pub(crate) use delegate_topic_store;

/// Implement every [`LogStore`](super::LogStore) method, inside an
/// `impl LogStore for ...`: `delegate_log_store!(Trait, field)` forwards to
/// `&self.field`, `delegate_log_store!(assoc Type)` to `Type`'s associated functions of the
/// same names.
macro_rules! delegate_log_store {
    (assoc $via:ident) => {
        fn insert_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            records: &[$crate::kafka::messages::Record],
        ) -> $crate::kafka::error::Result<i64> {
            $via::insert_records(topic_id, partition_id, records)
        }
        fn fetch_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            fetch_offset: i64,
            max_bytes: i32,
        ) -> $crate::kafka::error::Result<Vec<$crate::kafka::storage::FetchedMessage>> {
            $via::fetch_records(topic_id, partition_id, fetch_offset, max_bytes)
        }
        fn fetch_records_with_isolation(
            &self,
            topic_id: i32,
            partition_id: i32,
            fetch_offset: i64,
            max_bytes: i32,
            isolation_level: $crate::kafka::storage::IsolationLevel,
        ) -> $crate::kafka::error::Result<Vec<$crate::kafka::storage::FetchedMessage>> {
            $via::fetch_records_with_isolation(
                topic_id,
                partition_id,
                fetch_offset,
                max_bytes,
                isolation_level,
            )
        }
        fn get_high_watermark(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_high_watermark(topic_id, partition_id)
        }
        fn get_earliest_offset(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_earliest_offset(topic_id, partition_id)
        }
        fn get_log_start_offset(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_log_start_offset(topic_id, partition_id)
        }
        fn get_last_stable_offset(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_last_stable_offset(topic_id, partition_id)
        }
        fn get_partition_bounds(
            &self,
            topic_id: i32,
            partition_id: i32,
            isolation_level: $crate::kafka::storage::IsolationLevel,
        ) -> $crate::kafka::error::Result<$crate::kafka::storage::PartitionBounds> {
            $via::get_partition_bounds(topic_id, partition_id, isolation_level)
        }
        fn get_offset_for_timestamp(
            &self,
            topic_id: i32,
            partition_id: i32,
            timestamp_ms: i64,
        ) -> $crate::kafka::error::Result<Option<(i64, i64)>> {
            $via::get_offset_for_timestamp(topic_id, partition_id, timestamp_ms)
        }
        fn delete_records_before(
            &self,
            topic_id: i32,
            partition_id: i32,
            before_offset: i64,
        ) -> $crate::kafka::error::Result<i64> {
            $via::delete_records_before(topic_id, partition_id, before_offset)
        }
    };
    ($via:ident, $field:ident) => {
        fn insert_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            records: &[$crate::kafka::messages::Record],
        ) -> $crate::kafka::error::Result<i64> {
            $via::insert_records(&self.$field, topic_id, partition_id, records)
        }
        fn fetch_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            fetch_offset: i64,
            max_bytes: i32,
        ) -> $crate::kafka::error::Result<Vec<$crate::kafka::storage::FetchedMessage>> {
            $via::fetch_records(
                &self.$field,
                topic_id,
                partition_id,
                fetch_offset,
                max_bytes,
            )
        }
        fn fetch_records_with_isolation(
            &self,
            topic_id: i32,
            partition_id: i32,
            fetch_offset: i64,
            max_bytes: i32,
            isolation_level: $crate::kafka::storage::IsolationLevel,
        ) -> $crate::kafka::error::Result<Vec<$crate::kafka::storage::FetchedMessage>> {
            $via::fetch_records_with_isolation(
                &self.$field,
                topic_id,
                partition_id,
                fetch_offset,
                max_bytes,
                isolation_level,
            )
        }
        fn get_high_watermark(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_high_watermark(&self.$field, topic_id, partition_id)
        }
        fn get_earliest_offset(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_earliest_offset(&self.$field, topic_id, partition_id)
        }
        fn get_log_start_offset(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_log_start_offset(&self.$field, topic_id, partition_id)
        }
        fn get_last_stable_offset(
            &self,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<i64> {
            $via::get_last_stable_offset(&self.$field, topic_id, partition_id)
        }
        fn get_partition_bounds(
            &self,
            topic_id: i32,
            partition_id: i32,
            isolation_level: $crate::kafka::storage::IsolationLevel,
        ) -> $crate::kafka::error::Result<$crate::kafka::storage::PartitionBounds> {
            $via::get_partition_bounds(&self.$field, topic_id, partition_id, isolation_level)
        }
        fn get_offset_for_timestamp(
            &self,
            topic_id: i32,
            partition_id: i32,
            timestamp_ms: i64,
        ) -> $crate::kafka::error::Result<Option<(i64, i64)>> {
            $via::get_offset_for_timestamp(&self.$field, topic_id, partition_id, timestamp_ms)
        }
        fn delete_records_before(
            &self,
            topic_id: i32,
            partition_id: i32,
            before_offset: i64,
        ) -> $crate::kafka::error::Result<i64> {
            $via::delete_records_before(&self.$field, topic_id, partition_id, before_offset)
        }
    };
}
pub(crate) use delegate_log_store;

/// Implement every [`OffsetStore`](super::OffsetStore) method, inside an
/// `impl OffsetStore for ...`: `delegate_offset_store!(Trait, field)` forwards to
/// `&self.field`, `delegate_offset_store!(assoc Type)` to `Type`'s associated functions of the
/// same names.
macro_rules! delegate_offset_store {
    (assoc $via:ident) => {
        fn commit_offset(&self, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> $crate::kafka::error::Result<()> {
            $via::commit_offset(group_id, topic_id, partition_id, offset, metadata)
        }
        fn fetch_offset(&self, group_id: &str, topic_id: i32, partition_id: i32) -> $crate::kafka::error::Result<Option<$crate::kafka::storage::CommittedOffset>> {
            $via::fetch_offset(group_id, topic_id, partition_id)
        }
        fn fetch_all_offsets(&self, group_id: &str) -> $crate::kafka::error::Result<Vec<(String, i32, $crate::kafka::storage::CommittedOffset)>> {
            $via::fetch_all_offsets(group_id)
        }
        fn delete_consumer_group_offsets(&self, group_id: &str) -> $crate::kafka::error::Result<()> {
            $via::delete_consumer_group_offsets(group_id)
        }
        fn list_offset_groups(&self) -> $crate::kafka::error::Result<Vec<String>> {
            $via::list_offset_groups()
        }
        fn group_has_offsets(&self, group_id: &str) -> $crate::kafka::error::Result<bool> {
            $via::group_has_offsets(group_id)
        }
        fn pending_txn_offset_partitions(&self, group_id: &str) -> $crate::kafka::error::Result<Vec<(String, i32)>> {
            $via::pending_txn_offset_partitions(group_id)
        }
    };
    ($via:ident, $field:ident) => {
        fn commit_offset(&self, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> $crate::kafka::error::Result<()> {
            $via::commit_offset(&self.$field, group_id, topic_id, partition_id, offset, metadata)
        }
        fn fetch_offset(&self, group_id: &str, topic_id: i32, partition_id: i32) -> $crate::kafka::error::Result<Option<$crate::kafka::storage::CommittedOffset>> {
            $via::fetch_offset(&self.$field, group_id, topic_id, partition_id)
        }
        fn fetch_all_offsets(&self, group_id: &str) -> $crate::kafka::error::Result<Vec<(String, i32, $crate::kafka::storage::CommittedOffset)>> {
            $via::fetch_all_offsets(&self.$field, group_id)
        }
        fn delete_consumer_group_offsets(&self, group_id: &str) -> $crate::kafka::error::Result<()> {
            $via::delete_consumer_group_offsets(&self.$field, group_id)
        }
        fn list_offset_groups(&self) -> $crate::kafka::error::Result<Vec<String>> {
            $via::list_offset_groups(&self.$field)
        }
        fn group_has_offsets(&self, group_id: &str) -> $crate::kafka::error::Result<bool> {
            $via::group_has_offsets(&self.$field, group_id)
        }
        fn pending_txn_offset_partitions(&self, group_id: &str) -> $crate::kafka::error::Result<Vec<(String, i32)>> {
            $via::pending_txn_offset_partitions(&self.$field, group_id)
        }
    };
}
pub(crate) use delegate_offset_store;

/// Implement every [`ProducerStore`](super::ProducerStore) method, inside an
/// `impl ProducerStore for ...`: `delegate_producer_store!(Trait, field)` forwards to
/// `&self.field`, `delegate_producer_store!(assoc Type)` to `Type`'s associated functions of the
/// same names.
macro_rules! delegate_producer_store {
    (assoc $via:ident) => {
        fn allocate_producer_id(
            &self,
            client_id: Option<&str>,
            transactional_id: Option<&str>,
        ) -> $crate::kafka::error::Result<(i64, i16)> {
            $via::allocate_producer_id(client_id, transactional_id)
        }
        fn check_and_update_sequence(
            &self,
            producer_id: i64,
            producer_epoch: i16,
            topic_id: i32,
            partition_id: i32,
            base_sequence: i32,
            record_count: i32,
        ) -> $crate::kafka::error::Result<$crate::kafka::storage::SequenceCheck> {
            $via::check_and_update_sequence(
                producer_id,
                producer_epoch,
                topic_id,
                partition_id,
                base_sequence,
                record_count,
            )
        }
        fn record_producer_batch(
            &self,
            batch: $crate::kafka::storage::ProducerBatch,
        ) -> $crate::kafka::error::Result<()> {
            $via::record_producer_batch(batch)
        }
        fn add_txn_partitions(
            &self,
            transactional_id: &str,
            partitions: &[(i32, i32)],
        ) -> $crate::kafka::error::Result<()> {
            $via::add_txn_partitions(transactional_id, partitions)
        }
        fn txn_has_partition(
            &self,
            transactional_id: &str,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<bool> {
            $via::txn_has_partition(transactional_id, topic_id, partition_id)
        }
        fn add_txn_group(
            &self,
            transactional_id: &str,
            group_id: &str,
        ) -> $crate::kafka::error::Result<()> {
            $via::add_txn_group(transactional_id, group_id)
        }
        fn txn_has_group(
            &self,
            transactional_id: &str,
            group_id: &str,
        ) -> $crate::kafka::error::Result<bool> {
            $via::txn_has_group(transactional_id, group_id)
        }
        fn open_txn_epoch_on_partition(
            &self,
            producer_id: i64,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<Option<i16>> {
            $via::open_txn_epoch_on_partition(producer_id, topic_id, partition_id)
        }
        fn record_producer_sequence(
            &self,
            producer_id: i64,
            topic_id: i32,
            partition_id: i32,
            last_sequence: i32,
        ) -> $crate::kafka::error::Result<()> {
            $via::record_producer_sequence(producer_id, topic_id, partition_id, last_sequence)
        }
        fn transactional_producer(
            &self,
            transactional_id: &str,
        ) -> $crate::kafka::error::Result<Option<$crate::kafka::storage::TransactionalProducer>> {
            $via::transactional_producer(transactional_id)
        }
        fn remember_previous_epoch(
            &self,
            producer_id: i64,
            previous_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::remember_previous_epoch(producer_id, previous_epoch)
        }
    };
    ($via:ident, $field:ident) => {
        fn allocate_producer_id(
            &self,
            client_id: Option<&str>,
            transactional_id: Option<&str>,
        ) -> $crate::kafka::error::Result<(i64, i16)> {
            $via::allocate_producer_id(&self.$field, client_id, transactional_id)
        }
        fn check_and_update_sequence(
            &self,
            producer_id: i64,
            producer_epoch: i16,
            topic_id: i32,
            partition_id: i32,
            base_sequence: i32,
            record_count: i32,
        ) -> $crate::kafka::error::Result<$crate::kafka::storage::SequenceCheck> {
            $via::check_and_update_sequence(
                &self.$field,
                producer_id,
                producer_epoch,
                topic_id,
                partition_id,
                base_sequence,
                record_count,
            )
        }
        fn record_producer_batch(
            &self,
            batch: $crate::kafka::storage::ProducerBatch,
        ) -> $crate::kafka::error::Result<()> {
            $via::record_producer_batch(&self.$field, batch)
        }
        fn add_txn_partitions(
            &self,
            transactional_id: &str,
            partitions: &[(i32, i32)],
        ) -> $crate::kafka::error::Result<()> {
            $via::add_txn_partitions(&self.$field, transactional_id, partitions)
        }
        fn txn_has_partition(
            &self,
            transactional_id: &str,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<bool> {
            $via::txn_has_partition(&self.$field, transactional_id, topic_id, partition_id)
        }
        fn add_txn_group(
            &self,
            transactional_id: &str,
            group_id: &str,
        ) -> $crate::kafka::error::Result<()> {
            $via::add_txn_group(&self.$field, transactional_id, group_id)
        }
        fn txn_has_group(
            &self,
            transactional_id: &str,
            group_id: &str,
        ) -> $crate::kafka::error::Result<bool> {
            $via::txn_has_group(&self.$field, transactional_id, group_id)
        }
        fn open_txn_epoch_on_partition(
            &self,
            producer_id: i64,
            topic_id: i32,
            partition_id: i32,
        ) -> $crate::kafka::error::Result<Option<i16>> {
            $via::open_txn_epoch_on_partition(&self.$field, producer_id, topic_id, partition_id)
        }
        fn record_producer_sequence(
            &self,
            producer_id: i64,
            topic_id: i32,
            partition_id: i32,
            last_sequence: i32,
        ) -> $crate::kafka::error::Result<()> {
            $via::record_producer_sequence(
                &self.$field,
                producer_id,
                topic_id,
                partition_id,
                last_sequence,
            )
        }
        fn transactional_producer(
            &self,
            transactional_id: &str,
        ) -> $crate::kafka::error::Result<Option<$crate::kafka::storage::TransactionalProducer>> {
            $via::transactional_producer(&self.$field, transactional_id)
        }
        fn remember_previous_epoch(
            &self,
            producer_id: i64,
            previous_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::remember_previous_epoch(&self.$field, producer_id, previous_epoch)
        }
    };
}
pub(crate) use delegate_producer_store;

/// Implement every [`TransactionStore`](super::TransactionStore) method, inside an
/// `impl TransactionStore for ...`: `delegate_transaction_store!(Trait, field)` forwards to
/// `&self.field`, `delegate_transaction_store!(assoc Type)` to `Type`'s associated functions of the
/// same names.
macro_rules! delegate_transaction_store {
    (assoc $via:ident) => {
        fn get_or_create_transactional_producer(
            &self,
            transactional_id: &str,
            transaction_timeout_ms: i32,
            client_id: Option<&str>,
        ) -> $crate::kafka::error::Result<(i64, i16)> {
            $via::get_or_create_transactional_producer(
                transactional_id,
                transaction_timeout_ms,
                client_id,
            )
        }
        fn begin_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::begin_transaction(transactional_id, producer_id, producer_epoch)
        }
        fn begin_or_continue_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::begin_or_continue_transaction(transactional_id, producer_id, producer_epoch)
        }
        fn validate_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::validate_transaction(transactional_id, producer_id, producer_epoch)
        }
        fn insert_transactional_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            records: &[$crate::kafka::messages::Record],
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<i64> {
            $via::insert_transactional_records(
                topic_id,
                partition_id,
                records,
                producer_id,
                producer_epoch,
            )
        }
        fn store_txn_pending_offset(
            &self,
            transactional_id: &str,
            group_id: &str,
            topic_id: i32,
            partition_id: i32,
            offset: i64,
            metadata: Option<&str>,
        ) -> $crate::kafka::error::Result<()> {
            $via::store_txn_pending_offset(
                transactional_id,
                group_id,
                topic_id,
                partition_id,
                offset,
                metadata,
            )
        }
        fn commit_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::commit_transaction(transactional_id, producer_id, producer_epoch)
        }
        fn abort_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::abort_transaction(transactional_id, producer_id, producer_epoch)
        }
        fn get_transaction_state(
            &self,
            transactional_id: &str,
        ) -> $crate::kafka::error::Result<Option<$crate::kafka::storage::TransactionState>> {
            $via::get_transaction_state(transactional_id)
        }
    };
    ($via:ident, $field:ident) => {
        fn get_or_create_transactional_producer(
            &self,
            transactional_id: &str,
            transaction_timeout_ms: i32,
            client_id: Option<&str>,
        ) -> $crate::kafka::error::Result<(i64, i16)> {
            $via::get_or_create_transactional_producer(
                &self.$field,
                transactional_id,
                transaction_timeout_ms,
                client_id,
            )
        }
        fn begin_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::begin_transaction(&self.$field, transactional_id, producer_id, producer_epoch)
        }
        fn begin_or_continue_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::begin_or_continue_transaction(
                &self.$field,
                transactional_id,
                producer_id,
                producer_epoch,
            )
        }
        fn validate_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::validate_transaction(&self.$field, transactional_id, producer_id, producer_epoch)
        }
        fn insert_transactional_records(
            &self,
            topic_id: i32,
            partition_id: i32,
            records: &[$crate::kafka::messages::Record],
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<i64> {
            $via::insert_transactional_records(
                &self.$field,
                topic_id,
                partition_id,
                records,
                producer_id,
                producer_epoch,
            )
        }
        fn store_txn_pending_offset(
            &self,
            transactional_id: &str,
            group_id: &str,
            topic_id: i32,
            partition_id: i32,
            offset: i64,
            metadata: Option<&str>,
        ) -> $crate::kafka::error::Result<()> {
            $via::store_txn_pending_offset(
                &self.$field,
                transactional_id,
                group_id,
                topic_id,
                partition_id,
                offset,
                metadata,
            )
        }
        fn commit_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::commit_transaction(&self.$field, transactional_id, producer_id, producer_epoch)
        }
        fn abort_transaction(
            &self,
            transactional_id: &str,
            producer_id: i64,
            producer_epoch: i16,
        ) -> $crate::kafka::error::Result<()> {
            $via::abort_transaction(&self.$field, transactional_id, producer_id, producer_epoch)
        }
        fn get_transaction_state(
            &self,
            transactional_id: &str,
        ) -> $crate::kafka::error::Result<Option<$crate::kafka::storage::TransactionState>> {
            $via::get_transaction_state(&self.$field, transactional_id)
        }
    };
}
pub(crate) use delegate_transaction_store;

/// Implement every [`MaintenanceStore`](super::MaintenanceStore) method, inside an
/// `impl MaintenanceStore for ...`: `delegate_maintenance_store!(Trait, field)` forwards to
/// `&self.field`, `delegate_maintenance_store!(assoc Type)` to `Type`'s associated functions of the
/// same names.
macro_rules! delegate_maintenance_store {
    (assoc $via:ident) => {
        fn abort_timed_out_transactions(
            &self,
            timeout: std::time::Duration,
        ) -> $crate::kafka::error::Result<Vec<String>> {
            $via::abort_timed_out_transactions(timeout)
        }
        fn cleanup_aborted_messages(
            &self,
            older_than: std::time::Duration,
        ) -> $crate::kafka::error::Result<u64> {
            $via::cleanup_aborted_messages(older_than)
        }
    };
    ($via:ident, $field:ident) => {
        fn abort_timed_out_transactions(
            &self,
            timeout: std::time::Duration,
        ) -> $crate::kafka::error::Result<Vec<String>> {
            $via::abort_timed_out_transactions(&self.$field, timeout)
        }
        fn cleanup_aborted_messages(
            &self,
            older_than: std::time::Duration,
        ) -> $crate::kafka::error::Result<u64> {
            $via::cleanup_aborted_messages(&self.$field, older_than)
        }
    };
}
pub(crate) use delegate_maintenance_store;
