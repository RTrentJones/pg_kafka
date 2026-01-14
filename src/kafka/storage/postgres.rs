// PostgreSQL implementation of the KafkaStore trait
//
// This module implements the storage layer using PostgreSQL and pgrx SPI.
// It assumes it runs within a transaction context managed by the caller.

use super::{
    CommittedOffset, FetchedMessage, IsolationLevel, KafkaStore, TopicMetadata, TransactionState,
};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

/// PostgreSQL-backed implementation of KafkaStore
///
/// This implementation uses pgrx's SPI (Server Programming Interface) to execute
/// SQL queries. All operations assume they run within an active transaction
/// started by BackgroundWorker::transaction() in worker.rs.
pub struct PostgresStore;

impl PostgresStore {
    /// Create a new PostgresStore instance
    pub fn new() -> Self {
        PostgresStore
    }
}

impl Default for PostgresStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to encode bytes as hex string for JSONB storage
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

impl KafkaStore for PostgresStore {
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_topic: '{}' (default_partitions={})",
            name,
            default_partitions
        );

        Spi::connect_mut(|client| {
            let topic_name_string = name.to_string();

            let table = client.update(
                "INSERT INTO kafka.topics (name, partitions)
                 VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                 RETURNING id, partitions",
                None,
                &[topic_name_string.into(), default_partitions.into()],
            )?;

            let row = table.first();
            let topic_id: i32 = row
                .get_by_name("id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;
            let partition_count: i32 = row
                .get_by_name("partitions")?
                .ok_or_else(|| KafkaError::Internal("Failed to get partition count".into()))?;

            crate::pg_debug!(
                "Topic '{}' has id={}, partitions={}",
                name,
                topic_id,
                partition_count
            );
            Ok((topic_id, partition_count))
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_or_create_topic failed: {}", e)))
    }

    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        crate::pg_debug!("PostgresStore::get_topic_metadata");

        Spi::connect(|client| {
            let mut topics = Vec::new();

            if let Some(topic_names) = names {
                // Fetch specific topics
                for name in topic_names {
                    let mut table = client.select(
                        "SELECT id, partitions FROM kafka.topics WHERE name = $1",
                        None,
                        &[name.clone().into()],
                    )?;

                    if let Some(row) = table.next() {
                        let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                        let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);

                        topics.push(TopicMetadata {
                            name: name.clone(),
                            id,
                            partition_count: partitions,
                        });
                    }
                }
            } else {
                // Fetch all topics
                let table =
                    client.select("SELECT id, name, partitions FROM kafka.topics", None, &[])?;

                for row in table {
                    let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                    let name: String = row.get_by_name("name")?.unwrap_or_default();
                    let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);

                    topics.push(TopicMetadata {
                        name,
                        id,
                        partition_count: partitions,
                    });
                }
            }

            Ok(topics)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_topic_metadata failed: {}", e)))
    }

    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64> {
        if records.is_empty() {
            return Ok(0);
        }

        crate::pg_debug!(
            "PostgresStore::insert_records: {} records for topic_id={}, partition_id={}",
            records.len(),
            topic_id,
            partition_id
        );

        Spi::connect_mut(|client| {
            // Step 1: Lock the partition using advisory lock
            client.select(
                "SELECT pg_advisory_xact_lock($1, $2)",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            // Step 2: Get current max offset
            let table = client.select(
                "SELECT COALESCE(MAX(partition_offset), -1) as max_offset
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let max_offset: i64 = table
                .first()
                .get_by_name::<i64, _>("max_offset")?
                .unwrap_or(-1);

            let base_offset = max_offset + 1;

            crate::pg_debug!(
                "Current max_offset={}, new base_offset={}",
                max_offset,
                base_offset
            );

            // Step 3: Build parallel arrays for UNNEST-based bulk insert
            // This is type-safe (no SQL injection) and PostgreSQL-optimized
            let count = records.len();
            let topic_ids: Vec<i32> = vec![topic_id; count];
            let partition_ids: Vec<i32> = vec![partition_id; count];
            let offsets: Vec<i64> = (0..count).map(|i| base_offset + i as i64).collect();
            let keys: Vec<Option<Vec<u8>>> = records.iter().map(|r| r.key.clone()).collect();
            let values: Vec<Option<Vec<u8>>> = records.iter().map(|r| r.value.clone()).collect();
            let headers: Vec<String> = records
                .iter()
                .map(|r| {
                    if r.headers.is_empty() {
                        "{}".to_string()
                    } else {
                        let headers_map: HashMap<String, String> = r
                            .headers
                            .iter()
                            .map(|h| (h.key.clone(), hex_encode(&h.value)))
                            .collect();
                        serde_json::to_string(&headers_map).unwrap_or_else(|_| "{}".to_string())
                    }
                })
                .collect();

            // Execute single INSERT with UNNEST - type-safe parameterized query
            client
                .update(
                    "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value, headers)
                     SELECT * FROM unnest($1::int[], $2::int[], $3::bigint[], $4::bytea[], $5::bytea[], $6::jsonb[])",
                    None,
                    &[
                        topic_ids.into(),
                        partition_ids.into(),
                        offsets.into(),
                        keys.into(),
                        values.into(),
                        headers.into(),
                    ],
                )
                .map_err(|e| KafkaError::Internal(format!("Failed to insert records: {}", e)))?;

            crate::pg_debug!(
                "Successfully inserted {} records (offsets {} to {}) in single query",
                records.len(),
                base_offset,
                base_offset + records.len() as i64 - 1
            );

            Ok(base_offset)
        })
        .map_err(|e| match e {
            KafkaError::Internal(_) => e,
            _ => KafkaError::Internal(format!("insert_records failed: {}", e)),
        })
    }

    fn fetch_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> Result<Vec<FetchedMessage>> {
        crate::pg_debug!(
            "PostgresStore::fetch_records: topic_id={}, partition_id={}, fetch_offset={}, max_bytes={}",
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes
        );

        // Calculate limit based on max_bytes
        // Use conservative estimate of ~100 bytes per message minimum to avoid starving
        // high-throughput consumers with small messages. The hard cap is raised to 10,000
        // rows since the actual byte limit is enforced when building the response.
        // Note: max_bytes is typically 1MB (1,048,576), giving limit of 10,000 with /100.
        let limit = (max_bytes / 100).clamp(100, 10_000);

        Spi::connect(|client| {
            let table = client.select(
                "SELECT partition_offset, key, value,
                        EXTRACT(EPOCH FROM created_at)::bigint * 1000 as timestamp_ms
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
                 ORDER BY partition_offset
                 LIMIT $4",
                None,
                &[
                    topic_id.into(),
                    partition_id.into(),
                    fetch_offset.into(),
                    limit.into(),
                ],
            )?;

            let mut messages = Vec::new();
            for row in table {
                let partition_offset: i64 = row.get_by_name("partition_offset")?.unwrap_or(0);
                let key: Option<Vec<u8>> = row.get_by_name("key")?;
                let value: Option<Vec<u8>> = row.get_by_name("value")?;
                let timestamp: i64 = row.get_by_name("timestamp_ms")?.unwrap_or(0);

                messages.push(FetchedMessage {
                    partition_offset,
                    key,
                    value,
                    timestamp,
                });
            }

            crate::pg_debug!("Fetched {} messages", messages.len());
            Ok(messages)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("fetch_records failed: {}", e)))
    }

    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT COALESCE(MAX(partition_offset) + 1, 0) as high_watermark
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let high_watermark: i64 = table.first().get_by_name("high_watermark")?.unwrap_or(0);

            Ok(high_watermark)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_high_watermark failed: {}", e)))
    }

    fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT COALESCE(MIN(partition_offset), 0) as earliest_offset
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let earliest_offset: i64 = table.first().get_by_name("earliest_offset")?.unwrap_or(0);

            Ok(earliest_offset)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_earliest_offset failed: {}", e)))
    }

    fn commit_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::commit_offset: group_id={}, topic_id={}, partition_id={}, offset={}",
            group_id,
            topic_id,
            partition_id,
            offset
        );
        Spi::connect_mut(|client| {
            let query = "INSERT INTO kafka.consumer_offsets
                            (group_id, topic_id, partition_id, committed_offset, metadata)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (group_id, topic_id, partition_id)
                        DO UPDATE SET
                            committed_offset = EXCLUDED.committed_offset,
                            metadata = EXCLUDED.metadata,
                            commit_timestamp = NOW()";

            client.update(
                query,
                None,
                &[
                    group_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    offset.into(),
                    metadata.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("commit_offset failed: {}", e)))
    }

    fn fetch_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
    ) -> Result<Option<CommittedOffset>> {
        Spi::connect(|client| {
            let query = "SELECT committed_offset, metadata
                 FROM kafka.consumer_offsets
                 WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3";

            let mut table = client.select(
                query,
                Some(1),
                &[group_id.into(), topic_id.into(), partition_id.into()],
            )?;

            if let Some(row) = table.next() {
                let offset: i64 = row.get_by_name("committed_offset")?.unwrap_or(-1);
                let metadata: Option<String> = row.get_by_name("metadata")?;

                Ok(Some(CommittedOffset { offset, metadata }))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("fetch_offset failed: {}", e)))
    }

    fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>> {
        Spi::connect(|client| {
            let query = "SELECT t.name, co.partition_id, co.committed_offset, co.metadata
                 FROM kafka.consumer_offsets co
                 JOIN kafka.topics t ON co.topic_id = t.id
                 WHERE co.group_id = $1
                 ORDER BY t.name, co.partition_id";

            let table = client.select(query, None, &[group_id.into()])?;
            let mut results = Vec::new();

            for row in table {
                let topic_name: String = row.get_by_name("name")?.unwrap_or_default();
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                let offset: i64 = row.get_by_name("committed_offset")?.unwrap_or(-1);
                let metadata: Option<String> = row.get_by_name("metadata")?;

                results.push((
                    topic_name,
                    partition_id,
                    CommittedOffset { offset, metadata },
                ));
            }

            Ok(results)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("fetch_all_offsets failed: {}", e)))
    }

    // ===== Admin Topic Operations (Phase 6) =====

    fn topic_exists(&self, name: &str) -> Result<bool> {
        crate::pg_debug!("PostgresStore::topic_exists: '{}'", name);

        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;

            Ok(!table.is_empty())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("topic_exists failed: {}", e)))
    }

    fn create_topic(&self, name: &str, partition_count: i32) -> Result<i32> {
        crate::pg_debug!(
            "PostgresStore::create_topic: '{}' with {} partitions",
            name,
            partition_count
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "INSERT INTO kafka.topics (name, partitions)
                 VALUES ($1, $2)
                 RETURNING id",
                None,
                &[name.into(), partition_count.into()],
            )?;

            let topic_id: i32 = table
                .first()
                .get_by_name("id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;

            crate::pg_debug!("Created topic '{}' with id={}", name, topic_id);
            Ok(topic_id)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("create_topic failed: {}", e)))
    }

    fn get_topic_id(&self, name: &str) -> Result<Option<i32>> {
        crate::pg_debug!("PostgresStore::get_topic_id: '{}'", name);

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT id FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;

            if let Some(row) = table.next() {
                let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                Ok(Some(id))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_topic_id failed: {}", e)))
    }

    fn delete_topic(&self, topic_id: i32) -> Result<()> {
        crate::pg_debug!("PostgresStore::delete_topic: topic_id={}", topic_id);

        Spi::connect_mut(|client| {
            // Delete messages first (foreign key constraint)
            client.update(
                "DELETE FROM kafka.messages WHERE topic_id = $1",
                None,
                &[topic_id.into()],
            )?;

            // Delete consumer offsets
            client.update(
                "DELETE FROM kafka.consumer_offsets WHERE topic_id = $1",
                None,
                &[topic_id.into()],
            )?;

            // Delete topic
            client.update(
                "DELETE FROM kafka.topics WHERE id = $1",
                None,
                &[topic_id.into()],
            )?;

            crate::pg_debug!("Deleted topic with id={}", topic_id);
            Ok(())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("delete_topic failed: {}", e)))
    }

    fn get_topic_partition_count(&self, name: &str) -> Result<Option<i32>> {
        crate::pg_debug!("PostgresStore::get_topic_partition_count: '{}'", name);

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT partitions FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;

            if let Some(row) = table.next() {
                let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);
                Ok(Some(partitions))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_topic_partition_count failed: {}", e))
        })
    }

    fn set_topic_partition_count(&self, name: &str, partition_count: i32) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::set_topic_partition_count: '{}' to {}",
            name,
            partition_count
        );

        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.topics SET partitions = $2 WHERE name = $1",
                None,
                &[name.into(), partition_count.into()],
            )?;

            crate::pg_debug!(
                "Updated topic '{}' partition count to {}",
                name,
                partition_count
            );
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("set_topic_partition_count failed: {}", e))
        })
    }

    // ===== Admin Consumer Group Operations (Phase 6) =====

    fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::delete_consumer_group_offsets: '{}'",
            group_id
        );

        Spi::connect_mut(|client| {
            client.update(
                "DELETE FROM kafka.consumer_offsets WHERE group_id = $1",
                None,
                &[group_id.into()],
            )?;

            crate::pg_debug!("Deleted consumer offsets for group '{}'", group_id);
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("delete_consumer_group_offsets failed: {}", e))
        })
    }

    // ===== Idempotent Producer Operations (Phase 9) =====

    fn allocate_producer_id(
        &self,
        client_id: Option<&str>,
        transactional_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::allocate_producer_id: client_id={:?}, transactional_id={:?}",
            client_id,
            transactional_id
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "INSERT INTO kafka.producer_ids (client_id, transactional_id, epoch)
                 VALUES ($1, $2, 0)
                 RETURNING producer_id, epoch",
                None,
                &[client_id.into(), transactional_id.into()],
            )?;

            let row = table.first();
            let producer_id: i64 = row
                .get_by_name("producer_id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get producer_id".into()))?;
            let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

            crate::pg_debug!("Allocated producer_id={}, epoch={}", producer_id, epoch);
            Ok((producer_id, epoch))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("allocate_producer_id failed: {}", e))
        })
    }

    fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>> {
        crate::pg_debug!(
            "PostgresStore::get_producer_epoch: producer_id={}",
            producer_id
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                Some(1),
                &[producer_id.into()],
            )?;

            if let Some(row) = table.next() {
                let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);
                Ok(Some(epoch))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_producer_epoch failed: {}", e)))
    }

    fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16> {
        crate::pg_debug!(
            "PostgresStore::increment_producer_epoch: producer_id={}",
            producer_id
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "UPDATE kafka.producer_ids
                 SET epoch = epoch + 1, last_active_at = NOW()
                 WHERE producer_id = $1
                 RETURNING epoch",
                None,
                &[producer_id.into()],
            )?;

            if table.is_empty() {
                return Err(KafkaError::unknown_producer_id(producer_id));
            }

            let new_epoch: i16 = table
                .first()
                .get_by_name("epoch")?
                .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;

            crate::pg_debug!(
                "Incremented producer_id={} to epoch={}",
                producer_id,
                new_epoch
            );
            Ok(new_epoch)
        })
        .map_err(|e| match e {
            KafkaError::UnknownProducerId { .. } => e,
            _ => KafkaError::Internal(format!("increment_producer_epoch failed: {}", e)),
        })
    }

    fn check_and_update_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic_id: i32,
        partition_id: i32,
        base_sequence: i32,
        record_count: i32,
    ) -> Result<bool> {
        crate::pg_debug!(
            "PostgresStore::check_and_update_sequence: producer_id={}, epoch={}, topic_id={}, partition_id={}, base_seq={}, count={}",
            producer_id,
            producer_epoch,
            topic_id,
            partition_id,
            base_sequence,
            record_count
        );

        Spi::connect_mut(|client| {
            // Step 1: Verify producer epoch (fencing check)
            let epoch_table = client.select(
                "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                Some(1),
                &[producer_id.into()],
            )?;

            if epoch_table.is_empty() {
                return Err(KafkaError::unknown_producer_id(producer_id));
            }

            let current_epoch: i16 = epoch_table
                .first()
                .get_by_name("epoch")?
                .unwrap_or(0);

            if producer_epoch < current_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            // Step 2: Use advisory lock to serialize sequence checks for this producer+partition
            // Combine producer_id, topic_id, partition_id into a single bigint lock key
            // Using XOR and shifts to create a unique hash that fits in i64
            let lock_key: i64 = producer_id
                ^ ((topic_id as i64) << 20)
                ^ ((partition_id as i64) << 40);
            client.select(
                "SELECT pg_advisory_xact_lock($1)",
                None,
                &[lock_key.into()],
            )?;

            // Step 3: Get current last_sequence for this producer/topic/partition
            let seq_table = client.select(
                "SELECT last_sequence FROM kafka.producer_sequences
                 WHERE producer_id = $1 AND topic_id = $2 AND partition_id = $3",
                Some(1),
                &[producer_id.into(), topic_id.into(), partition_id.into()],
            )?;

            let last_sequence: i32 = if seq_table.is_empty() {
                -1 // First batch for this producer/partition
            } else {
                seq_table
                    .first()
                    .get_by_name("last_sequence")?
                    .unwrap_or(-1)
            };

            let expected_sequence = last_sequence + 1;

            // Step 4: Validate sequence
            if base_sequence < expected_sequence {
                // Duplicate - sequence is behind expected
                // Kafka spec: Return success (don't error), skip insert, don't update sequence
                crate::pg_debug!(
                    "Duplicate detected: producer_id={}, partition_id={}, base_sequence={}, expected={}",
                    producer_id,
                    partition_id,
                    base_sequence,
                    expected_sequence
                );
                return Ok(false); // Duplicate - skip insert
            } else if base_sequence > expected_sequence {
                // Gap - sequence is ahead of expected
                return Err(KafkaError::out_of_order_sequence(
                    producer_id,
                    partition_id,
                    base_sequence,
                    expected_sequence,
                ));
            }

            // Step 5: Sequence is valid - update last_sequence
            let new_last_sequence = base_sequence + record_count - 1;

            client.update(
                "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id, last_sequence)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (producer_id, topic_id, partition_id)
                 DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = NOW()",
                None,
                &[
                    producer_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    new_last_sequence.into(),
                ],
            )?;

            crate::pg_debug!(
                "Updated sequence for producer_id={}, partition_id={}: {} -> {}",
                producer_id,
                partition_id,
                last_sequence,
                new_last_sequence
            );

            Ok(true) // Valid sequence - proceed with insert
        })
        .map_err(|e| match e {
            KafkaError::UnknownProducerId { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::DuplicateSequence { .. }
            | KafkaError::OutOfOrderSequence { .. } => e,
            _ => KafkaError::Internal(format!("check_and_update_sequence failed: {}", e)),
        })
    }

    // ===== Transaction Operations (Phase 10) =====

    fn get_or_create_transactional_producer(
        &self,
        transactional_id: &str,
        transaction_timeout_ms: i32,
        client_id: Option<&str>,
    ) -> Result<(i64, i16)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_transactional_producer: transactional_id={}, timeout_ms={}",
            transactional_id,
            transaction_timeout_ms
        );

        Spi::connect_mut(|client| {
            // Look up existing producer by transactional_id
            let existing = client.select(
                "SELECT producer_id, epoch FROM kafka.producer_ids WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            let (producer_id, epoch) = if existing.is_empty() {
                // New transactional producer - allocate producer_id
                let table = client.update(
                    "INSERT INTO kafka.producer_ids (client_id, transactional_id, epoch)
                     VALUES ($1, $2, 0)
                     RETURNING producer_id, epoch",
                    None,
                    &[client_id.into(), transactional_id.into()],
                )?;

                let row = table.first();
                let producer_id: i64 = row
                    .get_by_name("producer_id")?
                    .ok_or_else(|| KafkaError::Internal("Failed to get producer_id".into()))?;
                let epoch: i16 = row.get_by_name("epoch")?.unwrap_or(0);

                crate::pg_debug!(
                    "Allocated new transactional producer_id={}, epoch={}",
                    producer_id,
                    epoch
                );
                (producer_id, epoch)
            } else {
                // Existing transactional producer - bump epoch (fences old producer)
                let row = existing.first();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);

                let table = client.update(
                    "UPDATE kafka.producer_ids
                     SET epoch = epoch + 1, last_active_at = NOW()
                     WHERE producer_id = $1
                     RETURNING epoch",
                    None,
                    &[producer_id.into()],
                )?;

                let new_epoch: i16 = table
                    .first()
                    .get_by_name("epoch")?
                    .ok_or_else(|| KafkaError::Internal("Failed to get new epoch".into()))?;

                crate::pg_debug!(
                    "Bumped epoch for existing transactional producer_id={}, new_epoch={}",
                    producer_id,
                    new_epoch
                );
                (producer_id, new_epoch)
            };

            // Create or update transaction record with state='Empty'
            client.update(
                "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch, state, timeout_ms)
                 VALUES ($1, $2, $3, 'Empty', $4)
                 ON CONFLICT (transactional_id) DO UPDATE SET
                     producer_id = EXCLUDED.producer_id,
                     producer_epoch = EXCLUDED.producer_epoch,
                     state = 'Empty',
                     timeout_ms = EXCLUDED.timeout_ms,
                     started_at = NULL,
                     last_updated_at = NOW()",
                None,
                &[
                    transactional_id.into(),
                    producer_id.into(),
                    epoch.into(),
                    transaction_timeout_ms.into(),
                ],
            )?;

            Ok((producer_id, epoch))
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_or_create_transactional_producer failed: {}", e))
        })
    }

    fn begin_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Verify ownership and update state to 'Ongoing'
            let table = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing', started_at = NOW(), last_updated_at = NOW()
                 WHERE transactional_id = $1 AND producer_id = $2 AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort')
                 RETURNING state",
                None,
                &[transactional_id.into(), producer_id.into(), producer_epoch.into()],
            )?;

            if table.is_empty() {
                // Check if transaction exists with different producer/epoch
                let exists = client.select(
                    "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                    Some(1),
                    &[transactional_id.into()],
                )?;

                if exists.is_empty() {
                    return Err(KafkaError::transactional_id_not_found(transactional_id));
                }

                let row = exists.first();
                let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
                let state: String = row.get_by_name("state")?.unwrap_or_default();

                if current_producer_id != producer_id || current_epoch != producer_epoch {
                    return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
                }

                // State is not valid for beginning a transaction
                return Err(KafkaError::invalid_txn_state(
                    transactional_id,
                    "Empty, CompleteCommit, or CompleteAbort",
                    &state,
                ));
            }

            crate::pg_debug!("Transaction {} started", transactional_id);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("begin_transaction failed: {}", e)),
        })
    }

    fn begin_or_continue_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::begin_or_continue_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Atomic compare-and-swap: transition to 'Ongoing' if in valid starting state
            // OR continue if already 'Ongoing' with matching producer
            let updated = client.update(
                "UPDATE kafka.transactions
                 SET state = 'Ongoing',
                     started_at = COALESCE(started_at, NOW()),
                     last_updated_at = NOW()
                 WHERE transactional_id = $1
                   AND producer_id = $2
                   AND producer_epoch = $3
                   AND state IN ('Empty', 'CompleteCommit', 'CompleteAbort', 'Ongoing')
                 RETURNING state",
                None,
                &[transactional_id.into(), producer_id.into(), producer_epoch.into()],
            )?;

            if !updated.is_empty() {
                crate::pg_debug!("Transaction {} is now Ongoing", transactional_id);
                return Ok(());
            }

            // Update failed - check why
            let exists = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if exists.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = exists.first();
            let current_producer_id: i64 =
                row.get_by_name("producer_id")?.ok_or_else(|| KafkaError::Database {
                    message: "Unexpected NULL in transactions.producer_id column".to_string(),
                })?;
            let current_epoch: i16 =
                row.get_by_name("producer_epoch")?.ok_or_else(|| KafkaError::Database {
                    message: "Unexpected NULL in transactions.producer_epoch column".to_string(),
                })?;
            let state: String = row.get_by_name("state")?.ok_or_else(|| KafkaError::Database {
                message: "Unexpected NULL in transactions.state column".to_string(),
            })?;

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(
                    producer_id,
                    producer_epoch,
                    current_epoch,
                ));
            }

            // State is not valid for beginning a transaction
            Err(KafkaError::invalid_txn_state(
                transactional_id,
                "Empty, CompleteCommit, or CompleteAbort",
                &state,
            ))
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("begin_or_continue_transaction failed: {}", e)),
        })
    }

    fn validate_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::validate_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect(|client| {
            let table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
            }

            // Note: We only validate ownership here, not state.
            // State validation is done by the handlers which know the expected states
            // for their specific operations (e.g., AddPartitionsToTxn allows Empty or Ongoing).
            let _ = state; // Acknowledge we read state but don't check it here

            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("validate_transaction failed: {}", e)),
        })
    }

    fn insert_transactional_records(
        &self,
        topic_id: i32,
        partition_id: i32,
        records: &[Record],
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<i64> {
        if records.is_empty() {
            return Ok(0);
        }

        crate::pg_debug!(
            "PostgresStore::insert_transactional_records: {} records for topic_id={}, partition_id={}, producer_id={}",
            records.len(),
            topic_id,
            partition_id,
            producer_id
        );

        Spi::connect_mut(|client| {
            // Step 1: Lock the partition using advisory lock
            client.select(
                "SELECT pg_advisory_xact_lock($1, $2)",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            // Step 2: Get current max offset
            let table = client.select(
                "SELECT COALESCE(MAX(partition_offset), -1) as max_offset
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let max_offset: i64 = table
                .first()
                .get_by_name::<i64, _>("max_offset")?
                .unwrap_or(-1);

            let base_offset = max_offset + 1;

            // Step 3: Build parallel arrays for UNNEST-based bulk insert
            let count = records.len();
            let topic_ids: Vec<i32> = vec![topic_id; count];
            let partition_ids: Vec<i32> = vec![partition_id; count];
            let offsets: Vec<i64> = (0..count).map(|i| base_offset + i as i64).collect();
            let keys: Vec<Option<Vec<u8>>> = records.iter().map(|r| r.key.clone()).collect();
            let values: Vec<Option<Vec<u8>>> = records.iter().map(|r| r.value.clone()).collect();
            let headers: Vec<String> = records
                .iter()
                .map(|r| {
                    if r.headers.is_empty() {
                        "{}".to_string()
                    } else {
                        let headers_map: HashMap<String, String> = r
                            .headers
                            .iter()
                            .map(|h| (h.key.clone(), hex_encode(&h.value)))
                            .collect();
                        serde_json::to_string(&headers_map).unwrap_or_else(|_| "{}".to_string())
                    }
                })
                .collect();
            let producer_ids: Vec<i64> = vec![producer_id; count];
            let producer_epochs: Vec<i16> = vec![producer_epoch; count];
            let txn_states: Vec<&str> = vec!["pending"; count];

            // Execute single INSERT with UNNEST including transaction columns
            client
                .update(
                    "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value, headers, producer_id, producer_epoch, txn_state)
                     SELECT * FROM unnest($1::int[], $2::int[], $3::bigint[], $4::bytea[], $5::bytea[], $6::jsonb[], $7::bigint[], $8::smallint[], $9::text[])",
                    None,
                    &[
                        topic_ids.into(),
                        partition_ids.into(),
                        offsets.into(),
                        keys.into(),
                        values.into(),
                        headers.into(),
                        producer_ids.into(),
                        producer_epochs.into(),
                        txn_states.into(),
                    ],
                )
                .map_err(|e| KafkaError::Internal(format!("Failed to insert transactional records: {}", e)))?;

            crate::pg_debug!(
                "Successfully inserted {} transactional records (offsets {} to {})",
                records.len(),
                base_offset,
                base_offset + records.len() as i64 - 1
            );

            Ok(base_offset)
        })
        .map_err(|e| match e {
            KafkaError::Internal(_) => e,
            _ => KafkaError::Internal(format!("insert_transactional_records failed: {}", e)),
        })
    }

    fn store_txn_pending_offset(
        &self,
        transactional_id: &str,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::store_txn_pending_offset: txn_id={}, group_id={}, topic_id={}, partition_id={}, offset={}",
            transactional_id,
            group_id,
            topic_id,
            partition_id,
            offset
        );

        Spi::connect_mut(|client| {
            client.update(
                "INSERT INTO kafka.txn_pending_offsets (transactional_id, group_id, topic_id, partition_id, pending_offset, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (transactional_id, group_id, topic_id, partition_id)
                 DO UPDATE SET pending_offset = EXCLUDED.pending_offset, metadata = EXCLUDED.metadata",
                None,
                &[
                    transactional_id.into(),
                    group_id.into(),
                    topic_id.into(),
                    partition_id.into(),
                    offset.into(),
                    metadata.into(),
                ],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("store_txn_pending_offset failed: {}", e))
        })
    }

    fn commit_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::commit_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Step 1: Validate transaction state
            let txn_table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if txn_table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = txn_table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
            }

            if state != "Ongoing" {
                return Err(KafkaError::invalid_txn_state(transactional_id, "Ongoing", &state));
            }

            // Step 2: Make messages visible (set txn_state = NULL)
            client.update(
                "UPDATE kafka.messages SET txn_state = NULL
                 WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                None,
                &[producer_id.into(), producer_epoch.into()],
            )?;

            // Step 3: Move pending offsets to consumer_offsets
            client.update(
                "INSERT INTO kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset, metadata)
                 SELECT group_id, topic_id, partition_id, pending_offset, metadata
                 FROM kafka.txn_pending_offsets
                 WHERE transactional_id = $1
                 ON CONFLICT (group_id, topic_id, partition_id) DO UPDATE SET
                     committed_offset = EXCLUDED.committed_offset,
                     metadata = EXCLUDED.metadata,
                     commit_timestamp = NOW()",
                None,
                &[transactional_id.into()],
            )?;

            // Step 4: Delete pending offsets
            client.update(
                "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;

            // Step 5: Update transaction state to CompleteCommit
            client.update(
                "UPDATE kafka.transactions SET state = 'CompleteCommit', last_updated_at = NOW()
                 WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;

            crate::pg_debug!("Transaction {} committed", transactional_id);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("commit_transaction failed: {}", e)),
        })
    }

    fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::abort_transaction: transactional_id={}, producer_id={}, epoch={}",
            transactional_id,
            producer_id,
            producer_epoch
        );

        Spi::connect_mut(|client| {
            // Step 1: Validate transaction state
            let txn_table = client.select(
                "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if txn_table.is_empty() {
                return Err(KafkaError::transactional_id_not_found(transactional_id));
            }

            let row = txn_table.first();
            let current_producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
            let current_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);
            let state: String = row.get_by_name("state")?.unwrap_or_default();

            if current_producer_id != producer_id || current_epoch != producer_epoch {
                return Err(KafkaError::producer_fenced(producer_id, producer_epoch, current_epoch));
            }

            if state != "Ongoing" {
                return Err(KafkaError::invalid_txn_state(transactional_id, "Ongoing", &state));
            }

            // Step 2: Mark messages as aborted
            client.update(
                "UPDATE kafka.messages SET txn_state = 'aborted'
                 WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                None,
                &[producer_id.into(), producer_epoch.into()],
            )?;

            // Step 3: Delete pending offsets
            client.update(
                "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;

            // Step 4: Update transaction state to CompleteAbort
            client.update(
                "UPDATE kafka.transactions SET state = 'CompleteAbort', last_updated_at = NOW()
                 WHERE transactional_id = $1",
                None,
                &[transactional_id.into()],
            )?;

            crate::pg_debug!("Transaction {} aborted", transactional_id);
            Ok(())
        })
        .map_err(|e| match e {
            KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidTxnState { .. } => e,
            _ => KafkaError::Internal(format!("abort_transaction failed: {}", e)),
        })
    }

    fn get_transaction_state(&self, transactional_id: &str) -> Result<Option<TransactionState>> {
        crate::pg_debug!(
            "PostgresStore::get_transaction_state: transactional_id={}",
            transactional_id
        );

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                Some(1),
                &[transactional_id.into()],
            )?;

            if let Some(row) = table.next() {
                let state_str: String = row.get_by_name("state")?.unwrap_or_default();
                Ok(TransactionState::parse(&state_str))
            } else {
                Ok(None)
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_transaction_state failed: {}", e))
        })
    }

    fn fetch_records_with_isolation(
        &self,
        topic_id: i32,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<FetchedMessage>> {
        crate::pg_debug!(
            "PostgresStore::fetch_records_with_isolation: topic_id={}, partition_id={}, offset={}, isolation={:?}",
            topic_id,
            partition_id,
            fetch_offset,
            isolation_level
        );

        let limit = (max_bytes / 100).clamp(100, 10_000);

        Spi::connect(|client| {
            let query = match isolation_level {
                IsolationLevel::ReadUncommitted => {
                    // Return all records including pending
                    "SELECT partition_offset, key, value,
                            EXTRACT(EPOCH FROM created_at)::bigint * 1000 as timestamp_ms
                     FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
                     ORDER BY partition_offset
                     LIMIT $4"
                }
                IsolationLevel::ReadCommitted => {
                    // Filter out pending and aborted records
                    "SELECT partition_offset, key, value,
                            EXTRACT(EPOCH FROM created_at)::bigint * 1000 as timestamp_ms
                     FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2 AND partition_offset >= $3
                       AND (txn_state IS NULL)
                     ORDER BY partition_offset
                     LIMIT $4"
                }
            };

            let table = client.select(
                query,
                None,
                &[
                    topic_id.into(),
                    partition_id.into(),
                    fetch_offset.into(),
                    limit.into(),
                ],
            )?;

            let mut messages = Vec::new();
            for row in table {
                let partition_offset: i64 = row.get_by_name("partition_offset")?.unwrap_or(0);
                let key: Option<Vec<u8>> = row.get_by_name("key")?;
                let value: Option<Vec<u8>> = row.get_by_name("value")?;
                let timestamp: i64 = row.get_by_name("timestamp_ms")?.unwrap_or(0);

                messages.push(FetchedMessage {
                    partition_offset,
                    key,
                    value,
                    timestamp,
                });
            }

            crate::pg_debug!(
                "Fetched {} messages with isolation {:?}",
                messages.len(),
                isolation_level
            );
            Ok(messages)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("fetch_records_with_isolation failed: {}", e))
        })
    }

    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64> {
        crate::pg_debug!(
            "PostgresStore::get_last_stable_offset: topic_id={}, partition_id={}",
            topic_id,
            partition_id
        );

        Spi::connect(|client| {
            // Get the minimum offset of pending messages, or high watermark if none
            let table = client.select(
                "SELECT COALESCE(
                    (SELECT MIN(partition_offset) FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2 AND txn_state = 'pending'),
                    (SELECT COALESCE(MAX(partition_offset) + 1, 0) FROM kafka.messages
                     WHERE topic_id = $1 AND partition_id = $2)
                 ) as lso",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let lso: i64 = table.first().get_by_name("lso")?.unwrap_or(0);
            Ok(lso)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_last_stable_offset failed: {}", e))
        })
    }

    fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>> {
        let timeout_secs = timeout.as_secs() as i64;
        crate::pg_debug!(
            "PostgresStore::abort_timed_out_transactions: timeout={}s",
            timeout_secs
        );

        Spi::connect_mut(|client| {
            // Find and abort timed-out transactions
            let table = client.select(
                "SELECT transactional_id, producer_id, producer_epoch
                 FROM kafka.transactions
                 WHERE state = 'Ongoing'
                   AND started_at < NOW() - ($1 || ' seconds')::interval",
                None,
                &[timeout_secs.into()],
            )?;

            let mut aborted = Vec::new();

            for row in table {
                let txn_id: String = row.get_by_name("transactional_id")?.unwrap_or_default();
                let producer_id: i64 = row.get_by_name("producer_id")?.unwrap_or(0);
                let producer_epoch: i16 = row.get_by_name("producer_epoch")?.unwrap_or(0);

                // Mark messages as aborted
                client.update(
                    "UPDATE kafka.messages SET txn_state = 'aborted'
                     WHERE producer_id = $1 AND producer_epoch = $2 AND txn_state = 'pending'",
                    None,
                    &[producer_id.into(), producer_epoch.into()],
                )?;

                // Delete pending offsets
                client.update(
                    "DELETE FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                    None,
                    &[txn_id.clone().into()],
                )?;

                // Update transaction state
                client.update(
                    "UPDATE kafka.transactions SET state = 'CompleteAbort', last_updated_at = NOW()
                     WHERE transactional_id = $1",
                    None,
                    &[txn_id.clone().into()],
                )?;

                crate::pg_debug!("Aborted timed-out transaction: {}", txn_id);
                aborted.push(txn_id);
            }

            Ok(aborted)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("abort_timed_out_transactions failed: {}", e))
        })
    }

    fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64> {
        let older_than_secs = older_than.as_secs() as i64;
        crate::pg_debug!(
            "PostgresStore::cleanup_aborted_messages: older_than={}s",
            older_than_secs
        );

        Spi::connect_mut(|client| {
            // Use DELETE ... RETURNING to count deleted rows
            let table = client.select(
                "DELETE FROM kafka.messages
                 WHERE txn_state = 'aborted'
                   AND created_at < NOW() - ($1 || ' seconds')::interval
                 RETURNING 1",
                None,
                &[older_than_secs.into()],
            )?;

            let deleted = table.len() as u64;
            crate::pg_debug!("Deleted {} aborted messages", deleted);
            Ok(deleted)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("cleanup_aborted_messages failed: {}", e))
        })
    }
}
