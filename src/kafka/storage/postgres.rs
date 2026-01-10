// PostgreSQL implementation of the KafkaStore trait
//
// This module implements the storage layer using PostgreSQL and pgrx SPI.
// It assumes it runs within a transaction context managed by the caller.

use super::{CommittedOffset, FetchedMessage, KafkaStore, TopicMetadata};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::messages::Record;
use pgrx::prelude::*;
use std::collections::HashMap;

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
}
