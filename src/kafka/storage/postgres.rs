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
    fn get_or_create_topic(&self, name: &str) -> Result<i32> {
        pgrx::debug1!("PostgresStore::get_or_create_topic: '{}'", name);

        Spi::connect(|_client| {
            let topic_name_string = name.to_string();

            let topic_id: i32 = Spi::get_one_with_args::<i32>(
                "INSERT INTO kafka.topics (name, partitions)
                 VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                 RETURNING id",
                &[
                    topic_name_string.into(),
                    crate::kafka::DEFAULT_TOPIC_PARTITIONS.into(),
                ],
            )?
            .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;

            pgrx::debug1!("Topic '{}' has id={}", name, topic_id);
            Ok(topic_id)
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("get_or_create_topic failed: {}", e)))
    }

    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        pgrx::debug1!("PostgresStore::get_topic_metadata");

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

        pgrx::debug1!(
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

            pgrx::debug1!(
                "Current max_offset={}, new base_offset={}",
                max_offset,
                base_offset
            );

            // Step 3: Insert all records
            for (i, record) in records.iter().enumerate() {
                let offset = base_offset + i as i64;

                // Serialize headers to JSONB
                let headers_json = if record.headers.is_empty() {
                    "{}".to_string()
                } else {
                    let headers_map: HashMap<String, String> = record
                        .headers
                        .iter()
                        .map(|h| (h.key.clone(), hex_encode(&h.value)))
                        .collect();
                    serde_json::to_string(&headers_map).map_err(|e| {
                        KafkaError::Internal(format!("Failed to serialize headers: {}", e))
                    })?
                };

                let key_vec: Option<Vec<u8>> = record.key.clone();
                let value_vec: Option<Vec<u8>> = record.value.clone();

                client.update(
                    "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value, headers)
                     VALUES ($1, $2, $3, $4, $5, $6::jsonb)",
                    None,
                    &[
                        topic_id.into(),
                        partition_id.into(),
                        offset.into(),
                        key_vec.into(),
                        value_vec.into(),
                        headers_json.into(),
                    ],
                ).map_err(|e| KafkaError::Internal(format!("Failed to insert record: {}", e)))?;
            }

            pgrx::debug1!(
                "Successfully inserted {} records (offsets {} to {})",
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
        pgrx::debug1!(
            "PostgresStore::fetch_records: topic_id={}, partition_id={}, fetch_offset={}, max_bytes={}",
            topic_id,
            partition_id,
            fetch_offset,
            max_bytes
        );

        // Calculate limit based on max_bytes (rough estimate: ~1KB per message)
        let limit = std::cmp::min(max_bytes / 1024, 1000);

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

            pgrx::debug1!("Fetched {} messages", messages.len());
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

    fn commit_offset(
        &self,
        group_id: &str,
        topic_id: i32,
        partition_id: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<()> {
        pgrx::debug1!(
            "PostgresStore::commit_offset: group_id={}, topic_id={}, partition_id={}, offset={}",
            group_id,
            topic_id,
            partition_id,
            offset
        );

        // SQL injection mitigation: escape single quotes
        // TODO: Use prepared statements when pgrx adds support
        let group_id_escaped = group_id.replace("'", "''");
        let metadata_escaped = metadata.map(|m| m.replace("'", "''"));

        Spi::connect_mut(|client| {
            let query = if let Some(meta) = metadata_escaped {
                format!(
                    "INSERT INTO kafka.consumer_offsets
                        (group_id, topic_id, partition_id, committed_offset, metadata)
                    VALUES ('{}', {}, {}, {}, '{}')
                    ON CONFLICT (group_id, topic_id, partition_id)
                    DO UPDATE SET
                        committed_offset = EXCLUDED.committed_offset,
                        metadata = EXCLUDED.metadata,
                        commit_timestamp = NOW()",
                    group_id_escaped, topic_id, partition_id, offset, meta
                )
            } else {
                format!(
                    "INSERT INTO kafka.consumer_offsets
                        (group_id, topic_id, partition_id, committed_offset, metadata)
                    VALUES ('{}', {}, {}, {}, NULL)
                    ON CONFLICT (group_id, topic_id, partition_id)
                    DO UPDATE SET
                        committed_offset = EXCLUDED.committed_offset,
                        metadata = EXCLUDED.metadata,
                        commit_timestamp = NOW()",
                    group_id_escaped, topic_id, partition_id, offset
                )
            };

            client.update(&query, None, &[])?;
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
        // SQL injection mitigation
        let group_id_escaped = group_id.replace("'", "''");

        Spi::connect(|client| {
            let query = format!(
                "SELECT committed_offset, metadata
                 FROM kafka.consumer_offsets
                 WHERE group_id = '{}' AND topic_id = {} AND partition_id = {}",
                group_id_escaped, topic_id, partition_id
            );

            let mut table = client.select(&query, Some(1), &[])?;

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
        // SQL injection mitigation
        let group_id_escaped = group_id.replace("'", "''");

        Spi::connect(|client| {
            let query = format!(
                "SELECT t.name, co.partition_id, co.committed_offset, co.metadata
                 FROM kafka.consumer_offsets co
                 JOIN kafka.topics t ON co.topic_id = t.id
                 WHERE co.group_id = '{}'
                 ORDER BY t.name, co.partition_id",
                group_id_escaped
            );

            let table = client.select(&query, None, &[])?;
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
}
