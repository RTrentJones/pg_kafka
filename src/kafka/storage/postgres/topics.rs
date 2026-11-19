//! Topic catalog SQL: lookup, auto-create, admin operations and per-topic config.

use super::PostgresStore;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::TopicMetadata;
use pgrx::prelude::*;

impl PostgresStore {
    pub(super) fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)> {
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

    pub(super) fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        crate::pg_debug!("PostgresStore::get_topic_metadata");

        Spi::connect(|client| {
            let mut topics = Vec::new();

            if let Some(topic_names) = names {
                // Fetch the requested topics in ONE statement (was one query per
                // name, an N+1 on every Metadata request), then restore the
                // requested order in Rust.
                if topic_names.is_empty() {
                    return Ok(topics);
                }
                let table = client.select(
                    "SELECT id, name, partitions FROM kafka.topics WHERE name = ANY($1::text[])",
                    None,
                    &[topic_names.to_vec().into()],
                )?;
                let mut rows = Vec::with_capacity(topic_names.len());
                for row in table {
                    let id: i32 = row.get_by_name("id")?.unwrap_or(0);
                    let name: String = row.get_by_name("name")?.unwrap_or_default();
                    let partitions: i32 = row.get_by_name("partitions")?.unwrap_or(1);
                    rows.push((name, id, partitions));
                }
                topics = crate::kafka::storage::order_topic_metadata(topic_names, rows);
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

    // ===== Admin Topic Operations (Phase 6) =====

    pub(super) fn topic_exists(&self, name: &str) -> Result<bool> {
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

    pub(super) fn create_topic(&self, name: &str, partition_count: i32) -> Result<i32> {
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

    pub(super) fn get_topic_id(&self, name: &str) -> Result<Option<i32>> {
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

    pub(super) fn delete_topic(&self, topic_id: i32) -> Result<()> {
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

    pub(super) fn get_topic_partition_count(&self, name: &str) -> Result<Option<i32>> {
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

    pub(super) fn set_topic_partition_count(&self, name: &str, partition_count: i32) -> Result<()> {
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

    // ===== wave3 groups: topic-name collisions, groups known only by their offsets =====

    pub(super) fn find_colliding_topic(&self, name: &str) -> Result<Option<String>> {
        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT name FROM kafka.topics
                 WHERE replace(name, '.', '_') = replace($1, '.', '_') AND name <> $1
                 ORDER BY name LIMIT 1",
                Some(1),
                &[name.into()],
            )?;
            match table.next() {
                Some(row) => Ok(row.get_by_name::<String, _>("name")?),
                None => Ok(None),
            }
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("find_colliding_topic failed: {}", e))
        })
    }

    pub(super) fn get_topic_retention_ms(&self, topic_id: i32) -> Result<Option<i64>> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT retention_ms FROM kafka.topics WHERE id = $1",
                Some(1),
                &[topic_id.into()],
            )?;
            if table.is_empty() {
                return Err(KafkaError::Internal(format!(
                    "topic id {} not found",
                    topic_id
                )));
            }
            Ok(table.first().get_by_name::<i64, _>("retention_ms")?)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("get_topic_retention_ms failed: {}", e))
        })
    }

    pub(super) fn set_topic_retention_ms(&self, topic_id: i32, retention_ms: Option<i64>) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::set_topic_retention_ms: topic_id={}, retention_ms={:?}",
            topic_id,
            retention_ms
        );
        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.topics SET retention_ms = $2 WHERE id = $1",
                None,
                &[topic_id.into(), retention_ms.into()],
            )?;
            Ok(())
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("set_topic_retention_ms failed: {}", e))
        })
    }
}
