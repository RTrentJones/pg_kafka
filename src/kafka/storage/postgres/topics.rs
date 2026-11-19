//! Topic catalog SQL: lookup, auto-create, admin operations and per-topic config.
//!
//! `kafka.topics` columns are NOT NULL except `retention_ms`, so the `unwrap_or`
//! fallbacks on rows that exist are unreachable; they never mask a real NULL.

use super::{internal, PostgresStore};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::{order_topic_metadata, TopicMetadata};
use pgrx::prelude::*;
use pgrx::spi::SpiTupleTable;

/// `(name, id, partition_count)` of every row of an `id, name, partitions` select.
fn topic_rows(table: SpiTupleTable<'_>) -> Result<Vec<(String, i32, i32)>> {
    let mut rows = Vec::with_capacity(table.len());
    for row in table {
        rows.push((
            row.get_by_name("name")?.unwrap_or_default(),
            row.get_by_name("id")?.unwrap_or(0),
            row.get_by_name("partitions")?.unwrap_or(1),
        ));
    }
    Ok(rows)
}

impl PostgresStore {
    pub(super) fn get_or_create_topic(name: &str, default_partitions: i32) -> Result<(i32, i32)> {
        crate::pg_debug!(
            "PostgresStore::get_or_create_topic: '{name}' (default_partitions={default_partitions})"
        );

        Spi::connect_mut(|client| {
            let table = client.update(
                "INSERT INTO kafka.topics (name, partitions)
                 VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                 RETURNING id, partitions",
                None,
                &[name.into(), default_partitions.into()],
            )?;

            let row = table.first();
            let topic_id: i32 = row
                .get_by_name("id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;
            let partition_count: i32 = row
                .get_by_name("partitions")?
                .ok_or_else(|| KafkaError::Internal("Failed to get partition count".into()))?;

            crate::pg_debug!("Topic '{name}' has id={topic_id}, partitions={partition_count}");
            Ok((topic_id, partition_count))
        })
        .map_err(internal("get_or_create_topic"))
    }

    /// The named topics in request order (one statement, not one per name), or every
    /// topic when `names` is `None`.
    pub(super) fn get_topic_metadata(names: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        crate::pg_debug!("PostgresStore::get_topic_metadata");

        Spi::connect(|client| match names {
            Some([]) => Ok(Vec::new()),
            Some(topic_names) => {
                let table = client.select(
                    "SELECT id, name, partitions FROM kafka.topics WHERE name = ANY($1::text[])",
                    None,
                    &[topic_names.to_vec().into()],
                )?;
                Ok(order_topic_metadata(topic_names, topic_rows(table)?))
            }
            None => {
                let table =
                    client.select("SELECT id, name, partitions FROM kafka.topics", None, &[])?;
                Ok(topic_rows(table)?
                    .into_iter()
                    .map(|(name, id, partition_count)| TopicMetadata {
                        name,
                        id,
                        partition_count,
                    })
                    .collect())
            }
        })
        .map_err(internal("get_topic_metadata"))
    }

    pub(super) fn topic_exists(name: &str) -> Result<bool> {
        crate::pg_debug!("PostgresStore::topic_exists: '{name}'");

        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;
            Ok(!table.is_empty())
        })
        .map_err(internal("topic_exists"))
    }

    pub(super) fn create_topic(name: &str, partition_count: i32) -> Result<i32> {
        crate::pg_debug!("PostgresStore::create_topic: '{name}' with {partition_count} partitions");

        Spi::connect_mut(|client| {
            let topic_id: i32 = client
                .update(
                    "INSERT INTO kafka.topics (name, partitions)
                     VALUES ($1, $2)
                     RETURNING id",
                    None,
                    &[name.into(), partition_count.into()],
                )?
                .first()
                .get_by_name("id")?
                .ok_or_else(|| KafkaError::Internal("Failed to get topic ID".into()))?;

            crate::pg_debug!("Created topic '{name}' with id={topic_id}");
            Ok(topic_id)
        })
        .map_err(internal("create_topic"))
    }

    pub(super) fn get_topic_id(name: &str) -> Result<Option<i32>> {
        crate::pg_debug!("PostgresStore::get_topic_id: '{name}'");

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT id FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;
            match table.next() {
                Some(row) => Ok(Some(row.get_by_name("id")?.unwrap_or(0))),
                None => Ok(None),
            }
        })
        .map_err(internal("get_topic_id"))
    }

    /// Delete a topic with its messages and committed offsets (the other per-topic rows
    /// go with it through ON DELETE CASCADE).
    pub(super) fn delete_topic(topic_id: i32) -> Result<()> {
        crate::pg_debug!("PostgresStore::delete_topic: topic_id={topic_id}");

        Spi::connect_mut(|client| {
            for sql in [
                "DELETE FROM kafka.messages WHERE topic_id = $1",
                "DELETE FROM kafka.consumer_offsets WHERE topic_id = $1",
                "DELETE FROM kafka.topics WHERE id = $1",
            ] {
                client.update(sql, None, &[topic_id.into()])?;
            }
            crate::pg_debug!("Deleted topic with id={topic_id}");
            Ok(())
        })
        .map_err(internal("delete_topic"))
    }

    pub(super) fn get_topic_partition_count(name: &str) -> Result<Option<i32>> {
        crate::pg_debug!("PostgresStore::get_topic_partition_count: '{name}'");

        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT partitions FROM kafka.topics WHERE name = $1",
                Some(1),
                &[name.into()],
            )?;
            match table.next() {
                Some(row) => Ok(Some(row.get_by_name("partitions")?.unwrap_or(1))),
                None => Ok(None),
            }
        })
        .map_err(internal("get_topic_partition_count"))
    }

    pub(super) fn set_topic_partition_count(name: &str, partition_count: i32) -> Result<()> {
        crate::pg_debug!("PostgresStore::set_topic_partition_count: '{name}' to {partition_count}");

        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.topics SET partitions = $2 WHERE name = $1",
                None,
                &[name.into(), partition_count.into()],
            )?;
            Ok(())
        })
        .map_err(internal("set_topic_partition_count"))
    }

    /// A topic whose name equals `name` once '.' and '_' are treated as equal (Kafka's
    /// metric-name collision rule), other than `name` itself.
    pub(super) fn find_colliding_topic(name: &str) -> Result<Option<String>> {
        Spi::connect(|client| {
            let mut table = client.select(
                "SELECT name FROM kafka.topics
                 WHERE replace(name, '.', '_') = replace($1, '.', '_') AND name <> $1
                 ORDER BY name
                 LIMIT 1",
                Some(1),
                &[name.into()],
            )?;
            match table.next() {
                Some(row) => Ok(row.get_by_name::<String, _>("name")?),
                None => Ok(None),
            }
        })
        .map_err(internal("find_colliding_topic"))
    }

    pub(super) fn get_topic_retention_ms(topic_id: i32) -> Result<Option<i64>> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT retention_ms FROM kafka.topics WHERE id = $1",
                Some(1),
                &[topic_id.into()],
            )?;
            if table.is_empty() {
                return Err(KafkaError::Internal(format!(
                    "topic id {topic_id} not found"
                )));
            }
            Ok(table.first().get_by_name::<i64, _>("retention_ms")?)
        })
        .map_err(internal("get_topic_retention_ms"))
    }

    pub(super) fn set_topic_retention_ms(topic_id: i32, retention_ms: Option<i64>) -> Result<()> {
        crate::pg_debug!(
            "PostgresStore::set_topic_retention_ms: topic_id={topic_id}, retention_ms={retention_ms:?}"
        );
        Spi::connect_mut(|client| {
            client.update(
                "UPDATE kafka.topics SET retention_ms = $2 WHERE id = $1",
                None,
                &[topic_id.into(), retention_ms.into()],
            )?;
            Ok(())
        })
        .map_err(internal("set_topic_retention_ms"))
    }
}
