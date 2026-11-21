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

        // Look the topic up first and insert only a missing one: an upsert that updates the
        // existing row writes a new row version on every call, and one request that names
        // a topic thousands of times would then walk an ever longer version chain.
        Spi::connect_mut(|client| {
            let lookup = "SELECT id, partitions FROM kafka.topics WHERE name = $1";
            let mut table = client.select(lookup, Some(1), &[name.into()])?;
            if table.is_empty() {
                table = client.update(
                    "INSERT INTO kafka.topics (name, partitions)
                     VALUES ($1, $2)
                     ON CONFLICT (name) DO NOTHING
                     RETURNING id, partitions",
                    None,
                    &[name.into(), default_partitions.into()],
                )?;
            }
            if table.is_empty() {
                // Created by another session since the lookup.
                table = client.select(lookup, Some(1), &[name.into()])?;
            }

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

    pub(super) fn get_or_create_topics(
        names: &[String],
        partition_count: i32,
    ) -> Result<Vec<TopicMetadata>> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        Spi::connect_mut(|client| {
            // The insert's snapshot does not see its own rows, so the topics it created come
            // from RETURNING and the ones that already existed from the table.
            let table = client.update(
                "WITH created AS (
                     INSERT INTO kafka.topics (name, partitions)
                     SELECT n, $2 FROM unnest($1::text[]) AS n
                     ON CONFLICT (name) DO NOTHING
                     RETURNING id, name, partitions
                 )
                 SELECT id, name, partitions FROM created
                 UNION ALL
                 SELECT id, name, partitions FROM kafka.topics
                 WHERE name = ANY($1::text[])",
                None,
                &[names.to_vec().into(), partition_count.into()],
            )?;
            let mut rows = topic_rows(table)?;
            // A name another session inserted and committed while the insert waited on it
            // is in neither half: the insert skips it and the SELECT reads the statement's
            // snapshot, taken before that commit. A second statement, with a new snapshot,
            // sees it (as get_or_create_topic re-selects).
            let missing: Vec<String> = {
                let found: std::collections::HashSet<&str> =
                    rows.iter().map(|(name, _, _)| name.as_str()).collect();
                names
                    .iter()
                    .filter(|name| !found.contains(name.as_str()))
                    .cloned()
                    .collect()
            };
            if !missing.is_empty() {
                let table = client.select(
                    "SELECT id, name, partitions FROM kafka.topics WHERE name = ANY($1::text[])",
                    None,
                    &[missing.into()],
                )?;
                rows.extend(topic_rows(table)?);
            }
            Ok(rows
                .into_iter()
                .map(|(name, id, partition_count)| TopicMetadata {
                    name,
                    id,
                    partition_count,
                })
                .collect())
        })
        .map_err(internal("get_or_create_topics"))
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

    /// The topics whose name equals one of `names` once '.' and '_' are treated as equal
    /// (Kafka's metric-name collision rule), the names themselves included, sorted.
    pub(super) fn colliding_topics(names: &[String]) -> Result<Vec<String>> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let normalized: Vec<String> = names.iter().map(|n| n.replace('.', "_")).collect();
        Spi::connect(|client| {
            let table = client.select(
                "SELECT name FROM kafka.topics
                 WHERE replace(name, '.', '_') = ANY($1::text[])
                 ORDER BY name",
                None,
                &[normalized.into()],
            )?;
            let mut found = Vec::new();
            for row in table {
                if let Some(name) = row.get_by_name::<String, _>("name")? {
                    found.push(name);
                }
            }
            Ok(found)
        })
        .map_err(internal("colliding_topics"))
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
