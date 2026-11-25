//! Committed consumer-group offset SQL. `committed_offset` is NOT NULL, so its
//! `unwrap_or(-1)` fallback on a row that exists is unreachable.

use super::{internal, PostgresStore};
use crate::kafka::error::Result;
use crate::kafka::storage::CommittedOffset;
use pgrx::prelude::*;
use std::collections::HashSet;

impl PostgresStore {
    pub(super) fn commit_offset(
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
        .map_err(internal("commit_offset"))
    }

    /// Every committed offset of a group as `(topic name, partition, offset)`, sorted.
    pub(super) fn fetch_all_offsets(group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>> {
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
        .map_err(internal("fetch_all_offsets"))
    }

    /// The committed offsets of `group_id` for `partitions` (`(topic_id, partition_id)`),
    /// as `(topic_id, partition_id, offset)`: index lookups of the requested partitions
    /// only, however many offsets the group has.
    pub(super) fn fetch_offsets(
        group_id: &str,
        partitions: &[(i32, i32)],
    ) -> Result<Vec<(i32, i32, CommittedOffset)>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let (topic_ids, partition_ids): (Vec<i32>, Vec<i32>) = partitions.iter().copied().unzip();
        Spi::connect(|client| {
            let table = client.select(
                "SELECT co.topic_id, co.partition_id, co.committed_offset, co.metadata
                 FROM unnest($2::int[], $3::int[]) AS r(topic_id, partition_id)
                 JOIN kafka.consumer_offsets co
                   ON co.group_id = $1
                  AND co.topic_id = r.topic_id
                  AND co.partition_id = r.partition_id",
                None,
                &[group_id.into(), topic_ids.into(), partition_ids.into()],
            )?;
            let mut results = Vec::with_capacity(table.len());
            for row in table {
                let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                let offset: i64 = row.get_by_name("committed_offset")?.unwrap_or(-1);
                let metadata: Option<String> = row.get_by_name("metadata")?;
                results.push((topic_id, partition_id, CommittedOffset { offset, metadata }));
            }
            Ok(results)
        })
        .map_err(internal("fetch_offsets"))
    }

    pub(super) fn delete_consumer_group_offsets(group_id: &str) -> Result<()> {
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
            // The group's pending transactional offsets go with it, as Kafka 3.7 drops
            // them with the group: otherwise the open transaction's commit would copy them
            // into consumer_offsets and bring the deleted group back.
            client.update(
                "DELETE FROM kafka.txn_pending_offsets WHERE group_id = $1",
                None,
                &[group_id.into()],
            )?;

            crate::pg_debug!("Deleted consumer offsets for group '{}'", group_id);
            Ok(())
        })
        .map_err(internal("delete_consumer_group_offsets"))
    }

    /// Every group with a committed offset or a pending transactional offset, sorted.
    pub(super) fn list_offset_groups() -> Result<Vec<String>> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT group_id FROM kafka.consumer_offsets
                 UNION
                 SELECT group_id FROM kafka.txn_pending_offsets
                 ORDER BY group_id",
                None,
                &[],
            )?;
            let mut groups = Vec::new();
            for row in table {
                if let Some(g) = row.get_by_name::<String, _>("group_id")? {
                    groups.push(g);
                }
            }
            Ok(groups)
        })
        .map_err(internal("list_offset_groups"))
    }

    pub(super) fn groups_with_offsets(group_ids: &[String]) -> Result<HashSet<String>> {
        if group_ids.is_empty() {
            return Ok(HashSet::new());
        }
        Spi::connect(|client| {
            let table = client.select(
                "SELECT group_id FROM kafka.consumer_offsets WHERE group_id = ANY($1::text[])
                 UNION
                 SELECT group_id FROM kafka.txn_pending_offsets
                 WHERE group_id = ANY($1::text[])",
                None,
                &[group_ids.to_vec().into()],
            )?;
            let mut groups = HashSet::new();
            for row in table {
                if let Some(g) = row.get_by_name::<String, _>("group_id")? {
                    groups.insert(g);
                }
            }
            Ok(groups)
        })
        .map_err(internal("groups_with_offsets"))
    }

    /// The `(topic name, partition)` pairs of `group_id` with a pending transactional
    /// offset: of `partitions` (`(topic_id, partition_id)`) only, by index lookups, when
    /// given; of the whole group otherwise. Both lead with the group on
    /// `idx_txn_pending_offsets_group`, so another group's open transaction is never read.
    pub(super) fn pending_txn_offset_partitions(
        group_id: &str,
        partitions: Option<&[(i32, i32)]>,
    ) -> Result<Vec<(String, i32)>> {
        // KIP-447 require_stable: rows exist only while the transaction that wrote them
        // is open (EndTxn, the timeout sweep and InitProducerId delete them).
        if partitions.is_some_and(<[(i32, i32)]>::is_empty) {
            return Ok(Vec::new());
        }
        Spi::connect(|client| {
            let table = match partitions {
                Some(partitions) => {
                    let (topic_ids, partition_ids): (Vec<i32>, Vec<i32>) =
                        partitions.iter().copied().unzip();
                    client.select(
                        "SELECT DISTINCT t.name, p.partition_id
                         FROM unnest($2::int[], $3::int[]) AS r(topic_id, partition_id)
                         JOIN kafka.txn_pending_offsets p
                           ON p.group_id = $1
                          AND p.topic_id = r.topic_id
                          AND p.partition_id = r.partition_id
                         JOIN kafka.topics t ON t.id = p.topic_id",
                        None,
                        &[group_id.into(), topic_ids.into(), partition_ids.into()],
                    )?
                }
                None => client.select(
                    "SELECT DISTINCT t.name, p.partition_id
                     FROM kafka.txn_pending_offsets p JOIN kafka.topics t ON t.id = p.topic_id
                     WHERE p.group_id = $1",
                    None,
                    &[group_id.into()],
                )?,
            };
            let mut out = Vec::new();
            for row in table {
                let name: String = row.get_by_name("name")?.unwrap_or_default();
                let partition: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                out.push((name, partition));
            }
            Ok(out)
        })
        .map_err(internal("pending_txn_offset_partitions"))
    }
}
