//! Committed consumer-group offset SQL. `committed_offset` is NOT NULL, so its
//! `unwrap_or(-1)` fallback on a row that exists is unreachable.

use super::{internal, PostgresStore};
use crate::kafka::error::Result;
use crate::kafka::storage::{CommittedOffset, GroupTopicOffsets, OffsetGroupPage};
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
            "PostgresStore::commit_offset: topic_id={}, partition_id={}, offset={}",
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

    /// Every committed offset of a group by topic, sorted, the offsets past
    /// `max_metadata_bytes` as `None` (see [`OffsetStore::fetch_all_offsets`]).
    ///
    /// The statement drops the metadata of the rows past the budget and sends each topic's
    /// name once, on its first row, and its result is read a chunk at a time
    /// ([`read_in_chunks`]), so the DB thread holds the metadata once, at most
    /// `max_metadata_bytes` (plus the first row's), each name once, and O(1) per row for
    /// the rest. The sort that orders the rows is the executor's (bounded by work_mem, then
    /// spilled).
    ///
    /// [`OffsetStore::fetch_all_offsets`]: crate::kafka::storage::OffsetStore::fetch_all_offsets
    pub(super) fn fetch_all_offsets(
        group_id: &str,
        max_metadata_bytes: usize,
    ) -> Result<Vec<GroupTopicOffsets>> {
        let budget = i64::try_from(max_metadata_bytes).unwrap_or(i64::MAX);
        let mut topics: Vec<GroupTopicOffsets> = Vec::new();
        read_in_chunks(
            "SELECT CASE WHEN first_of_topic THEN name END AS name, partition_id,
                    committed_offset, CASE WHEN kept THEN metadata END AS metadata, kept
             FROM (SELECT t.name, co.partition_id, co.committed_offset, co.metadata,
                          lag(t.name) OVER w IS DISTINCT FROM t.name AS first_of_topic,
                          coalesce(octet_length(co.metadata), 0) = 0
                          OR row_number() OVER w = 1
                          OR coalesce(sum(octet_length(co.metadata)) OVER w, 0) <= $2 AS kept
                   FROM kafka.consumer_offsets co
                   JOIN kafka.topics t ON co.topic_id = t.id
                   WHERE co.group_id = $1
                   WINDOW w AS (ORDER BY t.name, co.partition_id ROWS UNBOUNDED PRECEDING)) s
             ORDER BY s.name, partition_id",
            &[group_id.into(), budget.into()],
            |row| {
                if let Some(name) = row.get_by_name::<String, _>("name")? {
                    topics.push((name, Vec::new()));
                }
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                let offset = kept_offset(row)?;
                if let Some((_, partitions)) = topics.last_mut() {
                    partitions.push((partition_id, offset));
                }
                Ok(())
            },
        )
        .map_err(internal("fetch_all_offsets"))?;
        Ok(topics)
    }

    /// The committed offsets of `group_id` for `partitions` (`(topic_id, partition_id)`),
    /// as `(topic_id, partition_id, offset)`, the offsets past `max_metadata_bytes` (counted
    /// in the order of `partitions`) as `None`: index lookups of the requested partitions
    /// only, however many offsets the group has, read as [`Self::fetch_all_offsets`] reads.
    pub(super) fn fetch_offsets(
        group_id: &str,
        partitions: &[(i32, i32)],
        max_metadata_bytes: usize,
    ) -> Result<Vec<(i32, i32, Option<CommittedOffset>)>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let (topic_ids, partition_ids): (Vec<i32>, Vec<i32>) = partitions.iter().copied().unzip();
        let budget = i64::try_from(max_metadata_bytes).unwrap_or(i64::MAX);
        read_in_chunks(
            "SELECT topic_id, partition_id, committed_offset,
                    CASE WHEN kept THEN metadata END AS metadata, kept
             FROM (SELECT co.topic_id, co.partition_id, co.committed_offset, co.metadata,
                          coalesce(octet_length(co.metadata), 0) = 0
                          OR row_number() OVER w = 1
                          OR coalesce(sum(octet_length(co.metadata)) OVER w, 0) <= $4 AS kept
                   FROM unnest($2::int[], $3::int[]) WITH ORDINALITY AS r(topic_id, partition_id, ord)
                   JOIN kafka.consumer_offsets co
                     ON co.group_id = $1
                    AND co.topic_id = r.topic_id
                    AND co.partition_id = r.partition_id
                   WINDOW w AS (ORDER BY r.ord ROWS UNBOUNDED PRECEDING)) s",
            &[
                group_id.into(),
                topic_ids.into(),
                partition_ids.into(),
                budget.into(),
            ],
            |row| {
                let topic_id: i32 = row.get_by_name("topic_id")?.unwrap_or(0);
                let partition_id: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                Ok((topic_id, partition_id, kept_offset(row)?))
            },
        )
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

    /// The groups with offsets a ListGroups may list (see [`OffsetStore::list_offset_groups`]):
    /// their ids in byte order while the running total of `octet_length + LISTED_GROUP_OVERHEAD`
    /// before each stays within `max_bytes`, and whether another follows.
    ///
    /// A loose index scan: each step takes the next distinct id after the last one from the
    /// primary key of kafka.consumer_offsets and from idx_txn_pending_offsets_group (both lead
    /// with group_id, collated "C") and keeps the lower, so a group of many partitions costs
    /// two index probes like a group of one, and the walk stops one id past the budget. Cost:
    /// O(g log n) for the `g` ids returned (at most about `max_bytes / LISTED_GROUP_OVERHEAD`)
    /// over `n` offset rows, whatever the tables hold past them; memory: those ids. (Every
    /// row of both tables was read and sorted before: 504 ms for a million rows, on every
    /// ListGroups.) The order is byte order (`COLLATE "C"`, the columns' own collation) to
    /// agree with the handler's sort of the ids; under the database's collation the cut kept
    /// another set of groups than the handler then listed in byte order. The statement takes
    /// only ACCESS SHARE locks and reads no row lock, so it waits only for DDL on the tables.
    ///
    /// [`OffsetStore::list_offset_groups`]: crate::kafka::storage::OffsetStore::list_offset_groups
    pub(super) fn list_offset_groups(max_bytes: usize) -> Result<OffsetGroupPage> {
        let budget = i64::try_from(max_bytes).unwrap_or(i64::MAX);
        let overhead = i64::try_from(crate::kafka::storage::LISTED_GROUP_OVERHEAD).unwrap_or(0);
        let rows = read_in_chunks(
            r#"WITH RECURSIVE g(group_id, before) AS (
                 SELECT LEAST((SELECT o.group_id COLLATE "C" FROM kafka.consumer_offsets o
                               ORDER BY o.group_id COLLATE "C" LIMIT 1),
                              (SELECT p.group_id COLLATE "C" FROM kafka.txn_pending_offsets p
                               ORDER BY p.group_id COLLATE "C" LIMIT 1)),
                        0::bigint
                 UNION ALL
                 SELECT LEAST((SELECT o.group_id COLLATE "C" FROM kafka.consumer_offsets o
                               WHERE o.group_id COLLATE "C" > g.group_id
                               ORDER BY o.group_id COLLATE "C" LIMIT 1),
                              (SELECT p.group_id COLLATE "C" FROM kafka.txn_pending_offsets p
                               WHERE p.group_id COLLATE "C" > g.group_id
                               ORDER BY p.group_id COLLATE "C" LIMIT 1)),
                        g.before + octet_length(g.group_id) + $2
                 FROM g
                 WHERE g.group_id IS NOT NULL AND g.before <= $1
               )
               SELECT group_id, before <= $1 AS listed FROM g WHERE group_id IS NOT NULL"#,
            &[budget.into(), overhead.into()],
            |row| {
                Ok((
                    row.get_by_name::<String, _>("group_id")?
                        .unwrap_or_default(),
                    row.get_by_name::<bool, _>("listed")?.unwrap_or(false),
                ))
            },
        )
        .map_err(internal("list_offset_groups"))?;
        let mut page = OffsetGroupPage::default();
        for (group_id, listed) in rows {
            if listed {
                page.ids.push(group_id);
            } else {
                page.more = true;
            }
        }
        Ok(page)
    }

    pub(super) fn groups_with_offsets(group_ids: &[String]) -> Result<HashSet<String>> {
        if group_ids.is_empty() {
            return Ok(HashSet::new());
        }
        Spi::connect(|client| {
            // One index probe per asked group and table. `group_id = ANY($1)` in each
            // branch of a UNION instead filters both tables against the whole array, which
            // for a DescribeGroups of thousands of groups costs twice as long on the DB thread.
            let table = client.select(
                "SELECT g.group_id FROM unnest($1::text[]) AS g(group_id)
                 WHERE EXISTS (SELECT 1 FROM kafka.consumer_offsets c
                               WHERE c.group_id = g.group_id)
                 UNION
                 SELECT g.group_id FROM unnest($1::text[]) AS g(group_id)
                 WHERE EXISTS (SELECT 1 FROM kafka.txn_pending_offsets p
                               WHERE p.group_id = g.group_id)",
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

/// Rows of a chunked read ([`read_in_chunks`]): how many a chunk holds.
const READ_CHUNK_ROWS: i64 = 1024;

/// The rows of `query` (with `args`), each mapped by `row`, read through a cursor
/// [`READ_CHUNK_ROWS`] at a time, each chunk in its own SPI connection whose end frees that
/// chunk's tuple table: the DB thread holds one chunk of the result besides what `row`
/// keeps, where `select` would hold the whole result in SPI memory next to the copy made of
/// it.
fn read_in_chunks<T>(
    query: &str,
    args: &[pgrx::datum::DatumWithOid<'_>],
    mut row: impl FnMut(&spi::SpiHeapTupleData<'_>) -> spi::Result<T>,
) -> Result<Vec<T>> {
    let cursor = Spi::connect(|client| {
        Ok::<String, spi::SpiError>(client.try_open_cursor(query, args)?.detach_into_name())
    })?;
    let mut rows = Vec::new();
    loop {
        let fetched = Spi::connect(|client| {
            let mut cursor = client.find_cursor(&cursor)?;
            let mut fetched = 0_usize;
            for tuple in cursor.fetch(READ_CHUNK_ROWS)? {
                fetched += 1;
                rows.push(row(&tuple)?);
            }
            if fetched > 0 {
                // Keep the cursor open for the next chunk (dropping it closes it).
                cursor.detach_into_name();
            }
            Ok::<usize, spi::SpiError>(fetched)
        })?;
        if fetched == 0 {
            return Ok(rows);
        }
    }
}

/// The offset of a row with `committed_offset`, `metadata` and `kept` columns: `None` when
/// the row is past its read's metadata budget.
fn kept_offset(row: &spi::SpiHeapTupleData<'_>) -> spi::Result<Option<CommittedOffset>> {
    if !row.get_by_name::<bool, _>("kept")?.unwrap_or(false) {
        return Ok(None);
    }
    Ok(Some(CommittedOffset {
        offset: row.get_by_name("committed_offset")?.unwrap_or(-1),
        metadata: row.get_by_name("metadata")?,
    }))
}
