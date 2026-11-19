//! Committed consumer-group offset SQL.

use super::PostgresStore;
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::storage::CommittedOffset;
use pgrx::prelude::*;

impl PostgresStore {
    pub(super) fn commit_offset(
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

    pub(super) fn fetch_offset(
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

    pub(super) fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>> {
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

    // ===== Admin Consumer Group Operations (Phase 6) =====

    pub(super) fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()> {
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

    pub(super) fn list_offset_groups(&self) -> Result<Vec<String>> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT DISTINCT group_id FROM kafka.consumer_offsets ORDER BY group_id",
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
        .map_err(|e: KafkaError| KafkaError::Internal(format!("list_offset_groups failed: {}", e)))
    }

    pub(super) fn group_has_offsets(&self, group_id: &str) -> Result<bool> {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT 1 FROM kafka.consumer_offsets WHERE group_id = $1 LIMIT 1",
                Some(1),
                &[group_id.into()],
            )?;
            Ok(!table.is_empty())
        })
        .map_err(|e: KafkaError| KafkaError::Internal(format!("group_has_offsets failed: {}", e)))
    }

    pub(super) fn pending_txn_offset_partitions(&self, group_id: &str) -> Result<Vec<(String, i32)>> {
        // Wave 3 (KIP-447 require_stable): rows exist only while the transaction that
        // wrote them is open (EndTxn, the timeout sweep and InitProducerId delete them).
        Spi::connect(|client| {
            let table = client.select(
                "SELECT DISTINCT t.name, p.partition_id
                 FROM kafka.txn_pending_offsets p JOIN kafka.topics t ON t.id = p.topic_id
                 WHERE p.group_id = $1",
                None,
                &[group_id.into()],
            )?;
            let mut out = Vec::new();
            for row in table {
                let name: String = row.get_by_name("name")?.unwrap_or_default();
                let partition: i32 = row.get_by_name("partition_id")?.unwrap_or(0);
                out.push((name, partition));
            }
            Ok(out)
        })
        .map_err(|e: KafkaError| {
            KafkaError::Internal(format!("pending_txn_offset_partitions failed: {}", e))
        })
    }
}
