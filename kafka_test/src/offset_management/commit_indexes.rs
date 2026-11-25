//! What an offset commit writes to the indexes of kafka.consumer_offsets.

use super::fetch_parity::commit;
use crate::common::{hot_blocking_columns, DynResult, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::Wire;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_postgres::Client;

/// The committed row of (group, topic, partition 0), column by column, as text.
async fn offset_row(
    db: &Client,
    group: &str,
    topic: &str,
) -> DynResult<BTreeMap<String, Option<String>>> {
    Ok(db
        .query(
            "SELECT c.key, c.value
             FROM kafka.consumer_offsets co
             JOIN kafka.topics t ON t.id = co.topic_id,
             jsonb_each_text(to_jsonb(co)) c
             WHERE co.group_id = $1 AND t.name = $2 AND co.partition_id = 0",
            &[&group, &topic],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}

/// An offset commit of a partition the group committed before updates its row's
/// committed_offset, metadata and commit_timestamp. With an index on commit_timestamp
/// (read by no query) such an update was never HOT, and with a second index on group_id
/// (the primary key's leading column already) every commit wrote four index entries.
///
/// Asserts: kafka.consumer_offsets has only its primary key and idx_consumer_offsets_topic,
/// and a second OffsetCommit of a partition (answered NONE, a new offset and metadata,
/// 20 ms later) changes no column an index keys, filters on or uses in an expression.
pub async fn test_offset_commit_changes_no_indexed_column() -> TestResult {
    println!("=== Test: an offset commit changes no indexed column ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "commit-hot", 1).await?;
    let group = ctx.unique_group("commit-hot").await;

    let indexes: Vec<String> = db
        .query(
            "SELECT indexrelid::regclass::text FROM pg_index
             WHERE indrelid = 'kafka.consumer_offsets'::regclass ORDER BY 1",
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    println!("Step 1: indexes {indexes:?}");
    assert_eq!(
        indexes,
        vec![
            "kafka.consumer_offsets_pkey".to_string(),
            "kafka.idx_consumer_offsets_topic".to_string()
        ]
    );

    let mut w = Wire::connect().await?;
    assert_eq!(
        commit(&mut w, &group, &t, &[(0, 5, Some("a".into()))]).await,
        vec![error_codes::NONE]
    );
    let first = offset_row(db, &group, &t).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        commit(&mut w, &group, &t, &[(0, 9, Some("b".into()))]).await,
        vec![error_codes::NONE]
    );
    let second = offset_row(db, &group, &t).await?;
    let changed: Vec<&String> = second
        .iter()
        .filter(|(col, v)| first.get(*col) != Some(*v))
        .map(|(col, _)| col)
        .collect();
    let indexed = hot_blocking_columns(db, "kafka.consumer_offsets").await?;
    println!("Step 2: changed by the commit: {changed:?}; indexed: {indexed:?}");
    assert!(changed.iter().any(|c| *c == "committed_offset"));
    let blocking: Vec<&&String> = changed.iter().filter(|c| indexed.contains(c)).collect();
    assert!(
        blocking.is_empty(),
        "an offset commit changed indexed columns {blocking:?} (never HOT)"
    );
    ctx.cleanup().await?;
    println!("✅ offset commits maintain two indexes and can stay HOT");
    Ok(())
}
