//! Wave 3 stream `shadow`: shadow-mode checks that need no external Kafka, so
//! they run in the functional suite (category `w3_shadow`).

use crate::common::TestResult;
use crate::setup::TestContext;

/// kafka.shadow_partition_status / kafka.shadow_status report lag per
/// partition: the number of outbox rows still to forward (exact under
/// sampling, 0 when caught up), dead letters separately. The old
/// shadow_status.lag was MAX(offset) - MAX(last_forwarded_offset) across all
/// partitions of the topic.
///
/// The outbox rows are written by hand for offsets that have no message, so the
/// worker's outbox poll (which only claims rows whose message exists) leaves
/// them alone even when shadow mode is on.
pub async fn test_shadow_lag_view_is_per_partition() -> TestResult {
    println!("=== Test: shadow lag view is per partition (wave 3) ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("w3-lag").await;
    let db = ctx.db();
    let topic_id: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 3) RETURNING id",
            &[&topic],
        )
        .await?
        .get(0);
    let cap: i32 = db
        .query_one(
            "SELECT current_setting('pg_kafka.shadow_max_retries')::int",
            &[],
        )
        .await?
        .get(0);
    // Partition 0: three rows still to forward (one of them in flight).
    // Partition 1: one delivered, one dead letter.
    // Partition 2: everything delivered (forwarded up to 41), lag must be 0.
    db.execute(
        "INSERT INTO kafka.shadow_tracking \
            (topic_id, partition_id, local_offset, external_offset, retry_count, attempt_open) \
         VALUES ($1, 0, 1000, NULL, 0, false), ($1, 0, 1001, NULL, 1, false), \
                ($1, 0, 1002, NULL, 0, true), \
                ($1, 1, 1000, 7, 0, false), ($1, 1, 1001, NULL, $2, false), \
                ($1, 2, 1000, 8, 0, false)",
        &[&topic_id, &cap],
    )
    .await?;
    db.execute(
        "INSERT INTO kafka.shadow_metrics \
            (topic_id, partition_id, messages_forwarded, messages_skipped, last_forwarded_offset) \
         VALUES ($1, 1, 1, 2, 1000), ($1, 2, 42, 0, 41)",
        &[&topic_id],
    )
    .await?;

    let rows = db
        .query(
            "SELECT partition_id, lag, dead_lettered, in_flight, oldest_pending_offset \
             FROM kafka.shadow_partition_status WHERE topic_id = $1 ORDER BY partition_id",
            &[&topic_id],
        )
        .await?;
    let got: Vec<(i32, i64, i64, i64, Option<i64>)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect();
    println!("per-partition: {got:?}");
    assert_eq!(
        got,
        vec![
            (0, 3, 0, 1, Some(1000)),
            (1, 0, 1, 0, None),
            (2, 0, 0, 0, None),
        ]
    );

    let row = db
        .query_one(
            "SELECT lag, dead_lettered, max_partition_lag, total_forwarded, total_skipped \
             FROM kafka.shadow_status WHERE topic_name = $1",
            &[&topic],
        )
        .await?;
    let summary: (i64, i64, i64, i64, i64) =
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
    println!("topic: {summary:?}");
    assert_eq!(summary, (3, 1, 3, 43, 2));

    db.execute(
        "DELETE FROM kafka.shadow_metrics WHERE topic_id = $1",
        &[&topic_id],
    )
    .await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The `w3_shadow` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["w3_shadow";
        test_shadow_lag_view_is_per_partition,
    ]
}
