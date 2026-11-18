//! Regression tests for the `storage` workstream: retention catch-up, single-statement
//! fetch bounds, the request-level Fetch `max_bytes`, batched topic metadata, and the
//! least-privilege worker role.
//!
//! The changed code lives in `storage/postgres.rs` (SPI) and the worker's role switch,
//! which unit coverage cannot see, so each test drives the broker or the SQL entry point
//! and checks the stored state.

use crate::common::TestResult;
use crate::setup::TestContext;

/// Number of expired rows seeded by the retention test: well above the old fixed
/// 10,000-row cap per sweep pass.
const RETENTION_BACKLOG: i64 = 25_000;

/// Create a topic directly in SQL (no broker round trip) and return its id.
async fn create_topic_sql(
    ctx: &TestContext,
    name: &str,
    partitions: i32,
    retention_ms: Option<i64>,
) -> Result<i32, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, $2, $3) RETURNING id",
            &[&name, &partitions, &retention_ms],
        )
        .await?
        .get(0))
}

/// The retention sweep must catch up on a backlog in one pass instead of deleting a
/// single 10,000-row batch per pass (which let the table grow without bound above
/// ~167 records/s), and must still advance the partition's durable log start.
///
/// Fails before the fix: one `pg_kafka_run_retention_sweep()` call left 15,000 of the
/// 25,000 expired rows behind.
pub async fn test_retention_sweep_catches_up_on_backlog() -> TestResult {
    println!("=== Test: Retention Sweep Catches Up On A Backlog ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("storage-retention-backlog").await;
    // retention.ms = 1 hour: every seeded row (created a day ago) is expired.
    let topic_id = create_topic_sql(&ctx, &topic, 1, Some(3_600_000)).await?;

    println!("Step 1: seed {} expired rows...", RETENTION_BACKLOG);
    ctx.db()
        .execute(
            "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
             SELECT $1, 0, g, 'v'::bytea, NOW() - INTERVAL '1 day' + g * INTERVAL '1 millisecond'
             FROM generate_series(0::bigint, $2::bigint - 1) AS g",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;
    ctx.db()
        .execute(
            "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
             VALUES ($1, 0, $2)",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;
    // Two fresh rows that must survive the sweep.
    ctx.db()
        .execute(
            "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
             VALUES ($1, 0, $2, 'fresh'::bytea), ($1, 0, $2 + 1, 'fresh'::bytea)",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;
    ctx.db()
        .execute(
            "UPDATE kafka.partition_offsets SET next_offset = $2 + 2 WHERE topic_id = $1",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;

    println!("Step 2: one on-demand sweep...");
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await?;

    let remaining: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await?
        .get(0);
    let log_start: i64 = ctx
        .db()
        .query_one(
            "SELECT log_start_offset FROM kafka.partition_offsets WHERE topic_id = $1 AND partition_id = 0",
            &[&topic_id],
        )
        .await?
        .get(0);
    println!(
        "   remaining rows={} log_start_offset={}",
        remaining, log_start
    );

    if remaining != 2 {
        return Err(format!(
            "expected only the 2 fresh rows to survive one sweep, found {} rows \
             (the sweep did not catch up on the backlog)",
            remaining
        )
        .into());
    }
    assert_eq!(
        log_start, RETENTION_BACKLOG,
        "log_start_offset must advance to the oldest surviving offset"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
