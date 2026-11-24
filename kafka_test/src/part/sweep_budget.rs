//! Retention-sweep passes that run out of budget: what such passes still reach.

use crate::common::TestResult;
use crate::worker::{boxed, into_test_result, session, set_guc, unique, SendResult};
use tokio_postgres::Client;

/// Test-only GUC: the sweep's pass budget in ms (0 = every bounded step runs exactly
/// one statement per pass).
const PASS_BUDGET_GUC: &str = "pg_kafka.test_retention_pass_budget_ms";

/// Create the one-hour leaf starting `hour` hours after 2000-01-01 (a year no other
/// test's leaves use, so these are the oldest leaves) and return its name.
async fn hour_leaf(db: &Client, hour: i32) -> SendResult<String> {
    db.query_one(
        "SELECT kafka.create_message_partition(lo, lo + INTERVAL '1 hour')
         FROM (SELECT timestamptz '2000-01-01 00:00+00' + $1::int * INTERVAL '1 hour') AS b(lo)",
        &[&hour],
    )
    .await
    .map_err(boxed)?;
    Ok(db
        .query_one(
            "SELECT partition_oid::regclass::text FROM kafka.message_partitions()
             WHERE lower_bound = timestamptz '2000-01-01 00:00+00' + $1::int * INTERVAL '1 hour'",
            &[&hour],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// `rows` committed rows of `topic_id` in the leaf of `hour`, from `first_offset`.
async fn fill_leaf(
    db: &Client,
    topic_id: i32,
    hour: i32,
    first_offset: i64,
    rows: i64,
) -> SendResult<()> {
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
         SELECT $1, 0, $2::bigint + g, 'v'::bytea,
                timestamptz '2000-01-01 00:00+00' + $3::int * INTERVAL '1 hour'
                    + g * INTERVAL '1 second'
         FROM generate_series(0::bigint, $4::bigint - 1) AS g",
        &[&topic_id, &first_offset, &hour, &rows],
    )
    .await
    .map_err(boxed)?;
    Ok(())
}

async fn leaf_exists(db: &Client, leaf: &str) -> SendResult<bool> {
    Ok(db
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&leaf])
        .await
        .map_err(boxed)?
        .get(0))
}

async fn create_topic(db: &Client, name: &str, retention_ms: i64) -> SendResult<i32> {
    Ok(db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 1, $2)
             RETURNING id",
            &[&name, &retention_ms],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// The contents probes rotate over every candidate leaf whatever the list size, by the
/// probes each pass actually made. A pass whose budget other steps spent makes only its
/// one mandatory probe; if the rotation did not advance by it (lists of at most 64
/// candidates always restarted at the oldest), every such pass probed the same leaf,
/// and a wholly expired leaf further along was never dropped while the oldest stays
/// (here because it also holds an infinite-retention topic).
///
/// Asserts: with the pass budget at 0, the oldest past leaf holding an expired and an
/// infinite-retention topic, and the next one only the expired topic, repeated sweeps
/// drop the second leaf within two rotations over the probe candidates, and keep the
/// first leaf with the infinite-retention topic's rows.
pub async fn test_sweep_probe_rotation_reaches_every_leaf() -> TestResult {
    into_test_result(probe_rotation().await)
}

async fn probe_rotation() -> SendResult<()> {
    println!("=== Test: contents probes rotate over every leaf on a spent budget ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let expired = create_topic(&db, &unique("rot-expired"), 1000).await?;
    let forever = create_topic(&db, &unique("rot-forever"), -1).await?;
    let base_hour: i32 = db
        .query_one("SELECT (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let kept = hour_leaf(&db, base_hour).await?;
    let droppable = hour_leaf(&db, base_hour + 1).await?;
    fill_leaf(&db, expired, base_hour, 0, 20).await?;
    fill_leaf(&db, forever, base_hour, 0, 20).await?;
    fill_leaf(&db, expired, base_hour + 1, 20, 20).await?;
    println!("Step 0: {kept} (expired + infinite topic), {droppable} (expired topic only)");

    if !set_guc(&db, PASS_BUDGET_GUC, Some("0")).await? {
        return Err(format!("{PASS_BUDGET_GUC} is not known to the server").into());
    }
    let result: SendResult<()> = async {
        let candidates: i64 = db
            .query_one(
                "SELECT count(*) FROM kafka.message_partitions()
                 WHERE NOT is_default AND upper_bound <= now()",
                &[],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let passes = 2 * candidates + 2;
        let mut dropped_after = None;
        for pass in 1..=passes {
            db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
                .await
                .map_err(boxed)?;
            if !leaf_exists(&db, &droppable).await? {
                dropped_after = Some(pass);
                break;
            }
        }
        let Some(pass) = dropped_after else {
            return Err(format!(
                "{droppable} still exists after {passes} sweeps over {candidates} past leaves"
            )
            .into());
        };
        println!("Step 1: {droppable} dropped after {pass} sweep(s) ({candidates} past leaves)");
        assert!(leaf_exists(&db, &kept).await?, "{kept} must stay");
        let left: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {kept} WHERE topic_id = $1"),
                &[&forever],
            )
            .await
            .map_err(boxed)?
            .get(0);
        assert_eq!(left, 20, "the infinite-retention rows must stay");
        Ok(())
    }
    .await;
    set_guc(&db, PASS_BUDGET_GUC, None).await?;
    // The kept leaf holds only this test's topics: drop it as if both were expired.
    db.query(
        "SELECT dropped FROM kafka.drop_message_partition($1::text::regclass, ARRAY[$2::int, $3::int])",
        &[&kept, &expired, &forever],
    )
    .await
    .map_err(boxed)?;
    result?;
    println!("✅ every probe candidate was reached");
    Ok(())
}
