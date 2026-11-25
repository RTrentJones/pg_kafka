//! The retention sweep under a session DateStyle that prints zone abbreviations.

use super::drop_extension::MAINTENANCE_LOCK_KEY;
use super::sweep_budget::{create_topic, fill_leaf, hour_leaf};
use crate::common::TestResult;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use tokio_postgres::Client;

/// Rows of each topic the DateStyle test puts in its leaf.
const ROWS: i64 = 20;
/// Sweeps the DateStyle test runs per setting: the row expiry is done in the first
/// (default budget, one leaf), the rest are slack.
const SWEEPS: usize = 3;

/// (DateStyle, TimeZone) pairs whose printed timestamptz carries an abbreviation that
/// PostgreSQL 14 to 17 read back through `timezone_abbreviations`, not TimeZone:
/// Asia/Kolkata's IST as Israel's +02:00 (3 h 30 later), Asia/Shanghai's CST as US
/// Central -06:00 (14 h later). Either shift moves a one-hour leaf's bounds past it.
const SETTINGS: [(&str, &str); 2] = [
    ("SQL, DMY", "Asia/Kolkata"),
    ("Postgres, MDY", "Asia/Shanghai"),
];

/// The sweep carries each leaf's bounds from `kafka.message_partitions()` back into
/// the leaf-confined deletes. As text under the session's DateStyle they read back
/// shifted by the abbreviation, and the delete's range left part or all of the leaf
/// out: its expired rows were never deleted, by any later sweep either.
///
/// Asserts: for each setting, with the worker's sweep held off by the maintenance
/// lock, a past one-hour leaf holding rows of a 1 s retention topic (expired row by
/// row) and of an infinite one (keeping the leaf), sweeps run in a session with that
/// DateStyle and TimeZone delete every expired row and keep the infinite topic's.
pub async fn test_sweep_bounds_survive_the_session_datestyle() -> TestResult {
    into_test_result(datestyle_sweep().await)
}

async fn datestyle_sweep() -> SendResult<()> {
    println!("=== Test: the sweep's leaf bounds survive the session DateStyle ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    // The worker's own sweep (every 60 s, default DateStyle) skips while this session
    // holds the lock, so only this session's sweeps can delete the rows; those re-enter it.
    db.execute("SELECT pg_advisory_lock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed)?;
    let mut topics = Vec::new();
    let mut leaves = Vec::new();
    let result = run(&db, &mut topics, &mut leaves).await;
    let cleaned = cleanup(&db, &topics, &leaves).await;
    let unlocked = db
        .execute("SELECT pg_advisory_unlock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed);
    result?;
    cleaned?;
    unlocked?;
    println!("✅ every expired row was deleted under each DateStyle");
    Ok(())
}

async fn run(db: &Client, topics: &mut Vec<i32>, leaves: &mut Vec<String>) -> SendResult<()> {
    // Before 2000, so no other test's leaves are there.
    let base: i32 = db
        .query_one("SELECT -20 - (random() * 8000)::int", &[])
        .await
        .map_err(boxed)?
        .get(0);
    for (k, (datestyle, timezone)) in (0i32..).zip(SETTINGS) {
        let short = create_topic(db, &unique("datestyle-short"), 1000).await?;
        topics.push(short);
        let forever = create_topic(db, &unique("datestyle-forever"), -1).await?;
        topics.push(forever);
        let hour = base + k;
        leaves.push(hour_leaf(db, hour).await?);
        fill_leaf(db, short, hour, 0, ROWS).await?;
        fill_leaf(db, forever, hour, 0, ROWS).await?;
        let leaf = &leaves[leaves.len() - 1];

        db.batch_execute(&format!(
            "SET datestyle = '{datestyle}'; SET timezone = '{timezone}'"
        ))
        .await
        .map_err(boxed)?;
        let shift: String = db
            .query_one(
                "SELECT (lower_bound::text::timestamptz - lower_bound)::text
                 FROM kafka.message_partitions() WHERE partition_oid = $1::text::regclass",
                &[leaf],
            )
            .await
            .map_err(boxed)?
            .get(0);
        println!(
            "Step {k}: {leaf} holds {ROWS} expired and {ROWS} kept rows; under \
             DateStyle '{datestyle}', TimeZone {timezone} its bound reads back {shift} off"
        );
        for _ in 0..SWEEPS {
            db.query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
                .await
                .map_err(boxed)?;
        }
        db.batch_execute("RESET datestyle; RESET timezone")
            .await
            .map_err(boxed)?;
        let left = count(db, short).await?;
        let kept = count(db, forever).await?;
        println!("  after {SWEEPS} sweeps: {left} expired rows left, {kept} kept rows");
        if left != 0 {
            return Err(format!(
                "{left} expired rows left in {leaf} under DateStyle '{datestyle}', {timezone}"
            )
            .into());
        }
        if kept != ROWS {
            return Err(format!("the infinite topic's rows must stay: {kept} left").into());
        }
    }
    Ok(())
}

async fn count(db: &Client, topic_id: i32) -> SendResult<i64> {
    Ok(db
        .query_one(
            "SELECT count(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

async fn cleanup(db: &Client, topics: &[i32], leaves: &[String]) -> SendResult<()> {
    db.batch_execute("RESET datestyle; RESET timezone")
        .await
        .map_err(boxed)?;
    db.execute(
        "DELETE FROM kafka.messages WHERE topic_id = ANY($1)",
        &[&topics],
    )
    .await
    .map_err(boxed)?;
    for leaf in leaves {
        db.query(
            "SELECT dropped FROM kafka.drop_message_partition(to_regclass($1), '{}')",
            &[leaf],
        )
        .await
        .map_err(boxed)?;
    }
    db.execute("DELETE FROM kafka.topics WHERE id = ANY($1)", &[&topics])
        .await
        .map_err(boxed)?;
    Ok(())
}
