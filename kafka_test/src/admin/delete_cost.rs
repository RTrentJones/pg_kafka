//! DeleteTopics' cost: the per-topic rows of other tables go through the foreign keys'
//! ON DELETE CASCADE, so each deleted topic must find its rows by index, not by a scan
//! of every row of every topic.

use crate::common::{create_admin_client, create_db_client, TestResult};
use rdkafka::admin::AdminOptions;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// An error with its details (a database error's Display is only "db error").
fn dbg<E: std::fmt::Debug>(e: E) -> String {
    format!("{e:?}")
}

/// Topics one DeleteTopics request deletes.
const TOPICS: i64 = 40;
/// Rows of another topic seeded in each of the four tables that reference kafka.topics
/// without a primary key leading on topic_id.
const CHILD_ROWS: i64 = 500_000;
/// What the request may take. Scanning the four tables once per deleted topic took
/// 11.4 s on PG17 with 1,000,000 rows in each, about half that with these 500,000, and
/// more on a loaded machine; with the indexes the whole request took 12 ms.
const DELETE_LIMIT: Duration = Duration::from_millis(1500);

/// Each table that references kafka.topics(id) ON DELETE CASCADE is indexed on topic_id
/// (consumer_offsets, producer_sequences, txn_pending_offsets and txn_partitions have
/// primary keys that lead with another column), so a deleted topic's cascade reads only
/// that topic's rows. Without the indexes every deleted topic read each of the four
/// tables whole, all on the worker's single DB thread.
///
/// Asserts: with 500,000 rows of another topic in each of the four tables, one
/// DeleteTopics request for 40 topics (each with a row in every table) succeeds for
/// every topic within 1.5 s and removes their rows, and the other topic's rows stay.
pub async fn test_delete_topics_cascade_is_indexed() -> TestResult {
    println!("=== Test: a multi-topic DeleteTopics finds each topic's rows by index ===\n");
    let db = create_db_client().await.map_err(dbg)?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(dbg)?;
    let prefix = format!("delete-cost-{}", &Uuid::new_v4().to_string()[..8]);
    let bulk: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1 || '-bulk', 1) RETURNING id",
            &[&prefix],
        )
        .await
        .map_err(dbg)?
        .get(0);
    let txn_id = format!("{prefix}-txn");
    // A negative id the producer-id sequence never issues.
    let producer_id: i64 = db
        .query_one(
            "INSERT INTO kafka.producer_ids (producer_id, transactional_id)
             VALUES (-(floor(random() * 1000000)::bigint + 1) * 1000000, $1)
             RETURNING producer_id",
            &[&txn_id],
        )
        .await
        .map_err(dbg)?
        .get(0);
    db.execute(
        "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch, state)
         VALUES ($1, $2, 0, 'Empty')",
        &[&txn_id, &producer_id],
    )
    .await
    .map_err(dbg)?;

    // As a String: the boxed error is not Send, and the cleanup awaits after it.
    let result = delete_with_seeded_children(&db, &prefix, bulk, &txn_id)
        .await
        .map_err(|e| e.to_string());

    // The bulk topic's rows go with it (by its index now); then the transaction.
    db.execute("DELETE FROM kafka.topics WHERE id = $1", &[&bulk])
        .await
        .map_err(dbg)?;
    db.execute(
        "DELETE FROM kafka.transactions WHERE transactional_id = $1",
        &[&txn_id],
    )
    .await
    .map_err(dbg)?;
    db.execute(
        "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
        &[&producer_id],
    )
    .await
    .map_err(dbg)?;
    result?;
    println!("✅ the cascade of every deleted topic stayed cheap");
    Ok(())
}

async fn delete_with_seeded_children(
    db: &tokio_postgres::Client,
    prefix: &str,
    bulk: i32,
    txn_id: &str,
) -> TestResult {
    let t = Instant::now();
    db.batch_execute(&format!(
        "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id)
             SELECT g, {bulk}, 0 FROM generate_series(1, {CHILD_ROWS}) AS g;
         INSERT INTO kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset)
             SELECT 'g' || g, {bulk}, 0, 0 FROM generate_series(1, {CHILD_ROWS}) AS g;
         INSERT INTO kafka.txn_pending_offsets
                (transactional_id, group_id, topic_id, partition_id, pending_offset)
             SELECT '{txn_id}', 'g' || g, {bulk}, 0, 0 FROM generate_series(1, {CHILD_ROWS}) AS g;
         INSERT INTO kafka.txn_partitions (transactional_id, topic_id, partition_id)
             SELECT '{txn_id}', {bulk}, g FROM generate_series(0, {CHILD_ROWS} - 1) AS g;"
    ))
    .await
    .map_err(dbg)?;
    let names: Vec<String> = (0..TOPICS).map(|k| format!("{prefix}-{k}")).collect();
    db.execute(
        "WITH t AS (
             INSERT INTO kafka.topics (name, partitions)
             SELECT unnest($1::text[]), 1 RETURNING id),
         s AS (INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id)
               SELECT 1, id, 0 FROM t),
         o AS (INSERT INTO kafka.consumer_offsets
                      (group_id, topic_id, partition_id, committed_offset)
               SELECT 'g1', id, 0, 0 FROM t),
         p AS (INSERT INTO kafka.txn_pending_offsets
                      (transactional_id, group_id, topic_id, partition_id, pending_offset)
               SELECT $2, 'g1', id, 0, 0 FROM t)
         INSERT INTO kafka.txn_partitions (transactional_id, topic_id, partition_id)
         SELECT $2, id, 0 FROM t",
        &[&names, &txn_id],
    )
    .await
    .map_err(dbg)?;
    db.batch_execute(
        "ANALYZE kafka.producer_sequences, kafka.consumer_offsets, kafka.txn_pending_offsets,
                 kafka.txn_partitions",
    )
    .await
    .map_err(dbg)?;
    println!(
        "Step 0: {CHILD_ROWS} rows of another topic in each of 4 tables, {TOPICS} topics \
         with a row in each ({:?})",
        t.elapsed()
    );

    let admin = create_admin_client()?;
    let opts = AdminOptions::new()
        .operation_timeout(Some(Duration::from_secs(120)))
        .request_timeout(Some(Duration::from_secs(120)));
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let t = Instant::now();
    let results = admin.delete_topics(&refs, &opts).await.map_err(dbg)?;
    let took = t.elapsed();
    println!("Step 1: DeleteTopics of {TOPICS} topics took {took:?}");
    let failed: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    if !failed.is_empty() {
        return Err(format!("{} deletes failed: {failed:?}", failed.len()).into());
    }
    if took >= DELETE_LIMIT {
        return Err(format!("DeleteTopics of {TOPICS} topics took {took:?}").into());
    }

    let row = db
        .query_one(
            "SELECT (SELECT count(*) FROM kafka.topics WHERE name = ANY($1)),
                    (SELECT count(*) FROM kafka.producer_sequences WHERE topic_id = $2),
                    (SELECT count(*) FROM kafka.txn_partitions WHERE topic_id = $2),
                    (SELECT count(*) FROM kafka.producer_sequences ps
                     WHERE ps.producer_id = 1 AND ps.topic_id <> $2)",
            &[&names, &bulk],
        )
        .await
        .map_err(dbg)?;
    let (left, bulk_seqs, bulk_parts, deleted_seqs): (i64, i64, i64, i64) =
        (row.get(0), row.get(1), row.get(2), row.get(3));
    println!(
        "Step 2: {left} topics left; the other topic keeps {bulk_seqs} sequences and \
         {bulk_parts} txn partitions"
    );
    if left != 0 {
        return Err(format!("{left} topics still exist").into());
    }
    if bulk_seqs != CHILD_ROWS || bulk_parts != CHILD_ROWS {
        return Err("the other topic's rows must stay".into());
    }
    // The deleted topics' own sequence rows (producer 1 on each of them) are gone.
    if deleted_seqs != 0 {
        return Err(format!("{deleted_seqs} sequences of the deleted topics left").into());
    }
    Ok(())
}
