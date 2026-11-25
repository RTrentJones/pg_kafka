//! What ending a transaction reads: only its own producer's pending rows.

use crate::common::{create_transactional_producer, TestResult};
use crate::worker::{
    boxed, into_test_result, listener_pid, session, unique, wait_for_serving_listener, SendResult,
};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// Pending rows of other producers the test puts in kafka.messages.
const FOREIGN_PENDING: i64 = 200_000;
/// The producers those rows belong to.
const FOREIGN_PRODUCERS: i64 = 100;
/// Records each of the test's own transactions writes.
const OWN_RECORDS: usize = 5;
/// The most rows of kafka.messages the four transaction ends may read between them (by
/// sequential or index scans): far below one read of the foreign pending rows.
const MAX_ROWS_READ: i64 = FOREIGN_PENDING / 10;

/// Committing, aborting, fencing (InitProducerId while a transaction is open) and the
/// timeout abort each mark the producer's pending rows. Filtered on producer_id,
/// producer_epoch and txn_state = 'pending' with only the partition-keyed pending index
/// to use, every one of those statements read every pending row of every producer in
/// the database (a bitmap scan of idx_messages_txn_pending, or a sequential scan where
/// most rows are pending), on the single DB thread.
///
/// Asserts: with 200 000 pending rows of 100 other producers (and as many committed
/// rows) present, a commit, an abort, a fencing InitProducerId and a timeout abort of
/// the test's own transactions together read fewer than 20 000 rows of kafka.messages
/// (its leaves' `seq_tup_read + idx_tup_fetch`, the worker's statistics flushed by
/// ending it before and after), and each of the test's rows ends in its expected state.
pub async fn test_ending_a_transaction_reads_only_its_own_pending_rows() -> TestResult {
    into_test_result(ending_reads().await)
}

async fn ending_reads() -> SendResult<()> {
    println!("=== Test: ending a transaction reads only its own pending rows ===\n");
    let db = session().await?;
    db.batch_execute("SET statement_timeout = '120s'")
        .await
        .map_err(boxed)?;
    let foreign_topic: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 1) RETURNING id",
            &[&unique("pending-foreign")],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let result = steps(&db, foreign_topic).await;
    let cleaned = async {
        db.execute(
            "DELETE FROM kafka.messages WHERE topic_id = $1",
            &[&foreign_topic],
        )
        .await
        .map_err(boxed)?;
        db.execute("DELETE FROM kafka.topics WHERE id = $1", &[&foreign_topic])
            .await
            .map_err(boxed)?;
        SendResult::Ok(())
    }
    .await;
    result?;
    cleaned?;
    println!("✅ each transaction end read only its own pending rows");
    Ok(())
}

async fn steps(db: &Client, foreign_topic: i32) -> SendResult<()> {
    // Negative producer ids: never issued by the sequence, and no transaction owns them.
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     producer_id, producer_epoch, txn_state)
         SELECT $1, 0, g, 'v'::bytea, -1 - (g % $3::bigint), 0,
                CASE WHEN g % 2 = 0 THEN 'pending' END
         FROM generate_series(0::bigint, 2 * $2::bigint - 1) AS g",
        &[&foreign_topic, &FOREIGN_PENDING, &FOREIGN_PRODUCERS],
    )
    .await
    .map_err(boxed)?;
    db.batch_execute("ANALYZE kafka.messages")
        .await
        .map_err(boxed)?;
    println!(
        "Step 0: {FOREIGN_PENDING} pending and as many committed rows of \
         {FOREIGN_PRODUCERS} other producers"
    );

    let topic = unique("pending-own");
    let ids = [
        unique("pending-commit"),
        unique("pending-abort"),
        unique("pending-fenced"),
        unique("pending-timeout"),
    ];
    // Before any producer connects: ending the worker drops their connections.
    let before = worker_reads(db).await?;
    println!(
        "Step 1: so far {} rows of kafka.messages read and {} inserted",
        before.rows_read, before.inserted
    );
    let mut producers = Vec::new();
    for id in &ids {
        let p = create_transactional_producer(id).map_err(boxed)?;
        p.init_transactions(Duration::from_secs(10))
            .map_err(boxed)?;
        producers.push(p);
    }
    write_transaction(&producers[0], &topic).await?;
    producers[0]
        .commit_transaction(Duration::from_secs(10))
        .map_err(boxed)?;
    write_transaction(&producers[1], &topic).await?;
    producers[1]
        .abort_transaction(Duration::from_secs(10))
        .map_err(boxed)?;
    write_transaction(&producers[2], &topic).await?;
    let fencer = create_transactional_producer(&ids[2]).map_err(boxed)?;
    fencer
        .init_transactions(Duration::from_secs(10))
        .map_err(boxed)?;
    write_transaction(&producers[3], &topic).await?;
    db.execute(
        "UPDATE kafka.transactions
         SET started_at = NOW() - INTERVAL '90 seconds', timeout_ms = 1000
         WHERE transactional_id = $1",
        &[&ids[3]],
    )
    .await
    .map_err(boxed)?;
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let state: Option<String> = db
            .query_one(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                &[&ids[3]],
            )
            .await
            .map_err(boxed)?
            .get(0);
        if state.as_deref() == Some("CompleteAbort") {
            break;
        }
        if Instant::now() > deadline {
            return Err(
                format!("the timeout sweep never aborted the transaction ({state:?})").into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!("Step 2: committed, aborted, fenced and timed out a transaction of {OWN_RECORDS} records each");

    drop(fencer);
    drop(producers);
    let after = worker_reads(db).await?;
    let read = after.rows_read - before.rows_read;
    let inserted = after.inserted - before.inserted;
    println!(
        "Step 3: the four ends read {read} rows of kafka.messages (the worker inserted \
         {inserted})"
    );
    // The worker's statistics did reach the views: its inserts are counted.
    if inserted < 4 * i64::try_from(OWN_RECORDS).map_err(boxed)? {
        return Err(format!("the worker's statistics were not flushed: {inserted} inserts").into());
    }

    let states = db
        .query(
            "SELECT COALESCE(m.txn_state, 'committed'), count(*)
             FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 GROUP BY 1 ORDER BY 1",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
        .collect::<Vec<_>>();
    let own = i64::try_from(OWN_RECORDS).map_err(boxed)?;
    let expected = vec![
        ("aborted".to_string(), 3 * own),
        ("committed".to_string(), own),
    ];
    if states != expected {
        return Err(format!("the test's rows are {states:?}, expected {expected:?}").into());
    }
    if read >= MAX_ROWS_READ {
        return Err(format!(
            "ending four transactions read {read} rows of kafka.messages \
             ({FOREIGN_PENDING} foreign pending rows)"
        )
        .into());
    }
    Ok(())
}

async fn write_transaction(producer: &FutureProducer, topic: &str) -> SendResult<()> {
    producer.begin_transaction().map_err(boxed)?;
    for i in 0..OWN_RECORDS {
        let value = format!("v{i}");
        producer
            .send(
                FutureRecord::<(), _>::to(topic)
                    .payload(&value)
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| boxed(e))?;
    }
    producer.flush(Duration::from_secs(5)).map_err(boxed)?;
    Ok(())
}

/// What the worker's statistics count so far.
struct WorkerReads {
    /// Rows of kafka.messages read by sequential and index scans, over all its leaves.
    rows_read: i64,
    /// Rows inserted into kafka.messages, over all its leaves.
    inserted: i64,
}

/// The worker flushes its statistics only when it exits, so it is ended first (and
/// waited for to serve again).
async fn worker_reads(db: &Client) -> SendResult<WorkerReads> {
    let old = listener_pid(db).await?;
    if let Some(pid) = old {
        db.execute("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(boxed)?;
    }
    wait_for_serving_listener(db, old, Duration::from_secs(30)).await?;
    db.batch_execute("SELECT pg_stat_clear_snapshot()")
        .await
        .map_err(boxed)?;
    let row = db
        .query_one(
            "SELECT COALESCE(sum(COALESCE(s.seq_tup_read, 0) + COALESCE(s.idx_tup_fetch, 0)),
                             0)::bigint,
                    COALESCE(sum(s.n_tup_ins), 0)::bigint
             FROM pg_inherits i JOIN pg_stat_all_tables s ON s.relid = i.inhrelid
             WHERE i.inhparent = 'kafka.messages'::regclass",
            &[],
        )
        .await
        .map_err(boxed)?;
    Ok(WorkerReads {
        rows_read: row.get(0),
        inserted: row.get(1),
    })
}
