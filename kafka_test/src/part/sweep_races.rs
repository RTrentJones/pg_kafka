//! An on-demand sweep (`pg_kafka_run_retention_sweep()` in its own session) racing a
//! worker transaction on the rows it deletes or updates: the sweep must decide on the
//! row the other transaction committed, never on its own statement snapshot.
//!
//! The worker's side is played by a second session running the worker's statements,
//! so the test controls when it commits: after the sweep is seen waiting for its row
//! lock (a `transactionid` lock wait in pg_stat_activity).

use super::drop_extension::MAINTENANCE_LOCK_KEY;
use super::sweep_budget::create_topic;
use crate::common::TestResult;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// How long the writer waits for the sweep to block on its row lock.
const BLOCK_WAIT: Duration = Duration::from_secs(10);

/// Whether backend `pid` is waiting for another transaction (a row lock).
async fn waits_for_a_row(db: &Client, pid: i32) -> SendResult<bool> {
    Ok(db
        .query_one(
            "SELECT COALESCE(wait_event_type = 'Lock' AND wait_event = 'transactionid', false)
             FROM pg_stat_activity WHERE pid = $1",
            &[&pid],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Run `sweep` on `sweeper` while `writer` holds an open transaction: once the sweep
/// waits for one of the writer's rows, commit the writer. Returns the sweep's count of
/// `category` and whether the sweep waited (the writer is committed either way).
async fn sweep_against_writer(
    sweeper: &Client,
    observer: &Client,
    writer: &Client,
    category: &str,
) -> SendResult<(i64, bool)> {
    let pid: i32 = sweeper
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .map_err(boxed)?
        .get(0);
    let sweep = async {
        Ok::<i64, Box<dyn std::error::Error + Send + Sync>>(
            sweeper
                .query_one(
                    "SELECT deleted FROM pg_kafka_run_retention_sweep(0, 60)
                     WHERE category = $1",
                    &[&category],
                )
                .await
                .map_err(boxed)?
                .get(0),
        )
    };
    let commit = async {
        let deadline = Instant::now() + BLOCK_WAIT;
        let mut waited = false;
        while Instant::now() < deadline {
            if waits_for_a_row(observer, pid).await? {
                waited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if waited {
            // Let the wait settle into the lock queue before the commit wakes it.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        writer.batch_execute("COMMIT").await.map_err(boxed)?;
        Ok::<bool, Box<dyn std::error::Error + Send + Sync>>(waited)
    };
    let (swept, waited) = tokio::join!(sweep, commit);
    Ok((swept?, waited?))
}

/// The worker's sweep skips while this session holds the maintenance lock; the
/// session's own sweeps re-enter it.
async fn sweeper_session() -> SendResult<Client> {
    let sweeper = session().await?;
    sweeper
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    sweeper
        .execute("SELECT pg_advisory_lock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed)?;
    Ok(sweeper)
}

async fn release(sweeper: &Client) -> SendResult<()> {
    sweeper
        .execute("SELECT pg_advisory_unlock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await
        .map_err(boxed)?;
    Ok(())
}

/// The stale-producer prune picks its candidates from the statement's snapshot, and
/// a DELETE that waited for a row re-checks only its own WHERE on the committed
/// version: a producer idle past the 7-day window that InitProducerId (or a KIP-360
/// epoch bump) revived while the prune waited for it was deleted after the revival
/// committed, with its sequences, and the client held an id and epoch the broker no
/// longer knew.
///
/// Asserts: a producer idle for 20 years with a sequence row, revived (epoch bump,
/// last_active_at = NOW(), sequences reset, as `bump_producer_epoch` and
/// `reset_producer_sequences` do) by a transaction that commits while an on-demand
/// sweep waits for it, keeps its row at the new epoch and its sequence row.
pub async fn test_sweep_prune_keeps_a_producer_revived_during_its_wait() -> TestResult {
    into_test_result(revived_producer().await)
}

async fn revived_producer() -> SendResult<()> {
    println!("=== Test: the producer prune keeps a producer revived while it waits ===\n");
    let sweeper = sweeper_session().await?;
    let topic = create_topic(&sweeper, &unique("prune-race"), -1).await?;
    // An explicit negative id, never issued by the sequence.
    let producer_id: i64 = sweeper
        .query_one(
            "INSERT INTO kafka.producer_ids (producer_id, epoch, created_at, last_active_at)
             VALUES (-(floor(random() * 1000000)::bigint + 1) * 1000000 - 7, 3,
                     now() - INTERVAL '20 years', now() - INTERVAL '20 years')
             RETURNING producer_id",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let result: SendResult<()> = async {
        sweeper
            .execute(
                "INSERT INTO kafka.producer_sequences (producer_id, topic_id, partition_id,
                                                       last_sequence, updated_at)
                 VALUES ($1, $2, 0, 41, now() - INTERVAL '20 years')",
                &[&producer_id, &topic],
            )
            .await
            .map_err(boxed)?;
        println!("Step 1: producer {producer_id} idle for 20 years, with one sequence row");

        let writer = session().await?;
        let observer = session().await?;
        writer
            .batch_execute(&format!(
                "BEGIN;
                 UPDATE kafka.producer_ids
                 SET epoch = epoch + 1, last_epoch = NULL, last_active_at = NOW()
                 WHERE producer_id = {producer_id};
                 UPDATE kafka.producer_sequences SET last_sequence = -1, updated_at = NOW()
                 WHERE producer_id = {producer_id};"
            ))
            .await
            .map_err(boxed)?;
        println!("Step 2: a writer revives it (epoch 4) and holds its transaction open");

        let (pruned, waited) =
            sweep_against_writer(&sweeper, &observer, &writer, "stale_producers").await?;
        println!(
            "Step 3: the sweep pruned {pruned} producer(s); it waited for the writer: {waited}"
        );
        if !waited {
            return Err("the sweep never waited for the reviving transaction".into());
        }
        let epoch: Option<i16> = sweeper
            .query_opt(
                "SELECT epoch FROM kafka.producer_ids WHERE producer_id = $1",
                &[&producer_id],
            )
            .await
            .map_err(boxed)?
            .map(|r| r.get(0));
        let sequences: i64 = sweeper
            .query_one(
                "SELECT count(*) FROM kafka.producer_sequences WHERE producer_id = $1",
                &[&producer_id],
            )
            .await
            .map_err(boxed)?
            .get(0);
        println!("Step 4: producer epoch {epoch:?}, {sequences} sequence row(s)");
        if epoch != Some(4) || sequences != 1 {
            return Err(format!(
                "the prune deleted a producer revived during its wait \
                 (epoch {epoch:?}, sequence rows {sequences})"
            )
            .into());
        }
        Ok(())
    }
    .await;
    sweeper
        .execute(
            "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
            &[&producer_id],
        )
        .await
        .map_err(boxed)?;
    sweeper
        .execute(
            "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
            &[&producer_id],
        )
        .await
        .map_err(boxed)?;
    sweeper
        .execute("DELETE FROM kafka.topics WHERE id = $1", &[&topic])
        .await
        .map_err(boxed)?;
    release(&sweeper).await?;
    result?;
    println!("✅ the revived producer and its sequences survive the prune");
    Ok(())
}

/// The log start a sweep sets after removing every visible row of a partition is the
/// partition's next offset, which a Produce committing during the sweep advances past
/// a record the sweep never saw. The sweep's counter upsert locks the counter rows
/// first, so it waits for that Produce and the log-start update reads a snapshot that
/// holds its record: the log start never passes a record the sweep did not delete.
///
/// Asserts: a partition with offsets 0-4 expired (rows in the DEFAULT leaf) and a
/// writer holding a Produce of offset 5 (the worker's statements: append lock, insert,
/// counter upsert) open; an on-demand sweep that removes 0-4 waits for the writer, and
/// afterwards the log start is 5 with the record at 5 present.
pub async fn test_sweep_log_start_never_passes_a_concurrent_produce() -> TestResult {
    into_test_result(log_start_race().await)
}

async fn log_start_race() -> SendResult<()> {
    println!("=== Test: the sweep's log start never passes a record produced during it ===\n");
    let sweeper = sweeper_session().await?;
    let topic = create_topic(&sweeper, &unique("logstart-race"), 3_600_000).await?;
    let result: SendResult<()> = async {
        // 1990: older than every leaf, so the rows are in kafka.messages_default.
        sweeper
            .execute(
                "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                             created_at)
                 SELECT $1, 0, g, 'old'::bytea,
                        timestamptz '1990-01-01 00:00+00' + g * INTERVAL '1 second'
                 FROM generate_series(0::bigint, 4) AS g",
                &[&topic],
            )
            .await
            .map_err(boxed)?;
        sweeper
            .execute(
                "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
                 VALUES ($1, 0, 5)",
                &[&topic],
            )
            .await
            .map_err(boxed)?;
        println!("Step 1: offsets 0-4 expired (1990, retention.ms 1 h); next offset 5");

        let writer = session().await?;
        let observer = session().await?;
        writer
            .batch_execute(&format!(
                "BEGIN;
                 SELECT pg_advisory_xact_lock({topic}, 0);
                 INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
                 VALUES ({topic}, 0, 5, 'new'::bytea);
                 INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
                 VALUES ({topic}, 0, 6)
                 ON CONFLICT (topic_id, partition_id) DO UPDATE
                     SET next_offset = GREATEST(kafka.partition_offsets.next_offset,
                                                EXCLUDED.next_offset);"
            ))
            .await
            .map_err(boxed)?;
        println!("Step 2: a writer holds a Produce of offset 5 open (counter at 6)");

        let (expired, waited) =
            sweep_against_writer(&sweeper, &observer, &writer, "expired_messages").await?;
        println!("Step 3: the sweep expired {expired} row(s); it waited for the writer: {waited}");
        let offsets: Vec<i64> = sweeper
            .query(
                "SELECT partition_offset FROM kafka.messages WHERE topic_id = $1
                 ORDER BY partition_offset",
                &[&topic],
            )
            .await
            .map_err(boxed)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let row = sweeper
            .query_one(
                "SELECT next_offset, log_start_offset FROM kafka.partition_offsets
                 WHERE topic_id = $1 AND partition_id = 0",
                &[&topic],
            )
            .await
            .map_err(boxed)?;
        let (next, log_start): (i64, i64) = (row.get(0), row.get(1));
        println!("Step 4: offsets {offsets:?}, next offset {next}, log start {log_start}");
        if expired != 5 || !waited {
            return Err(format!(
                "the sweep did not remove offsets 0-4 while waiting for the writer \
                 (expired {expired}, waited {waited})"
            )
            .into());
        }
        if offsets != [5] || next != 6 || log_start != 5 {
            return Err(format!(
                "the log start passed a record the sweep did not delete \
                 (offsets {offsets:?}, next {next}, log start {log_start})"
            )
            .into());
        }
        Ok(())
    }
    .await;
    sweeper
        .execute("DELETE FROM kafka.messages WHERE topic_id = $1", &[&topic])
        .await
        .map_err(boxed)?;
    sweeper
        .execute("DELETE FROM kafka.topics WHERE id = $1", &[&topic])
        .await
        .map_err(boxed)?;
    release(&sweeper).await?;
    result?;
    println!("✅ the log start stops at the record produced during the sweep");
    Ok(())
}
