//! An on-demand sweep (`pg_kafka_run_retention_sweep()` in its own session) racing a
//! worker transaction on the rows it deletes or updates: the sweep must decide on the
//! row the other transaction committed, never on its own statement snapshot, and must
//! never close a lock cycle with it.
//!
//! The worker's side is played by a second session running the worker's statements,
//! so the test controls when it commits or runs its next statement: once the sweep is
//! seen waiting for one of its rows (a `transactionid` lock wait, or the sweep's poll
//! for a held offset counter, a `PgSleep` in pg_stat_activity), or once the sweep
//! returned.

use super::drop_extension::MAINTENANCE_LOCK_KEY;
use super::sweep_budget::create_topic;
use crate::common::TestResult;
use crate::wire::records::batch;
use crate::wire::Wire;
use crate::worker::{boxed, into_test_result, session, unique, SendResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// How long the writer waits for the sweep to block on (or poll for) its row.
const BLOCK_WAIT: Duration = Duration::from_secs(10);

/// Whether backend `pid` waits for a row another transaction holds: in the lock
/// manager, or in the sweep's poll for a held counter (or the producer prune's test
/// pause, `pg_kafka.test_producer_prune_pause_ms`), both a `pg_sleep`.
async fn waits_for_a_row(db: &Client, pid: i32) -> SendResult<bool> {
    Ok(db
        .query_one(
            "SELECT COALESCE((wait_event_type = 'Lock' AND wait_event = 'transactionid')
                             OR (wait_event_type = 'Timeout' AND wait_event = 'PgSleep'),
                             false)
             FROM pg_stat_activity WHERE pid = $1",
            &[&pid],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Poll until backend `pid` waits for a row ([`waits_for_a_row`]) or `done` is set,
/// at most `within`; returns whether it waited.
async fn wait_for_row_wait_or_done(
    db: &Client,
    pid: i32,
    done: &AtomicBool,
    within: Duration,
) -> SendResult<bool> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline && !done.load(Ordering::SeqCst) {
        if waits_for_a_row(db, pid).await? {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(false)
}

async fn backend_pid(db: &Client) -> SendResult<i32> {
    Ok(db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .map_err(boxed)?
        .get(0))
}

/// Run a sweep on `sweeper` while `writer` holds an open transaction, and commit the
/// writer as soon as the sweep waits for one of its rows (or once the sweep returned,
/// if it never does). Returns the sweep's count of `category` and whether the sweep
/// waited.
async fn sweep_against_writer(
    sweeper: &Client,
    observer: &Client,
    writer: &Client,
    category: &str,
) -> SendResult<(i64, bool)> {
    let pid = backend_pid(sweeper).await?;
    let done = AtomicBool::new(false);
    let sweep = async {
        let swept = sweeper
            .query_one(
                "SELECT deleted FROM pg_kafka_run_retention_sweep(0, 60)
                 WHERE category = $1",
                &[&category],
            )
            .await;
        done.store(true, Ordering::SeqCst);
        Ok::<i64, Box<dyn std::error::Error + Send + Sync>>(swept.map_err(boxed)?.get(0))
    };
    let commit = async {
        let waited = wait_for_row_wait_or_done(observer, pid, &done, BLOCK_WAIT).await?;
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
/// `reset_producer_sequences` do) by a transaction held open while an on-demand sweep
/// runs (the prune skips the row it holds, or, waiting for it, re-checks the committed
/// version), keeps its row at the new epoch and its sequence row.
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
/// a record the sweep never saw. The sweep locks the counter rows first, so it waits
/// (polls) for that Produce and the log-start update reads a snapshot that holds its
/// record: the log start never passes a record the sweep did not delete.
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

/// A Wire error (not `Send`) as the suite's error type.
fn wire_err(e: &dyn std::error::Error) -> Box<dyn std::error::Error + Send + Sync> {
    e.to_string().into()
}

/// Whether backend `pid` waits for a lock of type `event` (`advisory`, `transactionid`).
async fn waits_for(db: &Client, pid: i32, event: &str) -> SendResult<bool> {
    Ok(db
        .query_one(
            "SELECT COALESCE(wait_event_type = 'Lock' AND wait_event = $2, false)
             FROM pg_stat_activity WHERE pid = $1",
            &[&pid, &event],
        )
        .await
        .map_err(boxed)?
        .get(0))
}

/// Poll until backend `pid` waits for a lock of type `event`, at most `within`.
async fn wait_until_waiting(
    db: &Client,
    pid: i32,
    event: &str,
    within: Duration,
) -> SendResult<bool> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if waits_for(db, pid, event).await? {
            // Let the wait settle into the lock queue before anything wakes it.
            tokio::time::sleep(Duration::from_millis(200)).await;
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(false)
}

/// The listener worker's backend pid.
async fn listener(db: &Client) -> SendResult<i32> {
    crate::worker::listener_pid(db)
        .await?
        .ok_or_else(|| "no pg_kafka_listener runs".into())
}

/// A one-partition topic with infinite retention; returns its name and id.
async fn plain_topic(db: &Client, base: &str) -> SendResult<(String, i32)> {
    let name = unique(base);
    let topic: i32 = db
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 1, -1)
             RETURNING id",
            &[&name],
        )
        .await
        .map_err(boxed)?
        .get(0);
    Ok((name, topic))
}

/// A topic, an idempotent producer that appended sequence 0 ("a") to it, and then made
/// idle for 20 years (its producer row and its sequence row), so the next sweep's
/// producer prune takes it. Returns (topic name, topic id, producer id, epoch).
async fn idle_idempotent_producer(
    db: &Client,
    wire: &mut Wire,
) -> SendResult<(String, i32, i64, i16)> {
    let (name, topic) = plain_topic(db, "prune-produce").await?;
    let (err, pid, epoch) = wire
        .init_producer_id(None, 60_000)
        .await
        .map_err(|e| wire_err(&*e))?;
    if err != 0 {
        return Err(format!("InitProducerId failed with {err}").into());
    }
    let (err, offset) = wire
        .produce(&name, 0, batch(&["a"], pid, epoch, 0, false, None), None)
        .await
        .map_err(|e| wire_err(&*e))?;
    if (err, offset) != (0, 0) {
        return Err(format!("first produce: ({err}, {offset})").into());
    }
    db.execute(
        "UPDATE kafka.producer_ids SET last_active_at = now() - INTERVAL '20 years'
         WHERE producer_id = $1",
        &[&pid],
    )
    .await
    .map_err(boxed)?;
    db.execute(
        "UPDATE kafka.producer_sequences SET updated_at = now() - INTERVAL '20 years'
         WHERE producer_id = $1",
        &[&pid],
    )
    .await
    .map_err(boxed)?;
    Ok((name, topic, pid, epoch))
}

/// Retry the acked batch (sequence 1, "b") and count the topic's records: a retry Kafka deduplicates answers the original offset 1 and stores nothing.
async fn retry_is_deduplicated(
    db: &Client,
    wire: &mut Wire,
    name: &str,
    topic: i32,
    pid: i64,
    epoch: i16,
) -> SendResult<()> {
    let (err, offset) = wire
        .produce(name, 0, batch(&["b"], pid, epoch, 1, false, None), None)
        .await
        .map_err(|e| wire_err(&*e))?;
    let stored: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let known: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.producer_ids WHERE producer_id = $1",
            &[&pid],
        )
        .await
        .map_err(boxed)?
        .get(0);
    println!(
        "Retry of the acked batch: ({err}, {offset}); the topic holds {stored} record(s); \
         producer rows {known}"
    );
    if (err, offset) != (0, 1) || stored != 2 {
        return Err(format!(
            "the retry of an acked batch was appended again after the prune \
             (answer ({err}, {offset}), {stored} records stored, expected (0, 1) and 2)"
        )
        .into());
    }
    Ok(())
}

async fn drop_prune_produce_fixture(db: &Client, topic: i32, pid: i64) -> SendResult<()> {
    for sql in [
        "DELETE FROM kafka.messages WHERE topic_id = $1",
        "DELETE FROM kafka.partition_offsets WHERE topic_id = $1",
        "DELETE FROM kafka.topics WHERE id = $1",
    ] {
        db.execute(sql, &[&topic]).await.map_err(boxed)?;
    }
    for sql in [
        "DELETE FROM kafka.producer_sequences WHERE producer_id = $1",
        "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
    ] {
        db.execute(sql, &[&pid]).await.map_err(boxed)?;
    }
    Ok(())
}

/// The producer prune of an on-demand sweep against a Produce of the producer it
/// prunes that is still open: a Produce writes only the sequence and batch rows, so a
/// prune that decided on producer_ids alone deleted the producer after the Produce's
/// commit (its sequence row with it, the batch cache cascading), right after the batch
/// was acked; the producer's next batch found no state, was adopted as an unknown
/// producer at any sequence, and a retry of the acked batch was stored twice. Kafka
/// never expires state an append just refreshed.
///
/// Asserts: an idempotent producer idle past the 7-day window with sequence 0 on topic
/// A; one real Produce of sequence 1 to A and of sequence 0 to a new topic B, held by
/// the worker after it wrote A's batch (the test holds B's append lock); an on-demand
/// sweep run meanwhile (the prune skips the producer row the Produce holds); after both
/// finish, the retry of A's acked batch answers its original offset and stores nothing.
pub async fn test_sweep_prune_keeps_a_producer_whose_produce_is_open() -> TestResult {
    into_test_result(prune_during_produce().await)
}

async fn prune_during_produce() -> SendResult<()> {
    println!("=== Test: the producer prune keeps a producer whose Produce is open ===\n");
    let sweeper = sweeper_session().await?;
    let observer = session().await?;
    let blocker = session().await?;
    let mut wire = Wire::connect().await.map_err(|e| wire_err(&*e))?;
    let (name, topic, pid, epoch) = idle_idempotent_producer(&sweeper, &mut wire).await?;
    let (held_name, held_topic) = plain_topic(&sweeper, "prune-produce-held").await?;
    println!("Step 1: producer {pid} at sequence 0 on topic A, idle for 20 years; topic B new");
    let result: SendResult<()> = async {
        let worker = listener(&observer).await?;
        let sweeper_pid: i32 = sweeper
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(boxed)?
            .get(0);
        blocker
            .execute("SELECT pg_advisory_lock($1, 0)", &[&held_topic])
            .await
            .map_err(boxed)?;
        println!("Step 2: the test holds topic B's append lock");

        // Topics are written in request order: A's batch first, then B's.
        let request = [
            (
                name.as_str(),
                vec![(0, batch(&["b"], pid, epoch, 1, false, None))],
            ),
            (
                held_name.as_str(),
                vec![(0, batch(&["c"], pid, epoch, 0, false, None))],
            ),
        ];
        let produce = async {
            wire.produce_many(&request, None)
                .await
                .map_err(|e| wire_err(&*e))
        };
        let control = async {
            if !wait_until_waiting(&observer, worker, "advisory", BLOCK_WAIT).await? {
                blocker
                    .execute("SELECT pg_advisory_unlock($1, 0)", &[&held_topic])
                    .await
                    .map_err(boxed)?;
                return Err("the Produce never reached topic B's append lock".into());
            }
            let written: i64 = observer
                .query_one(
                    "SELECT count(*) FROM kafka.producer_sequences
                     WHERE producer_id = $1 AND topic_id = $2 AND xmax <> '0'",
                    &[&pid, &topic],
                )
                .await
                .map_err(boxed)?
                .get(0);
            println!(
                "Step 3: the Produce waits for topic B ({written} locked sequence row(s) of \
                 topic A)"
            );
            let sweep = async {
                Ok::<i64, Box<dyn std::error::Error + Send + Sync>>(
                    sweeper
                        .query_one(
                            "SELECT deleted FROM pg_kafka_run_retention_sweep(0, 60)
                             WHERE category = 'stale_producers'",
                            &[],
                        )
                        .await
                        .map_err(boxed)?
                        .get(0),
                )
            };
            let release = async {
                // The worker's lock waits end after worker_lock_timeout_ms (5 s).
                let waited = wait_until_waiting(
                    &observer,
                    sweeper_pid,
                    "transactionid",
                    Duration::from_secs(3),
                )
                .await?;
                blocker
                    .execute("SELECT pg_advisory_unlock($1, 0)", &[&held_topic])
                    .await
                    .map_err(boxed)?;
                Ok::<bool, Box<dyn std::error::Error + Send + Sync>>(waited)
            };
            let (pruned, waited) = tokio::join!(sweep, release);
            Ok::<(i64, bool), Box<dyn std::error::Error + Send + Sync>>((pruned?, waited?))
        };
        let (produced, control) = tokio::join!(produce, control);
        let (pruned, waited) = control?;
        let produced = produced?;
        println!(
            "Step 4: the sweep pruned {pruned} producer(s) (it waited for the Produce: \
             {waited}); the Produce answered {produced:?}"
        );
        let acked = |topic: &str| {
            produced
                .iter()
                .find(|p| p.0 == topic)
                .map(|p| (p.1, p.2, p.3))
        };
        if acked(&name) != Some((0, 0, 1)) || acked(&held_name) != Some((0, 0, 0)) {
            return Err(format!("the Produce answered {produced:?}").into());
        }
        retry_is_deduplicated(&sweeper, &mut wire, &name, topic, pid, epoch).await
    }
    .await;
    drop_prune_produce_fixture(&sweeper, topic, pid).await?;
    drop_prune_produce_fixture(&sweeper, held_topic, pid).await?;
    release(&sweeper).await?;
    result?;
    println!("✅ the producer whose Produce was open keeps its state");
    Ok(())
}

/// The other order: an on-demand sweep pruned an idle idempotent producer and has not
/// committed when a Produce of it arrives. The Produce read the producer (the prune's
/// delete not yet visible), waited for the deleted sequence row and wrote new sequence
/// and batch rows without a producer row; the producer's next batch then found no
/// state and was accepted at any sequence, so a retry of the acked batch was stored
/// twice.
///
/// Asserts: an idempotent producer idle past the 7-day window with sequence 0; a sweep
/// prunes it in an open transaction; a real Produce of sequence 1 waits for that
/// transaction, which then commits; the Produce is acked and the retry of its batch
/// answers the same offset and stores nothing.
pub async fn test_sweep_prune_then_produce_keeps_producer_state() -> TestResult {
    into_test_result(produce_after_prune().await)
}

async fn produce_after_prune() -> SendResult<()> {
    println!("=== Test: a Produce after an uncommitted prune keeps producer state ===\n");
    let sweeper = sweeper_session().await?;
    let observer = session().await?;
    let mut wire = Wire::connect().await.map_err(|e| wire_err(&*e))?;
    let (name, topic, pid, epoch) = idle_idempotent_producer(&sweeper, &mut wire).await?;
    println!("Step 1: producer {pid} at sequence 0, idle for 20 years");
    let result: SendResult<()> = async {
        let worker = listener(&observer).await?;
        sweeper.batch_execute("BEGIN").await.map_err(boxed)?;
        let pruned: i64 = sweeper
            .query_one(
                "SELECT deleted FROM pg_kafka_run_retention_sweep(0, 60)
                 WHERE category = 'stale_producers'",
                &[],
            )
            .await
            .map_err(boxed)?
            .get(0);
        println!("Step 2: a sweep pruned {pruned} producer(s) and holds its transaction open");
        if pruned < 1 {
            sweeper.batch_execute("ROLLBACK").await.map_err(boxed)?;
            return Err("the sweep did not prune the idle producer".into());
        }
        let produce = async {
            wire.produce(&name, 0, batch(&["b"], pid, epoch, 1, false, None), None)
                .await
                .map_err(|e| wire_err(&*e))
        };
        let commit = async {
            // The worker's lock waits end after worker_lock_timeout_ms (5 s).
            let waited =
                wait_until_waiting(&observer, worker, "transactionid", Duration::from_secs(3))
                    .await?;
            sweeper.batch_execute("COMMIT").await.map_err(boxed)?;
            Ok::<bool, Box<dyn std::error::Error + Send + Sync>>(waited)
        };
        let (produced, waited) = tokio::join!(produce, commit);
        let waited = waited?;
        let produced = produced?;
        println!(
            "Step 3: the Produce waited for the sweep: {waited}; after its commit it \
             answered {produced:?}"
        );
        if !waited {
            return Err("the Produce never waited for the sweep's transaction".into());
        }
        if produced != (0, 1) {
            return Err(format!("the Produce of sequence 1 answered {produced:?}").into());
        }
        retry_is_deduplicated(&sweeper, &mut wire, &name, topic, pid, epoch).await
    }
    .await;
    drop_prune_produce_fixture(&sweeper, topic, pid).await?;
    release(&sweeper).await?;
    result?;
    println!("✅ the producer keeps deduplicating after a prune it outlived");
    Ok(())
}

/// What [`sweep_against_two_writes`] saw: the sweep's rows (category, deleted) or its
/// error, the writer's error, and whether the sweep waited for (or polled for) one of
/// the writer's rows.
type TwoWrites = (Result<Vec<(String, i64)>, String>, Option<String>, bool);

/// Run an on-demand sweep on `sweeper` (autocommit) against `writer`, which holds an
/// open transaction that has run `hold`: once the sweep waits for one of the writer's
/// rows (or polls for it, or returned), the writer runs `then`, a write of a row the
/// sweep may already have deleted (the order of two requests in one group-commit
/// batch), and commits. Returns what both saw ([`TwoWrites`]); the writer's
/// transaction ends either way.
async fn sweep_against_two_writes(
    sweeper: &Client,
    observer: &Client,
    writer: &Client,
    hold: &str,
    then: &str,
) -> SendResult<TwoWrites> {
    let pid = backend_pid(sweeper).await?;
    writer
        .batch_execute(&format!("BEGIN; {hold}"))
        .await
        .map_err(boxed)?;
    let done = AtomicBool::new(false);
    let sweep = async {
        let swept = sweeper
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
            .await
            .map(|rows| {
                rows.iter()
                    .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
                    .collect::<Vec<_>>()
            })
            .map_err(|e| format!("{e:?}"));
        done.store(true, Ordering::SeqCst);
        swept
    };
    let write = async {
        let waited = wait_for_row_wait_or_done(observer, pid, &done, BLOCK_WAIT).await?;
        let failed = writer
            .batch_execute(then)
            .await
            .err()
            .map(|e| format!("{e:?}"));
        // COMMIT after a failed statement rolls back; either way the transaction ends.
        writer.batch_execute("COMMIT").await.map_err(boxed)?;
        Ok::<(Option<String>, bool), Box<dyn std::error::Error + Send + Sync>>((failed, waited))
    };
    let (swept, written) = tokio::join!(sweep, write);
    let (written, waited) = written?;
    Ok((swept, written, waited))
}

/// An on-demand sweep runs beside the worker, whose group commit holds the rows of
/// every request in a batch until one commit: a batch of two Produce (or
/// InitProducerId) requests updates two idle producers' rows (last_active_at) in
/// request order, as a batch of two InitProducerId requests updates two terminal
/// transactions' rows. The sweep's producer prune and terminal-transaction delete
/// took their rows in index order and waited for a row the batch held while holding
/// one the batch needed next: a deadlock, which aborted one side (the CI run of the
/// prune's index test against the worker's own sweep).
///
/// Asserts, for two producers idle 30 years (the lower id the older) and then for two
/// terminal transactions idle 30 years (the first-stored the older): a writer updates
/// the second one and holds its transaction; a sweep runs; the writer then updates the
/// first one and commits. Neither the sweep nor the writer fails, the sweep does not
/// wait for the writer, the first row is gone and the second (revived) one kept.
pub async fn test_sweep_never_deadlocks_with_a_batch_writing_two_rows() -> TestResult {
    into_test_result(two_row_batch().await)
}

async fn two_row_batch() -> SendResult<()> {
    println!("=== Test: the sweep never deadlocks with a batch writing two rows ===\n");
    let sweeper = sweeper_session().await?;
    let observer = session().await?;
    let writer = session().await?;
    writer
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    // Explicit negative ids, never issued by the sequence; the lower one inserted
    // first (so the lower id comes first in index and physical order).
    let base: i64 = sweeper
        .query_one(
            "SELECT -(floor(random() * 1000000)::bigint + 1) * 1000000",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let (lo, hi, owner) = (base - 3, base - 2, base - 1);
    let prefix = unique("two-row-batch");
    let (txn_lo, txn_hi) = (format!("{prefix}-lo"), format!("{prefix}-hi"));
    let result: SendResult<()> = async {
        sweeper
            .execute(
                "INSERT INTO kafka.producer_ids (producer_id, created_at, last_active_at)
                 VALUES ($1, now() - INTERVAL '31 years', now() - INTERVAL '31 years'),
                        ($2, now() - INTERVAL '30 years', now() - INTERVAL '30 years'),
                        ($3, now(), now())",
                &[&lo, &hi, &owner],
            )
            .await
            .map_err(boxed)?;
        println!("Step 1: producers {lo} and {hi} idle for 30 years");
        let (swept, written, waited) = sweep_against_two_writes(
            &sweeper,
            &observer,
            &writer,
            &format!(
                "UPDATE kafka.producer_ids SET last_active_at = now() WHERE producer_id = {hi}"
            ),
            &format!(
                "UPDATE kafka.producer_ids SET last_active_at = now() WHERE producer_id = {lo}"
            ),
        )
        .await?;
        let left: Vec<i64> = sweeper
            .query(
                "SELECT producer_id FROM kafka.producer_ids WHERE producer_id IN ($1, $2)
                 ORDER BY 1",
                &[&lo, &hi],
            )
            .await
            .map_err(boxed)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        println!(
            "Step 2: sweep error {swept:?}, writer error {written:?}, the sweep waited: \
             {waited}; producers left {left:?}"
        );
        if swept.is_err() || written.is_some() || waited || left != [hi] {
            return Err("the producer prune waited for a batch's row (deadlock)".into());
        }

        sweeper
            .execute(
                "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch,
                                                 state, last_updated_at)
                 VALUES ($1, $2, 0, 'CompleteCommit', now() - INTERVAL '31 years')",
                &[&txn_lo, &owner],
            )
            .await
            .map_err(boxed)?;
        sweeper
            .execute(
                "INSERT INTO kafka.transactions (transactional_id, producer_id, producer_epoch,
                                                 state, last_updated_at)
                 VALUES ($1, $2, 0, 'CompleteCommit', now() - INTERVAL '30 years')",
                &[&txn_hi, &owner],
            )
            .await
            .map_err(boxed)?;
        println!("Step 3: terminal transactions {txn_lo} and {txn_hi} idle for 30 years");
        let (swept, written, waited) = sweep_against_two_writes(
            &sweeper,
            &observer,
            &writer,
            &format!(
                "UPDATE kafka.transactions SET last_updated_at = now()
                 WHERE transactional_id = '{txn_hi}'"
            ),
            &format!(
                "UPDATE kafka.transactions SET last_updated_at = now()
                 WHERE transactional_id = '{txn_lo}'"
            ),
        )
        .await?;
        let left: Vec<String> = sweeper
            .query(
                "SELECT transactional_id FROM kafka.transactions
                 WHERE transactional_id IN ($1, $2) ORDER BY 1",
                &[&txn_lo, &txn_hi],
            )
            .await
            .map_err(boxed)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        println!(
            "Step 4: sweep error {swept:?}, writer error {written:?}, the sweep waited: \
             {waited}; transactions left {left:?}"
        );
        if swept.is_err() || written.is_some() || waited || left != [txn_hi.clone()] {
            return Err("the terminal-transaction delete waited for a batch's row".into());
        }
        Ok(())
    }
    .await;
    sweeper
        .execute(
            "DELETE FROM kafka.transactions WHERE transactional_id IN ($1, $2)",
            &[&txn_lo, &txn_hi],
        )
        .await
        .map_err(boxed)?;
    sweeper
        .execute(
            "DELETE FROM kafka.producer_ids WHERE producer_id IN ($1, $2, $3)",
            &[&lo, &hi, &owner],
        )
        .await
        .map_err(boxed)?;
    release(&sweeper).await?;
    result?;
    println!("✅ the sweep skipped the batch's rows and neither side failed");
    Ok(())
}

/// The count of `category` in a sweep's rows (0 when absent).
fn count_of(rows: &[(String, i64)], category: &str) -> i64 {
    rows.iter()
        .find(|(c, _)| c == category)
        .map_or(0, |(_, n)| *n)
}

/// A topic with retention.ms 1 h and offsets 0-4 dated 1990 in partition 0 (older than
/// every leaf: they are in kafka.messages_default), with its counter at 5.
async fn expired_partition(db: &Client, base: &str) -> SendResult<i32> {
    let topic = create_topic(db, &unique(base), 3_600_000).await?;
    db.execute(
        "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value,
                                     created_at)
         SELECT $1, 0, g, 'old'::bytea,
                timestamptz '1990-01-01 00:00+00' + g * INTERVAL '1 second'
         FROM generate_series(0::bigint, 4) AS g",
        &[&topic],
    )
    .await
    .map_err(boxed)?;
    db.execute(
        "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
         VALUES ($1, 0, 5)",
        &[&topic],
    )
    .await
    .map_err(boxed)?;
    Ok(topic)
}

/// Offsets of partition 0 of `topic`, and its (next offset, log start).
async fn partition_state(db: &Client, topic: i32) -> SendResult<(Vec<i64>, i64, i64)> {
    let offsets: Vec<i64> = db
        .query(
            "SELECT partition_offset FROM kafka.messages
             WHERE topic_id = $1 AND partition_id = 0 ORDER BY 1",
            &[&topic],
        )
        .await
        .map_err(boxed)?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let row = db
        .query_one(
            "SELECT next_offset, log_start_offset FROM kafka.partition_offsets
             WHERE topic_id = $1 AND partition_id = 0",
            &[&topic],
        )
        .await
        .map_err(boxed)?;
    Ok((offsets, row.get(0), row.get(1)))
}

async fn drop_topics(db: &Client, topics: &[i32]) -> SendResult<()> {
    for sql in [
        "DELETE FROM kafka.shadow_tracking WHERE topic_id = ANY ($1)",
        "DELETE FROM kafka.messages WHERE topic_id = ANY ($1)",
        "DELETE FROM kafka.partition_offsets WHERE topic_id = ANY ($1)",
        "DELETE FROM kafka.topics WHERE id = ANY ($1)",
    ] {
        db.execute(sql, &[&topics]).await.map_err(boxed)?;
    }
    Ok(())
}

/// An on-demand sweep is one transaction, holding every row it deleted until it
/// commits, beside the worker's group commit, which holds the rows of every request
/// of a batch until one commit. The sweep's log-start update locked the offset
/// counters of the partitions it expired, waiting for any a batch held, at the end of
/// the pass: after the producer prune had deleted an idle producer, and after the
/// expiry had deleted other partitions' rows. A batch holding a counter (a Produce's
/// advance_next_offset) whose next request then wrote that producer (an idempotent
/// Produce refreshing an idle producer, `read_producer_state`) or those rows (a
/// DeleteRecords) closed a cycle with it: "deadlock detected" failed the request or the
/// sweep.
///
/// Asserts, in two rounds, with a writer playing the batch: (1) an expired partition
/// and a producer idle for 30 years; the writer upserts the partition's counter and
/// holds it; a sweep runs; once it waits for (polls for) the counter, the writer
/// refreshes the producer and commits. (2) two expired partitions, A and B; the writer
/// upserts B's counter and holds it; once the sweep waits for it, the writer deletes
/// A's rows (DeleteRecords) and commits. In both, neither the sweep nor the writer
/// fails; the log start never moves for a partition whose rows stay; every row the
/// sweep did not remove is still there.
pub async fn test_sweep_never_deadlocks_with_a_batch_holding_a_counter() -> TestResult {
    into_test_result(counter_cycles().await)
}

async fn counter_cycles() -> SendResult<()> {
    println!("=== Test: the sweep never deadlocks with a batch holding an offset counter ===\n");
    let sweeper = sweeper_session().await?;
    let observer = session().await?;
    let writer = session().await?;
    writer
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let producer: i64 = sweeper
        .query_one(
            "INSERT INTO kafka.producer_ids (producer_id, created_at, last_active_at)
             VALUES (-(floor(random() * 1000000)::bigint + 1) * 1000000 - 11,
                     now() - INTERVAL '30 years', now() - INTERVAL '30 years')
             RETURNING producer_id",
            &[],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let mut topics = Vec::new();
    let result: SendResult<()> = async {
        let t = expired_partition(&sweeper, "counter-cycle").await?;
        topics.push(t);
        println!("Round 1: partition ({t}, 0) expired, producer {producer} idle for 30 years");
        let (swept, written, waited) = sweep_against_two_writes(
            &sweeper,
            &observer,
            &writer,
            &format!(
                "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
                 VALUES ({t}, 0, 6)
                 ON CONFLICT (topic_id, partition_id) DO UPDATE
                     SET next_offset = GREATEST(kafka.partition_offsets.next_offset,
                                                EXCLUDED.next_offset)"
            ),
            &format!(
                "UPDATE kafka.producer_ids SET last_active_at = NOW()
                 WHERE producer_id = {producer}"
            ),
        )
        .await?;
        let (offsets, next, log_start) = partition_state(&sweeper, t).await?;
        println!(
            "   sweep {swept:?}, writer error {written:?}, the sweep waited: {waited}; \
             offsets {offsets:?}, next {next}, log start {log_start}"
        );
        let swept = swept.map_err(|e| format!("round 1: the sweep failed: {e}"))?;
        if let Some(e) = written {
            return Err(format!("round 1: the writer failed: {e}").into());
        }
        let expired = count_of(&swept, "expired_messages");
        let rows_ok = (expired == 5 && offsets.is_empty() && log_start == next)
            || (expired == 0 && offsets.len() == 5 && log_start == 0);
        if !rows_ok {
            return Err(format!(
                "round 1: rows and log start disagree (expired {expired}, offsets \
                 {offsets:?}, log start {log_start})"
            )
            .into());
        }

        let a = expired_partition(&sweeper, "counter-cycle-a").await?;
        topics.push(a);
        let b = expired_partition(&sweeper, "counter-cycle-b").await?;
        topics.push(b);
        println!("Round 2: partitions ({a}, 0) and ({b}, 0) expired");
        let (swept, written, waited) = sweep_against_two_writes(
            &sweeper,
            &observer,
            &writer,
            &format!(
                "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
                 VALUES ({b}, 0, 6)
                 ON CONFLICT (topic_id, partition_id) DO UPDATE
                     SET next_offset = GREATEST(kafka.partition_offsets.next_offset,
                                                EXCLUDED.next_offset)"
            ),
            &format!(
                "DELETE FROM kafka.messages
                 WHERE topic_id = {a} AND partition_id = 0 AND partition_offset < 2"
            ),
        )
        .await?;
        let (a_offsets, a_next, a_start) = partition_state(&sweeper, a).await?;
        let (b_offsets, b_next, b_start) = partition_state(&sweeper, b).await?;
        println!(
            "   sweep {swept:?}, writer error {written:?}, the sweep waited: {waited}; \
             A offsets {a_offsets:?} log start {a_start} (next {a_next}), \
             B offsets {b_offsets:?} log start {b_start} (next {b_next})"
        );
        swept.map_err(|e| format!("round 2: the sweep failed: {e}"))?;
        if let Some(e) = written {
            return Err(format!("round 2: the writer failed: {e}").into());
        }
        for (name, offsets, next, start) in [
            ("A", &a_offsets, a_next, a_start),
            ("B", &b_offsets, b_next, b_start),
        ] {
            let lowest = offsets.first().copied().unwrap_or(next);
            if start > lowest {
                return Err(format!(
                    "round 2: partition {name}'s log start {start} passed its row {lowest}"
                )
                .into());
            }
        }
        if !waited {
            return Err("round 2: the sweep never reached the held counter".into());
        }
        Ok(())
    }
    .await;
    drop_topics(&sweeper, &topics).await?;
    sweeper
        .execute(
            "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
            &[&producer],
        )
        .await
        .map_err(boxed)?;
    release(&sweeper).await?;
    result?;
    println!("✅ neither the sweep nor the batch failed, and the log starts hold");
    Ok(())
}

/// The sweep's orphaned-outbox reaper deleted the rows below their partition's log
/// start in one statement, waiting for any a transaction held, in the sweep's one
/// transaction: the forwarder's ack batch (the worker's outbox tick, `apply_delivered`)
/// updates its rows in arrival order, so an ack batch holding one partition's row whose
/// next ack was a row the reaper already deleted closed a cycle with it: "deadlock
/// detected" failed the tick or the sweep.
///
/// Asserts: a two-partition topic with its log starts at 10 and an outbox row below
/// each, (p0, 5) stored first; a writer updates (p1, 7) as an ack would and holds it; a
/// sweep runs; once it waits for the row, or returned, the writer updates (p0, 5) and
/// commits. Neither fails, the sweep does not wait for the writer, it reaped (p0, 5)
/// and left (p1, 7), and the next sweep reaps (p1, 7).
pub async fn test_sweep_reaper_never_deadlocks_with_an_ack_batch() -> TestResult {
    into_test_result(reaper_cycle().await)
}

async fn reaper_cycle() -> SendResult<()> {
    println!("=== Test: the orphaned-outbox reaper never deadlocks with an ack batch ===\n");
    let sweeper = sweeper_session().await?;
    let observer = session().await?;
    let writer = session().await?;
    writer
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let name = unique("reaper-cycle");
    let topic: i32 = sweeper
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, 2, -1)
             RETURNING id",
            &[&name],
        )
        .await
        .map_err(boxed)?
        .get(0);
    let result: SendResult<()> = async {
        sweeper
            .execute(
                "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset,
                                                      log_start_offset)
                 VALUES ($1, 0, 10, 10), ($1, 1, 10, 10)",
                &[&topic],
            )
            .await
            .map_err(boxed)?;
        for (partition, offset) in [(0, 5i64), (1, 7i64)] {
            sweeper
                .execute(
                    "INSERT INTO kafka.shadow_tracking (topic_id, partition_id, local_offset,
                                                        external_offset, forwarded_at)
                     VALUES ($1, $2, $3, $3::bigint * 10, now())",
                    &[&topic, &partition, &offset],
                )
                .await
                .map_err(boxed)?;
        }
        println!("Step 1: outbox rows ({topic}, 0, 5) and ({topic}, 1, 7) below log start 10");
        let ack = |partition: i32, offset: i64| {
            format!(
                "UPDATE kafka.shadow_tracking SET forwarded_at = NOW(), attempt_open = false
                 WHERE topic_id = {topic} AND partition_id = {partition}
                   AND local_offset = {offset}"
            )
        };
        let (swept, written, waited) =
            sweep_against_two_writes(&sweeper, &observer, &writer, &ack(1, 7), &ack(0, 5)).await?;
        let left: Vec<i64> = sweeper
            .query(
                "SELECT local_offset FROM kafka.shadow_tracking WHERE topic_id = $1
                 ORDER BY 1",
                &[&topic],
            )
            .await
            .map_err(boxed)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        println!(
            "Step 2: sweep {swept:?}, writer error {written:?}, the sweep waited: {waited}; \
             rows left {left:?}"
        );
        let swept = swept.map_err(|e| format!("the sweep failed: {e}"))?;
        if let Some(e) = written {
            return Err(format!("the ack batch failed: {e}").into());
        }
        if waited || left != [7] || count_of(&swept, "shadow_orphaned_rows") < 1 {
            return Err("the reaper waited for the ack batch's row".into());
        }
        let rows = sweeper
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
            .await
            .map_err(boxed)?;
        let left: i64 = sweeper
            .query_one(
                "SELECT count(*) FROM kafka.shadow_tracking WHERE topic_id = $1",
                &[&topic],
            )
            .await
            .map_err(boxed)?
            .get(0);
        let reaped = rows
            .iter()
            .find(|r| r.get::<_, String>(0) == "shadow_orphaned_rows")
            .map_or(0, |r| r.get::<_, i64>(1));
        println!("Step 3: the next sweep reaped {reaped} row(s); {left} left");
        if left != 0 {
            return Err("the next sweep left the skipped row".into());
        }
        Ok(())
    }
    .await;
    drop_topics(&sweeper, &[topic]).await?;
    release(&sweeper).await?;
    result?;
    println!("✅ the reaper skipped the held row and took it at the next pass");
    Ok(())
}
