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
use crate::wire::records::batch;
use crate::wire::Wire;
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

/// Run an on-demand sweep on `sweeper` (autocommit) against `writer`, which holds an
/// open transaction that has run `hold`: if the sweep waits for one of the writer's
/// rows, the writer then runs `then`, a write of a row the sweep already deleted (the
/// order of two requests in one group-commit batch). Returns the sweep's and the
/// writer's outcomes (an error text, or None) and whether the sweep waited; the
/// writer's transaction ends either way.
async fn sweep_against_two_writes(
    sweeper: &Client,
    observer: &Client,
    writer: &Client,
    hold: &str,
    then: &str,
) -> SendResult<(Option<String>, Option<String>, bool)> {
    let pid: i32 = sweeper
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .map_err(boxed)?
        .get(0);
    writer
        .batch_execute(&format!("BEGIN; {hold}"))
        .await
        .map_err(boxed)?;
    let sweep = async {
        sweeper
            .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
            .await
            .err()
            .map(|e| format!("{e:?}"))
    };
    let write = async {
        let waited =
            wait_until_waiting(observer, pid, "transactionid", Duration::from_secs(3)).await?;
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
        if swept.is_some() || written.is_some() || waited || left != [hi] {
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
        if swept.is_some() || written.is_some() || waited || left != [txn_hi.clone()] {
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
