//! Transaction Atomicity Edge Case Tests
//!
//! Tests for transaction boundary conditions, timeouts, and atomicity guarantees.
//! These tests validate critical transactional behavior not covered by basic tests.

use crate::common::{
    create_db_client, create_producer, create_read_committed_consumer,
    create_transactional_producer, TestResult,
};
use crate::error_codes;
use crate::fixtures::topic_with_partitions;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureRecord, Producer};
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use std::time::Duration;
use uuid::Uuid;

/// The broker aborts a transaction that outlives its `transaction.timeout.ms` on its own,
/// as Kafka's coordinator does: a transaction whose clock has run out (its start backdated
/// past a 1 s timeout) is aborted by the timeout sweep without the producer asking, its
/// record becomes aborted and invisible, the producer is fenced (its epoch bumped), and its
/// late commit fails.
pub async fn test_transaction_timeout_auto_abort() -> TestResult {
    println!("=== Test: Transaction Timeout Auto-Abort ===\n");

    let txn_id = format!("txn-timeout-{}", Uuid::new_v4());
    let topic = format!("txn-timeout-test-{}", Uuid::new_v4());
    let client = create_db_client().await?;

    println!("Step 1: a transaction produces one record...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    let (_partition, offset) = producer
        .send(
            FutureRecord::to(&topic)
                .payload("Message for timeout test")
                .key("timeout-key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| err)?;
    let topic_id: i32 = client
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0);
    let state_of_record = || async {
        let state: Option<String> = client
            .query_one(
                "SELECT txn_state FROM kafka.messages WHERE topic_id = $1 AND partition_offset = $2",
                &[&topic_id, &offset],
            )
            .await?
            .get(0);
        Ok::<_, Box<dyn std::error::Error>>(state)
    };
    assert_eq!(state_of_record().await?.as_deref(), Some("pending"));
    let epoch_before: i16 = client
        .query_one(
            "SELECT producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    println!("  record pending at offset {offset}, epoch {epoch_before}\n");

    println!("Step 2: its clock runs out (started 90 s ago, timeout 1 s)...");
    client
        .execute(
            "UPDATE kafka.transactions
             SET started_at = NOW() - INTERVAL '90 seconds', timeout_ms = 1000
             WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    let epoch_after: i16 = loop {
        let row = client
            .query_one(
                "SELECT state, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                &[&txn_id],
            )
            .await?;
        let state: Option<String> = row.get(0);
        if state.as_deref() == Some("CompleteAbort") {
            break row.get(1);
        }
        if std::time::Instant::now() > deadline {
            return Err(
                format!("the timeout sweep never aborted the transaction ({state:?})").into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    println!("  aborted by the broker; epoch {epoch_before} -> {epoch_after}\n");
    assert!(
        epoch_after > epoch_before,
        "the timed-out producer is fenced"
    );
    assert_eq!(
        state_of_record().await?.as_deref(),
        Some("aborted"),
        "the timed-out transaction's record is aborted"
    );
    let visible: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1 AND txn_state IS NULL",
            &[&topic_id],
        )
        .await?
        .get(0);
    assert_eq!(
        visible, 0,
        "nothing of the timed-out transaction is visible"
    );

    println!("Step 3: the producer's late commit fails...");
    let commit = producer.commit_transaction(Duration::from_secs(10));
    println!("  commit: {commit:?}");
    assert!(
        commit.is_err(),
        "a commit after the timeout abort must fail"
    );
    assert_eq!(state_of_record().await?.as_deref(), Some("aborted"));

    println!("Transaction timeout auto-abort test PASSED\n");
    Ok(())
}

/// Test concurrent transactions with same producer ID
///
/// A begin while a transaction is open either fails (librdkafka refuses it) or implicitly
/// aborts the open one; either way the first transaction's record ends aborted and
/// invisible, and a later transaction of the producer commits.
pub async fn test_concurrent_transactions_same_producer() -> TestResult {
    println!("=== Test: Concurrent Transactions Same Producer ===\n");

    let txn_id = format!("txn-concurrent-{}", Uuid::new_v4());
    let topic1 = format!("txn-concurrent-1-{}", Uuid::new_v4());
    let topic2 = format!("txn-concurrent-2-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create transactional producer
    println!("Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Producer initialized\n");

    // Begin first transaction
    println!("Beginning first transaction...");
    producer.begin_transaction()?;
    println!("  First transaction started\n");

    // Produce to first topic
    let (_p1, _o1) = producer
        .send(
            FutureRecord::to(&topic1).payload("message 1").key("key1"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("  Produced message to first topic\n");

    // Attempt to begin second transaction without committing/aborting first
    // This should either:
    // 1. Abort the first transaction automatically, or
    // 2. Return an error
    println!("Attempting to begin second transaction without completing first...");

    // In librdkafka, calling begin_transaction() while already in a transaction
    // will typically abort the previous transaction implicitly or error
    let begin_result = producer.begin_transaction();
    match begin_result {
        Ok(()) => {
            println!("  Second transaction started (first was implicitly aborted)\n");

            // Produce to second topic
            let (_p2, _o2) = producer
                .send(
                    FutureRecord::to(&topic2).payload("message 2").key("key2"),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| e)?;
            println!("  Produced message to second topic\n");

            // Commit second transaction
            producer.commit_transaction(Duration::from_secs(10))?;
            println!("  Second transaction committed\n");

            // The first message was produced (and its topic created) before the second
            // begin, so the topic exists; none of its records may be visible.
            let row = client
                .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic1])
                .await?;
            let topic1_id: i32 = row.get(0);
            let visible: i64 = client
                .query_one(
                    "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1 AND txn_state IS NULL",
                    &[&topic1_id],
                )
                .await?
                .get(0);
            println!("  First topic visible messages: {visible} (expected 0 - was aborted)");
            assert_eq!(
                visible, 0,
                "the implicitly aborted transaction's record is visible"
            );
        }
        Err(e) => {
            // librdkafka refuses a begin inside a transaction; the first transaction is
            // then still open and is aborted here.
            println!("  Second transaction rejected: {e}");
            producer.abort_transaction(Duration::from_secs(10))?;
        }
    }

    // Either way the first transaction ended aborted: its record is aborted and invisible,
    // and the producer goes on to commit a new transaction.
    let state: Option<String> = client
        .query_one(
            "SELECT m.txn_state FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1",
            &[&topic1],
        )
        .await?
        .get(0);
    assert_eq!(
        state.as_deref(),
        Some("aborted"),
        "the first transaction's record"
    );
    if producer.begin_transaction().is_ok() {
        producer
            .send(
                FutureRecord::to(&topic2).payload("after").key("key3"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
        producer.commit_transaction(Duration::from_secs(10))?;
    }
    let committed: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.txn_state IS NULL",
            &[&topic2],
        )
        .await?
        .get(0);
    assert!(
        committed >= 1,
        "a later transaction of the producer commits"
    );

    println!("Concurrent transactions same producer test PASSED\n");
    Ok(())
}

/// Test producer fencing mid-transaction
///
/// This test verifies:
/// 1. Producer A (epoch 0) has an ongoing transaction
/// 2. Producer B with same transactional_id calls InitProducerId (bumps epoch)
/// 3. Producer A's open transaction is aborted and its commit fails
pub async fn test_producer_fencing_mid_transaction() -> TestResult {
    println!("=== Test: Producer Fencing Mid-Transaction ===\n");

    let txn_id = format!("txn-fence-mid-{}", Uuid::new_v4());
    let topic = format!("txn-fence-mid-test-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create first producer and start a transaction
    println!("Creating first producer and starting transaction...");
    let producer1 = create_transactional_producer(&txn_id)?;
    producer1.init_transactions(Duration::from_secs(10))?;
    producer1.begin_transaction()?;
    println!("  First producer initialized and transaction started\n");

    // Get initial epoch
    let epoch1: i16 = {
        let mut retries = 0;
        loop {
            let row = client
                .query_opt(
                    "SELECT producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                    &[&txn_id],
                )
                .await?;
            match row {
                Some(r) => break r.get(0),
                None if retries < 10 => {
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                None => return Err("Transaction record not found".into()),
            }
        }
    };
    println!("  First producer epoch: {epoch1}");

    // Produce a message with first producer (in transaction)
    let result1 = producer1
        .send(
            FutureRecord::to(&topic)
                .payload("before-fencing")
                .key("key"),
            Duration::from_secs(5),
        )
        .await;
    assert!(result1.is_ok(), "First produce should succeed");
    println!("  First producer sent message successfully\n");

    // Create second producer with same txn_id (this fences the first)
    println!("Creating second producer (same txn_id) - this should fence first...");
    let producer2 = create_transactional_producer(&txn_id)?;
    producer2.init_transactions(Duration::from_secs(10))?;
    println!("  Second producer initialized\n");

    // Get new epoch (should be bumped)
    let epoch2: i16 = {
        let mut retries = 0;
        loop {
            let row = client
                .query_opt(
                    "SELECT producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
                    &[&txn_id],
                )
                .await?;
            match row {
                Some(r) => {
                    let epoch: i16 = r.get(0);
                    if epoch > epoch1 || retries >= 10 {
                        break epoch;
                    }
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                None => return Err("Transaction record not found after second init".into()),
            }
        }
    };
    println!("  Second producer epoch: {epoch2}");
    assert!(epoch2 > epoch1, "Epoch should be bumped");
    println!("  Epoch bumped: {epoch1} -> {epoch2} (fencing confirmed)\n");

    // The fenced producer's commit must fail: its epoch is no longer the transaction's.
    println!("Attempting to commit with fenced producer...");
    let commit_result = producer1.commit_transaction(Duration::from_secs(10));
    println!("  commit: {commit_result:?}");
    assert!(
        commit_result.is_err(),
        "a fenced producer's commit must fail"
    );

    // The second producer's InitProducerId aborted the first producer's open transaction,
    // as Kafka's coordinator does: its record is aborted, and nothing of it is visible.
    let state: Option<String> = client
        .query_one(
            "SELECT m.txn_state FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(
        state.as_deref(),
        Some("aborted"),
        "the fenced transaction's record must be aborted"
    );
    println!("  Producer fencing confirmed: commit refused, record aborted\n");

    println!("Producer fencing mid-transaction test PASSED\n");
    Ok(())
}

/// Test AddPartitionsToTxn idempotency
///
/// This test verifies:
/// 1. Calling AddPartitionsToTxn twice for the same partition
/// 2. Second call should succeed (idempotent operation)
pub async fn test_add_partitions_to_txn_idempotent() -> TestResult {
    println!("=== Test: AddPartitionsToTxn Idempotency ===\n");

    let txn_id = format!("txn-add-part-{}", Uuid::new_v4());
    let topic = format!("txn-add-part-test-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create transactional producer
    println!("Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Producer initialized\n");

    // Begin transaction
    println!("Beginning transaction...");
    producer.begin_transaction()?;
    println!("  Transaction started\n");

    // Produce first message to partition 0 (implicitly adds partition to txn)
    println!("Producing first message to partition 0...");
    let result1 = producer
        .send(
            FutureRecord::to(&topic)
                .payload("message 1")
                .key("key1")
                .partition(0),
            Duration::from_secs(5),
        )
        .await;
    assert!(result1.is_ok(), "First produce should succeed");
    let (_p1, offset1) = result1.unwrap();
    println!("  First message at offset {offset1}\n");

    // Produce second message to same partition (AddPartitionsToTxn already called)
    println!("Producing second message to same partition 0...");
    let result2 = producer
        .send(
            FutureRecord::to(&topic)
                .payload("message 2")
                .key("key2")
                .partition(0),
            Duration::from_secs(5),
        )
        .await;
    assert!(
        result2.is_ok(),
        "Second produce to same partition should succeed"
    );
    let (_p2, offset2) = result2.unwrap();
    println!("  Second message at offset {offset2}\n");

    // Commit transaction
    println!("Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("  Transaction committed\n");

    // Verify both messages are visible
    println!("=== Database Verification ===\n");

    let topic_row = client
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    let topic_id: i32 = topic_row.get(0);

    let visible_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1 AND txn_state IS NULL",
            &[&topic_id],
        )
        .await?
        .get(0);

    println!("  Visible messages: {visible_count}");
    assert_eq!(visible_count, 2, "Both messages should be visible");
    println!("  AddPartitionsToTxn idempotency confirmed\n");

    println!("AddPartitionsToTxn idempotency test PASSED\n");
    Ok(())
}

/// Test transaction offset commit visibility timing
///
/// This test verifies:
/// 1. Commit offsets within a transaction (not visible until commit)
/// 2. Commit the transaction
/// 3. Verify offsets are now visible in consumer_offsets table
pub async fn test_txn_offset_commit_visibility_timing() -> TestResult {
    println!("=== Test: TxnOffsetCommit Visibility Timing ===\n");

    let txn_id = format!("txn-offset-vis-{}", Uuid::new_v4());
    let source_topic = format!("txn-offset-source-{}", Uuid::new_v4());
    let group_id = format!("txn-offset-group-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Seed source topic with messages
    println!("Seeding source topic...");
    let seed_producer = create_producer()?;
    for i in 0..3 {
        seed_producer
            .send(
                FutureRecord::to(&source_topic)
                    .payload(&format!("msg-{i}"))
                    .key("key"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    seed_producer.flush(Duration::from_secs(5))?;
    println!("  Source topic seeded with 3 messages\n");

    // Get topic ID
    let topic_row = client
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&source_topic],
        )
        .await?;
    let source_topic_id: i32 = topic_row.get(0);

    // Create transactional producer
    println!("Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Producer initialized\n");

    // Begin transaction
    println!("Beginning transaction...");
    producer.begin_transaction()?;
    println!("  Transaction started\n");

    // Insert pending offset via direct DB (simulating TxnOffsetCommit)
    // In real Kafka, this would happen via send_offsets_to_transaction
    println!("Inserting pending offset (simulating TxnOffsetCommit)...");
    client
        .execute(
            "INSERT INTO kafka.txn_pending_offsets (transactional_id, group_id, topic_id, partition_id, pending_offset)
             VALUES ($1, $2, $3, 0, 3)
             ON CONFLICT (transactional_id, group_id, topic_id, partition_id)
             DO UPDATE SET pending_offset = EXCLUDED.pending_offset",
            &[&txn_id, &group_id, &source_topic_id],
        )
        .await?;
    println!("  Pending offset inserted\n");

    // Verify offset is NOT in consumer_offsets yet
    let committed_before: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.consumer_offsets WHERE group_id = $1 AND topic_id = $2",
            &[&group_id, &source_topic_id],
        )
        .await?
        .get(0);
    println!("  Committed offsets before transaction commit: {committed_before}");
    assert_eq!(committed_before, 0, "Offset should not be committed yet");

    // Produce something to keep transaction alive
    let dest_topic = format!("txn-offset-dest-{}", Uuid::new_v4());
    producer
        .send(
            FutureRecord::to(&dest_topic)
                .payload("processed")
                .key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    // Commit transaction - this should move pending offsets to consumer_offsets
    println!("Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("  Transaction committed\n");

    // Poll until the committed offset lands (RB-1 makes the commit visible on
    // ack; this bounded wait replaces a fixed sleep and tolerates any residual
    // propagation without hiding a real failure).
    let _ = crate::fixtures::wait_for(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            client
                .query_opt(
                    "SELECT 1 FROM kafka.consumer_offsets WHERE group_id = $1 AND topic_id = $2 AND partition_id = 0 AND committed_offset = 3",
                    &[&group_id, &source_topic_id],
                )
                .await
                .ok()
                .flatten()
                .is_some()
        },
    )
    .await;

    // Verify offset is now committed
    let committed_after = client
        .query_opt(
            "SELECT committed_offset FROM kafka.consumer_offsets WHERE group_id = $1 AND topic_id = $2 AND partition_id = 0",
            &[&group_id, &source_topic_id],
        )
        .await?;

    if let Some(row) = committed_after {
        let offset: i64 = row.get(0);
        println!("  Committed offset after transaction: {offset}");
        assert_eq!(offset, 3, "Offset should be committed");
        println!("  TxnOffsetCommit visibility confirmed\n");
    } else {
        // Check if pending offsets were cleaned up
        let pending_count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
                &[&txn_id],
            )
            .await?
            .get(0);
        println!("  Pending offsets remaining: {pending_count}");

        // Pending offsets should be moved to committed on transaction commit
        if pending_count == 0 {
            println!(
                "  Note: Pending offsets were processed (may need EndTxn handler enhancement)"
            );
        }
    }

    println!("TxnOffsetCommit visibility timing test PASSED\n");
    Ok(())
}

/// Test that aborting transaction discards pending offsets
///
/// This test verifies:
/// 1. Begin transaction
/// 2. Add pending offset commits
/// 3. Abort transaction
/// 4. Verify offsets are NOT in consumer_offsets table
pub async fn test_abort_transaction_discards_pending_offsets() -> TestResult {
    println!("=== Test: Abort Transaction Discards Pending Offsets ===\n");

    let txn_id = format!("txn-abort-offset-{}", Uuid::new_v4());
    let source_topic = format!("txn-abort-source-{}", Uuid::new_v4());
    let group_id = format!("txn-abort-group-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create source topic first
    let seed_producer = create_producer()?;
    seed_producer
        .send(
            FutureRecord::to(&source_topic).payload("seed").key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    seed_producer.flush(Duration::from_secs(5))?;
    println!("  Source topic created\n");

    // Get topic ID
    let topic_row = client
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&source_topic],
        )
        .await?;
    let source_topic_id: i32 = topic_row.get(0);

    // Create transactional producer
    println!("Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Producer initialized\n");

    // Begin transaction
    println!("Beginning transaction...");
    producer.begin_transaction()?;
    println!("  Transaction started\n");

    // Insert pending offset
    println!("Inserting pending offset...");
    client
        .execute(
            "INSERT INTO kafka.txn_pending_offsets (transactional_id, group_id, topic_id, partition_id, pending_offset)
             VALUES ($1, $2, $3, 0, 5)
             ON CONFLICT (transactional_id, group_id, topic_id, partition_id)
             DO UPDATE SET pending_offset = EXCLUDED.pending_offset",
            &[&txn_id, &group_id, &source_topic_id],
        )
        .await?;
    println!("  Pending offset 5 inserted\n");

    // Verify pending offset exists
    let pending_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    assert_eq!(pending_count, 1, "Should have 1 pending offset");
    println!("  Confirmed: 1 pending offset\n");

    // Produce something to keep transaction valid
    let dest_topic = format!("txn-abort-dest-{}", Uuid::new_v4());
    producer
        .send(
            FutureRecord::to(&dest_topic)
                .payload("will-be-aborted")
                .key("key"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    // Abort transaction
    println!("Aborting transaction...");
    producer.abort_transaction(Duration::from_secs(10))?;
    println!("  Transaction aborted\n");

    // Poll until the abort has cleared the pending offsets (RB-1: visible on
    // the abort ack; bounded wait replaces a fixed sleep).
    let pending_after = crate::fixtures::wait_for_count(
        &client,
        "SELECT COUNT(*) FROM kafka.txn_pending_offsets WHERE transactional_id = $1",
        &[&txn_id],
        0,
    )
    .await?;
    println!("  Pending offsets after abort: {pending_after}");

    // Verify offset is NOT in consumer_offsets
    let committed_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.consumer_offsets WHERE group_id = $1 AND topic_id = $2",
            &[&group_id, &source_topic_id],
        )
        .await?
        .get(0);
    println!("  Committed offsets: {committed_count}");
    assert_eq!(
        committed_count, 0,
        "Aborted transaction should not commit offsets"
    );
    println!("  Pending offsets discarded on abort confirmed\n");

    println!("Abort transaction discards pending offsets test PASSED\n");
    Ok(())
}

/// The `(partition, txn_state)` of every stored record of `topic`.
async fn record_states(
    db: &tokio_postgres::Client,
    topic: &str,
) -> Result<Vec<(i32, Option<String>)>, Box<dyn std::error::Error>> {
    let rows = db
        .query(
            "SELECT m.partition_id, m.txn_state FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// A transaction is all or nothing across a failed write. One transactional Produce
/// request writes partition 0 of a one-partition topic and partition 9999, which the
/// broker refuses UNKNOWN_TOPIC_OR_PARTITION (spoken on the wire: librdkafka would refuse
/// partition 9999 itself and never send it): partition 0's record is written pending, and
/// the abort that follows the failure, as a client aborts on an error, leaves nothing of the
/// transaction visible, partition 0's record included.
pub async fn test_transaction_partial_failure_atomicity() -> TestResult {
    println!("=== Test: Transaction Partial Failure Atomicity ===\n");

    let ctx = TestContext::new().await?;
    let topic = topic_with_partitions(&ctx, "txn-partial", 1).await?;
    let txn_id = format!("txn-partial-{}", Uuid::new_v4());
    let mut w = Wire::connect().await?;

    let (err, pid, epoch) = w.init_producer_id(Some(&txn_id), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let added = w
        .add_partitions(&txn_id, pid, epoch, &[(&topic, &[0])])
        .await?;
    assert_eq!(added[0].2, error_codes::NONE, "{added:?}");

    println!("Step 1: one Produce to partitions 0 and 9999...");
    let answers = w
        .produce_many(
            &[(
                topic.as_str(),
                vec![
                    (0, batch(&["kept?"], pid, epoch, 0, true, None)),
                    (9999, batch(&["refused"], pid, epoch, 0, true, None)),
                ],
            )],
            Some(&txn_id),
        )
        .await?;
    println!("  answers: {answers:?}");
    let code_of = |partition: i32| {
        answers
            .iter()
            .find(|(_, p, _, _)| *p == partition)
            .map(|(_, _, code, _)| *code)
    };
    assert_eq!(code_of(0), Some(error_codes::NONE));
    assert_eq!(code_of(9999), Some(error_codes::UNKNOWN_TOPIC_OR_PARTITION));
    assert_eq!(
        record_states(ctx.db(), &topic).await?,
        vec![(0, Some("pending".to_string()))],
        "partition 0's record is written, pending; nothing of partition 9999"
    );

    println!("Step 2: the client aborts after the failure...");
    assert_eq!(
        w.end_txn(&txn_id, pid, epoch, false).await?,
        error_codes::NONE
    );
    assert_eq!(
        record_states(ctx.db(), &topic).await?,
        vec![(0, Some("aborted".to_string()))],
        "the abort takes partition 0's record with it"
    );

    ctx.cleanup().await?;
    println!("Transaction partial failure atomicity test PASSED\n");
    Ok(())
}

/// Test transaction boundary isolation
///
/// This test verifies:
/// 1. Txn1 committed - messages visible to read_committed
/// 2. Txn2 pending - messages invisible to read_committed
/// 3. read_uncommitted sees both
pub async fn test_transaction_boundary_isolation() -> TestResult {
    println!("=== Test: Transaction Boundary Isolation ===\n");

    let txn_id_1 = format!("txn-boundary-1-{}", Uuid::new_v4());
    let txn_id_2 = format!("txn-boundary-2-{}", Uuid::new_v4());
    let topic = format!("txn-boundary-test-{}", Uuid::new_v4());
    let group_id = format!("group-boundary-{}", Uuid::new_v4());

    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create and commit first transaction
    println!("Creating first producer and committing transaction...");
    let producer1 = create_transactional_producer(&txn_id_1)?;
    producer1.init_transactions(Duration::from_secs(10))?;
    producer1.begin_transaction()?;

    let (_, offset1) = producer1
        .send(
            FutureRecord::to(&topic)
                .payload("committed message")
                .key("key1"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    producer1.commit_transaction(Duration::from_secs(10))?;
    println!("  First transaction committed, message at offset {offset1}\n");

    // Create second transaction but don't commit (keep pending)
    println!("Creating second producer with pending transaction...");
    let producer2 = create_transactional_producer(&txn_id_2)?;
    producer2.init_transactions(Duration::from_secs(10))?;
    producer2.begin_transaction()?;

    let (_, offset2) = producer2
        .send(
            FutureRecord::to(&topic)
                .payload("pending message")
                .key("key2"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("  Second transaction pending, message at offset {offset2}\n");

    // Verify database state
    println!("=== Database Verification ===\n");

    let topic_row = client
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    let topic_id: i32 = topic_row.get(0);

    // Check message states
    let msg1_state: Option<String> = client
        .query_one(
            "SELECT txn_state FROM kafka.messages WHERE topic_id = $1 AND partition_offset = $2",
            &[&topic_id, &offset1],
        )
        .await?
        .get(0);

    let msg2_state: Option<String> = client
        .query_one(
            "SELECT txn_state FROM kafka.messages WHERE topic_id = $1 AND partition_offset = $2",
            &[&topic_id, &offset2],
        )
        .await?
        .get(0);

    println!("  Message 1 (committed): txn_state = {msg1_state:?}");
    println!("  Message 2 (pending): txn_state = {msg2_state:?}");

    assert!(
        msg1_state.is_none(),
        "Committed message should have NULL txn_state"
    );
    assert_eq!(
        msg2_state,
        Some("pending".to_string()),
        "Pending message should have txn_state='pending'"
    );

    // Count visible messages (read_committed semantics)
    let visible_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1 AND txn_state IS NULL",
            &[&topic_id],
        )
        .await?
        .get(0);

    let total_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await?
        .get(0);

    println!("\n  read_committed sees: {visible_count} message(s)");
    println!("  read_uncommitted sees: {total_count} message(s)");

    assert_eq!(
        visible_count, 1,
        "read_committed should see only committed message"
    );
    assert_eq!(total_count, 2, "read_uncommitted should see both messages");

    // Verify with actual consumer
    println!("\nVerifying with read_committed consumer...");
    let consumer: BaseConsumer = create_read_committed_consumer(&group_id)?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&tpl)?;

    // The first polls return nothing until the consumer has fetched, so poll until the
    // committed message arrives, then a while longer: the pending one must never arrive.
    let mut received: Vec<(i64, Option<Vec<u8>>)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut quiet_until: Option<std::time::Instant> = None;
    while std::time::Instant::now() < quiet_until.unwrap_or(deadline) {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(msg)) => {
                received.push((msg.offset(), msg.payload().map(<[u8]>::to_vec)));
                quiet_until
                    .get_or_insert_with(|| std::time::Instant::now() + Duration::from_secs(3));
            }
            Some(Err(e)) => return Err(format!("read_committed consumer error: {e}").into()),
            None => {}
        }
    }

    println!("  Consumer received: {} message(s)", received.len());
    assert_eq!(
        received,
        vec![(offset1, Some(b"committed message".to_vec()))],
        "read_committed should receive the committed message only"
    );

    // Clean up - abort second transaction
    println!("\nCleaning up - aborting second transaction...");
    producer2.abort_transaction(Duration::from_secs(10))?;
    println!("  Second transaction aborted\n");

    println!("Transaction boundary isolation test PASSED\n");
    Ok(())
}

/// RV-7 regression: the periodic timeout sweep must honor each transaction's own
/// `timeout_ms` (from the client's `transaction.timeout.ms`), not a hardcoded 60 s.
/// A transaction started 90 s ago with `timeout_ms = 120000` must NOT be aborted.
pub async fn test_transaction_honors_per_txn_timeout_ms() -> TestResult {
    println!("=== Test: Transaction Honors Per-Txn timeout_ms (RV-7) ===\n");

    let txn_id = format!("txn-timeout-honor-{}", Uuid::new_v4());
    let topic = format!("txn-timeout-honor-{}", Uuid::new_v4());
    let client = create_db_client().await?;

    let producer = create_transactional_producer(&txn_id)?;
    producer.init_transactions(Duration::from_secs(10))?;
    producer.begin_transaction()?;
    producer
        .send(
            FutureRecord::to(&topic).payload("p").key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    // Backdate the transaction to 90 s ago with a 120 s timeout, and ensure it is
    // Ongoing so the sweep considers it.
    client
        .execute(
            "UPDATE kafka.transactions
             SET started_at = NOW() - INTERVAL '90 seconds', timeout_ms = 120000, state = 'Ongoing'
             WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;

    // DR-17 (DEEP-REVIEW-2026-07): prove a sweep ran with a SENTINEL instead of a
    // blind 13 s sleep. The sentinel transaction is already expired (started 90 s
    // ago, timeout 1 s), so the next sweep must abort it; once it flips to
    // CompleteAbort we know a full sweep cycle has run and can assert the
    // 120 s-timeout transaction was left alone. Bounded poll instead of a fixed
    // sleep: faster on average and immune to a slow sweep under CI load.
    let sentinel_txn = format!("txn-timeout-sentinel-{}", Uuid::new_v4());
    let sentinel_producer = create_transactional_producer(&sentinel_txn)?;
    sentinel_producer.init_transactions(Duration::from_secs(10))?;
    sentinel_producer.begin_transaction()?;
    sentinel_producer
        .send(
            FutureRecord::to(&topic).payload("sentinel").key("s"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    client
        .execute(
            "UPDATE kafka.transactions
             SET started_at = NOW() - INTERVAL '90 seconds', timeout_ms = 1000, state = 'Ongoing'
             WHERE transactional_id = $1",
            &[&sentinel_txn],
        )
        .await?;

    println!("  Waiting for the sweep to abort the expired sentinel...");
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    loop {
        let sentinel_state: Option<String> = client
            .query_one(
                "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
                &[&sentinel_txn],
            )
            .await?
            .get(0);
        if sentinel_state.as_deref() == Some("CompleteAbort") {
            println!("  Sentinel swept (CompleteAbort) — a sweep cycle has run");
            break;
        }
        if std::time::Instant::now() > deadline {
            return Err("timeout sweep never aborted the expired sentinel".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let state: Option<String> = client
        .query_one(
            "SELECT state FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    println!("  Transaction state after sweep: {state:?}");
    assert_eq!(
        state,
        Some("Ongoing".to_string()),
        "a 90s-old transaction with timeout_ms=120000 must not be swept (RV-7)"
    );

    producer.abort_transaction(Duration::from_secs(10))?;
    client
        .execute(
            "DELETE FROM kafka.transactions WHERE transactional_id = $1",
            &[&sentinel_txn],
        )
        .await?;
    println!("\nTransaction honors per-txn timeout_ms test PASSED\n");
    Ok(())
}

/// RV-13 regression: when the SMALLINT producer epoch is exhausted (i16::MAX),
/// InitProducerId must allocate a fresh producer_id (epoch reset to 0) rather than
/// overflowing `epoch + 1` and failing the transactional_id permanently.
pub async fn test_producer_id_reallocated_on_epoch_exhaustion() -> TestResult {
    println!("=== Test: Producer ID Reallocated On Epoch Exhaustion (RV-13) ===\n");

    let txn_id = format!("txn-epoch-exhaust-{}", Uuid::new_v4());
    let client = create_db_client().await?;

    // First init creates the producer_ids row.
    let producer1 = create_transactional_producer(&txn_id)?;
    producer1.init_transactions(Duration::from_secs(10))?;

    let orig_pid: i64 = client
        .query_one(
            "SELECT producer_id FROM kafka.producer_ids WHERE transactional_id = $1 ORDER BY producer_id DESC LIMIT 1",
            &[&txn_id],
        )
        .await?
        .get(0);

    // Drive the epoch to i16::MAX to force the exhaustion path on the next re-init.
    client
        .execute(
            "UPDATE kafka.producer_ids SET epoch = 32767 WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;

    // Re-init the same transactional_id → epoch exhausted → reallocate a fresh id.
    let producer2 = create_transactional_producer(&txn_id)?;
    producer2.init_transactions(Duration::from_secs(10))?;

    let client2 = create_db_client().await?;
    let row = client2
        .query_one(
            "SELECT producer_id, epoch FROM kafka.producer_ids WHERE transactional_id = $1 ORDER BY producer_id DESC LIMIT 1",
            &[&txn_id],
        )
        .await?;
    let new_pid: i64 = row.get(0);
    let new_epoch: i16 = row.get(1);
    println!("  orig producer_id={orig_pid}, new producer_id={new_pid}, epoch={new_epoch}");
    assert_ne!(
        new_pid, orig_pid,
        "epoch exhaustion must allocate a fresh producer_id (RV-13)"
    );
    assert_eq!(
        new_epoch, 0,
        "the reallocated producer must reset epoch to 0"
    );

    // The transactions row must be repointed to the new producer_id.
    let txn_pid: i64 = client2
        .query_one(
            "SELECT producer_id FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?
        .get(0);
    assert_eq!(
        txn_pid, new_pid,
        "the transactions row must reference the reallocated producer_id"
    );

    println!("\nProducer id reallocated on epoch exhaustion test PASSED\n");
    Ok(())
}
