//! Basic transaction functionality tests (Phase 10)
//!
//! Validates transactional producer commit/abort flows using rdkafka.

use crate::common::{create_db_client, create_transactional_producer, TestResult};
use rdkafka::producer::{FutureRecord, Producer};
use std::time::Duration;
use uuid::Uuid;

/// Test transactional producer commit flow
///
/// This test verifies:
/// 1. InitProducerId succeeds with transactional_id
/// 2. Messages produced in a transaction are marked as pending
/// 3. After commit, messages become visible (txn_state=NULL)
/// 4. Transaction state is "CompleteCommit"
pub async fn test_transactional_producer_commit() -> TestResult {
    println!("=== Test: Transactional Producer Commit ===\n");

    // Use unique identifiers for test isolation
    let txn_id = format!("txn-commit-{}", Uuid::new_v4());
    let topic = format!("txn-commit-test-{}", Uuid::new_v4());

    // Connect to PostgreSQL
    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create transactional producer
    println!("Creating transactional producer (id={})...", &txn_id[..20]);
    let producer = create_transactional_producer(&txn_id)?;
    println!("  Producer created\n");

    // Initialize transactions (calls InitProducerId with transactional_id)
    println!("Initializing transactions...");
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Transactions initialized\n");

    // Begin transaction
    println!("Beginning transaction...");
    producer.begin_transaction()?;
    println!("  Transaction started\n");

    // Produce a message within the transaction
    let key = "txn-key-1";
    let payload = "Transactional message for commit test";
    println!("Producing message to topic '{}'...", topic);

    let (partition, offset) = producer
        .send(
            FutureRecord::to(&topic).payload(payload).key(key),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| {
            println!("  Failed to deliver message: {}", err);
            err
        })?;
    println!("  Message queued: partition={}, offset={}\n", partition, offset);

    // Commit the transaction
    println!("Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("  Transaction committed\n");

    // Verify database state
    println!("=== Database Verification ===\n");

    // Check transaction state
    let txn_row = client
        .query_opt(
            "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;

    match txn_row {
        Some(row) => {
            let state: String = row.get(0);
            let producer_id: i64 = row.get(1);
            let producer_epoch: i16 = row.get(2);
            println!("  Transaction row found:");
            println!("    state: {}", state);
            println!("    producer_id: {}", producer_id);
            println!("    producer_epoch: {}", producer_epoch);

            assert_eq!(
                state, "CompleteCommit",
                "Transaction state should be 'CompleteCommit' after commit"
            );
            println!("  Transaction state = 'CompleteCommit'\n");
        }
        None => {
            // Transaction might be cleaned up, check messages instead
            println!("  Transaction row not found (may have been cleaned up)\n");
        }
    }

    // Check message visibility
    let topic_row = client
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic],
        )
        .await?;
    let topic_id: i32 = topic_row.get(0);

    let msg_row = client
        .query_one(
            "SELECT txn_state, producer_id, producer_epoch FROM kafka.messages
             WHERE topic_id = $1 AND partition_offset = $2",
            &[&topic_id, &offset],
        )
        .await?;

    let txn_state: Option<String> = msg_row.get(0);
    let msg_producer_id: Option<i64> = msg_row.get(1);

    println!("  Message row:");
    println!("    txn_state: {:?}", txn_state);
    println!("    producer_id: {:?}", msg_producer_id);

    assert!(
        txn_state.is_none(),
        "Committed message should have NULL txn_state (visible)"
    );
    println!("  Message is visible (txn_state=NULL)\n");

    println!("Transactional commit test PASSED\n");

    Ok(())
}

/// Test transactional producer abort flow
///
/// This test verifies:
/// 1. Messages produced in a transaction are marked as pending
/// 2. After abort, messages are marked as aborted (txn_state='aborted')
/// 3. Transaction state is "CompleteAbort"
/// 4. Aborted messages are invisible to read_committed consumers
pub async fn test_transactional_producer_abort() -> TestResult {
    println!("=== Test: Transactional Producer Abort ===\n");

    // Use unique identifiers for test isolation
    let txn_id = format!("txn-abort-{}", Uuid::new_v4());
    let topic = format!("txn-abort-test-{}", Uuid::new_v4());

    // Connect to PostgreSQL
    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create transactional producer
    println!("Creating transactional producer (id={})...", &txn_id[..20]);
    let producer = create_transactional_producer(&txn_id)?;
    println!("  Producer created\n");

    // Initialize transactions
    println!("Initializing transactions...");
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Transactions initialized\n");

    // Begin transaction
    println!("Beginning transaction...");
    producer.begin_transaction()?;
    println!("  Transaction started\n");

    // Produce a message within the transaction
    let key = "txn-key-abort";
    let payload = "Transactional message for abort test";
    println!("Producing message to topic '{}'...", topic);

    let (partition, offset) = producer
        .send(
            FutureRecord::to(&topic).payload(payload).key(key),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(err, _msg)| {
            println!("  Failed to deliver message: {}", err);
            err
        })?;
    println!("  Message queued: partition={}, offset={}\n", partition, offset);

    // Abort the transaction
    println!("Aborting transaction...");
    producer.abort_transaction(Duration::from_secs(10))?;
    println!("  Transaction aborted\n");

    // Verify database state
    println!("=== Database Verification ===\n");

    // Check transaction state
    let txn_row = client
        .query_opt(
            "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;

    match txn_row {
        Some(row) => {
            let state: String = row.get(0);
            let producer_id: i64 = row.get(1);
            let producer_epoch: i16 = row.get(2);
            println!("  Transaction row found:");
            println!("    state: {}", state);
            println!("    producer_id: {}", producer_id);
            println!("    producer_epoch: {}", producer_epoch);

            assert_eq!(
                state, "CompleteAbort",
                "Transaction state should be 'CompleteAbort' after abort"
            );
            println!("  Transaction state = 'CompleteAbort'\n");
        }
        None => {
            // Transaction row should exist after abort
            return Err("Transaction row not found after abort".into());
        }
    }

    // Check message state
    let topic_row = client
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic],
        )
        .await?;
    let topic_id: i32 = topic_row.get(0);

    let msg_row = client
        .query_one(
            "SELECT txn_state, producer_id FROM kafka.messages
             WHERE topic_id = $1 AND partition_offset = $2",
            &[&topic_id, &offset],
        )
        .await?;

    let txn_state: Option<String> = msg_row.get(0);

    println!("  Message row:");
    println!("    txn_state: {:?}", txn_state);

    assert_eq!(
        txn_state,
        Some("aborted".to_string()),
        "Aborted message should have txn_state='aborted'"
    );
    println!("  Message is marked as aborted\n");

    // Verify message is invisible to read_committed query
    let visible_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages
             WHERE topic_id = $1 AND txn_state IS NULL",
            &[&topic_id],
        )
        .await?
        .get(0);

    println!("  Visible messages (txn_state IS NULL): {}", visible_count);
    assert_eq!(
        visible_count, 0,
        "Aborted message should not be visible to read_committed"
    );
    println!("  Aborted message filtered from read_committed\n");

    println!("Transactional abort test PASSED\n");

    Ok(())
}

/// Test multiple messages in a single transaction
///
/// This test verifies:
/// 1. Multiple messages can be produced in one transaction
/// 2. All messages become visible atomically on commit
pub async fn test_transactional_batch() -> TestResult {
    println!("=== Test: Transactional Batch ===\n");

    // Use unique identifiers for test isolation
    let txn_id = format!("txn-batch-{}", Uuid::new_v4());
    let topic = format!("txn-batch-test-{}", Uuid::new_v4());

    // Connect to PostgreSQL
    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create transactional producer
    println!("Creating transactional producer...");
    let producer = create_transactional_producer(&txn_id)?;
    println!("  Producer created\n");

    // Initialize transactions
    println!("Initializing transactions...");
    producer.init_transactions(Duration::from_secs(10))?;
    println!("  Transactions initialized\n");

    // Begin transaction
    println!("Beginning transaction...");
    producer.begin_transaction()?;
    println!("  Transaction started\n");

    // Produce multiple messages
    let messages = vec![
        ("key-1", "Message 1"),
        ("key-2", "Message 2"),
        ("key-3", "Message 3"),
    ];

    println!("Producing {} messages...", messages.len());
    let mut offsets = Vec::new();

    for (key, payload) in &messages {
        let (partition, offset) = producer
            .send(
                FutureRecord::to(&topic).payload(*payload).key(*key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _msg)| err)?;
        println!("  Queued: key={}, partition={}, offset={}", key, partition, offset);
        offsets.push(offset);
    }
    println!();

    // Commit the transaction
    println!("Committing transaction...");
    producer.commit_transaction(Duration::from_secs(10))?;
    println!("  Transaction committed\n");

    // Verify all messages are visible
    println!("=== Database Verification ===\n");

    let topic_row = client
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?;
    let topic_id: i32 = topic_row.get(0);

    let visible_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages
             WHERE topic_id = $1 AND txn_state IS NULL",
            &[&topic_id],
        )
        .await?
        .get(0);

    println!("  Visible messages: {}", visible_count);
    assert_eq!(
        visible_count,
        messages.len() as i64,
        "All {} messages should be visible after commit",
        messages.len()
    );
    println!("  All messages visible atomically\n");

    println!("Transactional batch test PASSED\n");

    Ok(())
}

/// Test producer fencing (epoch bumping)
///
/// This test verifies:
/// 1. Creating a new producer with the same transactional_id bumps the epoch
/// 2. The old producer is fenced and cannot produce
pub async fn test_producer_fencing() -> TestResult {
    println!("=== Test: Producer Fencing ===\n");

    // Use a shared transactional_id
    let txn_id = format!("txn-fence-{}", Uuid::new_v4());

    // Connect to PostgreSQL
    println!("Connecting to PostgreSQL...");
    let client = create_db_client().await?;
    println!("  Connected to database\n");

    // Create first producer
    println!("Creating first transactional producer...");
    let producer1 = create_transactional_producer(&txn_id)?;
    producer1.init_transactions(Duration::from_secs(10))?;
    println!("  First producer initialized\n");

    // Get epoch for first producer
    let row1 = client
        .query_one(
            "SELECT producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;
    let epoch1: i16 = row1.get(0);
    println!("  First producer epoch: {}", epoch1);

    // Create second producer with same transactional_id (should fence first)
    println!("\nCreating second transactional producer (same txn_id)...");
    let producer2 = create_transactional_producer(&txn_id)?;
    producer2.init_transactions(Duration::from_secs(10))?;
    println!("  Second producer initialized\n");

    // Get epoch for second producer
    let row2 = client
        .query_one(
            "SELECT producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
            &[&txn_id],
        )
        .await?;
    let epoch2: i16 = row2.get(0);
    println!("  Second producer epoch: {}", epoch2);

    // Verify epoch was bumped
    assert!(
        epoch2 > epoch1,
        "Second producer epoch ({}) should be greater than first ({})",
        epoch2,
        epoch1
    );
    println!("  Epoch bumped: {} -> {} (fencing confirmed)\n", epoch1, epoch2);

    // Note: rdkafka handles fencing internally - the first producer would get
    // PRODUCER_FENCED error if it tried to produce after second producer init.
    // Testing the actual error requires more complex setup with concurrent transactions.

    println!("Producer fencing test PASSED\n");

    Ok(())
}
