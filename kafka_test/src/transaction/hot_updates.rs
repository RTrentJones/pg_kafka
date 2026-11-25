//! What continuing an Ongoing transaction writes to kafka.transactions.

use crate::common::{hot_blocking_columns, DynResult, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::Wire;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_postgres::Client;
use uuid::Uuid;

/// The transaction row of `id`, column by column, as text.
async fn txn_row(db: &Client, id: &str) -> DynResult<BTreeMap<String, Option<String>>> {
    Ok(db
        .query(
            "SELECT c.key, c.value FROM kafka.transactions t, jsonb_each_text(to_jsonb(t)) c
             WHERE t.transactional_id = $1",
            &[&id],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}

/// Every AddPartitionsToTxn and AddOffsetsToTxn of an Ongoing transaction updates its
/// kafka.transactions row (the compare-and-swap that continues it). Such an update is
/// HOT only if it changes no column an index keys or filters on; when it set
/// `last_updated_at`, a key of idx_transactions_terminal, every one of them wrote a new
/// entry in all four indexes of the table.
///
/// Asserts: after the AddPartitionsToTxn that starts a transaction, a later
/// AddPartitionsToTxn of another partition and an AddOffsetsToTxn (both answered NONE)
/// change no column of the row that an index of kafka.transactions keys, names in its
/// predicate or uses in an expression; the transaction then commits.
pub async fn test_continuing_a_transaction_changes_no_indexed_column() -> TestResult {
    println!("=== Test: continuing a transaction changes no indexed column ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();
    let t = topic(&ctx, "txn-hot", 2).await?;
    let group = ctx.unique_group("txn-hot").await;
    let id = format!("txn-hot-{}", Uuid::new_v4());
    let mut w = Wire::connect().await?;
    let (err, pid, epoch) = w.init_producer_id(Some(&id), 60_000).await?;
    assert_eq!(err, error_codes::NONE);

    let codes = w.add_partitions(&id, pid, epoch, &[(&t, &[0])]).await?;
    assert!(codes.iter().all(|c| c.2 == error_codes::NONE), "{codes:?}");
    let started = txn_row(db, &id).await?;
    println!("Step 1: started: {started:?}");
    // A later NOW(), so a column set to it would differ.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let codes = w.add_partitions(&id, pid, epoch, &[(&t, &[1])]).await?;
    assert!(codes.iter().all(|c| c.2 == error_codes::NONE), "{codes:?}");
    assert_eq!(
        w.add_offsets(&id, pid, epoch, &group).await?,
        error_codes::NONE
    );
    let continued = txn_row(db, &id).await?;
    let changed: Vec<&String> = continued
        .iter()
        .filter(|(col, v)| started.get(*col) != Some(*v))
        .map(|(col, _)| col)
        .collect();
    let indexed = hot_blocking_columns(db, "kafka.transactions").await?;
    println!("Step 2: changed by continuing: {changed:?}; indexed: {indexed:?}");
    let blocking: Vec<&&String> = changed.iter().filter(|c| indexed.contains(c)).collect();
    assert!(
        blocking.is_empty(),
        "continuing the transaction changed indexed columns {blocking:?} (never HOT)"
    );
    assert_eq!(continued.get("state"), Some(&Some("Ongoing".to_string())));

    assert_eq!(w.end_txn(&id, pid, epoch, true).await?, error_codes::NONE);
    ctx.cleanup().await?;
    println!("✅ the updates that continue a transaction can stay HOT");
    Ok(())
}
