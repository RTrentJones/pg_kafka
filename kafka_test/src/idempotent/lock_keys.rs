//! An idempotent producer's id is the client's choice (a producer the broker holds no
//! state for is adopted), so nothing the broker does for a producer may depend on that
//! id colliding, or not, with a lock key the extension uses for something else.

use crate::common::{create_db_client, TestResult};
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use std::time::{Duration, Instant};

/// The partition-maintenance advisory lock (`retention::MAINTENANCE_LOCK_KEY`).
const MAINTENANCE_LOCK_KEY: i64 = 8_099_547_369_649_367_905;

/// A producer whose id, mixed with its topic and partition the way a per-partition
/// sequence lock key would be (`id ^ topic << 20 ^ partition << 40`), equals the
/// maintenance lock key produces while another session holds that lock (as a
/// retention sweep run from SQL does). Its Produce must not wait for the lock: the
/// worker would block its single database thread until `pg_kafka.worker_lock_timeout_ms`
/// and then fail the batch.
pub async fn test_producer_id_colliding_with_a_lock_key_is_not_blocked() -> TestResult {
    println!(
        "=== Test: a producer id colliding with the maintenance lock key is not blocked ===\n"
    );
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "idem-lock-key", 1).await?;
    let topic_id: i32 = ctx
        .db()
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&t])
        .await?
        .get(0);
    let pid = MAINTENANCE_LOCK_KEY ^ (i64::from(topic_id) << 20);
    let mut w = Wire::connect().await?;

    println!("Step 1: producer {pid} writes its first batch (adopted as a new producer)...");
    let (err, _) = w
        .produce(&t, 0, batch(&["a"], pid, 0, 0, false, None), None)
        .await?;
    assert_eq!(err, error_codes::NONE);

    println!("Step 2: another session takes the maintenance lock; the producer's next batch...");
    let holder = create_db_client().await?;
    holder
        .execute("SELECT pg_advisory_lock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await?;
    let started = Instant::now();
    let produced = w
        .produce(&t, 0, batch(&["b"], pid, 0, 1, false, None), None)
        .await
        .map_err(|e| e.to_string());
    let took = started.elapsed();
    holder
        .execute("SELECT pg_advisory_unlock($1)", &[&MAINTENANCE_LOCK_KEY])
        .await?;
    ctx.db()
        .execute(
            "DELETE FROM kafka.producer_ids WHERE producer_id = $1",
            &[&pid],
        )
        .await?;
    let (err, offset) = produced?;
    println!("  answered {err} at offset {offset} in {took:?}");
    assert_eq!(err, error_codes::NONE, "the batch is written");
    assert_eq!(offset, 1);
    assert!(
        took < Duration::from_secs(2),
        "the Produce waited {took:?} for a lock it has nothing to do with"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
