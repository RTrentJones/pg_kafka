//! Regression tests for the `storage` workstream: retention catch-up, single-statement
//! fetch bounds, the request-level Fetch `max_bytes`, batched topic metadata, and the
//! least-privilege worker role.
//!
//! The changed code lives in `storage/postgres.rs` (SPI) and the worker's role switch,
//! which unit coverage cannot see, so each test drives the broker or the SQL entry point
//! and checks the stored state.

use crate::common::{create_db_client, get_bootstrap_servers, TestResult};
use crate::error_paths::bug_hunt::{fetch_request, roundtrip};
use crate::fixtures::{generate_messages, TestMessage, TestTopicBuilder};
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::net::TcpStream;

const BROKER: &str = "localhost:9092";

/// Number of expired rows seeded by the retention test: well above the old fixed
/// 10,000-row cap per sweep pass.
const RETENTION_BACKLOG: i64 = 25_000;

/// Create a topic directly in SQL (no broker round trip) and return its id.
async fn create_topic_sql(
    ctx: &TestContext,
    name: &str,
    partitions: i32,
    retention_ms: Option<i64>,
) -> Result<i32, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "INSERT INTO kafka.topics (name, partitions, retention_ms) VALUES ($1, $2, $3) RETURNING id",
            &[&name, &partitions, &retention_ms],
        )
        .await?
        .get(0))
}

/// The retention sweep must catch up on a backlog in one pass instead of deleting a
/// single 10,000-row batch per pass (which let the table grow without bound above
/// ~167 records/s), and must still advance the partition's durable log start.
///
/// Fails before the fix: one `pg_kafka_run_retention_sweep()` call left 15,000 of the
/// 25,000 expired rows behind.
pub async fn test_retention_sweep_catches_up_on_backlog() -> TestResult {
    println!("=== Test: Retention Sweep Catches Up On A Backlog ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("storage-retention-backlog").await;
    // retention.ms = 1 hour: every seeded row (created a day ago) is expired.
    let topic_id = create_topic_sql(&ctx, &topic, 1, Some(3_600_000)).await?;

    println!("Step 1: seed {} expired rows...", RETENTION_BACKLOG);
    ctx.db()
        .execute(
            "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value, created_at)
             SELECT $1, 0, g, 'v'::bytea, NOW() - INTERVAL '1 day' + g * INTERVAL '1 millisecond'
             FROM generate_series(0::bigint, $2::bigint - 1) AS g",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;
    ctx.db()
        .execute(
            "INSERT INTO kafka.partition_offsets (topic_id, partition_id, next_offset)
             VALUES ($1, 0, $2)",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;
    // Two fresh rows that must survive the sweep.
    ctx.db()
        .execute(
            "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, value)
             VALUES ($1, 0, $2, 'fresh'::bytea), ($1, 0, $2 + 1, 'fresh'::bytea)",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;
    ctx.db()
        .execute(
            "UPDATE kafka.partition_offsets SET next_offset = $2 + 2 WHERE topic_id = $1",
            &[&topic_id, &RETENTION_BACKLOG],
        )
        .await?;

    println!("Step 2: one on-demand sweep...");
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 60)", &[])
        .await?;

    let remaining: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages WHERE topic_id = $1",
            &[&topic_id],
        )
        .await?
        .get(0);
    let log_start: i64 = ctx
        .db()
        .query_one(
            "SELECT log_start_offset FROM kafka.partition_offsets WHERE topic_id = $1 AND partition_id = 0",
            &[&topic_id],
        )
        .await?
        .get(0);
    println!(
        "   remaining rows={} log_start_offset={}",
        remaining, log_start
    );

    if remaining != 2 {
        return Err(format!(
            "expected only the 2 fresh rows to survive one sweep, found {} rows \
             (the sweep did not catch up on the backlog)",
            remaining
        )
        .into());
    }
    assert_eq!(
        log_start, RETENTION_BACKLOG,
        "log_start_offset must advance to the oldest surviving offset"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Fetch reports all offset bounds from one storage statement
/// (`PostgresStore::get_partition_bounds`). The bounds must match the single-value
/// queries they replaced: high watermark from the offset counter, last stable offset
/// at the first pending transactional row (read_committed only), log start offset
/// from the durable truncation point, and OFFSET_OUT_OF_RANGE below it.
pub async fn test_fetch_bounds_from_single_statement() -> TestResult {
    println!("=== Test: Fetch Bounds From A Single Statement ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "storage-fetch-bounds")
        .with_partitions(1)
        .build()
        .await?;
    topic.produce(&generate_messages(5, "m")).await?;
    let topic_id: i32 = ctx
        .db()
        .query_one(
            "SELECT id FROM kafka.topics WHERE name = $1",
            &[&topic.name],
        )
        .await?
        .get(0);

    println!("Step 1: truncate below offset 2 and add a pending row at offset 5...");
    ctx.db()
        .execute(
            "DELETE FROM kafka.messages WHERE topic_id = $1 AND partition_offset < 2",
            &[&topic_id],
        )
        .await?;
    ctx.db()
        .execute(
            "INSERT INTO kafka.messages
                 (topic_id, partition_id, partition_offset, value, producer_id, producer_epoch, txn_state)
             VALUES ($1, 0, 5, 'pending'::bytea, -42, 0, 'pending')",
            &[&topic_id],
        )
        .await?;
    ctx.db()
        .execute(
            "UPDATE kafka.partition_offsets SET log_start_offset = 2, next_offset = 6
             WHERE topic_id = $1 AND partition_id = 0",
            &[&topic_id],
        )
        .await?;

    let mut stream = TcpStream::connect(BROKER).await?;
    stream.set_nodelay(true)?;

    println!("Step 2: read_committed fetch from offset 2...");
    let mut req = fetch_request(&topic.name, 0, 2, 0);
    req.isolation_level = 1;
    let response = roundtrip(&mut stream, 4, 9101, &req).await?;
    let p = &response.responses[0].partitions[0];
    println!(
        "   error={} hwm={} lso={} log_start={}",
        p.error_code, p.high_watermark, p.last_stable_offset, p.log_start_offset
    );
    assert_eq!(p.error_code, 0, "fetch from the log start must succeed");
    assert_eq!(p.high_watermark, 6, "HWM comes from the offset counter");
    assert_eq!(p.last_stable_offset, 5, "LSO is the first pending offset");
    assert_eq!(p.log_start_offset, 2, "log start is the truncation point");
    assert!(
        p.records.as_ref().map(|r| !r.is_empty()).unwrap_or(false),
        "committed records 2..4 must be returned"
    );

    println!("Step 3: read_uncommitted fetch reports LSO = HWM...");
    let response = roundtrip(&mut stream, 4, 9102, &fetch_request(&topic.name, 0, 2, 0)).await?;
    let p = &response.responses[0].partitions[0];
    assert_eq!(p.error_code, 0);
    assert_eq!(p.high_watermark, 6);
    assert_eq!(p.last_stable_offset, 6);

    println!("Step 4: fetch below the log start is out of range...");
    let response = roundtrip(&mut stream, 4, 9103, &fetch_request(&topic.name, 0, 1, 0)).await?;
    let p = &response.responses[0].partitions[0];
    assert_eq!(p.error_code, 1, "expected OFFSET_OUT_OF_RANGE (1)");
    assert_eq!(p.log_start_offset, 2);

    ctx.db()
        .execute(
            "DELETE FROM kafka.messages WHERE topic_id = $1 AND txn_state = 'pending'",
            &[&topic_id],
        )
        .await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The request-level Fetch `max_bytes` (v3+) is honoured: once the first partition
/// has returned data the budget is spent, so the second partition reports its bounds
/// but no records. With a large `max_bytes` both partitions return records.
///
/// Fails before the fix: only `partition_max_bytes` was used, so both partitions
/// returned records even with `max_bytes = 1`.
pub async fn test_fetch_honours_request_max_bytes() -> TestResult {
    println!("=== Test: Fetch Honours Request-Level max_bytes ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "storage-fetch-max-bytes")
        .with_partitions(2)
        .build()
        .await?;
    let payload = "x".repeat(1000);
    let mut messages = Vec::new();
    for partition in 0..2 {
        for _ in 0..3 {
            let mut m = TestMessage::new(payload.clone());
            m.partition = Some(partition);
            messages.push(m);
        }
    }
    topic.produce(&messages).await?;

    let two_partitions = |max_bytes: i32| {
        let mut req = fetch_request(&topic.name, 0, 0, 0);
        let mut second = req.topics[0].partitions[0].clone();
        second.partition = 1;
        req.topics[0].partitions.push(second);
        req.max_bytes = max_bytes;
        req
    };

    let mut stream = TcpStream::connect(BROKER).await?;
    stream.set_nodelay(true)?;

    println!("Step 1: max_bytes = 1...");
    let response = roundtrip(&mut stream, 4, 9201, &two_partitions(1)).await?;
    let parts = &response.responses[0].partitions;
    let sizes: Vec<usize> = parts
        .iter()
        .map(|p| p.records.as_ref().map(|r| r.len()).unwrap_or(0))
        .collect();
    println!("   record bytes per partition: {:?}", sizes);
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|p| p.error_code == 0));
    assert!(
        sizes[0] > 0,
        "the first partition must still return a message"
    );
    if sizes[1] != 0 {
        return Err(format!(
            "partition 1 returned {} record bytes although max_bytes = 1 was already spent",
            sizes[1]
        )
        .into());
    }
    assert_eq!(parts[1].high_watermark, 3, "bounds are still reported");

    println!("Step 2: max_bytes = 1 MiB...");
    let response = roundtrip(&mut stream, 4, 9202, &two_partitions(1024 * 1024)).await?;
    let parts = &response.responses[0].partitions;
    assert!(
        parts
            .iter()
            .all(|p| p.records.as_ref().map(|r| !r.is_empty()).unwrap_or(false)),
        "with room in the budget both partitions return records"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The least-privilege worker role (`pgkafka_worker`) to which the background worker
/// switches via `pg_kafka.run_as_role`.
const WORKER_ROLE: &str = "pgkafka_worker";

/// Produce one record to `topic` with a short delivery timeout.
async fn produce_once(topic: &str, timeout_ms: u32) -> Result<(), String> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("message.timeout.ms", timeout_ms.to_string())
        .create()
        .map_err(|e| e.to_string())?;
    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload("probe"),
            Duration::from_millis(timeout_ms as u64),
        )
        .await
        .map(|_| ())
        .map_err(|(e, _)| e.to_string())
}

/// 0.1.0 privileges: the kafka schema is no longer readable by PUBLIC (a fresh role
/// is denied, and can read once granted `pgkafka_reader`), and the background worker
/// really runs as `pgkafka_worker`: with that role's INSERT on kafka.topics revoked,
/// auto-creating a topic fails; once re-granted, it succeeds.
///
/// Fails before the change: the schema was readable by PUBLIC, the roles did not
/// exist, and the worker ran as the bootstrap superuser (revoking a role's privilege
/// had no effect on it).
///
/// Not parallel-safe: it briefly revokes a worker privilege, which would fail any
/// concurrent test that auto-creates a topic.
pub async fn test_worker_runs_as_least_privilege_role() -> TestResult {
    println!("=== Test: Worker Runs As The Least-Privilege Role ===\n");

    let ctx = TestContext::new().await?;
    // A separate connection, so SET ROLE never leaks into the context's client.
    let db = create_db_client().await?;

    println!("Step 1: both roles exist...");
    let roles: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM pg_roles WHERE rolname IN ('pgkafka_reader', 'pgkafka_worker')",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(roles, 2, "pgkafka_reader and pgkafka_worker must exist");

    println!("Step 2: a role without grants cannot read kafka.messages...");
    let probe = format!("storage_e2e_probe_{}", uuid::Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {} NOLOGIN", probe))
        .await?;
    db.batch_execute(&format!("SET ROLE {}", probe)).await?;
    let denied = db.query("SELECT 1 FROM kafka.messages LIMIT 1", &[]).await;
    db.batch_execute("RESET ROLE").await?;
    db.batch_execute(&format!("GRANT pgkafka_reader TO {}", probe))
        .await?;
    db.batch_execute(&format!("SET ROLE {}", probe)).await?;
    let allowed = db.query("SELECT 1 FROM kafka.messages LIMIT 1", &[]).await;
    db.batch_execute("RESET ROLE").await?;
    db.batch_execute(&format!("DROP ROLE {}", probe)).await?;
    if denied.is_ok() {
        return Err(
            "a role with no grants could read kafka.messages (PUBLIC grant still present)".into(),
        );
    }
    if let Err(e) = allowed {
        return Err(format!("pgkafka_reader member could not read kafka.messages: {}", e).into());
    }

    println!("Step 3: revoke the worker role's INSERT on kafka.topics and auto-create a topic...");
    let topic = ctx.unique_topic("storage-worker-role").await;
    db.batch_execute(&format!(
        "REVOKE INSERT ON kafka.topics FROM {}",
        WORKER_ROLE
    ))
    .await?;
    let while_revoked = produce_once(&topic, 3000).await;
    // Restore before evaluating anything, so a failure cannot leave it revoked.
    db.batch_execute(&format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON kafka.topics TO {}",
        WORKER_ROLE
    ))
    .await?;
    let created_while_revoked: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.topics WHERE name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    println!(
        "   produce while revoked: {:?}, topic rows: {}",
        while_revoked.as_ref().map(|_| "ok"),
        created_while_revoked
    );
    if while_revoked.is_ok() || created_while_revoked != 0 {
        return Err(
            "the worker auto-created a topic without INSERT on kafka.topics: \
                    it is not running as pgkafka_worker"
                .into(),
        );
    }

    println!("Step 4: with the grant restored the same produce succeeds...");
    produce_once(&topic, 10_000)
        .await
        .map_err(|e| format!("produce after restoring the grant failed: {}", e))?;

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
