//! A schema at the library's own version that lacks an object the library uses.

use super::{boxed, into_test_result, session, wait_for_serving_listener, SendResult, ServerLog};
use crate::common::TestResult;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

/// A 0.1.0 schema created by an earlier 0.1.0 build lacks objects the current library
/// uses (here `kafka.messages.record_bytes`). The worker does not serve it, and its
/// WARNING told the operator to run `ALTER EXTENSION pg_kafka UPDATE`, which does nothing
/// at the same version: the worker then waited for an update that never came.
///
/// Asserts: with `kafka.messages.record_bytes` dropped from a current schema and the
/// configuration reloaded, the worker logs one WARNING that names the schema as the
/// library's own version, says ALTER EXTENSION UPDATE changes nothing at that version,
/// names re-creating the extension and the missing column, and does not tell the operator
/// to wait for ALTER EXTENSION pg_kafka UPDATE; after the extension is re-created the worker
/// serves again. Skipped (passes) when the server log is not a readable file.
pub async fn test_same_version_schema_warning_names_recreation() -> TestResult {
    into_test_result(same_version().await)
}

async fn same_version() -> SendResult<()> {
    println!("=== Test: the WARNING for a same-version schema that lacks an object ===\n");
    let db = session().await?;
    let Some(log) = ServerLog::open(&db).await? else {
        println!("  (server log not readable; skipped)");
        return Ok(());
    };
    let from = log.end();
    db.batch_execute(
        "ALTER TABLE kafka.messages DROP COLUMN record_bytes; SELECT pg_reload_conf()",
    )
    .await
    .map_err(boxed)?;
    println!("Step 1: dropped kafka.messages.record_bytes and reloaded the configuration");
    let result = warning(&log, from).await;
    let restored = recreate(&db).await;
    let text = result?;
    restored?;
    println!("Step 2: the WARNING: {text}");
    for want in [
        "the library's own",
        "ALTER EXTENSION pg_kafka UPDATE changes nothing at the same version",
        "re-create the extension",
        "(missing: kafka.messages.record_bytes)",
    ] {
        if !text.contains(want) {
            return Err(format!("the WARNING does not say '{want}': {text}").into());
        }
    }
    if text.contains("until ALTER EXTENSION pg_kafka UPDATE") {
        return Err(format!("the WARNING asks for a no-op ALTER EXTENSION UPDATE: {text}").into());
    }
    println!("✅ the WARNING names re-creating the extension; the worker serves again");
    Ok(())
}

/// The worker's WARNING about the unserved schema, logged since `from` (within 20 s).
async fn warning(log: &ServerLog, from: u64) -> SendResult<String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(line) = log.since(from).lines().find(|l| {
            l.contains("WARNING") && l.contains("answering Kafka requests with KAFKA_STORAGE_ERROR")
        }) {
            return Ok(line.to_string());
        }
        if Instant::now() > deadline {
            return Err("the worker logged no WARNING about the schema".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Re-create the extension and wait for the worker to serve it.
async fn recreate(db: &Client) -> SendResult<()> {
    db.batch_execute("DROP EXTENSION pg_kafka CASCADE; CREATE EXTENSION pg_kafka")
        .await
        .map_err(boxed)?;
    wait_for_serving_listener(db, None, Duration::from_secs(30)).await?;
    Ok(())
}
