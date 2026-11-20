//! Before SASL authentication, ApiVersions is answered by the network thread: an
//! anonymous peer cannot put work on the single DB thread.

use super::{auth_env, PLAINTEXT_PORT, SASL_PLAINTEXT_PORT};
use crate::common::{create_db_client, TestResult};
use crate::wire::{recv, roundtrip, send};
use kafka_protocol::messages::ApiVersionsRequest;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// The schema advisory lock every worker transaction takes in SHARE mode
/// (`SCHEMA_LOCK_KEY` in `src/worker/txn.rs`). Holding it exclusively stalls the DB thread
/// for up to `pg_kafka.worker_lock_timeout_ms` (5 s by default) per request batch.
const SCHEMA_LOCK_KEY: i64 = 8_099_547_369_649_367_906;

/// With the DB thread stalled in a lock wait, a pre-auth ApiVersions on the SASL listener
/// is still answered at once, while one on the PLAINTEXT listener (served by the DB
/// thread) waits.
pub async fn test_auth_pre_auth_api_versions_skips_db_thread() -> TestResult {
    println!("=== Test: pre-auth ApiVersions answered without the DB thread ===\n");
    auth_env().await?;
    let db = create_db_client().await?;

    db.execute("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
        .await?;
    let outcome = async {
        // Put the DB thread into the lock wait.
        let mut plain = TcpStream::connect(("127.0.0.1", PLAINTEXT_PORT)).await?;
        send(&mut plain, 3, 1, &ApiVersionsRequest::default()).await?;
        let stalled = tokio::time::timeout(
            Duration::from_millis(800),
            recv::<ApiVersionsRequest>(&mut plain, 3, 1),
        )
        .await
        .is_err();
        println!("Step 1: PLAINTEXT ApiVersions stalled behind the lock: {stalled}");

        let mut sasl = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
        let started = Instant::now();
        let answer = tokio::time::timeout(
            Duration::from_secs(2),
            roundtrip(&mut sasl, 3, 2, &ApiVersionsRequest::default()),
        )
        .await;
        let elapsed = started.elapsed();
        println!("Step 2: pre-auth ApiVersions on SASL_PLAINTEXT answered after {elapsed:?}");
        let answer = answer.map(|r| r.map_err(|e| e.to_string()));
        Ok::<_, Box<dyn std::error::Error>>((stalled, answer, elapsed))
    }
    .await
    .map_err(|e| e.to_string());
    db.execute("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY])
        .await?;

    let (stalled, answer, elapsed) = outcome?;
    assert!(
        stalled,
        "the DB thread must be stalled for this test to mean anything"
    );
    let response = answer
        .map_err(|_| format!("pre-auth ApiVersions not answered within 2 s ({elapsed:?})"))??;
    assert_eq!(response.error_code, 0);
    assert!(
        response.api_keys.iter().any(|k| k.api_key == 36),
        "SaslAuthenticate is advertised"
    );
    println!("✅ pre-auth ApiVersions is answered on the network thread\n");
    Ok(())
}
