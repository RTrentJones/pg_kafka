//! Before SASL authentication, ApiVersions is answered by the network thread: an
//! anonymous peer cannot put work on the single DB thread. A credential lookup the DB
//! thread cannot answer closes the connection instead of failing the authentication.

use super::{auth_env, PASSWORD, PLAINTEXT_PORT, SASL_PLAINTEXT_PORT, USER};
use crate::common::{create_db_client, TestResult};
use crate::wire::{recv, roundtrip, send};
use bytes::Bytes;
use kafka_protocol::messages::{ApiVersionsRequest, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;
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

fn plain_handshake() -> SaslHandshakeRequest {
    let mut r = SaslHandshakeRequest::default();
    r.mechanism = StrBytes::from_static_str("PLAIN");
    r
}

fn plain_authenticate() -> SaslAuthenticateRequest {
    let mut r = SaslAuthenticateRequest::default();
    r.auth_bytes = Bytes::from(format!("\0{USER}\0{PASSWORD}").into_bytes());
    r
}

/// A credential lookup the DB thread does not answer (here it waits on the schema lock
/// until `pg_kafka.worker_lock_timeout_ms`; the same refusal answers every lookup while
/// the schema is missing or older than the library) says nothing about the
/// credentials. The connection is closed without a SaslAuthenticate response, which
/// every client retries, not answered SASL_AUTHENTICATION_FAILED, which the Java client
/// treats as fatal. Once the DB thread serves again the same login succeeds.
pub async fn test_auth_unanswered_lookup_closes_without_failing_authentication() -> TestResult {
    println!("=== Test: an unanswered credential lookup closes, it does not fail auth ===\n");
    auth_env().await?;
    let db = create_db_client().await?;

    db.execute("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
        .await?;
    let outcome = async {
        let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
        let hs = roundtrip(&mut s, 1, 1, &plain_handshake()).await?;
        assert_eq!(hs.error_code, 0, "the handshake needs no lookup");
        send(&mut s, 2, 2, &plain_authenticate()).await?;
        let started = Instant::now();
        let answer = tokio::time::timeout(
            Duration::from_secs(9),
            recv::<SaslAuthenticateRequest>(&mut s, 2, 2),
        )
        .await;
        let elapsed = started.elapsed();
        Ok::<_, Box<dyn std::error::Error>>((answer.map(|r| r.map_err(|e| e.to_string())), elapsed))
    }
    .await
    .map_err(|e| e.to_string());
    db.execute("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY])
        .await?;

    let (answer, elapsed) = outcome?;
    match answer {
        Err(_) => {
            return Err(format!(
                "SaslAuthenticate neither answered nor closed within 9 s ({elapsed:?})"
            )
            .into())
        }
        Ok(Ok(response)) => {
            return Err(format!(
                "the unanswered lookup was answered with error {} after {elapsed:?}; \
                 expected the connection to close without an answer",
                response.error_code
            )
            .into())
        }
        Ok(Err(e)) => println!("Step 1: closed without an answer after {elapsed:?} ({e})"),
    }

    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let hs = roundtrip(&mut s, 1, 1, &plain_handshake()).await?;
    assert_eq!(hs.error_code, 0);
    let au = roundtrip(&mut s, 2, 2, &plain_authenticate()).await?;
    assert_eq!(au.error_code, 0, "the retried login succeeds");
    println!("✅ the retried login succeeds once the DB thread serves\n");
    Ok(())
}
