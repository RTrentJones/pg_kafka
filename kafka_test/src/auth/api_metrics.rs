//! Stream `api`: SaslHandshake / SaslAuthenticate are answered on the network thread, and
//! still counted in `kafka.api_metrics` (requests and errors) like worker-served APIs.

use super::{auth_env, DynResult, PASSWORD, SASL_PLAINTEXT_PORT, USER};
use crate::common::{create_db_client, TestResult};
use crate::error_paths::bug_hunt::roundtrip;
use bytes::Bytes;
use kafka_protocol::messages::{SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_postgres::Client;

/// `(requests, errors)` published for `api_name` (0, 0 before the first flush row).
async fn counts(db: &Client, api_name: &str) -> DynResult<(i64, i64)> {
    let row = db
        .query_opt(
            "SELECT requests, errors FROM kafka.api_metrics WHERE api_name = $1",
            &[&api_name],
        )
        .await?;
    Ok(row.map(|r| (r.get(0), r.get(1))).unwrap_or((0, 0)))
}

/// Poll until `api_name` reaches at least `requests`/`errors` (the worker flushes at most
/// once a second).
async fn wait_for(
    db: &Client,
    api_name: &str,
    requests: i64,
    errors: i64,
) -> DynResult<(i64, i64)> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let c = counts(db, api_name).await?;
        if (c.0 >= requests && c.1 >= errors) || Instant::now() > deadline {
            return Ok(c);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn sasl_plain(user: &str, password: &str) -> DynResult<i16> {
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let mut hs = SaslHandshakeRequest::default();
    hs.mechanism = StrBytes::from_static_str("PLAIN");
    let hs = roundtrip(&mut s, 1, 1, &hs).await?;
    assert_eq!(hs.error_code, 0, "handshake");
    let mut au = SaslAuthenticateRequest::default();
    au.auth_bytes = Bytes::from(format!("\0{}\0{}", user, password).into_bytes());
    Ok(roundtrip(&mut s, 2, 2, &au).await?.error_code)
}

/// One successful and one failed SASL/PLAIN exchange show up in kafka.api_metrics.
pub async fn test_auth_sasl_requests_counted_in_api_metrics() -> TestResult {
    println!("=== Test: SASL requests counted in kafka.api_metrics ===\n");
    auth_env().await?;
    let db = create_db_client().await?;
    let (hs0, _) = counts(&db, "SaslHandshake").await?;
    let (au0, au_err0) = counts(&db, "SaslAuthenticate").await?;

    assert_eq!(
        sasl_plain(USER, PASSWORD).await?,
        0,
        "good password accepted"
    );
    assert_ne!(sasl_plain(USER, "wrong").await?, 0, "bad password rejected");

    let (hs, hs_err) = wait_for(&db, "SaslHandshake", hs0 + 2, 0).await?;
    let (au, au_err) = wait_for(&db, "SaslAuthenticate", au0 + 2, au_err0 + 1).await?;
    println!("SaslHandshake requests={} errors={}", hs, hs_err);
    println!("SaslAuthenticate requests={} errors={}", au, au_err);
    assert!(hs >= hs0 + 2, "both handshakes are counted");
    assert!(au >= au0 + 2, "both authentications are counted");
    assert!(au_err > au_err0, "the failed authentication is an error");
    println!("✅ Test PASSED\n");
    Ok(())
}
