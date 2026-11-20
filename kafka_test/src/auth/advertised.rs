//! Advertised addresses: `pg_kafka.advertised_address` for the primary listener and the
//! `@host:port` suffix of a `pg_kafka.additional_listeners` entry are what Metadata and
//! FindCoordinator tell clients to connect to (Kafka's advertised.listeners), whatever
//! address the listener binds.

use super::{
    auth_env, configure_listeners, restart_listener, PASSWORD, PLAINTEXT_PORT, SASL_PLAINTEXT_PORT,
    SASL_SSL_PORT, SSL_PORT, USER,
};
use crate::common::{create_db_client, DynResult, TestResult};
use crate::wire::roundtrip;
use bytes::Bytes;
use kafka_protocol::messages::{
    FindCoordinatorRequest, MetadataRequest, SaslAuthenticateRequest, SaslHandshakeRequest,
};
use kafka_protocol::protocol::StrBytes;
use tokio::net::TcpStream;
use tokio_postgres::Client;

const PRIMARY_ADVERTISED: (&str, i32) = ("primary.pgkafka.test", 29092);
const SASL_ADVERTISED: (&str, i32) = ("sasl.pgkafka.test", 29093);

/// The listeners of `auth/mod.rs`, with an advertised address on the SASL_PLAINTEXT one.
fn listeners() -> String {
    format!(
        "SASL_PLAINTEXT://127.0.0.1:{SASL_PLAINTEXT_PORT}@{}:{},\
         SSL://127.0.0.1:{SSL_PORT},SASL_SSL://127.0.0.1:{SASL_SSL_PORT}",
        SASL_ADVERTISED.0, SASL_ADVERTISED.1
    )
}

/// `(host, port)` of the broker in Metadata and of the coordinator in FindCoordinator.
async fn advertised(stream: &mut TcpStream) -> DynResult<[(String, i32); 2]> {
    let metadata = roundtrip(stream, 1, 10, &MetadataRequest::default()).await?;
    let broker = metadata.brokers.first().ok_or("no broker in Metadata")?;
    let mut find = FindCoordinatorRequest::default();
    find.key = StrBytes::from_static_str("advertised-e2e-group");
    let coordinator = roundtrip(stream, 1, 11, &find).await?;
    assert_eq!(coordinator.error_code, 0, "FindCoordinator");
    Ok([
        (broker.host.to_string(), broker.port),
        (coordinator.host.to_string(), coordinator.port),
    ])
}

/// A connection to the SASL_PLAINTEXT listener, authenticated with SASL/PLAIN.
async fn sasl_connection() -> DynResult<TcpStream> {
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let mut hs = SaslHandshakeRequest::default();
    hs.mechanism = StrBytes::from_static_str("PLAIN");
    assert_eq!(roundtrip(&mut s, 1, 1, &hs).await?.error_code, 0);
    let mut au = SaslAuthenticateRequest::default();
    au.auth_bytes = Bytes::from(format!("\0{USER}\0{PASSWORD}").into_bytes());
    assert_eq!(roundtrip(&mut s, 2, 2, &au).await?.error_code, 0);
    Ok(s)
}

async fn check(db: &Client) -> TestResult {
    db.execute(
        &format!(
            "ALTER SYSTEM SET pg_kafka.advertised_address = '{}:{}'",
            PRIMARY_ADVERTISED.0, PRIMARY_ADVERTISED.1
        ),
        &[],
    )
    .await?;
    db.execute(
        &format!(
            "ALTER SYSTEM SET pg_kafka.additional_listeners = '{}'",
            listeners()
        ),
        &[],
    )
    .await?;
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    restart_listener(db, &[SASL_PLAINTEXT_PORT, SSL_PORT, SASL_SSL_PORT], &[]).await?;
    println!("Step 1: listener restarted with advertised addresses set");

    let mut plain = TcpStream::connect(("127.0.0.1", PLAINTEXT_PORT)).await?;
    let want = (PRIMARY_ADVERTISED.0.to_string(), PRIMARY_ADVERTISED.1);
    assert_eq!(
        advertised(&mut plain).await?,
        [want.clone(), want],
        "the primary listener advertises pg_kafka.advertised_address"
    );
    println!("✅ PLAINTEXT 127.0.0.1:{PLAINTEXT_PORT} advertises {PRIMARY_ADVERTISED:?}");

    let mut sasl = sasl_connection().await?;
    let want = (SASL_ADVERTISED.0.to_string(), SASL_ADVERTISED.1);
    assert_eq!(
        advertised(&mut sasl).await?,
        [want.clone(), want],
        "an additional listener advertises its @host:port"
    );
    println!("✅ SASL_PLAINTEXT 127.0.0.1:{SASL_PLAINTEXT_PORT} advertises {SASL_ADVERTISED:?}");
    Ok(())
}

/// Each listener advertises its configured address in Metadata and FindCoordinator.
pub async fn test_auth_listeners_advertise_configured_addresses() -> TestResult {
    println!("=== Test: per-listener advertised addresses ===\n");
    auth_env().await?;
    let db = create_db_client().await?;
    let outcome = check(&db).await.map_err(|e| e.to_string());

    // Back to the auth suite's listeners whatever happened.
    // (A server without the setting refuses the RESET; the listeners are restored anyway.)
    let _ = db
        .execute("ALTER SYSTEM RESET pg_kafka.advertised_address", &[])
        .await;
    configure_listeners(&db, true, false).await?;
    restart_listener(&db, &[SASL_PLAINTEXT_PORT, SSL_PORT, SASL_SSL_PORT], &[]).await?;
    outcome?;

    // Without the setting the bind address is advertised again.
    let mut plain = TcpStream::connect(("127.0.0.1", PLAINTEXT_PORT)).await?;
    let [broker, _] = advertised(&mut plain).await?;
    assert_ne!(broker.0, PRIMARY_ADVERTISED.0, "the reset applies");
    println!("✅ after the reset the primary listener advertises {broker:?}\n");
    Ok(())
}
