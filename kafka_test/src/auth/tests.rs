//! The auth E2E tests (see `auth/mod.rs` for the listener setup).

use super::*;
use crate::setup::TestContext;
use bytes::Bytes;
use kafka_protocol::messages::{MetadataRequest, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ERROR_ILLEGAL_SASL_STATE: i16 = 34;
const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;
const ERROR_UNSUPPORTED_VERSION: i16 = 35;

/// Client config for one of the listeners.
fn client_config(port: u16, protocol: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", format!("127.0.0.1:{}", port))
        .set("broker.address.family", "v4")
        .set("security.protocol", protocol)
        .set("message.timeout.ms", "10000")
        .set("socket.timeout.ms", "10000");
    if protocol.contains("ssl") {
        cfg.set("ssl.ca.location", cert_path("ca.pem"));
    }
    cfg
}

fn sasl_config(
    port: u16,
    protocol: &str,
    mechanism: &str,
    user: &str,
    password: &str,
) -> ClientConfig {
    let mut cfg = client_config(port, protocol);
    cfg.set("sasl.mechanism", mechanism)
        .set("sasl.username", user)
        .set("sasl.password", password);
    cfg
}

/// Produce 5 records through `cfg` and read them back through `cfg`; also check the
/// listener advertises itself in Metadata and the rows are in the database.
async fn round_trip(cfg: ClientConfig, port: u16, label: &str) -> TestResult {
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic(label).await;
    let group = ctx.unique_group(label).await;

    let producer: FutureProducer = cfg.create()?;
    for i in 0..5 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .key(&format!("k{}", i))
                    .payload(&format!("{}-{}", label, i))
                    .partition(0),
                Duration::from_secs(15),
            )
            .await
            .map_err(|(e, _)| format!("produce on port {} failed: {}", port, e))?;
    }
    let md = producer
        .client()
        .fetch_metadata(Some(&topic), Duration::from_secs(10))?;
    assert_eq!(md.brokers().len(), 1);
    assert_eq!(
        md.brokers()[0].port(),
        port as i32,
        "Metadata must advertise the listener the client is on"
    );
    println!("✅ produced 5 records on port {}", port);

    let mut ccfg = cfg.clone();
    ccfg.set("group.id", &group)
        .set("enable.auto.commit", "false");
    let topic_c = topic.clone();
    let label_c = label.to_string();
    let received = tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
        let consumer: BaseConsumer = ccfg.create().map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_c, 0, Offset::Beginning)
            .map_err(|e| e.to_string())?;
        consumer.assign(&tpl).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while out.len() < 5 && Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(m)) => {
                    out.push(String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned())
                }
                Some(Err(e)) => return Err(format!("{} consume error: {}", label_c, e)),
                None => {}
            }
        }
        Ok(out)
    })
    .await??;
    let expected: Vec<String> = (0..5).map(|i| format!("{}-{}", label, i)).collect();
    assert_eq!(received, expected, "consumed records on port {}", port);
    println!("✅ consumed the 5 records on port {}", port);

    let count: i64 = ctx
        .db()
        .query_one(
            "SELECT count(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id \
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(count, 5);
    ctx.cleanup().await?;
    Ok(())
}

/// Whether a client with this config can fetch metadata (i.e. connect and, where
/// required, authenticate).
async fn can_connect(cfg: ClientConfig) -> bool {
    tokio::task::spawn_blocking(move || {
        let producer: FutureProducer = match cfg.create() {
            Ok(p) => p,
            Err(_) => return false,
        };
        producer
            .client()
            .fetch_metadata(None, Duration::from_secs(5))
            .is_ok()
    })
    .await
    .unwrap_or(false)
}

/// Whether the server has closed `stream` (EOF or reset within 3 s).
async fn closed_by_server(stream: &mut TcpStream) -> bool {
    let mut buf = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

fn handshake(mechanism: &'static str) -> SaslHandshakeRequest {
    let mut r = SaslHandshakeRequest::default();
    r.mechanism = StrBytes::from_static_str(mechanism);
    r
}

fn authenticate(bytes: Vec<u8>) -> SaslAuthenticateRequest {
    let mut r = SaslAuthenticateRequest::default();
    r.auth_bytes = Bytes::from(bytes);
    r
}

fn plain_token(user: &str, password: &str) -> Vec<u8> {
    format!("\0{}\0{}", user, password).into_bytes()
}

/// SASL_PLAINTEXT + PLAIN: produce/consume round trip; Metadata advertises 19093.
pub async fn test_auth_sasl_plain_roundtrip() -> TestResult {
    println!("=== Test: SASL_PLAINTEXT PLAIN round trip ===\n");
    auth_env().await?;
    round_trip(
        sasl_config(
            SASL_PLAINTEXT_PORT,
            "sasl_plaintext",
            "PLAIN",
            USER,
            PASSWORD,
        ),
        SASL_PLAINTEXT_PORT,
        "auth-plain",
    )
    .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// SASL_PLAINTEXT + SCRAM-SHA-256: round trip.
pub async fn test_auth_sasl_scram_sha256_roundtrip() -> TestResult {
    println!("=== Test: SASL_PLAINTEXT SCRAM-SHA-256 round trip ===\n");
    auth_env().await?;
    round_trip(
        sasl_config(
            SASL_PLAINTEXT_PORT,
            "sasl_plaintext",
            "SCRAM-SHA-256",
            USER,
            PASSWORD,
        ),
        SASL_PLAINTEXT_PORT,
        "auth-scram256",
    )
    .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// SASL_PLAINTEXT + SCRAM-SHA-512: round trip.
pub async fn test_auth_sasl_scram_sha512_roundtrip() -> TestResult {
    println!("=== Test: SASL_PLAINTEXT SCRAM-SHA-512 round trip ===\n");
    auth_env().await?;
    round_trip(
        sasl_config(
            SASL_PLAINTEXT_PORT,
            "sasl_plaintext",
            "SCRAM-SHA-512",
            USER,
            PASSWORD,
        ),
        SASL_PLAINTEXT_PORT,
        "auth-scram512",
    )
    .await?;
    // After authentication the pre-auth frame limit (512 KiB) no longer applies.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("auth-scram512-big").await;
    let producer: FutureProducer = sasl_config(
        SASL_PLAINTEXT_PORT,
        "sasl_plaintext",
        "SCRAM-SHA-512",
        USER,
        PASSWORD,
    )
    .create()?;
    let big = vec![b'x'; 700 * 1024];
    producer
        .send(
            FutureRecord::<(), _>::to(&topic).payload(&big).partition(0),
            Duration::from_secs(15),
        )
        .await
        .map_err(|(e, _)| format!("700 KiB produce after authentication failed: {}", e))?;
    println!("✅ 700 KiB record accepted after authentication");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Wrong password, unknown user and a dropped user are rejected for every mechanism
/// (rdkafka cannot fetch metadata); on the wire the failure is
/// SASL_AUTHENTICATION_FAILED followed by the server closing the connection.
pub async fn test_auth_wrong_password_rejected() -> TestResult {
    println!("=== Test: SASL wrong password / unknown user rejected ===\n");
    auth_env().await?;
    for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        for (user, password) in [(USER, "wrong-password"), ("no-such-user", PASSWORD)] {
            let cfg = sasl_config(
                SASL_PLAINTEXT_PORT,
                "sasl_plaintext",
                mechanism,
                user,
                password,
            );
            assert!(
                !can_connect(cfg).await,
                "{} must reject user {:?} with a bad password",
                mechanism,
                user
            );
        }
        println!(
            "✅ {} rejects a wrong password and an unknown user",
            mechanism
        );
    }

    // Raw wire: PLAIN with a wrong password.
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let hs = roundtrip(&mut s, 1, 1, &handshake("PLAIN")).await?;
    assert_eq!(hs.error_code, 0);
    assert!(hs.mechanisms.iter().any(|m| m.as_str() == "SCRAM-SHA-512"));
    let au = roundtrip(&mut s, 2, 2, &authenticate(plain_token(USER, "nope"))).await?;
    assert_eq!(au.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
    let msg = au.error_message.as_deref().unwrap_or("").to_string();
    assert!(
        !msg.contains("nope"),
        "the error must not echo the password"
    );
    assert!(
        closed_by_server(&mut s).await,
        "connection closed after a failure"
    );
    println!("✅ wire: SASL_AUTHENTICATION_FAILED then close");

    // A dropped user can no longer authenticate.
    let db = create_db_client().await?;
    let temp_user = "e2e-auth-dropped";
    db.execute("SELECT pg_kafka_drop_user($1)", &[&temp_user])
        .await?;
    db.execute(
        "SELECT pg_kafka_create_user($1, 'pw-dropped')",
        &[&temp_user],
    )
    .await?;
    let cfg = || {
        sasl_config(
            SASL_PLAINTEXT_PORT,
            "sasl_plaintext",
            "SCRAM-SHA-256",
            temp_user,
            "pw-dropped",
        )
    };
    assert!(can_connect(cfg()).await, "the new user authenticates");
    let dropped: bool = db
        .query_one("SELECT pg_kafka_drop_user($1)", &[&temp_user])
        .await?
        .get(0);
    assert!(dropped);
    assert!(!can_connect(cfg()).await, "a dropped user is rejected");
    println!("✅ dropped user rejected");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// On a SASL listener nothing but ApiVersions / SaslHandshake / SaslAuthenticate is
/// served before authentication: Metadata closes the connection; SaslAuthenticate
/// before SaslHandshake answers ILLEGAL_SASL_STATE and closes; an unknown mechanism
/// answers UNSUPPORTED_SASL_MECHANISM; a PLAINTEXT client cannot use the listener.
pub async fn test_auth_unauthenticated_request_rejected() -> TestResult {
    println!("=== Test: unauthenticated requests on a SASL listener ===\n");
    auth_env().await?;

    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let av = roundtrip(&mut s, 0, 1, &ApiVersionsRequest::default()).await?;
    assert_eq!(
        av.error_code, 0,
        "ApiVersions is allowed before authentication"
    );
    for (key, min, max) in [(17i16, 0i16, 1i16), (36, 0, 2)] {
        let api = av
            .api_keys
            .iter()
            .find(|k| k.api_key == key)
            .ok_or(format!("api {} not advertised", key))?;
        assert_eq!((api.min_version, api.max_version), (min, max));
    }
    let no_response = roundtrip(&mut s, 4, 2, &MetadataRequest::default())
        .await
        .is_err();
    assert!(
        no_response,
        "Metadata before authentication gets no response"
    );
    assert!(closed_by_server(&mut s).await);
    println!("✅ Metadata before authentication closes the connection");

    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let au = roundtrip(&mut s, 2, 3, &authenticate(plain_token(USER, PASSWORD))).await?;
    assert_eq!(au.error_code, ERROR_ILLEGAL_SASL_STATE);
    assert!(closed_by_server(&mut s).await);
    println!("✅ SaslAuthenticate before SaslHandshake: ILLEGAL_SASL_STATE + close");

    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let hs = roundtrip(&mut s, 1, 5, &handshake("GSSAPI")).await?;
    assert_eq!(hs.error_code, ERROR_UNSUPPORTED_SASL_MECHANISM);
    assert_eq!(hs.mechanisms.len(), 3);
    assert!(closed_by_server(&mut s).await);
    println!("✅ unsupported mechanism: UNSUPPORTED_SASL_MECHANISM + close");

    // An idle connection that never authenticates is closed after 10 s.
    let mut idle = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let started = Instant::now();
    let mut buf = [0u8; 1];
    let closed = matches!(
        tokio::time::timeout(Duration::from_secs(20), idle.read(&mut buf)).await,
        Ok(Ok(0)) | Ok(Err(_))
    );
    let waited = started.elapsed();
    assert!(closed, "an unauthenticated idle connection must be closed");
    assert!(
        waited >= Duration::from_secs(8) && waited < Duration::from_secs(15),
        "closed after {:?}, expected about 10 s",
        waited
    );
    println!(
        "✅ idle unauthenticated connection closed after {:?}",
        waited
    );

    // A frame larger than the pre-authentication limit (512 KiB) is refused.
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    s.write_all(&(1024u32 * 1024).to_be_bytes()).await?;
    s.write_all(&[0u8; 64]).await?;
    assert!(closed_by_server(&mut s).await);
    println!("✅ 1 MiB frame before authentication closes the connection");

    let mut s = TcpStream::connect(("127.0.0.1", SASL_PLAINTEXT_PORT)).await?;
    let hs = roundtrip(&mut s, 0, 6, &handshake("PLAIN")).await?;
    assert_eq!(hs.error_code, ERROR_UNSUPPORTED_VERSION);
    assert!(closed_by_server(&mut s).await);
    println!("✅ SaslHandshake v0 (unframed tokens): UNSUPPORTED_VERSION + close");

    assert!(
        !can_connect(client_config(SASL_PLAINTEXT_PORT, "plaintext")).await,
        "a client without SASL cannot use the SASL listener"
    );
    println!("✅ Test PASSED\n");
    Ok(())
}

/// The default PLAINTEXT listener is unchanged: no authentication, and the SASL APIs
/// answer ILLEGAL_SASL_STATE while the connection stays usable.
pub async fn test_auth_plaintext_listener_unaffected() -> TestResult {
    println!("=== Test: PLAINTEXT listener keeps serving without auth ===\n");
    auth_env().await?;
    let mut s = TcpStream::connect(("127.0.0.1", PLAINTEXT_PORT)).await?;
    let hs = roundtrip(&mut s, 1, 1, &handshake("PLAIN")).await?;
    assert_eq!(hs.error_code, ERROR_ILLEGAL_SASL_STATE);
    let au = roundtrip(&mut s, 2, 2, &authenticate(plain_token(USER, PASSWORD))).await?;
    assert_eq!(au.error_code, ERROR_ILLEGAL_SASL_STATE);
    let md = roundtrip(&mut s, 4, 3, &MetadataRequest::default()).await?;
    assert_eq!(md.brokers.len(), 1);
    assert_eq!(md.brokers[0].port, PLAINTEXT_PORT as i32);
    println!("✅ SASL APIs answer ILLEGAL_SASL_STATE; Metadata still served");
    round_trip(
        client_config(PLAINTEXT_PORT, "plaintext"),
        PLAINTEXT_PORT,
        "auth-plaintext",
    )
    .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// SSL listener: round trip with a client trusting the test CA; a client trusting a
/// different CA and a PLAINTEXT client are both refused.
pub async fn test_auth_tls_roundtrip_and_untrusted_ca() -> TestResult {
    println!("=== Test: SSL listener ===\n");
    auth_env().await?;
    round_trip(client_config(SSL_PORT, "ssl"), SSL_PORT, "auth-ssl").await?;
    let mut untrusted = client_config(SSL_PORT, "ssl");
    untrusted.set("ssl.ca.location", cert_path("other-ca.pem"));
    assert!(
        !can_connect(untrusted).await,
        "a client that does not trust the server's CA must fail the handshake"
    );
    println!("✅ client with an unrelated CA refused");
    assert!(!can_connect(client_config(SSL_PORT, "plaintext")).await);
    println!("✅ PLAINTEXT client on the SSL listener refused");
    println!("✅ Test PASSED\n");
    Ok(())
}

/// SASL_SSL listener: SCRAM-SHA-512 and PLAIN round trips; a wrong password fails.
pub async fn test_auth_sasl_ssl_roundtrip() -> TestResult {
    println!("=== Test: SASL_SSL listener ===\n");
    auth_env().await?;
    round_trip(
        sasl_config(SASL_SSL_PORT, "sasl_ssl", "SCRAM-SHA-512", USER, PASSWORD),
        SASL_SSL_PORT,
        "auth-saslssl-scram",
    )
    .await?;
    round_trip(
        sasl_config(SASL_SSL_PORT, "sasl_ssl", "PLAIN", USER, PASSWORD),
        SASL_SSL_PORT,
        "auth-saslssl-plain",
    )
    .await?;
    assert!(
        !can_connect(sasl_config(
            SASL_SSL_PORT,
            "sasl_ssl",
            "SCRAM-SHA-256",
            USER,
            "bad"
        ))
        .await
    );
    println!("✅ Test PASSED\n");
    Ok(())
}

/// User management SQL: duplicates/unknown users are errors, pgkafka_reader cannot
/// read the secrets but sees kafka.sasl_users, no password is stored, and a verifier
/// computed client-side (libpq format, via postgres-protocol) authenticates.
pub async fn test_auth_user_management_sql() -> TestResult {
    println!("=== Test: Kafka user management SQL ===\n");
    auth_env().await?;
    let db = create_db_client().await?;
    let user = "e2e-auth-sql-user";
    db.execute("SELECT pg_kafka_drop_user($1)", &[&user])
        .await?;
    db.execute(
        "SELECT pg_kafka_create_user($1, 'first-pw', 8192)",
        &[&user],
    )
    .await?;
    let dup = db
        .execute("SELECT pg_kafka_create_user($1, 'x')", &[&user])
        .await;
    assert!(dup.is_err(), "creating an existing user fails");
    assert!(db
        .execute("SELECT pg_kafka_alter_user('e2e-auth-nobody', 'x')", &[])
        .await
        .is_err());
    assert!(
        db.execute(
            "SELECT pg_kafka_create_user('e2e-auth-weak', 'x', 100)",
            &[]
        )
        .await
        .is_err(),
        "iterations below 4096 are refused"
    );

    let rows = db
        .query(
            "SELECT mechanism, iterations, octet_length(stored_key), \
             position(convert_to('first-pw', 'UTF8') in stored_key || server_key || salt) \
             FROM kafka.sasl_credentials WHERE username = $1 ORDER BY mechanism",
            &[&user],
        )
        .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "SCRAM-SHA-256");
    assert_eq!(rows[0].get::<_, i32>(1), 8192);
    assert_eq!(rows[0].get::<_, i32>(2), 32);
    assert_eq!(rows[1].get::<_, i32>(2), 64);
    assert!(
        rows.iter().all(|r| r.get::<_, i32>(3) == 0),
        "no plaintext stored"
    );

    // pgkafka_reader: no access to the secrets, the view works.
    db.batch_execute("BEGIN; SET LOCAL ROLE pgkafka_reader")
        .await?;
    let denied = db.query("SELECT * FROM kafka.sasl_credentials", &[]).await;
    assert!(
        denied.is_err(),
        "pgkafka_reader must not read kafka.sasl_credentials"
    );
    db.batch_execute("ROLLBACK; BEGIN; SET LOCAL ROLE pgkafka_reader")
        .await?;
    let n: i64 = db
        .query_one(
            "SELECT count(*) FROM kafka.sasl_users WHERE username = $1",
            &[&user],
        )
        .await?
        .get(0);
    assert_eq!(n, 2);
    db.batch_execute("ROLLBACK").await?;
    println!("✅ reader sees kafka.sasl_users but not the credentials");

    // alter_user changes the password.
    db.execute("SELECT pg_kafka_alter_user($1, 'second-pw')", &[&user])
        .await?;
    let cfg =
        |mech: &str, pw: &str| sasl_config(SASL_PLAINTEXT_PORT, "sasl_plaintext", mech, user, pw);
    assert!(can_connect(cfg("SCRAM-SHA-512", "second-pw")).await);
    assert!(!can_connect(cfg("SCRAM-SHA-512", "first-pw")).await);

    // A verifier computed on the client: the plaintext never reaches the server.
    let verifier = postgres_protocol::password::scram_sha_256(b"verifier-pw");
    db.execute("SELECT pg_kafka_drop_user($1)", &[&user])
        .await?;
    db.execute(
        "SELECT pg_kafka_set_user_verifier($1, $2)",
        &[&user, &verifier],
    )
    .await?;
    assert!(can_connect(cfg("SCRAM-SHA-256", "verifier-pw")).await);
    assert!(
        can_connect(cfg("PLAIN", "verifier-pw")).await,
        "PLAIN checks the SCRAM-SHA-256 verifier"
    );
    assert!(
        !can_connect(cfg("SCRAM-SHA-512", "verifier-pw")).await,
        "only the installed mechanism exists"
    );
    assert!(db
        .execute("SELECT pg_kafka_set_user_verifier($1, 'md5abc')", &[&user])
        .await
        .is_err());
    db.execute("SELECT pg_kafka_drop_user($1)", &[&user])
        .await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Mutual TLS: with pg_kafka.ssl_ca_file set, the SSL listener requires a client
/// certificate signed by that CA; with one, the round trip works.
pub async fn test_auth_mutual_tls() -> TestResult {
    println!("=== Test: SSL listener with mutual TLS ===\n");
    auth_env().await?;
    let db = create_db_client().await?;
    configure_listeners(&db, true, true).await?;
    restart_listener(&db, &[SASL_PLAINTEXT_PORT, SSL_PORT, SASL_SSL_PORT], &[]).await?;
    let result = async {
        assert!(
            !can_connect(client_config(SSL_PORT, "ssl")).await,
            "a client without a certificate is refused"
        );
        println!("✅ client without a certificate refused");
        let mut with_cert = client_config(SSL_PORT, "ssl");
        with_cert
            .set("ssl.certificate.location", cert_path("client.pem"))
            .set("ssl.key.location", cert_path("client.key"));
        round_trip(with_cert, SSL_PORT, "auth-mtls").await?;
        println!("✅ client with a CA-signed certificate served");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());
    // Back to one-way TLS for the remaining tests, whatever happened.
    configure_listeners(&db, true, false).await?;
    restart_listener(&db, &[SASL_PLAINTEXT_PORT, SSL_PORT, SASL_SSL_PORT], &[]).await?;
    result?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Last auth test: reset the listener GUCs and restart the worker, so only the
/// PLAINTEXT listener is left (the server is as it was before the auth tests).
pub async fn test_auth_teardown_restores_default_listeners() -> TestResult {
    println!("=== Test: auth teardown ===\n");
    let db = create_db_client().await?;
    db.execute("SELECT pg_kafka_drop_user($1)", &[&USER])
        .await?;
    configure_listeners(&db, false, false).await?;
    restart_listener(&db, &[], &[SASL_PLAINTEXT_PORT, SSL_PORT, SASL_SSL_PORT]).await?;
    println!("✅ only the PLAINTEXT listener is left");
    println!("✅ Test PASSED\n");
    Ok(())
}
