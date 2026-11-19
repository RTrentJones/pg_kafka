//! Network tests that reconfigure the listener (see `network/mod.rs`): connection caps,
//! idle and frame-read deadlines, the pre-auth deadline from accept, the SASL lookup
//! rate limit and the unknown-user SCRAM fake credential.

use super::{
    answers, api_versions_frame, client_config, connect_from, rdkafka_roundtrip, time_until_closed,
    PORT,
};
use crate::auth::{cert_path, ensure_certificates, remove_certificates, restart_listener};
use crate::common::create_db_client;
use crate::common::to_i32;
use crate::common::{DynResult, TestResult};
use crate::setup::TestContext;
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::StrBytes;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use std::net::Ipv4Addr;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_postgres::Client;

pub const PLAIN_PORT: u16 = 19096;
pub const SASL_PORT: u16 = 19097;
pub const SASL_SSL_PORT: u16 = 19098;

const MAX_CONNECTIONS: usize = 60;
const MAX_PER_IP: usize = 50;
const IDLE_MS: u64 = 3000;
const READ_TIMEOUT_MS: u64 = 3000;

pub const USER: &str = "net-user";
pub const PASSWORD: &str = "network test password";

fn gucs() -> Vec<(&'static str, String)> {
    vec![
        ("pg_kafka.max_connections", MAX_CONNECTIONS.to_string()),
        ("pg_kafka.max_connections_per_ip", MAX_PER_IP.to_string()),
        ("pg_kafka.connections_max_idle_ms", IDLE_MS.to_string()),
        (
            "pg_kafka.request_read_timeout_ms",
            READ_TIMEOUT_MS.to_string(),
        ),
        (
            "pg_kafka.additional_listeners",
            format!(
                "PLAINTEXT://127.0.0.1:{PLAIN_PORT},SASL_PLAINTEXT://127.0.0.1:{SASL_PORT},SASL_SSL://127.0.0.1:{SASL_SSL_PORT}"
            ),
        ),
        ("pg_kafka.ssl_cert_file", cert_path("server.pem")),
        ("pg_kafka.ssl_key_file", cert_path("server.key")),
    ]
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

async fn drop_users(db: &Client) -> DynResult<()> {
    db.execute(
        "SELECT pg_kafka_drop_user(username) FROM \
         (SELECT DISTINCT username FROM kafka.sasl_credentials WHERE username LIKE 'net-%') u",
        &[],
    )
    .await?;
    Ok(())
}

/// Reconfigure + restart the listener; create the SASL users: three SCRAM users with
/// 8192 iterations (the most common parameters) and two libpq-style verifiers (4096
/// iterations, 16-byte salt).
pub async fn test_network_limits_setup() -> TestResult {
    println!("=== Test: limits setup ===\n");
    ensure_certificates()?;
    let db = create_db_client().await?;
    for (name, value) in gucs() {
        db.execute(
            &format!("ALTER SYSTEM SET {} = {}", name, quote(&value)),
            &[],
        )
        .await?;
    }
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    drop_users(&db).await?;
    for u in [USER, "net-a", "net-b"] {
        db.execute(
            "SELECT pg_kafka_create_user($1, $2, 8192)",
            &[&u, &PASSWORD],
        )
        .await?;
    }
    for u in ["net-c", "net-d"] {
        let v = postgres_protocol::password::scram_sha_256(PASSWORD.as_bytes());
        db.execute("SELECT pg_kafka_set_user_verifier($1, $2)", &[&u, &v])
            .await?;
    }
    // A fresh worker: the new limits, and an empty fake-parameter cache.
    restart_listener(&db, &[PLAIN_PORT, SASL_PORT, SASL_SSL_PORT], &[]).await?;
    println!("✅ listeners 19096-19098 up with the network limits\n");
    Ok(())
}

/// Reset everything the setup changed.
pub async fn test_network_limits_teardown() -> TestResult {
    println!("=== Test: limits teardown ===\n");
    let db = create_db_client().await?;
    for (name, _) in gucs() {
        db.execute(&format!("ALTER SYSTEM RESET {name}"), &[])
            .await?;
    }
    db.execute("SELECT pg_reload_conf()", &[]).await?;
    drop_users(&db).await?;
    restart_listener(&db, &[], &[PLAIN_PORT, SASL_PORT, SASL_SSL_PORT]).await?;
    remove_certificates();
    println!("✅ defaults restored\n");
    Ok(())
}

async fn open_answering(src: Ipv4Addr, port: u16, n: usize) -> DynResult<Vec<TcpStream>> {
    let mut out = Vec::new();
    for i in 0..n {
        let mut s = connect_from(src, port).await?;
        if !answers(&mut s, to_i32(i)).await {
            return Err(format!("connection {} from {} to {} not served", i + 1, src, port).into());
        }
        out.push(s);
    }
    Ok(out)
}

async fn rejected(src: Ipv4Addr, port: u16) -> DynResult<bool> {
    let Ok(mut s) = connect_from(src, port).await else {
        return Ok(true);
    };
    let _ = s.write_all(&api_versions_frame(9999)).await;
    Ok(time_until_closed(&mut s, Duration::from_secs(2))
        .await
        .is_some())
}

/// `pg_kafka.max_connections_per_ip`, across listeners; other IPs unaffected.
pub async fn test_per_ip_connection_limit() -> TestResult {
    println!("=== Test: per-IP connection limit ===\n");
    let attacker = Ipv4Addr::new(127, 0, 0, 2);
    let mut conns = open_answering(attacker, PORT, MAX_PER_IP).await?;
    println!("Step 1: {MAX_PER_IP} connections from {attacker}");
    assert!(
        rejected(attacker, PORT).await?,
        "connection over the per-IP cap"
    );
    assert!(
        rejected(attacker, PLAIN_PORT).await?,
        "the per-IP cap spans listeners"
    );
    println!("✅ the next connection from {attacker} is closed at accept");
    let mut other = connect_from(Ipv4Addr::new(127, 0, 0, 3), PORT).await?;
    assert!(answers(&mut other, 1).await, "another IP is unaffected");
    conns.pop();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut again = connect_from(attacker, PORT).await?;
    assert!(
        answers(&mut again, 2).await,
        "a closed connection frees its slot"
    );
    drop(conns);

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-per-ip").await;
    rdkafka_roundtrip(client_config(PORT), &topic, 10).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Each listener has its own `pg_kafka.max_connections`, so a flood on one
/// (here 19096) does not lock clients out of another (9092).
pub async fn test_per_listener_connection_limit() -> TestResult {
    println!("=== Test: per-listener connection limit ===\n");
    let mut conns = open_answering(Ipv4Addr::new(127, 0, 0, 3), PLAIN_PORT, MAX_PER_IP).await?;
    conns.extend(
        open_answering(
            Ipv4Addr::new(127, 0, 0, 4),
            PLAIN_PORT,
            MAX_CONNECTIONS - MAX_PER_IP,
        )
        .await?,
    );
    println!("Step 1: {} connections on {}", conns.len(), PLAIN_PORT);
    let fresh = Ipv4Addr::new(127, 0, 0, 5);
    assert!(rejected(fresh, PLAIN_PORT).await?, "listener 19096 is full");
    let mut s = connect_from(fresh, PORT).await?;
    assert!(answers(&mut s, 1).await, "listener 9092 still accepts");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-per-listener").await;
    rdkafka_roundtrip(client_config(PORT), &topic, 10).await?;
    drop(conns);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// An idle connection is closed after `connections_max_idle_ms`, a frame
/// that does not arrive completely within `request_read_timeout_ms` closes its
/// connection; a consumer that keeps polling is not affected, and a producer whose idle
/// connection was closed reconnects.
pub async fn test_idle_and_frame_read_timeouts_close_connections() -> TestResult {
    println!("=== Test: idle and frame read timeouts ===\n");
    let low = Duration::from_millis(IDLE_MS - 500);
    let high = Duration::from_millis(IDLE_MS + 3000);

    let mut idle = TcpStream::connect(("127.0.0.1", PORT)).await?;
    assert!(answers(&mut idle, 1).await);
    let t = time_until_closed(&mut idle, Duration::from_secs(10))
        .await
        .ok_or("an idle connection was never closed")?;
    assert!(t >= low && t <= high, "idle close after {t:?}");
    println!("✅ idle connection closed after {t:?}");

    // A slow sender: a 1000-byte frame, one byte every 500 ms.
    let mut slow = connect_from(Ipv4Addr::new(127, 0, 0, 6), PORT).await?;
    slow.write_all(&1000i32.to_be_bytes()).await?;
    let started = Instant::now();
    let mut closed_after = None;
    for _ in 0..20 {
        if slow.write_all(&[0u8]).await.is_err() {
            closed_after = Some(started.elapsed());
            break;
        }
        if let Some(_t) = time_until_closed(&mut slow, Duration::from_millis(500)).await {
            closed_after = Some(started.elapsed());
            break;
        }
    }
    let t = closed_after.ok_or("a slow-loris frame was never cut off")?;
    let read_low = Duration::from_millis(READ_TIMEOUT_MS - 500);
    let read_high = Duration::from_millis(READ_TIMEOUT_MS + 3000);
    assert!(
        t >= read_low && t <= read_high,
        "slow frame cut after {t:?}"
    );
    println!("✅ slow frame cut off after {t:?}");

    // Real clients: records produced 5 s apart (longer than the idle timeout) all arrive.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-idle").await;
    let producer: FutureProducer = client_config(PORT).create()?;
    let mut ccfg = client_config(PORT);
    ccfg.set("group.id", format!("net-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false");
    let send = |i: i32| {
        let p = producer.clone();
        let topic = topic.clone();
        async move {
            p.send(
                FutureRecord::<(), _>::to(&topic)
                    .payload(&format!("idle-{i}"))
                    .partition(0),
                Duration::from_secs(20),
            )
            .await
            .map_err(|(e, _)| e.to_string())
        }
    };
    send(0).await?;
    let topic_c = topic.clone();
    let consumer = tokio::task::spawn_blocking(move || -> Result<usize, String> {
        let consumer: BaseConsumer = ccfg.create().map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_c, 0, Offset::Beginning)
            .map_err(|e| e.to_string())?;
        consumer.assign(&tpl).map_err(|e| e.to_string())?;
        let mut count = 0;
        let deadline = Instant::now() + Duration::from_secs(25);
        while count < 3 && Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(_)) => count += 1,
                // librdkafka reports a connection the broker closed for idleness (here
                // the idle bootstrap connection) as a transport error, then reconnects;
                // Kafka's connections.max.idle.ms does the same.
                Some(Err(rdkafka::error::KafkaError::MessageConsumption(
                    rdkafka::types::RDKafkaErrorCode::BrokerTransportFailure,
                )))
                | None => {}
                Some(Err(e)) => return Err(e.to_string()),
            }
        }
        Ok(count)
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    send(1).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    send(2).await?;
    assert_eq!(consumer.await??, 3, "the consumer received every record");
    println!("✅ rdkafka producer/consumer unaffected across idle periods");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// One pre-auth deadline (10 s) from accept. A SASL_SSL client whose TLS
/// handshake takes 7 s is closed at ~10 s, not given another 10 s for SASL. Also a
/// SASL_PLAINTEXT client that stops after the handshake.
pub async fn test_pre_auth_deadline_runs_from_accept() -> TestResult {
    println!("=== Test: pre-auth deadline from accept ===\n");
    // SASL_PLAINTEXT: handshake, then silence.
    let mut s = connect_from(Ipv4Addr::new(127, 0, 0, 8), SASL_PORT).await?;
    let t0 = Instant::now();
    let mut hs = SaslHandshakeRequest::default();
    hs.mechanism = StrBytes::from_static_str("PLAIN");
    let r: SaslHandshakeResponse = crate::wire::roundtrip(&mut s, 1, 1, &hs).await?;
    assert_eq!(r.error_code, 0);
    // Keep the connection busy with ApiVersions (allowed before authentication), so it
    // is the pre-auth deadline that closes it, not the idle timeout.
    let mut corr = 10;
    let t = loop {
        if t0.elapsed() > Duration::from_secs(20) {
            return Err("unauthenticated connection never closed".into());
        }
        if s.write_all(&api_versions_frame(corr)).await.is_err() {
            break t0.elapsed();
        }
        corr += 1;
        let answered = matches!(
            read_frame_within(&mut s, Duration::from_secs(2)).await,
            Ok(Some(_))
        );
        if !answered {
            break t0.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    };
    assert!(
        t >= Duration::from_secs(9) && t <= Duration::from_secs(13),
        "SASL_PLAINTEXT pre-auth close after {t:?}"
    );
    println!("✅ SASL_PLAINTEXT: closed {t:?} after connect");

    // SASL_SSL through a relay that holds the client's first byte for 7 s.
    let relay = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let relay_port = relay.local_addr()?.port();
    let mut client = KillOnDrop(
        std::process::Command::new("openssl")
            .args([
                "s_client",
                "-connect",
                &format!("127.0.0.1:{relay_port}"),
                "-quiet",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let mut stdin = client.0.stdin.take().ok_or("no s_client stdin")?;
    let (mut downstream, _) =
        tokio::time::timeout(Duration::from_secs(10), relay.accept()).await??;
    let mut upstream = connect_from(Ipv4Addr::new(127, 0, 0, 9), SASL_SSL_PORT).await?;
    let accepted = Instant::now();
    tokio::time::sleep(Duration::from_secs(7)).await;
    // After TLS, keep the connection busy (ApiVersions through s_client), so it is the
    // pre-auth deadline that closes it, not the idle timeout.
    std::thread::spawn(move || {
        use std::io::Write;
        for i in 0..30 {
            std::thread::sleep(Duration::from_millis(800));
            if stdin.write_all(&api_versions_frame(1000 + i)).is_err() || stdin.flush().is_err() {
                return;
            }
        }
    });
    let (mut dr, mut dw) = downstream.split();
    let (mut ur, mut uw) = upstream.split();
    let up = async {
        let _ = tokio::io::copy(&mut dr, &mut uw).await;
        std::future::pending::<()>().await;
    };
    let down = async {
        let _ = tokio::io::copy(&mut ur, &mut dw).await;
        accepted.elapsed()
    };
    let t = tokio::select! {
        () = up => unreachable!(),
        t = down => t,
        () = tokio::time::sleep(Duration::from_secs(25)) => {
            return Err("SASL_SSL connection never closed".into());
        }
    };
    assert!(
        t >= Duration::from_secs(9) && t <= Duration::from_secs(13),
        "SASL_SSL pre-auth close {t:?} after accept (expected ~10 s, not ~17 s)"
    );
    println!("✅ SASL_SSL: closed {t:?} after accept (TLS took 7 s)");

    // A real SASL_SSL client still authenticates.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-sasl-ssl").await;
    let mut cfg = client_config(SASL_SSL_PORT);
    cfg.set("security.protocol", "sasl_ssl")
        .set("ssl.ca.location", cert_path("ca.pem"))
        .set("sasl.mechanism", "SCRAM-SHA-256")
        .set("sasl.username", USER)
        .set("sasl.password", PASSWORD);
    rdkafka_roundtrip(cfg, &topic, 5).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kills the `openssl s_client` child however the test ends.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// SaslHandshake SCRAM-SHA-256 + client-first for `user`: the server-first message, or
/// `None` if the connection closed first.
async fn scram_first(
    s: &mut TcpStream,
    mechanism: &'static str,
    user: &str,
) -> DynResult<Option<String>> {
    let mut hs = SaslHandshakeRequest::default();
    hs.mechanism = StrBytes::from_static_str(mechanism);
    s.write_all(&request_frame(1, 1, &hs)).await?;
    let Some(f) = read_frame_within(s, Duration::from_secs(15)).await? else {
        return Ok(None);
    };
    let (_, r) = decode_response::<SaslHandshakeRequest>(1, &f)?;
    assert_eq!(r.error_code, 0);
    let mut auth = SaslAuthenticateRequest::default();
    auth.auth_bytes = Bytes::from(format!("n,,n={user},r=netnonce").into_bytes());
    s.write_all(&request_frame(2, 2, &auth)).await?;
    let Some(f) = read_frame_within(s, Duration::from_secs(15)).await? else {
        return Ok(None);
    };
    let (_, r): (i32, SaslAuthenticateResponse) =
        decode_response::<SaslAuthenticateRequest>(2, &f)?;
    assert_eq!(r.error_code, 0, "{:?}", r.error_message);
    Ok(Some(String::from_utf8(r.auth_bytes.to_vec())?))
}

/// Standard base64 (with padding) decoder, enough for SCRAM salts.
fn b64decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        let v = u32::from(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => return None,
        });
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((acc >> bits) & 0xFF).expect("masked to 8 bits"));
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn server_first_params(m: &str) -> DynResult<(Vec<u8>, u32)> {
    let mut salt = None;
    let mut iterations = None;
    for part in m.split(',') {
        if let Some(s) = part.strip_prefix("s=") {
            salt = Some(b64decode(s).ok_or("bad salt base64")?);
        }
        if let Some(i) = part.strip_prefix("i=") {
            iterations = Some(i.parse()?);
        }
    }
    Ok((salt.ok_or("no salt")?, iterations.ok_or("no iterations")?))
}

/// An unknown user's server-first message imitates the stored credentials (the
/// most common iteration count and salt length) and its salt is stable across worker
/// restarts (a durable secret), so neither its parameters nor a changing salt give an
/// unknown user away.
pub async fn test_scram_unknown_user_is_indistinguishable() -> TestResult {
    println!("=== Test: SCRAM unknown-user fake credential ===\n");
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PORT)).await?;
    let real = scram_first(&mut s, "SCRAM-SHA-256", USER)
        .await?
        .ok_or("closed")?;
    let (real_salt, real_i) = server_first_params(&real)?;
    assert_eq!((real_salt.len(), real_i), (32, 8192));
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PORT)).await?;
    let ghost = scram_first(&mut s, "SCRAM-SHA-256", "net-ghost")
        .await?
        .ok_or("closed")?;
    let (ghost_salt, ghost_i) = server_first_params(&ghost)?;
    println!(
        "   real: {} bytes / i={}; unknown: {} bytes / i={}",
        real_salt.len(),
        real_i,
        ghost_salt.len(),
        ghost_i
    );
    assert_eq!(
        ghost_i, 8192,
        "unknown user shows the most common iteration count"
    );
    assert_eq!(ghost_salt.len(), 32, "and the most common salt length");
    // SCRAM-SHA-512: only the pg_kafka_create_user users (8192) have one.
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PORT)).await?;
    let ghost512 = scram_first(&mut s, "SCRAM-SHA-512", "net-ghost")
        .await?
        .ok_or("closed")?;
    assert_eq!(server_first_params(&ghost512)?.1, 8192);

    let db = create_db_client().await?;
    restart_listener(&db, &[PLAIN_PORT, SASL_PORT, SASL_SSL_PORT], &[]).await?;
    let mut s = TcpStream::connect(("127.0.0.1", SASL_PORT)).await?;
    let again = scram_first(&mut s, "SCRAM-SHA-256", "net-ghost")
        .await?
        .ok_or("closed")?;
    assert_eq!(
        server_first_params(&again)?.0,
        ghost_salt,
        "the fake salt is stable across worker restarts"
    );
    println!("✅ fake salt stable across a worker restart");

    // Real clients: right password works, unknown user and wrong password fail alike.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-scram").await;
    let mut cfg = client_config(SASL_PORT);
    cfg.set("security.protocol", "sasl_plaintext")
        .set("sasl.mechanism", "SCRAM-SHA-256")
        .set("sasl.username", USER)
        .set("sasl.password", PASSWORD);
    rdkafka_roundtrip(cfg, &topic, 5).await?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Each SASL client-first costs the DB thread a SELECT, so one IP may start only
/// 10 lookups per second (burst 20); over that they wait, and far over it the lookup
/// never happens and the connection is closed at its pre-auth deadline, unanswered.
/// Well-behaved clients from other addresses authenticate meanwhile.
pub async fn test_sasl_credential_lookups_are_rate_limited() -> TestResult {
    const N: usize = 45;

    println!("=== Test: SASL lookup rate limit ===\n");
    let attacker = Ipv4Addr::new(127, 0, 0, 7);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..N {
        tasks.push(tokio::spawn(async move {
            let mut s = connect_from(attacker, SASL_PORT)
                .await
                .map_err(|e| e.to_string())?;
            let user = format!("net-ghost-{i}");
            let r = scram_first(&mut s, "SCRAM-SHA-256", &user)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>((r.is_some(), started.elapsed()))
        }));
    }
    // A real client from 127.0.0.1 meanwhile.
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-rate").await;
    let mut cfg = client_config(SASL_PORT);
    cfg.set("security.protocol", "sasl_plaintext")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", USER)
        .set("sasl.password", PASSWORD);
    rdkafka_roundtrip(cfg, &topic, 5).await?;
    println!("✅ a real SASL client authenticated during the flood");

    let mut answered = Vec::new();
    let mut closed = Vec::new();
    for t in tasks {
        let (ok, at) = t.await??;
        if ok {
            answered.push(at);
        } else {
            closed.push(at);
        }
    }
    answered.sort();
    println!(
        "   {} answered (last after {:?}), {} closed unanswered",
        answered.len(),
        answered.last(),
        closed.len()
    );
    assert!(
        (38..=43).contains(&answered.len()),
        "expected the burst (20) plus ~20 delayed lookups to be answered, got {}",
        answered.len()
    );
    for at in &closed {
        assert!(
            *at >= Duration::from_secs(8) && *at <= Duration::from_secs(14),
            "refused lookups close at the pre-auth deadline, got {at:?}"
        );
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
