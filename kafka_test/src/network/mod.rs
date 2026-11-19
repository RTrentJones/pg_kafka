//! The `network` category: hostile clients at the network edge.
//!
//! One test per attack (frame-budget lockout, oversized response, Nagle, acks=0 close,
//! invalid acks, response before close, per-IP / per-listener limits, idle and frame
//! read deadlines, the pre-auth deadline from accept, the SASL lookup rate limit, SCRAM
//! enumeration), each checking that ordinary rdkafka clients keep working while the
//! attack runs.
//!
//! `hostile_clients` run against the default configuration. `limits` reconfigure the listener
//! (lower caps and timeouts, three extra listeners: PLAINTEXT 19096, SASL_PLAINTEXT
//! 19097, SASL_SSL 19098) through ALTER SYSTEM and restart the worker, like the auth
//! category; the last one resets everything. Attack connections bind other loopback
//! source addresses (127.0.0.2, ...) so their per-IP accounting is separate from the
//! well-behaved clients on 127.0.0.1. All tests are `parallel_safe: false`.

pub mod hostile_clients;
pub mod produce_decoding;

use crate::common::DynResult;
use crate::wire::{correlation_of, read_frame_within, request_frame};
pub mod limits;

use crate::common::TestResult;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

pub const PORT: u16 = 9092;

/// Connect to `127.0.0.1:port` from the loopback source address `src`.
pub async fn connect_from(src: Ipv4Addr, port: u16) -> std::io::Result<TcpStream> {
    let socket = TcpSocket::new_v4()?;
    socket.bind(SocketAddr::from((src, 0)))?;
    socket
        .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
}

/// An ApiVersions v0 frame.
pub fn api_versions_frame(correlation_id: i32) -> Vec<u8> {
    request_frame(
        0,
        correlation_id,
        &kafka_protocol::messages::ApiVersionsRequest::default(),
    )
}

/// How long until the server closes `stream` (EOF or reset), if it does within `within`.
/// Data the server sends meanwhile is discarded.
pub async fn time_until_closed(stream: &mut TcpStream, within: Duration) -> Option<Duration> {
    let start = Instant::now();
    let mut buf = [0u8; 4096];
    loop {
        let left = within.checked_sub(start.elapsed())?;
        match tokio::time::timeout(left, stream.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return Some(start.elapsed()),
            Ok(Ok(_)) => {}
            Err(_) => return None,
        }
    }
}

/// Whether ApiVersions answers on `stream` within 3 s.
pub async fn answers(stream: &mut TcpStream, correlation_id: i32) -> bool {
    if stream
        .write_all(&api_versions_frame(correlation_id))
        .await
        .is_err()
    {
        return false;
    }
    matches!(
        read_frame_within(stream, Duration::from_secs(3)).await,
        Ok(Some(f)) if correlation_of(&f) == correlation_id
    )
}

/// Rows stored for `topic`.
pub async fn stored_rows(db: &tokio_postgres::Client, topic: &str) -> DynResult<i64> {
    Ok(db
        .query_one(
            "SELECT count(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id \
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0))
}

/// rdkafka client config for `127.0.0.1:port`.
pub fn client_config(port: u16) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", format!("127.0.0.1:{port}"))
        .set("broker.address.family", "v4")
        .set("message.timeout.ms", "15000")
        .set("socket.timeout.ms", "15000");
    cfg
}

/// A well-behaved client: produce `n` records with rdkafka through `cfg` and read them
/// back from the beginning of partition 0.
pub async fn rdkafka_roundtrip(cfg: ClientConfig, topic: &str, n: usize) -> TestResult {
    let producer: FutureProducer = cfg.create()?;
    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&format!("k{i}"))
                    .payload(&format!("v{i}"))
                    .partition(0),
                Duration::from_secs(20),
            )
            .await
            .map_err(|(e, _)| format!("rdkafka produce failed: {e}"))?;
    }
    let mut ccfg = cfg.clone();
    ccfg.set("group.id", format!("net-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false");
    let topic_c = topic.to_string();
    let got = tokio::task::spawn_blocking(move || -> Result<usize, String> {
        let consumer: BaseConsumer = ccfg.create().map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic_c, 0, Offset::Beginning)
            .map_err(|e| e.to_string())?;
        consumer.assign(&tpl).map_err(|e| e.to_string())?;
        let mut count = 0;
        let deadline = Instant::now() + Duration::from_secs(20);
        while count < n && Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(m)) => {
                    assert!(m.payload().is_some());
                    count += 1;
                }
                Some(Err(e)) => return Err(format!("rdkafka consume error: {e}")),
                None => {}
            }
        }
        Ok(count)
    })
    .await??;
    if got != n {
        return Err(format!("rdkafka consumed {got} of {n} records").into());
    }
    Ok(())
}

/// The `network` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["network";
        // shared broker resources or reconfigure + restart the listener (limits::*,
        // setup first, teardown last).
        hostile_clients::test_partial_frames_cannot_exhaust_the_frame_budget: sequential,
        hostile_clients::test_response_larger_than_the_request_limit_is_delivered: sequential,
        hostile_clients::test_pipelined_requests_are_not_delayed_by_nagle: sequential,
        hostile_clients::test_acks0_produce_error_closes_connection: sequential,
        hostile_clients::test_invalid_acks_rejected_and_nothing_written: sequential,
        hostile_clients::test_request_before_closing_frame_is_answered: sequential,
        produce_decoding::test_unsupported_version_acks0_produce_closes_connection: sequential,
        limits::test_network_limits_setup: sequential,
        limits::test_scram_unknown_user_is_indistinguishable: sequential,
        limits::test_per_ip_connection_limit: sequential,
        limits::test_per_listener_connection_limit: sequential,
        limits::test_idle_and_frame_read_timeouts_close_connections: sequential,
        limits::test_pre_auth_deadline_runs_from_accept: sequential,
        limits::test_sasl_credential_lookups_are_rate_limited: sequential,
        limits::test_network_limits_teardown: sequential,
    ]
}
