//! Wave 3, stream `net`: hostile clients at the network edge.
//!
//! One test per finding (frame-budget lockout, oversized response, Nagle, acks=0 close,
//! invalid acks, response before close, per-IP / per-listener limits, idle and frame
//! read deadlines, the pre-auth deadline from accept, the SASL lookup rate limit, SCRAM
//! enumeration), each checking that ordinary rdkafka clients keep working while the
//! attack runs.
//!
//! `tests` run against the default configuration. `limits` reconfigure the listener
//! (lower caps and timeouts, three extra listeners: PLAINTEXT 19096, SASL_PLAINTEXT
//! 19097, SASL_SSL 19098) through ALTER SYSTEM and restart the worker, like the auth
//! category; the last one resets everything. Attack connections bind other loopback
//! source addresses (127.0.0.2, ...) so their per-IP accounting is separate from the
//! well-behaved clients on 127.0.0.1. All tests are `parallel_safe: false`.

pub mod limits;
pub mod tests;

use crate::common::TestResult;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::{RequestHeader, ResponseHeader, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

pub type DynResult<T> = Result<T, Box<dyn std::error::Error>>;

pub const PORT: u16 = 9092;

/// Connect to `127.0.0.1:port` from the loopback source address `src`.
pub async fn connect_from(src: Ipv4Addr, port: u16) -> std::io::Result<TcpStream> {
    let socket = TcpSocket::new_v4()?;
    socket.bind(SocketAddr::from((src, 0)))?;
    socket
        .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .await
}

/// An encoded request frame (length prefix included).
pub fn request_frame<R: Request>(version: i16, correlation_id: i32, request: &R) -> Vec<u8> {
    let mut header = RequestHeader::default();
    header.request_api_key = R::KEY;
    header.request_api_version = version;
    header.correlation_id = correlation_id;
    header.client_id = Some(StrBytes::from_static_str("w3-net-e2e"));
    let mut body = BytesMut::new();
    header
        .encode(&mut body, R::header_version(version))
        .expect("encode header");
    request.encode(&mut body, version).expect("encode body");
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame
}

/// An ApiVersions v0 frame.
pub fn api_versions_frame(correlation_id: i32) -> Vec<u8> {
    request_frame(
        0,
        correlation_id,
        &kafka_protocol::messages::ApiVersionsRequest::default(),
    )
}

/// Read one frame; `Ok(None)` on EOF / reset (the server closed the connection).
pub async fn read_frame(stream: &mut TcpStream, within: Duration) -> DynResult<Option<Vec<u8>>> {
    let read = async {
        let mut size = [0u8; 4];
        if stream.read_exact(&mut size).await.is_err() {
            return Ok::<_, Box<dyn std::error::Error>>(None);
        }
        let mut buf = vec![0u8; i32::from_be_bytes(size) as usize];
        if stream.read_exact(&mut buf).await.is_err() {
            return Ok(None);
        }
        Ok(Some(buf))
    };
    match tokio::time::timeout(within, read).await {
        Ok(r) => r,
        Err(_) => Err(format!("no frame and no close within {:?}", within).into()),
    }
}

/// Decode a response frame for `R` at `version`: (correlation id, body).
pub fn decode_response<R: Request>(version: i16, frame: &[u8]) -> DynResult<(i32, R::Response)> {
    let mut buf = BytesMut::from(frame);
    let header = ResponseHeader::decode(
        &mut buf,
        <R::Response as HeaderVersion>::header_version(version),
    )?;
    Ok((
        header.correlation_id,
        R::Response::decode(&mut buf, version)?,
    ))
}

/// The correlation id of a response frame (first 4 bytes for every header version).
pub fn correlation_of(frame: &[u8]) -> i32 {
    i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]])
}

/// How long until the server closes `stream` (EOF or reset), if it does within `within`.
/// Data the server sends meanwhile is discarded.
pub async fn time_until_closed(stream: &mut TcpStream, within: Duration) -> Option<Duration> {
    let start = Instant::now();
    let mut buf = [0u8; 4096];
    loop {
        let left = within.checked_sub(start.elapsed())?;
        match tokio::time::timeout(left, stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return Some(start.elapsed()),
            Ok(Ok(_)) => continue,
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
        read_frame(stream, Duration::from_secs(3)).await,
        Ok(Some(f)) if correlation_of(&f) == correlation_id
    )
}

/// One record batch (v2) holding `values`.
pub fn record_batch(values: &[Bytes], compression: Compression) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: -1,
            timestamp: 0,
            key: None,
            value: Some(v.clone()),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .expect("encode record batch");
    buf.freeze()
}

/// A Produce request: one topic, `(partition, batch)` pairs.
pub fn produce_request(topic: &str, parts: Vec<(i32, Bytes)>, acks: i16) -> ProduceRequest {
    let mut t = TopicProduceData::default();
    t.name = TopicName(StrBytes::from_string(topic.to_string()));
    for (index, batch) in parts {
        let mut p = PartitionProduceData::default();
        p.index = index;
        p.records = Some(batch);
        t.partition_data.push(p);
    }
    let mut r = ProduceRequest::default();
    r.acks = acks;
    r.timeout_ms = 5000;
    r.topic_data = vec![t];
    r
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
    cfg.set("bootstrap.servers", format!("127.0.0.1:{}", port))
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
                    .key(&format!("k{}", i))
                    .payload(&format!("v{}", i))
                    .partition(0),
                Duration::from_secs(20),
            )
            .await
            .map_err(|(e, _)| format!("rdkafka produce failed: {}", e))?;
    }
    let mut ccfg = cfg.clone();
    ccfg.set("group.id", format!("w3-net-{}", uuid::Uuid::new_v4()))
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
                Some(Err(e)) => return Err(format!("rdkafka consume error: {}", e)),
                None => {}
            }
        }
        Ok(count)
    })
    .await??;
    if got != n {
        return Err(format!("rdkafka consumed {} of {} records", got, n).into());
    }
    Ok(())
}
