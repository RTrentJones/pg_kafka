//! Write the seed corpora: valid request frames for the registry's APIs (each at its
//! lowest and highest served version) and a few record batches.
//!
//!   cargo run --bin gen_corpus   (from fuzz/)

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    fetch_request, produce_request, AddOffsetsToTxnRequest, AddPartitionsToTxnRequest,
    ApiVersionsRequest, CreatePartitionsRequest, CreateTopicsRequest, DeleteGroupsRequest,
    DeleteRecordsRequest, DeleteTopicsRequest, DescribeConfigsRequest, DescribeGroupsRequest,
    EndTxnRequest, FetchRequest, FindCoordinatorRequest, HeartbeatRequest,
    IncrementalAlterConfigsRequest, InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest,
    ListGroupsRequest, ListOffsetsRequest, MetadataRequest, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, RequestHeader, SaslAuthenticateRequest,
    SaslHandshakeRequest, SyncGroupRequest, TopicName, TxnOffsetCommitRequest,
};
use kafka_protocol::protocol::{Encodable, Request, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use std::path::Path;

fn frame<R: Request + Encodable>(version: i16, request: &R) -> Option<Vec<u8>> {
    let mut header = RequestHeader::default();
    header.request_api_key = R::KEY;
    header.request_api_version = version;
    header.correlation_id = 7;
    header.client_id = Some(StrBytes::from_static_str("fuzz"));
    let mut buf = BytesMut::new();
    header.encode(&mut buf, R::header_version(version)).ok()?;
    request.encode(&mut buf, version).ok()?;
    Some(buf.to_vec())
}

fn batch(compression: Compression, n: usize) -> Bytes {
    let records: Vec<Record> = (0..n)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: if i % 2 == 0 { -1 } else { 5 },
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(i).expect("seed batches are small"),
            sequence: i32::try_from(i).expect("seed batches are small"),
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("value-{i}"))),
            headers: kafka_protocol::indexmap::IndexMap::default(),
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
    .expect("encode batch");
    buf.freeze()
}

/// CRC-32C (Castagnoli), the checksum of a RecordBatch v2, bit by bit.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ 0x82F6_3B78
            };
        }
    }
    !crc
}

/// `v` as a zigzag varint appended to `out`.
fn varint(v: i64, out: &mut Vec<u8>) {
    let mut z = ((v << 1) ^ (v >> 63)).cast_unsigned();
    while z >= 0x80 {
        out.push(u8::try_from(z & 0x7f).expect("seven bits") | 0x80);
        z >>= 7;
    }
    out.push(u8::try_from(z).expect("seven bits"));
}

/// A batch of `n` records whose timestamp delta is a 6-byte varlong (Kafka reads it
/// whole) and whose key holds what a decoder reading a 5-byte varint there would take for
/// a null value and `headers` headers.
fn shifted_varint_batch(n: usize, headers: i64) -> Vec<u8> {
    let mut key = Vec::new();
    varint(-1, &mut key);
    varint(headers, &mut key);
    for i in 0..headers {
        let i = u8::try_from(i % 96).expect("under 96");
        key.extend_from_slice(&[0x04, 0x20, 0x20 + i, 0x01]);
    }
    let mut key_length = Vec::new();
    varint(i64::try_from(key.len()).expect("small"), &mut key_length);
    let mut body = vec![0u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00];
    varint(i64::try_from(key_length.len()).expect("small"), &mut body);
    body.extend_from_slice(&key_length);
    body.extend_from_slice(&key);
    varint(-1, &mut body);
    varint(0, &mut body);
    let mut section = Vec::new();
    for _ in 0..n {
        varint(i64::try_from(body.len()).expect("small"), &mut section);
        section.extend_from_slice(&body);
    }
    let count = i32::try_from(n).expect("small");
    let mut tail = BytesMut::new();
    tail.put_i16(0);
    tail.put_i32(count - 1);
    tail.put_i64(0);
    tail.put_i64(0);
    tail.put_i64(-1);
    tail.put_i16(-1);
    tail.put_i32(-1);
    tail.put_i32(count);
    tail.put_slice(&section);
    let mut batch = BytesMut::new();
    batch.put_i64(0);
    batch.put_i32(i32::try_from(9 + tail.len()).expect("small"));
    batch.put_i32(0);
    batch.put_i8(2);
    batch.put_u32(crc32c(&tail));
    batch.put_slice(&tail);
    batch.to_vec()
}

fn write(dir: &Path, name: &str, data: &[u8]) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(format!("seed-{name}")), data).unwrap();
}

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    let mut frames: Vec<(String, Vec<u8>)> = Vec::new();
    macro_rules! seed {
        ($name:literal, $req:expr, [$($v:expr),*]) => {
            $(if let Some(f) = frame($v, &$req) {
                frames.push((format!("{}-v{}", $name, $v), f));
            })*
        };
    }
    let mut produce = ProduceRequest::default();
    produce.acks = 1;
    produce.timeout_ms = 1000;
    let mut t = produce_request::TopicProduceData::default();
    t.name = TopicName(StrBytes::from_static_str("fuzz-topic"));
    let mut p = produce_request::PartitionProduceData::default();
    p.records = Some(batch(Compression::None, 3));
    t.partition_data.push(p);
    produce.topic_data.push(t);
    let mut fetch = FetchRequest::default();
    let mut ft = fetch_request::FetchTopic::default();
    ft.topic = TopicName(StrBytes::from_static_str("fuzz-topic"));
    ft.partitions.push(fetch_request::FetchPartition::default());
    fetch.topics.push(ft);
    fetch.max_wait_ms = 100;
    seed!("produce", produce, [3, 9]);
    seed!("fetch", fetch, [4, 11]);
    seed!("api_versions", ApiVersionsRequest::default(), [0, 3]);
    seed!("metadata", MetadataRequest::default(), [0, 12]);
    seed!("list_offsets", ListOffsetsRequest::default(), [1, 7]);
    seed!("offset_commit", OffsetCommitRequest::default(), [2, 8]);
    seed!("offset_fetch", OffsetFetchRequest::default(), [1, 7]);
    seed!(
        "find_coordinator",
        FindCoordinatorRequest::default(),
        [0, 4]
    );
    seed!("join_group", JoinGroupRequest::default(), [0, 9]);
    seed!("heartbeat", HeartbeatRequest::default(), [0, 4]);
    seed!("leave_group", LeaveGroupRequest::default(), [0, 5]);
    seed!("sync_group", SyncGroupRequest::default(), [0, 5]);
    seed!("describe_groups", DescribeGroupsRequest::default(), [0, 5]);
    seed!("list_groups", ListGroupsRequest::default(), [0, 4]);
    seed!("sasl_handshake", SaslHandshakeRequest::default(), [1]);
    seed!(
        "sasl_authenticate",
        SaslAuthenticateRequest::default(),
        [0, 2]
    );
    seed!("create_topics", CreateTopicsRequest::default(), [2, 5]);
    seed!("delete_topics", DeleteTopicsRequest::default(), [1, 4]);
    seed!("delete_records", DeleteRecordsRequest::default(), [0, 2]);
    seed!("init_producer_id", InitProducerIdRequest::default(), [0, 4]);
    seed!(
        "add_partitions_to_txn",
        AddPartitionsToTxnRequest::default(),
        [0, 3]
    );
    seed!(
        "add_offsets_to_txn",
        AddOffsetsToTxnRequest::default(),
        [0, 3]
    );
    seed!("end_txn", EndTxnRequest::default(), [0, 3]);
    seed!(
        "txn_offset_commit",
        TxnOffsetCommitRequest::default(),
        [0, 3]
    );
    seed!(
        "describe_configs",
        DescribeConfigsRequest::default(),
        [0, 4]
    );
    seed!(
        "create_partitions",
        CreatePartitionsRequest::default(),
        [0, 3]
    );
    seed!("delete_groups", DeleteGroupsRequest::default(), [0, 2]);
    seed!(
        "incremental_alter_configs",
        IncrementalAlterConfigsRequest::default(),
        [0, 1]
    );

    for (name, f) in &frames {
        write(&root.join("request_decode"), name, f);
        let mut framed = Vec::with_capacity(f.len() + 5);
        framed.push(0); // chunking byte
        framed.put_i32(i32::try_from(f.len()).expect("seed frames are small"));
        framed.extend_from_slice(f);
        write(&root.join("frame_codec"), name, &framed);
    }
    for (name, c) in [
        ("none", Compression::None),
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
        ("zstd", Compression::Zstd),
    ] {
        write(&root.join("record_batch"), name, &batch(c, 4));
    }
    write(&root.join("record_batch"), "varint-shift", &shifted_varint_batch(4, 64));
    println!("wrote {} request seeds", frames.len());
}
