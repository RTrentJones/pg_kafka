//! Tests of [`parse_request`]: per-API body decoding, header fields, version enforcement,
//! and that malformed input degrades to an error instead of a panic or an abort.

use bytes::BufMut;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Encodable, Request, StrBytes};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::*;
use crate::kafka::api::RequestBody;
use crate::kafka::constants::{
    API_KEY_DELETE_RECORDS, API_KEY_DESCRIBE_CONFIGS, API_KEY_FETCH,
    API_KEY_INCREMENTAL_ALTER_CONFIGS, ERROR_CORRUPT_MESSAGE,
};

/// [`super::parse_request`] as the listener runs it: a Produce whose records wait for room
/// to decode is finished ([`finish_request`]) before it is returned.
fn parse_request(
    frame: BytesMut,
    response_tx: UnboundedSender<KafkaResponse>,
) -> Result<Option<KafkaRequest>> {
    match super::parse_request(frame, response_tx) {
        Ok(Some(request)) => tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("a test runtime")
            .block_on(finish_request(request)),
        other => other,
    }
}

/// Correlation id [`decode`] sends and checks.
const CORRELATION_ID: i32 = 42;

fn channel() -> (
    UnboundedSender<KafkaResponse>,
    UnboundedReceiver<KafkaResponse>,
) {
    unbounded_channel()
}

/// A request header (client id "test-client") claiming `api_key`/`api_version`.
fn header_frame(
    api_key: i16,
    api_version: i16,
    header_version: i16,
    correlation_id: i32,
) -> BytesMut {
    let header = RequestHeader::default()
        .with_request_api_key(api_key)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("test-client")));
    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version).unwrap();
    buf
}

/// A frame as the listener hands it to [`parse_request`] (no length prefix): a header
/// claiming `api_key`/`api_version`, then `body` encoded at `body_version`.
fn raw_frame<R: Encodable>(
    api_key: i16,
    api_version: i16,
    header_version: i16,
    correlation_id: i32,
    body: &R,
    body_version: i16,
) -> BytesMut {
    let mut buf = header_frame(api_key, api_version, header_version, correlation_id);
    body.encode(&mut buf, body_version).unwrap();
    buf
}

/// `request` encoded at `version` behind the header version that version uses.
fn frame<R: Request>(request: &R, version: i16, correlation_id: i32) -> BytesMut {
    raw_frame(
        R::KEY,
        version,
        R::header_version(version),
        correlation_id,
        request,
        version,
    )
}

/// Parse `frame`, which must decode into a request for the DB thread.
fn parse_to_request(frame: BytesMut) -> KafkaRequest {
    let (tx, mut rx) = channel();
    match parse_request(frame, tx) {
        Ok(Some(request)) => request,
        other => panic!(
            "expected a request, got {other:?} (response queued: {:?})",
            rx.try_recv()
        ),
    }
}

/// Decode `request` at `version` and return its body, checking the header round-trips.
fn decode<R: Request>(request: &R, version: i16) -> RequestBody {
    let parsed = parse_to_request(frame(request, version, CORRELATION_ID));
    assert_eq!(parsed.header.api_key, R::KEY);
    assert_eq!(parsed.header.api_version, version);
    assert_eq!(parsed.header.correlation_id, CORRELATION_ID);
    assert_eq!(parsed.header.client_id.as_deref(), Some("test-client"));
    parsed.body
}

/// The data of a [`RequestBody`] that must be the given variant.
macro_rules! expect_body {
    ($body:expr, $variant:ident) => {
        match $body {
            RequestBody::$variant(data) => data,
            other => panic!("expected a {} request, got {other:?}", stringify!($variant)),
        }
    };
}

fn str(s: &'static str) -> StrBytes {
    StrBytes::from_static_str(s)
}

fn topic(s: &'static str) -> TopicName {
    TopicName::from(str(s))
}

fn group(s: &'static str) -> GroupId {
    GroupId::from(str(s))
}

fn txn_id(s: &'static str) -> TransactionalId {
    TransactionalId::from(str(s))
}

// ========== Malformed input ==========

fn corrupt_produce_frame(acks: i16) -> BytesMut {
    use kafka_protocol::messages::produce_request::{
        PartitionProduceData, ProduceRequest, TopicProduceData,
    };
    let mut p = PartitionProduceData::default();
    p.index = 0;
    // A record batch whose CRC does not match.
    let mut batch = one_record_batch(
        b"value".to_vec(),
        kafka_protocol::records::Compression::None,
    );
    let last = batch.len() - 1;
    batch[last] ^= 1;
    p.records = Some(bytes::Bytes::from(batch));
    let mut t = TopicProduceData::default();
    t.name = topic("t");
    t.partition_data = vec![p];
    let mut req = ProduceRequest::default();
    req.acks = acks;
    req.timeout_ms = 30_000;
    req.topic_data = vec![t];
    frame(&req, 7, 77)
}

/// A Produce whose partitions each carry `per_partition` bytes of zeros as one
/// gzip-compressed record: a few hundred KB on the wire however large it decodes.
/// One record batch holding one record of `value`.
fn one_record_batch(value: Vec<u8>, compression: kafka_protocol::records::Compression) -> BytesMut {
    use kafka_protocol::records::{Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
    let record = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 0,
        key: None,
        value: Some(bytes::Bytes::from(value)),
        headers: kafka_protocol::indexmap::IndexMap::default(),
    };
    let mut batch = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut batch,
        [record].iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .unwrap();
    batch
}

fn gzip_bomb_frame(partitions: i32, per_partition: usize, acks: i16) -> BytesMut {
    use kafka_protocol::messages::produce_request::{
        PartitionProduceData, ProduceRequest, TopicProduceData,
    };
    let batch = one_record_batch(
        vec![0u8; per_partition],
        kafka_protocol::records::Compression::Gzip,
    );
    let batch = batch.freeze();
    let mut t = TopicProduceData::default();
    t.name = topic("t");
    for index in 0..partitions {
        let mut p = PartitionProduceData::default();
        p.index = index;
        p.records = Some(batch.clone());
        t.partition_data.push(p);
    }
    let mut req = ProduceRequest::default();
    req.acks = acks;
    req.timeout_ms = 30_000;
    req.topic_data = vec![t];
    frame(&req, 7, 78)
}

/// Two partitions of 60 MB of gzip-compressed zeros: each batch alone is legal, but the
/// request decodes past the per-request limit. It is refused with MESSAGE_TOO_LARGE on
/// every partition (and closes an acks=0 connection) instead of being held decoded.
#[test]
fn test_produce_decoding_past_the_request_limit_is_refused() {
    let frame = gzip_bomb_frame(2, 60_000_000, 1);
    assert!(frame.len() < 1_000_000, "the bomb is small on the wire");
    let (tx, mut rx) = channel();
    let parsed = parse_request(frame, tx).unwrap();
    assert!(parsed.is_none(), "the request must not reach the DB thread");
    match rx.try_recv() {
        Ok(KafkaResponse {
            body: ResponseBody::Produce(response),
            ..
        }) => {
            let codes: Vec<i16> = response.responses[0]
                .partition_responses
                .iter()
                .map(|p| p.error_code)
                .collect();
            assert_eq!(codes, vec![10, 10], "MESSAGE_TOO_LARGE per partition");
        }
        other => panic!("expected a per-partition Produce error, got {other:?}"),
    }

    let (tx, mut rx) = channel();
    assert!(parse_request(gzip_bomb_frame(2, 60_000_000, 0), tx).is_err());
    assert!(rx.try_recv().is_err(), "acks=0 must not get a response");
}

/// A Produce frame of `partitions` partitions, each carrying `batch`.
fn produce_frame_of(batch: &bytes::Bytes, partitions: i32, acks: i16) -> BytesMut {
    use kafka_protocol::messages::produce_request::{
        PartitionProduceData, ProduceRequest, TopicProduceData,
    };
    let mut t = TopicProduceData::default();
    t.name = topic("t");
    for index in 0..partitions {
        let mut p = PartitionProduceData::default();
        p.index = index;
        p.records = Some(batch.clone());
        t.partition_data.push(p);
    }
    let mut req = ProduceRequest::default();
    req.acks = acks;
    req.timeout_ms = 30_000;
    req.topic_data = vec![t];
    frame(&req, 7, 79)
}

/// Produce error codes a frame is answered with before it reaches the DB thread.
fn refused_codes(frame: BytesMut) -> Vec<i16> {
    let (tx, mut rx) = channel();
    assert!(
        parse_request(frame, tx).unwrap().is_none(),
        "the request must not reach the DB thread"
    );
    match rx.try_recv() {
        Ok(KafkaResponse {
            body: ResponseBody::Produce(response),
            ..
        }) => response.responses[0]
            .partition_responses
            .iter()
            .map(|p| p.error_code)
            .collect(),
        other => panic!("expected a per-partition Produce error, got {other:?}"),
    }
}

/// Batches of 7-byte records: 2.4 million of them in two 100 KB gzip batches would take
/// some 600 MB decoded into records. Their declared counts are charged first, so the
/// request is refused with MESSAGE_TOO_LARGE before any record is built, and quickly.
#[test]
fn test_produce_of_tiny_records_past_the_request_limit_is_refused() {
    use crate::kafka::protocol::recordbatch::test_batches::{raw_batch, tiny_records};
    let n = 1_200_000;
    let batch = raw_batch(&tiny_records(n), i32::try_from(n).unwrap(), true);
    let frame = produce_frame_of(&batch, 2, 1);
    assert!(frame.len() < 200_000, "{} bytes on the wire", frame.len());
    let started = std::time::Instant::now();
    assert_eq!(
        refused_codes(frame),
        vec![10, 10],
        "MESSAGE_TOO_LARGE per partition"
    );
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
}

/// A compressed request that decodes to more than the inline limit but within the request
/// limit is decoded (off the I/O thread) and keeps its share of the decode budget until it
/// is dropped.
#[test]
fn test_large_compressed_produce_within_the_limit_decodes() {
    use crate::kafka::protocol::decode_budget::{DECODED, DECODED_BUDGET_BYTES};
    let request = parse_to_request(gzip_bomb_frame(2, 5_000_000, 1));
    let RequestBody::Produce(produce) = &request.body else {
        panic!("expected a Produce request");
    };
    let held = produce.decoded.bytes();
    assert!(held >= 10_000_000, "charged {held}");
    assert!(DECODED.remaining() <= DECODED_BUDGET_BYTES - held);
    let values: usize = produce
        .topic_data
        .iter()
        .flat_map(|t| t.partitions.iter())
        .flat_map(|p| p.records.iter())
        .map(|r| r.value.as_ref().map_or(0, Vec::len))
        .sum();
    assert_eq!(values, 10_000_000);
}

/// Every response the decoder queues must encode; when none can be shaped the connection
/// is closed instead. The seed is FindCoordinator v4 (pg_kafka serves v0-3): an
/// UNSUPPORTED_VERSION reply with a top-level error code cannot be encoded at v4, which
/// has none.
#[test]
fn test_unsupported_version_reply_always_encodes() {
    let crash: &[u8] = &[
        0x00, 0x0a, 0x00, 0x04, 0x00, 0x00, 0x00, 0x07, 0x00, 0x04, 0x66, 0x75, 0x7a, 0x7a, 0x00,
        0x00, 0x01, 0x00,
    ];
    let (tx, mut rx) = channel();
    let _ = parse_request(BytesMut::from(crash), tx);
    while let Ok(response) = rx.try_recv() {
        crate::kafka::protocol::encode_response(response).expect("queued response encodes");
    }
    // Every registered API at every version just past what it serves.
    for info in api::APIS {
        for version in [info.max_version + 1, info.max_version + 2, 15, 16] {
            let mut frame = BytesMut::new();
            frame.put_i16(info.key);
            frame.put_i16(version);
            frame.put_i32(9);
            frame.put_i16(-1);
            frame.extend_from_slice(&[0u8; 16]);
            let (tx, mut rx) = channel();
            let _ = parse_request(frame, tx);
            while let Ok(response) = rx.try_recv() {
                assert!(
                    crate::kafka::protocol::encode_response(response).is_ok(),
                    "{} v{version}: queued an unencodable response",
                    info.name
                );
            }
        }
    }
}

/// acks=0 never gets a reply; an unsolicited error frame would be read as the reply to the
/// client's next request. As in Kafka, the connection is closed instead (`parse_request`
/// errs, and the listener closes).
#[test]
fn test_corrupt_produce_acks0_gets_no_response() {
    let (tx, mut rx) = channel();
    assert!(parse_request(corrupt_produce_frame(0), tx).is_err());
    assert!(rx.try_recv().is_err(), "acks=0 must not get a response");
}

#[test]
fn test_corrupt_produce_reports_error_per_partition() {
    let (tx, mut rx) = channel();
    assert!(parse_request(corrupt_produce_frame(1), tx)
        .unwrap()
        .is_none());
    match rx.try_recv() {
        Ok(KafkaResponse {
            body: ResponseBody::Produce(response),
            ..
        }) => {
            let p = &response.responses[0].partition_responses[0];
            assert_eq!(p.index, 0);
            assert_eq!(p.error_code, ERROR_CORRUPT_MESSAGE);
        }
        other => panic!("expected a per-partition Produce error, got {other:?}"),
    }
}

/// A Fetch declaring a ~2.1-billion-entry topics array must not make the decoder
/// pre-allocate gigabytes. The patched kafka-protocol decoder (capped array
/// pre-allocation, see Cargo.toml `[patch.crates-io]`) turns it into an error; without the
/// patch this test aborts the test binary, an allocation failure `catch_unwind` cannot
/// contain.
#[test]
fn test_fetch_huge_topics_array_does_not_oom_abort() {
    let (tx, _rx) = channel();
    let mut buf = BytesMut::new();
    // Request header v1 (non-flexible): api_key, api_version, correlation_id, null client_id.
    buf.put_i16(API_KEY_FETCH);
    buf.put_i16(0);
    buf.put_i32(123);
    buf.put_i16(-1);
    // FetchRequest v0 body: replica_id, max_wait_ms, min_bytes, then the topics array count.
    buf.put_i32(-1);
    buf.put_i32(0);
    buf.put_i32(0);
    buf.put_i32(i32::MAX); // topics array length, with no entries following
    let _ = parse_request(buf, tx); // must return (Ok or Err) without aborting
}

// Full-pipeline fuzzing: arbitrary bytes, and header-seeded bodies that reach the per-API
// decoders, must return Ok or Err, never panic or abort. This is what surfaced the huge
// array pre-allocation above.
proptest::proptest! {
    #[test]
    fn fuzz_parse_request_never_panics(
        data in proptest::collection::vec(proptest::num::u8::ANY, 0..1024)
    ) {
        let (tx, _rx) = channel();
        let _ = parse_request(BytesMut::from(&data[..]), tx);
    }

    #[test]
    fn fuzz_parse_request_with_header_never_panics(
        api_key in 0i16..75,
        api_version in 0i16..16,
        correlation_id in proptest::num::i32::ANY,
        body in proptest::collection::vec(proptest::num::u8::ANY, 0..512),
    ) {
        let (tx, _rx) = channel();
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(api_version);
        buf.put_i32(correlation_id);
        buf.put_i16(-1); // null client_id
        buf.extend_from_slice(&body);
        let _ = parse_request(buf, tx);
    }
}

/// An empty frame is a graceful error. `parse_request` is called outside any
/// `catch_unwind` here, so a panic escaping it fails the test by unwinding.
#[test]
fn test_parse_empty_header_returns_err_no_panic() {
    let (tx, _rx) = channel();
    let result = parse_request(BytesMut::new(), tx);
    assert!(
        result.is_err(),
        "empty frame should be a graceful Err, got Ok({:?})",
        result.as_ref().map(Option::is_some)
    );
}

/// A header cut off after the api key is a graceful error.
#[test]
fn test_parse_truncated_header_returns_err_no_panic() {
    let (tx, _rx) = channel();
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    let result = parse_request(buf, tx);
    assert!(
        result.is_err(),
        "truncated header should be a graceful Err, got Ok({:?})",
        result.as_ref().map(Option::is_some)
    );
}

/// The config/log-management APIs answer a body that does not decode with a typed error
/// response instead of panicking or leaving the client waiting.
#[test]
fn test_parse_config_log_apis_malformed_body() {
    for (api_key, api_version, header_version) in [
        (API_KEY_DELETE_RECORDS, 1i16, 1i16),
        (API_KEY_DESCRIBE_CONFIGS, 1, 1),
        (API_KEY_INCREMENTAL_ALTER_CONFIGS, 0, 1),
    ] {
        let (tx, mut rx) = channel();
        // A header, then one stray byte that is not a valid body for any of these APIs.
        let mut frame = header_frame(api_key, api_version, header_version, 555);
        frame.put_u8(0xFF);
        let parsed = parse_request(frame, tx).unwrap();
        assert!(
            parsed.is_none(),
            "malformed api_key={api_key} must parse to None"
        );
        assert!(
            rx.try_recv().is_ok(),
            "malformed api_key={api_key} must queue an error response"
        );
    }
}

// ========== Header fields ==========

#[test]
fn test_correlation_id_preserved() {
    for correlation_id in [0i32, 1, -1, i32::MAX, i32::MIN] {
        let request = ApiVersionsRequest::default();
        let parsed = parse_to_request(frame(&request, 3, correlation_id));
        assert_eq!(
            parsed.header.correlation_id, correlation_id,
            "correlation id not preserved"
        );
        expect_body!(parsed.body, ApiVersions);
    }
}

fn api_versions_frame(client_id: Option<&'static str>) -> BytesMut {
    let header = RequestHeader::default()
        .with_request_api_key(ApiVersionsRequest::KEY)
        .with_request_api_version(3)
        .with_correlation_id(1)
        .with_client_id(client_id.map(str));
    let mut buf = BytesMut::new();
    header.encode(&mut buf, 2).unwrap();
    ApiVersionsRequest::default().encode(&mut buf, 3).unwrap();
    buf
}

#[test]
fn test_client_id_extracted() {
    let parsed = parse_to_request(api_versions_frame(Some("my-special-client")));
    assert_eq!(
        parsed.header.client_id.as_deref(),
        Some("my-special-client")
    );
    expect_body!(parsed.body, ApiVersions);
}

#[test]
fn test_client_id_none() {
    let parsed = parse_to_request(api_versions_frame(None));
    assert!(parsed.header.client_id.is_none());
    expect_body!(parsed.body, ApiVersions);
}

// ========== Version enforcement and unknown APIs ==========

/// An API key the crate knows but pg_kafka does not serve (29 = DescribeAcls) is answered
/// with the generic error frame.
#[test]
fn test_parse_unsupported_api_key() {
    let (tx, mut rx) = channel();
    let request = DescribeAclsRequest::default();
    assert!(parse_request(frame(&request, 1, 9999), tx)
        .unwrap()
        .is_none());
    match rx.try_recv() {
        Ok(KafkaResponse {
            body: ResponseBody::UnknownApi { error_code },
            ..
        }) => assert_eq!(error_code, ERROR_UNSUPPORTED_VERSION),
        other => panic!("expected the generic error frame, got {other:?}"),
    }
}

/// A version above the advertised maximum (Metadata v11; max 10) is rejected before the
/// body decode, with the API's own typed response at the requested version so a strict
/// client can parse it (Metadata has no top-level error code; errors are per topic).
#[test]
fn test_request_version_out_of_range_rejected() {
    let (tx, mut rx) = channel();
    let request = MetadataRequest::default();
    // v11's header is flexible like v10's, so the header still decodes.
    let frame = raw_frame(MetadataRequest::KEY, 11, 2, 7777, &request, 9);
    let parsed = parse_request(frame, tx).unwrap();
    assert!(
        parsed.is_none(),
        "out-of-range version should not yield a request"
    );
    match rx.try_recv() {
        Ok(KafkaResponse {
            correlation_id,
            api_version,
            body: ResponseBody::Metadata(_),
        }) => {
            assert_eq!(correlation_id, 7777);
            assert_eq!(api_version, 11);
        }
        other => panic!("expected a typed Metadata error response, got {other:?}"),
    }
}

/// Version enforcement does not over-reject: Metadata v10, which Sarama V2_8_0_0 sends
/// for its bootstrap Metadata, parses.
#[test]
fn test_request_version_in_range_accepted() {
    let request = MetadataRequest::default();
    expect_body!(decode(&request, 10), Metadata);
}

/// AddPartitionsToTxn v4 (the batched format) is above the advertised maximum (v3): it is
/// answered UNSUPPORTED_VERSION with the typed v4 response (which has a top-level error
/// code), not half-decoded and not with the generic frame a strict client cannot read.
#[test]
fn test_parse_add_partitions_to_txn_request_v4() {
    let (tx, mut rx) = channel();
    let mut request = AddPartitionsToTxnRequest::default();
    let mut txn = add_partitions_to_txn_request::AddPartitionsToTxnTransaction::default();
    txn.transactional_id = txn_id("batch-txn");
    txn.producer_id = ProducerId(44444);
    txn.producer_epoch = 1;
    let mut t = add_partitions_to_txn_request::AddPartitionsToTxnTopic::default();
    t.name = topic("batch-topic");
    t.partitions = vec![0, 1];
    txn.topics.push(t);
    request.transactions.push(txn);

    let parsed = parse_request(frame(&request, 4, 4001), tx).unwrap();
    assert!(
        parsed.is_none(),
        "AddPartitionsToTxn v4 is above the advertised max (v3) and must be rejected"
    );
    match rx.try_recv() {
        Ok(KafkaResponse {
            api_version: 4,
            body: ResponseBody::AddPartitionsToTxn(response),
            ..
        }) => assert_eq!(response.error_code, ERROR_UNSUPPORTED_VERSION),
        other => panic!("expected UNSUPPORTED_VERSION, got {other:?}"),
    }
}

// ========== ApiVersions and Metadata ==========

#[test]
fn test_parse_api_versions_request() {
    let request = ApiVersionsRequest::default();
    expect_body!(decode(&request, 3), ApiVersions);
}

/// A null topics array (v1+) means "all topics". It is set explicitly so the test does not
/// depend on the crate's default for the field.
#[test]
fn test_parse_metadata_request_all_topics() {
    let mut request = MetadataRequest::default();
    request.topics = None;
    let data = expect_body!(decode(&request, 9), Metadata);
    assert!(data.topics.is_none());
}

#[test]
fn test_parse_metadata_request_specific_topics() {
    let mut request = MetadataRequest::default();
    let mut t = metadata_request::MetadataRequestTopic::default();
    t.name = Some(topic("test-topic"));
    request.topics = Some(vec![t]);
    let data = expect_body!(decode(&request, 9), Metadata);
    assert_eq!(data.topics, Some(vec!["test-topic".to_string()]));
}

/// At v1+ an empty topics array means "no topics", not "all topics".
#[test]
fn test_parse_metadata_empty_array_v9_means_no_topics() {
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![]);
    let data = expect_body!(decode(&request, 9), Metadata);
    let topics = data.topics.expect("v9 empty array should be Some(empty)");
    assert!(
        topics.is_empty(),
        "empty array at v9 should request zero topics, got {topics:?}"
    );
}

/// v0 has no nullable array, so an empty array means "all topics" (None downstream).
#[test]
fn test_parse_metadata_empty_array_v0_means_all_topics() {
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![]);
    let data = expect_body!(decode(&request, 0), Metadata);
    assert!(
        data.topics.is_none(),
        "empty array at v0 should mean ALL topics (None), got {:?}",
        data.topics
    );
}

/// The v4+ allow_auto_topic_creation flag is carried into the request.
#[test]
fn test_parse_metadata_allow_auto_topic_creation_flag() {
    let mut request = MetadataRequest::default();
    request.allow_auto_topic_creation = false;
    let data = expect_body!(decode(&request, 4), Metadata);
    assert!(!data.allow_auto_topic_creation);
}

// ========== Consumer groups ==========

#[test]
fn test_parse_find_coordinator_request() {
    let mut request = FindCoordinatorRequest::default();
    request.key = str("test-group");
    request.key_type = 0; // consumer group
    let data = expect_body!(decode(&request, 3), FindCoordinator);
    assert_eq!(data.key, "test-group");
    assert_eq!(data.key_type, 0);
}

#[test]
fn test_parse_join_group_request() {
    let mut request = JoinGroupRequest::default();
    request.group_id = group("consumer-group-1");
    request.session_timeout_ms = 30000;
    request.rebalance_timeout_ms = 60000;
    request.member_id = str("");
    request.protocol_type = str("consumer");
    let data = expect_body!(decode(&request, 7), JoinGroup);
    assert_eq!(data.group_id, "consumer-group-1");
    assert_eq!(data.session_timeout_ms, 30000);
    assert_eq!(data.protocol_type, "consumer");
}

/// v4 is the highest SyncGroup version pg_kafka advertises.
#[test]
fn test_parse_sync_group_request() {
    let mut request = SyncGroupRequest::default();
    request.group_id = group("sync-group");
    request.generation_id = 1;
    request.member_id = str("member-123");
    let data = expect_body!(decode(&request, 4), SyncGroup);
    assert_eq!(data.group_id, "sync-group");
    assert_eq!(data.generation_id, 1);
    assert_eq!(data.member_id, "member-123");
}

#[test]
fn test_parse_heartbeat_request() {
    let mut request = HeartbeatRequest::default();
    request.group_id = group("heartbeat-group");
    request.generation_id = 5;
    request.member_id = str("hb-member");
    let data = expect_body!(decode(&request, 4), Heartbeat);
    assert_eq!(data.group_id, "heartbeat-group");
    assert_eq!(data.generation_id, 5);
    assert_eq!(data.member_id, "hb-member");
}

/// v2 carries a single member_id (v3+ a members array).
#[test]
fn test_parse_leave_group_request() {
    let mut request = LeaveGroupRequest::default();
    request.group_id = group("leave-group");
    request.member_id = str("leaving-member");
    let data = expect_body!(decode(&request, 2), LeaveGroup);
    assert_eq!(data.group_id, "leave-group");
    assert_eq!(data.member_id, "leaving-member");
}

#[test]
fn test_parse_describe_groups_request() {
    let mut request = DescribeGroupsRequest::default();
    request.groups = vec![group("describe-group")];
    let data = expect_body!(decode(&request, 3), DescribeGroups);
    assert_eq!(data.groups, vec!["describe-group".to_string()]);
}

#[test]
fn test_parse_list_groups_request() {
    let request = ListGroupsRequest::default();
    expect_body!(decode(&request, 4), ListGroups);
}

#[test]
fn test_parse_delete_groups_request() {
    let mut request = DeleteGroupsRequest::default();
    request.groups_names = vec![group("old-group")];
    let data = expect_body!(decode(&request, 2), DeleteGroups);
    assert_eq!(data.groups_names, vec!["old-group".to_string()]);
}

// ========== Offsets ==========

/// The generation and member id are carried into the request.
#[test]
fn test_parse_offset_commit_request() {
    let mut request = OffsetCommitRequest::default();
    request.group_id = group("commit-group");
    request.generation_id_or_member_epoch = 7;
    request.member_id = str("member-7");
    let mut t = offset_commit_request::OffsetCommitRequestTopic::default();
    t.name = topic("commit-topic");
    let mut partition = offset_commit_request::OffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = 100;
    t.partitions.push(partition);
    request.topics.push(t);

    let data = expect_body!(decode(&request, 8), OffsetCommit);
    assert_eq!(data.group_id, "commit-group");
    assert_eq!(data.generation_id, 7);
    assert_eq!(data.member_id, "member-7");
    assert_eq!(data.topics.len(), 1);
    assert_eq!(data.topics[0].name, "commit-topic");
    assert_eq!(data.topics[0].partitions[0].committed_offset, 100);
}

#[test]
fn test_parse_offset_commit_request_with_metadata() {
    let mut request = OffsetCommitRequest::default();
    request.group_id = group("metadata-group");
    let mut t = offset_commit_request::OffsetCommitRequestTopic::default();
    t.name = topic("metadata-topic");
    let mut partition = offset_commit_request::OffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = 999;
    partition.committed_metadata = Some(str("consumer-state-data"));
    t.partitions.push(partition);
    request.topics.push(t);

    let data = expect_body!(decode(&request, 8), OffsetCommit);
    assert_eq!(data.group_id, "metadata-group");
    assert_eq!(data.topics.len(), 1);
    assert_eq!(data.topics[0].partitions[0].committed_offset, 999);
    assert_eq!(
        data.topics[0].partitions[0].metadata.as_deref(),
        Some("consumer-state-data")
    );
}

#[test]
fn test_parse_offset_fetch_request() {
    let mut request = OffsetFetchRequest::default();
    request.group_id = group("fetch-group");
    let data = expect_body!(decode(&request, 7), OffsetFetch);
    assert_eq!(data.group_id, "fetch-group");
}

#[test]
fn test_parse_offset_fetch_request_with_topics() {
    let mut request = OffsetFetchRequest::default();
    request.group_id = group("fetch-offsets-group");
    let mut t = offset_fetch_request::OffsetFetchRequestTopic::default();
    t.name = topic("offset-topic");
    t.partition_indexes = vec![0, 1, 2];
    request.topics = Some(vec![t]);

    let data = expect_body!(decode(&request, 7), OffsetFetch);
    assert_eq!(data.group_id, "fetch-offsets-group");
    let topics = data.topics.expect("topics requested");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "offset-topic");
    assert_eq!(topics[0].partition_indexes, vec![0, 1, 2]);
}

/// An empty topics array asks for nothing (`Some(vec![])`); only a null array (v2+) means
/// "every partition of the group" (`None`).
#[test]
fn test_parse_offset_fetch_empty_vs_null_topics() {
    for (wire_topics, expect_all) in [(Some(vec![]), false), (None, true)] {
        let mut request = OffsetFetchRequest::default();
        request.group_id = group("g");
        request.topics = wire_topics;
        let data = expect_body!(decode(&request, 7), OffsetFetch);
        if expect_all {
            assert!(data.topics.is_none(), "null topics means all partitions");
        } else {
            assert_eq!(data.topics.map(|t| t.len()), Some(0));
        }
    }
}

#[test]
fn test_parse_list_offsets_request() {
    let mut request = ListOffsetsRequest::default();
    request.replica_id = BrokerId(-1);
    request.isolation_level = 0;
    let mut t = list_offsets_request::ListOffsetsTopic::default();
    t.name = topic("list-topic");
    let mut partition = list_offsets_request::ListOffsetsPartition::default();
    partition.partition_index = 0;
    partition.timestamp = -2; // earliest
    t.partitions.push(partition);
    request.topics.push(t);

    let data = expect_body!(decode(&request, 2), ListOffsets);
    assert_eq!(data.topics.len(), 1);
    assert_eq!(data.topics[0].name, "list-topic");
}

// ========== Fetch and Produce ==========

#[test]
fn test_parse_fetch_request() {
    let mut request = FetchRequest::default();
    request.max_wait_ms = 5000;
    request.min_bytes = 1;
    request.max_bytes = 1_048_576;
    request.isolation_level = 0;
    let mut t = fetch_request::FetchTopic::default();
    t.topic = topic("fetch-topic");
    let mut partition = fetch_request::FetchPartition::default();
    partition.partition = 0;
    partition.fetch_offset = 100;
    partition.partition_max_bytes = 65536;
    t.partitions.push(partition);
    request.topics.push(t);

    let data = expect_body!(decode(&request, 4), Fetch);
    assert_eq!(data.max_wait_ms, 5000);
    assert_eq!(data.min_bytes, 1);
    assert_eq!(data.max_bytes, 1_048_576);
    assert_eq!(data.topic_data.len(), 1);
    assert_eq!(data.topic_data[0].name, "fetch-topic");
    assert_eq!(data.topic_data[0].partitions[0].fetch_offset, 100);
}

#[test]
fn test_parse_fetch_request_with_isolation_level() {
    let mut request = FetchRequest::default();
    request.max_wait_ms = 1000;
    request.min_bytes = 0;
    request.max_bytes = 1_048_576;
    request.isolation_level = 1; // READ_COMMITTED
    let data = expect_body!(decode(&request, 4), Fetch);
    assert_eq!(data.isolation_level, 1);
}

#[test]
fn test_parse_fetch_request_multiple_topics() {
    fn partition(index: i32, fetch_offset: i64) -> fetch_request::FetchPartition {
        let mut p = fetch_request::FetchPartition::default();
        p.partition = index;
        p.fetch_offset = fetch_offset;
        p.partition_max_bytes = 32768;
        p
    }
    let mut request = FetchRequest::default();
    request.max_wait_ms = 1000;
    request.min_bytes = 1;
    request.max_bytes = 1_048_576;
    let mut topic_a = fetch_request::FetchTopic::default();
    topic_a.topic = topic("topic-a");
    topic_a.partitions = vec![partition(0, 0), partition(1, 100)];
    let mut topic_b = fetch_request::FetchTopic::default();
    topic_b.topic = topic("topic-b");
    topic_b.partitions = vec![partition(0, 50)];
    request.topics = vec![topic_a, topic_b];

    let data = expect_body!(decode(&request, 4), Fetch);
    let topics = &data.topic_data;
    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].name, "topic-a");
    assert_eq!(topics[0].partitions.len(), 2);
    assert_eq!(topics[0].partitions[0].fetch_offset, 0);
    assert_eq!(topics[0].partitions[1].fetch_offset, 100);
    assert_eq!(topics[1].name, "topic-b");
    assert_eq!(topics[1].partitions.len(), 1);
    assert_eq!(topics[1].partitions[0].fetch_offset, 50);
}

/// Produce with no partitions decodes `acks` and `timeout_ms`, for each acks value Kafka
/// accepts: 1 (leader), 0 (fire and forget) and -1 (all ISRs).
#[test]
fn test_parse_produce_request_acks() {
    for (acks, timeout_ms) in [(1, 5000), (0, 1000), (-1, 30000)] {
        let mut request = ProduceRequest::default();
        request.acks = acks;
        request.timeout_ms = timeout_ms;
        let data = expect_body!(decode(&request, 7), Produce);
        assert_eq!(data.acks, acks);
        assert_eq!(data.timeout_ms, timeout_ms);
    }
}

#[test]
fn test_parse_produce_request_with_transactional_id() {
    let mut request = ProduceRequest::default();
    request.transactional_id = Some(txn_id("txn-producer"));
    request.acks = -1;
    request.timeout_ms = 30000;
    let data = expect_body!(decode(&request, 7), Produce);
    assert_eq!(data.transactional_id.as_deref(), Some("txn-producer"));
    assert_eq!(data.acks, -1);
}

// ========== Admin ==========

#[test]
fn test_parse_create_topics_request() {
    let mut request = CreateTopicsRequest::default();
    let mut t = create_topics_request::CreatableTopic::default();
    t.name = topic("new-topic");
    t.num_partitions = 3;
    t.replication_factor = 1;
    request.topics.push(t);
    request.timeout_ms = 5000;
    request.validate_only = false;

    let data = expect_body!(decode(&request, 5), CreateTopics);
    assert_eq!(data.topics.len(), 1);
    assert_eq!(data.topics[0].name, "new-topic");
    assert_eq!(data.topics[0].num_partitions, 3);
    assert_eq!(data.timeout_ms, 5000);
    assert!(!data.validate_only);
}

/// v4 is the highest DeleteTopics version pg_kafka advertises.
#[test]
fn test_parse_delete_topics_request() {
    let mut request = DeleteTopicsRequest::default();
    request.topic_names = vec![topic("delete-me")];
    request.timeout_ms = 5000;
    let data = expect_body!(decode(&request, 4), DeleteTopics);
    assert_eq!(data.topic_names, vec!["delete-me".to_string()]);
    assert_eq!(data.timeout_ms, 5000);
}

/// v2 is the highest CreatePartitions version pg_kafka advertises.
#[test]
fn test_parse_create_partitions_request() {
    let mut request = CreatePartitionsRequest::default();
    let mut t = create_partitions_request::CreatePartitionsTopic::default();
    t.name = topic("expand-topic");
    t.count = 6;
    request.topics.push(t);
    request.timeout_ms = 5000;
    request.validate_only = true;

    let data = expect_body!(decode(&request, 2), CreatePartitions);
    assert_eq!(data.topics.len(), 1);
    assert_eq!(data.topics[0].name, "expand-topic");
    assert_eq!(data.topics[0].count, 6);
    assert!(data.validate_only);
}

#[test]
fn test_parse_delete_records_request() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
    };
    let mut request = DeleteRecordsRequest::default();
    request.timeout_ms = 30000;
    request.topics = vec![DeleteRecordsTopic::default()
        .with_name(topic("truncate-me"))
        .with_partitions(vec![DeleteRecordsPartition::default()
            .with_partition_index(0)
            .with_offset(42)])];

    let data = expect_body!(decode(&request, 1), DeleteRecords);
    assert_eq!(data.timeout_ms, 30000);
    assert_eq!(
        data.topics,
        vec![("truncate-me".to_string(), vec![(0, 42i64)])]
    );
}

#[test]
fn test_parse_describe_configs_request() {
    use kafka_protocol::messages::describe_configs_request::{
        DescribeConfigsRequest, DescribeConfigsResource,
    };
    let mut request = DescribeConfigsRequest::default();
    request.resources = vec![DescribeConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(str("my-topic"))
        .with_configuration_keys(Some(vec![str("retention.ms")]))];

    let data = expect_body!(decode(&request, 1), DescribeConfigs);
    assert_eq!(
        data.resources,
        vec![(
            2i8,
            "my-topic".to_string(),
            Some(vec!["retention.ms".to_string()])
        )]
    );
}

#[test]
fn test_parse_incremental_alter_configs_request() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    };
    let mut request = IncrementalAlterConfigsRequest::default();
    request.validate_only = true;
    request.resources = vec![AlterConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(str("my-topic"))
        .with_configs(vec![AlterableConfig::default()
            .with_name(str("retention.ms"))
            .with_config_operation(0)
            .with_value(Some(str("60000")))])];

    let data = expect_body!(decode(&request, 0), IncrementalAlterConfigs);
    assert!(data.validate_only);
    assert_eq!(
        data.resources,
        vec![(
            2i8,
            "my-topic".to_string(),
            vec![("retention.ms".to_string(), 0i8, Some("60000".to_string()))]
        )]
    );
}

// ========== Idempotent producers and transactions ==========

#[test]
fn test_parse_init_producer_id_request() {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = None;
    request.transaction_timeout_ms = 60000;
    request.producer_id = ProducerId(-1); // new producer
    request.producer_epoch = -1;

    let data = expect_body!(decode(&request, 4), InitProducerId);
    assert_eq!(data.transactional_id, None);
    assert_eq!(data.transaction_timeout_ms, 60000);
}

#[test]
fn test_parse_init_producer_id_with_transactional_id() {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = Some(txn_id("txn-producer-1"));
    request.transaction_timeout_ms = 30000;
    request.producer_id = ProducerId(-1);
    request.producer_epoch = -1;

    let data = expect_body!(decode(&request, 4), InitProducerId);
    assert_eq!(data.transactional_id.as_deref(), Some("txn-producer-1"));
}

/// v3 is the last version before the batched format.
#[test]
fn test_parse_add_partitions_to_txn_request_v3() {
    let mut request = AddPartitionsToTxnRequest::default();
    request.v3_and_below_transactional_id = txn_id("txn-test");
    request.v3_and_below_producer_id = ProducerId(11111);
    request.v3_and_below_producer_epoch = 0;
    let mut t = add_partitions_to_txn_request::AddPartitionsToTxnTopic::default();
    t.name = topic("txn-topic");
    t.partitions = vec![0, 1, 2];
    request.v3_and_below_topics.push(t);

    let data = expect_body!(decode(&request, 3), AddPartitionsToTxn);
    assert_eq!(data.transactional_id, "txn-test");
    assert_eq!(data.producer_id, 11111);
    assert_eq!(data.topics, vec![("txn-topic".to_string(), vec![0, 1, 2])]);
}

#[test]
fn test_parse_add_offsets_to_txn_request() {
    let mut request = AddOffsetsToTxnRequest::default();
    request.transactional_id = txn_id("txn-1");
    request.producer_id = ProducerId(12345);
    request.producer_epoch = 0;
    request.group_id = group("consumer-group");

    let data = expect_body!(decode(&request, 3), AddOffsetsToTxn);
    assert_eq!(data.transactional_id, "txn-1");
    assert_eq!(data.producer_id, 12345);
    assert_eq!(data.group_id, "consumer-group");
}

#[test]
fn test_parse_end_txn_request() {
    for (id, producer_id, epoch, committed) in [
        ("txn-commit", 99999, 1, true),
        ("txn-abort", 88888, 2, false),
    ] {
        let mut request = EndTxnRequest::default();
        request.transactional_id = txn_id(id);
        request.producer_id = ProducerId(producer_id);
        request.producer_epoch = epoch;
        request.committed = committed;

        let data = expect_body!(decode(&request, 3), EndTxn);
        assert_eq!(data.transactional_id, id);
        assert_eq!(data.committed, committed);
    }
}

#[test]
fn test_parse_txn_offset_commit_request() {
    let mut request = TxnOffsetCommitRequest::default();
    request.transactional_id = txn_id("txn-commit");
    request.group_id = group("commit-group");
    request.producer_id = ProducerId(22222);
    request.producer_epoch = 1;
    let mut t = txn_offset_commit_request::TxnOffsetCommitRequestTopic::default();
    t.name = topic("offset-topic");
    let mut partition = txn_offset_commit_request::TxnOffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = 500;
    partition.committed_metadata = Some(str("metadata"));
    t.partitions.push(partition);
    request.topics.push(t);

    let data = expect_body!(decode(&request, 3), TxnOffsetCommit);
    assert_eq!(data.transactional_id, "txn-commit");
    assert_eq!(data.group_id, "commit-group");
    assert_eq!(data.producer_id, 22222);
    assert_eq!(
        data.topics,
        vec![(
            "offset-topic".to_string(),
            vec![(0, 500, Some("metadata".to_string()))]
        )]
    );
}
