//! Registry-wide tests: every registered API decodes, encodes, answers errors and is
//! advertised consistently.

use super::*;
use crate::kafka::constants::*;
use crate::kafka::error::KafkaError;

/// The APIs the DB thread serves (every one but the network-thread SASL APIs).
pub(crate) fn worker_apis() -> impl Iterator<Item = &'static ApiInfo> {
    APIS.iter()
        .filter(|info| matches!(info.served_by, ServedBy::Worker(_)))
}

/// A minimal request body for `api_key` (tests only).
pub(crate) fn sample_request(api_key: i16) -> RequestBody {
    let header = RequestHeader {
        api_key,
        api_version: api_info(api_key).expect("registered").min_version,
        correlation_id: 1,
        client_id: None,
    };
    let mut buf = BytesMut::new();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    // Encode the crate's default request at the lowest supported version and decode it
    // through the registry, so the sample covers the real decode path.
    encode_default_request(api_key, header.api_version, &mut buf);
    match decode_body(&mut buf, header, tx) {
        Ok(Decoded::Request(request)) => request.body,
        Ok(Decoded::Answered) => panic!("api_key {api_key} default request was rejected"),
        Ok(Decoded::Close(reason)) => panic!("api_key {api_key}: {reason}"),
        Err((_header, _tx)) => panic!("api_key {api_key} is not registered"),
    }
}

/// Encode kafka-protocol's default request body for `api_key` at `version`.
fn encode_default_request(api_key: i16, version: i16, buf: &mut BytesMut) {
    use kafka_protocol::messages::*;
    macro_rules! enc {
        ($($t:ty),*) => {
            $(
                if api_key == <$t as WireRequest>::KEY {
                    <$t>::default().encode(buf, version).unwrap();
                    return;
                }
            )*
            panic!("no sample for api_key {}", api_key);
        };
    }
    enc!(
        ApiVersionsRequest,
        MetadataRequest,
        ProduceRequest,
        FetchRequest,
        ListOffsetsRequest,
        OffsetCommitRequest,
        OffsetFetchRequest,
        FindCoordinatorRequest,
        JoinGroupRequest,
        HeartbeatRequest,
        LeaveGroupRequest,
        SyncGroupRequest,
        DescribeGroupsRequest,
        ListGroupsRequest,
        CreateTopicsRequest,
        DeleteTopicsRequest,
        CreatePartitionsRequest,
        DeleteGroupsRequest,
        InitProducerIdRequest,
        AddPartitionsToTxnRequest,
        AddOffsetsToTxnRequest,
        EndTxnRequest,
        TxnOffsetCommitRequest,
        DeleteRecordsRequest,
        DescribeConfigsRequest,
        IncrementalAlterConfigsRequest
    );
}

#[test]
fn registry_keys_are_unique_and_named() {
    let mut keys: Vec<i16> = APIS.iter().map(|info| info.key).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), APIS.len(), "an api key is registered twice");
    for info in APIS {
        assert!(info.min_version <= info.max_version, "{}", info.name);
        assert_eq!(api_name(info.key), info.name);
        let found = api_info(info.key).unwrap();
        assert_eq!(
            (found.min_version, found.max_version),
            (info.min_version, info.max_version)
        );
    }
    assert_eq!(api_name(7), "Unknown");
    assert!(api_info(9999).is_none());
}

#[test]
fn registry_matches_the_api_key_constants() {
    let expected = [
        (API_KEY_PRODUCE, "Produce"),
        (API_KEY_FETCH, "Fetch"),
        (API_KEY_LIST_OFFSETS, "ListOffsets"),
        (API_KEY_METADATA, "Metadata"),
        (API_KEY_OFFSET_COMMIT, "OffsetCommit"),
        (API_KEY_OFFSET_FETCH, "OffsetFetch"),
        (API_KEY_FIND_COORDINATOR, "FindCoordinator"),
        (API_KEY_JOIN_GROUP, "JoinGroup"),
        (API_KEY_HEARTBEAT, "Heartbeat"),
        (API_KEY_LEAVE_GROUP, "LeaveGroup"),
        (API_KEY_SYNC_GROUP, "SyncGroup"),
        (API_KEY_DESCRIBE_GROUPS, "DescribeGroups"),
        (API_KEY_LIST_GROUPS, "ListGroups"),
        (API_KEY_API_VERSIONS, "ApiVersions"),
        (API_KEY_CREATE_TOPICS, "CreateTopics"),
        (API_KEY_DELETE_TOPICS, "DeleteTopics"),
        (API_KEY_DELETE_RECORDS, "DeleteRecords"),
        (API_KEY_INIT_PRODUCER_ID, "InitProducerId"),
        (API_KEY_ADD_PARTITIONS_TO_TXN, "AddPartitionsToTxn"),
        (API_KEY_ADD_OFFSETS_TO_TXN, "AddOffsetsToTxn"),
        (API_KEY_END_TXN, "EndTxn"),
        (API_KEY_TXN_OFFSET_COMMIT, "TxnOffsetCommit"),
        (API_KEY_DESCRIBE_CONFIGS, "DescribeConfigs"),
        (API_KEY_CREATE_PARTITIONS, "CreatePartitions"),
        (API_KEY_DELETE_GROUPS, "DeleteGroups"),
        (API_KEY_INCREMENTAL_ALTER_CONFIGS, "IncrementalAlterConfigs"),
        (API_KEY_SASL_HANDSHAKE, "SaslHandshake"),
        (API_KEY_SASL_AUTHENTICATE, "SaslAuthenticate"),
    ];
    assert_eq!(APIS.len(), expected.len());
    for (key, name) in expected {
        assert_eq!(api_name(key), name, "api key {key}");
    }
}

#[test]
fn every_advertised_version_encodes() {
    // Each advertised (api_key, version) must have a response the encoder accepts;
    // otherwise a client that negotiates that version loses its connection on the
    // first reply.
    for info in APIS {
        for version in info.min_version..=info.max_version {
            let body = error_response_for(info.key, version, ERROR_NONE)
                .unwrap_or_else(|| panic!("no error response for {}", info.name));
            assert_eq!(body.api_key(), Some(info.key));
            crate::kafka::protocol::encode_response(KafkaResponse::new(1, version, body))
                .unwrap_or_else(|e| panic!("{} v{} does not encode: {}", info.name, version, e));
        }
    }
}

#[test]
fn every_registered_api_decodes_its_minimum_version() {
    for info in worker_apis() {
        let body = sample_request(info.key);
        assert_eq!(body.api_key(), info.key, "{}", info.name);
        assert_eq!(body.api_name(), info.name);
    }
}

#[test]
fn echoed_error_responses_encode_and_carry_the_code() {
    // The error response built from a decoded request's echo must encode at every
    // advertised version and report an error.
    for info in worker_apis() {
        let echo = sample_request(info.key).echo();
        for version in info.min_version..=info.max_version {
            let body = echo.error_response(version, ERROR_UNKNOWN_SERVER_ERROR);
            assert_eq!(body.api_key(), Some(info.key));
            crate::kafka::protocol::encode_response(KafkaResponse::new(1, version, body))
                .unwrap_or_else(|e| panic!("{} v{} does not encode: {}", info.name, version, e));
        }
    }
}

#[test]
fn unknown_api_frame_has_correlation_id_and_code() {
    let bytes = crate::kafka::protocol::encode_response(KafkaResponse::new(
        77,
        0,
        ResponseBody::UnknownApi {
            error_code: ERROR_UNSUPPORTED_VERSION,
        },
    ))
    .unwrap();
    assert_eq!(&bytes[..4], &77i32.to_be_bytes());
    assert_eq!(&bytes[4..6], &ERROR_UNSUPPORTED_VERSION.to_be_bytes());
    assert!(ResponseBody::UnknownApi { error_code: 1 }.has_error());
    assert_eq!(ResponseBody::UnknownApi { error_code: 1 }.api_key(), None);
}

/// Response header v1 (flexible, one extra tagged-field byte) from an API's first flexible
/// version, v0 before it, and always v0 for ApiVersions (KIP-511).
#[test]
fn response_header_versions_follow_the_protocol() {
    fn header_len<R: Encodable>(body: ResponseBody, inner: &R, version: i16) -> usize {
        let framed = crate::kafka::protocol::encode_response(KafkaResponse::new(1, version, body))
            .unwrap()
            .len();
        let mut body_only = BytesMut::new();
        inner.encode(&mut body_only, version).unwrap();
        framed - body_only.len()
    }
    let hb = kafka_protocol::messages::heartbeat_response::HeartbeatResponse::default();
    assert_eq!(header_len(ResponseBody::Heartbeat(hb.clone()), &hb, 3), 4);
    assert_eq!(header_len(ResponseBody::Heartbeat(hb.clone()), &hb, 4), 5);
    let av = crate::kafka::response_builders::build_api_versions_response();
    assert_eq!(header_len(ResponseBody::ApiVersions(av.clone()), &av, 3), 4);
}

// ========== Error path: every failure after a valid header gets a typed response ==========

/// A request frame (header + body) for `R` at `version`, header version from the crate.
fn frame<R: WireRequest + Default>(request: &R, version: i16, correlation_id: i32) -> BytesMut {
    let header = kafka_protocol::messages::RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(kafka_protocol::protocol::StrBytes::from_static_str(
            "t",
        )));
    let mut buf = BytesMut::new();
    header.encode(&mut buf, R::header_version(version)).unwrap();
    request.encode(&mut buf, version).unwrap();
    buf
}

fn parse(
    frame: BytesMut,
) -> (
    crate::kafka::error::Result<Option<KafkaRequest>>,
    Option<KafkaResponse>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let parsed = crate::kafka::protocol::parse_request(frame, tx);
    (parsed, rx.try_recv().ok())
}

#[test]
fn truncated_body_gets_invalid_request_with_correlation_id() {
    use kafka_protocol::messages::HeartbeatRequest;
    let mut request = HeartbeatRequest::default();
    request.group_id = kafka_protocol::messages::GroupId("group".into());
    request.member_id = "member".into();
    let full = frame(&request, 3, 4242);
    // Cut the body in half: the header still decodes, the body decoder runs out of input
    // (kafka-protocol panics on that; the decode is guarded).
    let truncated = BytesMut::from(&full[..full.len() - 6]);
    let (parsed, response) = parse(truncated);
    assert!(parsed.unwrap().is_none(), "answered on the network thread");
    let response = response.expect("an error response, not a closed connection");
    assert_eq!(response.correlation_id, 4242);
    assert_eq!(response.api_version, 3);
    match response.body {
        ResponseBody::Heartbeat(r) => assert_eq!(r.error_code, ERROR_INVALID_REQUEST),
        other => panic!("expected a Heartbeat response, got {other:?}"),
    }
}

#[test]
fn truncated_body_of_every_api_is_answered_in_its_own_schema() {
    // For each registered API: a valid header at the minimum version followed by a body
    // that is cut short (one byte of garbage) yields that API's response, never a close.
    for info in worker_apis() {
        let mut buf = BytesMut::new();
        let header = kafka_protocol::messages::RequestHeader::default()
            .with_request_api_key(info.key)
            .with_request_api_version(info.min_version)
            .with_correlation_id(99)
            .with_client_id(None);
        let header_version = kafka_protocol::messages::ApiKey::try_from(info.key)
            .unwrap()
            .request_header_version(info.min_version);
        header.encode(&mut buf, header_version).unwrap();
        buf.extend_from_slice(&[0x7f]);
        let (parsed, response) = parse(buf);
        if parsed.as_ref().unwrap().is_some() {
            // Only an API whose minimum-version body is empty (ApiVersions v0,
            // ListGroups v0) can ignore the trailing byte.
            let mut body = BytesMut::new();
            encode_default_request(info.key, info.min_version, &mut body);
            assert!(body.is_empty(), "{} accepted a garbage body", info.name);
            continue;
        }
        let response = response.unwrap_or_else(|| panic!("{} got no response", info.name));
        assert_eq!(response.correlation_id, 99, "{}", info.name);
        assert_eq!(response.body.api_key(), Some(info.key), "{}", info.name);
        crate::kafka::protocol::encode_response(response)
            .unwrap_or_else(|e| panic!("{} error response does not encode: {}", info.name, e));
    }
}

#[test]
fn unsupported_version_echoes_the_decoded_request() {
    use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
    let mut request = MetadataRequest::default();
    let mut topic = MetadataRequestTopic::default();
    topic.name = Some(kafka_protocol::messages::TopicName("orders".into()));
    request.topics = Some(vec![topic]);
    let (parsed, response) = parse(frame(&request, 11, 7));
    assert!(parsed.unwrap().is_none());
    let response = response.expect("UNSUPPORTED_VERSION response");
    assert_eq!((response.correlation_id, response.api_version), (7, 11));
    match response.body {
        ResponseBody::Metadata(r) => {
            assert_eq!(r.topics.len(), 1, "the requested topic is echoed");
            assert_eq!(r.topics[0].name.as_ref().unwrap().as_str(), "orders");
            assert_eq!(r.topics[0].error_code, ERROR_UNSUPPORTED_VERSION);
        }
        other => panic!("expected Metadata, got {other:?}"),
    }
}

#[test]
fn unsupported_produce_version_with_acks_0_closes_the_connection() {
    // A client that expects no response cannot be told UNSUPPORTED_VERSION, and dropping
    // the request silently would lose its records unnoticed. Kafka closes the connection.
    use kafka_protocol::messages::ProduceRequest;
    for acks in [0, 1] {
        let mut request = ProduceRequest::default();
        request.acks = acks;
        let (parsed, response) = parse(frame(&request, 10, 9));
        if acks == 0 {
            assert!(parsed.is_err(), "acks=0 at v10 must close the connection");
            assert!(response.is_none(), "acks=0 must not get a response");
        } else {
            assert!(parsed.unwrap().is_none());
            let response = response.expect("UNSUPPORTED_VERSION response");
            assert_eq!((response.correlation_id, response.api_version), (9, 10));
        }
    }
}

#[test]
fn unsupported_fetch_version_reports_top_level_and_partition_codes() {
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
    let mut request = FetchRequest::default();
    let mut topic = FetchTopic::default();
    topic.topic = kafka_protocol::messages::TopicName("t".into());
    let mut partition = FetchPartition::default();
    partition.partition = 3;
    topic.partitions = vec![partition];
    request.topics = vec![topic];
    let (parsed, response) = parse(frame(&request, 12, 8));
    assert!(parsed.unwrap().is_none());
    match response.expect("a response").body {
        ResponseBody::Fetch(r) => {
            assert_eq!(r.error_code, ERROR_UNSUPPORTED_VERSION);
            assert_eq!(r.responses[0].partitions[0].partition_index, 3);
            assert_eq!(
                r.responses[0].partitions[0].error_code,
                ERROR_UNSUPPORTED_VERSION
            );
        }
        other => panic!("expected Fetch, got {other:?}"),
    }
}

#[test]
fn version_without_a_response_format_closes_the_connection() {
    // Fetch v3: kafka-protocol can neither decode the request nor encode a response.
    let mut buf = BytesMut::new();
    let header = kafka_protocol::messages::RequestHeader::default()
        .with_request_api_key(API_KEY_FETCH)
        .with_request_api_version(3)
        .with_correlation_id(5);
    header.encode(&mut buf, 1).unwrap();
    let (parsed, response) = parse(buf);
    assert!(parsed.is_err(), "no response format: close");
    assert!(response.is_none());
}

#[test]
fn handler_error_echoes_requested_partitions() {
    // A handler Err for an API without a top-level error code reports the code on every
    // requested partition; an empty response would read as success to the client.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let header = RequestHeader {
        api_key: API_KEY_LIST_OFFSETS,
        api_version: 5,
        correlation_id: 31,
        client_id: None,
    };
    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![crate::kafka::messages::ListOffsetsTopicData {
            name: "t".to_string(),
            partitions: vec![crate::kafka::messages::ListOffsetsPartitionData {
                partition_index: 2,
                current_leader_epoch: -1,
                timestamp: -1,
            }],
        }],
    };
    let outcome = Call::<ListOffsetsApi>::new(header, tx, ResponseBody::ListOffsets)
        .run(request, |_, _| Err(KafkaError::Internal("boom".into())));
    assert_eq!(outcome, RequestOutcome::Rollback);
    let response = rx.try_recv().unwrap();
    assert_eq!(response.correlation_id, 31);
    match response.body {
        ResponseBody::ListOffsets(r) => {
            let p = &r.topics[0].partitions[0];
            assert_eq!(p.partition_index, 2);
            assert_eq!(p.error_code, ERROR_UNKNOWN_SERVER_ERROR);
            assert_eq!(p.offset, -1);
        }
        other => panic!("expected ListOffsets, got {other:?}"),
    }
}

#[test]
fn handler_panic_echoes_requested_partitions_and_resumes() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let header = RequestHeader {
        api_key: API_KEY_DELETE_RECORDS,
        api_version: 1,
        correlation_id: 32,
        client_id: None,
    };
    let request = DeleteRecordsRequestData {
        topics: vec![("t".to_string(), vec![(0, 5), (1, 6)])],
        timeout_ms: 1000,
    };
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = Call::<DeleteRecordsApi>::new(header, tx, ResponseBody::DeleteRecords)
            .run(request, |_, _| panic!("handler boom"));
    }));
    assert!(
        unwound.is_err(),
        "the panic is re-raised for the subtransaction"
    );
    match rx.try_recv().expect("answered before resuming").body {
        ResponseBody::DeleteRecords(r) => {
            let codes: Vec<i16> = r.topics[0]
                .partitions
                .iter()
                .map(|p| p.error_code)
                .collect();
            assert_eq!(codes, vec![ERROR_UNKNOWN_SERVER_ERROR; 2]);
        }
        other => panic!("expected DeleteRecords, got {other:?}"),
    }
}

#[test]
fn commit_failure_echo_reports_every_produce_partition() {
    let body = RequestBody::Produce(ProduceRequestData {
        acks: 1,
        timeout_ms: 1000,
        topic_data: vec![crate::kafka::messages::TopicProduceData {
            name: "t".to_string(),
            partitions: vec![crate::kafka::messages::PartitionProduceData {
                partition_index: 4,
                records: vec![],
                producer_metadata: None,
            }],
        }],
        transactional_id: None,
        decoded: crate::kafka::protocol::DecodedCharge::default(),
    });
    match body.echo().error_response(9, ERROR_UNKNOWN_SERVER_ERROR) {
        ResponseBody::Produce(r) => {
            let p = &r.responses[0].partition_responses[0];
            assert_eq!((p.index, p.error_code), (4, ERROR_UNKNOWN_SERVER_ERROR));
        }
        other => panic!("expected Produce, got {other:?}"),
    }
}

#[test]
fn error_responses_without_echo_still_carry_top_level_codes() {
    for (key, code_of) in [
        (
            API_KEY_HEARTBEAT,
            (|b: &ResponseBody| match b {
                ResponseBody::Heartbeat(r) => r.error_code,
                _ => unreachable!(),
            }) as fn(&ResponseBody) -> i16,
        ),
        (API_KEY_FIND_COORDINATOR, |b| match b {
            ResponseBody::FindCoordinator(r) => r.error_code,
            _ => unreachable!(),
        }),
        (API_KEY_INIT_PRODUCER_ID, |b| match b {
            ResponseBody::InitProducerId(r) => r.error_code,
            _ => unreachable!(),
        }),
        (API_KEY_END_TXN, |b| match b {
            ResponseBody::EndTxn(r) => r.error_code,
            _ => unreachable!(),
        }),
        (API_KEY_LIST_GROUPS, |b| match b {
            ResponseBody::ListGroups(r) => r.error_code,
            _ => unreachable!(),
        }),
    ] {
        let body = error_response_for(key, 0, ERROR_INVALID_REQUEST).unwrap();
        assert_eq!(code_of(&body), ERROR_INVALID_REQUEST, "api key {key}");
        assert!(body.has_error());
    }
}

#[test]
fn network_thread_apis_are_advertised_but_never_reach_the_worker() {
    // SaslHandshake and SaslAuthenticate are answered by the connection's SASL session
    // (`listener::sasl`); the registry still advertises and encodes them, but a frame that
    // reaches the worker decoder is not turned into a request.
    for (key, versions) in [
        (API_KEY_SASL_HANDSHAKE, (0, 1)),
        (API_KEY_SASL_AUTHENTICATE, (0, 2)),
    ] {
        let info = api_info(key).expect("registered");
        assert_eq!(info.served_by, ServedBy::NetworkThread);
        assert_eq!((info.min_version, info.max_version), versions);
        let advertised = crate::kafka::response_builders::build_api_versions_response();
        assert!(advertised
            .api_keys
            .iter()
            .any(|a| a.api_key == key && (a.min_version, a.max_version) == versions));
        let mut buf = BytesMut::new();
        kafka_protocol::messages::RequestHeader::default()
            .with_request_api_key(key)
            .with_request_api_version(1)
            .with_correlation_id(3)
            .encode(&mut buf, 1)
            .unwrap();
        let (parsed, response) = parse(buf);
        assert!(
            parsed.is_err(),
            "api key {key} must not be decoded for the worker"
        );
        assert!(response.is_none());
        let body = error_response_for(key, 1, ERROR_ILLEGAL_SASL_STATE).unwrap();
        assert!(body.has_error());
    }
    assert!(worker_apis().all(|info| info.key != API_KEY_SASL_HANDSHAKE));
}
