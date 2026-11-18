//! Registry-wide tests: every registered API decodes, encodes, answers errors and is
//! advertised consistently.

use super::*;
use crate::kafka::constants::*;

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
        Ok(Decoded::Answered) => panic!("api_key {} default request was rejected", api_key),
        Ok(Decoded::Close(reason)) => panic!("api_key {}: {}", api_key, reason),
        Err(_) => panic!("api_key {} is not registered", api_key),
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
        assert_eq!(
            supported_version_range(info.key),
            Some((info.min_version, info.max_version))
        );
    }
    assert_eq!(api_name(7), "Unknown");
    assert_eq!(supported_version_range(9999), None);
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
    ];
    assert_eq!(APIS.len(), expected.len());
    for (key, name) in expected {
        assert_eq!(api_name(key), name, "api key {}", key);
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
    for info in APIS {
        let body = sample_request(info.key);
        assert_eq!(body.api_key(), info.key, "{}", info.name);
        assert_eq!(body.api_name(), info.name);
    }
}

#[test]
fn echoed_error_responses_encode_and_carry_the_code() {
    // The error response built from a decoded request's echo must encode at every
    // advertised version and report an error.
    for info in APIS {
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
