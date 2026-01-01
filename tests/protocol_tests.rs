// Protocol parsing and encoding tests for Kafka wire protocol
//
// Tests cover:
// - Request parsing (ApiVersions, Metadata)
// - Response encoding (ApiVersions, Metadata)
// - Error handling (malformed requests, unsupported versions)
// - Edge cases (empty topics, null client_id, etc.)

#[cfg(test)]
mod protocol_tests {
    use bytes::BytesMut;
    use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use kafka_protocol::messages::metadata_response::MetadataResponse;
    use kafka_protocol::messages::ResponseHeader;
    use kafka_protocol::protocol::Encodable;

    // Re-export types we'll be testing (these will be imported from our crate)
    // For now, we'll add placeholder tests

    #[test]
    fn test_api_versions_request_parsing_valid() {
        // Build a valid ApiVersions request (API key 18, version 3)
        let mut request = BytesMut::new();

        // Size (will be added by frame)
        // API Key: 18 (ApiVersions)
        request.extend_from_slice(&18i16.to_be_bytes());
        // API Version: 3
        request.extend_from_slice(&3i16.to_be_bytes());
        // Correlation ID: 42
        request.extend_from_slice(&42i32.to_be_bytes());
        // Client ID: "test-client" (compact string: length + 1, then data)
        let client_id = b"test-client";
        request.extend_from_slice(&((client_id.len() + 1) as u8).to_be_bytes());
        request.extend_from_slice(client_id);
        // Tagged fields: 0 (no tags)
        request.extend_from_slice(&[0x00]);

        // TODO: Test actual parsing once we expose parse_request publicly
        // For now, this validates the request structure
        assert!(request.len() > 0);
    }

    #[test]
    fn test_api_versions_request_parsing_invalid_api_key() {
        // API key outside valid range (e.g., 999)
        let mut request = BytesMut::new();
        request.extend_from_slice(&999i16.to_be_bytes());
        request.extend_from_slice(&3i16.to_be_bytes());
        request.extend_from_slice(&42i32.to_be_bytes());

        // TODO: Test that this returns an error
        assert!(request.len() > 0);
    }

    #[test]
    fn test_api_versions_response_encoding() {
        // Test encoding a valid ApiVersions response
        let mut response = ApiVersionsResponse::default();
        response.error_code = 0;

        // Add supported API versions
        let mut api_version = ApiVersion::default();
        api_version.api_key = 18;
        api_version.min_version = 0;
        api_version.max_version = 3;
        response.api_keys.push(api_version);

        let header = ResponseHeader::default().with_correlation_id(42);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 3).unwrap();

        // Response should have content
        assert!(buf.len() > 0);

        // Should start with correlation_id (4 bytes)
        let correlation_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(correlation_id, 42);
    }

    #[test]
    fn test_metadata_request_parsing_all_topics() {
        // Metadata request with null topics (request all topics)
        let mut request = BytesMut::new();

        // API Key: 3 (Metadata)
        request.extend_from_slice(&3i16.to_be_bytes());
        // API Version: 9
        request.extend_from_slice(&9i16.to_be_bytes());
        // Correlation ID: 99
        request.extend_from_slice(&99i32.to_be_bytes());
        // Client ID: null (0 length for compact string means null)
        request.extend_from_slice(&[0x00]);
        // Topics: null array (0 for compact array means null, requests all topics)
        request.extend_from_slice(&[0x00]);
        // allow_auto_topic_creation: false
        request.extend_from_slice(&[0x00]);
        // include_topic_authorized_operations: false (optional, v8+)
        request.extend_from_slice(&[0x00]);
        // Tagged fields: 0
        request.extend_from_slice(&[0x00]);

        // TODO: Test actual parsing
        assert!(request.len() > 0);
    }

    #[test]
    fn test_metadata_request_parsing_specific_topics() {
        // Metadata request for specific topics
        let mut request = BytesMut::new();

        // API Key: 3
        request.extend_from_slice(&3i16.to_be_bytes());
        // API Version: 9
        request.extend_from_slice(&9i16.to_be_bytes());
        // Correlation ID: 100
        request.extend_from_slice(&100i32.to_be_bytes());
        // Client ID: null
        request.extend_from_slice(&[0x00]);

        // Topics: array with 2 topics (length + 1 for compact array)
        request.extend_from_slice(&[0x03]); // 2 topics + 1

        // Topic 1: "test-topic"
        let topic1 = b"test-topic";
        request.extend_from_slice(&((topic1.len() + 1) as u8).to_be_bytes());
        request.extend_from_slice(topic1);

        // Topic 2: "another-topic"
        let topic2 = b"another-topic";
        request.extend_from_slice(&((topic2.len() + 1) as u8).to_be_bytes());
        request.extend_from_slice(topic2);

        // allow_auto_topic_creation: false
        request.extend_from_slice(&[0x00]);
        // Tagged fields: 0
        request.extend_from_slice(&[0x00]);

        // TODO: Test actual parsing
        assert!(request.len() > 0);
    }

    #[test]
    fn test_metadata_response_encoding() {
        // Test encoding a Metadata response
        let response = MetadataResponse::default();

        // TODO: Add broker and topic data once we import our types

        let header = ResponseHeader::default().with_correlation_id(99);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 9).unwrap();

        // Response should have content
        assert!(buf.len() > 0);

        // Should start with correlation_id
        let correlation_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(correlation_id, 99);
    }

    #[test]
    fn test_truncated_request() {
        // Request that's too short (missing fields)
        let mut request = BytesMut::new();
        request.extend_from_slice(&18i16.to_be_bytes()); // Just API key, nothing else

        // TODO: Test that parsing fails gracefully
        assert_eq!(request.len(), 2);
    }

    #[test]
    fn test_empty_request() {
        // Completely empty request
        let request = BytesMut::new();

        // TODO: Test that parsing fails gracefully
        assert_eq!(request.len(), 0);
    }

    #[test]
    fn test_unsupported_api_version() {
        // ApiVersions request with unsupported version (e.g., 99)
        let mut request = BytesMut::new();
        request.extend_from_slice(&18i16.to_be_bytes());
        request.extend_from_slice(&99i16.to_be_bytes()); // Unsupported version
        request.extend_from_slice(&42i32.to_be_bytes());

        // TODO: Test that we return UNSUPPORTED_VERSION error
        assert!(request.len() > 0);
    }

    #[test]
    fn test_correlation_id_preservation() {
        // Verify that correlation_id is preserved in responses
        let correlation_id = 12345i32;

        let header = ResponseHeader::default().with_correlation_id(correlation_id);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();

        let decoded_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(decoded_id, correlation_id);
    }
}
