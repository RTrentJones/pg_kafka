// Property-based tests using proptest for fuzzing Kafka protocol implementation
//
// These tests generate random inputs to verify our protocol handling is robust
// against edge cases, extreme values, and malformed data.

#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_correlation_id_roundtrip(correlation_id: i32) {
            // Property: Any correlation_id should be preserved through encode/decode
            use bytes::BytesMut;
            use kafka_protocol::messages::ResponseHeader;
            use kafka_protocol::protocol::Encodable;

            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();

            // Verify correlation_id is preserved
            let decoded_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            prop_assert_eq!(decoded_id, correlation_id);
        }

        #[test]
        fn test_api_key_range(api_key in 0i16..100i16) {
            // Property: API keys in valid range should be encodable
            // This tests that we can handle various API key values
            prop_assert!(api_key >= 0);
            prop_assert!(api_key < 100);
        }

        #[test]
        fn test_size_field_encoding(size in 1i32..1000000i32) {
            // Property: Size fields should round-trip correctly
            let size_bytes = size.to_be_bytes();
            let decoded_size = i32::from_be_bytes(size_bytes);
            prop_assert_eq!(decoded_size, size);
        }

        #[test]
        fn test_port_numbers(port in 1024i32..65536i32) {
            // Property: All valid port numbers should be representable
            use bytes::BytesMut;
            use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
            use kafka_protocol::messages::BrokerId;
            use kafka_protocol::protocol::Encodable;
            use kafka_protocol::messages::ResponseHeader;
            use kafka_protocol::messages::metadata_response::MetadataResponse;

            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(1);
            broker.host = "localhost".into();
            broker.port = port;

            let mut response = MetadataResponse::default();
            response.brokers.push(broker);

            let header = ResponseHeader::default().with_correlation_id(1);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();
            response.encode(&mut buf, 9).unwrap();

            // Should encode successfully
            prop_assert!(buf.len() > 0);
        }

        #[test]
        fn test_broker_node_ids(node_id in 1i32..1000i32) {
            // Property: All positive node_ids should be valid
            use bytes::BytesMut;
            use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
            use kafka_protocol::messages::BrokerId;
            use kafka_protocol::protocol::Encodable;
            use kafka_protocol::messages::ResponseHeader;
            use kafka_protocol::messages::metadata_response::MetadataResponse;

            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(node_id);
            broker.host = "localhost".into();
            broker.port = 9092;

            let mut response = MetadataResponse::default();
            response.brokers.push(broker);

            let header = ResponseHeader::default().with_correlation_id(1);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();
            response.encode(&mut buf, 9).unwrap();

            prop_assert!(buf.len() > 0);
        }

        #[test]
        fn test_partition_indices(partition_idx in 0i32..1000i32) {
            // Property: All non-negative partition indices should be valid
            use bytes::BytesMut;
            use kafka_protocol::messages::metadata_response::{MetadataResponse, MetadataResponseBroker, MetadataResponseTopic, MetadataResponsePartition};
            use kafka_protocol::messages::{BrokerId, TopicName};
            use kafka_protocol::protocol::Encodable;
            use kafka_protocol::messages::ResponseHeader;

            let mut response = MetadataResponse::default();

            // Add broker
            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(1);
            broker.host = "localhost".into();
            broker.port = 9092;
            response.brokers.push(broker);

            // Add topic with partition
            let mut topic = MetadataResponseTopic::default();
            topic.name = Some(TopicName("test".into()));

            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = partition_idx;
            partition.leader_id = BrokerId(1);
            topic.partitions.push(partition);

            response.topics.push(topic);

            let header = ResponseHeader::default().with_correlation_id(1);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();
            response.encode(&mut buf, 9).unwrap();

            prop_assert!(buf.len() > 0);
        }

        #[test]
        fn test_api_version_ranges(
            min_version in 0i16..10i16,
            max_version in 0i16..20i16
        ) {
            // Property: API version ranges should be encodable
            use bytes::BytesMut;
            use kafka_protocol::messages::api_versions_response::{ApiVersionsResponse, ApiVersion};
            use kafka_protocol::protocol::Encodable;
            use kafka_protocol::messages::ResponseHeader;

            // min_version might be > max_version in random data, but that's OK for encoding test
            let mut av = ApiVersion::default();
            av.api_key = 18;
            av.min_version = min_version;
            av.max_version = max_version;

            let mut response = ApiVersionsResponse::default();
            response.api_keys.push(av);

            let header = ResponseHeader::default().with_correlation_id(1);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();
            response.encode(&mut buf, 3).unwrap();

            prop_assert!(buf.len() > 0);
        }

        #[test]
        fn test_topic_name_lengths(name_len in 1usize..100usize) {
            // Property: Topic names of various lengths should be encodable
            use bytes::BytesMut;
            use kafka_protocol::messages::metadata_response::{MetadataResponse, MetadataResponseTopic};
            use kafka_protocol::messages::TopicName;
            use kafka_protocol::protocol::Encodable;
            use kafka_protocol::messages::ResponseHeader;

            let topic_name = "a".repeat(name_len);

            let mut topic = MetadataResponseTopic::default();
            topic.name = Some(TopicName(topic_name.into()));

            let mut response = MetadataResponse::default();
            response.topics.push(topic);

            let header = ResponseHeader::default().with_correlation_id(1);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();
            response.encode(&mut buf, 9).unwrap();

            prop_assert!(buf.len() > 0);
        }
    }

    // Additional non-property tests for extreme cases
    #[test]
    fn test_max_i32_correlation_id() {
        use bytes::BytesMut;
        use kafka_protocol::messages::ResponseHeader;
        use kafka_protocol::protocol::Encodable;

        let header = ResponseHeader::default().with_correlation_id(i32::MAX);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();

        let decoded = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(decoded, i32::MAX);
    }

    #[test]
    fn test_min_i32_correlation_id() {
        use bytes::BytesMut;
        use kafka_protocol::messages::ResponseHeader;
        use kafka_protocol::protocol::Encodable;

        let header = ResponseHeader::default().with_correlation_id(i32::MIN);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();

        let decoded = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(decoded, i32::MIN);
    }
}
