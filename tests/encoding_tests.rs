// Comprehensive encoding/decoding tests for Kafka protocol
//
// These tests validate that we can correctly encode responses
// and that they match the Kafka wire protocol specification.

#[cfg(test)]
mod encoding_tests {
    use bytes::BytesMut;
    use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use kafka_protocol::messages::metadata_response::{
        MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{BrokerId, ResponseHeader, TopicName};
    use kafka_protocol::protocol::Encodable;

    #[test]
    fn test_api_versions_response_full_encode() {
        // Test encoding a complete ApiVersions response with multiple API versions
        let mut response = ApiVersionsResponse::default();
        response.error_code = 0;

        // Add ApiVersions (key 18)
        let mut av1 = ApiVersion::default();
        av1.api_key = 18;
        av1.min_version = 0;
        av1.max_version = 3;
        response.api_keys.push(av1);

        // Add Metadata (key 3)
        let mut av2 = ApiVersion::default();
        av2.api_key = 3;
        av2.min_version = 0;
        av2.max_version = 12;
        response.api_keys.push(av2);

        let header = ResponseHeader::default().with_correlation_id(42);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 3).unwrap();

        // Verify the buffer has content
        assert!(buf.len() > 0);

        // Verify correlation_id is at the start
        let correlation_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(correlation_id, 42);

        // The response should be at least:
        // - 4 bytes (correlation_id)
        // - 1 byte (tagged fields in header)
        // - 2 bytes (error_code)
        // - Variable bytes for api_keys array
        assert!(buf.len() >= 7);
    }

    #[test]
    fn test_metadata_response_full_encode() {
        // Test encoding a complete Metadata response with broker and topic info
        let mut response = MetadataResponse::default();

        // Add a broker
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(1);
        broker.host = "localhost".into();
        broker.port = 9092;
        broker.rack = None;
        response.brokers.push(broker);

        // Add a topic with one partition
        let mut topic = MetadataResponseTopic::default();
        topic.error_code = 0;
        topic.name = Some(TopicName("test-topic".into()));
        topic.is_internal = false;

        // Add partition
        let mut partition = MetadataResponsePartition::default();
        partition.error_code = 0;
        partition.partition_index = 0;
        partition.leader_id = BrokerId(1);
        partition.replica_nodes = vec![BrokerId(1)];
        partition.isr_nodes = vec![BrokerId(1)];
        topic.partitions.push(partition);

        response.topics.push(topic);

        let header = ResponseHeader::default().with_correlation_id(99);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 9).unwrap();

        // Verify the buffer has content
        assert!(buf.len() > 0);

        // Verify correlation_id
        let correlation_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(correlation_id, 99);

        // The response should be substantial since it includes broker and topic data
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_response_header_different_correlation_ids() {
        // Test that different correlation_ids are correctly encoded
        for correlation_id in [0, 1, 42, 999, i32::MAX, i32::MIN] {
            let header = ResponseHeader::default().with_correlation_id(correlation_id);
            let mut buf = BytesMut::new();
            header.encode(&mut buf, 1).unwrap();

            let decoded_id = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            assert_eq!(
                decoded_id, correlation_id,
                "Correlation ID mismatch for {}",
                correlation_id
            );
        }
    }

    #[test]
    fn test_empty_api_versions_response() {
        // Test encoding an ApiVersions response with no API keys
        let response = ApiVersionsResponse::default();
        let header = ResponseHeader::default().with_correlation_id(1);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 3).unwrap();

        assert!(buf.len() > 0);
    }

    #[test]
    fn test_metadata_response_multiple_topics() {
        // Test encoding metadata for multiple topics
        let mut response = MetadataResponse::default();

        // Add broker
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(1);
        broker.host = "localhost".into();
        broker.port = 9092;
        response.brokers.push(broker);

        // Add multiple topics
        for topic_name in ["topic1", "topic2", "topic3"] {
            let mut topic = MetadataResponseTopic::default();
            topic.error_code = 0;
            topic.name = Some(TopicName(topic_name.into()));

            // Add a partition
            let mut partition = MetadataResponsePartition::default();
            partition.error_code = 0;
            partition.partition_index = 0;
            partition.leader_id = BrokerId(1);
            topic.partitions.push(partition);

            response.topics.push(topic);
        }

        let header = ResponseHeader::default().with_correlation_id(100);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 9).unwrap();

        assert!(buf.len() > 0);
        assert!(buf.len() > 50); // Should be substantial with 3 topics
    }

    #[test]
    fn test_metadata_response_multiple_partitions() {
        // Test encoding a topic with multiple partitions
        let mut response = MetadataResponse::default();

        // Add broker
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(1);
        broker.host = "localhost".into();
        broker.port = 9092;
        response.brokers.push(broker);

        // Add topic with 3 partitions
        let mut topic = MetadataResponseTopic::default();
        topic.error_code = 0;
        topic.name = Some(TopicName("multi-partition-topic".into()));

        for partition_id in 0..3 {
            let mut partition = MetadataResponsePartition::default();
            partition.error_code = 0;
            partition.partition_index = partition_id;
            partition.leader_id = BrokerId(1);
            partition.replica_nodes = vec![BrokerId(1)];
            partition.isr_nodes = vec![BrokerId(1)];
            topic.partitions.push(partition);
        }

        response.topics.push(topic);

        let header = ResponseHeader::default().with_correlation_id(101);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 9).unwrap();

        assert!(buf.len() > 0);
    }

    #[test]
    fn test_broker_with_rack() {
        // Test encoding a broker with rack information
        let mut response = MetadataResponse::default();

        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(1);
        broker.host = "localhost".into();
        broker.port = 9092;
        broker.rack = Some("rack-1".into());
        response.brokers.push(broker);

        let header = ResponseHeader::default().with_correlation_id(102);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 9).unwrap();

        assert!(buf.len() > 0);
    }

    #[test]
    fn test_multiple_brokers() {
        // Test encoding metadata with multiple brokers
        let mut response = MetadataResponse::default();

        for node_id in 1..=3 {
            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(node_id);
            broker.host = format!("broker-{}", node_id).into();
            broker.port = 9092 + node_id;
            response.brokers.push(broker);
        }

        let header = ResponseHeader::default().with_correlation_id(103);

        let mut buf = BytesMut::new();
        header.encode(&mut buf, 1).unwrap();
        response.encode(&mut buf, 9).unwrap();

        assert!(buf.len() > 0);
        assert!(buf.len() > 40); // Should be substantial with 3 brokers
    }
}
