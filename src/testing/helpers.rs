//! Test helper functions
//!
//! Utility functions for creating test fixtures and assertions

use crate::kafka::messages::{
    KafkaRequest, KafkaResponse, PartitionProduceData, Record, RecordHeader, TopicProduceData,
};

/// Creates a mock ApiVersions request with a channel for receiving the response
///
/// # Returns
/// (request, response_receiver) tuple
///
/// # Example
/// ```
/// let (request, mut rx) = mock_api_versions_request(42);
/// process_request(request);
/// let response = rx.try_recv().unwrap();
/// ```
pub fn mock_api_versions_request(
    correlation_id: i32,
) -> (
    KafkaRequest,
    tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
) {
    let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();

    let request = KafkaRequest::ApiVersions {
        correlation_id,
        client_id: None,
        api_version: 3, // Default to v3 for tests
        response_tx,
    };

    (request, response_rx)
}

/// Creates a mock ApiVersions request with a custom client_id
pub fn mock_api_versions_request_with_client(
    correlation_id: i32,
    client_id: String,
) -> (
    KafkaRequest,
    tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
) {
    let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();

    let request = KafkaRequest::ApiVersions {
        correlation_id,
        client_id: Some(client_id),
        api_version: 3, // Default to v3 for tests
        response_tx,
    };

    (request, response_rx)
}

/// Creates a mock Metadata request
///
/// # Arguments
/// * `correlation_id` - Correlation ID for the request
/// * `topics` - Optional list of topic names (None = request all topics)
///
/// # Returns
/// (request, response_receiver) tuple
pub fn mock_metadata_request(
    correlation_id: i32,
    topics: Option<Vec<String>>,
) -> (
    KafkaRequest,
    tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
) {
    let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();

    let request = KafkaRequest::Metadata {
        correlation_id,
        client_id: None,
        api_version: 4, // Default to v4 for tests
        topics,
        response_tx,
    };

    (request, response_rx)
}

/// Creates a mock Metadata request with a custom client_id
pub fn mock_metadata_request_with_client(
    correlation_id: i32,
    client_id: String,
    topics: Option<Vec<String>>,
) -> (
    KafkaRequest,
    tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
) {
    let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();

    let request = KafkaRequest::Metadata {
        correlation_id,
        client_id: Some(client_id),
        api_version: 4, // Default to v4 for tests
        topics,
        response_tx,
    };

    (request, response_rx)
}

/// Creates a simple Record for testing
///
/// # Arguments
/// * `key` - Optional message key as string
/// * `value` - Message value as string
///
/// # Returns
/// Record with key/value as byte vectors, no headers, no timestamp
pub fn simple_record(key: Option<&str>, value: &str) -> Record {
    Record {
        key: key.map(|k| k.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: Vec::new(),
        timestamp: None,
    }
}

/// Creates a Record with headers for testing
///
/// # Arguments
/// * `key` - Optional message key as string
/// * `value` - Message value as string
/// * `headers` - Vector of (key, value) tuples for headers
///
/// # Returns
/// Record with key/value/headers as byte vectors
pub fn record_with_headers(key: Option<&str>, value: &str, headers: Vec<(&str, &[u8])>) -> Record {
    let record_headers: Vec<RecordHeader> = headers
        .into_iter()
        .map(|(k, v)| RecordHeader {
            key: k.to_string(),
            value: v.to_vec(),
        })
        .collect();

    Record {
        key: key.map(|k| k.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: record_headers,
        timestamp: None,
    }
}

/// Creates a mock Produce request
///
/// # Arguments
/// * `correlation_id` - Correlation ID for the request
/// * `topic` - Topic name to produce to
/// * `partition` - Partition index
/// * `records` - Vector of records to produce
///
/// # Returns
/// (request, response_receiver) tuple
pub fn mock_produce_request(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    records: Vec<Record>,
) -> (
    KafkaRequest,
    tokio::sync::mpsc::UnboundedReceiver<KafkaResponse>,
) {
    let (response_tx, response_rx) = tokio::sync::mpsc::unbounded_channel();

    let partition_data = PartitionProduceData {
        partition_index: partition,
        records,
        producer_metadata: None,
    };

    let topic_data = TopicProduceData {
        name: topic.to_string(),
        partitions: vec![partition_data],
    };

    let request = KafkaRequest::Produce {
        correlation_id,
        client_id: None,
        api_version: 9, // Default to v9 for tests (flexible format)
        acks: 1,        // Wait for leader acknowledgment
        timeout_ms: 5000,
        topic_data: vec![topic_data],
        transactional_id: None, // Non-transactional for tests
        response_tx,
    };

    (request, response_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_api_versions_request() {
        let (request, _rx) = mock_api_versions_request(42);
        match request {
            KafkaRequest::ApiVersions {
                correlation_id,
                client_id,
                ..
            } => {
                assert_eq!(correlation_id, 42);
                assert!(client_id.is_none());
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    #[test]
    fn test_mock_api_versions_request_with_client() {
        let (request, _rx) = mock_api_versions_request_with_client(99, "test-client".to_string());
        match request {
            KafkaRequest::ApiVersions {
                correlation_id,
                client_id,
                ..
            } => {
                assert_eq!(correlation_id, 99);
                assert_eq!(client_id, Some("test-client".to_string()));
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    #[test]
    fn test_mock_metadata_request_all_topics() {
        let (request, _rx) = mock_metadata_request(100, None);
        match request {
            KafkaRequest::Metadata {
                correlation_id,
                topics,
                ..
            } => {
                assert_eq!(correlation_id, 100);
                assert!(topics.is_none());
            }
            _ => panic!("Expected Metadata request"),
        }
    }

    #[test]
    fn test_mock_metadata_request_specific_topics() {
        let (request, _rx) =
            mock_metadata_request(101, Some(vec!["topic1".to_string(), "topic2".to_string()]));
        match request {
            KafkaRequest::Metadata {
                correlation_id,
                topics,
                ..
            } => {
                assert_eq!(correlation_id, 101);
                assert_eq!(
                    topics,
                    Some(vec!["topic1".to_string(), "topic2".to_string()])
                );
            }
            _ => panic!("Expected Metadata request"),
        }
    }

    // ========== Additional Coverage Tests ==========

    #[test]
    fn test_mock_metadata_request_with_client_all_topics() {
        let (request, _rx) = mock_metadata_request_with_client(200, "my-client".to_string(), None);
        match request {
            KafkaRequest::Metadata {
                correlation_id,
                client_id,
                topics,
                api_version,
                ..
            } => {
                assert_eq!(correlation_id, 200);
                assert_eq!(client_id, Some("my-client".to_string()));
                assert!(topics.is_none());
                assert_eq!(api_version, 4);
            }
            _ => panic!("Expected Metadata request"),
        }
    }

    #[test]
    fn test_mock_metadata_request_with_client_specific_topics() {
        let topics = Some(vec!["topic-a".to_string(), "topic-b".to_string()]);
        let (request, _rx) = mock_metadata_request_with_client(201, "client-x".to_string(), topics);
        match request {
            KafkaRequest::Metadata {
                correlation_id,
                client_id,
                topics,
                ..
            } => {
                assert_eq!(correlation_id, 201);
                assert_eq!(client_id, Some("client-x".to_string()));
                assert_eq!(
                    topics,
                    Some(vec!["topic-a".to_string(), "topic-b".to_string()])
                );
            }
            _ => panic!("Expected Metadata request"),
        }
    }

    #[test]
    fn test_simple_record_with_key() {
        let record = simple_record(Some("my-key"), "my-value");
        assert_eq!(record.key, Some(b"my-key".to_vec()));
        assert_eq!(record.value, Some(b"my-value".to_vec()));
        assert!(record.headers.is_empty());
        assert!(record.timestamp.is_none());
    }

    #[test]
    fn test_simple_record_without_key() {
        let record = simple_record(None, "value-only");
        assert!(record.key.is_none());
        assert_eq!(record.value, Some(b"value-only".to_vec()));
        assert!(record.headers.is_empty());
        assert!(record.timestamp.is_none());
    }

    #[test]
    fn test_simple_record_empty_value() {
        let record = simple_record(None, "");
        assert!(record.key.is_none());
        assert_eq!(record.value, Some(Vec::new()));
        assert!(record.headers.is_empty());
    }

    #[test]
    fn test_record_with_headers_single_header() {
        let headers = vec![("header-key", b"header-value".as_slice())];
        let record = record_with_headers(Some("key"), "value", headers);

        assert_eq!(record.key, Some(b"key".to_vec()));
        assert_eq!(record.value, Some(b"value".to_vec()));
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.headers[0].key, "header-key");
        assert_eq!(record.headers[0].value, b"header-value".to_vec());
        assert!(record.timestamp.is_none());
    }

    #[test]
    fn test_record_with_headers_multiple_headers() {
        let headers = vec![
            ("h1", b"v1".as_slice()),
            ("h2", b"v2".as_slice()),
            ("h3", b"v3".as_slice()),
        ];
        let record = record_with_headers(None, "test-value", headers);

        assert!(record.key.is_none());
        assert_eq!(record.value, Some(b"test-value".to_vec()));
        assert_eq!(record.headers.len(), 3);
        assert_eq!(record.headers[0].key, "h1");
        assert_eq!(record.headers[1].key, "h2");
        assert_eq!(record.headers[2].key, "h3");
    }

    #[test]
    fn test_record_with_headers_empty_headers() {
        let headers: Vec<(&str, &[u8])> = vec![];
        let record = record_with_headers(Some("k"), "v", headers);

        assert_eq!(record.key, Some(b"k".to_vec()));
        assert_eq!(record.value, Some(b"v".to_vec()));
        assert!(record.headers.is_empty());
    }

    #[test]
    fn test_record_with_headers_binary_value() {
        let binary_data: &[u8] = &[0x00, 0x01, 0xFF, 0xFE];
        let headers = vec![("binary", binary_data)];
        let record = record_with_headers(None, "text", headers);

        assert_eq!(record.headers[0].value, vec![0x00, 0x01, 0xFF, 0xFE]);
    }

    #[test]
    fn test_mock_produce_request_basic() {
        let records = vec![simple_record(Some("k1"), "v1")];
        let (request, _rx) = mock_produce_request(300, "test-topic", 0, records);

        match request {
            KafkaRequest::Produce {
                correlation_id,
                client_id,
                api_version,
                acks,
                timeout_ms,
                topic_data,
                transactional_id,
                ..
            } => {
                assert_eq!(correlation_id, 300);
                assert!(client_id.is_none());
                assert_eq!(api_version, 9);
                assert_eq!(acks, 1);
                assert_eq!(timeout_ms, 5000);
                assert!(transactional_id.is_none());
                assert_eq!(topic_data.len(), 1);
                assert_eq!(topic_data[0].name, "test-topic");
                assert_eq!(topic_data[0].partitions.len(), 1);
                assert_eq!(topic_data[0].partitions[0].partition_index, 0);
                assert_eq!(topic_data[0].partitions[0].records.len(), 1);
            }
            _ => panic!("Expected Produce request"),
        }
    }

    #[test]
    fn test_mock_produce_request_multiple_records() {
        let records = vec![
            simple_record(Some("k1"), "v1"),
            simple_record(Some("k2"), "v2"),
            simple_record(None, "v3"),
        ];
        let (request, _rx) = mock_produce_request(301, "multi-topic", 2, records);

        match request {
            KafkaRequest::Produce {
                topic_data,
                correlation_id,
                ..
            } => {
                assert_eq!(correlation_id, 301);
                assert_eq!(topic_data[0].name, "multi-topic");
                assert_eq!(topic_data[0].partitions[0].partition_index, 2);
                assert_eq!(topic_data[0].partitions[0].records.len(), 3);
            }
            _ => panic!("Expected Produce request"),
        }
    }

    #[test]
    fn test_mock_produce_request_empty_records() {
        let records: Vec<Record> = vec![];
        let (request, _rx) = mock_produce_request(302, "empty-topic", 0, records);

        match request {
            KafkaRequest::Produce { topic_data, .. } => {
                assert!(topic_data[0].partitions[0].records.is_empty());
            }
            _ => panic!("Expected Produce request"),
        }
    }

    #[test]
    fn test_mock_produce_request_with_headers() {
        let headers = vec![("content-type", b"application/json".as_slice())];
        let record = record_with_headers(Some("key"), "{\"data\":1}", headers);
        let (request, _rx) = mock_produce_request(303, "headers-topic", 0, vec![record]);

        match request {
            KafkaRequest::Produce { topic_data, .. } => {
                let rec = &topic_data[0].partitions[0].records[0];
                assert_eq!(rec.headers.len(), 1);
                assert_eq!(rec.headers[0].key, "content-type");
            }
            _ => panic!("Expected Produce request"),
        }
    }

    #[test]
    fn test_mock_api_versions_request_api_version() {
        let (request, _rx) = mock_api_versions_request(1);
        match request {
            KafkaRequest::ApiVersions { api_version, .. } => {
                assert_eq!(api_version, 3);
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    #[test]
    fn test_mock_metadata_request_api_version() {
        let (request, _rx) = mock_metadata_request(1, None);
        match request {
            KafkaRequest::Metadata { api_version, .. } => {
                assert_eq!(api_version, 4);
            }
            _ => panic!("Expected Metadata request"),
        }
    }
}
