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
}
