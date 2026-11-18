//! Test helper functions
//!
//! Utility functions for creating test fixtures and assertions

use crate::kafka::messages::{Record, RecordHeader};

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

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Additional Coverage Tests ==========

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
}
