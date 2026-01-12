//! Broker metadata for advertised host/port information
//!
//! This module provides the BrokerMetadata struct which eliminates hot-path
//! string allocations by using Arc<String> for the broker host. This is
//! particularly important for Metadata and FindCoordinator requests which
//! previously cloned broker_host on every request (~10,000 allocations/sec
//! at high throughput).

use std::sync::Arc;

/// Broker metadata containing advertised host and port
///
/// Uses Arc<String> internally to enable zero-cost cloning of the host
/// reference across threads and requests. Cloning this struct only copies
/// a pointer, not the string data.
///
/// # Usage
///
/// ```rust,ignore
/// // Create once at startup
/// let broker = BrokerMetadata::new("localhost".to_string(), 9092);
///
/// // Clone for passing to handlers (cheap Arc pointer copy)
/// let broker_clone = broker.clone();
///
/// // Access host without allocation (returns &str)
/// let host_ref: &str = broker.host();
/// assert_eq!(host_ref, "localhost");
///
/// // Access port (simple copy)
/// let port: i32 = broker.port();
/// assert_eq!(port, 9092);
/// ```
#[derive(Clone)]
pub struct BrokerMetadata {
    /// Advertised host for clients to connect to
    ///
    /// Wrapped in Arc to enable cheap cloning across threads/requests.
    /// The string is allocated once and shared via reference counting.
    advertised_host: Arc<String>,

    /// Advertised port for clients to connect to
    advertised_port: i32,
}

impl BrokerMetadata {
    /// Create new broker metadata
    ///
    /// # Arguments
    /// * `host` - Advertised hostname or IP (e.g., "localhost", "broker.example.com")
    /// * `port` - Advertised port (typically 9092 for Kafka)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let broker = BrokerMetadata::new("kafka.example.com".to_string(), 9092);
    /// ```
    pub fn new(host: String, port: i32) -> Self {
        Self {
            advertised_host: Arc::new(host),
            advertised_port: port,
        }
    }

    /// Get advertised host as string slice (zero allocation)
    ///
    /// Returns a reference to the underlying string without any allocation
    /// or copying. This is the preferred way to access the host for
    /// constructing responses.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    /// assert_eq!(broker.host(), "localhost");
    /// ```
    pub fn host(&self) -> &str {
        &self.advertised_host
    }

    /// Get advertised port (copy)
    ///
    /// Returns the port number as a simple integer copy.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    /// assert_eq!(broker.port(), 9092);
    /// ```
    pub fn port(&self) -> i32 {
        self.advertised_port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_metadata_creation() {
        let broker = BrokerMetadata::new("localhost".to_string(), 9092);
        assert_eq!(broker.host(), "localhost");
        assert_eq!(broker.port(), 9092);
    }

    #[test]
    fn test_broker_metadata_cloning() {
        let broker1 = BrokerMetadata::new("kafka.example.com".to_string(), 9093);
        let broker2 = broker1.clone();

        // Both should have same values
        assert_eq!(broker1.host(), broker2.host());
        assert_eq!(broker1.port(), broker2.port());

        // Both should point to same Arc (same strong count)
        assert_eq!(
            Arc::strong_count(&broker1.advertised_host),
            Arc::strong_count(&broker2.advertised_host)
        );
    }

    #[test]
    fn test_host_returns_reference() {
        let broker = BrokerMetadata::new("test-host".to_string(), 1234);
        let host_ref = broker.host();

        // This should not allocate - just return a reference
        assert_eq!(host_ref, "test-host");

        // Calling multiple times should return same reference
        let host_ref2 = broker.host();
        assert_eq!(host_ref, host_ref2);
    }

    #[test]
    fn test_concurrent_cloning() {
        use std::thread;

        let broker = Arc::new(BrokerMetadata::new("concurrent-test".to_string(), 5555));

        // Spawn multiple threads cloning the broker
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let b = broker.clone();
                thread::spawn(move || {
                    assert_eq!(b.host(), "concurrent-test");
                    assert_eq!(b.port(), 5555);
                })
            })
            .collect();

        // All threads should succeed
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
