//! The broker's advertised host and port, as Metadata and FindCoordinator responses
//! report them.
//!
//! The host is shared behind an `Arc`, so handing it to every request is a pointer
//! copy rather than a string allocation on the hot Metadata / FindCoordinator path.

use std::sync::Arc;

/// The advertised host and port. Cloning copies a pointer, not the host string.
#[derive(Clone)]
pub struct BrokerMetadata {
    advertised_host: Arc<String>,
    advertised_port: i32,
}

impl BrokerMetadata {
    /// Metadata for `host` (a hostname or IP) and `port`.
    pub fn new(host: String, port: i32) -> Self {
        Self {
            advertised_host: Arc::new(host),
            advertised_port: port,
        }
    }

    /// The advertised host, borrowed.
    pub fn host(&self) -> &str {
        &self.advertised_host
    }

    /// The advertised port.
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
