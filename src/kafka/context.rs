//! Runtime context for shared configuration and state
//!
//! This module provides centralized access to configuration across all Kafka modules,
//! eliminating redundant Config::load() calls and enabling zero-cost config sharing.

use parking_lot::RwLock;
use std::sync::Arc;

use crate::config::Config;

/// Global runtime context accessible from any module
///
/// Provides thread-safe, shared access to configuration with support for
/// periodic reloading. Uses Arc<Config> internally so that cloning the
/// config reference is a single pointer copy (zero allocation).
///
/// # Thread Safety
///
/// Multiple threads can read config concurrently. Reload operations acquire
/// a write lock briefly to swap the Arc pointer.
///
/// # Usage
///
/// ```rust,ignore
/// // Create once at startup
/// let ctx = Arc::new(RuntimeContext::new(Config::load()));
///
/// // Clone Arc for passing to async tasks (cheap pointer copy)
/// let ctx_clone = ctx.clone();
///
/// // Access config (returns Arc<Config>, single pointer copy)
/// let cfg = ctx.config();
/// println!("Port: {}", cfg.port);
///
/// // Periodic reload (e.g., every 30 seconds)
/// ctx.reload();
/// ```
pub struct RuntimeContext {
    config: RwLock<Arc<Config>>,
}

impl RuntimeContext {
    /// Create a new runtime context with the given configuration
    pub fn new(config: Config) -> Self {
        Self {
            config: RwLock::new(Arc::new(config)),
        }
    }

    /// Get current config (cheap Arc clone - single pointer copy)
    ///
    /// This is a low-cost operation that returns a reference-counted
    /// pointer to the current configuration. Multiple callers can hold
    /// references to the same config instance.
    pub fn config(&self) -> Arc<Config> {
        self.config.read().clone()
    }

    /// Reload config from GUCs
    ///
    /// This loads fresh configuration from PostgreSQL GUCs and atomically
    /// swaps the internal Arc pointer. All subsequent calls to config()
    /// will return the new configuration.
    ///
    /// # Note
    ///
    /// Existing config references remain valid (Arc keeps them alive) but
    /// will reference the old configuration values.
    pub fn reload(&self) {
        let new_config = Config::load();
        *self.config.write() = Arc::new(new_config);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_context_creation() {
        // Create a config (in tests, Config::load() will use defaults)
        let config = Config::load();
        let ctx = RuntimeContext::new(config);

        // Should be able to get config
        let cfg = ctx.config();
        assert!(cfg.port > 0);
    }

    #[test]
    fn test_config_arc_cloning() {
        let config = Config::load();
        let ctx = RuntimeContext::new(config);

        // Get two references
        let cfg1 = ctx.config();
        let cfg2 = ctx.config();

        // Both should point to same data
        assert_eq!(cfg1.port, cfg2.port);
        assert_eq!(Arc::strong_count(&cfg1), Arc::strong_count(&cfg2));
    }

    #[test]
    fn test_config_reload() {
        let mut config = Config::load();
        config.port = 9092;
        let ctx = RuntimeContext::new(config);

        // Get initial config
        let cfg1 = ctx.config();
        assert_eq!(cfg1.port, 9092);

        // Simulate reload (in real code this calls Config::load())
        // For testing, we'll manually update
        let mut new_config = Config::load();
        new_config.port = 9093;
        *ctx.config.write() = Arc::new(new_config);

        // Get new config
        let cfg2 = ctx.config();
        assert_eq!(cfg2.port, 9093);

        // Old reference still has old value (Arc keeps it alive)
        assert_eq!(cfg1.port, 9092);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let config = Config::load();
        let ctx = Arc::new(RuntimeContext::new(config));

        // Spawn multiple threads reading config concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let ctx_clone = ctx.clone();
                thread::spawn(move || {
                    let cfg = ctx_clone.config();
                    cfg.port
                })
            })
            .collect();

        // All threads should read successfully
        for handle in handles {
            let port = handle.join().unwrap();
            assert!(port > 0);
        }
    }
}
