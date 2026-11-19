//! The runtime configuration shared by the worker's threads.
//!
//! The worker loads the GUCs into a `Config` once and reloads it periodically; every
//! module reads it through [`RuntimeContext`] instead of calling `Config::load()` itself.

use parking_lot::RwLock;
use std::sync::Arc;

use crate::config::Config;

/// The current configuration, replaceable as a whole.
///
/// [`config`](Self::config) returns an `Arc<Config>` (a pointer copy); [`reload`](Self::reload)
/// takes the write lock only to swap that pointer, so readers on any thread never wait
/// for a GUC read. A reader keeps the configuration it got until it asks again.
pub struct RuntimeContext {
    config: RwLock<Arc<Config>>,
}

impl RuntimeContext {
    /// A context holding `config`.
    pub fn new(config: Config) -> Self {
        Self {
            config: RwLock::new(Arc::new(config)),
        }
    }

    /// The current configuration.
    pub fn config(&self) -> Arc<Config> {
        self.config.read().clone()
    }

    /// Reload the configuration from the GUCs (DB thread: `Config::load` reads them).
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
