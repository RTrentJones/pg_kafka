//! Performance E2E tests
//!
//! Tests for throughput baselines and performance regression detection.

mod regression;
mod throughput;

pub use regression::{
    test_concurrent_connection_scaling, test_large_batch_throughput,
    test_long_poll_cpu_efficiency, test_produce_latency_percentiles,
};
pub use throughput::*;
