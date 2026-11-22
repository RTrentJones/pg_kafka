//! Performance E2E tests
//!
//! Tests for throughput baselines and performance regression detection.

mod regression;
mod throughput;

/// The `performance` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["performance";
        throughput::test_produce_throughput_baseline: sequential,
        throughput::test_consume_throughput_baseline: sequential,
        throughput::test_batch_vs_single_performance: sequential,
        regression::test_large_batch_throughput: sequential,
        regression::test_produce_latency_percentiles: sequential,
        regression::test_concurrent_connection_scaling: sequential,
        regression::test_long_poll_cpu_efficiency: sequential,
    ]
}
