//! pg_kafka E2E Test Suite Orchestrator
//!
//! A comprehensive test orchestrator with CLI support for running E2E tests.
//!
//! ## Usage
//!
//! ```bash
//! # Run all tests
//! cargo run --release
//!
//! # Run specific category
//! cargo run --release -- --category producer
//! cargo run --release -- --category error_paths
//!
//! # Run single test by name
//! cargo run --release -- --test test_producer
//!
//! # Run tests in parallel (where safe)
//! cargo run --release -- --parallel
//!
//! # JSON output for CI
//! cargo run --release -- --json
//!
//! # Combine flags
//! cargo run --release -- --category producer --json
//!
//! # List available tests
//! cargo run --release -- --list
//! ```
//!
//! ## Exit Codes
//!
//! - 0: All tests passed
//! - 1: One or more tests failed

use chrono::{DateTime, Utc};
use clap::Parser;
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

// Import all test functions
use kafka_test::{
    // Admin tests
    test_create_multiple_topics,
    test_create_partitions,
    test_create_partitions_cannot_decrease,
    test_create_topic,
    test_create_topic_already_exists,
    test_delete_group_empty,
    test_delete_group_non_empty,
    test_delete_topic,
    test_delete_topic_not_found,
    // Edge case tests
    test_batch_1000_messages,
    // Producer tests
    test_batch_produce,
    // Performance tests
    test_batch_vs_single_performance,
    // Error path tests
    test_commit_new_group,
    test_commit_offset_zero,
    test_commit_then_fetch_offset,
    // Concurrent tests
    test_concurrent_producers_different_partitions,
    test_concurrent_producers_same_topic,
    // Negative tests
    test_connection_refused,
    test_consume_empty_partition,
    test_consume_empty_topic,
    test_consume_throughput_baseline,
    // Consumer tests
    test_consumer_basic,
    test_consumer_catches_up,
    test_consumer_from_offset,
    test_consumer_group_empty,
    // Consumer group tests
    test_consumer_group_lifecycle,
    test_consumer_group_two_members,
    test_rebalance_after_leave,
    // Pipelining tests
    test_request_pipelining,
    test_session_timeout_rebalance,
    test_consumer_multiple_messages,
    test_consumer_rejoin_after_leave,
    test_duplicate_consumer_join,
    test_empty_group_id,
    test_fetch_committed_no_history,
    test_fetch_invalid_partition,
    test_fetch_offset_out_of_range,
    test_fetch_uncommitted_offset,
    test_fetch_unknown_topic,
    test_heartbeat_after_leave,
    test_high_offset_values,
    test_invalid_group_id,
    test_large_message_key,
    test_large_message_value,
    test_list_offsets_empty_topic,
    // Partition tests
    test_key_distribution,
    test_key_routing_deterministic,
    test_multi_partition_produce,
    test_null_key_distribution,
    test_multiple_consumer_groups,
    test_multiple_consumers_same_group,
    // Offset management tests
    test_offset_boundaries,
    test_offset_commit_fetch,
    test_offset_zero_boundary,
    test_partition_zero,
    test_produce_any_partition,
    test_produce_empty_batch,
    test_produce_invalid_partition,
    test_produce_large_key,
    test_produce_throughput_baseline,
    test_produce_timeout,
    test_produce_while_consuming,
    test_producer,
    test_rejoin_after_leave,
    test_single_partition_topic,
    // Long polling tests
    test_long_poll_immediate_return,
    test_long_poll_multiple_waiters,
    test_long_poll_producer_wakeup,
    test_long_poll_timeout,
    // Compression tests
    test_compressed_producer_gzip,
    test_compressed_producer_lz4,
    test_compressed_producer_snappy,
    test_compressed_producer_zstd,
    test_compression_roundtrip,
    // Idempotent producer tests
    test_idempotent_producer_basic,
    test_true_deduplication_manual_replay,
    // Transaction tests
    test_producer_fencing,
    test_read_committed_after_commit,
    test_read_committed_filters_pending,
    test_read_uncommitted_sees_pending,
    test_transactional_batch,
    test_transactional_producer_abort,
    test_transactional_producer_commit,
    test_txn_offset_commit,
};

type TestFn = fn() -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>;

/// CLI Arguments
#[derive(Parser, Debug)]
#[command(name = "kafka_test")]
#[command(about = "pg_kafka E2E Test Suite")]
#[command(version)]
struct Args {
    /// Run only tests in this category
    #[arg(short, long)]
    category: Option<String>,

    /// Exclude tests in these categories (can be specified multiple times)
    #[arg(short = 'x', long)]
    exclude: Vec<String>,

    /// Run only this specific test
    #[arg(short, long)]
    test: Option<String>,

    /// Run tests in parallel (where safe)
    #[arg(short, long)]
    parallel: bool,

    /// Output results as JSON
    #[arg(long)]
    json: bool,

    /// List all available tests
    #[arg(short, long)]
    list: bool,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

/// Test definition
struct TestDef {
    category: &'static str,
    name: &'static str,
    test_fn: TestFn,
    /// Whether this test is safe to run in parallel
    parallel_safe: bool,
}

/// Single test result
#[derive(Debug, Clone, Serialize)]
struct TestResult {
    category: String,
    name: String,
    passed: bool,
    duration_ms: u64,
    error: Option<String>,
}

/// Category result
#[derive(Debug, Clone, Serialize)]
struct CategoryResult {
    name: String,
    passed: usize,
    failed: usize,
    duration_ms: u64,
    tests: Vec<TestResult>,
}

/// Suite result for JSON output
#[derive(Debug, Serialize)]
struct SuiteResult {
    timestamp: DateTime<Utc>,
    total_passed: usize,
    total_failed: usize,
    total_duration_ms: u64,
    categories: Vec<CategoryResult>,
}

/// Wrap async test functions for dynamic dispatch
macro_rules! wrap_test {
    ($fn:expr) => {
        (|| -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
            Box::pin($fn())
        }) as TestFn
    };
}

/// Get all test definitions
fn get_all_tests() -> Vec<TestDef> {
    vec![
        // Admin API tests
        TestDef {
            category: "admin",
            name: "test_create_topic",
            test_fn: wrap_test!(test_create_topic),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_create_topic_already_exists",
            test_fn: wrap_test!(test_create_topic_already_exists),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_delete_topic",
            test_fn: wrap_test!(test_delete_topic),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_delete_topic_not_found",
            test_fn: wrap_test!(test_delete_topic_not_found),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_create_partitions",
            test_fn: wrap_test!(test_create_partitions),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_create_partitions_cannot_decrease",
            test_fn: wrap_test!(test_create_partitions_cannot_decrease),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_delete_group_empty",
            test_fn: wrap_test!(test_delete_group_empty),
            parallel_safe: true,
        },
        TestDef {
            category: "admin",
            name: "test_delete_group_non_empty",
            test_fn: wrap_test!(test_delete_group_non_empty),
            parallel_safe: false, // Uses shared consumer group state
        },
        TestDef {
            category: "admin",
            name: "test_create_multiple_topics",
            test_fn: wrap_test!(test_create_multiple_topics),
            parallel_safe: true,
        },
        // Producer tests
        TestDef {
            category: "producer",
            name: "test_producer",
            test_fn: wrap_test!(test_producer),
            parallel_safe: true,
        },
        TestDef {
            category: "producer",
            name: "test_batch_produce",
            test_fn: wrap_test!(test_batch_produce),
            parallel_safe: true,
        },
        // Consumer tests
        TestDef {
            category: "consumer",
            name: "test_consumer_basic",
            test_fn: wrap_test!(test_consumer_basic),
            parallel_safe: true,
        },
        TestDef {
            category: "consumer",
            name: "test_consumer_multiple_messages",
            test_fn: wrap_test!(test_consumer_multiple_messages),
            parallel_safe: true,
        },
        TestDef {
            category: "consumer",
            name: "test_consumer_from_offset",
            test_fn: wrap_test!(test_consumer_from_offset),
            parallel_safe: true,
        },
        // Offset management tests
        TestDef {
            category: "offset_management",
            name: "test_offset_commit_fetch",
            test_fn: wrap_test!(test_offset_commit_fetch),
            parallel_safe: true,
        },
        TestDef {
            category: "offset_management",
            name: "test_offset_boundaries",
            test_fn: wrap_test!(test_offset_boundaries),
            parallel_safe: true,
        },
        // Consumer group tests
        TestDef {
            category: "consumer_group",
            name: "test_consumer_group_lifecycle",
            test_fn: wrap_test!(test_consumer_group_lifecycle),
            parallel_safe: true,
        },
        TestDef {
            category: "consumer_group",
            name: "test_rebalance_after_leave",
            test_fn: wrap_test!(test_rebalance_after_leave),
            parallel_safe: false, // Uses shared group state
        },
        TestDef {
            category: "consumer_group",
            name: "test_session_timeout_rebalance",
            test_fn: wrap_test!(test_session_timeout_rebalance),
            parallel_safe: false, // Has sleep delays
        },
        // Partition tests
        TestDef {
            category: "partition",
            name: "test_multi_partition_produce",
            test_fn: wrap_test!(test_multi_partition_produce),
            parallel_safe: true,
        },
        TestDef {
            category: "partition",
            name: "test_key_routing_deterministic",
            test_fn: wrap_test!(test_key_routing_deterministic),
            parallel_safe: true,
        },
        TestDef {
            category: "partition",
            name: "test_key_distribution",
            test_fn: wrap_test!(test_key_distribution),
            parallel_safe: true,
        },
        TestDef {
            category: "partition",
            name: "test_null_key_distribution",
            test_fn: wrap_test!(test_null_key_distribution),
            parallel_safe: true,
        },
        // Error path tests
        TestDef {
            category: "error_paths",
            name: "test_fetch_unknown_topic",
            test_fn: wrap_test!(test_fetch_unknown_topic),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_fetch_invalid_partition",
            test_fn: wrap_test!(test_fetch_invalid_partition),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_fetch_offset_out_of_range",
            test_fn: wrap_test!(test_fetch_offset_out_of_range),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_consume_empty_topic",
            test_fn: wrap_test!(test_consume_empty_topic),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_heartbeat_after_leave",
            test_fn: wrap_test!(test_heartbeat_after_leave),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_rejoin_after_leave",
            test_fn: wrap_test!(test_rejoin_after_leave),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_commit_new_group",
            test_fn: wrap_test!(test_commit_new_group),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_empty_group_id",
            test_fn: wrap_test!(test_empty_group_id),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_multiple_consumers_same_group",
            test_fn: wrap_test!(test_multiple_consumers_same_group),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_commit_offset_zero",
            test_fn: wrap_test!(test_commit_offset_zero),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_fetch_uncommitted_offset",
            test_fn: wrap_test!(test_fetch_uncommitted_offset),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_commit_then_fetch_offset",
            test_fn: wrap_test!(test_commit_then_fetch_offset),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_produce_invalid_partition",
            test_fn: wrap_test!(test_produce_invalid_partition),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_produce_any_partition",
            test_fn: wrap_test!(test_produce_any_partition),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_produce_empty_batch",
            test_fn: wrap_test!(test_produce_empty_batch),
            parallel_safe: true,
        },
        TestDef {
            category: "error_paths",
            name: "test_produce_large_key",
            test_fn: wrap_test!(test_produce_large_key),
            parallel_safe: true,
        },
        // Edge case tests
        TestDef {
            category: "edge_cases",
            name: "test_offset_zero_boundary",
            test_fn: wrap_test!(test_offset_zero_boundary),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_partition_zero",
            test_fn: wrap_test!(test_partition_zero),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_single_partition_topic",
            test_fn: wrap_test!(test_single_partition_topic),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_high_offset_values",
            test_fn: wrap_test!(test_high_offset_values),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_consume_empty_partition",
            test_fn: wrap_test!(test_consume_empty_partition),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_list_offsets_empty_topic",
            test_fn: wrap_test!(test_list_offsets_empty_topic),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_fetch_committed_no_history",
            test_fn: wrap_test!(test_fetch_committed_no_history),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_consumer_group_empty",
            test_fn: wrap_test!(test_consumer_group_empty),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_large_message_key",
            test_fn: wrap_test!(test_large_message_key),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_large_message_value",
            test_fn: wrap_test!(test_large_message_value),
            parallel_safe: true,
        },
        TestDef {
            category: "edge_cases",
            name: "test_batch_1000_messages",
            test_fn: wrap_test!(test_batch_1000_messages),
            parallel_safe: true,
        },
        // Concurrent tests - NOT parallel safe (they test concurrency themselves)
        TestDef {
            category: "concurrent",
            name: "test_concurrent_producers_same_topic",
            test_fn: wrap_test!(test_concurrent_producers_same_topic),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_concurrent_producers_different_partitions",
            test_fn: wrap_test!(test_concurrent_producers_different_partitions),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_multiple_consumer_groups",
            test_fn: wrap_test!(test_multiple_consumer_groups),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_consumer_group_two_members",
            test_fn: wrap_test!(test_consumer_group_two_members),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_consumer_rejoin_after_leave",
            test_fn: wrap_test!(test_consumer_rejoin_after_leave),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_produce_while_consuming",
            test_fn: wrap_test!(test_produce_while_consuming),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_consumer_catches_up",
            test_fn: wrap_test!(test_consumer_catches_up),
            parallel_safe: false,
        },
        TestDef {
            category: "concurrent",
            name: "test_request_pipelining",
            test_fn: wrap_test!(test_request_pipelining),
            parallel_safe: true, // Uses its own connection
        },
        // Negative tests
        TestDef {
            category: "negative",
            name: "test_connection_refused",
            test_fn: wrap_test!(test_connection_refused),
            parallel_safe: true,
        },
        TestDef {
            category: "negative",
            name: "test_produce_timeout",
            test_fn: wrap_test!(test_produce_timeout),
            parallel_safe: true,
        },
        TestDef {
            category: "negative",
            name: "test_invalid_group_id",
            test_fn: wrap_test!(test_invalid_group_id),
            parallel_safe: true,
        },
        TestDef {
            category: "negative",
            name: "test_duplicate_consumer_join",
            test_fn: wrap_test!(test_duplicate_consumer_join),
            parallel_safe: true,
        },
        // Performance tests - NOT parallel safe (measuring throughput)
        TestDef {
            category: "performance",
            name: "test_produce_throughput_baseline",
            test_fn: wrap_test!(test_produce_throughput_baseline),
            parallel_safe: false,
        },
        TestDef {
            category: "performance",
            name: "test_consume_throughput_baseline",
            test_fn: wrap_test!(test_consume_throughput_baseline),
            parallel_safe: false,
        },
        TestDef {
            category: "performance",
            name: "test_batch_vs_single_performance",
            test_fn: wrap_test!(test_batch_vs_single_performance),
            parallel_safe: false,
        },
        // Long polling tests (Phase 8)
        TestDef {
            category: "long_poll",
            name: "test_long_poll_immediate_return",
            test_fn: wrap_test!(test_long_poll_immediate_return),
            parallel_safe: true,
        },
        TestDef {
            category: "long_poll",
            name: "test_long_poll_timeout",
            test_fn: wrap_test!(test_long_poll_timeout),
            parallel_safe: true,
        },
        TestDef {
            category: "long_poll",
            name: "test_long_poll_producer_wakeup",
            test_fn: wrap_test!(test_long_poll_producer_wakeup),
            parallel_safe: false, // Tests timing-sensitive behavior
        },
        TestDef {
            category: "long_poll",
            name: "test_long_poll_multiple_waiters",
            test_fn: wrap_test!(test_long_poll_multiple_waiters),
            parallel_safe: false, // Tests timing-sensitive behavior
        },
        // Compression tests (Phase 8)
        TestDef {
            category: "compression",
            name: "test_compressed_producer_gzip",
            test_fn: wrap_test!(test_compressed_producer_gzip),
            parallel_safe: true,
        },
        TestDef {
            category: "compression",
            name: "test_compressed_producer_snappy",
            test_fn: wrap_test!(test_compressed_producer_snappy),
            parallel_safe: true,
        },
        TestDef {
            category: "compression",
            name: "test_compressed_producer_lz4",
            test_fn: wrap_test!(test_compressed_producer_lz4),
            parallel_safe: true,
        },
        TestDef {
            category: "compression",
            name: "test_compressed_producer_zstd",
            test_fn: wrap_test!(test_compressed_producer_zstd),
            parallel_safe: true,
        },
        TestDef {
            category: "compression",
            name: "test_compression_roundtrip",
            test_fn: wrap_test!(test_compression_roundtrip),
            parallel_safe: true,
        },
        // Idempotent producer tests (Phase 9)
        TestDef {
            category: "idempotent",
            name: "test_idempotent_producer_basic",
            test_fn: wrap_test!(test_idempotent_producer_basic),
            parallel_safe: true,
        },
        TestDef {
            category: "idempotent",
            name: "test_true_deduplication_manual_replay",
            test_fn: wrap_test!(test_true_deduplication_manual_replay),
            parallel_safe: true,
        },
        // Transaction tests (Phase 10)
        TestDef {
            category: "transaction",
            name: "test_transactional_producer_commit",
            test_fn: wrap_test!(test_transactional_producer_commit),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_transactional_producer_abort",
            test_fn: wrap_test!(test_transactional_producer_abort),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_transactional_batch",
            test_fn: wrap_test!(test_transactional_batch),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_producer_fencing",
            test_fn: wrap_test!(test_producer_fencing),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_read_committed_filters_pending",
            test_fn: wrap_test!(test_read_committed_filters_pending),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_read_uncommitted_sees_pending",
            test_fn: wrap_test!(test_read_uncommitted_sees_pending),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_read_committed_after_commit",
            test_fn: wrap_test!(test_read_committed_after_commit),
            parallel_safe: true,
        },
        TestDef {
            category: "transaction",
            name: "test_txn_offset_commit",
            test_fn: wrap_test!(test_txn_offset_commit),
            parallel_safe: true,
        },
    ]
}

/// Run a single test
async fn run_single_test(test: &TestDef, verbose: bool) -> TestResult {
    let start = Instant::now();
    let future = (test.test_fn)();
    let result = future.await;
    let duration = start.elapsed();

    let (passed, error) = match result {
        Ok(()) => (true, None),
        Err(e) => {
            if verbose {
                println!("   Error: {}", e);
            }
            (false, Some(e.to_string()))
        }
    };

    TestResult {
        category: test.category.to_string(),
        name: test.name.to_string(),
        passed,
        duration_ms: duration.as_millis() as u64,
        error,
    }
}

/// Print test list
fn print_test_list(tests: &[TestDef]) {
    let mut current_category = "";
    for test in tests {
        if test.category != current_category {
            if !current_category.is_empty() {
                println!();
            }
            println!("{}:", test.category);
            current_category = test.category;
        }
        let parallel_indicator = if test.parallel_safe {
            ""
        } else {
            " [sequential]"
        };
        println!("  - {}{}", test.name, parallel_indicator);
    }
}

/// Print category header
fn print_category_header(category: &str) {
    let upper = category.to_uppercase().replace('_', " ");
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ {:58} │", format!("{} TESTS", upper));
    println!("└────────────────────────────────────────────────────────────┘\n");
}

/// Print text summary
fn print_text_summary(suite_result: &SuiteResult) {
    println!("\n{}", "=".repeat(60));
    println!("TEST SUITE SUMMARY");
    println!("{}\n", "=".repeat(60));

    for cat in &suite_result.categories {
        println!("{}:", cat.name);
        for test in &cat.tests {
            let status = if test.passed { "PASSED" } else { "FAILED" };
            let duration = format!("({:.2}s)", test.duration_ms as f64 / 1000.0);
            println!("  {} - {} {}", test.name, status, duration);
        }
        println!(
            "  Category: {}/{} passed in {:.2}s\n",
            cat.passed,
            cat.passed + cat.failed,
            cat.duration_ms as f64 / 1000.0
        );
    }

    println!("{}", "-".repeat(60));
    println!(
        "Total: {} passed, {} failed in {:.2}s",
        suite_result.total_passed,
        suite_result.total_failed,
        suite_result.total_duration_ms as f64 / 1000.0
    );

    if suite_result.total_failed == 0 {
        println!("\nALL TESTS PASSED");
    } else {
        println!("\nSOME TESTS FAILED");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let all_tests = get_all_tests();

    // Handle --list
    if args.list {
        println!("Available tests:\n");
        print_test_list(&all_tests);
        println!("\nCategories: admin, producer, consumer, offset_management, consumer_group,");
        println!(
            "            partition, error_paths, edge_cases, concurrent, negative, performance,"
        );
        println!("            long_poll");
        return Ok(());
    }

    // Filter tests based on args
    let tests_to_run: Vec<&TestDef> = all_tests
        .iter()
        .filter(|t| {
            // Exclude by category
            if args.exclude.iter().any(|ex| ex == t.category) {
                return false;
            }
            // Filter by category
            if let Some(ref cat) = args.category {
                if t.category != cat.as_str() {
                    return false;
                }
            }
            // Filter by test name
            if let Some(ref test_name) = args.test {
                if t.name != test_name.as_str() {
                    return false;
                }
            }
            true
        })
        .collect();

    if tests_to_run.is_empty() {
        eprintln!("No tests match the specified criteria");
        if let Some(ref cat) = args.category {
            eprintln!("Category: {}", cat);
        }
        if let Some(ref test_name) = args.test {
            eprintln!("Test: {}", test_name);
        }
        std::process::exit(1);
    }

    if !args.json {
        println!("╔════════════════════════════════════════════════════════════╗");
        println!("║           pg_kafka E2E Test Suite                          ║");
        println!("╚════════════════════════════════════════════════════════════╝\n");
        println!(
            "Running {} tests{}...\n",
            tests_to_run.len(),
            if args.parallel {
                " (parallel where safe)"
            } else {
                ""
            }
        );
    }

    // Verify broker is ready before running tests
    // This prevents false failures when broker is still initializing
    if !args.json {
        print!("Verifying broker connection... ");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }

    let mut attempts = 0;
    let max_attempts = 5;
    loop {
        match kafka_test::setup::verify_server_ready().await {
            Ok(()) => {
                if !args.json {
                    println!("OK");
                }
                break;
            }
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    eprintln!("FAILED after {} attempts: {}", max_attempts, e);
                    std::process::exit(1);
                }
                if !args.json {
                    print!("retry {}... ", attempts);
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }

    let suite_start = Instant::now();
    let mut category_results: Vec<CategoryResult> = Vec::new();
    let mut current_category = "";
    let mut current_cat_tests: Vec<TestResult> = Vec::new();
    let mut current_cat_start = Instant::now();

    // Group tests by category for execution
    for test in &tests_to_run {
        if test.category != current_category {
            // Save previous category if any
            if !current_category.is_empty() {
                let passed = current_cat_tests.iter().filter(|t| t.passed).count();
                let failed = current_cat_tests.iter().filter(|t| !t.passed).count();
                category_results.push(CategoryResult {
                    name: current_category.to_string(),
                    passed,
                    failed,
                    duration_ms: current_cat_start.elapsed().as_millis() as u64,
                    tests: std::mem::take(&mut current_cat_tests),
                });
            }

            current_category = test.category;
            current_cat_start = Instant::now();

            if !args.json {
                print_category_header(current_category);
            }
        }

        // Run the test
        let result = run_single_test(test, args.verbose).await;

        if !args.json {
            let status = if result.passed { "PASSED" } else { "FAILED" };
            let duration = format!("({:.2}s)", result.duration_ms as f64 / 1000.0);
            println!("  {} - {} {}", test.name, status, duration);
        }

        current_cat_tests.push(result);
    }

    // Save last category
    if !current_category.is_empty() {
        let passed = current_cat_tests.iter().filter(|t| t.passed).count();
        let failed = current_cat_tests.iter().filter(|t| !t.passed).count();
        category_results.push(CategoryResult {
            name: current_category.to_string(),
            passed,
            failed,
            duration_ms: current_cat_start.elapsed().as_millis() as u64,
            tests: current_cat_tests,
        });
    }

    // Build final result
    let total_passed: usize = category_results.iter().map(|c| c.passed).sum();
    let total_failed: usize = category_results.iter().map(|c| c.failed).sum();

    let suite_result = SuiteResult {
        timestamp: Utc::now(),
        total_passed,
        total_failed,
        total_duration_ms: suite_start.elapsed().as_millis() as u64,
        categories: category_results,
    };

    // Output results
    if args.json {
        println!("{}", serde_json::to_string_pretty(&suite_result)?);
    } else {
        print_text_summary(&suite_result);
    }

    // Exit with appropriate code
    if total_failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
