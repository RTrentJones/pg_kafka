//! pg_kafka E2E Test Suite Orchestrator
//!
//! This orchestrator runs all E2E tests in the correct order:
//! 1. Producer tests (basic functionality must work first)
//! 2. Consumer tests (depend on producer working)
//! 3. Offset management tests (depend on both working)
//! 4. Consumer group tests (advanced coordinator functionality)
//! 5. Partition tests (multi-partition support)
//!
//! ## Usage
//!
//! ```bash
//! # Run all tests
//! cargo run --release
//!
//! # With custom database URL
//! DATABASE_URL="host=localhost port=28814 user=postgres" cargo run --release
//! ```
//!
//! ## Exit Codes
//!
//! - 0: All tests passed
//! - 1: One or more tests failed

use kafka_test::{
    // Producer tests
    test_batch_produce,
    // Consumer tests
    test_consumer_basic,
    test_consumer_from_offset,
    // Consumer group tests
    test_consumer_group_lifecycle,
    test_consumer_multiple_messages,
    // Partition tests
    test_multi_partition_produce,
    // Offset management tests
    test_offset_boundaries,
    test_offset_commit_fetch,
    test_producer,
};

/// Test suite result tracking
struct TestSuiteResults {
    passed: usize,
    failed: usize,
    results: Vec<(&'static str, &'static str, bool)>, // (category, name, passed)
}

impl TestSuiteResults {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
            results: Vec::new(),
        }
    }

    fn record(&mut self, category: &'static str, name: &'static str, passed: bool) {
        if passed {
            self.passed += 1;
        } else {
            self.failed += 1;
        }
        self.results.push((category, name, passed));
    }

    fn print_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("TEST SUITE SUMMARY");
        println!("{}\n", "=".repeat(60));

        let mut current_category = "";
        for (category, name, passed) in &self.results {
            if *category != current_category {
                if !current_category.is_empty() {
                    println!();
                }
                println!("{}:", category);
                current_category = category;
            }
            let status = if *passed { "✅ PASSED" } else { "❌ FAILED" };
            println!("  {} - {}", name, status);
        }

        println!("\n{}", "-".repeat(60));
        println!(
            "Total: {} passed, {} failed, {} total",
            self.passed,
            self.failed,
            self.passed + self.failed
        );

        if self.failed == 0 {
            println!("\n✅ ALL TESTS PASSED");
        } else {
            println!("\n❌ SOME TESTS FAILED");
        }
    }
}

/// Run a single test and record the result
macro_rules! run_test {
    ($results:expr, $category:expr, $name:expr, $test_fn:expr) => {{
        let result = $test_fn.await;
        let passed = result.is_ok();
        if let Err(e) = &result {
            println!("❌ Test failed: {}", e);
        }
        $results.record($category, $name, passed);
        passed
    }};
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║           pg_kafka E2E Test Suite                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let mut results = TestSuiteResults::new();

    // ==================== PRODUCER TESTS ====================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ PRODUCER TESTS                                             │");
    println!("└────────────────────────────────────────────────────────────┘\n");

    run_test!(results, "Producer", "Basic Producer", test_producer());

    // ==================== CONSUMER TESTS ====================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ CONSUMER TESTS                                             │");
    println!("└────────────────────────────────────────────────────────────┘\n");

    run_test!(results, "Consumer", "Basic Consumer", test_consumer_basic());
    run_test!(
        results,
        "Consumer",
        "Multiple Messages",
        test_consumer_multiple_messages()
    );
    run_test!(
        results,
        "Consumer",
        "From Offset",
        test_consumer_from_offset()
    );

    // ==================== OFFSET MANAGEMENT TESTS ====================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ OFFSET MANAGEMENT TESTS                                    │");
    println!("└────────────────────────────────────────────────────────────┘\n");

    run_test!(
        results,
        "Offset Management",
        "Commit/Fetch",
        test_offset_commit_fetch()
    );
    run_test!(
        results,
        "Offset Management",
        "Boundaries",
        test_offset_boundaries()
    );

    // ==================== CONSUMER GROUP TESTS ====================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ CONSUMER GROUP TESTS                                       │");
    println!("└────────────────────────────────────────────────────────────┘\n");

    run_test!(
        results,
        "Consumer Group",
        "Lifecycle",
        test_consumer_group_lifecycle()
    );

    // ==================== BATCH TESTS ====================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ BATCH TESTS                                                │");
    println!("└────────────────────────────────────────────────────────────┘\n");

    run_test!(
        results,
        "Batch",
        "Batch Produce (100)",
        test_batch_produce()
    );

    // ==================== PARTITION TESTS ====================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ PARTITION TESTS                                            │");
    println!("└────────────────────────────────────────────────────────────┘\n");

    run_test!(
        results,
        "Partition",
        "Multi-Partition",
        test_multi_partition_produce()
    );

    // ==================== SUMMARY ====================
    results.print_summary();

    // Exit with appropriate code
    if results.failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
