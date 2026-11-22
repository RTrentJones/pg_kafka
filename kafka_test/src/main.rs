//! pg_kafka E2E Test Suite Orchestrator
//!
//! A comprehensive test orchestrator with CLI support for running E2E tests.
//!
//! ## Usage
//!
//! ```bash
//! # Run all tests (parallel by default, 16 concurrent)
//! cargo run --release
//!
//! # Run specific category
//! cargo run --release -- --category producer
//! cargo run --release -- --category error_paths
//!
//! # Run single test by name
//! cargo run --release -- --test test_producer
//!
//! # Run tests sequentially (disable parallelism)
//! cargo run --release -- --sequential
//!
//! # Adjust concurrency level
//! cargo run --release -- --concurrency 32
//!
//! # JSON output for CI
//! cargo run --release -- --json
//!
//! # Combine flags
//! cargo run --release -- --category producer --json
//!
//! # List available tests
//! cargo run --release -- --list
//!
//! # Per-test timeout (default 300 s); a hung or panicking test is reported
//! # FAILED and the run continues
//! cargo run --release -- --timeout-secs 120
//! ```
//!
//! ## Exit Codes
//!
//! - 0: All tests passed
//! - 1: One or more tests failed

use clap::Parser;
use futures::stream::{self, StreamExt};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

use kafka_test::registry::{TestDef, TestFn};

/// CLI Arguments
#[derive(Parser, Debug)]
#[command(name = "kafka_test")]
#[command(about = "pg_kafka E2E Test Suite")]
#[command(version)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent command-line flag"
)]
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

    /// Run tests sequentially (disables parallel execution)
    #[arg(short, long)]
    sequential: bool,

    /// Maximum concurrent tests when running in parallel (default: 16)
    #[arg(long, default_value = "16")]
    concurrency: usize,

    /// Output results as JSON
    #[arg(long)]
    json: bool,

    /// List all available tests
    #[arg(short, long)]
    list: bool,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Per-test timeout in seconds; a test still running after this is
    /// aborted and reported FAILED
    #[arg(long, default_value_t = DEFAULT_TEST_TIMEOUT_SECS)]
    timeout_secs: u64,
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

/// Format `time` as an RFC 3339 UTC timestamp with millisecond precision
/// (`2026-11-19T22:57:41.269Z`). Days are converted to a civil date with Howard Hinnant's
/// `civil_from_days` algorithm, valid for every date after 1970.
fn rfc3339_utc(time: std::time::SystemTime) -> String {
    let since_epoch = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since_epoch.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let z = i64::try_from(days).expect("days since 1970 fit in i64") + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        since_epoch.subsec_millis()
    )
}

/// Suite result for JSON output
#[derive(Debug, Serialize)]
struct SuiteResult {
    /// When the run finished, RFC 3339 in UTC.
    timestamp: String,
    total_passed: usize,
    total_failed: usize,
    total_duration_ms: u64,
    categories: Vec<CategoryResult>,
}

/// Default per-test timeout. Generous: the slowest tests (performance,
/// session-timeout rebalances, shadow dial-up) take well under a minute; this
/// only exists so a hung test reports FAILED instead of stalling the run.
const DEFAULT_TEST_TIMEOUT_SECS: u64 = 300;

/// `d` in whole milliseconds (saturating; no test runs for 584 million years).
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// `ms` milliseconds in seconds, for display.
fn secs(ms: u64) -> f64 {
    Duration::from_millis(ms).as_secs_f64()
}

/// Extract a readable message from a panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Run one test function in its own task, bounded by `timeout`.
///
/// The test runs in a spawned task so that a failed `assert!` (a panic) is
/// caught by the task boundary and reported as a FAILED result with its
/// message, instead of unwinding through the runner and aborting the whole
/// suite with no summary. On timeout the task is aborted (at its next await
/// point) and the test is reported FAILED. Returns `(passed, error)`.
async fn execute_test_fn(test_fn: TestFn, timeout: Duration) -> (bool, Option<String>) {
    // `Box<dyn Error>` is not `Send`, so stringify inside the task.
    let handle = tokio::spawn(async move { (test_fn)().await.map_err(|e| e.to_string()) });
    let abort = handle.abort_handle();
    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(Ok(()))) => (true, None),
        Ok(Ok(Err(e))) => (false, Some(e)),
        Ok(Err(join_err)) if join_err.is_panic() => {
            let payload = join_err.into_panic();
            (
                false,
                Some(format!("panicked: {}", panic_message(payload.as_ref()))),
            )
        }
        Ok(Err(join_err)) => (false, Some(format!("test task failed: {join_err}"))),
        Err(_) => {
            abort.abort();
            (
                false,
                Some(format!("timed out after {}s", timeout.as_secs_f64())),
            )
        }
    }
}

/// Run a single test
async fn run_single_test(test: &TestDef, verbose: bool, timeout: Duration) -> TestResult {
    let start = Instant::now();
    let (passed, error) = execute_test_fn(test.test_fn, timeout).await;
    let duration = start.elapsed();

    if verbose {
        if let Some(ref e) = error {
            println!("   Error: {e}");
        }
    }

    TestResult {
        category: test.category.to_string(),
        name: test.name.to_string(),
        passed,
        duration_ms: millis(duration),
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

/// Categories in registry order, each listed once
fn unique_categories<'a>(tests: impl Iterator<Item = &'a TestDef>) -> Vec<&'static str> {
    let mut categories: Vec<&'static str> = Vec::new();
    for test in tests {
        if !categories.contains(&test.category) {
            categories.push(test.category);
        }
    }
    categories
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
            let duration = format!("({:.2}s)", secs(test.duration_ms));
            println!("  {} - {} {}", test.name, status, duration);
            if let Some(error) = &test.error {
                println!("      error: {error}");
            }
        }
        println!(
            "  Category: {}/{} passed in {:.2}s\n",
            cat.passed,
            cat.passed + cat.failed,
            secs(cat.duration_ms)
        );
    }

    println!("{}", "-".repeat(60));
    println!(
        "Total: {} passed, {} failed in {:.2}s",
        suite_result.total_passed,
        suite_result.total_failed,
        secs(suite_result.total_duration_ms)
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

    let all_tests = kafka_test::all_tests();

    // Handle --list
    if args.list {
        println!("Available tests:\n");
        print_test_list(&all_tests);
        let categories = unique_categories(all_tests.iter());
        println!(
            "\n{} tests in {} categories: {}",
            all_tests.len(),
            categories.len(),
            categories.join(", ")
        );
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
            eprintln!("Category: {cat}");
        }
        if let Some(ref test_name) = args.test {
            eprintln!("Test: {test_name}");
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
            if args.sequential {
                " (sequential mode)"
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
        match kafka_test::setup::verify_server_ready() {
            Ok(()) => {
                if !args.json {
                    println!("OK");
                }
                break;
            }
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    eprintln!("FAILED after {max_attempts} attempts: {e}");
                    std::process::exit(1);
                }
                if !args.json {
                    print!("retry {attempts}... ");
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    let suite_start = Instant::now();
    let test_timeout = Duration::from_secs(args.timeout_secs);

    let all_results: Vec<TestResult> = if args.sequential {
        // Sequential execution mode (original behavior)
        let mut results = Vec::new();
        let mut current_category = "";

        for test in &tests_to_run {
            if test.category != current_category {
                current_category = test.category;
                if !args.json {
                    print_category_header(current_category);
                }
            }

            let result = run_single_test(test, args.verbose, test_timeout).await;
            if !args.json {
                let status = if result.passed { "PASSED" } else { "FAILED" };
                let duration = format!("({:.2}s)", secs(result.duration_ms));
                println!("  {} - {} {}", test.name, status, duration);
            }
            results.push(result);
        }
        results
    } else {
        // Parallel execution mode
        let (parallel_tests, sequential_tests): (Vec<_>, Vec<_>) =
            tests_to_run.iter().partition(|t| t.parallel_safe);

        if !args.json {
            println!(
                "Running {} tests in parallel (max {}), {} sequentially\n",
                parallel_tests.len(),
                args.concurrency,
                sequential_tests.len()
            );
        }

        // Run parallel-safe tests concurrently with semaphore limiting
        // Use a mutex to coordinate output printing (avoid interleaving)
        let semaphore = Arc::new(Semaphore::new(args.concurrency));
        let print_mutex = Arc::new(Mutex::new(()));
        let json_mode = args.json;

        let parallel_results: Vec<TestResult> = stream::iter(parallel_tests)
            .map(|test: &&TestDef| {
                let sem = semaphore.clone();
                let print_lock = print_mutex.clone();
                async move {
                    let _permit = sem.acquire().await.unwrap();
                    let result = run_single_test(test, false, test_timeout).await;

                    // Print result atomically (one test at a time)
                    if !json_mode {
                        let _lock = print_lock.lock().await;
                        let icon = if result.passed { "✓" } else { "✗" };
                        let duration_str = format!("{:.2}s", secs(result.duration_ms));
                        println!("  {} {} ({})", icon, result.name, duration_str);
                        if let Some(ref e) = result.error {
                            eprintln!("    Error: {e}");
                        }
                    }

                    result
                }
            })
            .buffer_unordered(args.concurrency)
            .collect()
            .await;

        // Run sequential tests one by one
        if !sequential_tests.is_empty() && !args.json {
            println!("\n--- Running sequential tests ---\n");
        }

        let mut sequential_results: Vec<TestResult> = Vec::new();
        for test in sequential_tests {
            let result = run_single_test(test, args.verbose, test_timeout).await;
            if !args.json {
                let status = if result.passed { "PASSED" } else { "FAILED" };
                let duration = format!("({:.2}s)", secs(result.duration_ms));
                println!("  {} - {} {}", test.name, status, duration);
            }
            sequential_results.push(result);
        }

        // Combine results
        parallel_results
            .into_iter()
            .chain(sequential_results)
            .collect()
    };

    // Group results by category
    let mut category_map: std::collections::HashMap<String, Vec<TestResult>> =
        std::collections::HashMap::new();
    for result in all_results {
        category_map
            .entry(result.category.clone())
            .or_default()
            .push(result);
    }

    // Build category results in consistent order
    let categories = unique_categories(tests_to_run.iter().copied());

    let mut category_results: Vec<CategoryResult> = Vec::new();
    for cat in categories {
        if let Some(tests) = category_map.remove(cat) {
            let passed = tests.iter().filter(|t| t.passed).count();
            let failed = tests.iter().filter(|t| !t.passed).count();
            let duration_ms: u64 = tests.iter().map(|t| t.duration_ms).sum();
            category_results.push(CategoryResult {
                name: cat.to_string(),
                passed,
                failed,
                duration_ms,
                tests,
            });
        }
    }

    // Build final result
    let total_passed: usize = category_results.iter().map(|c| c.passed).sum();
    let total_failed: usize = category_results.iter().map(|c| c.failed).sum();

    let suite_result = SuiteResult {
        timestamp: rfc3339_utc(std::time::SystemTime::now()),
        total_passed,
        total_failed,
        total_duration_ms: millis(suite_start.elapsed()),
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

#[cfg(test)]
mod runner_tests {
    use super::*;
    use kafka_test::registry::TestFuture;

    macro_rules! wrap_test {
        ($test:ident) => {
            (|| -> TestFuture { Box::pin($test()) }) as TestFn
        };
    }

    #[expect(clippy::unused_async, reason = "a registered test is an async fn")]
    async fn passes() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    #[expect(clippy::unused_async, reason = "a registered test is an async fn")]
    async fn returns_err() -> Result<(), Box<dyn std::error::Error>> {
        Err("expected failure".into())
    }

    #[expect(clippy::unused_async, reason = "a registered test is an async fn")]
    async fn panics_with_str() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(1, 2, "assertion inside test");
        Ok(())
    }

    #[expect(clippy::unused_async, reason = "a registered test is an async fn")]
    async fn panics_with_static_str() -> Result<(), Box<dyn std::error::Error>> {
        panic!("plain panic");
    }

    async fn hangs() -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }

    fn def(name: &'static str, test_fn: TestFn) -> TestDef {
        TestDef {
            category: "runner",
            name,
            test_fn,
            parallel_safe: true,
        }
    }

    #[tokio::test]
    async fn passing_test_is_passed() {
        let r = run_single_test(
            &def("ok", wrap_test!(passes)),
            false,
            Duration::from_secs(5),
        )
        .await;
        assert!(r.passed);
        assert!(r.error.is_none());
    }

    #[tokio::test]
    async fn err_is_failed_with_message() {
        let r = run_single_test(
            &def("err", wrap_test!(returns_err)),
            false,
            Duration::from_secs(5),
        )
        .await;
        assert!(!r.passed);
        assert_eq!(r.error.as_deref(), Some("expected failure"));
    }

    #[tokio::test]
    async fn panic_is_failed_with_message_and_runner_survives() {
        let r = run_single_test(
            &def("panic", wrap_test!(panics_with_str)),
            false,
            Duration::from_secs(5),
        )
        .await;
        assert!(!r.passed);
        let e = r.error.expect("panic must produce an error message");
        assert!(e.starts_with("panicked: "), "{e}");
        assert!(e.contains("assertion inside test"), "{e}");

        let r = run_single_test(
            &def("panic2", wrap_test!(panics_with_static_str)),
            false,
            Duration::from_secs(5),
        )
        .await;
        assert!(!r.passed);
        assert_eq!(r.error.as_deref(), Some("panicked: plain panic"));

        // The runner keeps going after a panic.
        let r = run_single_test(
            &def("ok", wrap_test!(passes)),
            false,
            Duration::from_secs(5),
        )
        .await;
        assert!(r.passed);
    }

    #[tokio::test]
    async fn hang_is_failed_after_timeout() {
        let start = Instant::now();
        let r = run_single_test(
            &def("hang", wrap_test!(hangs)),
            false,
            Duration::from_millis(200),
        )
        .await;
        assert!(!r.passed);
        assert!(start.elapsed() < Duration::from_secs(10));
        let e = r.error.expect("timeout must produce an error message");
        assert!(e.starts_with("timed out after"), "{e}");
    }

    #[test]
    fn categories_are_unique_and_ordered() {
        let tests = [
            TestDef {
                category: "b",
                ..def("1", wrap_test!(passes))
            },
            TestDef {
                category: "a",
                ..def("2", wrap_test!(passes))
            },
            TestDef {
                category: "b",
                ..def("3", wrap_test!(passes))
            },
        ];
        assert_eq!(unique_categories(tests.iter()), vec!["b", "a"]);
    }

    #[test]
    fn registry_names_are_unique() {
        let tests = kafka_test::all_tests();
        let mut names: Vec<&str> = tests.iter().map(|t| t.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "duplicate test name in registry");
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::rfc3339_utc;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn formats_known_instants_as_rfc3339_utc() {
        for (secs, millis, expected) in [
            (0, 0, "1970-01-01T00:00:00.000Z"),
            (951_782_400, 5, "2000-02-29T00:00:00.005Z"),
            (1_795_129_061, 269, "2026-11-19T22:57:41.269Z"),
            (4_102_444_799, 999, "2099-12-31T23:59:59.999Z"),
        ] {
            let t = UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis);
            assert_eq!(rfc3339_utc(t), expected);
        }
    }
}
