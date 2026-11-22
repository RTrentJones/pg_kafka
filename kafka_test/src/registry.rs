//! The test registry: every E2E test with its category and whether it may run
//! concurrently with others.
//!
//! Each category module lists its tests once, in its `tests()` function, with
//! [`test_defs!`](crate::test_defs); [`crate::all_tests`] joins the categories. The runner
//! keeps registry order: categories print in it, and the sequential tests run in it, so a
//! category whose tests share setup (a setup test first, a teardown test last) lists them
//! in that order.

use crate::common::TestResult;
use std::future::Future;
use std::pin::Pin;

/// The future one test returns.
pub type TestFuture = Pin<Box<dyn Future<Output = TestResult> + Send>>;

/// A test, type-erased so the registry can hold every test in one list.
pub type TestFn = fn() -> TestFuture;

/// One registered test.
pub struct TestDef {
    pub category: &'static str,
    pub name: &'static str,
    pub test_fn: TestFn,
    /// Whether the test may run concurrently with other tests. A test that changes
    /// server-wide state (GUCs, listeners, the worker, global sweeps) or that measures
    /// timing is sequential.
    pub parallel_safe: bool,
}

/// The function name at the end of a stringified path (`a::b::test_x` -> `test_x`).
pub fn test_name(path: &'static str) -> &'static str {
    path.rsplit("::").next().unwrap_or(path).trim()
}

/// The [`TestDef`]s of one category, in run order.
///
/// ```ignore
/// test_defs!["admin";
///     topics::test_create_topic,
///     topics::test_delete_group_non_empty: sequential, // shares group state
/// ]
/// ```
///
/// Each entry is the path of an `async fn() -> TestResult`; the test's name is the
/// function's name. `: sequential` marks a test that must not run concurrently.
#[macro_export]
macro_rules! test_defs {
    (@parallel_safe) => {
        true
    };
    (@parallel_safe sequential) => {
        false
    };
    ($category:literal; $($test:path $(: $mode:ident)?),* $(,)?) => {
        vec![$($crate::registry::TestDef {
            category: $category,
            name: $crate::registry::test_name(stringify!($test)),
            test_fn: || -> $crate::registry::TestFuture { Box::pin($test()) },
            parallel_safe: $crate::test_defs!(@parallel_safe $($mode)?),
        }),*]
    };
}

#[cfg(test)]
mod tests {
    use super::test_name;

    #[test]
    fn test_name_is_the_last_path_segment() {
        assert_eq!(test_name("test_x"), "test_x");
        assert_eq!(test_name("basic::test_x"), "test_x");
        assert_eq!(test_name("crate :: a :: test_x"), "test_x");
    }
}
