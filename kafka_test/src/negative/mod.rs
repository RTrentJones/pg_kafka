//! Negative E2E tests
//!
//! Tests for expected failure scenarios.

mod expected_failures;

/// The `negative` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["negative";
        expected_failures::test_sasl_password_guc_is_superuser_only: sequential,
        expected_failures::test_connection_refused,
        expected_failures::test_produce_timeout,
        expected_failures::test_invalid_group_id,
        expected_failures::test_duplicate_consumer_join,
    ]
}
