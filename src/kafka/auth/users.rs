//! SQL functions that manage Kafka users in `kafka.sasl_credentials`.
//!
//! The table holds one SCRAM credential per user and mechanism (salt, iteration count,
//! StoredKey, ServerKey); never a password. Only the extension owner (superusers) can
//! read or write it, plus `pgkafka_worker`, which may only SELECT it; `pgkafka_reader`
//! sees the `kafka.sasl_users` view (user, mechanism, iterations, timestamps) instead.
//!
//! The management functions are SECURITY INVOKER: they work for a role that may write
//! `kafka.sasl_credentials` (by default only superusers) and fail with "permission
//! denied" for everyone else.
//!
//! `pg_kafka_create_user` / `pg_kafka_alter_user` take the password as an argument, so
//! it is visible in that session's `pg_stat_activity.query` and in the server log if
//! statement logging is on; `pg_kafka_set_user_verifier` takes a pre-computed verifier
//! instead (for SCRAM-SHA-256 the same `SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`
//! format libpq's `PQencryptPasswordConn` produces), so the plaintext never reaches the
//! server.

use super::scram::{ScramCredential, ScramMechanism, MAX_ITERATIONS, MIN_ITERATIONS};
use pgrx::prelude::*;

fn check_user_args(username: &str, iterations: i32) -> u32 {
    if username.is_empty() || username.len() > 255 {
        pgrx::error!("Kafka user name must be 1 to 255 bytes long");
    }
    if !(MIN_ITERATIONS as i32..=MAX_ITERATIONS as i32).contains(&iterations) {
        pgrx::error!(
            "iterations must be between {} and {}",
            MIN_ITERATIONS,
            MAX_ITERATIONS
        );
    }
    iterations as u32
}

fn user_exists(username: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM kafka.sasl_credentials WHERE username = $1)",
        &[username.into()],
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
    .unwrap_or(false)
}

fn upsert_credential(username: &str, cred: &ScramCredential) {
    Spi::run_with_args(
        "INSERT INTO kafka.sasl_credentials \
             (username, mechanism, salt, iterations, stored_key, server_key) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (username, mechanism) DO UPDATE SET \
             salt = EXCLUDED.salt, iterations = EXCLUDED.iterations, \
             stored_key = EXCLUDED.stored_key, server_key = EXCLUDED.server_key, \
             updated_at = now()",
        &[
            username.into(),
            cred.mechanism.name().into(),
            cred.salt.clone().into(),
            (cred.iterations as i32).into(),
            cred.stored_key.clone().into(),
            cred.server_key.clone().into(),
        ],
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e));
}

fn set_password(username: &str, password: &str, iterations: u32) {
    if password.is_empty() {
        pgrx::error!("password must not be empty");
    }
    for m in ScramMechanism::STORED {
        upsert_credential(
            username,
            &ScramCredential::generate(m, password.as_bytes(), iterations),
        );
    }
}

/// Create a Kafka user with SCRAM-SHA-256 and SCRAM-SHA-512 credentials for `password`
/// (also used to check SASL/PLAIN). Fails if the user exists.
#[pg_extern(volatile)]
fn pg_kafka_create_user(username: &str, password: &str, iterations: default!(i32, 4096)) {
    let iterations = check_user_args(username, iterations);
    if user_exists(username) {
        pgrx::error!("Kafka user \"{}\" already exists", username);
    }
    set_password(username, password, iterations);
}

/// Replace a Kafka user's password (both SCRAM credentials). Fails if the user does not
/// exist.
#[pg_extern(volatile)]
fn pg_kafka_alter_user(username: &str, password: &str, iterations: default!(i32, 4096)) {
    let iterations = check_user_args(username, iterations);
    if !user_exists(username) {
        pgrx::error!("Kafka user \"{}\" does not exist", username);
    }
    set_password(username, password, iterations);
}

/// Install one pre-computed SCRAM verifier
/// (`SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`, base64 fields; or
/// `SCRAM-SHA-512$...`), creating the user if needed. Only that mechanism is set.
#[pg_extern(volatile)]
fn pg_kafka_set_user_verifier(username: &str, verifier: &str) {
    let cred = ScramCredential::from_verifier(verifier)
        .unwrap_or_else(|e| pgrx::error!("invalid SCRAM verifier: {}", e));
    check_user_args(username, cred.iterations as i32);
    upsert_credential(username, &cred);
}

/// Drop a Kafka user (all mechanisms). Returns false if it did not exist. Connections
/// already authenticated as the user stay open until they disconnect.
#[pg_extern(volatile)]
fn pg_kafka_drop_user(username: &str) -> bool {
    Spi::get_one_with_args::<i64>(
        "WITH d AS (DELETE FROM kafka.sasl_credentials WHERE username = $1 RETURNING 1) \
         SELECT count(*) FROM d",
        &[username.into()],
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
    .unwrap_or(0)
        > 0
}
