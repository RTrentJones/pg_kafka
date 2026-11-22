//! Handler unit tests: each sets expectations on a `MockKafkaStore` and checks the
//! response a handler builds from them, so protocol logic is tested without a database.
//! Tests that cover one helper of a handler live next to it in that handler's file.

mod configs;
mod error_paths;
mod fetch;
mod idempotence;
mod metadata;
mod offsets;
mod produce;
mod regressions;
mod transaction;
