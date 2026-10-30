# Repository Pattern Implementation

**Status:** Active (Phase 11 Complete)
**Last Updated:** 2026-10-29

This document describes the Repository Pattern architecture implemented in pg_kafka to separate storage logic from protocol handling.

## Overview

The codebase has been refactored to use the Repository Pattern, which provides:

1. **Clean Separation of Concerns** - Protocol logic is separate from storage logic
2. **Testability** - Handlers can be unit tested with mock storage implementations
3. **Maintainability** - Clear boundaries between layers make code easier to understand and modify
4. **Security** - Centralized SQL handling with parameterized SPI queries

## Architecture Layers

### 1. Storage Layer ([src/kafka/storage/](../src/kafka/storage/))

The storage layer defines and implements data persistence operations.

**Key Files:**
- [`storage/mod.rs`](../src/kafka/storage/mod.rs) - Defines the `KafkaStore` trait
- [`storage/postgres.rs`](../src/kafka/storage/postgres.rs) - PostgreSQL implementation (`PostgresStore`)
- [`storage/tests.rs`](../src/kafka/storage/tests.rs) - Storage-layer unit tests

**`KafkaStore` Trait (38 methods):**
```rust
pub trait KafkaStore {
    // ===== Topic Operations =====
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)>; // (topic_id, partition_count)
    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>>;

    // ===== Message Operations =====
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64>;
    fn fetch_records(&self, topic_id: i32, partition_id: i32, fetch_offset: i64, max_bytes: i32) -> Result<Vec<FetchedMessage>>;
    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
    fn get_earliest_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
    fn get_offset_for_timestamp(&self, topic_id: i32, partition_id: i32, timestamp_ms: i64) -> Result<Option<(i64, i64)>>;

    // ===== Consumer Offset Operations =====
    fn commit_offset(&self, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> Result<()>;
    fn fetch_offset(&self, group_id: &str, topic_id: i32, partition_id: i32) -> Result<Option<CommittedOffset>>;
    fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>>;

    // ===== Admin Topic Operations (Phase 6) =====
    fn topic_exists(&self, name: &str) -> Result<bool>;
    fn create_topic(&self, name: &str, partition_count: i32) -> Result<i32>;
    fn get_topic_id(&self, name: &str) -> Result<Option<i32>>;
    fn delete_topic(&self, topic_id: i32) -> Result<()>;
    fn get_topic_partition_count(&self, name: &str) -> Result<Option<i32>>;
    fn set_topic_partition_count(&self, name: &str, partition_count: i32) -> Result<()>;

    // ===== Admin Consumer Group Operations (Phase 6) =====
    fn delete_consumer_group_offsets(&self, group_id: &str) -> Result<()>;

    // ===== Idempotent Producer Operations (Phase 9) =====
    fn allocate_producer_id(&self, client_id: Option<&str>, transactional_id: Option<&str>) -> Result<(i64, i16)>;
    fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>>;
    fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16>;
    fn check_and_update_sequence(&self, producer_id: i64, producer_epoch: i16, topic_id: i32, partition_id: i32, base_sequence: i32, record_count: i32) -> Result<bool>;
    fn record_producer_sequence(&self, producer_id: i64, topic_id: i32, partition_id: i32, last_sequence: i32) -> Result<()>;

    // ===== Transaction Operations (Phase 10) =====
    fn get_or_create_transactional_producer(&self, transactional_id: &str, transaction_timeout_ms: i32, client_id: Option<&str>) -> Result<(i64, i16)>;
    fn begin_transaction(&self, transactional_id: &str, producer_id: i64, producer_epoch: i16) -> Result<()>;
    fn begin_or_continue_transaction(&self, transactional_id: &str, producer_id: i64, producer_epoch: i16) -> Result<()>;
    fn validate_transaction(&self, transactional_id: &str, producer_id: i64, producer_epoch: i16) -> Result<()>;
    fn insert_transactional_records(&self, topic_id: i32, partition_id: i32, records: &[Record], producer_id: i64, producer_epoch: i16) -> Result<i64>;
    fn store_txn_pending_offset(&self, transactional_id: &str, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> Result<()>;
    fn commit_transaction(&self, transactional_id: &str, producer_id: i64, producer_epoch: i16) -> Result<()>;
    fn abort_transaction(&self, transactional_id: &str, producer_id: i64, producer_epoch: i16) -> Result<()>;
    fn get_transaction_state(&self, transactional_id: &str) -> Result<Option<TransactionState>>;
    fn fetch_records_with_isolation(&self, topic_id: i32, partition_id: i32, fetch_offset: i64, max_bytes: i32, isolation_level: IsolationLevel) -> Result<Vec<FetchedMessage>>;
    fn get_last_stable_offset(&self, topic_id: i32, partition_id: i32) -> Result<i64>;
    fn abort_timed_out_transactions(&self, timeout: Duration) -> Result<Vec<String>>;
    fn cleanup_aborted_messages(&self, older_than: Duration) -> Result<u64>;

    // ===== Config / log management =====
    fn get_topic_retention_ms(&self, topic_id: i32) -> Result<Option<i64>>;
    fn set_topic_retention_ms(&self, topic_id: i32, retention_ms: Option<i64>) -> Result<()>;
    fn delete_records_before(&self, topic_id: i32, partition_id: i32, before_offset: i64) -> Result<i64>;
}
```

**Storage-Specific Types:**
- `TopicMetadata` - Internal representation of topic metadata
- `FetchedMessage` - Messages retrieved from storage before protocol conversion
- `CommittedOffset` - Consumer group offset information
- `TransactionState` - Transaction state machine values stored in `kafka.transactions.state`
- `IsolationLevel` - `ReadUncommitted` / `ReadCommitted` for fetches

**Implementations:**
- `PostgresStore` (`storage/postgres.rs`) - production implementation over pgrx SPI. All of its SQL lives here.
  It also has inherent (non-trait) maintenance methods such as `run_retention_sweep`, called by the worker
  and by the `pg_kafka_run_retention_sweep()` SQL function.
- `ShadowStore<S: KafkaStore>` (`shadow/store.rs`) - a decorator that implements `KafkaStore` by delegating
  to an inner store and writing the shadow-mode outbox. The worker uses `ShadowStore<PostgresStore>`.
- `MockKafkaStore` (`testing/mocks.rs`) - mockall-generated test double for handler unit tests.
- `MockStore` (`handler_context.rs`, test-only) - minimal stub used by the `HandlerContext` tests.

**Implementation Details:**
- Uses pgrx SPI (`Spi::connect` / `Spi::connect_mut`) for database operations
- User-supplied values are passed as SPI arguments to parameterized queries (`$1`, `$2`, ...), never spliced
  into SQL text

### 2. Handler Layer ([src/kafka/handlers/](../src/kafka/handlers/))

The handler layer contains pure Kafka protocol logic. It is a module directory with one `pub fn handle_*` per
supported API (26 in total); `handlers/tests.rs` holds 80 of the module's 107 unit tests.

**Key Handlers:**
```rust
// No storage needed - returns static protocol info
pub fn handle_api_versions() -> ApiVersionsResponse

// Every other handler takes a HandlerContext (src/kafka/handler_context.rs) whose
// `store: &dyn KafkaStore` field is the injected storage backend, alongside the
// group coordinator, broker metadata, default partition count, and outbound compression.
pub fn handle_metadata(ctx: &HandlerContext, requested_topics: Option<Vec<String>>, allow_auto_topic_creation: bool) -> Result<MetadataResponse>
pub fn handle_produce(ctx: &HandlerContext, topic_data: Vec<TopicProduceData>, producer_metadata: Option<&ProducerMetadata>, transactional_id: Option<&str>) -> Result<ProduceResponse>
pub fn handle_fetch(ctx: &HandlerContext, topic_data: Vec<TopicFetchData>, isolation_level: IsolationLevel) -> Result<FetchResponse>
pub fn handle_offset_commit(ctx: &HandlerContext, ...) -> Result<OffsetCommitResponse>
pub fn handle_offset_fetch(ctx: &HandlerContext, ...) -> Result<OffsetFetchResponse>
```

**Handler Responsibilities:**
- Parse Kafka protocol types
- Coordinate storage operations
- Build Kafka protocol responses
- Handle protocol-level errors

**What Handlers DON'T Know:**
- SQL syntax
- Database schema
- SPI implementation details
- Transaction management

### 3. Coordination Layer ([src/worker.rs](../src/worker.rs))

The worker coordinates requests, manages transactions, and composes the other layers.

**Key Responsibilities:**
```rust
pub fn process_request(
    request: KafkaRequest,
    coordinator: &Arc<GroupCoordinator>,
    broker_metadata: &BrokerMetadata,
    default_partitions: i32,
    notify_tx: &Sender<InternalNotification>,
    compression: Compression,
    shadow_store: &Arc<ShadowStore<PostgresStore>>,
) {
    // 1. Build one HandlerContext over the ShadowStore (which wraps PostgresStore)
    // 2. Match the request variant and call the handler through the
    //    dispatch_response / dispatch_infallible / dispatch_produce helpers (src/kafka/dispatch.rs)
    // 3. Map handler errors to Kafka error codes and send the response
}
```

**Transaction Management:**
- Transaction boundaries are EXPLICIT in worker.rs
- `execute_request_in_transaction` runs each request inside `BackgroundWorker::transaction()` plus an
  internal subtransaction guard, so a Postgres ERROR or panic rolls back that request instead of killing the worker
- Responses are buffered and released only after the transaction commits (the RB-1 response-after-commit barrier)
- Storage assumes it runs within a transaction
- Handlers have no knowledge of transactions

## Code Reduction

The original refactoring significantly reduced code duplication. The figures below are a historical
record of that refactor; the files have since grown and been split (see File Reference).

| Module | Before | After | Reduction |
|--------|--------|-------|-----------|
| worker.rs (Metadata) | 106 lines | 60 lines | 43% |
| worker.rs (Produce) | 144 lines | 60 lines | 58% |
| worker.rs (Fetch) | 220 lines | 38 lines | **83%** |
| worker.rs (OffsetCommit) | 168 lines | 43 lines | 74% |
| worker.rs (OffsetFetch) | 222 lines | 43 lines | 81% |
| **Total Removed** | **~618 lines** | - | - |

At the time, the removed code was consolidated into:
- 407 lines in `storage/postgres.rs` (reusable storage logic)
- ~550 lines in `handlers.rs` (testable protocol logic; now the `handlers/` module directory)

## Testing Strategy

### Unit Testing Handlers

Handlers are unit tested with the mockall-generated `MockKafkaStore` (`src/testing/mocks.rs`):

```rust
#[test]
fn test_handle_produce_success() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1))); // (topic_id, partition_count)
    mock.expect_insert_records()
        .returning(|_, _, _| Ok(0)); // base offset

    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();
    assert_eq!(response.responses.len(), 1);
}
```

### Integration Testing

`PostgresStore` is not tested with `#[pg_test]`: pg_kafka registers `PGC_POSTMASTER` GUCs, which
`cargo pgrx test` cannot load (see [PGRX_TESTING_GUIDE.md](PGRX_TESTING_GUIDE.md)). It is excluded from unit
coverage in `codecov.yml` and covered by the E2E suite instead, and the `spi-e2e-guard` CI job requires a
`kafka_test/src/` change alongside any change to it.

### E2E Testing

The E2E tests in `kafka_test/` verify the complete integration against a running PostgreSQL with the extension
preloaded.

## Error Handling

**Storage-related `KafkaError` variants:**
```rust
pub enum KafkaError {
    // ... other variants

    /// Database operation failed
    #[error("Database error: {message}")]
    Database { message: String },

    /// Internal error with Kafka error code
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<pgrx::spi::SpiError> for KafkaError {
    fn from(err: pgrx::spi::SpiError) -> Self {
        KafkaError::Database {
            message: format!("SPI error: {}", err),
        }
    }
}
```

This enables the `?` operator throughout the storage layer. `PostgresStore` methods typically wrap failures
as `KafkaError::Internal("<method> failed: ...")`. Both variants map to `UNKNOWN_SERVER_ERROR` on the wire.

## Migration Benefits

### Before (Monolithic)
```rust
// worker.rs - everything mixed together
pub fn process_request(request: KafkaRequest) {
    match request {
        Fetch { ... } => {
            // 220 lines of:
            // - SQL queries inline
            // - Protocol logic mixed with storage
            // - Hard to test
            // - SQL injection risks scattered
        }
    }
}
```

### After (Layered)
```rust
// worker.rs - clean coordination (abridged)
pub fn process_request(request: KafkaRequest, /* coordinator, broker, ..., shadow_store */) {
    let store: &dyn KafkaStore = shadow_store.as_ref();
    let ctx = HandlerContext::new(store, coordinator, broker_metadata, default_partitions, compression);
    match request {
        KafkaRequest::Fetch { topic_data, isolation_level, response_tx, .. } => {
            dispatch_response(
                "Fetch",
                response_tx,
                || handlers::handle_fetch(&ctx, topic_data, isolation),
                /* wrap success */, /* build error response */,
            );
        }
        // ...
    }
}

// handlers/fetch.rs - pure protocol logic
pub fn handle_fetch(ctx: &HandlerContext, topic_data: Vec<TopicFetchData>, isolation_level: IsolationLevel) -> Result<FetchResponse> {
    // Focus on Kafka protocol
    // No SQL knowledge needed
}

// storage/postgres.rs - centralized SQL
impl KafkaStore for PostgresStore {
    fn fetch_records(...) -> Result<Vec<FetchedMessage>> {
        // All SQL in one place
        // Parameterized queries
        // Reusable
    }
}
```

## Security Improvements

**SQL Injection Mitigation:**
- All SQL is centralized in the storage layer
- User-supplied values (topic names, group ids, transactional ids, payloads) are bound as SPI arguments to
  parameterized queries rather than spliced into SQL text
- Easier to audit (one file vs scattered throughout)

**Example:**
```rust
// storage/postgres.rs
let table = client.select(
    "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
    Some(1),
    &[transactional_id.into()],
)?;
```

## Future Work

### 1. Parameterized Queries (done)
`PostgresStore` now binds every user-supplied value as an SPI argument (see Security Improvements), so the
earlier string-escaping TODO is resolved.

### 2. Additional Storage Implementations
The trait-based design allows alternative storage backends. `ShadowStore<S>` already uses it as a decorator
over `PostgresStore`; hypothetical further backends would look like:
```rust
pub struct FileSystemStore;
pub struct S3Store;
pub struct RedisStore;

// All implement KafkaStore trait
```

### 3. Handler Unit Tests (done)
Handlers have unit tests using `MockKafkaStore` covering error paths, edge cases, and protocol compliance,
with no database required. New handlers should follow the same pattern.

### 4. Performance Optimizations
- Connection pooling in storage layer
- Batch operations
- Query optimization
- Caching layer (between handlers and storage)

## File Reference

**Core Files:**
Line counts are approximate and reflect the current tree.

- [`src/kafka/storage/mod.rs`](../src/kafka/storage/mod.rs) - Storage trait definition and storage types (~790 lines)
- [`src/kafka/storage/postgres.rs`](../src/kafka/storage/postgres.rs) - PostgreSQL implementation (~2,070 lines)
- [`src/kafka/handlers/`](../src/kafka/handlers/) - Protocol handlers, one file per API family, plus `tests.rs`
- [`src/kafka/handler_context.rs`](../src/kafka/handler_context.rs) - `HandlerContext` passed to every handler
- [`src/kafka/dispatch.rs`](../src/kafka/dispatch.rs) - Response dispatch helpers used by the worker
- [`src/kafka/shadow/store.rs`](../src/kafka/shadow/store.rs) - `ShadowStore` decorator
- [`src/testing/mocks.rs`](../src/testing/mocks.rs) - `MockKafkaStore`
- [`src/kafka/error.rs`](../src/kafka/error.rs) - Error types (`Database`, `Internal`, and protocol variants)
- [`src/worker.rs`](../src/worker.rs) - Request coordinator and background-worker loop (~2,000 lines)

**Related Files:**
- [`src/kafka/mod.rs`](../src/kafka/mod.rs) - Exports for `handlers` and `storage`
- [`src/kafka/constants.rs`](../src/kafka/constants.rs) - Protocol constants and error codes
- [`src/kafka/protocol/`](../src/kafka/protocol/) - Wire decoding/encoding
- [`src/kafka/messages.rs`](../src/kafka/messages.rs) - Request/response types
- [`src/kafka/response_builders.rs`](../src/kafka/response_builders.rs) - Response construction

## Summary

The Repository Pattern refactoring successfully:
- ✅ Separates storage from protocol logic
- ✅ Reduces code duplication by ~618 lines
- ✅ Improves testability with dependency injection
- ✅ Centralizes SQL (parameterized) for better security
- ✅ Maintains all existing functionality
- ✅ Compiles without warnings
- ✅ Passes existing tests

The architecture is now more maintainable, testable, and secure while maintaining full backward compatibility.

**Test Coverage:**
- Handler tests: 80 unit tests in `src/kafka/handlers/tests.rs` (107 across the `handlers/` module)
- Storage tests: 53 unit tests in `src/kafka/storage/tests.rs`
- Mock infrastructure: `MockKafkaStore` in `src/testing/mocks.rs`
