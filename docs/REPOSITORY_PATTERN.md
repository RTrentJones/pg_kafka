# Repository Pattern Implementation

This document describes the Repository Pattern architecture implemented in pg_kafka to separate storage logic from protocol handling.

## Overview

The codebase has been refactored to use the Repository Pattern, which provides:

1. **Clean Separation of Concerns** - Protocol logic is separate from storage logic
2. **Testability** - Handlers can be unit tested with mock storage implementations
3. **Maintainability** - Clear boundaries between layers make code easier to understand and modify
4. **Security** - Centralized SQL handling with proper escaping/sanitization

## Architecture Layers

### 1. Storage Layer ([src/kafka/storage/](../src/kafka/storage/))

The storage layer defines and implements data persistence operations.

**Key Files:**
- [`storage/mod.rs`](../src/kafka/storage/mod.rs) - Defines the `KafkaStore` trait
- [`storage/postgres.rs`](../src/kafka/storage/postgres.rs) - PostgreSQL implementation

**`KafkaStore` Trait:**
```rust
pub trait KafkaStore {
    // Topic operations
    fn get_or_create_topic(&self, name: &str) -> Result<i32>;
    fn get_topic_metadata(&self, names: Option<&[String]>) -> Result<Vec<TopicMetadata>>;

    // Message operations
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64>;
    fn fetch_records(&self, topic_id: i32, partition_id: i32, fetch_offset: i64, max_bytes: i32) -> Result<Vec<FetchedMessage>>;
    fn get_high_watermark(&self, topic_id: i32, partition_id: i32) -> Result<i64>;

    // Consumer offset operations
    fn commit_offset(&self, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> Result<()>;
    fn fetch_offset(&self, group_id: &str, topic_id: i32, partition_id: i32) -> Result<Option<CommittedOffset>>;
    fn fetch_all_offsets(&self, group_id: &str) -> Result<Vec<(String, i32, CommittedOffset)>>;
}
```

**Storage-Specific Types:**
- `TopicMetadata` - Internal representation of topic metadata
- `FetchedMessage` - Messages retrieved from storage before protocol conversion
- `CommittedOffset` - Consumer group offset information

**Implementation Details:**
- All SQL queries are in `PostgresStore`
- Uses pgrx SPI for database operations
- SQL injection mitigation via `.replace("'", "''")`
- TODO: Prepared statements when pgrx adds support

### 2. Handler Layer ([src/kafka/handlers.rs](../src/kafka/handlers.rs))

The handler layer contains pure Kafka protocol logic.

**Key Handlers:**
```rust
// No storage needed - returns static protocol info
pub fn handle_api_versions() -> ApiVersionsResponse

// Handlers that use storage via dependency injection
pub fn handle_metadata(store: &impl KafkaStore, ...) -> Result<MetadataResponse>
pub fn handle_produce(store: &impl KafkaStore, ...) -> Result<ProduceResponse>
pub fn handle_fetch(store: &impl KafkaStore, ...) -> Result<FetchResponse>
pub fn handle_offset_commit(store: &impl KafkaStore, ...) -> Result<OffsetCommitResponse>
pub fn handle_offset_fetch(store: &impl KafkaStore, ...) -> Result<OffsetFetchResponse>
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
pub fn process_request(request: KafkaRequest) {
    // 1. Extract request parameters
    // 2. Instantiate PostgresStore (within transaction context)
    // 3. Call appropriate handler
    // 4. Handle errors and send response
}
```

**Transaction Management:**
- Transaction boundaries are EXPLICIT in worker.rs
- `BackgroundWorker::transaction()` creates transaction scope
- Storage assumes it runs within a transaction
- Handlers have no knowledge of transactions

## Code Reduction

The refactoring significantly reduced code duplication:

| Module | Before | After | Reduction |
|--------|--------|-------|-----------|
| worker.rs (Metadata) | 106 lines | 60 lines | 43% |
| worker.rs (Produce) | 144 lines | 60 lines | 58% |
| worker.rs (Fetch) | 220 lines | 38 lines | **83%** |
| worker.rs (OffsetCommit) | 168 lines | 43 lines | 74% |
| worker.rs (OffsetFetch) | 222 lines | 43 lines | 81% |
| **Total Removed** | **~618 lines** | - | - |

The removed code was consolidated into:
- 407 lines in `storage/postgres.rs` (reusable storage logic)
- ~550 lines in `handlers.rs` (testable protocol logic)

## Testing Strategy

### Unit Testing Handlers

Handlers can now be unit tested with a `MockStore`:

```rust
struct MockStore {
    topics: HashMap<String, i32>,
    messages: Vec<FetchedMessage>,
}

impl KafkaStore for MockStore {
    fn get_or_create_topic(&self, name: &str) -> Result<i32> {
        Ok(*self.topics.get(name).unwrap_or(&1))
    }
    // ... implement other methods
}

#[test]
fn test_produce_handler() {
    let mock = MockStore::new();
    let response = handlers::handle_produce(&mock, topic_data)?;
    assert_eq!(response.responses.len(), 1);
}
```

### Integration Testing

The storage layer can be tested independently:

```rust
#[pg_test]
fn test_postgres_store_insert() {
    let store = PostgresStore::new();
    let topic_id = store.get_or_create_topic("test-topic")?;
    let base_offset = store.insert_records(topic_id, 0, &records)?;
    assert_eq!(base_offset, 0);
}
```

### E2E Testing

The existing E2E tests in `kafka_test/` verify the complete integration works correctly.

## Error Handling

**`KafkaError::Internal` Variant:**
```rust
pub enum KafkaError {
    // ... existing variants

    /// Internal storage/database error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<pgrx::spi::SpiError> for KafkaError {
    fn from(err: pgrx::spi::SpiError) -> Self {
        KafkaError::Internal(format!("Database error: {}", err))
    }
}
```

This enables the `?` operator throughout the storage layer.

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
// worker.rs - clean coordination
pub fn process_request(request: KafkaRequest) {
    match request {
        Fetch { topic_data, ... } => {
            let store = PostgresStore::new();
            let response = handlers::handle_fetch(&store, topic_data)?;
            // Send response (38 lines total)
        }
    }
}

// handlers.rs - pure protocol logic
pub fn handle_fetch(store: &impl KafkaStore, topic_data: Vec<TopicFetchData>) -> Result<FetchResponse> {
    // Focus on Kafka protocol
    // No SQL knowledge needed
}

// storage/postgres.rs - centralized SQL
impl KafkaStore for PostgresStore {
    fn fetch_records(...) -> Result<Vec<FetchedMessage>> {
        // All SQL in one place
        // Proper escaping
        // Reusable
    }
}
```

## Security Improvements

**SQL Injection Mitigation:**
- All user input escaping centralized in storage layer
- Uses `.replace("'", "''")` for dynamic SQL
- Clear TODO markers for prepared statement migration
- Easier to audit (one file vs scattered throughout)

**Example:**
```rust
// storage/postgres.rs
fn commit_offset(&self, group_id: &str, ...) -> Result<()> {
    // SQL injection mitigation: escape single quotes
    // TODO: Use prepared statements when pgrx adds support
    let group_id_escaped = group_id.replace("'", "''");
    // ... safe SQL construction
}
```

## Future Work

### 1. Prepared Statements
When pgrx adds support for prepared statements, update `PostgresStore` to use them:
```rust
// TODO: Replace string escaping with prepared statements
Spi::get_one_with_args::<i32>(
    "SELECT id FROM kafka.topics WHERE name = $1",
    &[topic_name.into()],
)
```

### 2. Additional Storage Implementations
The trait-based design allows alternative storage backends:
```rust
pub struct FileSystemStore;
pub struct S3Store;
pub struct RedisStore;

// All implement KafkaStore trait
```

### 3. Handler Unit Tests
Add comprehensive unit tests for handlers using `MockStore`:
- Test error paths
- Test edge cases
- Test protocol compliance
- No database required for testing

### 4. Performance Optimizations
- Connection pooling in storage layer
- Batch operations
- Query optimization
- Caching layer (between handlers and storage)

## File Reference

**Core Files:**
- [`src/kafka/storage/mod.rs`](../src/kafka/storage/mod.rs) - Storage trait definition (187 lines)
- [`src/kafka/storage/postgres.rs`](../src/kafka/storage/postgres.rs) - PostgreSQL implementation (407 lines)
- [`src/kafka/handlers.rs`](../src/kafka/handlers.rs) - Protocol handlers (~550 lines)
- [`src/kafka/error.rs`](../src/kafka/error.rs) - Error types with `Internal` variant
- [`src/worker.rs`](../src/worker.rs) - Request coordinator (simplified from 1159 to ~900 lines)

**Modified Files:**
- [`src/kafka/mod.rs`](../src/kafka/mod.rs) - Added exports for `handlers` and `storage`
- [`src/kafka/constants.rs`](../src/kafka/constants.rs) - No changes
- [`src/kafka/protocol.rs`](../src/kafka/protocol.rs) - No changes
- [`src/kafka/messages.rs`](../src/kafka/messages.rs) - No changes
- [`src/kafka/response_builders.rs`](../src/kafka/response_builders.rs) - No changes

## Summary

The Repository Pattern refactoring successfully:
- ✅ Separates storage from protocol logic
- ✅ Reduces code duplication by ~618 lines
- ✅ Improves testability with dependency injection
- ✅ Centralizes SQL for better security
- ✅ Maintains all existing functionality
- ✅ Compiles without warnings
- ✅ Passes existing tests

The architecture is now more maintainable, testable, and secure while maintaining full backward compatibility.
