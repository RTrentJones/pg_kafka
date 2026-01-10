# Testing pgrx Extensions: Best Practices

This guide documents the testing strategy for pg_kafka and provides patterns for testing pgrx extensions that use PGC_POSTMASTER GUCs and background workers.

## The Challenge

pgrx extensions face unique testing challenges:

1. **`#[pg_test]` requires running Postgres** - Tests execute within a Postgres backend
2. **PGC_POSTMASTER GUCs must be set before Postgres starts** - Via `shared_preload_libraries`
3. **`cargo pgrx test` creates extensions AFTER startup** - Causing fatal errors for extensions with postmaster-level GUCs
4. **SPI calls require database context** - Cannot be called from unit tests

### Why `cargo pgrx test` Doesn't Work for pg_kafka

pg_kafka uses `PGC_POSTMASTER` GUC variables (defined in `src/config.rs`) that control:
- TCP port binding (`pg_kafka.port`)
- Network interface binding (`pg_kafka.host`)
- Background worker registration

When `cargo pgrx test` runs:
1. Postgres starts without `pg_kafka` in `shared_preload_libraries`
2. The test framework tries to `CREATE EXTENSION pg_kafka`
3. `_PG_init()` attempts to register GUCs and background worker
4. **FATAL ERROR**: "cannot create PGC_POSTMASTER variables after startup"

## The Solution: Repository Pattern + E2E Testing

We use a three-tier testing strategy that works around these constraints:

### Tier 1: Unit Tests (No Database Required)

**Location:** `src/kafka/handlers/tests.rs`, `src/kafka/storage/tests.rs`, etc.

**Key Pattern:** Dependency injection via the `KafkaStore` trait.

```rust
// Define the trait (src/kafka/storage/mod.rs)
pub trait KafkaStore {
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)>;
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64>;
    fn fetch_records(&self, ...) -> Result<Vec<FetchedMessage>>;
    // ... more methods
}

// Production implementation (SPI-dependent)
pub struct PostgresStore;
impl KafkaStore for PostgresStore {
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)> {
        Spi::connect(|client| { ... })
    }
}

// Test implementation (no database needed)
mock! {
    pub KafkaStore {}
    impl KafkaStore for KafkaStore {
        fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)>;
        fn insert_records(&self, ...) -> Result<i64>;
        // ... mock all methods
    }
}
```

**Handler Testing Pattern:**

```rust
// Handler accepts &dyn KafkaStore for testability
pub fn handle_produce(
    store: &dyn KafkaStore,
    topic_data: Vec<TopicProduceData>,
    acks: i16,
    timeout_ms: Option<i32>,
    compression_codec: Option<&CompressionCodec>,
) -> Result<ProduceResponse> {
    // Business logic here - testable without database
}

// Test with mock
#[test]
fn test_handle_produce_success() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1)));
    mock.expect_insert_records()
        .returning(|_, _, _| Ok(0));

    let response = handle_produce(&mock, topic_data, 1, None, None).unwrap();
    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);
}
```

**Run unit tests:**
```bash
cargo test --features pg14
```

### Tier 2: Protocol Tests (No Database Required)

**Location:** `tests/encoding_tests.rs`, `tests/protocol_tests.rs`, `tests/property_tests.rs`

These test wire protocol encoding/decoding without database interaction:

```rust
#[test]
fn test_encode_decode_roundtrip() {
    let original = create_test_response();
    let encoded = encode_response(original.clone()).unwrap();
    let decoded = decode_response(&encoded).unwrap();
    assert_eq!(original, decoded);
}
```

### Tier 3: E2E Tests (Full Integration)

**Location:** `kafka_test/src/`

These test the complete system with a running Postgres instance:

```rust
pub async fn test_producer() -> TestResult {
    // 1. SETUP: Create context and unique resources
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("producer").await;

    // 2. ACTION: Use real rdkafka client
    let producer = create_producer()?;
    producer.send(FutureRecord::to(&topic).payload("test"), Duration::from_secs(5)).await?;

    // 3. VERIFY: Check database state
    let count = ctx.db().query_one(
        "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
        &[&topic]
    ).await?.get::<_, i64>(0);
    assert_eq!(count, 1);

    // 4. CLEANUP: Automatic via TestContext drop
    Ok(())
}
```

**Run E2E tests:**
```bash
cargo pgrx start pg14
cd kafka_test && cargo run --release
```

## Test Organization Summary

| Layer | Test Approach | Coverage Source | Speed |
|-------|--------------|-----------------|-------|
| Handlers | Unit tests with MockKafkaStore | `cargo llvm-cov` | Fast (~2s) |
| Protocol Encoding | Unit tests | `cargo llvm-cov` | Fast |
| Response Builders | Unit tests | `cargo llvm-cov` | Fast |
| PostgresStore | E2E tests only | N/A (excluded) | Slow (~30s) |
| Worker/Listener | E2E tests only | N/A (excluded) | Slow |

## Adding New Features Checklist

When adding new functionality to pg_kafka:

1. **Extract pure logic into testable functions**
   - Separate business logic from SPI calls
   - Use `&dyn KafkaStore` parameter for database operations

2. **Add handler unit tests with MockKafkaStore**
   ```rust
   #[test]
   fn test_new_handler_success() {
       let mut mock = MockKafkaStore::new();
       mock.expect_new_method().returning(|...| Ok(...));

       let result = handle_new_api(&mock, ...).unwrap();
       assert!(result.is_valid());
   }
   ```

3. **Add protocol tests for encoding/decoding**
   ```rust
   #[test]
   fn test_new_response_encoding() {
       let response = build_new_response(...);
       let encoded = encode_response(response).unwrap();
       assert!(encoded.len() > 0);
   }
   ```

4. **Add E2E test for integration verification**
   ```rust
   pub async fn test_new_feature() -> TestResult {
       let ctx = TestContext::new().await?;
       // Test with real Kafka client
       // Verify database state
       Ok(())
   }
   ```

5. **Verify coverage meets thresholds**
   ```bash
   cargo llvm-cov --lib --features pg14 --summary-only
   ```

## Code That Cannot Be Unit Tested

Some code paths cannot be covered by unit tests due to pgrx/SPI dependencies. These are excluded from coverage requirements and tested via E2E:

| File | Reason | Covered By |
|------|--------|------------|
| `src/kafka/storage/postgres.rs` | SPI calls | E2E tests |
| `src/worker.rs` | Main loop, SPI | E2E tests |
| `src/kafka/listener.rs` | Async runtime, tokio | E2E tests |
| `src/config.rs` | GUC loading | E2E tests |
| `src/lib.rs` | `_PG_init` hook | E2E tests |

## Test-Safe Logging

The codebase uses conditional logging macros that work in both test and production:

```rust
// Production logging - uses pgrx::log!()
#[cfg(not(test))]
#[macro_export]
macro_rules! pg_log {
    ($($arg:tt)*) => { pgrx::log!($($arg)*) };
}

// Test logging - consumes args to avoid unused variable warnings
#[cfg(test)]
#[macro_export]
macro_rules! pg_log {
    ($($arg:tt)*) => {
        let _ = format!($($arg)*);
    };
}
```

## CI Integration

The CI pipeline handles the PGC_POSTMASTER constraint:

```yaml
# 1. Run unit tests (no Postgres needed)
- name: Run unit tests and generate coverage
  run: cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info

# 2. Install extension
- name: Install Extension
  run: cargo pgrx install --release

# 3. Configure shared_preload_libraries (CRITICAL)
- name: Configure Postgres for Background Worker
  run: |
    echo "shared_preload_libraries = 'pg_kafka'" >> $PG_DATA_DIR/postgresql.conf

# 4. Start Postgres (extension now loads at startup)
- name: Start Postgres
  run: cargo pgrx start pg14

# 5. Run E2E tests
- name: Run E2E Tests
  run: cd kafka_test && cargo run --release
```

## Coverage Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Project Coverage | 80%+ | Testable code only |
| Patch Coverage | 90%+ | New/modified code |
| Handler Tests | 95%+ | Core protocol logic |

Files in `codecov.yml` ignore list are excluded from these targets.

## Summary

1. **Use the Repository Pattern** - Abstract storage behind traits for testability
2. **Prefer unit tests** - They're fast and don't require Postgres
3. **Use E2E tests for integration** - Covers SPI-dependent code paths
4. **Exclude runtime code from coverage** - Document as E2E-tested
5. **Follow the CI pattern** - Configure `shared_preload_libraries` before starting Postgres
