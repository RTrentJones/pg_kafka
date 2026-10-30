# Testing pgrx Extensions: Best Practices

This guide documents the testing strategy for pg_kafka and provides patterns for testing pgrx extensions that use PGC_POSTMASTER GUCs and background workers.

## The Challenge

pgrx extensions face unique testing challenges:

1. **`#[pg_test]` requires running Postgres** - Tests execute within a Postgres backend
2. **PGC_POSTMASTER GUCs must be set before Postgres starts** - Via `shared_preload_libraries`
3. **`cargo pgrx test` creates extensions AFTER startup** - Causing fatal errors for extensions with postmaster-level GUCs
4. **SPI calls require database context** - Cannot be called from unit tests

### Why `cargo pgrx test` Doesn't Work for pg_kafka

pg_kafka uses `PGC_POSTMASTER` GUC variables (defined in `src/config.rs`), for example:
- TCP port binding (`pg_kafka.port`)
- Network interface binding (`pg_kafka.host`)
- The database the worker connects to (`pg_kafka.database`)

`_PG_init()` also registers the background worker, which is only possible while the
library is loaded through `shared_preload_libraries`.

When `cargo pgrx test` runs:
1. Postgres starts without `pg_kafka` in `shared_preload_libraries`
2. The test framework tries to `CREATE EXTENSION pg_kafka`
3. `_PG_init()` attempts to register GUCs and background worker
4. **FATAL ERROR**: "cannot create PGC_POSTMASTER variables after startup"

## The Solution: Repository Pattern + E2E Testing

We use a three-tier testing strategy that works around these constraints:

### Tier 1: Unit Tests (No Database Required)

**Location:** `#[cfg(test)]` modules throughout `src/` (e.g. `src/kafka/handlers/tests.rs`,
`src/kafka/storage/tests.rs`, `src/kafka/protocol/decoding.rs`). Mocks live in `src/testing/mocks.rs`.

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

// Test implementation (no database needed): mockall-generated MockKafkaStore
// in src/testing/mocks.rs
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
// Handlers take a HandlerContext whose `store` field is a `&dyn KafkaStore`
// (src/kafka/handler_context.rs), so any KafkaStore implementation can be injected
pub fn handle_produce(
    ctx: &HandlerContext,
    topic_data: Vec<TopicProduceData>,
    producer_metadata: Option<&ProducerMetadata>,
    transactional_id: Option<&str>,
) -> Result<ProduceResponse> {
    // Business logic here - testable without database
}

// Test with mock (src/kafka/handlers/tests.rs)
#[test]
fn test_handle_produce_success() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1)));
    mock.expect_insert_records()
        .returning(|_, _, _| Ok(0));

    // Create HandlerContext AFTER setting expectations
    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();
    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);
}
```

**Run unit tests:**
```bash
cargo test --features pg14 --lib
```

### Tier 2: Protocol and Property Tests (No Database Required)

**Location:** the unit-test modules in `src/kafka/protocol/` (`decoding.rs`, `encoding.rs`,
`recordbatch.rs`, including proptest fuzz harnesses that assert the parsers never panic or abort on
arbitrary bytes), plus the integration target `tests/property_tests.rs` (proptest invariants for
header fields, ports, node ids, partition indices, API version ranges, and topic-name lengths).

These test wire protocol encoding/decoding without database interaction:

```rust
proptest! {
    #[test]
    fn fuzz_parse_request_never_panics(
        data in proptest::collection::vec(proptest::num::u8::ANY, 0..1024)
    ) {
        let (tx, _rx) = create_test_channel();
        let buf = BytesMut::from(&data[..]);
        let _ = parse_request(buf, tx);
    }
}
```

`cargo llvm-cov --lib` does not compile `tests/property_tests.rs`, so CI runs it as its own step:
```bash
cargo test --features pg14 --test property_tests
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

    // 4. CLEANUP: explicit; TestContext's Drop is only a best-effort fallback
    ctx.cleanup().await?;
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

       let coordinator = GroupCoordinator::new();
       let broker = BrokerMetadata::new("localhost".to_string(), 9092);
       let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);
       let result = handle_new_api(&ctx, ...).unwrap();
       assert!(result.is_valid());
   }
   ```

3. **Add protocol tests for encoding/decoding**
   ```rust
   #[test]
   fn test_new_response_encoding() {
       let response = KafkaResponse::NewApi { ... };
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

Some code paths cannot be covered by unit tests due to pgrx/SPI dependencies. These are excluded from
coverage requirements (via the `ignore:` list in `codecov.yml`) and tested via E2E. This table mirrors
that list exactly:

| File | Reason | Covered By |
|------|--------|------------|
| `src/kafka/storage/postgres.rs` | SPI calls | E2E tests |
| `src/worker.rs` | Main loop, SPI | E2E tests |
| `src/kafka/listener.rs` | Async runtime, tokio | E2E tests |
| `src/kafka/shadow/store.rs` | SPI (wraps `PostgresStore`) | E2E (shadow) tests |
| `src/kafka/handler_context.rs` | Holds the `MockStore` test double (drags coverage) | Handler unit tests |
| `src/lib.rs` | `_PG_init` hook | E2E tests |
| `src/bin/**`, `**/*_embed.rs` | pgrx-generated | N/A |

> **Note:** `src/config.rs` is **not** excluded. Its GUC loading needs the running server, but the parse/
> validation/default logic is unit-testable and gated — it has its own unit tests and is subject to the
> coverage targets like any other non-ignored file. (It was previously listed here in error.)

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

The `Test Suite` job in `.github/workflows/ci.yml` handles the PGC_POSTMASTER constraint
(abridged; see the workflow for the full steps):

```yaml
# 1. Run unit tests (no Postgres needed), then the property-test target
- name: Run unit tests and generate coverage
  run: cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info
- name: Run property tests
  run: cargo test --features pg14 --test property_tests

# 2. Install extension into the pgrx-managed PostgreSQL 14
- name: Install Extension
  run: cargo pgrx install --release --pg-config="$PG_CONFIG"

# 3. Start/stop once so pgrx creates the data directory
- name: Initialize Postgres Data Directory
  run: cargo pgrx start pg14 || true; sleep 2; cargo pgrx stop pg14 || true

# 4. Configure shared_preload_libraries (CRITICAL)
- name: Configure Postgres for Background Worker
  run: |
    printf "\nshared_preload_libraries = 'pg_kafka'\npg_kafka.host = '0.0.0.0'\n" >> "$PG14_DATA_DIR/postgresql.conf"

# 5. Start Postgres (extension now loads at startup), then CREATE EXTENSION in the postgres DB
- name: Start Postgres
  run: cargo pgrx start pg14

# 6. Run E2E tests (functional, then shadow against external Kafka, then performance)
- name: Run Functional E2E Tests
  run: cd kafka_test && cargo run --release -- --exclude performance --exclude shadow
```

## Coverage Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Project Coverage | 80%+ | Testable code only |
| Patch Coverage | 90%+ | New/modified code |
| Handler Tests | 95%+ | Aspirational; not enforced by `codecov.yml` |

Files in `codecov.yml` ignore list are excluded from these targets. The `spi-e2e-guard` CI job
(`.github/scripts/require-e2e-for-spi.sh`) compensates by requiring a `kafka_test/src/*.rs` change
in any pull request that touches a `src/` file on that list (waivable with the `no-e2e-needed` label).

## Summary

1. **Use the Repository Pattern** - Abstract storage behind traits for testability
2. **Prefer unit tests** - They're fast and don't require Postgres
3. **Use E2E tests for integration** - Covers SPI-dependent code paths
4. **Exclude runtime code from coverage** - Document as E2E-tested
5. **Follow the CI pattern** - Configure `shared_preload_libraries` before starting Postgres
