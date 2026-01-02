# pg_kafka Test Strategy

**Date:** 2026-01-02
**Status:** Phase 2 Complete

## The PGC_POSTMASTER Problem

### Why We Can't Use `#[pg_test]`

**Root Cause:**
```
FATAL SQLSTATE[XX000]: cannot create PGC_POSTMASTER variables after startup
```

**Explanation:**
1. `pg_kafka` uses `PGC_POSTMASTER` GUC variables in [src/config.rs](../src/config.rs)
2. These variables **MUST** be defined before PostgreSQL starts via `shared_preload_libraries`
3. `cargo pgrx test` creates the extension **AFTER** starting PostgreSQL
4. This causes a fatal error that cannot be worked around

**Impact:**
- ❌ Cannot use `#[pg_test]` for SPI integration tests
- ❌ Lost 12 unit tests from `src/worker.rs`
- ✅ Can still use `#[test]` for non-SPI logic tests
- ✅ E2E tests in `kafka_test/` work correctly (use proper loading)

## Current Test Coverage

### ✅ Unit Tests (`cargo test --lib`)

**Location:** `tests/` directory

| File | Tests | Coverage |
|------|-------|----------|
| protocol_tests.rs | 10 | Protocol parsing, request/response encoding |
| encoding_tests.rs | 8 | Binary encoding, framing |
| property_tests.rs | 10 | Property-based fuzzing |
| helpers.rs | 6 | Mock factories, test utilities |
| **Total** | **34** | **Non-SPI logic** |

**What's Tested:**
- ✅ Kafka wire protocol parsing
- ✅ Binary encoding/decoding
- ✅ Error type display and conversion
- ✅ Mock request builders
- ✅ Property-based testing (arbitrary inputs)

**What's NOT Tested:**
- ❌ SPI database operations
- ❌ Background worker lifecycle
- ❌ GUC configuration loading
- ❌ Request processing with database

### ✅ E2E Integration Test (`kafka_test/`)

**Location:** `kafka_test/src/main.rs`

**What's Tested:**
- ✅ Full produce flow (TCP → Protocol → SPI → Database)
- ✅ Topic auto-creation
- ✅ Offset calculation (dual-offset design)
- ✅ Database persistence verification
- ✅ Real Kafka client compatibility (rdkafka)

**Test Execution:**
1. Starts PostgreSQL with `shared_preload_libraries='pg_kafka'`
2. Creates extension via `CREATE EXTENSION pg_kafka;`
3. Uses real Kafka client (rdkafka) to produce messages
4. Verifies data in PostgreSQL via SQL queries
5. Validates offsets, keys, values, topic creation

### ❌ Missing Coverage

**SPI Unit Tests (Previously in worker.rs):**
- ❌ `get_or_create_topic()` edge cases
- ❌ `insert_records()` batch operations
- ❌ ProduceRequest error paths
- ❌ acks=0 rejection (tested in E2E but not unit-level)
- ❌ Invalid partition handling
- ❌ Transaction rollback scenarios
- ❌ Advisory lock behavior
- ❌ JSONB serialization failures

## Test Execution

### Locally

```bash
# Unit tests (non-SPI)
cargo test --lib --features pg14

# E2E tests (full integration)
cargo pgrx start pg14  # Start Postgres with extension loaded
cd kafka_test && cargo run
```

### CI/CD (GitHub Actions)

```yaml
- name: Run unit tests
  run: cargo test --lib --features pg14

- name: Install Extension
  run: cargo pgrx install --pg14 --release

- name: Configure Postgres
  run: echo "shared_preload_libraries = 'pg_kafka'" >> ~/.pgrx/data-14/postgresql.conf

- name: Start Postgres
  run: cargo pgrx start pg14

- name: Run E2E Tests
  run: cd kafka_test && cargo run --release
```

## Coverage Metrics

### Estimated Coverage by Module

| Module | Lines | Estimated Coverage | Testing Method |
|--------|-------|-------------------|----------------|
| protocol.rs | 512 | 85% | Unit tests (tests/protocol_tests.rs) |
| error.rs | 97 | 90% | Unit tests (tests/error.rs#tests) |
| messages.rs | 211 | 70% | Unit + E2E |
| listener.rs | 191 | 50% | E2E only |
| config.rs | 104 | 40% | E2E only (implicit) |
| lib.rs | 118 | 40% | E2E only |
| worker.rs | 1201 | 30% | E2E only |
| response_builders.rs | 102 | 70% | E2E only |
| constants.rs | 198 | N/A | Constants (no tests needed) |
| **Total** | **2734** | **~45%** | **Mixed** |

### Critical Path Coverage

**What's Covered (Happy Path):**
- ✅ ApiVersions request/response
- ✅ Metadata request/response
- ✅ ProduceRequest with database persistence
- ✅ Topic auto-creation
- ✅ Offset calculation
- ✅ Binary protocol encoding/decoding

**What's NOT Covered (Error Paths):**
- ❌ SPI connection failures
- ❌ Database constraint violations
- ❌ Out-of-memory conditions
- ❌ Concurrent write race conditions (advisory lock effectiveness)
- ❌ Connection errors during request processing
- ❌ Malformed requests beyond basic protocol tests

## Improving Coverage

### Short-Term (Phase 2)

**Option 1: Accept Current Coverage**
- Rely on E2E test for SPI validation
- 45% coverage is acceptable for a learning project
- Focus on adding features (Phase 3: Consumer support)

**Option 2: Expand E2E Tests**
Add more test scenarios to `kafka_test/`:

```rust
// kafka_test/src/main.rs additions
fn test_acks_zero_rejected() { ... }
fn test_invalid_partition() { ... }
fn test_batch_produce() { ... }
fn test_concurrent_producers() { ... }
fn test_large_message() { ... }
```

### Long-Term (Phase 3+)

**Option 3: Mock-Based SPI Tests**
Create a mock SPI layer that doesn't require PostgreSQL:

```rust
// tests/worker_tests.rs
#[test]
fn test_get_or_create_topic_with_mock_spi() {
    let mock_spi = MockSpi::new();
    mock_spi.expect_query("INSERT INTO kafka.topics...")
        .times(1)
        .returning(Ok(42));

    let result = get_or_create_topic_mocked(&mock_spi, "test-topic");
    assert_eq!(result.unwrap(), 42);
}
```

**Pros:**
- Can test SPI logic without PostgreSQL
- Fast execution
- Easy to test error conditions

**Cons:**
- Requires refactoring to inject SPI dependency
- Mocks can drift from real behavior
- Significant engineering effort

## Comparison with Real Kafka

### Kafka Test Strategy
- Unit tests for protocol, serialization, compression
- Integration tests with ZooKeeper/KRaft
- System tests with multi-broker clusters
- Chaos engineering (network partitions, node failures)

### pg_kafka Test Strategy
- Unit tests for protocol, serialization (same as Kafka)
- E2E tests with PostgreSQL (instead of ZooKeeper)
- No multi-broker tests (single-node by design)
- Rely on PostgreSQL's robustness for HA

## Recommendation

### For Phase 2 (Current)

✅ **Accept 45% coverage** with current test strategy:
- 34 unit tests for non-SPI logic
- 1 comprehensive E2E test
- Good enough for a portfolio/learning project

### For Phase 3 (Consumer Support)

✅ **Expand E2E tests** in `kafka_test/`:
1. Add `test_consumer_flow()` - FetchRequest with polling
2. Add `test_acks_variants()` - Test acks=0, acks=1, acks=all
3. Add `test_batch_operations()` - Multiple records in one request
4. Add `test_concurrent_clients()` - Stress testing

**Expected Coverage After:** 60-65%

### For Production Use (Future)

⚠️ **Would Need:**
- Mock-based SPI tests (~20 additional tests)
- Chaos testing (connection drops, Postgres crashes)
- Performance benchmarks (throughput, latency)
- Fuzz testing (malformed Kafka protocol inputs)

**Expected Coverage:** 80%+

## Conclusion

**Current State:**
- ✅ Good coverage for protocol logic (85%+)
- ⚠️ Weak coverage for SPI integration (30%)
- ✅ E2E test validates happy path end-to-end
- ❌ Cannot use pgrx test framework due to PGC_POSTMASTER limitation

**Next Steps:**
1. ✅ Accept current coverage for Phase 2
2. 🔄 Expand `kafka_test/` with more scenarios in Phase 3
3. 📝 Document test limitations in README

**Is This Good Enough?**
- **For a portfolio project:** Yes, 45% with good E2E coverage is acceptable
- **For production use:** No, would need 70%+ coverage with chaos testing
- **For learning objectives:** Yes, demonstrates real-world testing constraints

---

**Last Updated:** 2026-01-02
**Phase:** Phase 2 Complete
**Test Count:** 35 tests (34 unit + 1 E2E)
**Coverage:** ~45% estimated
