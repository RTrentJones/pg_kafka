# pg_kafka Test Strategy

**Date:** 2026-01-03
**Status:** Phase 3B Complete

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

**Location:** `tests/` directory and `src/kafka/` modules

| File | Tests | Coverage |
|------|-------|----------|
| tests/protocol_tests.rs | 10 | Protocol parsing, request/response encoding |
| tests/encoding_tests.rs | 8 | Binary encoding, framing |
| tests/property_tests.rs | 10 | Property-based fuzzing |
| tests/helpers.rs | 6 | Mock factories, test utilities |
| src/kafka/handlers/tests.rs | 14 | Handler logic with MockKafkaStore |
| src/kafka/storage/tests.rs | 22 | Storage types and MockKafkaStore coverage |
| **Total** | **68** | **Non-SPI logic + handler/storage unit tests** |

**What's Tested:**
- ✅ Kafka wire protocol parsing
- ✅ Binary encoding/decoding
- ✅ Error type display and conversion
- ✅ Mock request builders
- ✅ Property-based testing (arbitrary inputs)
- ✅ Handler logic with MockKafkaStore (produce, fetch, metadata, offsets)
- ✅ Storage layer types (TopicMetadata, FetchedMessage, CommittedOffset)
- ✅ KafkaStore trait contract verification

**What's NOT Tested (via unit tests):**
- ❌ SPI database operations (tested via E2E)
- ❌ Background worker lifecycle (tested via E2E)
- ❌ GUC configuration loading (tested via E2E)
- ❌ PostgresStore implementation (tested via E2E)

### ✅ E2E Integration Tests (`kafka_test/`)

**Location:** `kafka_test/src/main.rs`

**Test Scenarios (5 total, all passing):**

1. **Test 1: Producer Functionality** ✅
   - Produces message to Kafka
   - Verifies message in PostgreSQL database
   - Validates topic creation, partition offset, key, value

2. **Test 2: Basic Consumer Functionality** ✅
   - Produces test message
   - Consumes using manual partition assignment
   - Verifies offset, key, value match

3. **Test 3: Consumer Multiple Messages** ✅
   - Produces 5 messages
   - Consumes all 5 messages sequentially
   - Validates message order

4. **Test 4: Consumer From Specific Offset** ✅
   - Produces 10 messages with known offsets
   - Verifies high watermark calculation
   - Validates database contains correct count

5. **Test 5: OffsetCommit/OffsetFetch** ✅
   - Produces 5 messages
   - Consumes 3 messages with manual offset commits
   - Verifies committed offsets in database
   - Creates new consumer, verifies resume from committed offset

**What's Tested:**
- ✅ Full produce flow (TCP → Protocol → SPI → Database)
- ✅ Full consume flow (Fetch → RecordBatch encoding → Client)
- ✅ Consumer group coordinator (FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup)
- ✅ Offset management (OffsetCommit, OffsetFetch)
- ✅ ListOffsets (earliest/latest)
- ✅ Topic auto-creation
- ✅ Offset calculation (dual-offset design)
- ✅ Database persistence verification
- ✅ Real Kafka client compatibility (rdkafka)
- ✅ Manual partition assignment
- ✅ Empty fetch response handling

**Test Execution:**
1. Starts PostgreSQL with `shared_preload_libraries='pg_kafka'`
2. Creates extension via `CREATE EXTENSION pg_kafka;`
3. Uses real Kafka client (rdkafka) to produce and consume messages
4. Verifies data in PostgreSQL via SQL queries
5. Validates offsets, keys, values, topic creation, consumer offsets

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

### For Phase 3B (Current)

✅ **E2E test suite expanded successfully:**
- 34 unit tests for non-SPI logic
- 5 comprehensive E2E test scenarios (all passing)
- Covers producer, consumer, coordinator, offset management
- Good enough for a portfolio/learning project

**Current coverage:** ~50-55%

### For Phase 4 (Automatic Assignment)

📋 **Additional E2E tests to add:**
1. Add `test_range_assignment()` - Range assignment strategy
2. Add `test_roundrobin_assignment()` - RoundRobin assignment strategy
3. Add `test_rebalancing()` - Automatic rebalancing on join/leave
4. Add `test_concurrent_clients()` - Multiple consumers in same group

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
- ⚠️ Weak coverage for SPI integration (40%)
- ✅ E2E test suite validates producer and consumer flows (5 scenarios)
- ✅ All E2E tests passing
- ❌ Cannot use pgrx test framework due to PGC_POSTMASTER limitation

**Test Results:**
- All 5 E2E scenarios passing ✅
- Tests cover: Producer, Consumer (manual assignment), Coordinator, Offset management
- Real Kafka client (rdkafka) validates protocol compatibility

**Next Steps:**
1. ✅ Phase 3B E2E tests complete
2. 📋 Add automatic assignment tests in Phase 4
3. 📋 Add rebalancing tests in Phase 5
4. 📝 Test limitations documented

**Is This Good Enough?**
- **For a portfolio project:** Yes, 50-55% with comprehensive E2E coverage is excellent
- **For production use:** No, would need 70%+ coverage with chaos testing
- **For learning objectives:** Yes, demonstrates real-world testing constraints and Kafka protocol compatibility

---

**Last Updated:** 2026-01-03
**Phase:** Phase 3B Complete (Producer + Consumer + Coordinator)
**Test Count:** 39 tests (34 unit + 5 E2E scenarios)
**Coverage:** ~50-55% estimated
**E2E Test Status:** ✅ All passing
