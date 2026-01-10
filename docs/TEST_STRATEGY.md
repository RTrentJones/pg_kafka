# Test Strategy

**Status:** Phase 10 Complete
**Last Updated:** 2026-01-10

---

## Test Summary

| Category | Count | Coverage |
|----------|-------|----------|
| Unit Tests | 287 | Core logic, handlers, storage, protocol |
| E2E Tests | 74 | Full protocol integration |
| **Total** | **361** | **Comprehensive** |

---

## Unit Test Distribution (184 tests)

| Module | Tests | Focus |
|--------|-------|-------|
| Assignment Strategies | 61 | Range, RoundRobin, Sticky algorithms |
| Protocol Handlers | 22 | All 18 API handlers with MockKafkaStore |
| Storage Layer | 22 | KafkaStore trait, types, mock verification |
| Coordinator | 8 | Group state management, generation tracking |
| Error Handling | 10 | Error code mapping, typed errors |
| Partitioner | 7 | Murmur2 hash, key-based routing |
| Protocol Encoding | 34 | Wire format, properties, framing |
| Infrastructure | 20 | Config, mocks, helpers, constants |

### Running Unit Tests

```bash
cargo test --features pg14
```

---

## E2E Test Categories (74 tests)

| Category | Tests | Purpose |
|----------|-------|---------|
| Admin APIs | 9 | CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups |
| Producer | 2 | Basic produce, batch produce |
| Consumer | 3 | Basic consume, from offset, multiple messages |
| Consumer Groups | 3 | Lifecycle, two-member, rebalance after leave |
| Offset Management | 2 | Commit/fetch, boundaries |
| Partitioning | 4 | Multi-partition produce, key routing, distribution |
| Error Paths | 16 | Invalid partitions, unknown topics, coordinator errors |
| Edge Cases | 11 | Empty topics, large messages, boundary values |
| Concurrent | 8 | Multi-producer, multi-consumer, pipelining |
| Negative | 4 | Connection refused, timeouts, invalid operations |
| Performance | 3 | Throughput baselines (produce, consume, batch) |
| Long Polling | 4 | Timeout, immediate return, producer wakeup |
| Compression | 5 | gzip, snappy, lz4, zstd, roundtrip |

### Running E2E Tests

```bash
# Start PostgreSQL with extension
cargo pgrx start pg14

# Run all E2E tests
cd kafka_test && cargo run --release

# Run specific category
cd kafka_test && cargo run --release -- --category compression

# Run single test
cd kafka_test && cargo run --release -- --test test_compressed_producer_gzip
```

---

## Test Architecture

### Unit Tests (No Database Required)

- Use `MockKafkaStore` for storage operations
- Handler tests verify protocol logic in isolation
- Fast execution (~2 seconds)

### E2E Tests (Full Integration)

- Real rdkafka client against running extension
- Test isolation via unique topic/group names (UUID suffix)
- Database verification via tokio-postgres
- Parallel-safe where marked

---

## The PGC_POSTMASTER Constraint

**Why `#[pg_test]` doesn't work:**

pg_kafka uses `PGC_POSTMASTER` GUC variables that must be defined before PostgreSQL starts via `shared_preload_libraries`. The `cargo pgrx test` framework creates extensions after startup, causing a fatal error.

**Solution:** All integration testing is done via the E2E test suite in `kafka_test/`.

---

## Coverage by Phase

| Phase | Feature | Unit Tests | E2E Tests |
|-------|---------|------------|-----------|
| 1-2 | Producer/Metadata | Protocol encoding | Basic produce |
| 3 | Consumer | Fetch handlers | Consumer tests |
| 3B | Coordinator | Group state tests | Group lifecycle |
| 4 | Assignment | 61 strategy tests | Multi-member |
| 5 | Rebalancing | Timeout detection | Session timeout |
| 6 | Admin APIs | Handler tests | Create/delete ops |
| 7 | Partitioning | Murmur2 hash | Key routing |
| 8 | Compression | Parse compression | All codecs |

---

## CI/CD Integration

GitHub Actions runs both test suites:

```yaml
- name: Unit Tests
  run: cargo test --features pg14

- name: E2E Tests
  run: |
    cargo pgrx start pg14
    cd kafka_test && cargo run --release
```

---

## Code Coverage

### Coverage Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Project Coverage | 80%+ | Testable code only |
| Patch Coverage | 90%+ | New/modified code must be well-tested |
| Handler Tests | 95%+ | Core protocol logic |

### Generating Coverage Reports

```bash
# Summary only
cargo llvm-cov --lib --features pg14 --summary-only

# HTML report (recommended for analysis)
cargo llvm-cov --lib --features pg14 --html --output-dir coverage-report
open coverage-report/index.html

# LCOV format (for CI/Codecov)
cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info

# Fail if below threshold
cargo llvm-cov --lib --features pg14 --fail-under-lines 80
```

### Coverage Enforcement

Coverage is enforced in CI via:
1. **Codecov Integration** - Uploads coverage and blocks PRs below threshold
2. **CI Threshold Check** - `--fail-under-lines 80` in workflow

See `codecov.yml` for configuration details.

### Intentionally Uncovered Code

Some code cannot be unit tested due to pgrx/SPI dependencies. These are excluded from coverage requirements:

| File | Reason | Tested By |
|------|--------|-----------|
| `src/kafka/storage/postgres.rs` | SPI calls | E2E tests |
| `src/worker.rs` | Main loop, SPI | E2E tests |
| `src/kafka/listener.rs` | Async runtime | E2E tests |
| `src/config.rs` | GUC loading | E2E tests |
| `src/lib.rs` | `_PG_init` hook | E2E tests |

See `docs/PGRX_TESTING_GUIDE.md` for the full testing strategy.

---

**Test Count:** 361 (287 unit + 74 E2E)
**Coverage Target:** 80%+ (testable code)
**All Tests Passing:** ✅
