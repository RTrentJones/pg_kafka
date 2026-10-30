# Test Strategy

**Status:** Phase 11 Complete (Shadow Mode)
**Last Updated:** 2026-10-29

---

## Test Summary

| Category | Count | Coverage |
|----------|-------|----------|
| Unit Tests | 713 | Core logic, handlers, storage, protocol, shadow (`src/`, includes 4 in-module proptest fuzz harnesses) |
| Property Tests | 10 | `tests/property_tests.rs`: 8 proptest wire-format invariants + 2 boundary tests (CI-run) |
| E2E Tests | 202 | Full protocol integration (`kafka_test/`) |
| **Total** | **~925** | **Comprehensive** |

> DR-22 (DEEP-REVIEW-2026-07): counts here are refreshed manually and drift as
> tests land — **the CI run is the source of truth.** Regenerate with
> `cargo test --features pg14 --lib` (unit), `cargo test --features pg14 --test property_tests`
> (property), `grep -c wrap_test! kafka_test/src/main.rs` (E2E).

---

## Unit Test Distribution (indicative)

| Module | Tests | Focus |
|--------|-------|-------|
| Shadow Mode (`kafka/shadow/`) | 128 | External forwarding, SASL/SSL settings, licensing, routing, outbox, config |
| Handler Logic (`kafka/handlers/`) | 107 | All 26 API handlers, mostly with MockKafkaStore (`handlers/tests.rs` holds 80) |
| Protocol (`kafka/protocol/`) | 105 | Decoding, encoding, RecordBatch framing, 4 proptest fuzz harnesses |
| Assignment (`kafka/assignment/`) | 69 | Range, RoundRobin, Sticky algorithms, subscription/assignment codecs |
| Storage Layer (`kafka/storage/tests.rs`) | 53 | KafkaStore trait, types, mock verification |
| Messages | 53 | Request/response types, size estimators |
| Config | 36 | GUC configuration |
| Response Builders | 35 | Response construction |
| Coordinator | 28 | Group state management |
| Testing Infrastructure (`testing/`) | 23 | Mocks, helpers |
| Error Handling | 22 | Error code mapping, typed errors |
| Partitioner | 9 | Murmur2 hash, key-based routing |
| Other | 45 | Constants, dispatch, pending fetches, broker metadata, context, listener, worker |

### Running Unit Tests

```bash
cargo test --features pg14 --lib                     # unit tests in src/
cargo test --features pg14 --test property_tests     # tests/property_tests.rs
cargo test --features pg14                           # both
```

---

## E2E Test Categories (202 tests)

| Category | Tests | Purpose |
|----------|-------|---------|
| Admin APIs (`admin`) | 19 | CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups, DescribeConfigs, IncrementalAlterConfigs, DeleteRecords, per-topic retention |
| Producer (`producer`) | 5 | Basic produce, batch produce, acks=0, ack-implies-visibility, timestamps |
| Consumer (`consumer`) | 4 | Basic consume, headers, from offset, multiple messages |
| Consumer Groups (`consumer_group`) | 13 | Lifecycle, two-member, rebalance, strategies |
| Offset Management (`offset_management`) | 10 | Commit/fetch, boundaries, no offset reuse, ListOffsets by timestamp, multi-partition |
| Partitioning (`partition`) | 9 | Multi-partition produce, key routing, distribution |
| Compression (`compression`) | 10 | gzip, snappy, lz4, zstd, roundtrip, edge cases |
| Idempotent (`idempotent`) | 7 | InitProducerId, deduplication, epoch, multi-partition |
| Transaction (`transaction`) | 20 | EOS, isolation levels, LSO, fencing, timeouts, atomicity |
| Shadow (`shadow`) | 27 | Dual-write, external-only/local-only, percentage dial-up, topic mapping, outbox, transactions, replay |
| Long Polling (`long_poll`) | 10 | Timeout, immediate return, producer wakeup, precision, per-connection cap |
| Error Paths (`error_paths`) | 21 | Invalid partitions, unknown topics, coordinator errors |
| Edge Cases (`edge_cases`) | 13 | Empty topics, large messages, boundary values, retention sweep |
| Concurrent (`concurrent`) | 14 | Multi-producer, multi-consumer, pipelining, races, heartbeat bypass |
| Negative (`negative`) | 5 | Connection refused, timeouts, invalid operations, superuser-only GUC |
| Performance (`performance`) | 7 | Throughput baselines, latency percentiles, scaling |
| Metadata (`metadata`) | 3 | Metadata refresh, all topics, nonexistent topic |
| Protocol (`protocol`) | 5 | ApiVersions negotiation, correlation ID, unknown API, pipelining, ordering with long poll |

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

# Exclude categories (repeatable), as CI's functional run does
cd kafka_test && cargo run --release -- --exclude performance --exclude shadow
```

Other flags: `--sequential` (disable parallelism; runs are parallel by default),
`--concurrency <N>` (default 16), `--json`, `--list`, `--verbose`. The runner reads
`DATABASE_URL` and falls back to `host=localhost port=28814 user=postgres dbname=postgres`.

---

## Test Architecture

### Unit Tests (No Database Required)

- Use `MockKafkaStore` for storage operations
- Handler tests verify protocol logic in isolation
- Fast execution (~2 seconds)

### E2E Tests (Full Integration)

- Real rdkafka client against running extension
- Test isolation via unique topic/group names (test id plus a UUID prefix)
- Database verification via tokio-postgres
- Run in parallel by default; tests marked `parallel_safe: false` are run separately

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
| 4 | Assignment | 69 assignment-module tests | Multi-member |
| 5 | Rebalancing | Timeout detection | Session timeout |
| 6 | Admin APIs | Handler tests | Create/delete ops |
| 7 | Partitioning | Murmur2 hash | Key routing |
| 8 | Compression | Parse compression | All codecs |
| 9 | Idempotent producer | Sequence validation | Deduplication, epochs |
| 10 | Transactions | Transaction handlers | EOS, isolation, fencing |
| 11 | Shadow mode | Shadow module tests | Forwarding, outbox, replay |

---

## CI/CD Integration

The `Test Suite` job in `.github/workflows/ci.yml` runs every suite (abridged):

```yaml
- name: Run unit tests and generate coverage
  run: cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info

- name: Run property tests
  run: cargo test --features pg14 --test property_tests

# ... install the extension, add it to shared_preload_libraries, cargo pgrx start pg14 ...

- name: Run Functional E2E Tests
  run: cd kafka_test && cargo run --release -- --exclude performance --exclude shadow

- name: Run Shadow Mode E2E Tests      # against the external-kafka docker compose service
  run: cd kafka_test && cargo run --release -- --category shadow

- name: Run Performance E2E Tests      # blocking; conservative regression floors
  run: cd kafka_test && cargo run --release -- --category performance
```

Separate jobs cover lint/format (`Lint & Format`), `cargo audit` + `cargo deny` (`Security Audit`),
lockfile freshness (`Verify Lockfile`), compile + unit tests on PostgreSQL 15/16/17 (`pg-matrix`), and the
QA-1 guard (`spi-e2e-guard`, pull requests only).

---

## Code Coverage

### Coverage Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Project Coverage | 80%+ | Testable code only |
| Patch Coverage | 90%+ | New/modified code must be well-tested |
| Handler Tests | 95%+ | Aspirational; not enforced by `codecov.yml` |

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

Coverage is enforced in CI via **Codecov's commit status checks** (configured in `codecov.yml`): the
`test` job uploads `lcov.info` (produced by `cargo llvm-cov --lib`), and Codecov computes the two statuses
that gate the PR — `codecov/project` (target 80%, threshold 2%) and `codecov/patch` (target 90% on
changed lines, threshold 5%). The
workflow itself does **not** run a local `--fail-under-lines` gate; the Codecov statuses are the
enforcement.

See `codecov.yml` for configuration details.

### Intentionally Uncovered Code

Some code cannot be unit tested due to pgrx/SPI dependencies. These are excluded (via the `ignore:` list in
`codecov.yml`, mirrored here):

| File | Reason | Tested By |
|------|--------|-----------|
| `src/kafka/storage/postgres.rs` | SPI calls | E2E tests |
| `src/worker.rs` | Main loop, SPI | E2E tests |
| `src/kafka/listener.rs` | Async runtime | E2E tests |
| `src/kafka/shadow/store.rs` | SPI (wraps `PostgresStore`) | E2E (shadow) tests |
| `src/kafka/handler_context.rs` | Holds the `MockStore` test double | Handler unit tests |
| `src/lib.rs` | `_PG_init` hook | E2E tests |
| `src/bin/**`, `**/*_embed.rs` | pgrx-generated | N/A |

**QA-1 guard:** because unit coverage cannot see these files, the `spi-e2e-guard` CI job
(`.github/scripts/require-e2e-for-spi.sh`) fails a pull request that changes any `src/` entry in this
list without also changing a Rust file under `kafka_test/src/`. The `no-e2e-needed` label waives it for
test-neutral edits.

**Note:** `src/config.rs` is **not** excluded — its parse/validation/default logic is unit-tested and
subject to the coverage targets (it was previously listed here in error).

See `docs/PGRX_TESTING_GUIDE.md` for the full testing strategy.

---

**Test Count:** ~925 (713 unit + 10 property + 202 E2E; CI is the source of truth)
**Coverage Target:** 80%+ (testable code)
**All Tests Passing:** ✅
