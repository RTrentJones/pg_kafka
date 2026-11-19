# Test Strategy

**Status:** Phase 11 Complete (Shadow Mode)
**Last Updated:** 2026-11-18

---

## Test Summary

**This section is the one place the repository states test counts.** Other docs
(`README.md`, `CLAUDE.md`, `docs/*`) link here instead of repeating numbers. The numbers below
are generated (`scripts/gen-docs.py --write --only tests`) and CI fails when they are stale, so
they match the commit they are in.

| Suite | Count | Coverage |
|-------|-------|----------|
| Unit Tests | <!--gen:tests-unit-->859<!--/gen--> | Core logic, handlers, storage, protocol, shadow (`src/`, includes in-module proptest fuzz harnesses) |
| Property Tests | <!--gen:tests-property-->10<!--/gen--> | `tests/property_tests.rs`: proptest wire-format invariants and boundary tests |
| E2E Tests | <!--gen:tests-e2e-->259<!--/gen--> | Full protocol integration (`kafka_test/`, per category [below](#e2e-test-categories)) |
| **Total** | **<!--gen:tests-total-->1128<!--/gen-->** | |

The generator counts with (none of these needs a running broker):

```bash
cargo test --features pg14 --lib -- --list | grep -c ': test$'                # unit
cargo test --features pg14 --test property_tests -- --list | grep -c ': test$'  # property
cd kafka_test && cargo run --release -- --list                                 # E2E per category
```

`kafka_test`'s own runner unit tests (`cargo test --bin kafka_test` in `kafka_test/`)
are not counted above.

---

## Unit Test Distribution

Where the unit tests live (`cargo test --features pg14 --lib <module path> -- --list` counts one
module):

| Module | Focus |
|--------|-------|
| Shadow Mode (`kafka/shadow/`) | External forwarding, SASL/SSL settings, licensing, routing, outbox, config |
| Handler Logic (`kafka/handlers/`) | Every API handler, mostly with MockKafkaStore (`handlers/tests.rs`) |
| Protocol (`kafka/protocol/`) | Decoding, encoding, RecordBatch framing, proptest fuzz harnesses |
| Assignment (`kafka/assignment/`) | Range, RoundRobin, Sticky algorithms, subscription/assignment codecs |
| Storage Layer (`kafka/storage/`) | Store role traits, types, mock verification, retention and partition-drop planning, privileges |
| API Registry (`kafka/api/`) | Every advertised version encodes, error-response echoes, network-thread APIs, generated docs |
| Authentication (`kafka/auth/`) | SCRAM (RFC 5802/7677 vectors), PLAIN, session rules, listener and TLS config |
| Messages, response builders, coordinator, errors, partitioner, config | Request/response types, group state, error-code mapping, murmur2, GUC defaults |
| Testing Infrastructure (`testing/`) | Mocks, helpers |

### Running Unit Tests

```bash
cargo test --features pg14 --lib                     # unit tests in src/
cargo test --features pg14 --test property_tests     # tests/property_tests.rs
cargo test --features pg14                           # both
```

---

## E2E Test Categories

Generated from `kafka_test --list` (what each category covers: CLAUDE.md, "Test Categories").

<!--gen:tests-e2e-categories-->
| Category | Tests |
|----------|-------|
| `admin` | 19 |
| `producer` | 5 |
| `consumer` | 4 |
| `offset_management` | 10 |
| `consumer_group` | 13 |
| `partition` | 9 |
| `error_paths` | 61 |
| `edge_cases` | 13 |
| `concurrent` | 14 |
| `negative` | 5 |
| `performance` | 7 |
| `long_poll` | 10 |
| `compression` | 10 |
| `idempotent` | 7 |
| `transaction` | 20 |
| `shadow` | 29 |
| `metadata` | 3 |
| `protocol` | 5 |
| `part` | 3 |
| `auth` | 12 |
| **Total** | **259** |
<!--/gen-->

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
`--concurrency <N>` (default 16), `--timeout-secs <N>` (per-test timeout, default 300),
`--json`, `--list`, `--verbose`. The runner reads
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
- Each test runs in its own tokio task: a panic (failed `assert!`) is reported as
  FAILED with the panic message, and a test still running after `--timeout-secs` is
  aborted and reported FAILED, so one bad test never ends the run without a summary
- Every test creates its own topics and groups (`TestContext::unique_topic` /
  `unique_group` or a UUID suffix), so the suite can be re-run against the same database

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
| 4 | Assignment | Assignment-module tests | Multi-member |
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

Some code cannot be unit tested due to pgrx/SPI dependencies. These files are excluded via the
`ignore:` list in `codecov.yml` (mirrored here):

| File | Reason | Tested By |
|------|--------|-----------|
| `src/kafka/storage/postgres.rs` | SPI calls | E2E tests |
| `src/kafka/storage/postgres/roles.rs` | Forwards every storage role to `postgres.rs` | E2E tests |
| `src/worker.rs` | Main loop, SPI | E2E tests |
| `src/kafka/listener.rs` | Async runtime | E2E tests |
| `src/kafka/shadow/store.rs` | SPI (wraps `PostgresStore`) | E2E (shadow) tests |
| `src/kafka/auth/credentials.rs` | SELECT on `kafka.sasl_credentials` | E2E (auth) tests |
| `src/kafka/auth/users.rs` | `#[pg_extern]` user-management functions | E2E (auth) tests |
| `src/lib.rs` | `_PG_init` hook, SQL functions | E2E tests |
| `src/bin/**`, `**/*_embed.rs` | pgrx-generated | N/A |

Files that mix SPI with unit-tested logic stay in the coverage figures but are listed as
`# qa1-guard:` lines in `codecov.yml`: `src/kafka/metrics.rs` (the SPI flush),
`src/kafka/shadow/outbox.rs` (`SpiShadowDb`), `src/kafka/shadow/primary.rs` (`is_primary`).

**QA-1 guard:** because unit coverage cannot see this code, the `spi-e2e-guard` CI job
(`.github/scripts/require-e2e-for-spi.sh`) fails a pull request that changes a file on either
list without also changing a Rust file under `kafka_test/src/`. The `no-e2e-needed` label waives
it for test-neutral edits. `.github/scripts/check-coverage-policy.sh` (lint job) keeps the lists
honest: it fails when a `src/` file calls SPI (`Spi::*`, `BackgroundWorker::*`, `#[pg_extern]`)
without being on one of them, when a listed file does not exist, or when the guard does not fire
for a listed file.

`src/kafka/handler_context.rs` is **not** excluded any more: it never held a mock store (that is
`src/testing/mocks.rs`), it is a plain struct the handler unit tests construct.

**Note:** `src/config.rs` is **not** excluded — its parse/validation/default logic is unit-tested and
subject to the coverage targets (it was previously listed here in error).

See `docs/PGRX_TESTING_GUIDE.md` for the full testing strategy.

---

**Test Count:** see [Test Summary](#test-summary)
**Coverage Target:** 80%+ (testable code)
**All Tests Passing:** ✅
