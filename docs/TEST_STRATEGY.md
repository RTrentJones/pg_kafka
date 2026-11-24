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
| Unit Tests | <!--gen:tests-unit-->1019<!--/gen--> | Core logic, handlers, storage, protocol, shadow (`src/`, includes in-module proptest fuzz harnesses) |
| Property Tests | <!--gen:tests-property-->10<!--/gen--> | `tests/property_tests.rs`: proptest wire-format invariants and boundary tests |
| E2E Tests | <!--gen:tests-e2e-->423<!--/gen--> | Full protocol integration (`kafka_test/`, per category [below](#e2e-test-categories)) |
| **Total** | **<!--gen:tests-total-->1452<!--/gen-->** | |

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
| `admin` | 22 |
| `producer` | 8 |
| `consumer` | 5 |
| `offset_management` | 15 |
| `consumer_group` | 23 |
| `partition` | 9 |
| `error_paths` | 70 |
| `edge_cases` | 13 |
| `concurrent` | 15 |
| `negative` | 5 |
| `performance` | 7 |
| `long_poll` | 11 |
| `compression` | 10 |
| `idempotent` | 16 |
| `transaction` | 38 |
| `shadow` | 54 |
| `metadata` | 5 |
| `protocol` | 5 |
| `part` | 16 |
| `auth` | 16 |
| `network` | 54 |
| `worker` | 6 |
| **Total** | **423** |
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

## Real-Client and Differential Checks (`interop/`)

Two harnesses check pg_kafka against Apache Kafka's own clients and against a real Kafka broker.
Both run in CI (job `interop`, PostgreSQL 18) and locally against any running pg_kafka:

```bash
# Reference broker: single-node KRaft Kafka 3.7 on 127.0.0.1:9093
interop/kafka-ref.sh start /path/to/kafka_2.13-3.7.0 /tmp/kref

# Java tools and clients: verifiable producer + 3 verifiable consumers through a rebalance
# (no loss, no duplicates, bounded rebalances), consumer-groups --describe lag, get-offsets,
# transactional perf-test + read_committed consumer, TransactionalMessageCopier with random
# aborts, aborted / open transactions at the tail, SASL SCRAM-SHA-256/512 + PLAIN, Kafka Streams
KAFKA_HOME=/path/to/kafka_2.13-3.7.0 PGHOST=127.0.0.1 PGPORT=28814 PGUSER=postgres \
  interop/run-java-checks.sh [--only name,...] [--list]

# The same raw request sequences against pg_kafka and Kafka, normalised and compared
KAFKA_HOME=/path/to/kafka_2.13-3.7.0 python3 interop/diff/run.py --pg 127.0.0.1:9092 --kafka 127.0.0.1:9093
```

`interop/diff/KafkaWire.java` encodes and decodes with Kafka's generated protocol classes, so
every API and version is read exactly as the Java client reads it. Every difference the
differential harness finds is classified in `KNOWN` (`interop/diff/run.py`) as a documented
deviation (with its `PROTOCOL_DEVIATIONS.md` section) or a bug; an unclassified difference fails
the run. Checks and observations marked as bugs (`XFAIL`) do not fail the run; once fixed they
report `XPASS` and the mark should be removed. The Java checks pass against the reference broker
itself (`BOOTSTRAP=127.0.0.1:9093`, skipping the two that need pg_kafka's SQL), which is how they
were validated.

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

`.github/workflows/ci.yml` (PostgreSQL from apt.postgresql.org through
`.github/actions/setup-pg`, toolchain from `rust-toolchain.toml`):

| Job | What it runs |
|-----|--------------|
| `Lint & Format` | `cargo fmt --check` and clippy `-D warnings` on both crates (pg14), the QA-1 policy check (`.github/scripts/check-coverage-policy.sh`) |
| `Test Suite` | unit tests under `cargo llvm-cov` (pg14, uploaded to Codecov; includes the API-docs drift test), property tests |
| `Build kafka_test` | the E2E runner, once, shared with the `e2e` legs as an artifact |
| `E2E pg14` … `E2E pg18` | per major: clippy, unit and property tests, `cargo pgrx install` into that PostgreSQL, `scripts/ci-pg.sh start`, the functional E2E suite (`--exclude performance --exclude shadow`, 25 min step timeout), `scripts/check-upgrade.sh`, the generated GUC reference check; pg14 also the test-count check and the `shadow` and `performance` categories |
| `Interop` | `interop/run-java-checks.sh` and `interop/diff/run.py` on PG18 against Kafka 3.7 (see above) |
| `Security Audit`, `Verify Lockfile`, `SPI change needs E2E test` | `cargo audit` + `cargo deny`; `--locked` checks; the QA-1 guard (pull requests) |

The functional suite (365 tests, including the auth, network and worker categories' listener
restarts) took 9.6 minutes on PG16 in a 4-core sandbox shared with two other builds; the 25-minute
step timeout leaves about a 2.5x margin over that loaded run, and the job timeout (100 min)
covers a cold build, the suite and, on pg14, the shadow and performance categories.

---

## Code Coverage

### Coverage Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Project Coverage | 80% (gate 78%) | Testable code only; `codecov.yml` threshold 2% |
| Patch Coverage | 90% (gate 85%) | New/modified code; threshold 5% |
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
that gate the PR — `codecov/project` (target 80%, threshold 2%: passes at 78% and above) and
`codecov/patch` (target 90% on changed lines, threshold 5%: passes at 85% and above). The
workflow itself does **not** run a local `--fail-under-lines` gate; the Codecov statuses are the
enforcement.

See `codecov.yml` for configuration details.

### Intentionally Uncovered Code

Code that calls SPI, runs inside the background worker or drives the tokio listener cannot run in
a unit test. `codecov.yml` is the one list of it: its `ignore:` entries are excluded from the
coverage figures (each with its reason), and its `# qa1-guard:` lines name the files that mix SPI
with unit-tested logic and so stay in them.

**QA-1 guard:** because unit coverage cannot see this code, the `spi-e2e-guard` CI job
(`.github/scripts/require-e2e-for-spi.sh`) fails a pull request that changes a file on either
list without also changing a Rust file under `kafka_test/src/`. The `no-e2e-needed` label waives
it for test-neutral edits. `.github/scripts/check-coverage-policy.sh` (lint job) keeps the lists
honest: it fails when a `src/` file calls SPI (`Spi::*`, `BackgroundWorker::*`, `#[pg_extern]`)
without being on one of them, when a listed file does not exist, or when the guard does not fire
for a listed file.

See `docs/PGRX_TESTING_GUIDE.md` for the full testing strategy.

---

**Test Count:** see [Test Summary](#test-summary)
**Coverage Target:** 80% of testable code (gated at 78%)
**All Tests Passing:** ✅
