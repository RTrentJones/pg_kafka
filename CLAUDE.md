# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pg_kafka` is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire protocol listener directly into the Postgres runtime. It allows standard Kafka clients to produce and consume messages using Postgres as the backing store.

**Status:** Phase 11 Complete - Shadow Mode (Portfolio/Learning Project)

**Current Implementation:**
- ✅ **Phase 1 Complete:** Metadata support (ApiVersions, Metadata requests)
- ✅ **Phase 2 Complete:** Producer support (ProduceRequest, database storage, E2E tests, Repository Pattern)
- ✅ **Phase 3 Complete:** Consumer support (FetchRequest, ListOffsets, OffsetCommit/Fetch)
- ✅ **Phase 3B Complete:** Consumer group coordinator (FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup)
- ✅ **Phase 4 Complete:** Automatic partition assignment strategies (Range, RoundRobin, Sticky)
- ✅ **Phase 5 Complete:** Automatic rebalancing (LeaveGroup trigger, timeout detection, REBALANCE_IN_PROGRESS)
- ✅ **Phase 6 Complete:** Admin APIs (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups)
- ✅ **Phase 7 Complete:** Multi-Partition Topics (key-based routing, default partition config)
- ✅ **Phase 8 Complete:** Compression Support (gzip, snappy, lz4, zstd)
- ✅ **Phase 9 Complete:** Idempotent Producer (InitProducerId, sequence validation, deduplication)
- ✅ **Phase 10 Complete:** Transaction Support (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
- ✅ **Phase 11 Complete:** Shadow Mode (external Kafka forwarding, SASL/SSL, per-topic config, replay)
- ✅ **Enhancement:** Long Polling (max_wait_ms/min_bytes support, in-memory notification)
- ✅ **Enhancement:** Config & log management (DescribeConfigs, IncrementalAlterConfigs for `retention.ms`, DeleteRecords) and a periodic retention sweep
- ✅ **Enhancement:** Listener security: TLS (rustls, optional mutual TLS) and SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 (SaslHandshake, SaslAuthenticate) on Kafka-style listeners; **no ACLs** (see `docs/AUTHENTICATION.md`)
- ✅ **CI/CD:** GitHub Actions pipeline with automated testing

**What Works Now:**
- TCP listener on port 9092 with full Kafka wire protocol parsing
- ProduceRequest handling with database persistence (acks=0 and acks>=1 supported)
- Idempotent producer support with InitProducerId API and sequence validation
- Producer deduplication (DUPLICATE_SEQUENCE_NUMBER, OUT_OF_ORDER_SEQUENCE_NUMBER detection)
- Transaction support (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
- Read-committed isolation level for consumers (filters pending/aborted transactions)
- FetchRequest with RecordBatch v2 encoding/decoding
- Compression support: gzip, snappy, lz4, zstd (inbound automatic, outbound configurable)
- Consumer group coordinator with automatic partition assignment (Range, RoundRobin, Sticky strategies)
- Automatic rebalancing on member leave or session timeout
- DescribeGroups/ListGroups for consumer group visibility
- OffsetCommit/OffsetFetch for consumer progress tracking
- ListOffsets for earliest/latest/by-timestamp offset queries
- Admin operations: CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups
- Config/log management: DescribeConfigs, IncrementalAlterConfigs (only `retention.ms`; other keys are rejected with INVALID_CONFIG), DeleteRecords
- Time-based retention sweep (`pg_kafka.message_retention_hours`, per-topic `retention.ms`), also runnable on demand via `pg_kafka_run_retention_sweep()`
- Time-partitioned `kafka.messages` (RANGE on `created_at`, `pg_kafka.message_partition_minutes`, default one day, plus a DEFAULT leaf): retention drops whole leaves where every row is expired (ADR-001)
- Multi-partition topics with key-based partition routing (murmur2 hash)
- Dual-offset design (partition_offset + global_offset)
- Repository Pattern storage abstraction (KafkaStore trait + PostgresStore impl)
- Shadow mode for forwarding to external Kafka with SASL/SSL, per-topic config, replay support
- Listener security protocols PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL, several listeners at once (`pg_kafka.additional_listeners`), each advertising its own address; SCRAM credentials in `kafka.sasl_credentials`, managed with `pg_kafka_create_user` & co.; per-connection principal recorded (authorization is not implemented)
- Automated E2E tests with real Kafka client (rdkafka)

**API Coverage:** <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen--> (full table: README "Implemented APIs", generated from the `kafka_apis!` registry)
**Test Status:** unit + property + E2E, all CI-gated. Counts live only in `docs/TEST_STRATEGY.md` ("Test Summary"); do not repeat them elsewhere.
**Coverage Target:** 80%+ (testable code)

## Development Setup

This project uses Docker for development to ensure consistent pgrx and Postgres environments.

### Option 1: VS Code Dev Container (Recommended)

The project includes a `.devcontainer` configuration for seamless VS Code integration:

1. Open the project in VS Code
2. Install the "Dev Containers" extension
3. Press `F1` → "Dev Containers: Reopen in Container"
4. VS Code will build and connect to the container automatically

The devcontainer includes:
- Rust toolchain with nightly channel
- pgrx pre-initialized with pg14
- Rust-analyzer and debugging extensions
- Port forwarding for PostgreSQL (5432) and Kafka Protocol (9092)

### Option 2: Manual Docker Setup

```bash
docker-compose up -d
docker exec -it pg_kafka_dev bash
```

### Git Hooks Setup (Required for Contributors)

**Important:** Install git hooks to enforce code quality before commits:

```bash
./hooks/install.sh
```

This installs two hooks:
- **pre-commit:** `cargo fmt --check` in both the root crate and `kafka_test/` - Ensures code formatting
- **pre-push:** `cargo clippy --all-targets --features pg14 -- -D warnings` (plus the `kafka_test/` equivalent) - Ensures no linting warnings

See [hooks/README.md](hooks/README.md) for more details.

## Build & Test Commands

```bash
# Build and check
cargo build --features pg14          # Build the extension
cargo check --features pg14          # Quick compile check
cargo clippy --features pg14         # Lint with clippy
cargo fmt                            # Format code

# Run Postgres with the extension loaded (interactive PSQL shell)
cargo pgrx run pg14

# Postgres lifecycle
cargo pgrx start pg14                # Start postgres in background
cargo pgrx stop pg14                 # Stop postgres
cargo pgrx status pg14               # Check if running

# Unit tests (fast, no PostgreSQL required)
cargo test --features pg14                           # Run all unit tests
cargo test --features pg14 kafka::protocol           # Run tests under a module path
cargo test --features pg14 test_encode_decode        # Run single test by name
cargo test --features pg14 -- --nocapture            # Show println! output
cargo test --features pg14 --test property_tests     # Property tests only (tests/property_tests.rs)

# pgrx integration tests (limited - see note below)
cargo pgrx test pg14

# E2E tests (requires running PostgreSQL with extension)
cargo pgrx start pg14
cd kafka_test && cargo run --release

# Quick development iteration (kills postgres, rebuilds, restarts, recreates extension)
./scripts/restart.sh

# Test coverage
cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info
cargo llvm-cov --lib --features pg14 --summary-only           # Quick summary
cargo llvm-cov --lib --features pg14 --html --output-dir coverage-report  # HTML report

# Package for installation
cargo pgrx package                   # Creates installable package

# Schema generation (after adding SQL-visible functions)
cargo pgrx schema pg14               # Generate SQL schema files
```

**Note on pgrx tests:** This extension uses `PGC_POSTMASTER` GUCs which require `shared_preload_libraries` BEFORE Postgres starts. `cargo pgrx test` creates the extension AFTER startup, causing "FATAL: cannot create PGC_POSTMASTER variables after startup". Most integration testing is done via the E2E suite in `kafka_test/`. See `docs/PGRX_TESTING_GUIDE.md` for the full testing strategy.

**Note:** The project includes a [rust-toolchain.toml](rust-toolchain.toml) that specifies Rust nightly, which is required for the `home` crate dependency that uses edition2024 features (needed by pgrx 0.16.1). The toolchain file pins a dated nightly (`nightly-2026-11-08`); bump it deliberately and re-run clippy on both crates. The default feature is `pg14` as specified in [Cargo.toml](Cargo.toml); features `pg13` through `pg18` exist. CI (`.github/workflows/ci.yml`, job `e2e`) builds, lints and unit-tests every major from 14 to 18, installs the extension into that PostgreSQL (from apt.postgresql.org) and runs the functional E2E suite and `scripts/check-upgrade.sh` against it (`scripts/ci-pg.sh` brings the cluster up the same way locally); pg14 also runs the shadow and performance categories. PG18-specific code is `#[cfg(feature = "pg18")]`-scoped. `pg13` is built by no CI job (PostgreSQL 13 is end-of-life).

## Code Coverage Requirements

**Targets:**
- Project Coverage: 80%+ (testable code only)
- Patch Coverage: 90%+ (new/modified code)

**Enforcement:**
- CI enforces coverage via Codecov's `project`/`patch` commit statuses (see `codecov.yml`)
- PRs with coverage below threshold will be blocked
- Files in `codecov.yml` ignore list are excluded (SPI-dependent code tested via E2E)

**QA-1 rule — SPI/async change ⇒ E2E test (machine-enforced):**
Code that needs a live Postgres (SPI) or the tokio listener can't be seen by unit coverage,
so the coverage gate can't protect it. `codecov.yml` lists these files twice over: its
`ignore:` list holds the files that are SPI/async/generated through and through (`lib.rs`,
`worker.rs`, `storage/postgres.rs`, `storage/postgres/roles.rs`, `shadow/store.rs`,
`auth/credentials.rs`, `auth/users.rs`, `listener.rs`), and its `# qa1-guard:` lines hold the
files that mix SPI calls with unit-tested logic and so stay in the coverage figures
(`metrics.rs`, `shadow/outbox.rs`, `shadow/primary.rs`). The mitigation is mandatory:
**every behavioural change to a file on either list must ship a fail-before / pass-after
`kafka_test/` regression test.** The `spi-e2e-guard` CI job
(`.github/scripts/require-e2e-for-spi.sh`, which reads both lists) fails a PR that touches
one of them without also changing a Rust file under `kafka_test/src/`, and
`.github/scripts/check-coverage-policy.sh` (lint job) fails when a `src/` file that calls SPI
(`Spi::*`, `BackgroundWorker::*`, `#[pg_extern]`) is on neither list. For a genuinely
test-neutral edit (comments/docs/renames), add the `no-e2e-needed` label to waive the guard.
Do **not** add a file to the ignore list to route untested logic around the patch gate —
prefer extracting the pure logic into a testable free function (see `validate_sequence` in
`storage/mod.rs` and `build_producer_settings` in `shadow/producer.rs` for the pattern).

**Single-DB-thread invariant (why the above is safe):** all handlers, the `Arc<RwLock>`
coordinator, and the `ShadowStore` run on the *one* background-worker DB thread; only the
crossbeam channel ends cross to the tokio network thread. Several structures
(coordinator lock, shadow config-cache reload, metrics RMW) are correct *only* under this
invariant. If you ever make one of them reachable from another thread, you must add
synchronization — do not assume the current check-then-act code is race-free off-thread.

**Quick Check:**
```bash
cargo llvm-cov --lib --features pg14 --summary-only
```

See `docs/TEST_STRATEGY.md` for detailed coverage information.

## Codebase Structure

```
src/
├── lib.rs              # Extension entry point, _PG_init hook, bootstrap SQL include, SQL functions
├── config.rs           # GUC configuration (pg_kafka.port, pg_kafka.host, etc.)
├── worker.rs           # Background worker main loop, request processing, SPI calls
├── testing/            # Test utilities and mocks
│   ├── mod.rs          # Test module exports
│   ├── helpers.rs      # Test fixture/assertion helpers
│   └── mocks.rs        # MockKafkaStore for unit testing handlers/storage
├── kafka/
│   ├── mod.rs          # Module organization, re-exports
│   ├── api/            # One `KafkaApi` impl per Kafka API + the registry (decode, error responses, dispatch)
│   │   ├── mod.rs      # KafkaApi trait, Call, decode_api, kafka_apis! registry (RequestBody/ResponseBody/APIS)
│   │   ├── metadata.rs, produce.rs, fetch.rs, consumer.rs, group.rs, admin.rs, configs.rs, transaction.rs
│   │   ├── sasl.rs     # SaslHandshake/SaslAuthenticate schema (answered on the network thread)
│   │   └── tests.rs    # Registry-wide tests (every advertised version encodes, echoes, header versions)
│   ├── listener.rs     # TCP listener, tokio runtime, connection handling
│   ├── protocol/       # Binary protocol parsing/encoding (uses kafka-protocol crate)
│   │   ├── mod.rs      # Framing and module exports
│   │   ├── decoding.rs # Header parsing; the body goes to the API registry
│   │   ├── encoding.rs # Response encoding (header version from the API's response type)
│   │   └── recordbatch.rs  # RecordBatch v2 / legacy MessageSet parsing
│   ├── messages.rs     # KafkaRequest/KafkaResponse envelopes, request lanes, handler data types
│   ├── metrics.rs      # Broker request metrics (atomics) + periodic flush to kafka.broker_* tables
│   ├── dispatch.rs     # dispatch_response (handler Err/panic → error response) + RequestOutcome
│   ├── context.rs      # Runtime context (shared configuration access)
│   ├── handler_context.rs  # HandlerContext (store, coordinator, broker metadata) passed to handlers
│   ├── broker_metadata.rs  # Advertised host/port for Metadata/FindCoordinator
│   ├── notifications.rs    # DB-thread/network-thread notifications for long polling
│   ├── pending_fetches.rs  # Registry of FetchRequests waiting for data (long polling)
│   ├── coordinator.rs  # Consumer group coordinator (Arc<RwLock> shared state)
│   ├── auth/           # Listener security (TLS, SASL), see docs/AUTHENTICATION.md
│   │   ├── mod.rs      # ConnectionPrincipal, re-exports
│   │   ├── settings.rs # Security protocols, listener specs, the listener-security GUCs
│   │   ├── tls.rs      # rustls ServerConfig from PEM files; client-cert subject -> principal
│   │   ├── session.rs  # Per-connection SASL state machine (network thread), credential channel
│   │   ├── scram.rs    # SCRAM-SHA-256/512 server + stored credential format (RFC 5802/7677)
│   │   ├── plain.rs    # SASL/PLAIN parsing (RFC 4616)
│   │   ├── credentials.rs  # DB-thread credential lookup (SELECT on kafka.sasl_credentials)
│   │   └── users.rs    # pg_kafka_create_user / _alter_user / _set_user_verifier / _drop_user
│   ├── assignment/     # Partition assignment (Phase 4)
│   │   ├── mod.rs
│   │   ├── subscription.rs       # MemberSubscription (JoinGroup metadata) parsing/encoding
│   │   ├── member_assignment.rs  # MemberAssignment (SyncGroup) parsing/encoding
│   │   ├── strategies/           # range.rs, roundrobin.rs, sticky.rs
│   │   └── tests.rs              # Cross-module assignment tests
│   ├── partitioner.rs  # Key-based partition routing (murmur2 hash, Phase 7)
│   ├── response_builders.rs  # Response construction helpers (ApiVersions list from the API registry)
│   ├── constants.rs    # Protocol constants and Kafka error codes
│   ├── error.rs        # Typed errors with Kafka error code mapping (to_kafka_error_code())
│   ├── handlers/       # Protocol request handlers (modular organization)
│   │   ├── mod.rs      # Handler re-exports
│   │   ├── admin.rs    # CreateTopics/DeleteTopics/CreatePartitions/DeleteGroups (Phase 6)
│   │   ├── configs.rs  # DescribeConfigs/IncrementalAlterConfigs/DeleteRecords
│   │   ├── consumer.rs # OffsetCommit/OffsetFetch handlers
│   │   ├── coordinator.rs  # Group coordination handlers (FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups)
│   │   ├── fetch.rs    # Fetch/ListOffsets handlers
│   │   ├── helpers.rs  # Topic resolution utilities (TopicResolution enum)
│   │   ├── init_producer_id.rs  # InitProducerId handler (Phase 9)
│   │   ├── metadata.rs # ApiVersions/Metadata handlers
│   │   ├── produce.rs  # ProduceRequest handler (with sequence validation)
│   │   ├── transaction.rs  # AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/TxnOffsetCommit (Phase 10)
│   │   └── tests.rs    # Handler unit tests with MockKafkaStore
│   ├── shadow/         # Shadow mode (Phase 11; commercial license, see shadow/LICENSE)
│   │   ├── mod.rs      # Module docs, re-exports, forward request/ack types
│   │   ├── config.rs   # ShadowConfig, per-topic TopicShadowConfig, modes
│   │   ├── store.rs    # ShadowStore: store wrapper writing the durable outbox
│   │   ├── forwarder.rs, routing.rs  # Forward decision + percentage routing hash
│   │   ├── producer.rs # rdkafka FutureProducer wrapper (SASL/SSL settings)
│   │   ├── inflight.rs # In-flight forward tracking for the outbox
│   │   ├── primary.rs  # Primary detection (only the primary forwards)
│   │   ├── license.rs  # License key validation
│   │   ├── error.rs    # ShadowError
│   │   └── tests.rs    # Shadow component tests
│   └── storage/        # Storage abstraction layer (Repository Pattern)
│       ├── mod.rs      # Storage role traits, KafkaStore (their sum), delegate_*_store! macros
│       ├── postgres/roles.rs  # PostgresStore's role impls (forward to its inherent SQL methods)
│       ├── postgres.rs # PostgreSQL implementation (PostgresStore)
│       ├── retention.rs # Pure retention planning (windows, batches, time-partition drop/trim decisions)
│       ├── privileges.rs # Worker/reader role grants and the startup role check
│       └── tests.rs    # Storage layer tests
└── bin/
    └── pgrx_embed.rs   # pgrx embedding binary (generated)

tests/
└── property_tests.rs   # proptest suite (run in CI via `cargo test --features pg14 --test property_tests`)
kafka_test/             # E2E test suite (counts: docs/TEST_STRATEGY.md)
    └── src/
        ├── main.rs               # Test orchestrator with CLI + test registry
        ├── lib.rs                # Test re-exports
        ├── common.rs             # Client factories
        ├── setup.rs              # TestContext for isolation
        ├── fixtures.rs           # Test builders
        ├── assertions.rs         # Domain assertions
        ├── admin.rs              # Admin API tests (CreateTopics, DeleteTopics, ...)
        ├── configs.rs            # DescribeConfigs/IncrementalAlterConfigs/DeleteRecords tests
        ├── metadata/             # Metadata tests
        ├── protocol/             # Protocol-level tests (ApiVersions, correlation IDs)
        ├── producer/             # Producer tests
        ├── consumer/             # Consumer tests
        ├── consumer_group/       # Consumer group tests
        ├── offset_management/    # Offset commit/fetch tests
        ├── partition/            # Multi-partition tests
        ├── compression/          # Compression tests
        ├── idempotent/           # Idempotent producer tests
        ├── transaction/          # Transaction tests
        ├── shadow/               # Shadow mode tests (Phase 11)
        ├── auth/                 # TLS + SASL listener tests (reconfigure + restart the worker)
        ├── long_poll/            # Long polling tests
        ├── error_paths/          # Error handling tests
        ├── edge_cases/           # Boundary condition tests
        ├── concurrent/           # Concurrency tests
        ├── negative/             # Expected failure tests
        ├── part/                 # Time partitions + partition-drop retention (ADR-001)
        └── performance/          # Performance benchmarks

sql/
├── bootstrap.sql       # Schema definition (kafka.messages, kafka.topics, kafka.consumer_offsets, ...)
├── pg_kafka--0.0.0--0.1.0.sql  # Upgrade script (ALTER EXTENSION pg_kafka UPDATE; see docs/UPGRADING.md)
├── upgrade-test/       # Frozen 0.0.0 install script used by scripts/check-upgrade.sh
└── tune_autovacuum.sql # Optional performance tuning script

docs/
├── README.md                   # Documentation index
├── KAFKA_PROTOCOL_COVERAGE.md  # Comprehensive API coverage analysis
├── FULL_KAFKA_COMPLIANCE_PLAN.md  # Plan for remaining Kafka APIs
├── PROTOCOL_DEVIATIONS.md      # Intentional deviations from Kafka spec
├── TEST_STRATEGY.md            # Test approach and coverage; the one home of test counts
├── PGRX_TESTING_GUIDE.md       # pgrx testing constraints and strategy
├── REPOSITORY_PATTERN.md       # Storage abstraction design
├── PERFORMANCE.md              # Performance guide and benchmarks
├── SHADOW_MODE_TESTING.md      # Shadow mode setup and testing
├── SHADOW_MODE_FLOW_DIAGRAM.md # Shadow mode flow diagram
├── SHADOW_EVIDENCE_PLAN.md     # Shadow forwarding evidence/reporting plan
├── LICENSING.md                # Dual-licensing model
├── UPGRADING.md                # Extension versioning and the ALTER EXTENSION UPDATE procedure
├── archive/                    # Dated point-in-time records
│   ├── AUDIT-2026-06.md        # June 2026 code audit
│   └── DEEP-REVIEW-2026-07.md  # July 2026 deep review (source of DR-* references)
└── architecture/
    ├── ADR-001-partitioning-and-retention.md  # Design decisions
    └── ADR-002-staff-level-architectural-improvements.md

scripts/
├── restart.sh          # Quick rebuild and restart script for development
├── check-upgrade.sh    # CI: 0.0.0 -> current upgrade must match a fresh install
├── run-shadow-tests.sh # Sets up Docker + PostgreSQL and runs the shadow E2E tests
├── generate-evidence.sh  # Builds the evidence artifacts (conformance, bench, session, shadow)
└── record-session.sh   # Records the kcat produce/consume session as an animated SVG
Cargo.lock              # Locked dependencies for reproducible builds
```

## Architecture

### Async/Sync Bridge Pattern

The core architectural challenge is bridging async Kafka protocol handling with sync PostgreSQL SPI calls. The extension runs **one BackgroundWorker process** managed by Postgres Postmaster:

```
TCP:9092 → Tokio Runtime (listener.rs) → crossbeam-channel → Background Worker (worker.rs) → SPI
```

1. **Async Layer** (`src/kafka/listener.rs`): Tokio runtime accepts TCP connections on every configured listener (TLS handshake where configured), parses Kafka wire protocol. SaslHandshake/SaslAuthenticate never become `KafkaRequest`s: the per-connection `SaslSession` (`src/kafka/auth/session.rs`) answers them on the network thread and gates every other request until authentication succeeds; its credential lookups go to the worker over a separate `CredentialRequest` channel that the main loop `select!`s next to the main lane
2. **Message Queue** (`src/kafka/messages.rs`): `crossbeam-channel` bridges async→sync with `KafkaRequest` values on two lanes (`RequestLanes`: `main`, plus a `liveness` lane for Heartbeat that the worker drains first); each request carries a tokio `response_tx` that the worker answers with a `KafkaResponse`
3. **Sync Layer** (`src/worker.rs`): Background worker processes requests via SPI within postgres transactions. **Group commit:** every request already queued (cap 64, ~50 ms budget, ends early when a Heartbeat waits) runs in one top-level transaction, each request in its own subtransaction (a handler returns `RequestOutcome::Rollback` on `Err`; panics are caught and rolled back), and all responses are released only after the single commit (RB-1). The worker is registered with a 5 s restart interval and exits with status 1 on SIGTERM, so `pg_terminate_backend()` or a crash brings up a fresh listener; it waits for the schema indefinitely. It starts once recovery finishes, so it does not run on a hot standby.

**Critical constraint:** The tokio runtime cannot call SPI directly (different thread context). All database operations must go through the background worker.

**Connection multiplexing:** Postgres processes are heavy (10MB+). Cannot spawn one process per Kafka client. The tokio listener runs in one BGWorker process, handling thousands of TCP connections but using only one SPI connection to the DB.

### Repository Pattern (KafkaStore Trait)

Storage is abstracted via role traits in `src/kafka/storage/mod.rs`, with `KafkaStore` as their sum
(implemented for every type that has all six):

```rust
pub trait TopicStore { fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)>; fn get_topic_id(..); /* catalog, admin, retention.ms */ }
pub trait LogStore { fn insert_records(..) -> Result<i64>; fn fetch_records_with_isolation(..); /* offsets bounds, DeleteRecords */ }
pub trait OffsetStore { fn commit_offset(..); fn fetch_offset(..); /* group offsets */ }
pub trait ProducerStore { /* producer ids, epochs, sequences (Phase 9) */ }
pub trait TransactionStore { /* transactional producers, writes, commit/abort (Phase 10) */ }
pub trait MaintenanceStore { fn abort_timed_out_transactions(..); fn cleanup_aborted_messages(..); }
pub trait KafkaStore: TopicStore + LogStore + OffsetStore + ProducerStore + TransactionStore + MaintenanceStore {}
```

- `PostgresStore` (`src/kafka/storage/postgres.rs`): Production implementation using SPI. The SQL is one inherent `impl PostgresStore` block; `storage/postgres/roles.rs` implements the roles by forwarding to it
- `ShadowStore` (`src/kafka/shadow/store.rs`): Wraps `PostgresStore` and adds shadow-mode forwarding; it forwards the roles it does not change wholesale (`delegate_topic_store!(TopicStore, inner)`, likewise offsets and producer state) and implements `LogStore`, `TransactionStore` and `MaintenanceStore`
- `MockKafkaStore` (`src/testing/mocks.rs`): Test double for handler unit tests
- Helpers take the narrowest role (`resolve_topic_id<S: TopicStore + ?Sized>`); calling store methods on a concrete store type needs `use crate::kafka::storage::roles::*`

This separation enables testing handlers without a running database.

### Handler Organization

Protocol handlers live in `src/kafka/handlers/`:
- Each Kafka API has a `handle_*` function; all except `handle_api_versions` take a `&HandlerContext` (`src/kafka/handler_context.rs`), whose `store: &dyn KafkaStore` field keeps them testable
- The pipeline around them (decode, version range, lane, error responses, dispatch) is the API's `KafkaApi` impl in `src/kafka/api/` (see "Adding a New Kafka API Handler")
- `helpers.rs`: `TopicResolution` enum for topic lookup logic shared across handlers
- `tests.rs`: handler tests using `MockKafkaStore` (more live in per-file `#[cfg(test)]` modules)

### Error Handling

`KafkaError` (`src/kafka/error.rs`) provides typed errors that map to Kafka protocol error codes:

```rust
impl KafkaError {
    pub fn to_kafka_error_code(&self) -> i16 {
        match self {
            KafkaError::UnknownTopic { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            KafkaError::UnknownMemberId { .. } => ERROR_UNKNOWN_MEMBER_ID,
            // ... every variant maps to a specific Kafka error code
        }
    }
}
```

### Consumer Group Coordinator

In-memory state management (`src/kafka/coordinator.rs`):
- `Arc<RwLock<HashMap<String, ConsumerGroup>>>` for group state (each `ConsumerGroup` has a `GroupState`)
- Automatic partition assignment with Range, RoundRobin, and Sticky strategies
- Automatic rebalancing on member leave or session timeout
- Tracks: generation IDs, member registry, partition assignments
- JoinGroup responses are held until the join phase completes (every known member rejoined, or the rest left / timed out), as in Kafka; the generation moves when the join completes. `GroupCoordinator::join_group` takes a `JoinResponder` it calls exactly once (immediately or later, from another member's JoinGroup, a LeaveGroup or the ~1 s `check_and_remove_timed_out_members` sweep). `JoinGroupApi::handle` answers on the client's own sender (`KafkaApi::DEFERRED_REPLY`, so `swap_response_tx` leaves it in place for JoinGroup and SyncGroup), wrapped in `handlers::JoinGroupReply`, which answers UNKNOWN_SERVER_ERROR if dropped unsent. A follower's SyncGroup is held the same way (`handlers::SyncGroupReply`, `pending_syncs`) until the leader's SyncGroup delivers the assignment, so a follower that syncs first waits instead of being told REBALANCE_IN_PROGRESS

### Storage Schema

Kafka's hierarchy (Topic → Partition → Offset) maps to relational schema with a **dual-offset design**:

```sql
-- Excerpt of sql/bootstrap.sql (indexes, GRANTs and COMMENTs omitted)

CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT NOT NULL DEFAULT 1,
    retention_ms BIGINT,               -- Per-topic retention.ms (NULL = inherit pg_kafka.message_retention_hours; < 0 = infinite)
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- The "Log" with dual-offset design
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,           -- Monotonic across ALL partitions (temporal ordering)
    partition_offset BIGINT NOT NULL,  -- Per-partition offset (Kafka protocol compatibility)
    key BYTEA,
    value BYTEA,
    headers JSONB,
    timestamp_ms BIGINT NOT NULL DEFAULT -1,  -- Producer record timestamp (-1 = none; fetch falls back to created_at)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- Broker insert time
    producer_id BIGINT,                -- Phase 10: NULL for non-transactional messages
    producer_epoch SMALLINT,
    txn_state TEXT,                    -- NULL (committed/non-txn), 'pending' (uncommitted), 'aborted'
    CHECK (partition_id >= 0),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);
-- Time partitions (ADR-001): leaves kafka.messages_pYYYYMMDD_HHMMSS of
-- pg_kafka.message_partition_minutes (UTC-aligned) + kafka.messages_default. No primary key on
-- the parent (Postgres needs the partition key in it): offsets are unique because they are
-- allocated from kafka.partition_offsets under lock and never reused; every leaf carries
-- UNIQUE (topic_id, partition_id, partition_offset) and UNIQUE (global_offset) as a backstop.
-- Leaves are extension members (pg_dump leaves them out like the parent).

-- Per-partition monotonic offset counter (never decremented, so offsets are never reused)
CREATE TABLE IF NOT EXISTS kafka.partition_offsets (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL DEFAULT 0,
    log_start_offset BIGINT NOT NULL DEFAULT 0,  -- Advanced by DeleteRecords (API 21)
    CHECK (partition_id >= 0),
    PRIMARY KEY (topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

CREATE TABLE kafka.consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    commit_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK (partition_id >= 0),
    PRIMARY KEY (group_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- NOTE: group MEMBERSHIP is in-memory in the GroupCoordinator; only committed
-- offsets persist. (The former kafka.consumer_groups table was dead schema and
-- was removed — DR-6, DEEP-REVIEW-2026-07.)

-- Phase 9: Idempotent Producer support
CREATE TABLE kafka.producer_ids (
    producer_id BIGSERIAL PRIMARY KEY,
    epoch SMALLINT NOT NULL DEFAULT 0,
    client_id TEXT,
    transactional_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_active_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE kafka.producer_sequences (
    producer_id BIGINT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    last_sequence INT NOT NULL DEFAULT -1,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK (partition_id >= 0),
    PRIMARY KEY (producer_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Phase 10: Transaction support
CREATE TABLE kafka.transactions (
    transactional_id TEXT PRIMARY KEY,
    producer_id BIGINT NOT NULL REFERENCES kafka.producer_ids(producer_id) ON UPDATE CASCADE,
    producer_epoch SMALLINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'Empty',  -- Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit, CompleteAbort
    timeout_ms INT NOT NULL DEFAULT 60000,
    started_at TIMESTAMPTZ,
    last_updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE kafka.txn_pending_offsets (
    transactional_id TEXT NOT NULL REFERENCES kafka.transactions(transactional_id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL REFERENCES kafka.topics(id) ON DELETE CASCADE,
    partition_id INT NOT NULL,
    pending_offset BIGINT NOT NULL,
    metadata TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CHECK (partition_id >= 0),
    PRIMARY KEY (transactional_id, group_id, topic_id, partition_id)
);

-- Phase 11 (Shadow Mode): kafka.shadow_config, kafka.shadow_tracking (durable outbox),
-- kafka.shadow_metrics, view kafka.shadow_status, function kafka.replay_shadow_messages().
-- Monitoring: view kafka.metrics (row-count snapshot); views kafka.api_metrics (per-API
-- requests/errors/latency), kafka.broker_metrics (connections, request-queue depth) and
-- kafka.consumer_lag. The first two read UNLOGGED tables the worker upserts at most once per
-- second when its in-memory counters (src/kafka/metrics.rs) changed.
```

**Why Dual Offsets?**
- `global_offset`: Provides total temporal ordering across ALL messages (useful for replication, debugging)
- `partition_offset`: Kafka clients expect per-partition monotonic offsets starting from 0

**Offset Counter:**
- `partition_offsets`: Next `partition_offset` per partition plus the durable log start offset, so offsets stay strictly increasing even after aborted rows are cleaned up or records are deleted

**Idempotent Producer Tables:**
- `producer_ids`: Tracks allocated producer IDs with epochs (fencing mechanism)
- `producer_sequences`: Per-partition sequence tracking for deduplication

**Transaction Tables (Phase 10):**
- `transactions`: Tracks transaction state by transactional_id
- `txn_pending_offsets`: Pending offset commits until transaction completes
- `messages.txn_state`: Controls visibility (NULL=visible, 'pending'=invisible to read_committed)

**Roles (0.1.0):** the extension creates two NOLOGIN roles. `pgkafka_reader` has read-only
access to the kafka schema (nothing is granted to PUBLIC any more; grant this role to readers).
`pgkafka_worker` holds exactly the privileges the worker's SQL needs (listed in
`src/kafka/storage/privileges.rs`, whose unit tests check the GRANTs in `sql/bootstrap.sql` and
scan the worker's SQL for uncovered tables/verbs). A new worker statement on a new table, or a new
worker-written table (e.g. a metrics table), needs a matching GRANT plus an entry there.
`kafka.sasl_credentials` (SCRAM verifiers) is readable only by superusers and `pgkafka_worker`
(SELECT); `pgkafka_reader` gets the secret-free `kafka.sasl_users` view.

**Time partitions (ADR-001, 0.1.0):** all partition DDL runs in SECURITY DEFINER functions in
`sql/bootstrap.sql` (fixed `search_path`, `lock_timeout`, each refuses anything but a leaf of
`kafka.messages`): `kafka.ensure_message_partitions(width_minutes, ahead)` (worker: at start and
every sweep interval; creates the current + next two leaves and moves rows out of the DEFAULT
leaf), `kafka.drop_message_partition(leaf, expired_topics)` (re-checks under lock),
`kafka.message_partition_contents(leaf)`, internal `kafka.create_message_partition`;
`kafka.message_partitions()` lists the leaves. `pgkafka_worker` gets only EXECUTE on them
(`WORKER_FUNCTION_GRANTS` in `privileges.rs`). The drop/trim decisions are pure functions in
`src/kafka/storage/retention.rs`. Retention for a window >= one width is leaf-granular (a row
lives at most window + width, like Kafka segments); shorter windows and the DEFAULT leaf are
expired row by row. Never write an UPDATE that changes `created_at` (it would move rows between
leaves), and confine a ctid-based statement to one leaf (`tableoid`), since ctids repeat across leaves. Statements on
`kafka.messages` that do not name `created_at` plan over every leaf: hot ones go through
`select_cached` in `postgres.rs` (kept prepared plans, executed with a fresh snapshot).

**SQL Functions** (from `src/lib.rs`, in the `public` schema):
- `pg_kafka_run_retention_sweep(message_retention_hours INT DEFAULT NULL, aborted_grace_seconds BIGINT DEFAULT 60)`: Runs one retention sweep on demand (the worker runs the same sweep periodically) and returns `(category, deleted)` rows for `aborted_messages`, `expired_messages`, `dropped_partitions` (whole time partitions dropped), `stale_producers`, `terminal_transactions`, `shadow_delivered_rows`. A NULL first argument uses `pg_kafka.message_retention_hours`.
- `pg_kafka_run_partition_maintenance(partition_minutes INT DEFAULT NULL)`: Runs one time-partition maintenance pass on demand (same as the worker's) and returns the number of leaves created. NULL uses `pg_kafka.message_partition_minutes`.
- `hello_pg_kafka()`: Smoke-test function returning `'Hello, pg_kafka'`.
- `pg_kafka_create_user(username, password, iterations INT DEFAULT 4096)`, `pg_kafka_alter_user(...)`, `pg_kafka_set_user_verifier(username, verifier)`, `pg_kafka_drop_user(username) RETURNS bool`: manage SASL users (`src/kafka/auth/users.rs`; SECURITY INVOKER, so only roles that may write `kafka.sasl_credentials`, i.e. superusers by default)

## Configuration

The complete, generated reference (type, default, range, context of every GUC) is
`docs/CONFIGURATION.md`; the block below is an annotated overview.

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_kafka'

-- Read at worker start (GucContext::Postmaster, requires restart)
pg_kafka.port = 9092              -- TCP port (default: 9092; range 1024-65535)
pg_kafka.host = '127.0.0.1'       -- Bind address (default: 127.0.0.1 = local clients only; set '0.0.0.0' for all interfaces)
pg_kafka.database = 'postgres'    -- Database where CREATE EXTENSION pg_kafka was run (default: postgres)
pg_kafka.log_connections = false  -- Log each connection
pg_kafka.shutdown_timeout_ms = 5000  -- Listener shutdown timeout (100-60000ms)
pg_kafka.default_partitions = 1   -- Default partitions for auto-created topics (1-10000)
pg_kafka.compression_type = 'none' -- Outbound compression (none, gzip, snappy, lz4, zstd)
pg_kafka.log_timing = false       -- Enable timing instrumentation for benchmarking
pg_kafka.enable_long_polling = true  -- Enable long polling for FetchRequest
pg_kafka.fetch_poll_interval_ms = 1000 -- Long-poll safety-net re-check (10-60000ms); waiting fetches otherwise wake only on Produce/EndTxn notifications or max_wait_ms
pg_kafka.run_as_role = 'pgkafka_worker' -- Role the worker switches to once the schema exists ('' = stay bootstrap superuser;
                                          -- a missing role or privilege logs a WARNING and keeps the superuser)

-- Reloadable (GucContext::Sighup, pg_reload_conf())
pg_kafka.message_retention_hours = 0 -- Retention sweep for kafka.messages (0 = keep forever; 0-87600; DR-2)
pg_kafka.message_partition_minutes = 1440 -- Width of one time partition of kafka.messages (5-43200; new leaves only; ADR-001)
pg_kafka.config_reload_interval_ms = 30000  -- How often the worker reloads GUCs + shadow config (100-300000ms)

-- Test-only fault injection (Sighup, default 0 = off; never set in production)
pg_kafka.test_forward_ack_delay_ms = 0   -- Delay before the shadow forwarder acks (0-30000ms)
pg_kafka.test_pre_commit_delay_ms = 0    -- Delay between a Produce handler and its commit (0-10000ms)

-- Listener security (Sighup, but read only when the worker starts: reload, then
-- pg_terminate_backend() the pg_kafka_listener worker; see docs/AUTHENTICATION.md)
pg_kafka.security_protocol = 'PLAINTEXT'   -- primary listener: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
pg_kafka.additional_listeners = ''         -- 'SASL_SSL://0.0.0.0:9093,SSL://:9094' (empty host = pg_kafka.host)
pg_kafka.ssl_cert_file = ''                -- PEM chain (required by SSL / SASL_SSL listeners)
pg_kafka.ssl_key_file = ''                 -- PEM private key
pg_kafka.ssl_ca_file = ''                  -- client CA bundle; set = mutual TLS on TLS listeners
pg_kafka.sasl_enabled_mechanisms = 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-512'

-- See "Shadow Mode Configuration" section below for shadow mode settings
```

## Shadow Mode Configuration (Phase 11)

Shadow mode allows pg_kafka to forward messages to an external Kafka cluster while still storing them locally. This enables dual-write patterns for migration, validation, or hybrid architectures.

```sql
-- Enable shadow mode globally (Sighup)
pg_kafka.shadow_mode_enabled = true                    -- Default: false

-- External Kafka connection
pg_kafka.shadow_bootstrap_servers = 'kafka1:9092,kafka2:9092'  -- Required (Sighup)
pg_kafka.shadow_security_protocol = 'SASL_SSL'         -- Default: SASL_SSL (use PLAINTEXT for local/no-auth) (Sighup)
pg_kafka.shadow_sasl_mechanism = 'SCRAM-SHA-256'       -- Default: PLAIN (restart)
pg_kafka.shadow_sasl_username = 'your_username'        -- Default: empty (restart; superuser-only, hidden from SHOW ALL)
pg_kafka.shadow_sasl_password = 'your_password'        -- Default: empty (restart; superuser-only, hidden from SHOW ALL)
pg_kafka.shadow_ssl_ca_location = '/path/to/ca-cert'   -- Default: empty (restart)

-- Batching and retry configuration (Suset)
pg_kafka.shadow_batch_size = 1000                      -- Messages per batch (default 1000; 1-100000)
pg_kafka.shadow_linger_ms = 10                         -- Batch delay (default 10; 0-60000ms)
pg_kafka.shadow_retry_backoff_ms = 100                 -- Retry delay (default 100; 10-60000ms)
pg_kafka.shadow_max_retries = 3                        -- Max retries (default 3; 0-100)
pg_kafka.shadow_external_primary_strict = false        -- Keep external_only topics read-suppressed even after a dead-lettered forward (default false)

-- Observability (Suset)
pg_kafka.shadow_metrics_enabled = true                 -- Enable metrics collection (kafka.shadow_metrics)

-- License (restart; superuser-only, hidden from SHOW ALL)
pg_kafka.shadow_license_key = 'eval'                   -- 'sponsor_id:token' or 'eval'
```

**Per-Topic Shadow Configuration:**

Shadow mode is configured per-topic via the `kafka.shadow_config` table, keyed by `topic_id`. Columns: `mode` (`'local_only'` | `'shadow'`), `forward_percentage` (0-100), `external_topic_name`, `sync_mode` (`'async'` | `'sync'`, default `'sync'`; there is no global default GUC; `sync` forwards right after the local commit and holds the produce response until the external ack or 5 s), `write_mode` (`'dual_write'` | `'external_only'`). The worker picks up changes on its next config reload (`pg_kafka.config_reload_interval_ms`, or immediately after `pg_reload_conf()`).

```sql
-- Forward specific topics with different settings
INSERT INTO kafka.shadow_config (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode)
SELECT id, 'shadow', 100, 'prod-orders', 'sync', 'dual_write' FROM kafka.topics WHERE name = 'orders';      -- 100% sync forwarding
INSERT INTO kafka.shadow_config (topic_id, mode, forward_percentage, external_topic_name, sync_mode, write_mode)
SELECT id, 'shadow', 50, 'prod-analytics', 'async', 'dual_write' FROM kafka.topics WHERE name = 'analytics'; -- 50% async sampling
SELECT pg_reload_conf();

-- Replay stored messages to external Kafka: re-queues partition_offset range
-- [p_from_offset, p_to_offset) of a topic into the outbox (p_to_offset NULL = to the end).
-- Only committed/non-transactional rows are replayed. Returns the number of rows queued.
SELECT kafka.replay_shadow_messages(p_topic_id => 1, p_from_offset => 0, p_to_offset => NULL);

-- Monitoring
SELECT * FROM kafka.shadow_status;
```

**Use Cases:**
- **Migration**: Dual-write to external Kafka during pg_kafka → Kafka migration
- **Validation**: Compare pg_kafka behavior against production Kafka
- **Hybrid Architecture**: Store locally + forward to centralized Kafka cluster

## Testing with Kafka Clients

```bash
# List topics (after cargo pgrx start pg14)
kcat -L -b localhost:9092

# Produce messages
echo "key1:value1" | kcat -P -b localhost:9092 -t my-topic -K:

# Consume messages
kcat -C -b localhost:9092 -t my-topic -p 0 -o beginning

# Verify in database
psql -h localhost -p 28814 -U postgres -d postgres -c \
  "SELECT topic_id, partition_id, partition_offset, key, value FROM kafka.messages;"
```

## Development Workflow

### Adding New Functionality

1. **Add Rust code** to [src/lib.rs](src/lib.rs) or new modules
2. **Mark functions with `#[pg_extern]`** to expose them to SQL
3. **Run `cargo pgrx run pg14`** to start Postgres with your extension loaded
4. **Test in psql**: Functions are available in the `public` schema by default
5. **Write tests**: `#[pg_test]` can't run here (see the PGC_POSTMASTER note above), so cover SQL-visible functions with a `kafka_test/` E2E test (e.g. `edge_cases/retention_sweep.rs` calls `pg_kafka_run_retention_sweep()`)

### Adding a New Kafka API Handler

Every API is described once, by a type implementing `KafkaApi` (`src/kafka/api/`), and listed
once in the `kafka_apis!` registry at the bottom of `src/kafka/api/mod.rs`. The registry
generates the `RequestBody`/`ResponseBody` enums, the decode/encode/dispatch matches, the
ApiVersions list, the metrics names and the error path; nothing else lists the APIs.

1. Write the handler in `src/kafka/handlers/` taking `&HandlerContext` (its
   `store: &dyn KafkaStore` keeps it testable) and returning the kafka-protocol response or a
   `KafkaError`. Add unit tests with `MockKafkaStore` (`src/kafka/handlers/tests.rs`).
2. In the fitting `src/kafka/api/*.rs` file, add a request data struct and `impl KafkaApi`:
   - `type Wire` = the kafka-protocol request type (its `KEY` is the api key, its `Response`
     the response body); `NAME`, `MIN_VERSION`, `MAX_VERSION` (advertised and enforced),
     and `LANE` / `DEFERRED_REPLY` if not the defaults (main lane, reply after commit);
   - `decode`: wire request → your data struct (return `Rejection::Reply` to answer an
     invalid request with an error, `Rejection::Silent` to send nothing);
   - `type Echo` + `echo` + `error_response`: what an error response needs from the request
     (`()` when the response has a top-level error code; the requested topics/partitions/
     groups otherwise, so every entry carries the code);
   - `response_has_error` (metrics) and `handle`, usually
     `call.run(request, |r, header| handlers::handle_x(&env.ctx, ...))`.
3. Add `Variant => YourApi,` to `kafka_apis!` (and an `API_KEY_*` constant in `constants.rs`
   if other code needs the key). The registry tests (`src/kafka/api/tests.rs`) then check that
   every advertised version encodes and that the error responses encode.
4. Add an E2E test under `kafka_test/src/` and register it in `kafka_test/src/main.rs`.

**An API answered on the network thread** (from per-connection state, never sent to the
worker; today SaslHandshake/SaslAuthenticate, answered by `auth::session` from
`listener.rs`) implements only `ApiSchema` (wire types, name, versions, error response,
`response_has_error`; see `src/kafka/api/sasl.rs`) and goes in the registry's
`network_thread { .. }` section. It is advertised, encoded and named from the table like
the others (`ApiInfo::served_by` is `ServedBy::NetworkThread`) but has no `RequestBody`
variant: the listener must intercept its frames before `parse_request`, build responses as
`ResponseBody::<Variant>`, and count them with `metrics::RequestTimer` itself.

## E2E Testing Best Practices (kafka_test/)

### Test Infrastructure

Tests use shared infrastructure from `kafka_test/src/`:
- **common.rs**: Client factories (`create_producer()`, `create_idempotent_producer()`, `create_transactional_producer()`, `create_stream_consumer()`, etc.)
- **setup.rs**: `TestContext` for RAII-based isolation with automatic cleanup
- **fixtures.rs**: Builders (`TestTopicBuilder`, `TestConsumerBuilder`, `TestProducerBuilder`)
- **assertions.rs**: Domain-specific assertions (`assert_topic_exists()`, `assert_message_count()`, etc.)

### Test Categories

Per-category counts are in `docs/TEST_STRATEGY.md`; `cargo run --release -- --list` prints the live registry and total.

| Category | Purpose |
|----------|---------|
| **admin** | CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups, DescribeConfigs, IncrementalAlterConfigs, DeleteRecords, per-topic retention |
| **producer** | Basic produce, batch produce, acks=0, ack-implies-committed, timestamp roundtrip |
| **consumer** | Basic consume, from offset, multiple messages, headers |
| **consumer_group** | Lifecycle, two-member groups, rebalancing, strategies |
| **offset_management** | Commit/fetch, boundary conditions, multi-partition, by-timestamp, no offset reuse |
| **partition** | Multi-partition topics, key routing, distribution |
| **compression** | gzip, snappy, lz4, zstd, roundtrip, edge cases |
| **idempotent** | InitProducerId, deduplication, epoch, multi-partition |
| **transaction** | Transactions, isolation levels, EOS, fencing, atomicity, timeouts |
| **shadow** | External forwarding, SASL/SSL, topic mapping, replay, dial-up |
| **long_poll** | Timeout behavior, immediate return, producer wakeup, precision, per-connection task cap |
| **error_paths** | Invalid partitions, unknown topics, coordinator errors, protocol/transaction/group regressions |
| **edge_cases** | Empty topics, large messages, boundary values, retention sweep |
| **part** | Time partitions of kafka.messages: whole-leaf retention drops, row-delete fallback, pending-transaction blocking, maintenance, least privilege |
| **concurrent** | Multi-producer, multi-consumer, request pipelining, races, heartbeat liveness lane |
| **negative** | Connection refused, timeouts, expected failures, superuser-only SASL password GUC |
| **performance** | Throughput baselines, latency percentiles, scaling |
| **metadata** | Metadata refresh, all topics, nonexistent topic |
| **protocol** | ApiVersions negotiation, correlation ID, unknown API, pipelining, response ordering |
| **auth** | SASL_PLAINTEXT (PLAIN, SCRAM-SHA-256/512), SSL (+ untrusted CA, mutual TLS), SASL_SSL, wrong passwords, pre-auth gating, user SQL. Sequential: generates certs with the `openssl` CLI, adds listeners 19093-19095 via ALTER SYSTEM and restarts the worker; the last test resets them. Needs the DB host's filesystem and superuser |

### Running E2E Tests with CLI Options

```bash
# Run all tests
cd kafka_test && cargo run --release

# Run specific category
cargo run --release -- --category compression
cargo run --release -- --category shadow

# Run single test by name
cargo run --release -- --test test_idempotent_producer_basic

# Exclude categories (repeatable; CI runs functional tests this way)
cargo run --release -- --exclude performance --exclude shadow

# By default parallel_safe tests run concurrently (up to --concurrency, default 16)
# and the rest run sequentially; force everything sequential
cargo run --release -- --sequential

# JSON output for CI
cargo run --release -- --json

# List all available tests
cargo run --release -- --list

# Per-test timeout (default 300 s). Each test runs in its own task: a panic or a
# timeout is reported as FAILED and the run continues
cargo run --release -- --timeout-secs 120

# Combine flags
cargo run --release -- --category transaction --json
```

### Standard Test Structure

Every test follows a 4-step pattern:

```rust
pub async fn test_feature_name() -> TestResult {
    println!("=== Test: Feature Name ===\n");

    // 1. SETUP: Create context and unique resources
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("feature").await;
    let group = ctx.unique_group("consumer").await;

    // 2. ACTION: Execute test scenario
    println!("Step 1: Performing action...");
    let producer = create_producer()?;
    producer.send(FutureRecord::to(&topic).payload("test"), Duration::from_secs(5)).await?;
    println!("✅ Action completed\n");

    // 3. VERIFY: Assert via database queries
    println!("=== Database Verification ===\n");
    let count_row = ctx.db().query_one(
        "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
        &[&topic]
    ).await?;
    let count: i64 = count_row.get(0);
    assert_eq!(count, 1, "Expected 1 message");
    println!("✅ Verification passed\n");

    // 4. CLEANUP: Automatic via TestContext drop (or explicit)
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
```

### Key Principles

1. **Always use unique resource names**: `ctx.unique_topic()` and `ctx.unique_group()` prevent test pollution
2. **Two-level verification**: Verify both Kafka protocol response AND database state
3. **Use shared client factories**: `create_producer()`, `create_idempotent_producer()`, `create_transactional_producer()`
4. **Use ctx.db() for queries**: Don't call `create_db_client()` separately when using TestContext
5. **Mark parallel safety**: Set `parallel_safe: false` in main.rs for timing-sensitive or concurrent tests
6. **Verbose output**: Print step-by-step progress with checkmarks for CI debugging

### Adding New E2E Tests

1. Create test in appropriate category directory (e.g., `producer/`, `consumer/`, `idempotent/`)
2. Export from the category's `mod.rs`
3. Re-export from `lib.rs`
4. Register in `main.rs` with category and `parallel_safe` flag
5. Follow the standard 4-step structure
6. Use TestContext for isolation

### When rdkafka Doesn't Support a Feature

If rdkafka lacks support for a Kafka feature:
1. Test using raw TCP protocol encoding (see `idempotent/protocol_encoding.rs` for helpers)
2. Verify behavior via database queries using `ctx.db()`
3. Document the approach in test comments
4. Reference unit tests in `src/kafka/handlers/tests.rs` for comprehensive coverage

### Debugging

The devcontainer includes:
- **vscode-lldb** extension for debugging Rust code
- **SYS_PTRACE** capability enabled for gdb/perf profiling
- **rust-analyzer** for IDE integration

To debug:
1. Set breakpoints in VS Code
2. Use the debugger on the Postgres process spawned by `cargo pgrx run`
3. Check logs with `RUST_LOG=debug cargo pgrx run pg14`
   (the network thread's `tracing` output goes to the server's stderr; with `RUST_LOG` unset it logs `pg_kafka=warn` and above, and a set `RUST_LOG` replaces that filter)

### Important pgrx Patterns

**Background Workers:**
- Use `pgrx::bgworkers::BackgroundWorker` to spawn persistent processes
- Register in `_PG_init()` hook for startup initialization
- Critical for the TCP listener implementation

**SPI (Server Programming Interface):**
- Use `Spi::connect()` to execute SQL from Rust
- Wrap in transactions for consistency
- See pgrx examples: `Spi::execute()`, `Spi::get_one()`, etc.

**Memory Context:**
- Postgres has its own memory management
- Use pgrx's `PgMemoryContexts` for allocations that must survive function calls
- Background workers need careful memory handling

## Key Dependencies

- **pgrx 0.16.1:** Postgres extension framework for Rust (requires nightly toolchain)
- **tokio 1.48:** Async runtime embedded in BackgroundWorker
- **kafka-protocol 0.17:** Kafka wire protocol parsing/encoding (patched via `[patch.crates-io]` to a pinned fork that bounds allocation and decompression sizes, SEC-5/SEC-9)
- **crossbeam-channel 0.5:** Message passing between async and sync layers
- **rdkafka 0.36:** Shadow-mode producer to external Kafka (main crate, system OpenSSL) and E2E testing with a real Kafka client (in kafka_test/)

## Non-Goals (v1)

- Full Kafka Protocol Compliance: Focus on core APIs, not 100% feature parity
- High Availability: Rely on standard Postgres HA (Patroni/RDS) rather than Kafka's ISR
- Broker Clustering: Single-node "broker" design (scale via Postgres replication)

## Future Enhancements

- Cooperative rebalancing (KIP-429)
- Static group membership (KIP-345)
- Log compaction (KIP-58)
- Read replica support via PostgreSQL streaming replication
