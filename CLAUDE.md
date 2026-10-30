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
- Multi-partition topics with key-based partition routing (murmur2 hash)
- Dual-offset design (partition_offset + global_offset)
- Repository Pattern storage abstraction (KafkaStore trait + PostgresStore impl)
- Shadow mode for forwarding to external Kafka with SASL/SSL, per-topic config, replay support
- Automated E2E tests with real Kafka client (rdkafka)

**API Coverage:** 26 of ~50 standard Kafka APIs (52%)
**Test Status:** 713 unit (incl. 4 in-module proptests) + 10 property + 202 E2E ≈ 925 total ✅ (CI is the source of truth; counts drift)
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

**Note:** The project includes a [rust-toolchain.toml](rust-toolchain.toml) that specifies Rust nightly, which is required for the `home` crate dependency that uses edition2024 features (needed by pgrx 0.16.1). The default feature is `pg14` as specified in [Cargo.toml](Cargo.toml); features `pg13` through `pg18` exist, and CI additionally checks and unit-tests `pg15`, `pg16` and `pg17`.

## Code Coverage Requirements

**Targets:**
- Project Coverage: 80%+ (testable code only)
- Patch Coverage: 90%+ (new/modified code)

**Enforcement:**
- CI enforces coverage via Codecov's `project`/`patch` commit statuses (see `codecov.yml`)
- PRs with coverage below threshold will be blocked
- Files in `codecov.yml` ignore list are excluded (SPI-dependent code tested via E2E)

**QA-1 rule — SPI/async change ⇒ E2E test (machine-enforced):**
The SPI/async files in the `codecov.yml` `ignore:` list (`worker.rs`, `storage/postgres.rs`,
`shadow/store.rs`, `listener.rs`) can't be seen by unit coverage, so the coverage gate
can't protect them. The mitigation is mandatory: **every behavioural change to one of
those files must ship a fail-before / pass-after `kafka_test/` regression test.** This is
enforced by the `spi-e2e-guard` CI job (`.github/scripts/require-e2e-for-spi.sh`), which
fails a PR that touches one of those files without also changing a Rust file under
`kafka_test/src/`. The guard derives its file list from every `src/` entry in that
`ignore:` list, so it also fires on `src/lib.rs` and `src/kafka/handler_context.rs`. For a genuinely test-neutral edit (comments/docs/renames), add the
`no-e2e-needed` label to waive it. Do **not** add a file to the ignore list to route
untested logic around the patch gate — prefer extracting the pure logic into a testable
free function (see `validate_sequence` in `storage/mod.rs` and `build_producer_settings`
in `shadow/producer.rs` for the pattern).

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
│   ├── listener.rs     # TCP listener, tokio runtime, connection handling
│   ├── protocol/       # Binary protocol parsing/encoding (uses kafka-protocol crate)
│   │   ├── mod.rs      # Framing and module exports
│   │   ├── decoding.rs # Request decoding + version enforcement
│   │   ├── encoding.rs # Response encoding
│   │   └── recordbatch.rs  # RecordBatch v2 / legacy MessageSet parsing
│   ├── messages.rs     # Request/response types, message queues
│   ├── dispatch.rs     # Request dispatch helpers used by worker.rs
│   ├── context.rs      # Runtime context (shared configuration access)
│   ├── handler_context.rs  # HandlerContext (store, coordinator, broker metadata) passed to handlers
│   ├── broker_metadata.rs  # Advertised host/port for Metadata/FindCoordinator
│   ├── notifications.rs    # DB-thread/network-thread notifications for long polling
│   ├── pending_fetches.rs  # Registry of FetchRequests waiting for data (long polling)
│   ├── coordinator.rs  # Consumer group coordinator (Arc<RwLock> shared state)
│   ├── assignment/     # Partition assignment (Phase 4)
│   │   ├── mod.rs
│   │   ├── subscription.rs       # MemberSubscription (JoinGroup metadata) parsing/encoding
│   │   ├── member_assignment.rs  # MemberAssignment (SyncGroup) parsing/encoding
│   │   ├── strategies/           # range.rs, roundrobin.rs, sticky.rs
│   │   └── tests.rs              # Cross-module assignment tests
│   ├── partitioner.rs  # Key-based partition routing (murmur2 hash, Phase 7)
│   ├── response_builders.rs  # Response construction helpers, API_VERSION_RANGES table
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
│   │   ├── store.rs    # ShadowStore: KafkaStore wrapper writing the durable outbox
│   │   ├── forwarder.rs, routing.rs  # Forward decision + percentage routing hash
│   │   ├── producer.rs # rdkafka FutureProducer wrapper (SASL/SSL settings)
│   │   ├── inflight.rs # In-flight forward tracking for the outbox
│   │   ├── primary.rs  # Primary detection (only the primary forwards)
│   │   ├── license.rs  # License key validation
│   │   ├── error.rs    # ShadowError
│   │   └── tests.rs    # Shadow component tests
│   └── storage/        # Storage abstraction layer (Repository Pattern)
│       ├── mod.rs      # KafkaStore trait definition
│       ├── postgres.rs # PostgreSQL implementation (PostgresStore)
│       └── tests.rs    # Storage layer tests
└── bin/
    └── pgrx_embed.rs   # pgrx embedding binary (generated)

tests/
└── property_tests.rs   # proptest suite (run in CI via `cargo test --features pg14 --test property_tests`)
kafka_test/             # E2E test suite (202 tests)
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
        ├── long_poll/            # Long polling tests
        ├── error_paths/          # Error handling tests
        ├── edge_cases/           # Boundary condition tests
        ├── concurrent/           # Concurrency tests
        ├── negative/             # Expected failure tests
        └── performance/          # Performance benchmarks

sql/
├── bootstrap.sql       # Schema definition (kafka.messages, kafka.topics, kafka.consumer_offsets, ...)
└── tune_autovacuum.sql # Optional performance tuning script

docs/
├── README.md                   # Documentation index
├── KAFKA_PROTOCOL_COVERAGE.md  # Comprehensive API coverage analysis
├── FULL_KAFKA_COMPLIANCE_PLAN.md  # Plan for remaining Kafka APIs
├── PROTOCOL_DEVIATIONS.md      # Intentional deviations from Kafka spec
├── TEST_STRATEGY.md            # Test approach and coverage
├── PGRX_TESTING_GUIDE.md       # pgrx testing constraints and strategy
├── REPOSITORY_PATTERN.md       # Storage abstraction design
├── PERFORMANCE.md              # Performance guide and benchmarks
├── SHADOW_MODE_TESTING.md      # Shadow mode setup and testing
├── SHADOW_MODE_FLOW_DIAGRAM.md # Shadow mode flow diagram
├── SHADOW_EVIDENCE_PLAN.md     # Shadow forwarding evidence/reporting plan
├── LICENSING.md                # Dual-licensing model
├── AUDIT-2026-06.md            # June 2026 code audit
├── DEEP-REVIEW-2026-07.md      # July 2026 deep review
└── architecture/
    ├── ADR-001-partitioning-and-retention.md  # Design decisions
    └── ADR-002-staff-level-architectural-improvements.md

scripts/
├── restart.sh          # Quick rebuild and restart script for development
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

1. **Async Layer** (`src/kafka/listener.rs`): Tokio runtime accepts TCP connections, parses Kafka wire protocol
2. **Message Queue** (`src/kafka/messages.rs`): `crossbeam-channel` bridges async→sync with `KafkaRequest` values on two lanes (`RequestLanes`: `main`, plus a `liveness` lane for Heartbeat that the worker drains first); each request carries a tokio `response_tx` that the worker answers with a `KafkaResponse`
3. **Sync Layer** (`src/worker.rs`): Background worker processes requests via SPI within postgres transactions

**Critical constraint:** The tokio runtime cannot call SPI directly (different thread context). All database operations must go through the background worker.

**Connection multiplexing:** Postgres processes are heavy (10MB+). Cannot spawn one process per Kafka client. The tokio listener runs in one BGWorker process, handling thousands of TCP connections but using only one SPI connection to the DB.

### Repository Pattern (KafkaStore Trait)

Storage is abstracted via the `KafkaStore` trait (`src/kafka/storage/mod.rs`):

```rust
pub trait KafkaStore {
    fn get_or_create_topic(&self, name: &str, default_partitions: i32) -> Result<(i32, i32)>;
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64>;
    fn fetch_records(&self, topic_id: i32, partition_id: i32, fetch_offset: i64, max_bytes: i32) -> Result<Vec<FetchedMessage>>;
    fn commit_offset(&self, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> Result<()>;
    // ... more methods
}
```

- `PostgresStore` (`src/kafka/storage/postgres.rs`): Production implementation using SPI
- `ShadowStore` (`src/kafka/shadow/store.rs`): Wraps `PostgresStore` and adds shadow-mode forwarding
- `MockKafkaStore` (`src/testing/mocks.rs`): Test double for handler unit tests

This separation enables testing handlers without a running database.

### Handler Organization

Protocol handlers live in `src/kafka/handlers/`:
- Each Kafka API has a `handle_*` function; all except `handle_api_versions` take a `&HandlerContext` (`src/kafka/handler_context.rs`), whose `store: &dyn KafkaStore` field keeps them testable
- `helpers.rs`: `TopicResolution` enum for topic lookup logic shared across handlers
- `tests.rs`: 80 handler tests using `MockKafkaStore` (more live in per-file `#[cfg(test)]` modules)

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
    PRIMARY KEY (topic_id, partition_id, partition_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE,
    UNIQUE (global_offset)
);

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
-- Monitoring: view kafka.metrics.
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

**SQL Functions** (from `src/lib.rs`, in the `public` schema):
- `pg_kafka_run_retention_sweep(message_retention_hours INT DEFAULT NULL, aborted_grace_seconds BIGINT DEFAULT 60)`: Runs one retention sweep on demand (the worker runs the same sweep periodically) and returns `(category, deleted)` rows for `aborted_messages`, `expired_messages`, `stale_producers`, `terminal_transactions`, `shadow_delivered_rows`. A NULL first argument uses `pg_kafka.message_retention_hours`.
- `hello_pg_kafka()`: Smoke-test function returning `'Hello, pg_kafka'`.

## Configuration

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_kafka'

-- Read at worker start (GucContext::Postmaster, requires restart)
pg_kafka.port = 9092              -- TCP port (default: 9092; range 1024-65535)
pg_kafka.host = '0.0.0.0'         -- Bind address (default: 0.0.0.0)
pg_kafka.database = 'postgres'    -- Database where CREATE EXTENSION pg_kafka was run (default: postgres)
pg_kafka.log_connections = false  -- Log each connection
pg_kafka.shutdown_timeout_ms = 5000  -- Listener shutdown timeout (100-60000ms)
pg_kafka.default_partitions = 1   -- Default partitions for auto-created topics (1-10000)
pg_kafka.compression_type = 'none' -- Outbound compression (none, gzip, snappy, lz4, zstd)
pg_kafka.log_timing = false       -- Enable timing instrumentation for benchmarking
pg_kafka.enable_long_polling = true  -- Enable long polling for FetchRequest
pg_kafka.fetch_poll_interval_ms = 100  -- Fallback polling interval (10-5000ms)

-- Reloadable (GucContext::Sighup, pg_reload_conf())
pg_kafka.message_retention_hours = 0 -- Retention sweep for kafka.messages (0 = keep forever; 0-87600; DR-2)
pg_kafka.config_reload_interval_ms = 30000  -- How often the worker reloads GUCs + shadow config (100-300000ms)

-- Test-only fault injection (Sighup, default 0 = off; never set in production)
pg_kafka.test_forward_ack_delay_ms = 0   -- Delay before the shadow forwarder acks (0-30000ms)
pg_kafka.test_pre_commit_delay_ms = 0    -- Delay between a Produce handler and its commit (0-10000ms)

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
pg_kafka.shadow_default_sync_mode = 'async'            -- Default: async. Parsed into ShadowConfig but not currently consulted;
                                                       -- the per-topic kafka.shadow_config.sync_mode column (default 'sync') decides
pg_kafka.shadow_external_primary_strict = false        -- Keep external_only topics read-suppressed even after a dead-lettered forward (default false)

-- Observability (Suset)
pg_kafka.shadow_metrics_enabled = true                 -- Enable metrics collection (kafka.shadow_metrics)
pg_kafka.shadow_otel_endpoint = 'http://otel:4317'     -- OpenTelemetry endpoint (default: empty = disabled)

-- License (restart; superuser-only, hidden from SHOW ALL)
pg_kafka.shadow_license_key = 'eval'                   -- 'sponsor_id:token' or 'eval'
```

**Per-Topic Shadow Configuration:**

Shadow mode is configured per-topic via the `kafka.shadow_config` table, keyed by `topic_id`. Columns: `mode` (`'local_only'` | `'shadow'`), `forward_percentage` (0-100), `external_topic_name`, `sync_mode` (`'async'` | `'sync'`), `write_mode` (`'dual_write'` | `'external_only'`). The worker picks up changes on its next config reload (`pg_kafka.config_reload_interval_ms`, or immediately after `pg_reload_conf()`).

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

1. Add the API key to `src/kafka/constants.rs` and its supported version range to `API_VERSION_RANGES` in `src/kafka/response_builders.rs` (advertised in ApiVersions and enforced on decode)
2. Add `KafkaRequest`/`KafkaResponse` variants in `src/kafka/messages.rs`, decoding in `src/kafka/protocol/decoding.rs`, and encoding in `src/kafka/protocol/encoding.rs`
3. Create the handler function in the appropriate file under `src/kafka/handlers/`, taking `&HandlerContext` (its `store: &dyn KafkaStore` keeps it testable)
4. Use `KafkaError` variants for error handling (they map to Kafka error codes)
5. Add unit tests in `src/kafka/handlers/tests.rs` using `MockKafkaStore`
6. Wire up the handler in the `src/worker.rs` request processing loop
7. Add an E2E test under `kafka_test/src/` and register it in `kafka_test/src/main.rs`

## E2E Testing Best Practices (kafka_test/)

### Test Infrastructure

Tests use shared infrastructure from `kafka_test/src/`:
- **common.rs**: Client factories (`create_producer()`, `create_idempotent_producer()`, `create_transactional_producer()`, `create_stream_consumer()`, etc.)
- **setup.rs**: `TestContext` for RAII-based isolation with automatic cleanup
- **fixtures.rs**: Builders (`TestTopicBuilder`, `TestConsumerBuilder`, `TestProducerBuilder`)
- **assertions.rs**: Domain-specific assertions (`assert_topic_exists()`, `assert_message_count()`, etc.)

### Test Categories (202 tests)

Counts are from the registry in `kafka_test/src/main.rs` (`--list` prints it); CI is the source of truth.

| Category | Tests | Purpose |
|----------|-------|---------|
| **admin** | 19 | CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups, DescribeConfigs, IncrementalAlterConfigs, DeleteRecords, per-topic retention |
| **producer** | 5 | Basic produce, batch produce, acks=0, ack-implies-committed, timestamp roundtrip |
| **consumer** | 4 | Basic consume, from offset, multiple messages, headers |
| **consumer_group** | 13 | Lifecycle, two-member groups, rebalancing, strategies |
| **offset_management** | 10 | Commit/fetch, boundary conditions, multi-partition, by-timestamp, no offset reuse |
| **partition** | 9 | Multi-partition topics, key routing, distribution |
| **compression** | 10 | gzip, snappy, lz4, zstd, roundtrip, edge cases |
| **idempotent** | 7 | InitProducerId, deduplication, epoch, multi-partition |
| **transaction** | 20 | Transactions, isolation levels, EOS, fencing, atomicity, timeouts |
| **shadow** | 27 | External forwarding, SASL/SSL, topic mapping, replay, dial-up |
| **long_poll** | 10 | Timeout behavior, immediate return, producer wakeup, precision, per-connection task cap |
| **error_paths** | 21 | Invalid partitions, unknown topics, coordinator errors |
| **edge_cases** | 13 | Empty topics, large messages, boundary values, retention sweep |
| **concurrent** | 14 | Multi-producer, multi-consumer, request pipelining, races, heartbeat liveness lane |
| **negative** | 5 | Connection refused, timeouts, expected failures, superuser-only SASL password GUC |
| **performance** | 7 | Throughput baselines, latency percentiles, scaling |
| **metadata** | 3 | Metadata refresh, all topics, nonexistent topic |
| **protocol** | 5 | ApiVersions negotiation, correlation ID, unknown API, pipelining, response ordering |

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

- Table partitioning (for high-volume use cases; time-based retention already exists via `pg_kafka.message_retention_hours` and per-topic `retention.ms`)
- Cooperative rebalancing (KIP-429)
- Static group membership (KIP-345)
- Log compaction (KIP-58)
- Read replica support via PostgreSQL streaming replication
