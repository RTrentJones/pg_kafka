# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pg_kafka` is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire protocol listener directly into the Postgres runtime. It allows standard Kafka clients to produce and consume messages using Postgres as the backing store.

**Status:** Phase 5 Complete - Automatic Rebalancing (Portfolio/Learning Project)

**Current Implementation:**
- ✅ **Phase 1 Complete:** Metadata support (ApiVersions, Metadata requests)
- ✅ **Phase 2 Complete:** Producer support (ProduceRequest, database storage, E2E tests, Repository Pattern)
- ✅ **Phase 3 Complete:** Consumer support (FetchRequest, ListOffsets, OffsetCommit/Fetch)
- ✅ **Phase 3B Complete:** Consumer group coordinator (FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup)
- ✅ **Phase 4 Complete:** Automatic partition assignment strategies (Range, RoundRobin, Sticky)
- ✅ **Phase 5 Complete:** Automatic rebalancing (LeaveGroup trigger, timeout detection, REBALANCE_IN_PROGRESS)
- ✅ **CI/CD:** GitHub Actions pipeline with automated testing
- ⏳ **Phase 6:** Shadow Mode (Logical Decoding → external Kafka)

**What Works Now:**
- TCP listener on port 9092 with full Kafka wire protocol parsing
- ProduceRequest handling with database persistence
- FetchRequest with RecordBatch v2 encoding/decoding
- Consumer group coordinator with automatic partition assignment (Range, RoundRobin, Sticky strategies)
- Automatic rebalancing on member leave or session timeout
- DescribeGroups/ListGroups for consumer group visibility
- OffsetCommit/OffsetFetch for consumer progress tracking
- ListOffsets for earliest/latest offset queries
- Dual-offset design (partition_offset + global_offset)
- Repository Pattern storage abstraction (KafkaStore trait + PostgresStore impl)
- Automated E2E tests with real Kafka client (rdkafka)

**API Coverage:** 14 of ~50 standard Kafka APIs (28%)
**Test Status:** All unit tests (163) and E2E tests passing ✅

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

This installs a pre-commit hook that runs:
- `cargo fmt --check` - Ensures code formatting
- `cargo clippy --features pg14 -- -D warnings` - Ensures no linting warnings

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
cargo test --features pg14 protocol_tests            # Run specific test module
cargo test --features pg14 test_encode_decode        # Run single test by name
cargo test --features pg14 -- --nocapture            # Show println! output

# pgrx integration tests (limited - see note below)
cargo pgrx test pg14

# E2E tests (requires running PostgreSQL with extension)
cargo pgrx start pg14
cd kafka_test && cargo run --release

# Quick development iteration (kills postgres, rebuilds, restarts, recreates extension)
./restart.sh

# Test coverage
cargo llvm-cov --lib --features pg14 --lcov --output-path lcov.info

# Package for installation
cargo pgrx package                   # Creates installable package

# Schema generation (after adding SQL-visible functions)
cargo pgrx schema pg14               # Generate SQL schema files
```

**Note on pgrx tests:** This extension uses `PGC_POSTMASTER` GUCs which require `shared_preload_libraries` BEFORE Postgres starts. `cargo pgrx test` creates the extension AFTER startup, causing "FATAL: cannot create PGC_POSTMASTER variables after startup". Most integration testing is done via the E2E suite in `kafka_test/`.

**Note:** The project includes a [rust-toolchain.toml](rust-toolchain.toml) that specifies Rust nightly, which is required for the `home` crate dependency that uses edition2024 features (needed by pgrx 0.16.1). The default feature is `pg14` as specified in [Cargo.toml](Cargo.toml).

## Codebase Structure

```
src/
├── lib.rs              # Extension entry point, _PG_init hook, schema creation
├── config.rs           # GUC configuration (pg_kafka.port, pg_kafka.host, etc.)
├── worker.rs           # Background worker main loop, request processing, SPI calls
├── testing/            # Test utilities and mocks
│   ├── mod.rs          # Test module exports
│   └── mocks.rs        # MockKafkaStore for unit testing handlers/storage
├── kafka/
│   ├── mod.rs          # Module organization, re-exports
│   ├── listener.rs     # TCP listener, tokio runtime, connection handling
│   ├── protocol.rs     # Binary protocol parsing/encoding (uses kafka-protocol crate)
│   ├── messages.rs     # Request/response types, message queues
│   ├── coordinator.rs  # Consumer group coordinator (Arc<RwLock> shared state)
│   ├── response_builders.rs  # Response construction helpers
│   ├── constants.rs    # Protocol constants and Kafka error codes
│   ├── error.rs        # Typed errors with Kafka error code mapping (to_kafka_error_code())
│   ├── handlers/       # Protocol request handlers (modular organization)
│   │   ├── mod.rs      # Handler re-exports
│   │   ├── consumer.rs # OffsetCommit/OffsetFetch handlers
│   │   ├── coordinator.rs  # Group coordination handlers (JoinGroup, Heartbeat, etc.)
│   │   ├── fetch.rs    # Fetch/ListOffsets handlers
│   │   ├── helpers.rs  # Topic resolution utilities (TopicResolution enum)
│   │   ├── metadata.rs # ApiVersions/Metadata handlers
│   │   ├── produce.rs  # ProduceRequest handler
│   │   └── tests.rs    # Handler unit tests (14 tests with MockKafkaStore)
│   └── storage/        # Storage abstraction layer (Repository Pattern)
│       ├── mod.rs      # KafkaStore trait definition
│       ├── postgres.rs # PostgreSQL implementation (PostgresStore)
│       └── tests.rs    # Storage layer tests (22 tests)
└── bin/
    └── pgrx_embed.rs   # pgrx embedding binary (generated)

tests/                  # pgrx integration tests (limited due to PGC_POSTMASTER)
kafka_test/             # E2E test suite using rdkafka client
    └── src/
        └── main.rs     # Automated E2E tests (5 scenarios, all passing)

sql/
├── pg_kafka--0.0.0.sql # Schema definition (kafka.messages, kafka.topics, kafka.consumer_offsets)
└── tune_autovacuum.sql # Optional performance tuning script

docs/
├── KAFKA_PROTOCOL_COVERAGE.md  # Comprehensive API coverage analysis
├── PROTOCOL_DEVIATIONS.md      # Intentional deviations from Kafka spec
├── PHASE_3B_COORDINATOR_DESIGN.md  # Consumer group coordinator design
├── TEST_STRATEGY.md            # Test approach and coverage
├── REPOSITORY_PATTERN.md       # Storage abstraction design
├── PERFORMANCE.md              # Performance guide and benchmarks
└── architecture/
    └── ADR-001-partitioning-and-retention.md  # Design decisions

restart.sh              # Quick rebuild and restart script for development
Cargo.lock              # Locked dependencies for reproducible builds
```

## Architecture

### Async/Sync Bridge Pattern

The core architectural challenge is bridging async Kafka protocol handling with sync PostgreSQL SPI calls. The extension runs **one BackgroundWorker process** managed by Postgres Postmaster:

```
TCP:9092 → Tokio Runtime (listener.rs) → crossbeam-channel → Background Worker (worker.rs) → SPI
```

1. **Async Layer** (`src/kafka/listener.rs`): Tokio runtime accepts TCP connections, parses Kafka wire protocol
2. **Message Queue** (`src/kafka/messages.rs`): `crossbeam-channel` bridges async→sync with `RequestMessage`/`ResponseMessage`
3. **Sync Layer** (`src/worker.rs`): Background worker processes requests via SPI within postgres transactions

**Critical constraint:** The tokio runtime cannot call SPI directly (different thread context). All database operations must go through the background worker.

**Connection multiplexing:** Postgres processes are heavy (10MB+). Cannot spawn one process per Kafka client. The tokio listener runs in one BGWorker process, handling thousands of TCP connections but using only one SPI connection to the DB.

### Repository Pattern (KafkaStore Trait)

Storage is abstracted via the `KafkaStore` trait (`src/kafka/storage/mod.rs`):

```rust
pub trait KafkaStore {
    fn get_or_create_topic(&self, name: &str) -> Result<i32>;
    fn insert_records(&self, topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64>;
    fn fetch_records(&self, topic_id: i32, partition_id: i32, fetch_offset: i64, max_bytes: i32) -> Result<Vec<FetchedMessage>>;
    fn commit_offset(&self, group_id: &str, topic_id: i32, partition_id: i32, offset: i64, metadata: Option<&str>) -> Result<()>;
    // ... more methods
}
```

- `PostgresStore` (`src/kafka/storage/postgres.rs`): Production implementation using SPI
- `MockKafkaStore` (`src/testing/mocks.rs`): Test double for handler unit tests

This separation enables testing handlers without a running database.

### Handler Organization

Protocol handlers live in `src/kafka/handlers/`:
- Each Kafka API has a handler function taking `&dyn KafkaStore` for testability
- `helpers.rs`: `TopicResolution` enum for topic lookup logic shared across handlers
- `tests.rs`: 14 handler tests using `MockKafkaStore`

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
- `Arc<RwLock<HashMap<String, GroupState>>>` for thread-safe group state
- Currently supports manual partition assignment only (no automatic rebalancing)
- Tracks: generation IDs, member registry, partition assignments

### Storage Schema

Kafka's hierarchy (Topic → Partition → Offset) maps to relational schema with a **dual-offset design**:

```sql
-- The "Log" with dual-offset design
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,           -- Monotonic across ALL partitions (temporal ordering)
    partition_offset BIGINT NOT NULL,  -- Per-partition offset (Kafka protocol compatibility)
    key BYTEA,
    value BYTEA,
    headers JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic_id, partition_id, partition_offset)
);

CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT DEFAULT 1
);

CREATE TABLE kafka.consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    commit_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, partition_id)
);
```

**Why Dual Offsets?**
- `global_offset`: Provides total temporal ordering across ALL messages (useful for replication, debugging)
- `partition_offset`: Kafka clients expect per-partition monotonic offsets starting from 0

## Configuration

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_kafka'

-- Network configuration (requires restart)
pg_kafka.port = 9092              -- TCP port (default: 9092)
pg_kafka.host = '0.0.0.0'         -- Bind address (default: 0.0.0.0)

-- Runtime configuration
pg_kafka.log_connections = false  -- Log each connection
pg_kafka.shutdown_timeout_ms = 5000
```

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
5. **Write tests** in the `tests` module using `#[pg_test]` attribute

### Adding a New Kafka API Handler

1. Create handler function in appropriate file under `src/kafka/handlers/`
2. Handler should take `&dyn KafkaStore` parameter for testability
3. Use `KafkaError` variants for error handling (they map to Kafka error codes)
4. Add unit tests in `src/kafka/handlers/tests.rs` using `MockKafkaStore`
5. Wire up handler in `src/worker.rs` request processing loop
6. Add E2E test in `kafka_test/src/main.rs`

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
- **kafka-protocol 0.17:** Kafka wire protocol parsing/encoding
- **crossbeam-channel 0.5:** Message passing between async and sync layers
- **rdkafka 0.36:** E2E testing with real Kafka client (in kafka_test/)

## Non-Goals (v1)

- Full Protocol Compliance: No Transactions or Compression support
- High Availability: Rely on standard Postgres HA (Patroni/RDS) rather than Kafka's ISR
- Broker Clustering: Single-node "broker" design

## Planned Features (Phase 6)

- Long polling via LISTEN/NOTIFY (optional optimization)
- Shadow replication worker using Logical Decoding
- Table partitioning and retention policies
- Cooperative rebalancing (KIP-429)
- Static group membership (KIP-345)
