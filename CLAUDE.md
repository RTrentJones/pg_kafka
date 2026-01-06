# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pg_kafka` is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire protocol listener directly into the Postgres runtime. It allows standard Kafka clients to produce and consume messages using Postgres as the backing store.

**Status:** Phase 3B Complete - Full Producer/Consumer Support (Portfolio/Learning Project)

**Current Implementation:**
- ✅ **Phase 1 Complete:** Metadata support (ApiVersions, Metadata requests)
- ✅ **Phase 2 Complete:** Producer support (ProduceRequest, database storage, E2E tests, Repository Pattern)
- ✅ **Phase 3 Complete:** Consumer support (FetchRequest, ListOffsets, OffsetCommit/Fetch)
- ✅ **Phase 3B Complete:** Consumer group coordinator (FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup)
- ✅ **CI/CD:** GitHub Actions pipeline with automated testing
- ⏳ **Phase 4:** Automatic partition assignment strategies
- ⏳ **Phase 5:** Automatic rebalancing
- ⏳ **Phase 6:** Shadow Mode (Logical Decoding → external Kafka)

**What Works Now:**
- TCP listener on port 9092 with full Kafka wire protocol parsing
- ProduceRequest handling with database persistence
- FetchRequest with RecordBatch v2 encoding/decoding
- Consumer group coordinator with manual partition assignment
- OffsetCommit/OffsetFetch for consumer progress tracking
- ListOffsets for earliest/latest offset queries
- Dual-offset design (partition_offset + global_offset)
- Repository Pattern storage abstraction (KafkaStore trait + PostgresStore impl)
- Automated E2E tests with real Kafka client (rdkafka) - 5 test scenarios passing

**API Coverage:** 12 of ~50 standard Kafka APIs (24%)
**Test Status:** All E2E tests passing ✅

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

### Key Commands

Once inside the container:

```bash
# Update Rust toolchain (required for pgrx 0.16.1 dependencies)
rustup update

# Initialize pgrx (if not already done - auto-runs via devcontainer postCreateCommand)
cargo pgrx init --pg14 download

# Build and check
cargo build --features pg14          # Build the extension
cargo check --features pg14          # Quick compile check
cargo clippy --features pg14         # Lint with clippy
cargo fmt                            # Format code

# Run Postgres with the extension loaded (interactive PSQL shell)
cargo pgrx run pg14

# Testing
cargo pgrx test pg14                 # Run all tests
cargo test --features pg14           # Run unit tests only

# Package for installation
cargo pgrx package                   # Creates installable package

# Schema generation (after adding SQL-visible functions)
cargo pgrx schema pg14               # Generate SQL schema files
```

**Note:** The project includes a [rust-toolchain.toml](rust-toolchain.toml) that specifies Rust nightly, which is required for the `home` crate dependency that uses edition2024 features (needed by pgrx 0.16.1). The default feature is `pg14` as specified in [Cargo.toml](Cargo.toml).

## Current Codebase Structure

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

.github/
├── workflows/
│   └── ci.yml          # GitHub Actions CI/CD pipeline (lint, test, security, lockfile)
└── SETUP.md            # CI/CD setup guide
```

**Implemented (Phase 1-3B):**
- ✅ Kafka wire protocol parser using kafka-protocol crate v0.17
- ✅ Background worker with TCP listener on port 9092
- ✅ Storage schema with dual-offset design (3 tables)
- ✅ Repository Pattern storage abstraction (KafkaStore trait)
- ✅ SPI integration for database operations
- ✅ Request handlers: ApiVersions, Metadata, Produce, Fetch, ListOffsets, OffsetCommit, OffsetFetch, FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup
- ✅ Consumer group coordinator with thread-safe state management
- ✅ RecordBatch v2 encoding/decoding
- ✅ E2E tests with automated database verification (5 scenarios)
- ✅ CI/CD pipeline with GitHub Actions

**Planned (Phase 4-6):**
- ⏳ Automatic partition assignment strategies (Range, RoundRobin, Sticky)
- ⏳ Automatic rebalancing on member join/leave
- ⏳ Member timeout detection
- ⏳ Long polling via LISTEN/NOTIFY (optional optimization)
- ⏳ Shadow replication worker using Logical Decoding
- ⏳ Table partitioning and retention policies

## Architecture (Target Design)

### Core Components

The extension introduces **two persistent BackgroundWorker processes** managed by the Postgres Postmaster:

1. **Protocol Listener (Ingress):**
   - Binds to TCP port 9092
   - Rust tokio runtime running inside a Postgres BackgroundWorker
   - Flow: Accepts connections → Parses Request bytes → Maps to SPI calls → Writes to Postgres Tables
   - **Critical constraint:** Cannot block the Postgres main loop. Uses pgrx's background worker support with embedded tokio runtime.

2. **Replicator (Egress/Shadow Mode):**
   - Reads Postgres WAL and produces to external Kafka
   - Uses Postgres Logical Decoding (via pgoutput plugin)
   - Flow: Tails WAL → Decodes INSERT → Sends ProduceRequest to Real Kafka
   - **Key feature:** Enables zero-downtime migration from pg_kafka to real Kafka

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
    PRIMARY KEY (topic_id, partition_id, partition_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE,
    UNIQUE (global_offset)  -- Ensures global ordering is strictly monotonic
);

-- Metadata
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT DEFAULT 1
);

-- Consumer group offset tracking (Phase 3)
CREATE TABLE kafka.consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    commit_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);
```

**Why Dual Offsets?**
- `global_offset`: Provides total temporal ordering across ALL messages (useful for replication, debugging)
- `partition_offset`: Kafka clients expect per-partition monotonic offsets starting from 0
- Allows queries like "show me all messages in temporal order" AND "fetch from partition X starting at offset Y"

### Protocol Implementation

**ProduceRequest Handler:**
1. Parse binary frame (Size + Header + Body)
2. Attach to Postgres Shared Memory (SPI Context)
3. Start Postgres Transaction
4. Execute `INSERT INTO kafka.messages ... RETURNING offset`
5. Serialize ProduceResponse with new offset

**FetchRequest Handler:**
1. Query: `SELECT * FROM kafka.messages WHERE topic_id=$1 AND partition_id=$2 AND "offset" >= $3 LIMIT $4`
2. Implement Long Poll using Postgres `LISTEN/NOTIFY`
3. Fetch handler waits on condition variable; Produce handler fires `NOTIFY` upon commit

## Technical Constraints

### Connection Multiplexing
- Postgres processes are heavy (10MB+). Cannot spawn one process per Kafka client.
- **Solution:** The tokio listener runs in one BGWorker process, handling thousands of TCP connections but using only one SPI connection to the DB.

### Async/Sync Integration
- If tokio blocks on a DB lock, port 9092 stops responding
- **Solution:** Use pgrx's `run_in_background` for SPI calls to keep async runtime responsive for heartbeats

### Offset Guarantees
- BIGSERIAL can have gaps if transactions roll back
- Kafka clients tolerate offset gaps, but must ensure strictly monotonic increase
- Postgres Sequence guarantees this property

## Implementation Phases

### Phase 1: Metadata Support
- Scaffold pgrx extension
- Implement `ApiVersions` and `Metadata` requests
- Bind TCP listener in `_PG_init`
- Test: `kcat -L -b localhost:9092` returns metadata

### Phase 2: Producer
- Implement `ProduceRequest` parser
- Wire up SPI INSERT
- Handle ACKs logic
- Test: `kcat -P` writes data to Postgres table

### Phase 3: Consumer
- Implement `FetchRequest`
- Implement Long Polling via ConditionVariable
- Test: `kcat -C` reads data back

### Phase 4: Shadow Mode (Key Feature)
- Implement Logical Decoding client
- Integrate rdkafka crate for external Kafka production
- Only advance Replication Slot LSN after external broker ACK (At-Least-Once delivery)
- Test: Data written to port 9092 appears in external Kafka cluster

## Non-Goals (v1)

- Full Protocol Compliance: Consumer Groups have manual assignment only (no automatic rebalancing), no Transactions or Compression
- High Availability: Rely on standard Postgres HA (Patroni/RDS) rather than Kafka's ISR
- Broker Clustering: Single-node "broker" design

## Key Dependencies

- **pgrx 0.16.1:** Postgres extension framework for Rust (requires nightly toolchain)
- **tokio:** (Planned) Async runtime embedded in BackgroundWorker
- **rdkafka:** (Planned) Rust Kafka client for Shadow Mode replication
- Kafka Wire Protocol v2+ specification

## Development Workflow

### Adding New Functionality

1. **Add Rust code** to [src/lib.rs](src/lib.rs) or new modules
2. **Mark functions with `#[pg_extern]`** to expose them to SQL
3. **Run `cargo pgrx run pg14`** to start Postgres with your extension loaded
4. **Test in psql**: Functions are available in the `public` schema by default
5. **Write tests** in the `tests` module using `#[pg_test]` attribute

### Testing Workflow

```sql
-- After running: cargo pgrx run pg14
-- You'll be in a psql shell:

SELECT hello_pg_kafka();  -- Test your extension functions
```

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
