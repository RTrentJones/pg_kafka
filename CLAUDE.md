# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pg_kafka` is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire protocol listener directly into the Postgres runtime. It allows standard Kafka clients to produce and consume messages using Postgres as the backing store.

**Status:** Phase 2 Complete - Producer Support Functional (Portfolio/Learning Project)

**Current Implementation:**
- ✅ **Phase 1 Complete:** Metadata support (ApiVersions, Metadata requests)
- ✅ **Phase 2 Complete:** Producer support (ProduceRequest, database storage, E2E tests)
- ✅ **CI/CD:** GitHub Actions pipeline with automated testing
- ⏳ **Phase 3:** Consumer support (FetchRequest, long polling)
- ⏳ **Phase 4:** Shadow Mode (Logical Decoding → external Kafka)

**What Works Now:**
- TCP listener on port 9092 with full Kafka wire protocol parsing
- ProduceRequest handling with database persistence
- Dual-offset design (partition_offset + global_offset)
- Automated E2E tests with real Kafka client (rdkafka)

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
├── kafka/
│   ├── mod.rs          # Module organization, re-exports
│   ├── listener.rs     # TCP listener, tokio runtime, connection handling
│   ├── protocol.rs     # Binary protocol parsing (uses kafka-protocol crate)
│   ├── messages.rs     # Request/response types, message queue
│   ├── response_builders.rs  # Response construction helpers
│   ├── constants.rs    # Protocol constants
│   └── error.rs        # Error types
├── testing/
│   ├── mod.rs          # Test infrastructure
│   ├── mocks.rs        # Mock implementations for testing
│   └── helpers.rs      # Test helper functions
└── bin/
    └── pgrx_embed.rs   # pgrx embedding binary (generated)

tests/                  # pgrx integration tests
kafka_test/             # E2E test binary using rdkafka client
    └── src/
        └── main.rs     # Automated E2E test with database verification

sql/
├── pg_kafka--0.0.0.sql # Schema definition (kafka.messages, kafka.topics)
└── tune_autovacuum.sql # Optional performance tuning script

docs/
├── PERFORMANCE.md      # Performance guide and benchmarks
└── architecture/
    └── ADR-001-partitioning-and-retention.md  # Design decisions

.github/
├── workflows/
│   └── ci.yml          # GitHub Actions CI/CD pipeline
└── SETUP.md            # CI/CD setup guide
```

**Implemented (Phase 1-2):**
- ✅ Kafka wire protocol parser using kafka-protocol crate
- ✅ Background worker with TCP listener on port 9092
- ✅ Storage schema with dual-offset design
- ✅ SPI integration for database operations
- ✅ Request handlers: ApiVersions, Metadata, Produce
- ✅ E2E tests with automated database verification
- ✅ CI/CD pipeline with GitHub Actions

**Planned (Phase 3-4):**
- ⏳ Fetch request handler (Consumer support)
- ⏳ Long polling via LISTEN/NOTIFY
- ⏳ Consumer group management
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

-- Partition state tracking (for offset computation)
CREATE TABLE kafka.partition_state (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id, partition_id),
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

- Full Protocol Compliance: No Consumer Groups, Transactions, or Compression in v1
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
