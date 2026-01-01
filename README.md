# pg_kafka 🐘➡️🕸️

[![CI/CD Pipeline](https://github.com/YOUR_USERNAME/pg_kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/pg_kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/pg_kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/pg_kafka)

**A high-performance PostgreSQL extension that speaks the Kafka Wire Protocol.**

`pg_kafka` allows standard Kafka clients (Java, Python, Go, etc.) to produce and consume messages directly from a PostgreSQL database. It runs as a native background worker in Postgres itself, eliminating the network hop of external proxies and the operational overhead of managing a separate Kafka cluster for small-to-medium workloads.

---

⚠️ **Status: Phase 1 Complete - Metadata Support Functional**

**Current Implementation:**
- ✅ **Phase 1 Complete:** TCP listener with Kafka protocol support
  - ✅ Background worker with tokio async runtime
  - ✅ ApiVersions request/response (API key 18)
  - ✅ Metadata request/response (API key 3)
  - ✅ Configurable port/host via GUC parameters
  - ✅ Full async/sync architecture (tokio ↔ Postgres SPI bridge)

**What Works Now:**
```bash
# Query broker metadata
kcat -L -b localhost:9092
# Returns: 1 broker (localhost:9092), 1 test topic with 1 partition
```

**Next Steps:**
- 🚧 Phase 1.5: Comprehensive unit test coverage (planned)
- ⏳ Phase 2: Database integration (storage schema, Produce/Fetch APIs)
- ⏳ Phase 3: Consumer features (long polling via LISTEN/NOTIFY)
- ⏳ Phase 4: Shadow replication (Logical Decoding → external Kafka)

---

## 🚀 Why pg_kafka?

Teams often choose Postgres for queues to keep infrastructure simple, but hit a "scaling cliff" that forces a painful rewrite to Kafka. `pg_kafka` bridges this gap:

1. **Dev/Test:** Use standard Kafka clients with zero infrastructure setup
2. **Production:** Run on RDS/Aurora until you hit scale limits
3. **Migration:** Use the built-in **Shadow Replicator** to dual-write to a real Kafka cluster and switch over with zero downtime

## 🛠️ Architecture

* **Language:** Rust (via [`pgrx`](https://github.com/pgcentralfoundation/pgrx) 0.16.1)
* **Protocol:** Kafka v2+ (ApiVersions, Metadata implemented; Produce, Fetch planned)
* **Storage:** Native Postgres Heap Tables (planned Phase 2)
* **Concurrency:** `tokio` async runtime embedded in Postgres Background Worker

### Core Components (Current)

**Async/Sync Architecture:**
- **Tokio Runtime:** Handles thousands of TCP connections on port 9092
- **Message Queue:** Bridges async network I/O and sync database operations
- **Protocol Parser:** Binary Kafka wire protocol (size-prefixed frames)
- **Background Worker:** Main thread processes requests via message queue

**Why This Design?**
- Postgres SPI is NOT thread-safe → must run on main thread
- Network I/O benefits from async → tokio handles 1000s of connections
- Message queue cleanly separates concerns → no deadlocks

## 🏃 Quick Start

### Prerequisites

- Docker and Docker Compose
- VS Code with "Dev Containers" extension (recommended)

### Option 1: VS Code Dev Container (Recommended)

1. Open the project in VS Code
2. Press `F1` → "Dev Containers: Reopen in Container"
3. Wait for the container to build and initialize
4. Run the extension:
   ```bash
   cargo pgrx run pg14
   ```

### Option 2: Manual Docker Setup

```bash
# Start the development container
docker-compose up -d

# Connect to the container
docker exec -it pg_kafka_dev bash

# Update Rust toolchain to nightly (required)
rustup update

# Run Postgres with the extension loaded
cargo pgrx run pg14
```

## 📦 Building and Testing

### Build Commands

```bash
# Build the extension
cargo build --features pg14

# Run clippy for linting
cargo clippy --features pg14

# Format code
cargo fmt

# Package for installation
cargo pgrx package
```

### Running Tests

```bash
# Run all pgrx tests (including integration tests)
cargo pgrx test pg14

# Run unit tests only
cargo test --features pg14
```

### Testing the Extension

Once you run `cargo pgrx run pg14`, you'll be in a psql shell:

```sql
-- Test the basic function
SELECT hello_pg_kafka();

-- Check background worker status
SELECT pid, backend_type, state, query
FROM pg_stat_activity
WHERE backend_type LIKE '%pg_kafka%';
```

In the Postgres logs, you should see:
```
LOG:  pg_kafka GUC parameters initialized
LOG:  pg_kafka background worker started (port: 9092, host: 0.0.0.0)
LOG:  pg_kafka TCP listener bound to 0.0.0.0:9092
```

## 🧪 Testing with Kafka Clients

The development container includes `kcat` (formerly kafkacat) for testing:

### Currently Working (Phase 1):

```bash
# Test broker metadata (ApiVersions + Metadata)
kcat -L -b localhost:9092

# Expected output:
# Metadata for all topics (from broker 1: localhost:9092):
#  1 broker:
#   broker 1 at localhost:9092
#  1 topic:
#   topic "test-topic" with 1 partition:
#     partition 0, leader 1, replicas: 1, isrs: 1
```

### Planned (Phase 2+):

```bash
# Produce messages (Phase 2 - planned)
echo "test message" | kcat -P -b localhost:9092 -t test_topic

# Consume messages (Phase 3 - planned)
kcat -C -b localhost:9092 -t test_topic
```

## 📁 Project Structure

```
pg_kafka/
├── src/
│   ├── lib.rs              # Extension entry point, _PG_init hook, worker registration
│   ├── config.rs           # GUC configuration (pg_kafka.port, pg_kafka.host, etc.)
│   ├── worker.rs           # Background worker main loop, request processing
│   ├── kafka/
│   │   ├── mod.rs          # Module organization, re-exports
│   │   ├── listener.rs     # TCP listener, connection handling
│   │   ├── protocol.rs     # Binary protocol parsing/encoding
│   │   └── messages.rs     # Request/response types, message queues
│   └── bin/
│       └── pgrx_embed.rs   # pgrx embedding binary (generated)
├── Cargo.toml              # Rust dependencies: pgrx 0.16.1, tokio, kafka-protocol
├── rust-toolchain.toml     # Requires nightly for edition2024
├── Dockerfile.dev          # Development container with pgrx + kcat
├── docker-compose.yml      # Container orchestration
├── .devcontainer/          # VS Code devcontainer config
├── CLAUDE.md               # Development guide for Claude Code
├── PROJECT.md              # Detailed design document
├── STEP4_SUMMARY.md        # Phase 1 completion summary
└── PHASE_1.5_PLAN.md       # Unit test coverage plan
```

## 🗺️ Implementation Roadmap

- [x] **Phase 0:** Project scaffold, Docker environment, pgrx setup
- [x] **Phase 1:** Metadata support ✅ **COMPLETE**
  - [x] Step 1: Background worker scaffold with _PG_init registration
  - [x] Step 2: TCP listener on port 9092 with tokio runtime
  - [x] Step 3: ApiVersions request/response parsing
  - [x] Step 4: Metadata request/response with hardcoded test topic
- [ ] **Phase 1.5:** Unit test coverage (protocol, worker, integration tests)
- [ ] **Phase 2:** Producer support (ProduceRequest, storage schema, SPI INSERT)
- [ ] **Phase 3:** Consumer support (FetchRequest, Long Polling via LISTEN/NOTIFY)
- [ ] **Phase 4:** Shadow Mode (Logical Decoding, rdkafka integration, zero-downtime migration)

## 🔧 Configuration

The extension uses Postgres GUC (Grand Unified Configuration) parameters:

```sql
-- In postgresql.conf or via ALTER SYSTEM:

-- Network configuration (requires restart)
pg_kafka.port = 9092                    -- TCP port to bind (default: 9092)
pg_kafka.host = '0.0.0.0'               -- Interface to bind (default: 0.0.0.0)

-- Observability (can change at runtime)
pg_kafka.log_connections = false        -- Log each connection (default: false)
pg_kafka.shutdown_timeout_ms = 5000     -- Shutdown timeout (default: 5000ms)
```

**Important:** The extension must be loaded via `shared_preload_libraries`:

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_kafka'
```

## 📚 Documentation

- **[CLAUDE.md](CLAUDE.md)** - Development workflow, commands, architecture overview
- **[PROJECT.md](PROJECT.md)** - Complete design document with technical details
- **[STEP4_SUMMARY.md](STEP4_SUMMARY.md)** - Phase 1 implementation summary
- **[PHASE_1.5_PLAN.md](PHASE_1.5_PLAN.md)** - Unit test coverage plan
- [pgrx Documentation](https://docs.rs/pgrx/latest/pgrx/)
- [Kafka Protocol Specification](https://kafka.apache.org/protocol)

## 🔧 Technical Requirements

- **Rust:** Nightly toolchain (required for edition2024 features)
- **PostgreSQL:** Version 14 (primary target; 12-17 supported)
- **pgrx:** Version 0.16.1
- **Dependencies:** tokio (async runtime), kafka-protocol (binary protocol), crossbeam-channel (message queue)

## 📊 Architecture Highlights

### Async/Sync Bridge

The extension solves a fundamental incompatibility:
- **Tokio wants async:** Network I/O is non-blocking, handles thousands of connections
- **Postgres SPI wants sync:** Database operations must run on main thread

**Solution:** Message queue architecture
```
┌─────────────────────────────────────────────────────────────┐
│ ASYNC WORLD (Tokio LocalSet)                                │
│ - TCP accept() on port 9092                                 │
│ - Parse binary Kafka protocol                               │
│ - Send KafkaRequest → REQUEST_QUEUE                         │
│ - Receive KafkaResponse ← RESPONSE_CHANNEL                  │
└─────────────────────────────────────────────────────────────┘
                          ↓ ↑ (crossbeam + tokio channels)
┌─────────────────────────────────────────────────────────────┐
│ SYNC WORLD (Background Worker Main Thread)                  │
│ - Receive from REQUEST_QUEUE (non-blocking)                 │
│ - Process request (call SPI in Phase 2)                     │
│ - Send response via RESPONSE_CHANNEL                        │
│ - Check for SIGTERM (graceful shutdown)                     │
└─────────────────────────────────────────────────────────────┘
```

### Current Request Flow (Phase 1)

1. Client connects to port 9092
2. Sends ApiVersions or Metadata request
3. Async task parses binary protocol → KafkaRequest
4. Request sent to main thread via queue
5. Main thread processes → builds hardcoded response
6. Response sent back via tokio channel
7. Async task encodes response → writes to socket

### Future Request Flow (Phase 2+)

Step 4 will become:
```rust
// In process_request():
Spi::connect(|client| {
    client.update(
        "INSERT INTO kafka.messages (topic_id, partition_id, key, value)
         VALUES ($1, $2, $3, $4) RETURNING offset",
        None, None
    )
})
```

## 🤝 Contributing

This is currently a portfolio project in active development. Contributions, issues, and feature requests are welcome!

## 📄 License

[Add your license here]

## 🙏 Acknowledgments

- Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) - Postgres extension framework for Rust
- Inspired by the need for smoother Postgres → Kafka migration paths
- Uses [kafka-protocol](https://crates.io/crates/kafka-protocol) for binary protocol handling
