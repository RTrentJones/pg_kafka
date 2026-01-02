# pg_kafka 🐘➡️🕸️

[![CI/CD Pipeline](https://github.com/RTrentJones/pg_kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/RTrentJones/pg_kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/RTrentJones/pg_kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/RTrentJones/pg_kafka)

**A high-performance PostgreSQL extension that speaks the Kafka Wire Protocol.**

`pg_kafka` allows standard Kafka clients (Java, Python, Go, etc.) to produce and consume messages directly from a PostgreSQL database. It runs as a native background worker in Postgres itself, eliminating the network hop of external proxies and the operational overhead of managing a separate Kafka cluster for small-to-medium workloads.

---

⚠️ **Status: Phase 2 Complete - Producer Support Functional**

**Current Implementation:**
- ✅ **Phase 1 Complete:** TCP listener with Kafka protocol support
  - ✅ Background worker with tokio async runtime
  - ✅ ApiVersions request/response (API key 18)
  - ✅ Metadata request/response (API key 3)
  - ✅ Configurable port/host via GUC parameters
  - ✅ Full async/sync architecture (tokio ↔ Postgres SPI bridge)

- ✅ **Phase 2 Complete:** Producer support with database persistence
  - ✅ ProduceRequest/Response handling (API key 0)
  - ✅ Dual-offset design (partition_offset + global_offset)
  - ✅ SPI integration for database writes
  - ✅ Topic auto-creation
  - ✅ E2E tests with real Kafka client (rdkafka)
  - ✅ Automated database verification
  - ✅ CI/CD pipeline with GitHub Actions

**What Works Now:**
```bash
# Query broker metadata
kcat -L -b localhost:9092

# Produce messages to database
echo "key1:value1" | kcat -P -b localhost:9092 -t my-topic -K:

# Verify in database
psql -c "SELECT * FROM kafka.messages;"
```

**Next Steps:**
- ⏳ Phase 3: Consumer support (FetchRequest, long polling via LISTEN/NOTIFY)
- ⏳ Phase 4: Shadow replication (Logical Decoding → external Kafka)

---

## 🚀 Why pg_kafka?

### Project Purpose

This is a **learning project** and **technical exploration** to understand:
- How Kafka's wire protocol actually works
- How to build PostgreSQL extensions with Rust (pgrx)
- The architectural tradeoffs between message queues and databases
- Async/sync bridging patterns in systems programming

While PostgreSQL *can* be used for queuing/pub-sub, it's rarely the first choice due to:
- No standard Kafka protocol support
- Custom implementations don't scale well
- Eventual migration to real Kafka requires rewriting all client code

**`pg_kafka` explores whether we can solve these friction points** by embedding a Kafka-compatible protocol listener directly into Postgres.

### Potential Use Cases

If this project reaches production quality, it could serve:

1. **Local Development:** Run Kafka clients against Postgres (zero infrastructure)
2. **Testing/CI:** Lightweight Kafka substitute for test suites
3. **Prototyping:** Validate Kafka-based designs before infrastructure commitment
4. **Migration Tool:** Shadow Mode enables zero-downtime cutover to real Kafka
5. **Small-Scale Production:** Teams already on Postgres who need <50K msg/sec

### When NOT to Use This

❌ **Use Real Kafka for:**
- Long-term event sourcing (months/years of retention needed)
- Massive scale (>100K msg/sec sustained throughput)
- Multi-datacenter replication
- Production systems requiring Kafka's full feature set (exactly-once semantics, compaction, transactions)
- Compliance requirements for immutable audit logs

See [docs/architecture/ADR-001-partitioning-and-retention.md](docs/architecture/ADR-001-partitioning-and-retention.md) for detailed tradeoff analysis on retention and partitioning.

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
  - [x] Background worker scaffold with _PG_init registration
  - [x] TCP listener on port 9092 with tokio runtime
  - [x] ApiVersions request/response parsing
  - [x] Metadata request/response
- [x] **Phase 2:** Producer support ✅ **COMPLETE**
  - [x] ProduceRequest/Response handling
  - [x] Storage schema with dual-offset design
  - [x] SPI INSERT integration
  - [x] Topic auto-creation
  - [x] E2E tests with rdkafka client
  - [x] Automated database verification
  - [x] CI/CD pipeline with GitHub Actions
- [ ] **Phase 3:** Consumer support (FetchRequest, long polling via LISTEN/NOTIFY)
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

## ⚡ Performance Tuning

For high-throughput workloads (>10,000 messages/sec), consider applying aggressive autovacuum settings:

```bash
# Optional: Apply performance tuning for kafka.messages table
psql -d your_database -f sql/tune_autovacuum.sql
```

This configures the `kafka.messages` table to vacuum more frequently, preventing index bloat in high-churn scenarios.

**Benchmark Validation:** PostgreSQL can handle 1M+ reads/sec and 200K+ writes/sec on a single node when properly tuned. See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for detailed benchmarks and configuration recommendations.

**Key Recommendations:**
- Use `autovacuum_vacuum_scale_factor = 0.05` for high-throughput tables
- Enable `huge_pages = on` for systems with 64GB+ RAM
- Consider partitioning `kafka.messages` for retention policies (Phase 3+)

See the [Performance Guide](docs/PERFORMANCE.md) for complete tuning recommendations.

## 📚 Documentation

### Getting Started
- **[CLAUDE.md](CLAUDE.md)** - Development workflow, commands, architecture overview
- **[PROJECT.md](PROJECT.md)** - Complete design document with technical details
- **[.github/SETUP.md](.github/SETUP.md)** - CI/CD pipeline setup guide

### Performance & Operations
- **[docs/PERFORMANCE.md](docs/PERFORMANCE.md)** - Performance tuning, benchmarks, and configuration recommendations
- **[docs/PROTOCOL_DEVIATIONS.md](docs/PROTOCOL_DEVIATIONS.md)** - Intentional Kafka protocol deviations and compatibility notes
- **[docs/architecture/ADR-001-partitioning-and-retention.md](docs/architecture/ADR-001-partitioning-and-retention.md)** - Partitioning strategy and data retention tradeoffs

### Development History
- **[STEP4_SUMMARY.md](STEP4_SUMMARY.md)** - Phase 1 implementation summary
- **[PHASE_1.5_PLAN.md](PHASE_1.5_PLAN.md)** - Unit test coverage plan

### External References
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

### Current Request Flow (Phase 2)

1. Client connects to port 9092
2. Sends Kafka request (ApiVersions, Metadata, or Produce)
3. Async task parses binary protocol → KafkaRequest
4. Request sent to main thread via queue
5. Main thread processes request:
   - **Metadata:** Query `kafka.topics` table
   - **Produce:** Insert into `kafka.messages` via SPI
6. Response sent back via tokio channel
7. Async task encodes response → writes to socket

**ProduceRequest Flow (Implemented):**
```rust
// In process_request():
Spi::connect(|client| {
    // 1. Get or create topic
    let topic_id = get_or_create_topic(&client, &topic_name)?;

    // 2. Compute next partition offset (atomic)
    let partition_offset = get_next_partition_offset(&client, topic_id, partition_id)?;

    // 3. Insert message
    client.update(
        "INSERT INTO kafka.messages
         (topic_id, partition_id, partition_offset, key, value, headers)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING global_offset",
        None, None
    )?;

    // 4. Return offset to client in ProduceResponse
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
