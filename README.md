# pg_kafka

[![CI/CD Pipeline](https://github.com/RTrentJones/pg_kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/RTrentJones/pg_kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/RTrentJones/pg_kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/RTrentJones/pg_kafka)

**A PostgreSQL extension that implements the Kafka wire protocol, allowing standard Kafka clients to produce and consume messages using PostgreSQL as the storage backend.**

```bash
# Works with any Kafka client - no code changes required
echo "key1:value1" | kcat -P -b localhost:9092 -t my-topic -K:
kcat -C -b localhost:9092 -t my-topic -p 0 -o beginning
```

## Project Status

| Component | Status | Details |
|-----------|--------|---------|
| **Protocol Layer** | Production-ready | 23 Kafka APIs implemented (46% coverage) |
| **Producer** | Production-ready | Idempotent + transactional support |
| **Consumer** | Production-ready | Fetch, ListOffsets, automatic partition assignment |
| **Consumer Groups** | Complete | Full coordinator with auto-rebalancing |
| **Transactions** | Complete | Full EOS with read-committed isolation |
| **Shadow Mode** | Complete | External Kafka forwarding with SASL/SSL |
| **Test Suite** | 782 tests | 609 unit tests + 173 E2E scenarios |
| **CI/CD** | Complete | GitHub Actions with lint, test, security audit |

**Current Phase:** Phase 11 Complete - Shadow Mode

---

## Key Features

### What Works Today

- **Full Kafka Wire Protocol** - Binary protocol parsing using `kafka-protocol` crate
- **Idempotent Producer** - InitProducerId API with sequence validation and deduplication
- **Producer Support** - ProduceRequest with database persistence and offset tracking
- **Consumer Support** - FetchRequest with RecordBatch v2 encoding
- **Consumer Groups** - FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup with automatic rebalancing
- **Partition Assignment** - Range, RoundRobin, and Sticky strategies
- **Offset Management** - OffsetCommit/OffsetFetch for consumer progress tracking
- **Admin APIs** - CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups
- **Compression** - gzip, snappy, lz4, zstd (inbound/outbound)
- **Multi-Partition Topics** - Key-based routing with murmur2 hash
- **Topic Auto-Creation** - Topics created on first produce
- **Dual-Offset Design** - Both `partition_offset` (Kafka-compatible) and `global_offset` (temporal ordering)
- **Transaction Support** - Full ACID with AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
- **Read-Committed Isolation** - Consumers filter pending/aborted transactions
- **Shadow Mode** - Dual-write to external Kafka with SASL/SSL authentication

### Implemented APIs (23 total)

| API | Key | Description |
|-----|-----|-------------|
| Produce | 0 | Write messages to database |
| Fetch | 1 | Read messages with RecordBatch v2 encoding |
| ListOffsets | 2 | Query earliest/latest offsets |
| Metadata | 3 | Broker and topic metadata |
| OffsetCommit | 8 | Commit consumer offsets |
| OffsetFetch | 9 | Retrieve committed offsets |
| FindCoordinator | 10 | Locate group coordinator |
| JoinGroup | 11 | Register consumer group member |
| Heartbeat | 12 | Maintain group membership |
| LeaveGroup | 13 | Graceful group departure |
| SyncGroup | 14 | Distribute partition assignments |
| DescribeGroups | 15 | Get consumer group state and members |
| ListGroups | 16 | List all consumer groups |
| ApiVersions | 18 | Protocol version negotiation |
| CreateTopics | 19 | Create topics with partition count |
| DeleteTopics | 20 | Delete topics and messages |
| InitProducerId | 22 | Allocate producer ID for idempotency |
| AddPartitionsToTxn | 24 | Register partitions in transaction |
| AddOffsetsToTxn | 25 | Include offsets in transaction |
| EndTxn | 26 | Commit or abort transaction |
| TxnOffsetCommit | 28 | Commit offsets within transaction |
| CreatePartitions | 37 | Add partitions to existing topics |
| DeleteGroups | 42 | Delete consumer groups |

---

## Quick Start

### Using VS Code Dev Container (Recommended)

```bash
# 1. Open in VS Code and reopen in container (F1 → "Dev Containers: Reopen in Container")
# 2. Start PostgreSQL with the extension
cargo pgrx run pg14

# 3. In another terminal, test with kcat
kcat -L -b localhost:9092                              # List metadata
echo "key1:value1" | kcat -P -b localhost:9092 -t test -K:  # Produce
kcat -C -b localhost:9092 -t test -p 0 -o beginning    # Consume
```

### Using Docker

```bash
docker-compose up -d
docker exec -it pg_kafka_dev bash
cargo pgrx run pg14
```

### Verify in Database

```sql
-- Messages are stored in PostgreSQL
SELECT topic_id, partition_id, partition_offset, key, value
FROM kafka.messages ORDER BY partition_offset;

-- Consumer offsets are tracked
SELECT group_id, topic_id, partition_id, committed_offset
FROM kafka.consumer_offsets;
```

---

## Architecture

### Design Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka Clients                               │
│            (kcat, rdkafka, kafka-python, etc.)                  │
└─────────────────────────────────────────────────────────────────┘
                              │ TCP :9092
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  ASYNC LAYER (Tokio Runtime)                                    │
│  ┌──────────────┐ ┌──────────────┐  ┌────────────────────────┐  │
│  │ TCP Listener │→│ Protocol     │→ │ Request Queue          │  │
│  │ (accept)     │ │ Parser       │  │ (crossbeam-channel)    │  │
│  └──────────────┘ └──────────────┘  └────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│  SYNC LAYER (PostgreSQL Background Worker)                   │
│  ┌─────────────────┐ ┌──────────────┐ ┌───────────────────┐  │
│  │ Request Handler │→│ KafkaStore   │→│ PostgreSQL (SPI)  │  │
│  │ (handlers/)     │ │ (Repository) │ │ kafka.messages    │  │
│  └─────────────────┘ └──────────────┘ └───────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Async/Sync Bridge** - Tokio handles network I/O; PostgreSQL SPI runs on main thread
2. **Repository Pattern** - `KafkaStore` trait abstracts storage for testability
3. **Typed Error Handling** - `KafkaError` variants map directly to Kafka protocol error codes
4. **Modular Handlers** - Each API handler is a separate module with its own tests

### Storage Schema

```sql
-- Messages with dual-offset design
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,        -- Temporal ordering across all partitions
    partition_offset BIGINT NOT NULL, -- Kafka-compatible per-partition offset
    key BYTEA,
    value BYTEA,
    headers JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic_id, partition_id, partition_offset)
);

-- Topic metadata
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT DEFAULT 1
);

-- Consumer group offset tracking
CREATE TABLE kafka.consumer_offsets (
    group_id TEXT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    commit_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, partition_id)
);

-- Idempotent producer support (Phase 9)
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
    PRIMARY KEY (producer_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);
```

---

## Project Structure

```
src/
├── lib.rs                  # Extension entry point, _PG_init hook
├── config.rs               # GUC configuration (pg_kafka.port, etc.)
├── worker.rs               # Background worker main loop, SPI integration
├── testing/
│   └── mocks.rs            # MockKafkaStore for unit testing
├── kafka/
│   ├── mod.rs              # Module organization
│   ├── listener.rs         # TCP listener, tokio runtime
│   ├── protocol.rs         # Binary protocol parsing (kafka-protocol crate)
│   ├── messages.rs         # Request/response types, message queues
│   ├── coordinator.rs      # Consumer group coordinator (Arc<RwLock>)
│   ├── constants.rs        # Protocol constants, Kafka error codes
│   ├── error.rs            # Typed errors with to_kafka_error_code()
│   ├── handlers/           # Protocol request handlers
│   │   ├── consumer.rs     # OffsetCommit/OffsetFetch
│   │   ├── coordinator.rs  # JoinGroup, Heartbeat, LeaveGroup, SyncGroup
│   │   ├── fetch.rs        # Fetch, ListOffsets
│   │   ├── helpers.rs      # Topic resolution utilities
│   │   ├── metadata.rs     # ApiVersions, Metadata
│   │   ├── produce.rs      # ProduceRequest
│   │   └── tests.rs        # Handler unit tests (56 tests)
│   └── storage/            # Storage abstraction layer
│       ├── mod.rs          # KafkaStore trait definition
│       ├── postgres.rs     # PostgreSQL implementation
│       └── tests.rs        # Storage tests (43 tests)
└── bin/
    └── pgrx_embed.rs       # pgrx embedding binary

kafka_test/                 # E2E test suite using rdkafka client (173 tests)
docs/                       # Architecture decisions, protocol coverage
```

---

## Testing

### Test Coverage

| Category | Tests | Description |
|----------|-------|-------------|
| Shadow Mode | 146 | External Kafka forwarding, SASL/SSL, replay |
| Protocol | 85 | Request/response parsing and encoding |
| Handlers | 76 | Handler logic with MockKafkaStore |
| Assignment | 67 | Partition assignment strategies |
| Storage | 43 | Storage types and trait verification |
| Messages | 39 | Message encoding/decoding |
| Response Builders | 30 | Response construction |
| Error Handling | 22 | KafkaError variants |
| Config | 32 | GUC configuration |
| Testing Utils | 23 | Test infrastructure |
| Other | 46 | Coordinator, partitioner, constants |
| E2E | 173 | Full integration with rdkafka client |
| **Total** | **782** | 609 unit + 173 E2E |

### Running Tests

```bash
# Unit tests (fast, no PostgreSQL required)
cargo test --features pg14

# E2E tests (requires running PostgreSQL)
cargo pgrx start pg14
cd kafka_test && cargo run --release
```

### CI Pipeline

The GitHub Actions pipeline runs:
1. **Lint** - `cargo fmt` and `cargo clippy`
2. **Unit Tests** - With code coverage via `cargo-llvm-cov`
3. **E2E Tests** - Full integration with rdkafka client
4. **Security Audit** - `cargo-audit` for vulnerability scanning
5. **Lockfile Verification** - Ensures `Cargo.lock` is synchronized

---

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

---

## Roadmap

### Completed

- [x] **Phase 1:** TCP listener, ApiVersions, Metadata
- [x] **Phase 2:** Producer with database persistence, CI/CD
- [x] **Phase 3:** Consumer with FetchRequest, ListOffsets, RecordBatch v2
- [x] **Phase 3B:** Consumer group coordinator (manual assignment)
- [x] **Phase 4:** Automatic partition assignment (Range, RoundRobin, Sticky)
- [x] **Phase 5:** Automatic rebalancing, member timeout detection
- [x] **Phase 6:** Admin APIs (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups)
- [x] **Phase 7:** Multi-Partition Topics (key-based routing)
- [x] **Phase 8:** Compression Support (gzip, snappy, lz4, zstd)
- [x] **Phase 9:** Idempotent Producer (InitProducerId, sequence validation)
- [x] **Phase 10:** Transaction Support (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
- [x] **Phase 11:** Shadow Mode (external Kafka forwarding, SASL/SSL, per-topic config)

### Planned

- [ ] **Cooperative Rebalancing** (KIP-429)
- [ ] **Static Group Membership** (KIP-345)
- [ ] Table partitioning and retention policies

---

## When to Use pg_kafka

### Good Fit

- **Local Development** - Run Kafka clients against PostgreSQL with zero infrastructure
- **Testing/CI** - Lightweight Kafka substitute for test suites
- **Prototyping** - Validate Kafka-based designs before infrastructure commitment
- **Small-Scale Production** - Teams already on PostgreSQL who need <50K msg/sec

### Use Real Kafka Instead

- Long-term event sourcing (months/years of retention)
- Massive scale (>100K msg/sec sustained)
- Multi-datacenter replication
- Log compaction (KIP-58)

See [PROTOCOL_DEVIATIONS.md](docs/PROTOCOL_DEVIATIONS.md) for detailed compatibility notes.

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| Language | Rust (nightly) |
| PostgreSQL Integration | pgrx 0.16.1 |
| Async Runtime | tokio 1.48 |
| Protocol Parsing | kafka-protocol 0.17 |
| Message Queue | crossbeam-channel 0.5 |
| Error Handling | thiserror 2.0 |
| Testing | mockall, proptest, rdkafka |

---

## Documentation

- [CLAUDE.md](CLAUDE.md) - Development workflow and architecture
- [docs/KAFKA_PROTOCOL_COVERAGE.md](docs/KAFKA_PROTOCOL_COVERAGE.md) - API coverage analysis
- [docs/PROTOCOL_DEVIATIONS.md](docs/PROTOCOL_DEVIATIONS.md) - Intentional protocol deviations
- [docs/TEST_STRATEGY.md](docs/TEST_STRATEGY.md) - Test strategy and coverage
- [docs/REPOSITORY_PATTERN.md](docs/REPOSITORY_PATTERN.md) - Storage abstraction design
- [docs/SHADOW_MODE_TESTING.md](docs/SHADOW_MODE_TESTING.md) - Shadow mode setup and testing
- [docs/LICENSING.md](docs/LICENSING.md) - Dual-licensing information
- [docs/architecture/ADR-001-partitioning-and-retention.md](docs/architecture/ADR-001-partitioning-and-retention.md) - Partitioning decisions

---

## Acknowledgments

- Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) - PostgreSQL extension framework for Rust
- Protocol parsing via [kafka-protocol](https://crates.io/crates/kafka-protocol)
- E2E testing with [rdkafka](https://github.com/fede1024/rust-rdkafka)
