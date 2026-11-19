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
| **Protocol Layer** | Production-ready | 28 Kafka APIs implemented (56% coverage) |
| **Producer** | Production-ready | Idempotent + transactional support |
| **Consumer** | Production-ready | Fetch, ListOffsets, automatic partition assignment |
| **Consumer Groups** | Complete | Full coordinator with auto-rebalancing |
| **Transactions** | Complete | Full EOS with read-committed isolation |
| **Shadow Mode** | Complete | External Kafka forwarding with SASL/SSL |
| **Test Suite** | Unit + property + E2E | Counts in [docs/TEST_STRATEGY.md](docs/TEST_STRATEGY.md#test-summary) (CI is the source of truth) |
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
- **Config & Log Management** - DescribeConfigs, IncrementalAlterConfigs (`retention.ms` only), DeleteRecords
- **Retention** - Periodic time-based sweep (`pg_kafka.message_retention_hours`, per-topic `retention.ms`), also runnable via `SELECT * FROM pg_kafka_run_retention_sweep()`; `kafka.messages` is time-partitioned (`pg_kafka.message_partition_minutes`, default one day), so expired data goes by dropping whole partitions instead of row deletes (ADR-001)
- **Compression** - gzip, snappy, lz4, zstd (inbound/outbound)
- **Multi-Partition Topics** - Key-based routing with murmur2 hash
- **Topic Auto-Creation** - Topics created on first produce
- **Dual-Offset Design** - Both `partition_offset` (Kafka-compatible) and `global_offset` (temporal ordering)
- **Transaction Support** - Full ACID with AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
- **Read-Committed Isolation** - Consumers filter pending/aborted transactions
- **Shadow Mode** - Dual-write to external Kafka with SASL/SSL authentication
- **Listener Security** - TLS (rustls, optional mutual TLS) and SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 on Kafka-style `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` listeners; no ACLs ([docs/AUTHENTICATION.md](docs/AUTHENTICATION.md))

### Implemented APIs (28 total)

Versions are the ranges advertised in ApiVersions and enforced on inbound requests (each API's `MIN_VERSION`/`MAX_VERSION` in `src/kafka/api/`).

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| Produce | 0 | 3-9 | Write messages to database |
| Fetch | 1 | 0-11 | Read messages with RecordBatch v2 encoding |
| ListOffsets | 2 | 0-7 | Query earliest/latest/by-timestamp offsets |
| Metadata | 3 | 0-10 | Broker and topic metadata |
| OffsetCommit | 8 | 0-8 | Commit consumer offsets |
| OffsetFetch | 9 | 0-7 | Retrieve committed offsets |
| FindCoordinator | 10 | 0-3 | Locate group coordinator |
| JoinGroup | 11 | 0-7 | Register consumer group member |
| Heartbeat | 12 | 0-4 | Maintain group membership |
| LeaveGroup | 13 | 0-4 | Graceful group departure |
| SyncGroup | 14 | 0-4 | Distribute partition assignments |
| DescribeGroups | 15 | 0-5 | Get consumer group state and members |
| ListGroups | 16 | 0-4 | List all consumer groups |
| SaslHandshake | 17 | 0-1 | Negotiate the SASL mechanism (SASL listeners; v0 answered UNSUPPORTED_VERSION) |
| ApiVersions | 18 | 0-3 | Protocol version negotiation |
| CreateTopics | 19 | 0-5 | Create topics with partition count |
| DeleteTopics | 20 | 0-4 | Delete topics and messages |
| DeleteRecords | 21 | 0-2 | Truncate partitions below an offset |
| InitProducerId | 22 | 0-4 | Allocate producer ID for idempotency |
| AddPartitionsToTxn | 24 | 0-3 | Register partitions in transaction |
| AddOffsetsToTxn | 25 | 0-3 | Include offsets in transaction |
| EndTxn | 26 | 0-3 | Commit or abort transaction |
| TxnOffsetCommit | 28 | 0-3 | Commit offsets within transaction |
| DescribeConfigs | 32 | 1-4 | Report the topic configs pg_kafka honors |
| SaslAuthenticate | 36 | 0-2 | PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 exchange |
| CreatePartitions | 37 | 0-2 | Add partitions to existing topics |
| DeleteGroups | 42 | 0-2 | Delete consumer groups |
| IncrementalAlterConfigs | 44 | 0-1 | Set per-topic `retention.ms` (other keys rejected) |

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
-- Excerpt of sql/bootstrap.sql (indexes, GRANTs and COMMENTs omitted)

-- Topic metadata
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT NOT NULL DEFAULT 1,
    retention_ms BIGINT,              -- Per-topic retention.ms override (NULL = inherit global)
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Messages with dual-offset design
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,        -- Temporal ordering across all partitions
    partition_offset BIGINT NOT NULL, -- Kafka-compatible per-partition offset
    key BYTEA,
    value BYTEA,
    headers JSONB,
    timestamp_ms BIGINT NOT NULL DEFAULT -1,  -- Producer record timestamp (-1 = none)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    producer_id BIGINT,             -- Transactional messages only
    producer_epoch SMALLINT,
    txn_state TEXT,                 -- NULL (committed), 'pending', 'aborted'
    CHECK (partition_id >= 0),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);
-- Leaves: kafka.messages_pYYYYMMDD_HHMMSS (one per pg_kafka.message_partition_minutes,
-- created ahead by the worker) + kafka.messages_default; each leaf has UNIQUE
-- (topic_id, partition_id, partition_offset) and UNIQUE (global_offset).

-- Consumer group offset tracking
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
    CHECK (partition_id >= 0),
    PRIMARY KEY (producer_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);

-- Also: kafka.partition_offsets (per-partition offset counter + log start offset),
-- kafka.transactions and kafka.txn_pending_offsets (Phase 10), and the shadow-mode
-- tables kafka.shadow_config / shadow_tracking / shadow_metrics (Phase 11).
```

---

## Project Structure

```
src/
├── lib.rs                  # Extension entry point, _PG_init hook, SQL functions
├── config.rs               # GUC configuration (pg_kafka.port, etc.)
├── worker.rs               # Background worker main loop, SPI integration
├── testing/
│   ├── helpers.rs          # Test fixtures and assertions
│   └── mocks.rs            # MockKafkaStore for unit testing
├── kafka/
│   ├── mod.rs              # Module organization
│   ├── listener.rs         # TCP listener, tokio runtime
│   ├── protocol/           # Binary protocol decoding/encoding, RecordBatch parsing (kafka-protocol crate)
│   ├── messages.rs         # Request/response types, message queues
│   ├── dispatch.rs         # Request dispatch helpers
│   ├── handler_context.rs  # HandlerContext passed to every handler
│   ├── coordinator.rs      # Consumer group coordinator (Arc<RwLock>)
│   ├── assignment/         # Range, RoundRobin, Sticky assignment strategies
│   ├── partitioner.rs      # Key-based partition routing (murmur2)
│   ├── notifications.rs, pending_fetches.rs  # Long polling support
│   ├── response_builders.rs  # Response construction, supported API version table
│   ├── constants.rs        # Protocol constants, Kafka error codes
│   ├── error.rs            # Typed errors with to_kafka_error_code()
│   ├── handlers/           # Protocol request handlers
│   │   ├── admin.rs        # CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups
│   │   ├── configs.rs      # DescribeConfigs, IncrementalAlterConfigs, DeleteRecords
│   │   ├── consumer.rs     # OffsetCommit/OffsetFetch
│   │   ├── coordinator.rs  # FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups
│   │   ├── fetch.rs        # Fetch, ListOffsets
│   │   ├── helpers.rs      # Topic resolution utilities
│   │   ├── init_producer_id.rs  # InitProducerId
│   │   ├── metadata.rs     # ApiVersions, Metadata
│   │   ├── produce.rs      # ProduceRequest
│   │   ├── transaction.rs  # AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
│   │   └── tests.rs        # Handler unit tests (MockKafkaStore)
│   ├── shadow/             # Shadow mode: ShadowStore, outbox forwarder, rdkafka producer, licensing
│   └── storage/            # Storage abstraction layer
│       ├── mod.rs          # KafkaStore trait definition
│       ├── postgres.rs     # PostgreSQL implementation
│       └── tests.rs        # Storage tests
└── bin/
    └── pgrx_embed.rs       # pgrx embedding binary

tests/property_tests.rs     # proptest suite
kafka_test/                 # E2E test suite using rdkafka client
sql/bootstrap.sql           # Schema definition
docs/                       # Architecture decisions, protocol coverage
```

---

## Testing

### Test Coverage

Unit tests (`src/`, with `MockKafkaStore` standing in for Postgres), property tests
(`tests/property_tests.rs`, proptest) and the E2E suite (`kafka_test/`, a real rdkafka
client against a running extension). Current counts, per module and per E2E
category, are kept in one place: [docs/TEST_STRATEGY.md](docs/TEST_STRATEGY.md#test-summary).

### Running Tests

```bash
# Unit tests (fast, no PostgreSQL required)
cargo test --features pg14

# Property tests only
cargo test --features pg14 --test property_tests

# E2E tests (requires running PostgreSQL)
cargo pgrx start pg14
cd kafka_test && cargo run --release
```

### CI Pipeline

The GitHub Actions pipeline runs:
1. **Lint** - `cargo fmt` and `cargo clippy` (root crate and `kafka_test/`)
2. **Unit Tests** - With code coverage via `cargo-llvm-cov`, plus the property tests
3. **E2E Tests** - Functional, shadow mode (against an external Kafka container), and performance suites with the rdkafka client
4. **Security Audit** - `cargo-audit` and `cargo-deny`
5. **Lockfile Verification** - Ensures `Cargo.lock` is synchronized (root and `kafka_test/`)
6. **PostgreSQL Matrix** - `cargo check` and unit tests against PostgreSQL 15, 16 and 17
7. **SPI E2E Guard** - On PRs, a change to an SPI/async file must come with a `kafka_test/src/` test change

---

## Configuration

> ⚠️ **Security: authentication is opt-in, and there is no authorization.** By
> default the only listener is PLAINTEXT on `127.0.0.1:9092`: anyone who can reach
> it can produce, consume, and administer topics. TLS and SASL (PLAIN,
> SCRAM-SHA-256, SCRAM-SHA-512) listeners can be added with
> `pg_kafka.security_protocol` / `pg_kafka.additional_listeners` (see
> [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md)); an authenticated client may
> still do everything, because there are no ACLs. The extension logs a startup
> WARNING for every non-loopback listener that does not authenticate clients
> (`docs/PROTOCOL_DEVIATIONS.md` "Network Security Posture").

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_kafka'

-- Read at worker start (requires restart)
pg_kafka.port = 9092              -- TCP port (default: 9092)
pg_kafka.host = '127.0.0.1'       -- Bind address (default: 127.0.0.1; '0.0.0.0' = all interfaces, see warning above)
pg_kafka.database = 'postgres'    -- Database where CREATE EXTENSION pg_kafka was run
pg_kafka.log_connections = false  -- Log each connection
pg_kafka.shutdown_timeout_ms = 5000

-- Reloadable with pg_reload_conf()
pg_kafka.message_retention_hours = 0  -- Time-based retention sweep (0 = keep forever)
pg_kafka.message_partition_minutes = 1440  -- Width of one time partition of kafka.messages
-- wave3 groups: resource bounds
pg_kafka.max_partitions_per_topic = 10000  -- CreateTopics/CreatePartitions cap (1-100000; INVALID_PARTITIONS above)
pg_kafka.group_max_size = 1000             -- members per consumer group (GROUP_MAX_SIZE_REACHED above)
pg_kafka.max_consumer_groups = 100000      -- groups held in memory (oldest Empty group evicted when full)

-- Listener security (read when the worker starts; see docs/AUTHENTICATION.md)
pg_kafka.security_protocol = 'PLAINTEXT'      -- primary listener: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
pg_kafka.additional_listeners = ''            -- e.g. 'SASL_SSL://0.0.0.0:9093'
pg_kafka.ssl_cert_file = ''                   -- PEM chain / key for SSL listeners
pg_kafka.ssl_key_file = ''
pg_kafka.ssl_ca_file = ''                     -- set = mutual TLS
pg_kafka.sasl_enabled_mechanisms = 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-512'
```

Kafka users: `SELECT pg_kafka_create_user('alice', 'secret');` (also `pg_kafka_alter_user`,
`pg_kafka_set_user_verifier`, `pg_kafka_drop_user`, view `kafka.sasl_users`).

See [CLAUDE.md](CLAUDE.md#configuration) for the full GUC list, including long polling, compression and Shadow Mode settings.

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
- [x] Time partitioning of kafka.messages with retention by partition drop (0.1.0, ADR-001)

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
