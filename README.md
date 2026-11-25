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
| **Protocol Layer** | Production-ready | <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen--> |
| **Producer** | Production-ready | Idempotent + transactional support |
| **Consumer** | Production-ready | Fetch, ListOffsets, automatic partition assignment |
| **Consumer Groups** | Complete | Full coordinator with auto-rebalancing |
| **Transactions** | Complete | Full EOS with read-committed isolation |
| **Shadow Mode** | Complete | External Kafka forwarding with SASL/SSL |
| **Test Suite** | Unit + property + E2E | Counts in [docs/TEST_STRATEGY.md](docs/TEST_STRATEGY.md#test-summary) (CI is the source of truth) |
| **CI/CD** | Complete | GitHub Actions: lint, tests, E2E on PostgreSQL 14-18, interop, fuzzing, security audit |

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
- **Retention** - Periodic time-based sweep (`pg_kafka.message_retention_hours`, per-topic `retention.ms`), also runnable via `SELECT * FROM pg_kafka_run_retention_sweep()` (superusers only, like `pg_kafka_run_partition_maintenance()`: EXECUTE is revoked from PUBLIC); `kafka.messages` is time-partitioned (`pg_kafka.message_partition_minutes`, default one day), so expired data goes by dropping whole partitions instead of row deletes (ADR-001)
- **Compression** - gzip, snappy, lz4, zstd (inbound/outbound)
- **Multi-Partition Topics** - Key-based routing with murmur2 hash
- **Topic Auto-Creation** - Topics created on first produce
- **Dual-Offset Design** - Both `partition_offset` (Kafka-compatible) and `global_offset` (temporal ordering)
- **Transaction Support** - Full ACID with AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
- **Read-Committed Isolation** - Consumers filter pending/aborted transactions
- **Shadow Mode** - Dual-write to external Kafka with SASL/SSL authentication
- **Listener Security** - TLS (rustls, optional mutual TLS) and SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 on Kafka-style `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` listeners; no ACLs ([docs/AUTHENTICATION.md](docs/AUTHENTICATION.md))

### Implemented APIs

<!--gen:api-count-->28<!--/gen--> APIs. Versions are the ranges advertised in ApiVersions and enforced on inbound requests (each API's `MIN_VERSION`/`MAX_VERSION` in `src/kafka/api/`). The table is generated from the `kafka_apis!` registry (`src/kafka/api/docs.rs`); a unit test fails when it is stale.

<!--gen:api-table-->
| API | Key | Versions | Served by | Description |
|-----|-----|----------|-----------|-------------|
| Produce | 0 | 3-9 | DB thread | Write messages to the database |
| Fetch | 1 | 4-11 | DB thread | Read messages (RecordBatch v2), with long polling |
| ListOffsets | 2 | 1-7 | DB thread | Earliest/latest/by-timestamp offsets |
| Metadata | 3 | 0-10 | DB thread | Broker and topic metadata |
| OffsetCommit | 8 | 2-8 | DB thread | Commit consumer offsets |
| OffsetFetch | 9 | 1-7 | DB thread | Retrieve committed offsets |
| FindCoordinator | 10 | 0-3 | DB thread | Locate the group / transaction coordinator |
| JoinGroup | 11 | 0-7 | DB thread | Join a consumer group |
| Heartbeat | 12 | 0-4 | DB thread (liveness lane) | Keep group membership alive |
| LeaveGroup | 13 | 0-4 | DB thread | Leave a consumer group |
| SyncGroup | 14 | 0-4 | DB thread | Distribute partition assignments |
| DescribeGroups | 15 | 0-5 | DB thread | Consumer group state and members |
| ListGroups | 16 | 0-4 | DB thread | List consumer groups |
| SaslHandshake | 17 | 0-1 | network thread | Negotiate the SASL mechanism (SASL listeners; v0 answered UNSUPPORTED_VERSION) |
| ApiVersions | 18 | 0-3 | DB thread | Protocol version negotiation |
| CreateTopics | 19 | 2-5 | DB thread | Create topics |
| DeleteTopics | 20 | 1-4 | DB thread | Delete topics and their messages |
| DeleteRecords | 21 | 0-2 | DB thread | Truncate partitions below an offset |
| InitProducerId | 22 | 0-4 | DB thread | Allocate a producer id (idempotence, transactions) |
| AddPartitionsToTxn | 24 | 0-3 | DB thread | Register partitions in a transaction |
| AddOffsetsToTxn | 25 | 0-3 | DB thread | Add a group's offsets to a transaction |
| EndTxn | 26 | 0-3 | DB thread | Commit or abort a transaction |
| TxnOffsetCommit | 28 | 0-3 | DB thread | Commit offsets inside a transaction |
| DescribeConfigs | 32 | 1-4 | DB thread | Report the topic configs pg_kafka honors |
| SaslAuthenticate | 36 | 0-2 | network thread | PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 exchange |
| CreatePartitions | 37 | 0-2 | DB thread | Add partitions to existing topics |
| DeleteGroups | 42 | 0-2 | DB thread | Delete consumer groups |
| IncrementalAlterConfigs | 44 | 0-1 | DB thread | Set per-topic `retention.ms` (other keys rejected) |
<!--/gen-->

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

1. **Async/Sync Bridge** - Tokio handles network I/O; all SPI runs on the background worker's one database thread
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
-- kafka.producer_batches (the last 5 batches per producer and partition, for duplicates),
-- kafka.transactions / txn_partitions / txn_groups / txn_pending_offsets (Phase 10), the shadow-mode
-- tables kafka.shadow_config / shadow_tracking / shadow_metrics (Phase 11), the SASL
-- credentials (kafka.sasl_credentials, kafka.sasl_server_secret), the broker metrics
-- tables, and the views kafka.metrics, api_metrics, broker_metrics, consumer_lag,
-- shadow_status, shadow_partition_status and sasl_users.
```

---

## Project Structure

| Path | Contents |
|------|----------|
| `src/` | The extension: worker, listener, protocol, handlers, coordinator, storage, shadow mode ([module map](CLAUDE.md#codebase-structure)) |
| `sql/` | Schema (`bootstrap.sql`), the 0.0.0 to 0.1.0 upgrade script, optional tuning |
| `tests/property_tests.rs` | proptest round-trips of the kafka-protocol crate's encodings (no pg_kafka code) |
| `kafka_test/` | E2E suite: a real rdkafka client against a running extension |
| `fuzz/` | libFuzzer targets for the network-edge decoders |
| `interop/` | Apache Kafka Java client checks and the differential harness against a real broker |
| `evidence/` | Conformance matrix, benchmark and recorded session published by `evidence.yml` |
| `scripts/` | Development and CI helpers (`restart.sh`, `ci-pg.sh`, `check-upgrade.sh`, ...) |
| `docs/` | Protocol coverage and deviations, configuration, testing, ADRs ([index](docs/README.md)) |

---

## Testing

### Test Coverage

Unit tests (`src/`, with `MockKafkaStore` standing in for Postgres), property tests
(`tests/property_tests.rs`, proptest round-trips of the kafka-protocol crate's encodings) and the E2E suite (`kafka_test/`, a real rdkafka
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

GitHub Actions runs lint, unit and property tests with coverage, the E2E suite on PostgreSQL 14
to 18 (plus the shadow and performance categories on 14), the Java-client and differential
interop checks, fuzzing, a security audit, the lockfile check, and on pull requests the SPI E2E
guard. [.github/SETUP.md](.github/SETUP.md) lists the jobs.

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
-- Resource bounds
pg_kafka.max_partitions_per_topic = 10000  -- CreateTopics/CreatePartitions cap (1-100000; INVALID_PARTITIONS above)
pg_kafka.group_max_size = 1000             -- members per consumer group (GROUP_MAX_SIZE_REACHED above)
pg_kafka.max_consumer_groups = 100000      -- groups held in memory (oldest Empty group evicted when full)

-- Listener security (read when the worker starts; see docs/AUTHENTICATION.md)
pg_kafka.security_protocol = 'PLAINTEXT'      -- primary listener: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
pg_kafka.additional_listeners = ''            -- e.g. 'SASL_SSL://0.0.0.0:9093@broker.example.com:9093'
pg_kafka.advertised_address = ''              -- host[:port] clients are told for the primary listener
pg_kafka.ssl_cert_file = ''                   -- PEM chain / key for SSL listeners
pg_kafka.ssl_key_file = ''
pg_kafka.ssl_ca_file = ''                     -- set = mutual TLS
pg_kafka.sasl_enabled_mechanisms = 'PLAIN,SCRAM-SHA-256,SCRAM-SHA-512'
```

Kafka users: `SELECT pg_kafka_create_user('alice', 'secret');` (also `pg_kafka_alter_user`,
`pg_kafka_set_user_verifier`, `pg_kafka_drop_user`, view `kafka.sasl_users`).

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for every setting (generated from the GUC definitions: type, default, range, when a change takes effect).

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
- [x] Time partitioning of kafka.messages with retention by partition drop (0.1.0, ADR-001)

### Planned

- [ ] **Cooperative Rebalancing** (KIP-429)
- [ ] **Static Group Membership** (KIP-345)

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
| Testing | mockall, proptest, rdkafka (E2E), libFuzzer |

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
