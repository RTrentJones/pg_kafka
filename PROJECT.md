# Design Document: pg_kafka

**Author:** Trent Jones
**Status:** Phase 3B Complete
**Last Updated:** January 2026
**Repository:** [github.com/RTrentJones/pg_kafka](https://github.com/RTrentJones/pg_kafka)

---

## 1. Executive Summary

pg_kafka is a PostgreSQL extension written in Rust (via pgrx) that embeds a Kafka-compatible wire protocol listener directly into the Postgres runtime. It allows standard Kafka clients to produce and consume messages using Postgres as the backing store, eliminating the need for a separate Kafka cluster for small-to-medium workloads.

### Current Status

| Component | Status | Implementation |
|-----------|--------|----------------|
| Protocol Layer | Complete | 12 Kafka APIs (24% coverage) |
| Producer | Complete | ProduceRequest with database persistence |
| Consumer | Complete | FetchRequest with RecordBatch v2 encoding |
| Consumer Groups | Partial | Coordinator with manual partition assignment |
| Offset Management | Complete | OffsetCommit/OffsetFetch with database storage |
| Test Suite | 73 tests | 68 unit + 5 E2E scenarios |
| CI/CD | Complete | GitHub Actions with lint, test, security |

---

## 2. Problem Statement

For early-stage startups and internal tools, introducing Kafka adds significant operational overhead (ZooKeeper/KRaft, JVM tuning, disk management). Teams often start with Postgres for queues but eventually hit scaling limits, requiring a painful "stop-the-world" migration to Kafka.

**Opportunity:** There is no smooth graduation path from "Postgres as a Queue" to "Enterprise Kafka."

**Solution:** pg_kafka provides:
1. Standard Kafka protocol support - existing clients work without code changes
2. PostgreSQL as storage - leverage existing infrastructure and expertise
3. Migration path - Shadow Mode (planned) enables zero-downtime cutover

---

## 3. Goals & Non-Goals

### Goals (Achieved)

- **Wire Compatibility:** Support Kafka Protocol (v2+) so standard clients (Java, Python, kcat) connect without code changes ✅
- **Native Integration:** Run entirely within Postgres as a BackgroundWorker ✅
- **Full Producer Support:** ProduceRequest with database persistence ✅
- **Full Consumer Support:** FetchRequest with RecordBatch v2 encoding ✅
- **Consumer Group Coordinator:** Basic coordinator with manual partition assignment ✅
- **Offset Management:** OffsetCommit/OffsetFetch for consumer progress ✅
- **Testability:** Repository Pattern for mock-based testing ✅

### Goals (Planned)

- **Automatic Partition Assignment:** Range, RoundRobin, Sticky strategies
- **Automatic Rebalancing:** Trigger rebalance on member join/leave
- **Shadow Replication:** Enable seamless migration to external Kafka cluster

### Non-Goals

- **Full Protocol Compliance:** No Transactions, Compression, or Admin APIs
- **High Availability:** Rely on standard Postgres HA (Patroni/RDS) rather than Kafka's ISR
- **Broker Clustering:** Single-node "broker" design

---

## 4. System Architecture

### 4.1 High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka Clients                                │
│            (kcat, rdkafka, kafka-python, etc.)                  │
└─────────────────────────────────────────────────────────────────┘
                              │ TCP :9092
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  ASYNC LAYER (Tokio Runtime in Background Worker)              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ TCP Listener │→│ Protocol     │→│ Request Queue          │ │
│  │ (listener.rs)│  │ (protocol.rs)│  │ (crossbeam-channel)   │ │
│  └─────────────┘  └──────────────┘  └────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  SYNC LAYER (PostgreSQL Background Worker Main Thread)         │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Request Handler │→│ KafkaStore   │→│ PostgreSQL (SPI)  │  │
│  │ (handlers/)     │  │ (storage/)   │  │ kafka.* tables    │  │
│  └─────────────────┘  └──────────────┘  └───────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Component Breakdown

**Protocol Listener (Ingress):**
- Binds to TCP port 9092 (configurable via GUC)
- Tokio runtime running inside PostgreSQL BackgroundWorker
- Parses binary Kafka wire protocol using `kafka-protocol` crate
- Routes requests through crossbeam-channel to main thread

**Request Handlers (handlers/):**
- Modular design - each API has its own module
- Uses `KafkaStore` trait for storage abstraction
- Returns typed errors that map to Kafka protocol error codes

**Storage Layer (storage/):**
- `KafkaStore` trait defines storage contract
- `PostgresStore` implements trait using PostgreSQL SPI
- `MockKafkaStore` enables unit testing without database

**Consumer Group Coordinator:**
- Thread-safe in-memory state using `Arc<RwLock<HashMap>>`
- Tracks group membership, generation IDs, member assignments
- Currently supports manual partition assignment only

---

## 5. Detailed Design

### 5.1 Storage Schema

```sql
-- Messages with dual-offset design
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,           -- Temporal ordering across all partitions
    partition_offset BIGINT NOT NULL,  -- Kafka-compatible per-partition offset
    key BYTEA,
    value BYTEA,
    headers JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic_id, partition_id, partition_offset),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE,
    UNIQUE (global_offset)
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
    PRIMARY KEY (group_id, topic_id, partition_id),
    FOREIGN KEY (topic_id) REFERENCES kafka.topics(id) ON DELETE CASCADE
);
```

**Why Dual Offsets?**
- `global_offset`: Provides total temporal ordering across ALL messages (useful for replication, debugging)
- `partition_offset`: Kafka clients expect per-partition monotonic offsets starting from 0
- Allows queries like "show me all messages in temporal order" AND "fetch from partition X starting at offset Y"

### 5.2 Request Handlers

**ProduceRequest Handler (handlers/produce.rs):**
1. Parse binary frame (Size + Header + Body)
2. Resolve topic (auto-create if needed)
3. Validate partition (currently only partition 0 supported)
4. Insert records via `KafkaStore::insert_records()`
5. Return ProduceResponse with assigned offsets

**FetchRequest Handler (handlers/fetch.rs):**
1. Resolve topic to topic_id
2. Validate partition
3. Fetch records via `KafkaStore::fetch_records()`
4. Encode as RecordBatch v2 format
5. Return FetchResponse with high watermark

**Consumer Group Handlers (handlers/coordinator.rs):**
1. FindCoordinator - Returns self as coordinator
2. JoinGroup - Register member, assign leader
3. Heartbeat - Validate membership
4. LeaveGroup - Remove member from group
5. SyncGroup - Distribute partition assignments

### 5.3 Error Handling

The `KafkaError` enum provides typed errors that map to Kafka protocol error codes:

```rust
pub enum KafkaError {
    UnknownTopic { topic: String },
    UnknownMemberId { group_id: String, member_id: String },
    IllegalGeneration { group_id: String, generation: i32, expected: i32 },
    // ... more variants
}

impl KafkaError {
    pub fn to_kafka_error_code(&self) -> i16 {
        match self {
            KafkaError::UnknownTopic { .. } => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            KafkaError::UnknownMemberId { .. } => ERROR_UNKNOWN_MEMBER_ID,
            KafkaError::IllegalGeneration { .. } => ERROR_ILLEGAL_GENERATION,
            // ... more mappings
        }
    }
}
```

---

## 6. Technical Challenges & Solutions

| Challenge | Risk | Solution |
|-----------|------|----------|
| **Async/Sync Bridge** | PostgreSQL SPI is not thread-safe; tokio wants async | Message queue architecture with crossbeam-channel; tokio handles network I/O, main thread handles SPI |
| **Connection Multiplexing** | Postgres processes are heavy (10MB+) | Single BGWorker with tokio handles thousands of TCP connections, one SPI connection to DB |
| **Offset Gaps** | BIGSERIAL can have gaps on rollback | Kafka clients tolerate gaps; use advisory locks for partition_offset calculation |
| **Testing Without Database** | pgrx tests require full Postgres | Repository Pattern with MockKafkaStore enables unit testing handlers |
| **PGC_POSTMASTER Constraint** | GUCs must be defined at startup | Extension must be in shared_preload_libraries; E2E tests use proper loading |

---

## 7. Implementation Status

### Phase 1: Metadata Support ✅ COMPLETE

- Background worker scaffold with `_PG_init` registration
- TCP listener on port 9092 with tokio runtime
- ApiVersions request/response (API key 18)
- Metadata request/response (API key 3)

### Phase 2: Producer Support ✅ COMPLETE

- ProduceRequest/Response handling (API key 0)
- Storage schema with dual-offset design
- SPI INSERT integration
- Topic auto-creation
- Repository Pattern storage abstraction
- CI/CD pipeline with GitHub Actions

### Phase 3: Consumer Support ✅ COMPLETE

- FetchRequest/Response handling (API key 1)
- ListOffsets support for earliest/latest (API key 2)
- OffsetCommit/OffsetFetch (API keys 8, 9)
- RecordBatch v2 encoding/decoding
- Empty fetch response handling

### Phase 3B: Consumer Group Coordinator ✅ COMPLETE

- FindCoordinator API (API key 10)
- JoinGroup API (API key 11)
- Heartbeat API (API key 12)
- LeaveGroup API (API key 13)
- SyncGroup API (API key 14)
- Thread-safe coordinator state (Arc<RwLock>)
- Manual partition assignment

### Phase 4: Automatic Assignment (Planned)

- Range assignment strategy
- RoundRobin assignment strategy
- Sticky assignment strategy (KIP-54)

### Phase 5: Automatic Rebalancing (Planned)

- Trigger rebalance on member join/leave
- Member timeout detection
- Cooperative rebalancing (KIP-429)

### Phase 6: Shadow Mode (Planned)

- Logical Decoding client for WAL tailing
- rdkafka integration for external Kafka production
- At-Least-Once delivery guarantees

---

## 8. Test Strategy

### Unit Tests (68 tests)

| Module | Tests | Description |
|--------|-------|-------------|
| protocol_tests.rs | 10 | Request/response parsing |
| encoding_tests.rs | 8 | Binary encoding, framing |
| property_tests.rs | 10 | Property-based fuzzing |
| handlers/tests.rs | 14 | Handler logic with MockKafkaStore |
| storage/tests.rs | 22 | Storage types and trait verification |

### E2E Tests (5 scenarios)

1. Producer functionality with database verification
2. Basic consumer with manual partition assignment
3. Consumer multiple messages sequentially
4. Consumer from specific offset
5. OffsetCommit/OffsetFetch round-trip

### CI/CD Pipeline

- **Lint Job:** cargo fmt, cargo clippy
- **Test Job:** Unit tests with coverage, E2E tests
- **Security Job:** cargo-audit for vulnerabilities
- **Lockfile Job:** Verify Cargo.lock is synchronized

---

## 9. Why This Project?

### Technical Learning

- **Protocol Engineering:** Implementing binary wire protocols requires exactness and low-level buffer manipulation
- **Systems Integration:** Embedding an async runtime (tokio) inside a synchronous process model (postgres) is non-trivial
- **Database Internals:** Working with PostgreSQL SPI and background workers provides deep database knowledge

### Practical Value

- **Zero Infrastructure:** Run Kafka clients against PostgreSQL for development/testing
- **Migration Path:** Shadow Mode enables zero-downtime cutover to real Kafka
- **Familiar Operations:** Teams can leverage existing PostgreSQL expertise

### Demonstrated Skills

- Rust systems programming with async/await
- PostgreSQL extension development with pgrx
- Binary protocol implementation
- Repository Pattern for testability
- CI/CD pipeline design
- Technical documentation

---

## 10. References

- [Kafka Protocol Specification](https://kafka.apache.org/protocol)
- [pgrx Documentation](https://docs.rs/pgrx/latest/pgrx/)
- [kafka-protocol Crate](https://crates.io/crates/kafka-protocol)
- [PostgreSQL Background Workers](https://www.postgresql.org/docs/current/bgworker.html)
