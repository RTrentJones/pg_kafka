# Kafka Protocol Deviations

This document lists intentional deviations from the official Kafka protocol specification.

## Summary

`pg_kafka` implements a **subset** of the Kafka wire protocol (46% API coverage). The following deviations are intentional design decisions that optimize for PostgreSQL's strengths while maintaining compatibility with standard Kafka clients.

| Category | Status |
|----------|--------|
| **Producer APIs** | ✅ Full support (idempotent + transactional) |
| **Consumer APIs** | ✅ Full support (with automatic rebalancing) |
| **Coordinator APIs** | ✅ Full support (Range, RoundRobin, Sticky strategies) |
| **Admin APIs** | ✅ Full support (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups) |
| **Transaction APIs** | ✅ Full support (EOS with read-committed isolation) |
| **Shadow Mode** | ✅ Full support (external Kafka forwarding) |

**Current Implementation:** Phase 11 Complete (Shadow Mode)

## Producer API Deviations

### 1. acks=0 (Fire-and-Forget) ✅ Supported

**Status:** Implemented
**Kafka Behavior:** Client sends message and does not wait for acknowledgment; the broker sends NO response frame at all
**pg_kafka Behavior:** Matches Kafka — no response frame is sent; best-effort database write

**Implementation:**
- No response is sent for acks=0 (sending one would desync clients that
  enforce strict per-connection request/response pairing, e.g. the Java client)
- Data is still written to PostgreSQL (best-effort)
- Errors are logged but not returned to client (per acks=0 contract)
- Long-polling consumers are notified if write succeeds
- Idempotency/transactional validation is intentionally skipped for acks=0

**Trade-offs:**
- True fire-and-forget semantics (client doesn't wait)
- Data loss possible if PostgreSQL fails after the write was accepted
- Suitable for high-throughput analytics pipelines where some loss is acceptable

**Client Impact:**
- Works with standard Kafka clients using `acks=0`

### 2. Compression ✅ Fully Supported (Phase 8)

**Status:** Fully Implemented
**Kafka Behavior:** Supports gzip, snappy, lz4, zstd compression
**pg_kafka Behavior:** Full support for all compression codecs

**Implementation:**
- ✅ **Inbound (Producer → Server)**: Automatic decompression via `kafka-protocol` crate
- ✅ **Outbound (Server → Consumer)**: Configurable via `pg_kafka.compression_type` GUC
- ✅ **Supported codecs**: none, gzip, snappy, lz4, zstd

**Configuration:**
```sql
-- Set outbound compression (default: none)
SET pg_kafka.compression_type = 'gzip';
```

**Client Impact:**
- Clients can use any compression type (gzip, snappy, lz4, zstd)
- Server decompresses messages before storage
- Server compresses responses based on GUC setting

### 3. Idempotent Producer ✅ Fully Supported (Phase 9)

**Status:** Fully Implemented
**Kafka Behavior:** `enable.idempotence=true` prevents duplicate messages
**pg_kafka Behavior:** Full idempotency support with sequence validation

**Implementation:**
- ✅ InitProducerId API allocates producer IDs with epoch
- ✅ Sequence number tracking per topic-partition
- ✅ Duplicate detection (DUPLICATE_SEQUENCE_NUMBER error)
- ✅ Out-of-order detection (OUT_OF_ORDER_SEQUENCE_NUMBER error)
- ✅ Producer fencing via epoch (PRODUCER_FENCED error)
- ✅ PostgreSQL advisory locks for sequence serialization

**Client Impact:**
- Clients can use `enable.idempotence=true` (default in librdkafka 2.0+)
- Full exactly-once delivery semantics within a producer session
- Standard duplicate handling works as expected

### 4. Transactions ✅ Fully Supported (Phase 10)

**Status:** Fully Implemented
**Kafka Behavior:** Multi-partition atomic writes via `transactional.id`
**pg_kafka Behavior:** Full transaction coordinator with EOS

**Implementation:**
- ✅ AddPartitionsToTxn registers partitions in transaction
- ✅ AddOffsetsToTxn includes consumer offsets in transaction
- ✅ EndTxn commits or aborts atomically
- ✅ TxnOffsetCommit commits offsets within transaction context
- ✅ Read-committed isolation level filters pending/aborted messages
- ✅ Transactional producer fencing via epoch

**Client Impact:**
- Transactional producers work without modification
- Exactly-once consume-transform-produce patterns supported
- Read-committed consumers see only committed messages

## Consumer API Deviations

**Status:** Phase 5 Complete (Full Implementation)

### 5. Consumer Groups ✅ Fully Implemented

**Status:** Full support with automatic partition assignment and rebalancing
**Kafka Behavior:** Automatic partition assignment with rebalancing
**pg_kafka Behavior:** Same, with Range, RoundRobin, and Sticky strategies

**What Works:**
- ✅ FindCoordinator API
- ✅ JoinGroup API (member registration)
- ✅ Heartbeat API (membership maintenance)
- ✅ LeaveGroup API (graceful departure)
- ✅ SyncGroup API (partition assignment distribution)
- ✅ DescribeGroups/ListGroups APIs
- ✅ Thread-safe coordinator state (Arc<RwLock>)
- ✅ Automatic partition assignment (Range, RoundRobin, Sticky strategies)
- ✅ Automatic rebalancing on member join/leave
- ✅ Member timeout detection (session timeout)
- ✅ REBALANCE_IN_PROGRESS error handling

**What's Not Implemented:**
- ❌ Static group membership (KIP-345)
- ❌ Cooperative rebalancing (KIP-429)

**Client Impact:**
- Full consumer group functionality works with standard Kafka clients
- Both manual partition assignment and automatic subscription supported

### 6. Offset Management ✅ Implemented

**Status:** Fully implemented
**Kafka Behavior:** Stores offsets in compacted __consumer_offsets topic
**pg_kafka Behavior:** Stores offsets in `kafka.consumer_offsets` table

**Implementation:**
- ✅ OffsetCommit API stores offsets in PostgreSQL
- ✅ OffsetFetch API retrieves committed offsets
- ✅ Consumer groups can track progress across restarts
- ✅ Supports offset metadata field

**Difference:**
- PostgreSQL table instead of Kafka's compacted topic
- No log compaction (not needed with SQL UPDATE semantics)

### 7. Fetch Request ✅ Implemented

**Status:** Fully implemented
**Kafka Behavior:** Returns RecordBatch format messages
**pg_kafka Behavior:** Same, with limitations

**What Works:**
- ✅ FetchRequest/Response handling
- ✅ RecordBatch v2 encoding
- ✅ Empty fetch responses (returns empty bytes)
- ✅ Partition watermarks (high watermark, log start offset)
- ✅ Long polling (max_wait_ms/min_bytes support) - Phase 8

**What's Missing:**
- ❌ Fetch sessions (always uses fetch session ID 0)

**Client Impact:**
- Works with standard Kafka clients
- Low latency with long polling enabled (default: on)

### 8. ListOffsets ✅ Implemented

**Status:** Fully implemented
**Kafka Behavior:** Returns offsets by timestamp or special values
**pg_kafka Behavior:** Supports earliest (-2), latest (-1), and timestamp lookup

**Implementation:**
- ✅ Returns earliest offset (first message in partition)
- ✅ Returns latest offset (high watermark)
- ✅ Timestamp-based lookup — returns the earliest offset whose record timestamp is ≥ the requested time (record timestamps are persisted; AUDIT-2026-06 BUG-7 + CONF-7)

## Metadata API Deviations

### 9. Single Broker Only

**Status:** By Design
**Kafka Behavior:** Returns list of all brokers in cluster
**pg_kafka Behavior:** Returns single broker (self)

**Rationale:**
- `pg_kafka` is a single-node "broker" backed by PostgreSQL
- High availability comes from PostgreSQL HA (Patroni, RDS Multi-AZ, etc.)

### 10. Dynamic Topic Creation

**Status:** Always enabled
**Kafka Behavior:** `auto.create.topics.enable` defaults to `false`
**pg_kafka Behavior:** Topics auto-created on first produce

**Client Impact:**
- Typos in topic names create unwanted topics

### 11. Multi-Partition Topics ✅ Implemented (Phase 7)

**Status:** Fully implemented
**Kafka Behavior:** Supports multiple partitions per topic with key-based routing
**pg_kafka Behavior:** Same, with configurable default partition count

**Implementation:**
- ✅ Topics can have any number of partitions (set via CreateTopics or `pg_kafka.default_partitions` GUC)
- ✅ Key-based partition routing using murmur2 hash (Kafka-compatible)
- ✅ Explicit partition assignment in ProduceRequest supported
- ✅ Metadata reports correct partition count

**Technical Details:**
- Uses `murmur2` crate with `KAFKA_SEED` for hash compatibility with Kafka
- When partition_index == -1, server computes: `murmur2(key) % partition_count`

### 12. Null Key Partition Routing (Minor Deviation)

**Status:** Minor deviation from Kafka behavior
**Kafka Behavior:** Uses "sticky partitioner" for null keys (batches to same partition until batch completes)
**pg_kafka Behavior:** Uses random partition selection for null keys

**Rationale:**
- Sticky partitioner requires tracking batch state across requests
- Random distribution provides similar overall distribution
- Kafka clients typically handle null-key partitioning client-side anyway

**Client Impact:**
- Messages without keys may not batch to the same partition
- No impact on ordering guarantees (null keys have no ordering guarantee in Kafka either)

## API Version Support

### Supported APIs (18 total)

| API Key | Name | Versions | Status |
|---------|------|----------|--------|
| 0 | Produce | 3-9 | ✅ Full support (with key-based routing) |
| 1 | Fetch | 0-13 | ✅ Full support |
| 2 | ListOffsets | 0-7 | ✅ Implemented (incl. timestamp lookup) |
| 3 | Metadata | 0-9 | ✅ Full support |
| 8 | OffsetCommit | 0-8 | ✅ Full support |
| 9 | OffsetFetch | 0-7 | ✅ Full support |
| 10 | FindCoordinator | 0-3 | ✅ Full support |
| 11 | JoinGroup | 0-7 | ✅ Full support |
| 12 | Heartbeat | 0-4 | ✅ Full support |
| 13 | LeaveGroup | 0-4 | ✅ Full support |
| 14 | SyncGroup | 0-4 | ✅ Full support |
| 15 | DescribeGroups | 0-4 | ✅ Full support |
| 16 | ListGroups | 0-4 | ✅ Full support |
| 18 | ApiVersions | 0-3 | ✅ Full support |
| 19 | CreateTopics | 0-7 | ✅ Full support |
| 20 | DeleteTopics | 0-6 | ✅ Full support |
| 37 | CreatePartitions | 0-3 | ✅ Full support |
| 42 | DeleteGroups | 0-2 | ✅ Full support |

**Note:** OffsetFetch v8+ uses different response format. We support v0-7.

## Error Handling

pg_kafka implements typed error handling that maps internal errors to Kafka protocol error codes:

```rust
pub enum KafkaError {
    UnknownTopic { topic: String },
    UnknownMemberId { group_id: String, member_id: String },
    IllegalGeneration { group_id: String, generation: i32, expected: i32 },
    NotCoordinator { group_id: String },
    // ... more variants
}

impl KafkaError {
    pub fn to_kafka_error_code(&self) -> i16 {
        match self {
            KafkaError::UnknownTopic { .. } => 3,  // UNKNOWN_TOPIC_OR_PARTITION
            KafkaError::UnknownMemberId { .. } => 25, // UNKNOWN_MEMBER_ID
            KafkaError::IllegalGeneration { .. } => 22, // ILLEGAL_GENERATION
            // ... more mappings
        }
    }
}
```

**Key Design Decision:** Errors are typed rather than string-based, enabling:
- Compile-time exhaustiveness checking
- Direct mapping to Kafka protocol error codes
- Consistent error responses across all handlers

### Notable Missing APIs

- Security APIs (SaslHandshake/SaslAuthenticate, ACLs) - Not planned; use PostgreSQL auth + network controls
- Broker-internal / admin-utility APIs (DescribeTransactions, ListTransactions, DescribeProducers) - Not planned (monitoring-only)
- Log compaction - Not planned (PostgreSQL UPDATE provides similar semantics)

> Transaction APIs (InitProducerId 22, AddPartitionsToTxn 24, AddOffsetsToTxn 25, EndTxn 26, TxnOffsetCommit 28) and the core Admin APIs (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups) **are** implemented — see the Summary above and `KAFKA_PROTOCOL_COVERAGE.md`.

## Network Security Posture (SEC-7)

**Status:** Accepted by design, with a startup warning.

pg_kafka speaks the Kafka wire protocol with **no authentication or authorization** — there is no SaslHandshake/SaslAuthenticate negotiation and no ACL enforcement. Any client that can open a TCP connection to the listener can read and write any topic.

- **Default bind is `0.0.0.0`** (`pg_kafka.host`), i.e. all interfaces — chosen because a broker is typically reached from other hosts, and a loopback-only default would make the out-of-the-box configuration unusable for that case.
- **Mitigation (required for non-loopback binds):** restrict access at the network layer — firewall / cloud security group / private subnet — and rely on PostgreSQL's own authentication for the control plane. Set `pg_kafka.host = '127.0.0.1'` to accept only local clients.
- **Loud by default:** when the listener binds to a non-loopback interface, pg_kafka emits a startup `WARNING` (see `bind_is_publicly_exposed` in `config.rs` / the bind path in `worker.rs`) so the no-auth exposure is never silent.

Wire-level SASL/ACLs are **not planned**; the accepted posture is network-control + PostgreSQL auth.

## Compatibility Testing

### Tested Clients

| Client | Version | Status | Notes |
|--------|---------|--------|-------|
| kcat | 1.7.0+ | ✅ Works | Producer and consumer tested |
| rdkafka (Rust) | 0.36+ | ✅ Works | Full E2E test suite (181 tests) |

### Client Configuration

```properties
# Producer settings
bootstrap.servers=localhost:9092
acks=1  # Default (acks=0 also supported for fire-and-forget)
compression.type=gzip  # Optional: none, gzip, snappy, lz4, zstd
enable.idempotence=true  # Supported (idempotent producer — Phase 9 / InitProducerId)

# Consumer settings
bootstrap.servers=localhost:9092
group.id=my-consumer-group
enable.auto.commit=false  # Manual commits recommended
auto.offset.reset=earliest
```

## Future Work

> **Shadow Mode is implemented** (Phase 11 — external Kafka forwarding, per-topic config, SASL/SSL) and is no longer future work. See `KAFKA_PROTOCOL_COVERAGE.md` and the `SHADOW_MODE_*` docs. Known correctness limitations (durable forwarding, replay idempotency, the license gate) are tracked as SH-1..16 in `AUDIT-2026-06.md`.

### Advanced Consumer Features (Future)

| Feature | Description | Complexity |
|---------|-------------|------------|
| Static Group Membership (KIP-345) | Stable member IDs across restarts | Medium |
| Cooperative Rebalancing (KIP-429) | Incremental rebalancing | Very High |

### May Not Implement

| Feature | Rationale |
|---------|-----------|
| Log compaction | PostgreSQL UPDATE provides similar semantics |
| Replication protocol | Rely on PostgreSQL HA (Patroni, RDS) |
| Quotas | Not needed for target use cases |
| SASL authentication | Use PostgreSQL authentication instead |

> Exactly-once semantics (EOS) **is** implemented via the Phase 10 transaction APIs and read-committed isolation — see the Summary above and `KAFKA_PROTOCOL_COVERAGE.md`.

---

**Last Updated:** 2026-06-29
**Applies To:** pg_kafka Phase 11 Complete (Shadow Mode)
**API Coverage:** 23 of ~50 Kafka APIs (46%)
**Test Status:** 672 unit tests + 181 E2E tests (CI-gated)
