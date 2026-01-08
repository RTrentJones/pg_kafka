# Kafka Protocol Deviations

This document lists intentional deviations from the official Kafka protocol specification.

## Summary

`pg_kafka` implements a **subset** of the Kafka wire protocol (36% API coverage). The following deviations are intentional design decisions that optimize for PostgreSQL's strengths while maintaining compatibility with standard Kafka clients.

| Category | Status |
|----------|--------|
| **Producer APIs** | ✅ Full support (with documented limitations) |
| **Consumer APIs** | ✅ Full support (with automatic rebalancing) |
| **Coordinator APIs** | ✅ Full support (Range, RoundRobin, Sticky strategies) |
| **Admin APIs** | ✅ Full support (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups) |
| **Transaction APIs** | ❌ Not planned |

**Current Implementation:** Phase 7 Complete

## Producer API Deviations

### 1. acks=0 (Fire-and-Forget) Not Supported

**Status:** Rejected
**Kafka Behavior:** Client sends message, does not wait for acknowledgment
**pg_kafka Behavior:** Returns error response with `INVALID_REQUEST`

**Rationale:**
- `acks=0` violates ACID guarantees that are a core benefit of using PostgreSQL
- No way to return offset to client (fire-and-forget means no response)
- Would require additional async machinery to handle "send but don't wait" semantics
- Users wanting fire-and-forget should use real Kafka

**Client Impact:**
- Clients using `acks=0` will receive error responses
- Workaround: Use `acks=1` (leader acknowledgment) instead

### 2. Compression Not Supported

**Status:** Not Implemented
**Kafka Behavior:** Supports gzip, snappy, lz4, zstd compression
**pg_kafka Behavior:** Accepts only uncompressed messages

**Rationale:**
- Compression adds significant complexity
- PostgreSQL already has TOAST for large values
- Can be added in future phases if needed

**Client Impact:**
- Clients must set `compression.type=none`

### 3. Idempotent Producer Not Supported

**Status:** Not Implemented
**Kafka Behavior:** `enable.idempotence=true` prevents duplicate messages
**pg_kafka Behavior:** No deduplication mechanism

**Rationale:**
- Requires tracking producer IDs and sequence numbers
- PostgreSQL transactions already provide atomicity
- Duplicates can occur on network retry (same as Kafka without idempotence)

**Client Impact:**
- Clients should handle potential duplicates at application level
- Or rely on database constraints (e.g., UNIQUE on message key)

### 4. Transactions Not Supported

**Status:** Not Implemented
**Kafka Behavior:** Multi-partition atomic writes via `transactional.id`
**pg_kafka Behavior:** No transaction coordinator

**Rationale:**
- Kafka transactions require two-phase commit across partitions
- Current focus is on single-partition producer/consumer support
- Could potentially leverage PostgreSQL's native transaction support in future phases

**Client Impact:**
- Clients using Kafka transactions will fail
- Workaround: Use PostgreSQL transactions at application level (each produce is already atomic)

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

**What's Missing:**
- ❌ Long polling optimization (no LISTEN/NOTIFY yet)
- ❌ Fetch sessions (always uses fetch session ID 0)

**Client Impact:**
- Works with standard Kafka clients
- May have higher CPU usage due to polling

### 8. ListOffsets ✅ Implemented

**Status:** Fully implemented for special timestamps
**Kafka Behavior:** Returns offsets by timestamp or special values
**pg_kafka Behavior:** Supports earliest (-2) and latest (-1) only

**Implementation:**
- ✅ Returns earliest offset (first message in partition)
- ✅ Returns latest offset (high watermark)
- ❌ Timestamp-based lookup not implemented (returns UNSUPPORTED_VERSION)

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
| 2 | ListOffsets | 0-7 | ✅ Partial (special offsets only) |
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

- Transaction APIs (22-26) - Not planned
- Admin/Security APIs - Not planned
- Log compaction - Not planned (PostgreSQL UPDATE provides similar semantics)

## Compatibility Testing

### Tested Clients

| Client | Version | Status | Notes |
|--------|---------|--------|-------|
| kcat | 1.7.0+ | ✅ Works | Producer and consumer tested |
| rdkafka (Rust) | 0.36+ | ✅ Works | Full E2E test suite (5 scenarios) |

### Client Configuration

```properties
# Producer settings
bootstrap.servers=localhost:9092
acks=1  # REQUIRED
compression.type=none  # REQUIRED
enable.idempotence=false  # REQUIRED

# Consumer settings
bootstrap.servers=localhost:9092
group.id=my-consumer-group
enable.auto.commit=false  # Manual commits recommended
auto.offset.reset=earliest
```

## Future Work

### Shadow Mode (Future Phase)

| Feature | Description | Complexity |
|---------|-------------|------------|
| Logical Decoding | Tail PostgreSQL WAL | High |
| External Kafka Production | Forward to real Kafka cluster | Medium |
| At-Least-Once Delivery | Acknowledge only after external ACK | Medium |

### Advanced Consumer Features (Future)

| Feature | Description | Complexity |
|---------|-------------|------------|
| Static Group Membership (KIP-345) | Stable member IDs across restarts | Medium |
| Cooperative Rebalancing (KIP-429) | Incremental rebalancing | Very High |

### May Not Implement

| Feature | Rationale |
|---------|-----------|
| Log compaction | PostgreSQL UPDATE provides similar semantics |
| Exactly-once semantics | Requires full transaction coordinator |
| Replication protocol | Rely on PostgreSQL HA (Patroni, RDS) |
| Quotas | Not needed for target use cases |
| SASL authentication | Use PostgreSQL authentication instead |

---

**Last Updated:** 2026-01-08
**Applies To:** pg_kafka Phase 7 Complete
**API Coverage:** 18 of ~50 Kafka APIs (36%)
**Test Status:** 175 unit tests + 90 E2E tests passing
