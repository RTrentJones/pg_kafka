# Kafka Protocol Deviations

This document lists intentional deviations from the official Kafka protocol specification.

## Summary

`pg_kafka` implements a **subset** of the Kafka wire protocol. The following deviations are by design for Phase 2 (Producer Support).

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

**Error Response:**
```rust
ProduceResponse {
    error_code: 42, // INVALID_REQUEST
    error_message: Some("acks=0 not yet supported".to_string()),
    ...
}
```

**Client Impact:**
- Clients using `acks=0` will receive error responses
- Workaround: Use `acks=1` (leader acknowledgment) instead

**Implementation:** [src/worker.rs:625-633](../src/worker.rs)

### 2. Compression Not Supported

**Status:** Not Implemented (Phase 2)
**Kafka Behavior:** Supports gzip, snappy, lz4, zstd compression
**pg_kafka Behavior:** Accepts only uncompressed messages

**Rationale:**
- Compression adds significant complexity
- PostgreSQL already has TOAST (Transparent Oversized Storage Technique) for large values
- Can be added in Phase 3+ if needed

**Client Impact:**
- Clients must set `compression.type=none`
- Compressed messages will be rejected with `UNSUPPORTED_COMPRESSION_TYPE`

### 3. Idempotent Producer Not Supported

**Status:** Not Implemented (Phase 2)
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

**Status:** Not Implemented (Phase 2)
**Kafka Behavior:** Multi-partition atomic writes via `transactional.id`
**pg_kafka Behavior:** No transaction coordinator

**Rationale:**
- Kafka transactions require two-phase commit across partitions
- Phase 2 focuses on single-partition producer support
- May be added in Phase 4 using PostgreSQL's native transaction support

**Client Impact:**
- Clients using Kafka transactions will fail
- Workaround: Use PostgreSQL transactions at application level

## Consumer API Deviations

**Status:** Phase 3 (Not Yet Implemented)

Consumer support is planned for Phase 3. Expected deviations:

### 5. Consumer Groups (Partial Support)

**Planned:** Simplified consumer group coordination
**Deviation:** No full rebalance protocol (Join/Sync/Heartbeat/Leave)

### 6. Offset Management (Simplified)

**Planned:** Store offsets in `kafka.consumer_offsets` table
**Deviation:** No compacted log semantics

## Metadata API Deviations

### 7. Single Broker Only

**Status:** By Design
**Kafka Behavior:** Returns list of all brokers in cluster
**pg_kafka Behavior:** Returns single broker (self)

**Rationale:**
- `pg_kafka` is a single-node "broker" backed by PostgreSQL
- No clustering/replication at the Kafka protocol level
- High availability comes from PostgreSQL HA (Patroni, RDS Multi-AZ, etc.)

**Client Impact:**
- Metadata response always shows 1 broker
- Clients cannot fail over to other brokers (use PostgreSQL HA instead)

### 8. Dynamic Topic Creation

**Status:** Supported (Differs from Kafka default)
**Kafka Behavior:** `auto.create.topics.enable` defaults to `false` in modern Kafka
**pg_kafka Behavior:** Topics auto-created on first produce (always enabled)

**Rationale:**
- Simplifies development workflow
- Aligns with PostgreSQL's "create if not exists" patterns
- Can be disabled in future via GUC parameter if needed

**Client Impact:**
- Typos in topic names create unwanted topics
- Consider implementing `pg_kafka.auto_create_topics` GUC in Phase 3

## API Version Support

### Supported APIs

| API Key | Name | Versions Supported |
|---------|------|-------------------|
| 0 | Produce | 0-9 |
| 3 | Metadata | 0-12 |
| 18 | ApiVersions | 0-3 |

### Unsupported APIs

All other Kafka APIs return `UNSUPPORTED_VERSION` or are not advertised in `ApiVersions` response.

**Notable Missing APIs (Phase 2):**
- Fetch (API key 1) - Planned for Phase 3
- ListOffsets (API key 2) - Planned for Phase 3
- OffsetCommit (API key 8) - Planned for Phase 3
- OffsetFetch (API key 9) - Planned for Phase 3
- FindCoordinator (API key 10) - May not implement
- JoinGroup (API key 11) - Simplified in Phase 3
- Heartbeat (API key 12) - Simplified in Phase 3
- LeaveGroup (API key 13) - Simplified in Phase 3
- SyncGroup (API key 14) - Simplified in Phase 3

## Performance Deviations

### 9. No Zero-Copy Transfers

**Status:** By Design
**Kafka Behavior:** Uses sendfile() for zero-copy data transfer
**pg_kafka Behavior:** Data is copied from PostgreSQL → Rust → Network

**Rationale:**
- PostgreSQL SPI requires copying data into Rust memory
- Zero-copy would require direct access to PostgreSQL shared buffers (unsafe)

**Impact:** ~10-20% higher CPU usage compared to optimized Kafka

### 10. No Log Segments

**Status:** By Design
**Kafka Behavior:** Data stored in immutable log segments on disk
**pg_kafka Behavior:** Data stored in PostgreSQL heap tables

**Rationale:**
- PostgreSQL's storage engine handles all persistence
- Partitioning strategy (Phase 3) provides similar segment-like behavior

**Impact:**
- Different performance characteristics (PostgreSQL B-tree vs Kafka append-only log)
- Better random access, worse sequential throughput compared to Kafka

## Security Deviations

### 11. No SASL/SSL Support (Phase 2)

**Status:** Not Implemented
**Kafka Behavior:** Supports SASL (PLAIN, SCRAM, GSSAPI, OAUTHBEARER) and SSL/TLS
**pg_kafka Behavior:** No authentication/encryption at Kafka protocol level

**Rationale:**
- Security should be handled at PostgreSQL level (pg_hba.conf, SSL)
- Kafka protocol security adds complexity
- Can be added in Phase 4 if needed

**Client Impact:**
- Use PostgreSQL's native authentication instead
- Network encryption via PostgreSQL SSL/TLS

**Workaround:**
- Bind to `127.0.0.1` (localhost only) for development
- Use PostgreSQL's SSL for encryption
- Use firewall rules to restrict access to port 9092

## Compatibility Testing

### Tested Clients

| Client | Version | Status | Notes |
|--------|---------|--------|-------|
| kcat | 1.7.0+ | ✅ Works | Requires `acks=1` |
| rdkafka (Rust) | 0.36+ | ✅ Works | E2E test suite |
| kafka-python | TBD | ⏳ Untested | Should work with limitations |
| confluent-kafka (Python) | TBD | ⏳ Untested | Should work with limitations |
| KafkaJS | TBD | ⏳ Untested | Should work with limitations |
| Java Producer | TBD | ⏳ Untested | Should work with limitations |

### Client Configuration

To use pg_kafka with standard Kafka clients:

```properties
# Producer settings
bootstrap.servers=localhost:9092
acks=1  # REQUIRED: acks=0 not supported
compression.type=none  # REQUIRED: compression not supported
enable.idempotence=false  # REQUIRED: not supported
transactional.id=  # MUST be unset (transactions not supported)

# Retry settings (optional but recommended)
retries=3
max.in.flight.requests.per.connection=1  # For ordering guarantees
```

## Future Work

### Planned Improvements (Phase 3+)

1. **FetchRequest Support** - Enable consumer functionality
2. **Long Polling** - Use PostgreSQL `LISTEN/NOTIFY` for efficient polling
3. **Table Partitioning** - Time-based partitioning for retention management
4. **Consumer Group Coordination** - Simplified version using PostgreSQL tables

### May Not Implement

1. **Log Compaction** - PostgreSQL doesn't have log-structured storage
2. **Exactly-Once Semantics** - Requires idempotent producer + transactions
3. **Replication Protocol** - Use PostgreSQL replication instead
4. **Quotas** - Use PostgreSQL resource limits instead

## Reporting Issues

If you encounter unexpected behavior not documented here, please report:

1. Client library and version
2. Configuration settings
3. Expected vs actual behavior
4. Error messages from `pg_kafka` logs

**Note:** Behavior not listed in this document should match standard Kafka protocol specification. If it doesn't, that's a bug!

---

**Last Updated:** 2026-01-02
**Applies To:** pg_kafka Phase 2 (Producer Support)
