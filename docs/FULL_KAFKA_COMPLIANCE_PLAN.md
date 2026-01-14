# Full Kafka API Compliance Plan for pg_kafka

**Last Updated:** 2026-01-14

## Current State (Phase 11 Complete)

**APIs Implemented:** 23 of ~50 (46%)
**Tests:** 630 unit + 104 E2E (all passing)
**Architecture:** Repository Pattern, typed errors, clean handler structure, key-based partition routing, compression, transactions, shadow mode

### Implemented APIs
| Category | APIs |
|----------|------|
| Metadata | ApiVersions (18), Metadata (3) |
| Producer | Produce (0), InitProducerId (22) |
| Consumer | Fetch (1), ListOffsets (2), OffsetCommit (8), OffsetFetch (9) |
| Coordinator | FindCoordinator (10), JoinGroup (11), SyncGroup (14), Heartbeat (12), LeaveGroup (13), DescribeGroups (15), ListGroups (16) |
| Admin | CreateTopics (19), DeleteTopics (20), CreatePartitions (37), DeleteGroups (42) |
| Transaction | AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), TxnOffsetCommit (28) |

---

## Implementation Phases

### Phase 6: Admin APIs ✅ COMPLETE
**Goal:** Programmatic topic and group management

#### 6.1 CreateTopics (API 19)
**Files to modify:**
- `src/kafka/constants.rs` - Add `API_KEY_CREATE_TOPICS = 19`
- `src/kafka/messages.rs` - Add `KafkaRequest::CreateTopics`, `KafkaResponse::CreateTopics`
- `src/kafka/protocol/decoding.rs` - Add `parse_create_topics()`
- `src/kafka/handlers/admin.rs` - Create new file with `handle_create_topics()`
- `src/kafka/handlers/mod.rs` - Export admin handlers
- `src/kafka/storage/mod.rs` - Add `create_topic()` to KafkaStore trait
- `src/kafka/storage/postgres.rs` - Implement `create_topic()` with partition count
- `src/worker.rs` - Wire up dispatch

**Implementation:**
```rust
fn handle_create_topics(store: &impl KafkaStore, topics: Vec<CreateTopicRequest>)
    -> Result<CreateTopicsResponse>
```

#### 6.2 DeleteTopics (API 20)
**Files to modify:** Same pattern as CreateTopics
- Add `delete_topic()` to KafkaStore
- CASCADE delete messages and consumer_offsets

#### 6.3 CreatePartitions (API 37)
**Files to modify:**
- `src/kafka/storage/postgres.rs` - `add_partitions(topic_id, new_count)`
- Trigger rebalance for affected consumer groups

#### 6.4 DeleteGroups (API 42)
**Files to modify:**
- `src/kafka/coordinator.rs` - Add `delete_group()` method
- `src/kafka/storage/postgres.rs` - Delete from `kafka.consumer_offsets`

**Testing:**
- Unit tests in `src/kafka/handlers/tests.rs`
- E2E tests in `kafka_test/src/admin/`

---

### Phase 7: Multi-Partition Topics ✅ COMPLETE
**Goal:** Enable horizontal scaling with multiple partitions per topic

#### 7.1 Partition Count Configuration
**Files to modify:**
- `src/config.rs` - Add `pg_kafka.default_partitions` GUC
- `src/kafka/storage/postgres.rs` - Use partition count in `get_or_create_topic()`

#### 7.2 Key-Based Partition Routing
**Files to modify:**
- `src/kafka/handlers/produce.rs` - Add partition selection logic

```rust
fn compute_partition(key: Option<&[u8]>, partition_count: i32, explicit: i32) -> i32 {
    if explicit >= 0 { explicit }
    else if let Some(k) = key { murmur2_hash(k) % partition_count }
    else { rand::random::<i32>() % partition_count }
}
```

#### 7.3 Metadata Response Update
**Files to modify:**
- `src/kafka/handlers/metadata.rs` - Return actual partition count from `kafka.topics`

**Testing:**
- E2E: Produce to multi-partition topic, verify key distribution
- E2E: Multiple consumers on multi-partition topic

---

### Phase 8: Compression Support ✅ COMPLETE
**Goal:** Support gzip, snappy, lz4, zstd compression

#### Implementation (Completed)
- **Inbound (Producer → Server):** Automatic decompression via `kafka-protocol` crate
- **Outbound (Server → Consumer):** Configurable via `pg_kafka.compression_type` GUC
- **Supported codecs:** none, gzip, snappy, lz4, zstd

**Configuration:**
```sql
SET pg_kafka.compression_type = 'lz4';  -- Recommended for balanced performance
```

**Testing:**
- 5 E2E compression tests (gzip, snappy, lz4, zstd, roundtrip)
- Unit tests for compression parameter handling

---

### Phase 9: Idempotent Producer ✅ COMPLETE
**Goal:** Exactly-once delivery semantics

#### 9.1 Schema Changes
**Files to create:**
- `sql/idempotent.sql`:
```sql
CREATE TABLE kafka.producer_ids (
    producer_id BIGSERIAL PRIMARY KEY,
    epoch SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE kafka.producer_sequences (
    producer_id BIGINT NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    sequence_number INT NOT NULL,
    PRIMARY KEY (producer_id, topic_id, partition_id)
);
```

#### 9.2 InitProducerId Handler (API 22)
**Files to modify:**
- `src/kafka/handlers/idempotent.rs` - Create `handle_init_producer_id()`
- `src/kafka/storage/mod.rs` - Add `allocate_producer_id()`, `get_sequence()`

#### 9.3 Produce Deduplication
**Files to modify:**
- `src/kafka/handlers/produce.rs` - Check sequence numbers, reject duplicates

**Testing:**
- E2E: Producer with `enable.idempotence=true`
- Unit: Duplicate detection logic

---

### Phase 10: Transaction Support ✅ COMPLETE
**Goal:** Multi-partition atomic writes

#### 10.1 Transaction Coordinator
**APIs to implement:**
- AddPartitionsToTxn (24)
- AddOffsetsToTxn (25)
- EndTxn (26)
- TxnOffsetCommit (28)

**Schema:**
```sql
CREATE TABLE kafka.transactions (
    transactional_id TEXT PRIMARY KEY,
    producer_id BIGINT NOT NULL,
    producer_epoch SMALLINT NOT NULL,
    state TEXT NOT NULL, -- 'Ongoing', 'PrepareCommit', 'PrepareAbort', 'CompleteCommit', 'CompleteAbort'
    partitions JSONB,
    timeout_ms INT,
    started_at TIMESTAMP
);
```

**Architecture:**
- Map Kafka transaction states to PostgreSQL savepoints
- Use PostgreSQL's MVCC for isolation

---

### Phase 11: Shadow Mode ✅ COMPLETE
**Goal:** Forward messages to external Kafka cluster

**Implemented Features:**
- ✅ External Kafka connection with SASL/SSL authentication
- ✅ Dual-write (local PostgreSQL + external Kafka)
- ✅ Per-topic shadow configuration via `kafka.topic_shadow_config`
- ✅ Sync and async forwarding modes
- ✅ Percentage-based dial-up routing
- ✅ Historical message replay to external Kafka
- ✅ Graceful degradation when external Kafka unavailable
- ✅ 141 unit tests + 20 E2E tests for shadow mode

**Configuration:**
```sql
SET pg_kafka.shadow_mode_enabled = true;
SET pg_kafka.shadow_bootstrap_servers = 'kafka1:9092,kafka2:9092';
SET pg_kafka.shadow_security_protocol = 'SASL_SSL';
```

---

### Phase 12: Advanced Coordinator Features (Priority: LOW)
**Goal:** Reduce rebalance disruption

#### 12.1 Cooperative Rebalancing (KIP-429)
**Files to modify:**
- `src/kafka/assignment/strategies/sticky.rs` - Two-phase revoke/assign
- `src/kafka/coordinator.rs` - Track `owned_partitions` per member

#### 12.2 Static Group Membership (KIP-345)
**Files to modify:**
- `src/kafka/coordinator.rs` - Honor `group_instance_id`, skip rebalance on reconnect

---

## Implementation Order & Status

| Phase | Scope | Status | Dependencies |
|-------|-------|--------|--------------|
| **6** | Admin APIs | ✅ Complete | None |
| **7** | Multi-Partition | ✅ Complete | Phase 6 (CreatePartitions) |
| **8** | Compression | ✅ Complete | None |
| **9** | Idempotent Producer | ✅ Complete | None |
| **10** | Transactions | ✅ Complete | Phase 9 |
| **11** | Shadow Mode | ✅ Complete | None |
| **12** | Advanced Coordinator | Planned | None |

**Completed:** Phases 1-11
**Future:** Phase 12 (Advanced Coordinator), Cooperative Rebalancing (KIP-429), Static Group Membership (KIP-345)

---

## Critical Files Reference

| File | Purpose |
|------|---------|
| `src/kafka/handlers/` | All API handlers |
| `src/kafka/storage/mod.rs` | KafkaStore trait |
| `src/kafka/storage/postgres.rs` | PostgresStore implementation |
| `src/kafka/coordinator.rs` | Group state machine |
| `src/kafka/protocol/decoding.rs` | Request parsing |
| `src/kafka/messages.rs` | Request/Response enums |
| `src/worker.rs` | Request dispatch |
| `src/kafka/constants.rs` | API keys, error codes |
| `src/kafka/error.rs` | KafkaError → error codes |

---

## Testing Strategy

Each phase includes:
1. **Unit tests** - Handler logic with MockKafkaStore
2. **Storage tests** - PostgresStore with real DB
3. **E2E tests** - rdkafka client in `kafka_test/`

**Coverage targets:**
- Maintain 630+ unit tests
- Current: 104 E2E tests across all categories

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Breaking existing clients | Version gate new features, maintain backward compat |
| Compression library issues | Pin versions, test with real Kafka clients |
| Transaction complexity | Leverage PostgreSQL transactions, document deviations |
| Performance regression | Benchmark before/after each phase |

---

## Success Criteria

- [x] Phase 6: kcat/rdkafka can create/delete topics programmatically ✅
- [x] Phase 7: Multi-consumer groups work on multi-partition topics ✅
- [x] Phase 8: Compressed messages accepted and stored ✅
- [x] Phase 9: `enable.idempotence=true` works without duplicates ✅
- [x] Phase 10: Transactional producer commits atomically ✅
- [x] Phase 11: Shadow mode forwards to external Kafka ✅
- [x] All tests passing, zero compilation warnings ✅
