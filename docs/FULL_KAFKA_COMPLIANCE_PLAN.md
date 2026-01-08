# Full Kafka API Compliance Plan for pg_kafka

## Current State (Post Phase 7)

**APIs Implemented:** 18 of ~50 (36%)
**Tests:** 175 unit + 90 E2E (all passing)
**Architecture:** Repository Pattern, typed errors, clean handler structure, key-based partition routing

### Implemented APIs
| Category | APIs |
|----------|------|
| Metadata | ApiVersions (18), Metadata (3) |
| Producer | Produce (0) with key-based partition routing |
| Consumer | Fetch (1), ListOffsets (2), OffsetCommit (8), OffsetFetch (9) |
| Coordinator | FindCoordinator (10), JoinGroup (11), SyncGroup (14), Heartbeat (12), LeaveGroup (13), DescribeGroups (15), ListGroups (16) |
| Admin | CreateTopics (19), DeleteTopics (20), CreatePartitions (37), DeleteGroups (42) |

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

### Phase 8: Compression Support (Priority: MEDIUM)
**Goal:** Support gzip, snappy, lz4, zstd compression

#### 8.1 Add Dependencies
**Files to modify:**
- `Cargo.toml`:
```toml
flate2 = "1.0"    # gzip
snap = "1.1"      # snappy
lz4 = "1.24"      # lz4
zstd = "0.13"     # zstd
```

#### 8.2 Decompression in Protocol Layer
**Files to create:**
- `src/kafka/compression.rs` - Codec implementations

**Files to modify:**
- `src/kafka/protocol/recordbatch.rs` - Decompress before parsing records
- `src/kafka/handlers/produce.rs` - Store uncompressed (PostgreSQL handles storage compression)

#### 8.3 Optional Response Compression
- Honor client's `compression.type` preference in FetchResponse

**Testing:**
- Unit tests for each codec
- E2E with compressed producer config

---

### Phase 9: Idempotent Producer (Priority: MEDIUM)
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

### Phase 10: Transaction Support (Priority: LOW - Full Parity)
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

### Phase 11: Security APIs (Priority: LOW - Optional)
**Goal:** SASL authentication (alternative: use PostgreSQL auth)

#### 11.1 SaslHandshake (API 17) + SaslAuthenticate (API 36)
- Support PLAIN mechanism minimum
- Map to PostgreSQL roles

#### 11.2 ACL APIs (29, 30, 31)
- Map to PostgreSQL row-level security policies
- Store ACLs in `kafka.acls` table

**Note:** May defer entirely - PostgreSQL's native auth is more robust

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

## Implementation Order & Estimates

| Phase | Scope | Effort | Dependencies |
|-------|-------|--------|--------------|
| **6** | Admin APIs | 5-7 days | None |
| **7** | Multi-Partition | 3-5 days | Phase 6 (CreatePartitions) |
| **8** | Compression | 5-7 days | None |
| **9** | Idempotent Producer | 4-5 days | None |
| **10** | Transactions | 15-20 days | Phase 9 |
| **11** | Security | 10-15 days | None (optional) |
| **12** | Advanced Coordinator | 5-7 days | None |

**Total for Core Parity (Phases 6-9):** ~20 days
**Total for Full Parity (All Phases):** ~50-60 days

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
- Maintain 158+ unit tests
- Add 10-15 E2E tests per major phase

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

- [ ] Phase 6: kcat/rdkafka can create/delete topics programmatically
- [ ] Phase 7: Multi-consumer groups work on multi-partition topics
- [ ] Phase 8: Compressed messages accepted and stored
- [ ] Phase 9: `enable.idempotence=true` works without duplicates
- [ ] Phase 10: Transactional producer commits atomically
- [ ] All tests passing, zero compilation warnings
