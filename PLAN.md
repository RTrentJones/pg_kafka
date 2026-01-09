# Phase 9: Idempotent Producer Implementation Plan

## Overview

Implement exactly-once delivery semantics for pg_kafka by adding:
1. **InitProducerId API (API key 22)** - Allocates producer IDs and epochs
2. **Sequence tracking** - Database tables to track producer sequences
3. **Deduplication logic** - Validate sequences in ProduceRequest handler

## Design Decisions

### 1. Sequence Tracking: Separate Table (Recommended)
Use a separate `kafka.producer_sequences` table rather than adding columns to `kafka.messages`:
- Avoids bloating every message row for non-idempotent producers
- Efficient upsert operations with `ON CONFLICT` for deduplication
- Matches Kafka's internal architecture

### 2. Non-Idempotent Producers (producer_id = -1)
Skip idempotency checks when `producer_id = -1`:
- Maintains backward compatibility with existing clients
- No changes to current non-idempotent producer behavior

### 3. Sequence Validation Strategy
- **Duplicate sequence** → Error code 46 (DUPLICATE_SEQUENCE_NUMBER)
- **Sequence gap** → Error code 47 (OUT_OF_ORDER_SEQUENCE_NUMBER)
- **Old epoch** → Error code 91 (PRODUCER_FENCED)

---

## Implementation Steps

### Step 1: Database Schema Changes
**File:** `sql/bootstrap.sql`

Add two new tables:

```sql
-- Producer ID allocation for idempotent producers
CREATE TABLE kafka.producer_ids (
    producer_id BIGSERIAL PRIMARY KEY,
    epoch SMALLINT NOT NULL DEFAULT 0,
    client_id TEXT,
    transactional_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_active_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_producer_ids_txn_id
ON kafka.producer_ids(transactional_id) WHERE transactional_id IS NOT NULL;

-- Sequence tracking for idempotent deduplication
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

### Step 2: Add Constants
**File:** `src/kafka/constants.rs`

```rust
pub const API_KEY_INIT_PRODUCER_ID: i16 = 22;
pub const ERROR_DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
pub const ERROR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 47;
pub const ERROR_PRODUCER_FENCED: i16 = 91;
```

Update `get_flexible_format_threshold()` to include API 22.

---

### Step 3: Add Error Variants
**File:** `src/kafka/error.rs`

Add new variants:
- `DuplicateSequence { producer_id, sequence, expected }`
- `OutOfOrderSequence { producer_id, sequence, expected }`
- `ProducerFenced { producer_id, epoch, expected_epoch }`

Update `to_kafka_error_code()` to map these errors.

---

### Step 4: Extend KafkaStore Trait
**File:** `src/kafka/storage/mod.rs`

Add new methods:
```rust
// ===== Idempotent Producer Operations (Phase 9) =====

/// Allocate a new producer ID with epoch 0
fn allocate_producer_id(
    &self,
    client_id: Option<&str>,
    transactional_id: Option<&str>
) -> Result<(i64, i16)>;

/// Get existing producer info (for epoch validation)
fn get_producer_epoch(&self, producer_id: i64) -> Result<Option<i16>>;

/// Increment producer epoch (for reconnection)
fn increment_producer_epoch(&self, producer_id: i64) -> Result<i16>;

/// Check sequence and update if valid
/// Returns Ok(()) if valid, Err(DuplicateSequence/OutOfOrderSequence) if invalid
fn check_and_update_sequence(
    &self,
    producer_id: i64,
    producer_epoch: i16,
    topic_id: i32,
    partition_id: i32,
    base_sequence: i32,
    record_count: i32,
) -> Result<()>;
```

---

### Step 5: Update MockKafkaStore
**File:** `src/testing/mocks.rs`

Add mock expectations for all new KafkaStore methods.

---

### Step 6: Implement PostgresStore Methods
**File:** `src/kafka/storage/postgres.rs`

Implement each new trait method:
- `allocate_producer_id`: INSERT into `kafka.producer_ids`, return (id, epoch=0)
- `get_producer_epoch`: SELECT epoch FROM `kafka.producer_ids`
- `increment_producer_epoch`: UPDATE epoch + 1, return new epoch
- `check_and_update_sequence`:
  1. Use `pg_advisory_xact_lock(producer_id, hash)` for serialization
  2. SELECT last_sequence from `kafka.producer_sequences`
  3. Validate: `base_sequence == last_sequence + 1`
  4. UPSERT new sequence value: `last_sequence + record_count`

---

### Step 7: Add Message Types
**File:** `src/kafka/messages.rs`

Add to `KafkaRequest` enum:
```rust
InitProducerId {
    correlation_id: i32,
    client_id: Option<String>,
    api_version: i16,
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    producer_id: i64,      // -1 for new producer
    producer_epoch: i16,   // -1 for new producer
    response_tx: ResponseSender,
}
```

Add to `KafkaResponse` enum:
```rust
InitProducerId {
    correlation_id: i32,
    api_version: i16,
    response: InitProducerIdResponse,
}
```

Add `ProducerMetadata` struct for passing through produce pipeline:
```rust
pub struct ProducerMetadata {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}
```

---

### Step 8: Add Protocol Parsing
**File:** `src/kafka/protocol/decoding.rs`

Add `parse_init_producer_id()` function following existing patterns.
Add case to `parse_request()` match for `API_KEY_INIT_PRODUCER_ID`.

---

### Step 9: Extract Producer Metadata from RecordBatch
**File:** `src/kafka/protocol/recordbatch.rs`

Modify `parse_record_batch()` to return producer metadata:
- Extract `producer_id` (i64) at byte offset 23
- Extract `producer_epoch` (i16) at byte offset 31
- Extract `base_sequence` (i32) at byte offset 33

Return a struct containing both records and metadata.

---

### Step 10: Add Response Encoding
**File:** `src/kafka/protocol/encoding.rs`

Add encoding case for `KafkaResponse::InitProducerId`.

---

### Step 11: Create InitProducerId Handler
**File:** `src/kafka/handlers/init_producer_id.rs` (new file)

```rust
pub fn handle_init_producer_id(
    store: &impl KafkaStore,
    transactional_id: Option<String>,
    existing_producer_id: i64,
    existing_epoch: i16,
) -> Result<(i64, i16)> {
    if existing_producer_id == -1 {
        // New producer: allocate ID with epoch 0
        store.allocate_producer_id(None, transactional_id.as_deref())
    } else {
        // Existing producer: validate and bump epoch
        let current_epoch = store.get_producer_epoch(existing_producer_id)?
            .ok_or(KafkaError::UnknownProducerId { producer_id: existing_producer_id })?;

        if existing_epoch < current_epoch {
            return Err(KafkaError::ProducerFenced { ... });
        }

        let new_epoch = store.increment_producer_epoch(existing_producer_id)?;
        Ok((existing_producer_id, new_epoch))
    }
}
```

---

### Step 12: Update Produce Handler
**File:** `src/kafka/handlers/produce.rs`

Modify `handle_produce()` to accept and validate producer metadata:

```rust
pub fn handle_produce(
    store: &impl KafkaStore,
    topic_data: Vec<TopicProduceData>,
    default_partitions: i32,
    producer_metadata: Option<ProducerMetadata>,  // NEW
) -> Result<ProduceResponse> {
    // Before insert_records, validate sequence for idempotent producers
    if let Some(ref meta) = producer_metadata {
        if meta.producer_id >= 0 {
            store.check_and_update_sequence(
                meta.producer_id,
                meta.producer_epoch,
                topic_id,
                partition_index,
                meta.base_sequence,
                records.len() as i32,
            )?;
        }
    }

    // Continue with existing insert logic...
}
```

---

### Step 13: Update Response Builders
**File:** `src/kafka/response_builders.rs`

Add `build_init_producer_id_response()` and error response builder.

Update `build_api_versions_response()` to advertise InitProducerId API:
```rust
// InitProducerId (API 22): versions 0-4
api_keys.push(ApiVersion { api_key: 22, min_version: 0, max_version: 4 });
```

---

### Step 14: Update Handlers Module
**File:** `src/kafka/handlers/mod.rs`

```rust
mod init_producer_id;
pub use init_producer_id::handle_init_producer_id;
```

---

### Step 15: Update Worker Dispatch
**File:** `src/worker.rs`

Add dispatch case for `KafkaRequest::InitProducerId`.
Update Produce dispatch to extract and pass producer metadata.

---

## Test Cases

### Unit Tests (src/kafka/handlers/tests.rs)

| Test | Description |
|------|-------------|
| `test_handle_init_producer_id_new` | New producer gets ID with epoch 0 |
| `test_handle_init_producer_id_reconnect` | Existing producer gets epoch bumped |
| `test_handle_init_producer_id_fenced` | Old epoch returns PRODUCER_FENCED |
| `test_handle_produce_idempotent_success` | Valid sequence accepted |
| `test_handle_produce_duplicate_sequence` | Duplicate returns error 46 |
| `test_handle_produce_out_of_order` | Gap returns error 47 |
| `test_handle_produce_non_idempotent` | producer_id=-1 skips validation |

### Storage Tests (src/kafka/storage/tests.rs)

| Test | Description |
|------|-------------|
| `test_allocate_producer_id` | ID allocation works |
| `test_allocate_producer_id_with_txn_id` | Transactional ID stored |
| `test_get_producer_epoch` | Epoch retrieval works |
| `test_increment_producer_epoch` | Epoch bump works |
| `test_check_sequence_first` | First sequence (0) accepted |
| `test_check_sequence_increment` | Sequential sequences work |
| `test_check_sequence_duplicate` | Duplicate rejected |
| `test_check_sequence_gap` | Gap rejected |

### E2E Tests (kafka_test/src/lib.rs)

| Test | Description |
|------|-------------|
| `test_idempotent_producer_basic` | enable.idempotence=true works |
| `test_idempotent_producer_retry` | Retry produces no duplicates |
| `test_non_idempotent_still_works` | Backward compatibility |

---

## Documentation Updates

### CLAUDE.md
- Update status: "Phase 9 Complete: Idempotent Producer"
- Add to "What Works Now": InitProducerId, sequence validation
- Update API Coverage: 19 of 50 (38%)
- Update test count

### README.md
- Add Phase 9 to completed phases
- Document `enable.idempotence=true` client support

### docs/KAFKA_PROTOCOL_COVERAGE.md
- Add InitProducerId (API 22) to supported APIs table
- Document idempotency semantics

### docs/PROTOCOL_DEVIATIONS.md
- Note: Idempotent producers supported, transactions NOT yet (Phase 10)

---

## File Change Summary

| File | Change Type |
|------|-------------|
| `sql/bootstrap.sql` | Add 2 tables |
| `src/kafka/constants.rs` | Add 4 constants |
| `src/kafka/error.rs` | Add 3 error variants |
| `src/kafka/storage/mod.rs` | Add 4 trait methods |
| `src/kafka/storage/postgres.rs` | Implement 4 methods |
| `src/testing/mocks.rs` | Add mock expectations |
| `src/kafka/messages.rs` | Add request/response variants |
| `src/kafka/protocol/decoding.rs` | Add parser |
| `src/kafka/protocol/encoding.rs` | Add encoder |
| `src/kafka/protocol/recordbatch.rs` | Extract producer metadata |
| `src/kafka/handlers/init_producer_id.rs` | New file |
| `src/kafka/handlers/produce.rs` | Add sequence validation |
| `src/kafka/handlers/mod.rs` | Export new handler |
| `src/kafka/response_builders.rs` | Add response builders |
| `src/worker.rs` | Add dispatch case |
| `src/kafka/handlers/tests.rs` | Add 7 tests |
| `src/kafka/storage/tests.rs` | Add 8 tests |
| `kafka_test/src/lib.rs` | Add 3 E2E tests |
| `CLAUDE.md` | Update status |
| `README.md` | Update status |
| `docs/KAFKA_PROTOCOL_COVERAGE.md` | Add API 22 |
| `docs/PROTOCOL_DEVIATIONS.md` | Document limitations |

---

## Commit Message

```
feat(phase9): Add idempotent producer support

Implement exactly-once delivery semantics with InitProducerId API:

- Add InitProducerId API (key 22) for producer ID allocation
- Add kafka.producer_ids table for ID/epoch tracking
- Add kafka.producer_sequences table for sequence deduplication
- Validate producer sequences in ProduceRequest handler
- Return DUPLICATE_SEQUENCE_NUMBER (46) for duplicates
- Return OUT_OF_ORDER_SEQUENCE_NUMBER (47) for gaps
- Skip validation for non-idempotent producers (producer_id=-1)

Clients can now use `enable.idempotence=true` for exactly-once semantics.

API Coverage: 19/50 (38%)
Tests: 15 new unit tests, 3 new E2E tests

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
```
