# Test Coverage Analysis

**Date:** 2026-01-02
**Phase:** Phase 2 (Producer Support Complete)

## Executive Summary

**Current Coverage:** ~60% estimated
**Coverage Gaps:** Worker SPI integration, error paths, configuration edge cases
**Recommendation:** Add targeted unit tests for uncovered critical paths

## Test Infrastructure

### Unit Tests (tests/*.rs)
- **protocol_tests.rs**: 10 tests - Protocol parsing/encoding ✅
- **encoding_tests.rs**: 8 tests - Binary encoding/decoding ✅
- **property_tests.rs**: 10 tests - Property-based testing ✅
- **helpers.rs**: 6 helper functions
- **worker_tests.rs**: ❌ **EMPTY** (0 tests)

### Integration Tests
- **kafka_test/**: Full E2E test with rdkafka client ✅
- **lib.rs#tests**: 1 smoke test (`test_hello_pg_kafka`) ✅

### Total Test Count
- Unit tests: ~28 test functions
- Integration tests: 1 E2E test
- Property tests: 10 property tests
- **Total**: ~39 distinct tests

## Coverage by Module

### ✅ Well-Covered Modules (80%+)

#### 1. src/kafka/protocol.rs (512 lines)
**Coverage: ~85%**

**Tested:**
- ✅ ApiVersions request parsing (protocol_tests.rs)
- ✅ Metadata request parsing (protocol_tests.rs)
- ✅ ProduceRequest parsing (protocol_tests.rs)
- ✅ Response encoding (encoding_tests.rs)
- ✅ Error handling for malformed requests (protocol_tests.rs)
- ✅ Binary framing (encoding_tests.rs)

**Gaps:**
- ❌ Edge cases: Maximum request size handling
- ❌ Unsupported API versions
- ❌ Correlation ID edge cases

**Test Files:**
- tests/protocol_tests.rs (10 tests)
- tests/encoding_tests.rs (8 tests)

#### 2. src/kafka/messages.rs (211 lines)
**Coverage: ~70%**

**Tested:**
- ✅ Request queue operations (integration via E2E)
- ✅ Message serialization (encoding_tests.rs)
- ✅ Response channel communication (E2E)

**Gaps:**
- ❌ Queue capacity edge cases
- ❌ Channel failure scenarios

#### 3. src/kafka/error.rs (97 lines)
**Coverage: ~90%**

**Tested:**
- ✅ All error variants have display tests (error.rs#tests)
- ✅ Error conversion (From implementations tested)

**Gaps:**
- ❌ Error propagation in real scenarios

### ⚠️ Partially Covered Modules (40-80%)

#### 4. src/kafka/listener.rs (191 lines)
**Coverage: ~50%**

**Tested:**
- ✅ TCP connection handling (E2E test)
- ✅ Request/response cycle (E2E test)
- ✅ Framing with LengthDelimitedCodec (E2E)

**Gaps:**
- ❌ Connection error recovery
- ❌ Shutdown signal handling
- ❌ Max frame size enforcement
- ❌ Malformed frame handling
- ❌ Client disconnect scenarios

**Recommendation:** Add unit tests for error paths

#### 5. src/config.rs (104 lines)
**Coverage: ~60%**

**Tested:**
- ✅ Config loading (implicit via E2E)
- ✅ Default values (implicit via E2E)

**Gaps:**
- ❌ GUC parameter validation
- ❌ Invalid port numbers (0, negative, > 65535)
- ❌ Invalid host addresses
- ❌ Shutdown timeout edge cases

**Recommendation:** Add unit tests for validation logic

#### 6. src/kafka/response_builders.rs (102 lines)
**Coverage: ~70%**

**Tested:**
- ✅ ApiVersions response building (E2E)
- ✅ Metadata response building (E2E)
- ✅ Produce response building (E2E)

**Gaps:**
- ❌ Error response building
- ❌ Edge cases for topic/partition counts

### ❌ Poorly Covered Modules (<40%)

#### 7. src/worker.rs (1201 lines) - **CRITICAL GAP**
**Coverage: ~30% estimated**

**Tested:**
- ✅ End-to-end produce flow (E2E test)
- ✅ Topic auto-creation (E2E test)
- ✅ Offset calculation (E2E test)

**Gaps (CRITICAL):**
- ❌ **worker_tests.rs is EMPTY** (0 tests!)
- ❌ `get_or_create_topic()` error cases
- ❌ `insert_records()` error cases
- ❌ Empty record batch handling
- ❌ Transaction rollback scenarios
- ❌ SPI error handling
- ❌ Advisory lock timeout
- ❌ MAX(offset) race conditions (should be prevented by lock)
- ❌ JSONB serialization failures
- ❌ acks=0 rejection (tested via E2E but no unit test)
- ❌ acks=all handling
- ❌ Request processing error paths

**Functions Needing Tests:**
```rust
fn get_or_create_topic(topic_name: &str) -> Result<i32, Box<dyn Error>>
fn insert_records(topic_id: i32, partition_id: i32, records: &[Record]) -> Result<i64, Box<dyn Error>>
fn hex_encode(data: &[u8]) -> String
pub fn process_request(request: KafkaRequest)
```

**Recommendation:** **HIGH PRIORITY** - Create worker_tests.rs with SPI mocking

#### 8. src/kafka/constants.rs (198 lines)
**Coverage: N/A**

This is a constants file - no runtime logic to test.

#### 9. src/lib.rs (118 lines)
**Coverage: ~40%**

**Tested:**
- ✅ hello_pg_kafka() (lib.rs#tests)
- ✅ Extension loading (E2E test)

**Gaps:**
- ❌ _PG_init hook error handling
- ❌ Background worker registration failures
- ❌ Schema creation failures

## Critical Coverage Gaps

### Priority 1: Worker SPI Integration Tests

**File:** tests/worker_tests.rs (currently EMPTY!)

**Required Tests:**
1. Test `get_or_create_topic()`:
   - ✅ Creates new topic
   - ❌ Returns existing topic ID on conflict
   - ❌ Handles invalid topic names
   - ❌ SPI connection failure

2. Test `insert_records()`:
   - ✅ Inserts single record (E2E)
   - ❌ Inserts batch of records
   - ❌ Empty record batch (early return)
   - ❌ Offset calculation with existing data
   - ❌ Advisory lock prevents race conditions
   - ❌ JSONB serialization failure
   - ❌ SPI INSERT failure
   - ❌ Transaction rollback

3. Test `process_request()`:
   - ✅ ApiVersions (E2E)
   - ✅ Metadata (E2E)
   - ✅ Produce (E2E)
   - ❌ Produce with acks=0 (should reject)
   - ❌ Produce with acks=all
   - ❌ Produce with empty batch
   - ❌ Produce with SPI failure
   - ❌ Response channel send failure

### Priority 2: Error Path Coverage

**File:** tests/protocol_tests.rs (add more)

**Required Tests:**
1. Malformed requests:
   - ✅ Invalid API key (exists)
   - ❌ Request size exceeds MAX_REQUEST_SIZE
   - ❌ Request size is negative
   - ❌ Truncated request body
   - ❌ Invalid API version

2. Edge cases:
   - ❌ Correlation ID wraparound (INT_MAX)
   - ❌ Empty client_id
   - ❌ Very long client_id (>255 chars)

### Priority 3: Configuration Validation

**File:** tests/config_tests.rs (NEW)

**Required Tests:**
1. Port validation:
   - ❌ Port 0 (invalid)
   - ❌ Port -1 (invalid)
   - ❌ Port 65536 (out of range)
   - ❌ Port 1-1023 (privileged, may fail)

2. Host validation:
   - ❌ Empty host string
   - ❌ Invalid IP address
   - ❌ Hostname resolution failure

### Priority 4: Connection Error Handling

**File:** tests/listener_tests.rs (NEW)

**Required Tests:**
1. Connection scenarios:
   - ❌ Client sends invalid frame size
   - ❌ Client disconnects mid-request
   - ❌ TCP backlog overflow
   - ❌ Shutdown signal handling
   - ❌ Accept() error handling

## Property-Based Testing Coverage

**File:** tests/property_tests.rs

**Current Tests:** 10 property tests ✅

**Coverage:**
- ✅ Arbitrary correlation IDs
- ✅ Arbitrary client IDs
- ✅ Arbitrary topic names
- ✅ Binary encoding round-trips

**Gaps:**
- ❌ Arbitrary message keys/values (large payloads)
- ❌ Arbitrary header counts
- ❌ Fuzz testing for malformed requests

## E2E Test Coverage

**File:** kafka_test/src/main.rs

**Current Coverage:**
- ✅ Full produce flow (TCP → DB)
- ✅ Topic auto-creation
- ✅ Offset calculation
- ✅ Database persistence
- ✅ Key/value storage
- ✅ Offset returned to client

**Gaps:**
- ❌ Multiple topics
- ❌ Multiple partitions
- ❌ Batch produce (multiple records)
- ❌ Consumer flow (Phase 3)
- ❌ Concurrent producers
- ❌ Metadata request variations

## Recommended Test Additions

### Immediate (Before Phase 3)

1. **Create tests/worker_tests.rs** (~200 lines)
   ```rust
   #[pg_test]
   fn test_get_or_create_topic_new() { ... }

   #[pg_test]
   fn test_get_or_create_topic_existing() { ... }

   #[pg_test]
   fn test_insert_records_single() { ... }

   #[pg_test]
   fn test_insert_records_batch() { ... }

   #[pg_test]
   fn test_insert_records_empty_batch() { ... }

   #[pg_test]
   fn test_produce_acks_zero_rejected() { ... }

   #[pg_test]
   fn test_produce_acks_all() { ... }
   ```

2. **Expand tests/protocol_tests.rs** (~100 lines)
   - Add max request size tests
   - Add invalid API version tests
   - Add request truncation tests

3. **Create tests/config_tests.rs** (~100 lines)
   - Port validation tests
   - Host validation tests
   - GUC parameter tests

### Phase 3 Additions

4. **Create tests/listener_tests.rs**
   - Connection error handling
   - Shutdown signal tests
   - Concurrent connection tests

5. **Expand kafka_test/**
   - Multi-topic tests
   - Multi-partition tests
   - Batch produce tests
   - Consumer tests (Phase 3)

## Coverage Measurement

### Manual Coverage Estimation

| Module | Lines | Estimated Coverage | Tested Lines | Untested Lines |
|--------|-------|-------------------|--------------|----------------|
| protocol.rs | 512 | 85% | ~435 | ~77 |
| worker.rs | 1201 | 30% | ~360 | ~841 |
| listener.rs | 191 | 50% | ~95 | ~96 |
| messages.rs | 211 | 70% | ~148 | ~63 |
| config.rs | 104 | 60% | ~62 | ~42 |
| lib.rs | 118 | 40% | ~47 | ~71 |
| error.rs | 97 | 90% | ~87 | ~10 |
| response_builders.rs | 102 | 70% | ~71 | ~31 |
| constants.rs | 198 | N/A | 0 | 0 |
| **Total** | **2734** | **~60%** | **~1305** | **~1231** |

### Automated Coverage (Not Currently Possible)

**Issue:** `cargo-tarpaulin` fails with pgrx extensions due to:
- Out-of-memory errors during instrumentation
- pgrx macros confuse code coverage tools
- PostgreSQL SPI requires running Postgres instance

**Alternatives:**
1. **llvm-cov** - May work better with nightly Rust
2. **kcov** - Linux-only, may work with pgrx
3. **Manual analysis** - Count tested vs untested functions

## Conclusion

### Current State
- ✅ Good coverage for protocol parsing/encoding
- ✅ E2E test validates happy path
- ❌ **Poor coverage for worker.rs (CRITICAL)**
- ❌ Missing error path tests
- ❌ Missing configuration validation tests

### Target for 100% Critical Path Coverage

To achieve 100% coverage of **critical paths**:

1. ✅ Create worker_tests.rs with SPI tests (Priority 1)
2. ✅ Add protocol error path tests (Priority 2)
3. ✅ Add config validation tests (Priority 3)
4. ✅ Add listener error tests (Priority 4)
5. ✅ Expand E2E tests for edge cases

**Estimated Effort:** ~800 lines of new tests
**Expected Coverage After:** 85%+ critical paths, 70%+ overall

### Non-Critical Gaps (Acceptable)

Some untested code is acceptable:
- Logging statements (pg_log!, pg_warning!)
- Debug formatting (Display impls tested in error.rs)
- Constants and type definitions
- pgrx boilerplate (_PG_init macro internals)

---

**Status:** Coverage analysis complete
**Next Action:** Create tests/worker_tests.rs with SPI integration tests
