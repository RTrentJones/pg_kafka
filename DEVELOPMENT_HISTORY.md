# Development History

This document summarizes the development journey of pg_kafka from initial concept to Phase 3B completion.

**Last Updated:** 2026-01-03
**Current Status:** Phase 3B Complete - Full Producer/Consumer Support

---

## Phase Timeline

### Phase 0: Project Scaffold (Dec 2025)
- Docker development environment setup
- pgrx initialization
- Basic project structure
- Initial design documents

### Phase 1: Metadata Support (Dec 2025)
✅ **Complete**
- Background worker with TCP listener on port 9092
- ApiVersions request/response (API key 18)
- Metadata request/response (API key 3)
- Async/sync architecture (tokio ↔ Postgres SPI)
- GUC configuration parameters

**Reference:** `.documents/STEP4_SUMMARY.md`

### Phase 1.5: Unit Test Coverage (Dec 31, 2025)
✅ **Complete**
- Protocol parsing tests
- Encoding/decoding tests
- Property-based testing
- Mock infrastructure
- Total: 34 unit tests

**Challenge:** Discovered PGC_POSTMASTER limitation preventing use of `#[pg_test]` for SPI integration tests

**References:**
- `.documents/PHASE_1.5_PLAN.md`
- `.documents/PHASE_1.5_COMPLETE.md`
- `.documents/TESTING_BEST_PRACTICES.md`

### Phase 2: Producer Support (Dec 31 - Jan 1, 2026)
✅ **Complete**
- ProduceRequest/Response handling (API key 0)
- Dual-offset design (partition_offset + global_offset)
- SPI integration for database writes
- Topic auto-creation
- E2E test with rdkafka client
- Automated database verification
- CI/CD pipeline with GitHub Actions

**Key Achievements:**
- Clean separation of protocol and storage logic
- Working Kafka producer compatibility
- Real client testing with rdkafka

**References:**
- `.documents/PHASE_2_PROGRESS.md`
- `.documents/PHASE2_ANALYSIS.md`
- `.documents/PHASE2_SUMMARY.md`

**Refactoring:**
- Code audit and cleanup
- Response builders extraction
- Improved error handling

**References:**
- `.documents/CODE_AUDIT.md`
- `.documents/REFACTORING_PLAN.md`
- `.documents/REFACTORING_SUMMARY.md`
- `.documents/POST_REFACTORING_AUDIT.md`

### Phase 3: Consumer Support (Jan 2, 2026)
✅ **Complete**
- FetchRequest/Response handling (API key 1)
- ListOffsets support for earliest/latest (API key 2)
- OffsetCommit/OffsetFetch for consumer groups (API keys 8, 9)
- RecordBatch v2 encoding/decoding
- Repository Pattern storage abstraction
- Empty fetch response handling

**Key Achievements:**
- KafkaStore trait with PostgresStore implementation
- Clean separation between handlers and storage
- Consumer offset tracking in PostgreSQL

**References:**
- `.documents/PHASE_3_PLAN.md`
- `.documents/PHASE_3_FULL_KAFKA_PLAN.md`
- `.documents/PHASE_3_PROGRESS.md`
- `docs/REPOSITORY_PATTERN.md`

### Phase 3B: Consumer Group Coordinator (Jan 2-3, 2026)
✅ **Complete**
- FindCoordinator support (API key 10)
- JoinGroup for member registration (API key 11)
- Heartbeat for membership maintenance (API key 12)
- LeaveGroup for graceful departure (API key 13)
- SyncGroup for partition assignment (API key 14)
- Thread-safe in-memory coordinator state (Arc<RwLock>)
- consumer_offsets table
- 5 comprehensive E2E test scenarios

**Key Achievements:**
- Full consumer group protocol implementation
- All E2E tests passing
- Manual partition assignment working
- OffsetFetch v8 protocol issue resolved
- Empty Fetch response bug fixed

**References:**
- `docs/PHASE_3B_COORDINATOR_DESIGN.md`
- `PHASE_3B_COMPLETE.md`

**E2E Test Fixes (Jan 3, 2026):**
1. Fixed missing consumer_offsets table issue
2. Fixed OffsetFetch v8 protocol underflow (limited to v0-v7)
3. Fixed Fetch v12 "invalid MessageSetSize -1" error
4. Fixed test offset mismatch issues
5. Updated Test 5 to use manual partition assignment

---

## Architecture Evolution

### Initial Design
- Single background worker
- Direct SPI calls from async context (problematic)

### Current Design (Phase 3B)
- **Async World:** Tokio runtime handling TCP connections
- **Message Queue:** Crossbeam channels bridging async/sync
- **Sync World:** Background worker with SPI calls
- **Storage Abstraction:** Repository Pattern (KafkaStore trait)
- **Consumer Groups:** Thread-safe Arc<RwLock> coordinator state

### Database Schema Evolution
**Phase 1-2:**
```sql
kafka.messages (topic_id, partition_id, partition_offset, global_offset, key, value)
kafka.topics (id, name, partitions)
```

**Phase 3B:**
```sql
kafka.messages (same as above)
kafka.topics (same as above)
kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset, metadata)
```

---

## Testing Evolution

### Phase 1
- Manual testing with kcat
- Basic protocol parsing tests

### Phase 1.5
- 34 unit tests
- Property-based testing
- Mock infrastructure

### Phase 2
- 1 E2E test scenario (producer)
- Automated database verification

### Phase 3B
- 5 E2E test scenarios:
  1. Producer functionality
  2. Basic consumer functionality
  3. Consumer multiple messages
  4. Consumer from specific offset
  5. OffsetCommit/OffsetFetch
- All tests passing ✅
- Coverage: ~50-55%

---

## Key Technical Challenges Solved

1. **PGC_POSTMASTER Limitation**
   - Cannot use `#[pg_test]` for SPI tests
   - Solution: Comprehensive E2E tests with real client

2. **Async/Sync Bridge**
   - Postgres SPI not thread-safe
   - Solution: Message queue architecture

3. **Offset Management**
   - Dual-offset design for Kafka compatibility
   - Repository Pattern for clean abstraction

4. **Protocol Version Compatibility**
   - OffsetFetch v8 different response format
   - Solution: Limit to v0-v7, document deviation

5. **Empty Fetch Responses**
   - Flexible format (v12) doesn't accept None for records
   - Solution: Return empty Bytes instead of None

6. **Consumer Group State**
   - Thread-safe state across async tasks
   - Solution: Arc<RwLock> with proper locking

---

## Documentation Created

### Main Documentation
- `README.md` - Project overview and quick start
- `CLAUDE.md` - Development guide for AI assistants
- `PROJECT.md` - Detailed design document

### Technical Documentation
- `docs/KAFKA_PROTOCOL_COVERAGE.md` - API coverage analysis (24%)
- `docs/PROTOCOL_DEVIATIONS.md` - Intentional deviations from Kafka spec
- `docs/PHASE_3B_COORDINATOR_DESIGN.md` - Coordinator implementation
- `docs/TEST_STRATEGY.md` - Testing approach and coverage
- `docs/REPOSITORY_PATTERN.md` - Storage abstraction design
- `docs/PERFORMANCE.md` - Performance tuning guide
- `docs/architecture/ADR-001-partitioning-and-retention.md` - Design decisions

---

## Lessons Learned

1. **pgrx Testing Limitations:** Background workers with PGC_POSTMASTER GUCs cannot use standard pgrx test framework
2. **Protocol Version Complexity:** Kafka protocol versions can have breaking changes (e.g., OffsetFetch v8)
3. **E2E Testing Value:** Real Kafka clients provide better validation than unit tests alone
4. **Repository Pattern Benefits:** Clean separation enables easier testing and future refactoring
5. **Empty Response Handling:** Protocol edge cases (empty fetches) need careful attention

---

## Statistics

**Phase 3B Final Metrics:**
- **APIs Implemented:** 12 of ~50 (24%)
- **Test Count:** 39 tests (34 unit + 5 E2E scenarios)
- **Test Status:** All passing ✅
- **Code Coverage:** ~50-55%
- **Lines of Code:** ~3500 (estimated)
- **Development Time:** ~1 month (Dec 2025 - Jan 2026)

**API Breakdown:**
- Core Metadata: 2 APIs (ApiVersions, Metadata)
- Producer: 1 API (Produce)
- Consumer Data Access: 4 APIs (Fetch, ListOffsets, OffsetCommit, OffsetFetch)
- Consumer Coordinator: 5 APIs (FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup)

---

## Next Phase Preview

**Phase 4: Automatic Partition Assignment (Planned)**
- Range assignment strategy
- RoundRobin assignment strategy
- Sticky assignment strategy (KIP-54)

**Phase 5: Automatic Rebalancing (Planned)**
- Trigger rebalance on member join/leave
- Member timeout detection
- Cooperative rebalancing (KIP-429)

**Phase 6: Shadow Mode (Planned)**
- Logical Decoding integration
- External Kafka production
- Zero-downtime migration support

---

**Status:** This is a living document tracking the evolution of pg_kafka. For current status, see `README.md`.
