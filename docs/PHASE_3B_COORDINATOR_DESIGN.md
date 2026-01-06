# Phase 3B: Consumer Group Coordinator Protocol Design

**Date**: 2026-01-06
**Status**: ✅ Complete
**Goal**: Implement full Kafka consumer group coordinator protocol to enable consumer E2E tests
**Result**: All 5 coordinator APIs implemented, E2E tests passing

---

## Overview

Phase 3B adds the missing consumer group coordinator protocol that enables Kafka consumers to:
1. Discover the coordinator for their consumer group
2. Join a consumer group
3. Receive partition assignments
4. Send periodic heartbeats to maintain membership
5. Leave the consumer group gracefully

This is the final piece needed to enable the disabled consumer E2E tests in [kafka_test/src/main.rs](../kafka_test/src/main.rs).

## Current State Analysis

### What Works (Phase 3A Complete)
- ✅ **Fetch API**: Consumers can read messages from specific partitions
- ✅ **OffsetCommit API**: Consumers can commit their offsets
- ✅ **OffsetFetch API**: Consumers can retrieve committed offsets
- ✅ **Storage Layer**: PostgresStore has methods for offset management
- ✅ **Repository Pattern**: Clean separation between storage and handlers

### What's Implemented (Phase 3B Complete)
- ✅ **FindCoordinator API** (key 10): Returns coordinator for a consumer group
- ✅ **JoinGroup API** (key 11): Consumer joins a group and gets member ID
- ✅ **SyncGroup API** (key 14): Leader receives assignments, followers wait
- ✅ **Heartbeat API** (key 12): Periodic keep-alive from consumers
- ✅ **LeaveGroup API** (key 13): Graceful consumer departure
- ✅ **Consumer Group State**: In-memory tracking with Arc<RwLock<HashMap>>
- ⚠️ **Partition Assignment**: Manual assignment only (automatic strategies planned for Phase 4)

### Why Consumer Tests Originally Failed (Resolved)

Before Phase 3B, the consumer tests in [kafka_test/src/main.rs](../kafka_test/src/main.rs) failed with:
```
Error: BrokerTransportFailure
Error: AllBrokersDown
```

This happened because rdkafka consumer clients perform this sequence:
1. Send **FindCoordinator** request → Got rejected (unsupported API)
2. Client treated this as broker failure
3. All subsequent operations failed

**Resolution**: Implemented all 5 coordinator APIs. Consumer tests now pass ✅

---

## Kafka Consumer Group Protocol Specification

### Protocol Flow

```
Consumer Startup:
┌──────────────┐
│  Consumer A  │
└──────┬───────┘
       │
       ├─1─→ FindCoordinator(group_id="my-group")
       │     ← CoordinatorResponse(node_id=1, host="localhost", port=9092)
       │
       ├─2─→ JoinGroup(group_id="my-group", member_id="", ...)
       │     ← JoinGroupResponse(member_id="consumer-A-uuid", leader=true, members=[...])
       │
       ├─3─→ SyncGroup(group_id="my-group", member_id="consumer-A-uuid", assignments=[...])
       │     ← SyncGroupResponse(assignment=[partition 0, 1, 2])
       │
       ├─4─→ Heartbeat(group_id="my-group", member_id="consumer-A-uuid", generation_id=1)
       │     ← HeartbeatResponse(error_code=0)
       │     (repeats every heartbeat.interval.ms = 3000ms)
       │
       └─5─→ LeaveGroup(group_id="my-group", member_id="consumer-A-uuid")
             ← LeaveGroupResponse(error_code=0)
```

### API Details

#### 1. FindCoordinator (API Key 10)

**Purpose**: Client asks "who is the coordinator for consumer group X?"

**Request**:
```rust
struct FindCoordinatorRequest {
    key: String,               // Consumer group ID
    key_type: i8,              // 0 = consumer group, 1 = transaction
    coordinator_keys: Vec<String>, // v4+ batch API (we'll use v0-v3)
}
```

**Response**:
```rust
struct FindCoordinatorResponse {
    error_code: i16,
    error_message: Option<String>,
    node_id: i32,              // Coordinator broker ID
    host: String,              // Coordinator host
    port: i32,                 // Coordinator port
}
```

**Implementation**: Always return our single broker (node_id=1, localhost:9092).

#### 2. JoinGroup (API Key 11)

**Purpose**: Consumer joins a group and participates in rebalance.

**Request**:
```rust
struct JoinGroupRequest {
    group_id: String,
    session_timeout_ms: i32,    // How long before member is considered dead
    rebalance_timeout_ms: i32,  // v1+: How long to wait for rebalance
    member_id: String,          // "" for first join, uuid on rejoin
    group_instance_id: Option<String>, // v5+: Static membership
    protocol_type: String,      // "consumer" for consumers
    protocols: Vec<JoinGroupProtocol>, // Supported assignment strategies
}

struct JoinGroupProtocol {
    name: String,               // "range", "roundrobin", "sticky"
    metadata: Bytes,            // Protocol-specific subscription info
}
```

**Response**:
```rust
struct JoinGroupResponse {
    error_code: i16,
    generation_id: i32,         // Increments on each rebalance
    protocol_name: Option<String>, // Chosen assignment strategy
    leader: String,             // Member ID of group leader
    member_id: String,          // This consumer's assigned member ID
    members: Vec<JoinGroupMember>, // Only sent to leader
}

struct JoinGroupMember {
    member_id: String,
    group_instance_id: Option<String>,
    metadata: Bytes,            // Subscription info from JoinGroupRequest
}
```

**Implementation**:
- First consumer to join becomes the leader
- Generate member_id using UUID
- Increment generation_id on each rebalance
- Store members in in-memory state (HashMap)

#### 3. SyncGroup (API Key 14)

**Purpose**: Leader distributes partition assignments, followers wait to receive theirs.

**Request**:
```rust
struct SyncGroupRequest {
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    protocol_type: Option<String>, // v5+
    protocol_name: Option<String>, // v5+
    assignments: Vec<SyncGroupAssignment>, // Only leader sends this
}

struct SyncGroupAssignment {
    member_id: String,
    assignment: Bytes,          // MemberAssignment protobuf
}
```

**Response**:
```rust
struct SyncGroupResponse {
    error_code: i16,
    protocol_type: Option<String>, // v5+
    protocol_name: Option<String>, // v5+
    assignment: Bytes,          // MemberAssignment for this consumer
}
```

**Implementation**:
- Leader sends assignments for all members
- Followers send empty assignments array
- Store assignments in in-memory state
- Return appropriate assignment to each member

#### 4. Heartbeat (API Key 12)

**Purpose**: Consumer sends periodic keep-alive to maintain group membership.

**Request**:
```rust
struct HeartbeatRequest {
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>, // v3+
}
```

**Response**:
```rust
struct HeartbeatResponse {
    error_code: i16,
}
```

**Error Codes**:
- `0 (NONE)`: Heartbeat accepted
- `25 (UNKNOWN_MEMBER_ID)`: Member not in group (must rejoin)
- `27 (REBALANCE_IN_PROGRESS)`: Rebalance happening (rejoin)
- `22 (ILLEGAL_GENERATION)`: Generation mismatch (rejoin)

**Implementation**:
- Update last_heartbeat timestamp for member
- Return UNKNOWN_MEMBER_ID if member not found
- Return REBALANCE_IN_PROGRESS if we trigger rebalance (future)

#### 5. LeaveGroup (API Key 13)

**Purpose**: Consumer gracefully leaves the consumer group.

**Request**:
```rust
struct LeaveGroupRequest {
    group_id: String,
    member_id: String,          // v0-v2
    members: Vec<MemberIdentity>, // v3+: Batch leave
}
```

**Response**:
```rust
struct LeaveGroupResponse {
    error_code: i16,
    members: Vec<MemberResponse>, // v3+
}
```

**Implementation**:
- Remove member from group state
- Trigger rebalance for remaining members (future optimization)
- For now: Just remove member, next heartbeat will cause others to discover

---

## Architecture Design

### 1. Storage Layer Extension

Extend `KafkaStore` trait in [src/kafka/storage/mod.rs](../src/kafka/storage/mod.rs):

```rust
pub trait KafkaStore {
    // ... existing methods ...

    // Consumer group coordinator methods
    fn get_consumer_groups(&self) -> Result<Vec<String>>;
    fn create_consumer_group(&self, group_id: &str) -> Result<()>;
    fn delete_consumer_group(&self, group_id: &str) -> Result<()>;
}
```

**Note**: Consumer group **membership state** will be in-memory only (not persisted). This matches Kafka's design where coordinator state is ephemeral. Only committed offsets are persisted (already implemented).

### 2. In-Memory State Management

Create [src/kafka/coordinator.rs](../src/kafka/coordinator.rs):

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Consumer group member information
#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>, // (name, metadata)
    pub assignment: Option<Vec<u8>>,       // Current partition assignment
    pub last_heartbeat: Instant,
}

/// Consumer group state
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub generation_id: i32,
    pub protocol_name: Option<String>,    // Chosen assignment strategy
    pub leader: Option<String>,           // Leader member_id
    pub members: HashMap<String, GroupMember>,
    pub state: GroupState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupState {
    Empty,              // No members
    PreparingRebalance, // Waiting for all members to rejoin
    CompletingRebalance, // Waiting for leader to send assignments
    Stable,             // All members have assignments
    Dead,               // Group is being deleted
}

/// Thread-safe consumer group coordinator
pub struct GroupCoordinator {
    groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn join_group(&self, request: JoinGroupRequest) -> Result<JoinGroupResponse>;
    pub fn sync_group(&self, request: SyncGroupRequest) -> Result<SyncGroupResponse>;
    pub fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse>;
    pub fn leave_group(&self, request: LeaveGroupRequest) -> Result<LeaveGroupResponse>;
}
```

**Storage Strategy**:
- ✅ In-memory: Group membership, assignments, generation IDs (ephemeral)
- ✅ PostgreSQL: Committed offsets (persistent) - already implemented in Phase 3A

This matches Kafka's design where coordinator state is rebuilt from member JoinGroup requests after coordinator restart.

### 3. Protocol Layer

Add to [src/kafka/constants.rs](../src/kafka/constants.rs):

```rust
// Consumer group coordinator API keys
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
pub const API_KEY_JOIN_GROUP: i16 = 11;
pub const API_KEY_HEARTBEAT: i16 = 12;
pub const API_KEY_LEAVE_GROUP: i16 = 13;
pub const API_KEY_SYNC_GROUP: i16 = 14;

// Coordinator error codes
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
pub const ERROR_NOT_COORDINATOR: i16 = 16;
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
```

Add to [src/kafka/messages.rs](../src/kafka/messages.rs):

```rust
pub enum KafkaRequest {
    // ... existing variants ...

    FindCoordinator {
        correlation_id: i32,
        client_id: Option<String>,
        api_version: i16,
        key: String,              // Group ID
        key_type: i8,             // 0 = consumer group
        response_tx: UnboundedSender<KafkaResponse>,
    },

    JoinGroup {
        correlation_id: i32,
        client_id: Option<String>,
        api_version: i16,
        group_id: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: String,
        protocol_type: String,
        protocols: Vec<JoinGroupProtocol>,
        response_tx: UnboundedSender<KafkaResponse>,
    },

    SyncGroup { /* ... */ },
    Heartbeat { /* ... */ },
    LeaveGroup { /* ... */ },
}

pub enum KafkaResponse {
    // ... existing variants ...

    FindCoordinator { /* ... */ },
    JoinGroup { /* ... */ },
    SyncGroup { /* ... */ },
    Heartbeat { /* ... */ },
    LeaveGroup { /* ... */ },
}
```

### 4. Handler Layer

Add to [src/kafka/handlers.rs](../src/kafka/handlers.rs):

```rust
pub fn handle_find_coordinator(
    broker_host: String,
    broker_port: i32,
    key: String,
    key_type: i8,
) -> Result<FindCoordinatorResponse> {
    // Always return ourselves as coordinator (single-node setup)
}

pub fn handle_join_group(
    coordinator: &GroupCoordinator,
    request: JoinGroupRequest,
) -> Result<JoinGroupResponse> {
    coordinator.join_group(request)
}

pub fn handle_sync_group(
    coordinator: &GroupCoordinator,
    request: SyncGroupRequest,
) -> Result<SyncGroupResponse> {
    coordinator.sync_group(request)
}

pub fn handle_heartbeat(
    coordinator: &GroupCoordinator,
    request: HeartbeatRequest,
) -> Result<HeartbeatResponse> {
    coordinator.heartbeat(request)
}

pub fn handle_leave_group(
    coordinator: &GroupCoordinator,
    request: LeaveGroupRequest,
) -> Result<LeaveGroupResponse> {
    coordinator.leave_group(request)
}
```

### 5. Worker Integration

Modify [src/worker.rs](../src/worker.rs):

```rust
pub fn kafka_worker() {
    // ... existing setup ...

    // Create group coordinator (shared across all requests)
    let coordinator = Arc::new(GroupCoordinator::new());

    loop {
        match request {
            KafkaRequest::FindCoordinator { key, key_type, .. } => {
                let response = handlers::handle_find_coordinator(
                    broker_host.clone(),
                    broker_port,
                    key,
                    key_type,
                );
                // Send response
            }

            KafkaRequest::JoinGroup { .. } => {
                let response = handlers::handle_join_group(&coordinator, ...);
                // Send response
            }

            // ... other coordinator handlers ...
        }
    }
}
```

---

## Implementation Plan

### Step 1: Add Constants and Error Codes ✅
**Files**: [src/kafka/constants.rs](../src/kafka/constants.rs)
- Add 5 coordinator API keys
- Add coordinator-specific error codes

### Step 2: Extend Storage Trait (Minimal) ✅
**Files**: [src/kafka/storage/mod.rs](../src/kafka/storage/mod.rs), [src/kafka/storage/postgres.rs](../src/kafka/storage/postgres.rs)
- Add methods for persistent group metadata (if needed)
- Most state will be in-memory, so this may be minimal

### Step 3: Create Coordinator State Management ✅
**Files**: [src/kafka/coordinator.rs](../src/kafka/coordinator.rs) (new)
- Implement `GroupCoordinator` with RwLock-protected HashMap
- Implement state machine: Empty → PreparingRebalance → CompletingRebalance → Stable
- Implement member timeout checking (optional for v1)

### Step 4: Add Protocol Types ✅
**Files**: [src/kafka/messages.rs](../src/kafka/messages.rs)
- Add 5 new KafkaRequest variants
- Add 5 new KafkaResponse variants
- Define supporting structs (JoinGroupProtocol, etc.)

### Step 5: Implement Protocol Parsing ✅
**Files**: [src/kafka/protocol.rs](../src/kafka/protocol.rs)
- Add parsing for 5 coordinator requests using kafka-protocol crate
- Add encoding for 5 coordinator responses

### Step 6: Implement Handlers ✅
**Files**: [src/kafka/handlers.rs](../src/kafka/handlers.rs)
- `handle_find_coordinator()`: Return single broker
- `handle_join_group()`: Delegate to GroupCoordinator
- `handle_sync_group()`: Delegate to GroupCoordinator
- `handle_heartbeat()`: Delegate to GroupCoordinator
- `handle_leave_group()`: Delegate to GroupCoordinator

### Step 7: Wire Up Worker ✅
**Files**: [src/worker.rs](../src/worker.rs)
- Create GroupCoordinator instance
- Add match arms for 5 coordinator request types
- Pass coordinator reference to handlers

### Step 8: Update ApiVersions Response ✅
**Files**: [src/kafka/response_builders.rs](../src/kafka/response_builders.rs)
- Add FindCoordinator (v0-v3)
- Add JoinGroup (v0-v7)
- Add SyncGroup (v0-v4)
- Add Heartbeat (v0-v4)
- Add LeaveGroup (v0-v4)

### Step 9: Enable E2E Consumer Tests ✅
**Files**: [kafka_test/src/main.rs](../kafka_test/src/main.rs)
- Uncomment consumer test functions
- Run and verify all tests pass

---

## Testing Strategy

### Unit Tests
- GroupCoordinator state transitions
- Member ID generation
- Generation ID increments
- Timeout handling

### Integration Tests (pgrx)
- FindCoordinator returns correct broker
- JoinGroup assigns member IDs
- SyncGroup distributes assignments
- Heartbeat updates timestamps
- LeaveGroup removes members

### E2E Tests (kafka_test)
- Single consumer reads messages
- Multiple consumers with partition assignment
- Consumer offset commit/fetch
- Consumer group rebalance (future)

---

## Limitations and Future Work

### Phase 3B Scope (Minimal Viable Coordinator)
- ✅ Single consumer group support
- ✅ Static partition assignment (no rebalance on member changes)
- ✅ In-memory state only (lost on restart)
- ✅ No automatic rebalancing
- ✅ No partition assignment strategies (leader must provide assignments)

### Future Enhancements (Phase 4+)
- ⏳ Automatic partition assignment strategies (range, roundrobin, sticky)
- ⏳ Automatic rebalancing on member join/leave
- ⏳ Member timeout detection and removal
- ⏳ Persistent group state (survive coordinator restart)
- ⏳ Multiple consumer groups
- ⏳ Static group membership (KIP-345)
- ⏳ Incremental cooperative rebalancing (KIP-429)

---

## Success Criteria

Phase 3B is complete when:

1. ✅ All 5 coordinator APIs implemented and tested
2. ✅ GroupCoordinator manages member state correctly
3. ✅ E2E consumer tests pass:
   - `test_consumer_basic()` ✅
   - `test_consumer_multiple_messages()` ✅
   - `test_consumer_from_offset()` ✅
   - `test_offset_commit_fetch()` ✅
4. ✅ rdkafka consumer client successfully:
   - Finds coordinator
   - Joins group and receives member ID
   - Syncs and receives partition assignment
   - Sends heartbeats without errors
   - Fetches messages from assigned partitions
   - Commits offsets successfully
   - Leaves group gracefully

---

## References

- [Kafka Protocol Specification](https://kafka.apache.org/protocol.html)
- [KIP-62: Consumer Group Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Consumer+Group+Protocol)
- [kafka-protocol Rust Crate](https://docs.rs/kafka-protocol/latest/kafka_protocol/)
- [rdkafka Consumer Documentation](https://docs.rs/rdkafka/latest/rdkafka/consumer/index.html)
