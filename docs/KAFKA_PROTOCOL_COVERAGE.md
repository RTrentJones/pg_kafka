# Kafka Protocol Coverage Analysis

**Date**: 2026-10-29
**pg_kafka Version**: Phase 11 Complete (Shadow Mode)
**Analysis**: Comprehensive review of implemented vs standard Kafka protocol

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **API Coverage** | <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen--> |
| **Build Status** | ✅ Compiles with zero warnings |
| **Test Suite** | Unit + property + E2E; counts in [TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary) |
| **Architecture** | Repository Pattern with typed errors |
| **Client Compatibility** | ✅ kcat, rdkafka, and the Apache Kafka 3.7 Java clients (console/verifiable producer and consumer, consumer groups, transactions) verified |

---

## Implemented APIs (<!--gen:api-count-->28<!--/gen--> total)

Version ranges below are the ones advertised in the ApiVersions response and enforced on inbound requests (each API's `MIN_VERSION`/`MAX_VERSION` in `src/kafka/api/`). A request above its range is answered with UNSUPPORTED_VERSION in the requested version (echoing the requested topics/partitions/groups where the response has no top-level error code); a request below it closes the connection, as Kafka does (the minimums are the lowest versions the kafka-protocol crate can decode and encode). A body that does not decode gets INVALID_REQUEST; see PROTOCOL_DEVIATIONS.md, "Error responses". ApiVersions outside v0-v3 is answered in the v0 format with UNSUPPORTED_VERSION and the full key list (KIP-511).

### 1. Core Metadata (2 APIs) ✅ 100% Coverage
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| ApiVersions | 18 | <!--gen:api-vversions:ApiVersions-->v0-v3<!--/gen--> | ✅ Complete | Returns supported API versions |
| Metadata | 3 | <!--gen:api-vversions:Metadata-->v0-v10<!--/gen--> | ✅ Complete | Topic and broker discovery; v4+ `allow_auto_topic_creation=false` is honored; v10 `topic_id` is left as the zero UUID |

### 2. Producer (2 APIs) ✅ 100% Coverage
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| Produce | 0 | <!--gen:api-vversions:Produce-->v3-v9<!--/gen--> | ✅ Complete | RecordBatch v2 format with sequence validation |
| InitProducerId | 22 | <!--gen:api-vversions:InitProducerId-->v0-v4<!--/gen--> | ✅ Complete | Allocate producer ID for idempotency |

**Implementation Notes (Phase 9)**:
- ✅ Idempotent producer support with sequence validation
- ✅ Deduplication, Kafka's model: a retry of one of the producer's last 5 batches
  on the partition is acked with its original base offset and not re-written (as in Kafka,
  DUPLICATE_SEQUENCE_NUMBER is never returned); anything else out of sequence is
  OUT_OF_ORDER_SEQUENCE_NUMBER; sequences wrap at 2147483647
- ✅ Producer epoch fencing: a stale epoch on Produce is INVALID_PRODUCER_EPOCH (47, as
  Kafka 3.7); a client-bumped epoch (KIP-360) is accepted from sequence 0
- ✅ Full transaction support via Phase 10 APIs (see Transaction APIs section)

### 3. Consumer - Data Access (4 APIs) ✅ 100% Coverage
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| Fetch | 1 | <!--gen:api-vversions:Fetch-->v4-v11<!--/gen--> | ✅ Complete | Read messages with long polling support (capped at v11: v13+ drops topic names for topic IDs) |
| OffsetCommit | 8 | <!--gen:api-vversions:OffsetCommit-->v2-v8<!--/gen--> | ✅ Complete | Commit consumed offsets |
| OffsetFetch | 9 | <!--gen:api-vversions:OffsetFetch-->v1-v7<!--/gen--> | ✅ Complete | Retrieve committed offsets (v8+ not supported) |
| ListOffsets | 2 | <!--gen:api-vversions:ListOffsets-->v1-v7<!--/gen--> | ✅ Complete | Get earliest/latest/by-timestamp offsets |

**Implementation Notes:**
- ✅ ListOffsets supports special timestamps (-2 = earliest, -1 = latest: the LSO for read_committed, -3 = max timestamp, KIP-734) and arbitrary timestamp lookup (earliest offset whose record timestamp ≥ target), bounded by the readable end like Kafka; an unknown partition is UNKNOWN_TOPIC_OR_PARTITION
- ✅ OffsetFetch v7 `require_stable` (KIP-447): UNSTABLE_OFFSET_COMMIT for partitions with a pending transactional offset commit
- ✅ Fetch returns records grouped into one RecordBatch per run of a producer's consecutive offsets, and a control batch over offsets a consumer cannot see (aborted, cleaned up) so its position reaches the end (see PROTOCOL_DEVIATIONS.md §7)
- ✅ OffsetFetch limited to <!--gen:api-vversions:OffsetFetch-->v1-v7<!--/gen--> (v8+ requires different response format)
- ✅ All consumer data access APIs fully functional
- ✅ Long polling with max_wait_ms/min_bytes support

### 4. Consumer Group Coordinator (7 APIs) ✅ 100% Coverage
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| FindCoordinator | 10 | <!--gen:api-vversions:FindCoordinator-->v0-v3<!--/gen--> | ✅ Complete | Discover group coordinator |
| JoinGroup | 11 | <!--gen:api-vversions:JoinGroup-->v0-v7<!--/gen--> | ✅ Complete | Join group, get member ID |
| Heartbeat | 12 | <!--gen:api-vversions:Heartbeat-->v0-v4<!--/gen--> | ✅ Complete | Maintain membership, returns REBALANCE_IN_PROGRESS |
| LeaveGroup | 13 | <!--gen:api-vversions:LeaveGroup-->v0-v4<!--/gen--> | ✅ Complete | Graceful departure, triggers rebalance |
| SyncGroup | 14 | <!--gen:api-vversions:SyncGroup-->v0-v4<!--/gen--> | ✅ Complete | Partition assignment sync |
| DescribeGroups | 15 | <!--gen:api-vversions:DescribeGroups-->v0-v5<!--/gen--> | ✅ Complete | Get consumer group state and members |
| ListGroups | 16 | <!--gen:api-vversions:ListGroups-->v0-v4<!--/gen--> | ✅ Complete | List all consumer groups |

**Implementation Notes (Phase 5 Complete)**:
- In-memory coordinator state (ephemeral, rebuilt on restart)
- Thread-safe with Arc<RwLock>
- ✅ Automatic partition assignment (Range, RoundRobin, Sticky strategies)
- ✅ Automatic rebalancing on member leave
- ✅ Session timeout detection with background scanner
- ✅ REBALANCE_IN_PROGRESS (error 27) forces client rejoin

### 5. Admin APIs (4 APIs) ✅ 100% Coverage (Phase 6)
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| CreateTopics | 19 | <!--gen:api-vversions:CreateTopics-->v2-v5<!--/gen--> | ✅ Complete | Create topics with partition count |
| DeleteTopics | 20 | <!--gen:api-vversions:DeleteTopics-->v1-v4<!--/gen--> | ✅ Complete | Delete topics and messages |
| CreatePartitions | 37 | <!--gen:api-vversions:CreatePartitions-->v0-v2<!--/gen--> | ✅ Complete | Add partitions to existing topics |
| DeleteGroups | 42 | <!--gen:api-vversions:DeleteGroups-->v0-v2<!--/gen--> | ✅ Complete | Delete consumer groups |

**Implementation Notes (Phase 6):**
- ✅ CreateTopics respects the partition count parameter (`num_partitions = -1` uses `pg_kafka.default_partitions`; `0` or other values below 1 return INVALID_PARTITIONS); per-topic `configs`, replica assignments, and `replication_factor` are ignored (the response always reports replication factor 1)
- ✅ CreateTopics validates topic names (Kafka rules: length ≤ 249, `[a-zA-Z0-9._-]`, not `.`/`..`) and honors `validate_only`
- ✅ DeleteTopics cascades to messages and consumer offsets
- ✅ CreatePartitions adds partitions to existing topics (decreases rejected with INVALID_PARTITIONS)
- ✅ DeleteGroups validates group is empty before deletion (NON_EMPTY_GROUP)

### 6. Transaction APIs (4 APIs) ✅ 100% Coverage
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| AddPartitionsToTxn | 24 | <!--gen:api-vversions:AddPartitionsToTxn-->v0-v3<!--/gen--> | ✅ Complete | Register partitions in transaction |
| AddOffsetsToTxn | 25 | <!--gen:api-vversions:AddOffsetsToTxn-->v0-v3<!--/gen--> | ✅ Complete | Include consumer offsets in transaction |
| EndTxn | 26 | <!--gen:api-vversions:EndTxn-->v0-v3<!--/gen--> | ✅ Complete | Commit or abort transaction |
| TxnOffsetCommit | 28 | <!--gen:api-vversions:TxnOffsetCommit-->v0-v3<!--/gen--> | ✅ Complete | Commit offsets atomically within transaction |

**Implementation Notes (Phase 10):**
- ✅ Full exactly-once semantics (EOS)
- ✅ Read-committed isolation level for consumers
- ✅ Transaction state machine (Empty → Ongoing → CompleteCommit/CompleteAbort). EndTxn applies the outcome in a single PostgreSQL transaction, so the PrepareCommit/PrepareAbort states are defined but never persisted
- ✅ Transactional producer fencing via epoch
- ✅ Pending/aborted messages filtered server-side for read_committed consumers; a fetch over
  only hidden offsets returns a control batch so the consumer moves past them (see
  PROTOCOL_DEVIATIONS.md §7)
- ✅ Validation as Kafka: InitProducerId timeout bound (`pg_kafka.transaction_max_timeout_ms`,
  INVALID_TRANSACTION_TIMEOUT), AddPartitionsToTxn never creates topics
  (UNKNOWN_TOPIC_OR_PARTITION, others OPERATION_NOT_ATTEMPTED), a transactional Produce
  to a partition not added to the transaction is INVALID_TXN_STATE

### 7. Configuration & Log Management (3 APIs) ✅
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| DescribeConfigs | 32 | <!--gen:api-vversions:DescribeConfigs-->v1-v4<!--/gen--> | ✅ Complete | TOPIC resources only: `retention.ms` and `cleanup.policy=delete` |
| IncrementalAlterConfigs | 44 | <!--gen:api-vversions:IncrementalAlterConfigs-->v0-v1<!--/gen--> | ✅ Complete | SET/DELETE of `retention.ms` per topic only |
| DeleteRecords | 21 | <!--gen:api-vversions:DeleteRecords-->v0-v2<!--/gen--> | ✅ Complete | Truncate a partition below an offset |

**Implementation Notes:**
- ✅ DescribeConfigs reports `retention.ms` with its real value and source (DYNAMIC_TOPIC when a per-topic override is set, otherwise DEFAULT derived from `pg_kafka.message_retention_hours`, `-1` = infinite); non-topic resources get INVALID_REQUEST and unknown topics UNKNOWN_TOPIC_OR_PARTITION. Min version is v1 because the kafka-protocol crate implements v1-v4 only
- ✅ IncrementalAlterConfigs persists `retention.ms` to `kafka.topics.retention_ms`, which the retention sweep enforces; any other key or operation (APPEND/SUBTRACT) is rejected with INVALID_CONFIG, all-or-nothing per resource; `validate_only` runs the checks without applying
- ✅ DeleteRecords treats offset `-1` as "truncate to the high watermark", returns OFFSET_OUT_OF_RANGE for offsets past the high watermark, and persists the new log start offset (`kafka.partition_offsets.log_start_offset`) so ListOffsets(EARLIEST) and Fetch `log_start_offset` do not regress

### 8. Security APIs (2 APIs) ✅
| API | Key | Versions | Status | Notes |
|-----|-----|----------|--------|-------|
| SaslHandshake | 17 | <!--gen:api-vversions:SaslHandshake-->v0-v1<!--/gen--> | ✅ Complete | Mechanism negotiation; v0 (unframed tokens) advertised but answered UNSUPPORTED_VERSION |
| SaslAuthenticate | 36 | <!--gen:api-vversions:SaslAuthenticate-->v0-v2<!--/gen--> | ✅ Complete | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512; no re-authentication (`session_lifetime_ms` 0) |

**Implementation Notes:**
- ✅ Kafka-style listener security protocols (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL), several listeners at once, TLS via rustls with optional mutual TLS
- ✅ Credentials are SCRAM verifiers in `kafka.sasl_credentials` (never passwords), managed with SQL functions
- ✅ Handled on the network thread per connection; only the credential lookup reaches the DB thread
- ❌ No ACLs: an authenticated principal may do everything. See [AUTHENTICATION.md](AUTHENTICATION.md)

---

## Unimplemented APIs Analysis

**Source**: [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol.html)
**Total Standard Kafka APIs**: ~50
**Implemented**: <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen-->
**Unimplemented**: ~22 (44%)

This section analyzes all unimplemented APIs, their use cases, and priority for drop-in Kafka replacement.

---

### Security & Authentication (5 remaining APIs) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **CreateAcls** | 30 | Creates access control lists (e.g., "user X can produce to topic Y") | Low - Use PostgreSQL GRANT/REVOKE |
| **DescribeAcls** | 29 | Lists existing ACL rules for auditing/debugging permissions | Low - Admin utility |
| **DeleteAcls** | 31 | Removes ACL rules | Low - Admin utility |
| **DescribeUserScramCredentials** | 50 | Lists SCRAM credentials for users (SCRAM-SHA-256/512 auth) | Low - Only for SCRAM auth |
| **AlterUserScramCredentials** | 51 | Creates/updates/deletes SCRAM credentials | Low - Only for SCRAM auth |

**pg_kafka Approach**: SaslHandshake/SaslAuthenticate are implemented (§8 above); SCRAM users are managed with SQL functions instead of the SCRAM credential APIs, and there are no ACLs yet (an authenticated principal may do everything).

**Client Impact**: `kafka-configs --alter --entity-type users` and ACL tooling do not work; SASL clients connect normally.

---

### Transactions (3 remaining APIs) - Priority: Low

**Note:** 4 of 7 transaction APIs are implemented (Phase 10). See "Implemented APIs" section above.

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **WriteTxnMarkers** | 27 | Internal: coordinator writes commit/abort markers to partitions | Low - Broker-to-broker internal |
| **DescribeTransactions** | 65 | Lists active transactions for monitoring/debugging | Low - Admin utility |
| **ListTransactions** | 66 | Finds transactions by producer ID or state | Low - Admin utility |

**pg_kafka Approach**: Core transaction APIs (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit) are implemented. The remaining 3 APIs are internal broker communication or admin utilities.

**Client Impact**: None. Transactional producers work fully with the implemented APIs.

---

### Delegation Tokens (4 APIs) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **CreateDelegationToken** | 38 | Creates token for lightweight authentication (avoiding full Kerberos) | Low - Only for Kerberos environments |
| **RenewDelegationToken** | 39 | Extends token expiry | Low - Token lifecycle management |
| **ExpireDelegationToken** | 40 | Invalidates a token immediately | Low - Token lifecycle management |
| **DescribeDelegationToken** | 41 | Lists tokens and their metadata | Low - Admin utility |

**Use Case**: Short-lived tokens for distributed workers (Spark, Flink jobs) that need Kafka access without distributing long-term credentials.

**pg_kafka Approach**: Skip entirely. Only relevant for Kerberos-secured clusters.

**Client Impact**: None. Clients never request this unless explicitly configured for delegation tokens.

---

### Configuration Management (2 remaining APIs) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| ✅ **DescribeConfigs** | 32 | **IMPLEMENTED** — reports the configs pg_kafka honors (retention.ms with real source/value, cleanup.policy) for TOPIC resources | Done |
| ✅ **IncrementalAlterConfigs** | 44 | **IMPLEMENTED** — SET/DELETE of `retention.ms` per topic (persisted to `kafka.topics.retention_ms`, enforced by the retention sweep); unsupported keys rejected with INVALID_CONFIG | Done |
| **AlterConfigs** | 33 | Modifies configs (deprecated, replaced by IncrementalAlterConfigs) | Low - Deprecated API |
| **ListConfigResources** | 74 | Lists configurable resources | Low - Admin utility |

**Use Case**: Dynamic configuration changes without broker restart. Admin tools like `kafka-configs.sh` use these.

**pg_kafka Approach**: Honest-minimal — describe/alter only the configs the engine actually enforces, reject the rest rather than silently accepting.

**Client Impact**: `kafka-configs.sh --describe/--alter` for topic `retention.ms` works; other keys are explicitly rejected.

---

### Cluster Operations (8 APIs) - Priority: None (Single-Node)

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **ElectLeaders** | 43 | Triggers leader election for partitions (preferred or unclean) | None - No replicas |
| **AlterPartitionReassignments** | 45 | Moves partition replicas between brokers (rebalancing) | None - No multi-broker |
| **ListPartitionReassignments** | 46 | Shows in-progress partition moves | None - No multi-broker |
| **AlterReplicaLogDirs** | 34 | Moves partition data between disks on the same broker | None - No disk management |
| **DescribeLogDirs** | 35 | Shows disk usage per partition per broker | None - Use PostgreSQL stats |
| **UnregisterBroker** | 64 | Removes a broker from cluster metadata (KRaft mode) | None - No KRaft |
| **DescribeCluster** | 60 | Returns cluster ID, controller, and broker list | Low - Easy to stub |
| **DescribeQuorum** | 55 | Shows KRaft quorum state (voter lag, leader info) | None - No KRaft |

**Use Case**: Cluster administration, capacity planning, broker decommissioning, replica management.

**pg_kafka Approach**: Single-node design. No replicas, no multi-broker coordination needed.

**Client Impact**: None. Clients never call these - they're broker-to-broker or admin-only tools.

---

### Log Management (1 remaining API) - Priority: None

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| ✅ **DeleteRecords** | 21 | **IMPLEMENTED** — deletes records below a per-partition offset (`-1` = truncate to high watermark), reports the new low watermark; GDPR/compliance truncation | Done |
| **OffsetForLeaderEpoch** | 23 | Resolves offset divergence after leader failover (truncation detection) | None - No replica failover |

**Use Case**:
- DeleteRecords: Compliance (GDPR deletion), manual log cleanup
- OffsetForLeaderEpoch: Prevents consumers from reading divergent data after unclean leader election

**pg_kafka Approach**:
- DeleteRecords: `DELETE FROM kafka.messages WHERE partition_offset < $1` behind the wire API, with OFFSET_OUT_OF_RANGE validation; the advanced log start offset is persisted in `kafka.partition_offsets.log_start_offset`
- OffsetForLeaderEpoch: Skip (only needed for replica divergence detection)

**Client Impact**: DeleteRecords works via the wire protocol. OffsetForLeaderEpoch is automatic (clients don't manually call).

---

### Consumer Group Management (3 APIs) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **OffsetDelete** | 47 | Deletes committed offsets for a group (cleanup before group deletion) | Low - Admin utility |
| **ConsumerGroupHeartbeat** | 68 | New consumer protocol (KIP-848) - replaces JoinGroup/SyncGroup/Heartbeat | None - Kafka 3.7+ only (GA in 4.0) |
| **ConsumerGroupDescribe** | 69 | New consumer protocol - describes group using new model | None - Kafka 3.7+ only (GA in 4.0) |

**Use Case**:
- OffsetDelete: Cleanup orphaned offset data
- ConsumerGroup* APIs: Next-generation consumer protocol that simplifies rebalancing

**pg_kafka Approach**: OffsetDelete could be implemented with `DELETE FROM kafka.consumer_offsets`. New consumer protocol is cutting-edge (low adoption).

**Client Impact**: None. OffsetDelete is admin-only. New consumer protocol requires client opt-in.

---

### Quotas (2 APIs) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **DescribeClientQuotas** | 48 | Lists quota limits (bytes/sec, request rate) per client/user | Low - Multi-tenant only |
| **AlterClientQuotas** | 49 | Sets or removes quota limits | Low - Multi-tenant only |

**Use Case**: Multi-tenant clusters where you need to prevent one client from overwhelming the cluster.

**pg_kafka Approach**: Use PostgreSQL connection limits and resource governor instead.

**Client Impact**: None. Clients don't call these - admin tools only.

---

### Feature Flags & Metadata (2 APIs) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **UpdateFeatures** | 57 | Enables/disables cluster features (version upgrades) | Low - Kafka version management |
| **DescribeProducers** | 61 | Lists active producers on a partition (for transaction recovery) | Low - Transaction debugging |

**Use Case**: Cluster-wide feature flag management during rolling upgrades.

**pg_kafka Approach**: Not applicable (single-version deployment).

**Client Impact**: None. Admin tools only.

---

### KRaft Consensus (3 APIs) - Priority: None

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **AddRaftVoter** | 80 | Adds a new voter to KRaft quorum | None - No KRaft |
| **RemoveRaftVoter** | 81 | Removes a voter from quorum | None - No KRaft |
| **DescribeQuorum** | 55 | Shows quorum health and voter status | None - No KRaft |

**Use Case**: Managing the KRaft consensus cluster that replaced ZooKeeper in Kafka 2.8+.

**pg_kafka Approach**: Not applicable (no distributed consensus needed).

**Client Impact**: None. Internal Kafka broker APIs only.

---

### Telemetry (2 APIs) - Priority: None

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **GetTelemetrySubscriptions** | 71 | Client asks what metrics the broker wants | None - Optional metrics |
| **PushTelemetry** | 72 | Client pushes metrics to broker (KIP-714) | None - Optional metrics |

**Use Case**: Centralized client-side metrics collection (KIP-714) for debugging client performance issues.

**pg_kafka Approach**: Not needed. Use PostgreSQL statistics views instead.

**Client Impact**: None. Completely optional - clients work without this.

---

### Share Groups (12 APIs) - Priority: None (Kafka 4.0+)

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **ShareGroupHeartbeat** | 76 | Maintains membership in a share group | None - Kafka 4.0+ feature |
| **ShareGroupDescribe** | 77 | Describes share group state | None - Kafka 4.0+ feature |
| **ShareFetch** | 78 | Fetches records with shared consumption semantics | None - New consumption model |
| **ShareAcknowledge** | 79 | Acknowledges record processing (accept/reject/release) | None - New consumption model |
| **InitializeShareGroupState** | 83 | Initializes share group state | None - Kafka 4.0+ |
| **ReadShareGroupState** | 84 | Reads share group state | None - Kafka 4.0+ |
| **WriteShareGroupState** | 85 | Writes share group state | None - Kafka 4.0+ |
| **DeleteShareGroupState** | 86 | Deletes share group state | None - Kafka 4.0+ |
| **ReadShareGroupStateSummary** | 87 | Reads summary of share group state | None - Kafka 4.0+ |
| **DescribeShareGroupOffsets** | 90 | Describes offsets for share groups | None - Kafka 4.0+ |
| **AlterShareGroupOffsets** | 91 | Modifies share group offsets | None - Kafka 4.0+ |
| **DeleteShareGroupOffsets** | 92 | Deletes share group offsets | None - Kafka 4.0+ |

**Use Case**: Share groups (KIP-932) enable **competing consumers** where multiple consumers in the same group can process the same partition concurrently (like RabbitMQ/SQS). Records are "locked" until acknowledged. Major new feature for queue-like workloads.

**pg_kafka Approach**: Could be implemented in future using PostgreSQL row-level locking for record acknowledgment.

**Client Impact**: None. Kafka 4.0+ only, very low adoption currently.

---

### Topic Introspection (1 API) - Priority: Low

| API | Key | Use Case | Drop-In Priority |
|-----|-----|----------|------------------|
| **DescribeTopicPartitions** | 75 | Paginated topic/partition metadata (for clusters with many partitions) | Low - Performance optimization |

**Use Case**: Efficient metadata retrieval for clusters with thousands of partitions.

**pg_kafka Approach**: Metadata API already handles this for small-medium clusters.

**Client Impact**: None. Clients fall back to Metadata API.

---

## Drop-In Replacement Priority Matrix

| Priority | APIs | Rationale | Client Impact Without Implementation |
|----------|------|-----------|-------------------------------------|
| **Critical** | ✅ InitProducerId (22) | **ALREADY IMPLEMENTED** - librdkafka defaults to idempotence | Clients fail or require `enable.idempotence=false` |
| **High** | ✅ SaslHandshake (17), SaslAuthenticate (36) | **IMPLEMENTED** (no ACLs) | — |
| **Medium** | ✅ DescribeConfigs (32), ✅ IncrementalAlterConfigs (44) | **ALREADY IMPLEMENTED** (retention.ms honest-minimal) | — |
| **Medium** | ✅ DeleteRecords (21) | **ALREADY IMPLEMENTED** | — |
| **Low** | OffsetDelete (47), DescribeCluster (60) | Admin utilities | Minor admin CLI failures |
| **None** | All others (~25 APIs) | Cluster ops, KRaft, quotas, share groups, telemetry | No client impact - admin/internal only |

### Recommendation for Drop-In Replacement

**Current State**:
<!--gen:api-count-->28<!--/gen--> APIs (produce/consume, coordinator, transactions, admin, config/log
management, SASL), covering the produce/consume workloads.

**Comfortable Drop-In (add 1 API)**:
```
✅ DescribeConfigs (32)    - Already implemented
✅ DeleteRecords (21)      - Already implemented
+ DescribeCluster (60)     - Admin tools work (easy to stub)
```

**With Authentication**:
```
✅ SaslHandshake (17)      - Already implemented
✅ SaslAuthenticate (36)   - Already implemented
```

**Verdict**: pg_kafka is already a viable drop-in replacement for **most Kafka workloads**. The remaining standard APIs are either:
- Advanced or monitoring features (transaction admin APIs, share groups)
- Multi-broker operations (replication, leader election)
- Admin utilities (can be done via SQL)
- Bleeding-edge features (KIP-932 share groups, KIP-714 telemetry)

---

## Feature Gap Analysis

### Consumer Group Functionality

#### What We Have ✅ (Phase 5 Complete)
```
Consumer Flow (Current):
1. FindCoordinator → Returns localhost:9092
2. JoinGroup → Assigns member ID, generation
3. Leader/Server computes assignment → Range, RoundRobin, or Sticky strategy
4. SyncGroup → Distributes partition assignments
5. Heartbeat → Maintains membership (returns REBALANCE_IN_PROGRESS during rebalance)
6. Fetch → Reads assigned partitions
7. OffsetCommit → Tracks progress
8. LeaveGroup → Graceful exit (triggers rebalance for remaining members)
9. DescribeGroups/ListGroups → View group state
```

**Implemented Features (Phase 4-5):**
- ✅ **Automatic Partition Assignment**: Range, RoundRobin, Sticky (KIP-54) strategies
- ✅ **Automatic Rebalancing**: Trigger on member join/leave
- ✅ **Member Timeout Detection**: Background scanner removes dead members
- ✅ **REBALANCE_IN_PROGRESS**: Error code 27 forces client rejoin
- ✅ **Group Visibility**: DescribeGroups and ListGroups APIs

#### What's Still Missing ❌
- **Cooperative Rebalancing** (KIP-429)
  - Incremental partition revocation/assignment
  - Current implementation uses eager rebalancing

- **Static Group Membership** (KIP-345)
  - Persist member IDs across restarts
  - Prevent rebalance on consumer restart

### Multi-Partition Support ✅ (Phase 7)

- ✅ Configurable partition count (via CreateTopics or `pg_kafka.default_partitions` GUC)
- ✅ Key-based partition routing using murmur2 hash (Kafka-compatible)
- ✅ Metadata response includes correct partition count
- ✅ Null-key messages use random partition selection

---

## Roadmap

### Completed Phases

- **Phase 6** ✅ Admin APIs (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups)
- **Phase 7** ✅ Multi-Partition Topics with key-based routing
- **Phase 8** ✅ Compression Support (gzip, snappy, lz4, zstd)
- **Phase 9** ✅ Idempotent Producer (InitProducerId, sequence validation, deduplication)
- **Phase 10** ✅ Transaction Support (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
- **Phase 11** ✅ Shadow Mode (external Kafka forwarding, SASL/SSL, per-topic config)
- **Config & Log Management** ✅ DescribeConfigs, IncrementalAlterConfigs (`retention.ms`), DeleteRecords

### Enhancements

- **Long Polling** ✅ max_wait_ms/min_bytes support for efficient consumer waiting
- **Retention Sweep** ✅ time-based retention (`pg_kafka.message_retention_hours` GUC, per-topic `retention.ms` override)

### Future Phases

- **Cooperative Rebalancing** (KIP-429)
- **Static Group Membership** (KIP-345)
- Table partitioning (time-based retention is already implemented)

---

## Current vs Standard Kafka

### Protocol Version Support
| Feature | Standard Kafka | pg_kafka | Notes |
|---------|---------------|----------|-------|
| Wire Protocol | v0-v17+ | Per-API ranges, highest advertised is v11 (Fetch) | Flexible (tagged-field) versions supported; the response header version comes from each API's kafka-protocol response type |
| RecordBatch | v0, v1, v2 | v2 (Produce v3+) | Produce v0-v2 (MessageSet) is not advertised; a MessageSet v0/v1 batch in Produce v3+ is INVALID_RECORD, as in Kafka |
| Compression | All codecs | All codecs | gzip, snappy, lz4, zstd fully supported |
| Transactions | Yes | Yes | ✅ Phase 10 - EOS with server-side read_committed filtering |
| SASL/TLS | Yes | Yes | PLAIN, SCRAM-SHA-256/512, TLS + mTLS (see AUTHENTICATION.md) |
| ACLs | Yes | No | Not implemented: authenticated principals may do everything |

### Consumer Groups
| Feature | Standard Kafka | pg_kafka | Status |
|---------|---------------|----------|--------|
| Coordinator | Yes | Yes | ✅ Phase 3B |
| Member tracking | Yes | Yes | ✅ In-memory |
| Generation IDs | Yes | Yes | ✅ Implemented |
| Heartbeats | Yes | Yes | ✅ Implemented |
| Assignment strategies | Range, RR, Sticky | Range, RR, Sticky | ✅ Phase 4 |
| Auto-rebalance | Yes | Yes | ✅ Phase 5 |
| Timeout detection | Yes | Yes | ✅ Phase 5 |
| Group visibility | DescribeGroups, ListGroups | DescribeGroups, ListGroups | ✅ Phase 5 |
| Cooperative rebalance | Yes (KIP-429) | No | ❌ Future |
| Static membership | Yes (KIP-345) | No | ❌ Future |

---

## Quality Metrics

### Code Quality ✅
- **Compilation**: Zero errors, zero warnings
- **Architecture**: Clean Repository Pattern separation
- **Testing**: E2E tests with rdkafka
- **Documentation**: Comprehensive design docs

### Test Coverage

Unit tests (`src/`, per module), property tests (`tests/property_tests.rs`) and the E2E
suite (`kafka_test/`, 18 categories). Counts per module and per E2E category are kept in
one place: [TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary).

---

## Conclusion

**Current State**: Comprehensive Kafka-compatible broker with <!--gen:api-count-->28<!--/gen--> APIs implemented
**Coverage**: <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen--> (full producer/consumer/coordinator/admin/transaction/config support)
**Architecture**: Clean, maintainable, well-documented with Repository Pattern
**Test Status**: CI-gated; counts in [TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary)

**Readiness**:
- ✅ **Producer**: Production-ready with idempotency, transactions, and compression
- ✅ **Consumer**: Fully functional with long polling and automatic partition assignment
- ✅ **Coordinator**: Complete with automatic rebalancing
- ✅ **Admin APIs**: CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups
- ✅ **Config & Log Management**: DescribeConfigs, IncrementalAlterConfigs (`retention.ms`), DeleteRecords
- ✅ **Multi-Partition**: Key-based routing with murmur2 hash
- ✅ **Long Polling**: max_wait_ms/min_bytes support
- ✅ **Compression**: gzip, snappy, lz4, zstd (inbound/outbound)
- ✅ **Idempotency**: InitProducerId with sequence validation and deduplication
- ✅ **Transactions**: Full EOS with read-committed isolation
- ✅ **Shadow Mode**: External Kafka forwarding with SASL/SSL

**Recent Achievements (Phase 10-11)**:
1. ✅ Transaction APIs (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
2. ✅ Exactly-once semantics with read-committed isolation
3. ✅ Transactional producer fencing via epoch
4. ✅ Shadow mode for external Kafka forwarding
5. ✅ SASL/SSL authentication to external Kafka
6. ✅ Per-topic shadow configuration
7. ✅ Historical message replay to external Kafka
8. ✅ Unit and E2E coverage for transactions and shadow mode

**Future Phases**:
1. Cooperative rebalancing (KIP-429)
2. Static group membership (KIP-345)
3. Table partitioning

---

**Overall Assessment**: Phase 11 Complete - pg_kafka provides full producer/consumer support with idempotency, transactions, compression, long polling, multi-partition topics, admin APIs, and shadow mode. The implementation features clean architecture (Repository Pattern), comprehensive test coverage (unit, property and E2E), and typed error handling with full Kafka error code mapping.

**Last Updated:** 2026-10-29
**Phase:** 11 Complete (Shadow Mode)
**Tests:** counts in [TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary)
