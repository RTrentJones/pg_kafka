# Kafka Protocol Deviations

This document lists intentional deviations from the official Kafka protocol specification.

## Summary

`pg_kafka` implements a **subset** of the Kafka wire protocol (56% API coverage). The following deviations are intentional design decisions that optimize for PostgreSQL's strengths while maintaining compatibility with standard Kafka clients.

| Category | Status |
|----------|--------|
| **Producer APIs** | ✅ Full support (idempotent + transactional) |
| **Consumer APIs** | ✅ Full support (with automatic rebalancing) |
| **Coordinator APIs** | ✅ Full support (Range, RoundRobin, Sticky strategies) |
| **Admin APIs** | ✅ Supported (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups; see §13) |
| **Config & Log APIs** | ✅ Minimal support (DescribeConfigs, IncrementalAlterConfigs for `retention.ms` only, DeleteRecords) |
| **Transaction APIs** | ✅ Full support (EOS with read-committed isolation) |
| **Shadow Mode** | ✅ Full support (external Kafka forwarding) |
| **Security APIs** | ✅ SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 and TLS listeners; **no ACLs** (see "Network Security Posture") |

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
-- In postgresql.conf (postmaster-level GUC, requires a server restart; default: none)
pg_kafka.compression_type = 'gzip'
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
- ✅ Sequence number tracking per topic-partition, Kafka's model (wave 3): the last 5
  batches of every producer on every partition are remembered (`kafka.producer_batches`:
  first/last sequence and base offset, durable, so it survives worker restarts)
- ✅ Duplicate detection: a retry of one of those 5 batches (same epoch, same sequence
  range) is acknowledged with error code 0, **its original base offset**, and nothing is
  re-inserted, as in Kafka (DUPLICATE_SEQUENCE_NUMBER is defined but, as in Kafka, never
  returned)
- ✅ Anything else out of sequence (a gap, an overlap, a range at or below the last
  sequence that was never written, a retry older than the 5 cached batches) is
  OUT_OF_ORDER_SEQUENCE_NUMBER (45); sequences wrap from 2147483647 to 0
- ✅ Epochs: a Produce with an epoch older than the producer's current one gets
  INVALID_PRODUCER_EPOCH (47), what Kafka 3.7 answers on Produce for idempotent and
  transactional producers alike; a higher epoch (an idempotent producer bumping its own
  epoch, KIP-360, as the Java client does) is accepted when it starts at sequence 0 and
  becomes the producer's epoch
- ✅ PostgreSQL advisory locks for sequence serialization
- **Deviation:** a producer with no state on a partition must start at sequence 0 there
  (OUT_OF_ORDER_SEQUENCE_NUMBER otherwise). Kafka accepts any first sequence from an
  unknown producer (its producer state can expire with the log); pg_kafka keeps producer
  state for 7 idle days and refuses a first batch whose predecessors never arrived.
- Produce responses report `log_start_offset = -1` and `log_append_time_ms = -1`

**Client Impact:**
- Clients can use `enable.idempotence=true` (default in librdkafka 2.0+)
- Full exactly-once delivery semantics within a producer session
- Standard duplicate handling works as expected

### 4. Transactions ✅ Fully Supported (Phase 10)

**Status:** Fully Implemented
**Kafka Behavior:** Multi-partition atomic writes via `transactional.id`
**pg_kafka Behavior:** Full transaction coordinator with EOS

**Implementation:**
- ✅ AddPartitionsToTxn registers partitions in the transaction (`kafka.txn_partitions`);
  a transactional Produce to a partition the open transaction did not add is refused with
  INVALID_TXN_STATE (48), as Kafka 3.6+ does. AddPartitionsToTxn does not create topics: an
  unknown topic or partition is UNKNOWN_TOPIC_OR_PARTITION and the request's other
  partitions get OPERATION_NOT_ATTEMPTED (55), nothing is added
- ✅ InitProducerId refuses a transaction timeout <= 0 or above
  `pg_kafka.transaction_max_timeout_ms` (Kafka's `transaction.max.timeout.ms`, default
  900000) with INVALID_TRANSACTION_TIMEOUT (50)
- ✅ AddOffsetsToTxn includes consumer offsets in transaction
- ✅ EndTxn commits or aborts atomically
- ✅ TxnOffsetCommit commits offsets within transaction context
- ✅ Read-committed isolation level filters pending/aborted messages
- ✅ Transactional producer fencing via epoch

**Difference:** EndTxn moves a transaction directly from `Ongoing` to `CompleteCommit`/`CompleteAbort` inside one PostgreSQL transaction; no transaction markers are written to the log, and the `PrepareCommit`/`PrepareAbort` states are never persisted. Read-committed visibility comes from the `txn_state` column instead (see §7).

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
- ✅ JoinGroup responses are held until the join phase completes, as in Kafka: a
  rebalance waits for every known member to rejoin (members that have not learn of it
  from Heartbeat, REBALANCE_IN_PROGRESS), and the generation is incremented when the
  join completes, so every member gets the same generation and the leader's member
  list covers all of them. The phase also completes when the members that have not
  rejoined leave, miss their session timeout, or are evicted by the rebalance timeout
  (the largest `rebalance_timeout_ms` the members advertise); a periodic sweep (about
  every second) runs those timeouts, so a held JoinGroup is answered even if no other
  request arrives. A group whose only member is the joiner completes at once. While a
  member waits on its JoinGroup its session timeout is suspended. OffsetCommit at the
  current generation is accepted during the join phase (the commit made when
  partitions are revoked) and fenced with REBALANCE_IN_PROGRESS during the sync phase.
  A stable group rebalances when a member joins, a member's subscription changes, or
  the leader rejoins; a follower rejoining with unchanged metadata keeps its
  generation and assignment. Heartbeat during the sync phase answers NONE.
- ✅ A follower's SyncGroup that arrives before the leader's is held, as in Kafka,
  and answered with its assignment (empty if the leader left it out) when the
  leader's SyncGroup arrives; the generation does not move. A held SyncGroup is
  answered REBALANCE_IN_PROGRESS if a new rebalance starts first (a member joins or
  leaves, or the leader misses the rebalance timeout and the sweep evicts it), and
  UNKNOWN_MEMBER_ID if the member itself is removed. Like a waiting joiner, a member
  whose SyncGroup is held is exempt from its session timeout. Server-side
  assignment (a leader sending empty assignments) is computed only for the leader.

**What's Not Implemented:**
- ❌ Static group membership (KIP-345): `group_instance_id` is not honored
- ❌ Cooperative rebalancing (KIP-429)
- ❌ `MEMBER_ID_REQUIRED` (KIP-394): a first JoinGroup is admitted with a new member id
  directly. A client that closes while its first JoinGroup is held never learns its
  id, so it cannot send LeaveGroup; it is removed by its session timeout once the join
  phase completes.
- ❌ No initial rebalance delay (`group.initial.rebalance.delay.ms`): the first member
  of an empty group completes its join at once, and members joining right after it
  start a second rebalance.

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
- ✅ `require_stable` (OffsetFetch v7, KIP-447): a partition with a TxnOffsetCommit whose
  transaction is still open is answered UNSTABLE_OFFSET_COMMIT (88) with offset -1, at
  partition level as Kafka 3.7 does, so a read_committed consumer waits for it
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
- ✅ Record headers returned to consumers (DR-8, DEEP-REVIEW-2026-07)
- ✅ Empty fetch responses (returns empty bytes)
- ✅ Partition watermarks (high watermark, log start offset)
- ✅ Byte-budget fetch sizing — small-message fetches fill `max_bytes` via bounded
  iterative queries instead of capping at a fixed row count (DR-9)
- ✅ Request-level `max_bytes` (KIP-74): partitions are filled in request order until
  the response budget is spent; the first partition with data always returns at least
  one message, later partitions return only what still fits (sizes are estimated as
  key + value + 64 bytes per record, the same accounting `partition_max_bytes` uses)
- ✅ Long polling (max_wait_ms/min_bytes support). A waiting fetch is re-run only
  when a Produce to one of its partitions (or, for `read_committed`, any EndTxn)
  signals new data, at `max_wait_ms`, or at the slow safety-net interval
  `pg_kafka.fetch_poll_interval_ms` (default 1000 ms) — idle consumers do not
  generate repeated fetch work on the DB thread

**What's Missing:**
- ❌ Fetch sessions (always uses fetch session ID 0)
- At most 64 long-poll fetches may wait concurrently per connection; a fetch over that cap is answered immediately (possibly empty) instead of waiting for `max_wait_ms`
- At most 128 requests per connection may be in flight (read but not yet answered on the socket); beyond that the broker stops reading from the connection until responses are consumed (TCP backpressure), much as Kafka mutes a connection while it has an outstanding request

**Client Impact:**
- Works with standard Kafka clients
- Low latency with long polling enabled (default: on)

**Deviation: server-side read_committed filtering (DR-11, DEEP-REVIEW-2026-07; wave 3)**

Real Kafka returns *all* records (including aborted ones) to a `read_committed`
consumer along with control batches and an `aborted_transactions` list, and the
client filters aborted ranges itself (the wire contract for Fetch ≥ v4).
pg_kafka instead filters **server-side**: pending/aborted rows are excluded by
the fetch query, results are clamped to the Last Stable Offset (RV-4), and the
response carries no `aborted_transactions` list. pg_kafka writes no transaction
markers, so aborted offsets (and rows the aborted-row cleanup deleted) are holes in
the log:

- Records between holes carry their own offsets, so consumers skip holes in the
  middle of a response.
- When nothing is visible from the fetch offset up to the readable end (the LSO for
  `read_committed`, the high watermark for `read_uncommitted`), the response holds one
  **control batch** covering the hole, with a single COMMIT marker at its last offset,
  like the marker Kafka writes when a transaction ends. Clients skip control records
  and move their position past it (the Java client to the batch's last offset + 1,
  librdkafka to the marker's offset + 1), so their position and committed offset reach
  the end and the lag goes to 0. Before wave 3 such a fetch was empty and the consumer
  re-fetched the same offset forever. An open transaction still blocks at the LSO.
- Fetched records are re-encoded, grouped into one RecordBatch per run of consecutive
  offsets written by the same producer id/epoch (wave 3; before, every record was its own
  batch, ~60 bytes of overhead each). Batches of transactional rows carry their producer
  id/epoch and the transactional flag; other batches `producer_id = -1`. The base
  sequence is sent as -1, `partition_leader_epoch` is 0, and batch boundaries may differ
  from the original produce.
- The data a client receives is already clean, so read_committed correctness holds for
  clients that do their own abort filtering too (verified with the Java consumer and
  librdkafka); what differs from Kafka is the byte-level batch layout and that
  `read_uncommitted` consumers no longer see aborted records once the cleanup deleted them.
- Header order is preserved (headers are stored as an ordered array of pairs), but
  duplicate header keys collapse to the last value: the kafka-protocol record type
  holds headers in a map on both decode and encode. Kafka's wire format permits
  duplicate keys.

Related minor deviations: `throttle_time_ms` is always 0 (no quota system) and
`log_append_time_ms` is always -1 (LogAppendTime timestamp type is not
honored; producer CreateTime timestamps are preserved — BUG-7).

### 8. ListOffsets ✅ Implemented

**Status:** Fully implemented
**Kafka Behavior:** Returns offsets by timestamp or special values
**pg_kafka Behavior:** Supports earliest (-2), latest (-1), max timestamp (-3) and timestamp lookup

**Implementation:**
- ✅ Returns earliest offset (first message in partition; -4 EARLIEST_LOCAL is the same)
- ✅ Returns latest offset: the high watermark, or the last stable offset for a
  `read_committed` request (wave 3)
- ✅ MAX_TIMESTAMP (-3, KIP-734): the offset and timestamp of the record with the largest
  timestamp (the lowest offset among equal ones; wave 3). Kafka 3.7.0 itself answers the
  last offset of that record's batch (KAFKA-16310, fixed in later releases)
- ✅ Timestamp-based lookup — returns the earliest offset whose record timestamp is ≥ the requested time (record timestamps are persisted; AUDIT-2026-06 BUG-7 + CONF-7)
- ✅ Timestamp lookups are bounded like Kafka's: a match at or past the high watermark
  (`read_committed`: the LSO) is offset -1; aborted records are never a match
- ✅ A partition the topic does not have is UNKNOWN_TOPIC_OR_PARTITION (was: offset 0)

## Metadata API Deviations

### 9. Single Broker Only

**Status:** By Design
**Kafka Behavior:** Returns list of all brokers in cluster
**pg_kafka Behavior:** Returns single broker (self)

**Rationale:**
- `pg_kafka` is a single-node "broker" backed by PostgreSQL
- High availability comes from PostgreSQL HA (Patroni, RDS Multi-AZ, etc.)

### 10. Dynamic Topic Creation

**Status:** Always enabled for Produce
**Kafka Behavior:** Governed by the broker setting `auto.create.topics.enable` (default `true`, commonly disabled in production)
**pg_kafka Behavior:** There is no broker-level switch. Topics are always auto-created on first produce. A Metadata request also auto-creates requested topics unless the client sends `allow_auto_topic_creation=false` (Metadata v4+), in which case a missing topic is reported as UNKNOWN_TOPIC_OR_PARTITION

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
- Idempotent/transactional producers (`producer_id >= 0`) cannot use partition -1: a batch's sequence numbers are tracked per partition, so splitting it by key would corrupt sequence tracking. Such a batch is rejected with UNKNOWN_TOPIC_OR_PARTITION (real Kafka does not route -1 at all; clients always pick the partition).

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

## Admin API Deviations

### 13. CreateTopics / DeleteGroups / Config APIs

**Status:** Minor deviations by design

- **CreateTopics ignores topic configs.** Per-topic `configs` (e.g. `retention.ms`), manual replica assignments, and `replication_factor` in the request are accepted but not applied; the response always reports replication factor 1. Set `retention.ms` afterwards with IncrementalAlterConfigs.
- **CreateTopics default partition count.** `num_partitions = -1` ("broker default", what most admin clients send) creates a topic with `pg_kafka.default_partitions` partitions, the same default auto-created topics get. Any other value below 1, such as `0`, is rejected with INVALID_PARTITIONS.
- **DeleteGroups on an unknown group** succeeds (error code 0, any orphaned committed offsets are deleted) instead of returning GROUP_ID_NOT_FOUND.
- **CreatePartitions** does not itself trigger a rebalance; consumers pick up the new partitions on their next metadata refresh.
- **DescribeConfigs** supports only TOPIC resources and reports only `retention.ms` and `cleanup.policy` (always `delete`; there is no log compaction). BROKER and other resource types return INVALID_REQUEST. Versions v1-v4 only (the kafka-protocol crate does not implement v0).
- **IncrementalAlterConfigs** accepts only SET/DELETE of `retention.ms` on TOPIC resources. Any other key or operation (APPEND/SUBTRACT) fails the whole resource with INVALID_CONFIG rather than being silently accepted.
- **AlterConfigs (33)** is not implemented.

## API Version Support

### Supported APIs (28 total)

These ranges are both advertised in ApiVersions and enforced on inbound requests (each API's `MIN_VERSION`/`MAX_VERSION` in `src/kafka/api/`); a request above its range gets UNSUPPORTED_VERSION in the requested version (see "Error responses" above).

| API Key | Name | Versions | Status |
|---------|------|----------|--------|
| 0 | Produce | 3-9 | ✅ Full support (with key-based routing) |
| 1 | Fetch | 4-11 | ✅ Full support (v12+ not advertised; v13+ uses topic IDs) |
| 2 | ListOffsets | 1-7 | ✅ Implemented (incl. timestamp lookup) |
| 3 | Metadata | 0-10 | ✅ Full support (v10 `topic_id` is the zero UUID) |
| 8 | OffsetCommit | 2-8 | ✅ Full support |
| 9 | OffsetFetch | 1-7 | ✅ Full support |
| 10 | FindCoordinator | 0-3 | ✅ Full support |
| 11 | JoinGroup | 0-7 | ✅ Full support |
| 12 | Heartbeat | 0-4 | ✅ Full support |
| 13 | LeaveGroup | 0-4 | ✅ Full support |
| 14 | SyncGroup | 0-4 | ✅ Full support |
| 15 | DescribeGroups | 0-5 | ✅ Full support |
| 16 | ListGroups | 0-4 | ✅ Full support |
| 17 | SaslHandshake | 0-1 | ✅ Network thread; v0 is advertised (librdkafka requires it) but answered UNSUPPORTED_VERSION, see "Network Security Posture" |
| 18 | ApiVersions | 0-3 | ✅ Full support |
| 19 | CreateTopics | 2-5 | ✅ Supported (topic configs ignored, see §13) |
| 20 | DeleteTopics | 1-4 | ✅ Full support |
| 21 | DeleteRecords | 0-2 | ✅ Full support |
| 22 | InitProducerId | 0-4 | ✅ Full support |
| 24 | AddPartitionsToTxn | 0-3 | ✅ Full support |
| 25 | AddOffsetsToTxn | 0-3 | ✅ Full support |
| 26 | EndTxn | 0-3 | ✅ Full support |
| 28 | TxnOffsetCommit | 0-3 | ✅ Full support |
| 32 | DescribeConfigs | 1-4 | ✅ Minimal (TOPIC resources, see §13) |
| 36 | SaslAuthenticate | 0-2 | ✅ PLAIN, SCRAM-SHA-256, SCRAM-SHA-512; `session_lifetime_ms` = 0 (no re-authentication) |
| 37 | CreatePartitions | 0-2 | ✅ Full support |
| 42 | DeleteGroups | 0-2 | ✅ Full support |
| 44 | IncrementalAlterConfigs | 0-1 | ✅ Minimal (`retention.ms` only, see §13) |

**Note:** OffsetFetch v8+ uses a different (multi-group) response format. We support v0-7.

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

**Per-item errors.** Produce, OffsetCommit, ListOffsets, DescribeGroups, CreateTopics,
DeleteTopics, CreatePartitions, DeleteGroups, DeleteRecords, DescribeConfigs and
IncrementalAlterConfigs have no top-level error code, so their handlers report every
failure, storage failures included, on the topic/partition/resource/group it affects and
keep processing the other items (as Kafka does).

**Error responses for requests that never reach a handler result.** Every request that
fails after a valid header is answered with a well-formed response of its own API and
version carrying an error code, built by the API's `KafkaApi::error_response`
(`src/kafka/api/`), which puts the code in the top-level error field where the response has
one and on every requested topic/partition/group/resource it echoes:

| Failure | Error code | Echoed entries |
|---------|------------|----------------|
| Version above the advertised range (kafka-protocol can encode it) | UNSUPPORTED_VERSION (35), in the requested version | whatever the body decodes to at that version |
| Body that does not decode (garbage, or truncated after the header) | INVALID_REQUEST (42) | none (nothing was decoded) |
| Handler returns `Err`, or panics (a Postgres ERROR) | the error's code / UNKNOWN_SERVER_ERROR (-1) | every requested entry |
| The batch's commit fails | UNKNOWN_SERVER_ERROR (-1) | every requested entry |

Remaining deviations: a body that does not decode has no entries to echo, so APIs without
a top-level error code (Produce, ListOffsets, DescribeGroups, CreateTopics, ...) answer with
an empty but well-formed response. A version kafka-protocol cannot encode a response in
(below the advertised minimum, which is the crate's own minimum, or above the crate's
maximum) closes the connection, as Kafka does for an unsupported version. A frame whose
header cannot be parsed closes the connection. An API key pg_kafka does not serve gets the
generic error frame (correlation id, UNSUPPORTED_VERSION, null message), since there is no
response schema to answer in. An acks=0 Produce never gets a response.

### Notable Missing APIs

- ACL APIs (DescribeAcls, CreateAcls, DeleteAcls) and SCRAM credential admin APIs (DescribeUserScramCredentials, AlterUserScramCredentials) - not implemented; users are managed with SQL functions (`docs/AUTHENTICATION.md`)
- Broker-internal / admin-utility APIs (DescribeTransactions, ListTransactions, DescribeProducers) - Not planned (monitoring-only)
- Log compaction - Not planned (PostgreSQL UPDATE provides similar semantics)

> Transaction APIs (InitProducerId 22, AddPartitionsToTxn 24, AddOffsetsToTxn 25, EndTxn 26, TxnOffsetCommit 28), the core Admin APIs (CreateTopics, DeleteTopics, CreatePartitions, DeleteGroups), and the config/log APIs (DescribeConfigs 32, IncrementalAlterConfigs 44, DeleteRecords 21) **are** implemented — see the Summary above and `KAFKA_PROTOCOL_COVERAGE.md`.

## Acknowledgment Durability (RB-1)

**Status:** Kafka-conformant as of the response-after-commit barrier.

For `acks >= 1`, a produce (or offset-commit/admin) response is delivered to
the client **only after the backing PostgreSQL transaction has committed** —
the ack implies the write is durable (WAL-flushed) and visible to any other
PostgreSQL connection. Historically the response was sent from inside the
open transaction, leaving a microsecond-scale window where an acked write
could be lost on a crash; that window no longer exists. (`acks=0` remains
fire-and-forget with no response frame and no durability claim — see §1.)

## Shadow Forwarding Delivery Semantics (Issue #93)

**Status:** At-least-once on decoupled paths, by design.

Shadow forwarding to an external Kafka cluster uses a durable outbox
(`kafka.shadow_tracking`) drained by a poll loop:

- **Sync mode** (`forward_sync_after_commit`): effectively exactly-once at the
  external broker in normal operation — after the local commit, and before the
  produce response is sent, the worker forwards the request's rows and waits
  (bounded, 5 s) for the external acks. Rows of a request that rolled back are
  never forwarded, so the external broker never holds a record pg_kafka did
  not store.
- **Async / forward-on-commit modes:** **at-least-once**. A row whose forward
  ack is lost (worker crash between delivery and finalization, dropped ack on
  a full channel) is re-forwarded after its retry lease expires, so the
  external topic can receive duplicates in failure windows. This is the
  standard Kafka mirroring guarantee; consumers of the external topic that
  need exactly-once semantics must deduplicate (e.g. by key + producer
  metadata).
- **In-flight gating:** a row whose forward is merely *slow* (ack pending) is
  no longer re-claimed at lease expiry — the DB thread tracks dispatched
  forwards and the outbox claim skips them until the ack resolves or the
  entry ages out (`INFLIGHT_EVICT_AFTER`, sized above rdkafka's
  `message.timeout.ms`). Duplicates therefore occur only on genuine ack
  *loss*, not ack *latency*. Local storage and `kafka.shadow_tracking`
  finalization remain exactly-once in all modes.

## Network Security Posture (SEC-7)

**Status:** Authentication available (TLS, SASL); **authorization not implemented**. Full
reference: [AUTHENTICATION.md](AUTHENTICATION.md).

- **Default:** one PLAINTEXT listener on `127.0.0.1:9092` (`pg_kafka.host`, `pg_kafka.port`,
  `pg_kafka.security_protocol = 'PLAINTEXT'`) with no authentication, exactly as before. Any
  client that can open a TCP connection to a PLAINTEXT listener can read and write any topic.
- **Listener security protocols** as in Kafka: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`,
  for the primary listener and for any `pg_kafka.additional_listeners`
  (`'SASL_SSL://0.0.0.0:9093,...'`). Each listener advertises its own address in
  Metadata/FindCoordinator. TLS is rustls; mutual TLS when `pg_kafka.ssl_ca_file` is set.
- **SASL:** PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (`pg_kafka.sasl_enabled_mechanisms`), checked
  against salted SCRAM verifiers in `kafka.sasl_credentials`. GSSAPI and OAUTHBEARER are not
  supported.
- **No ACLs.** An authenticated principal may do everything, like any client of a PLAINTEXT
  listener. The principal (`User:<name>`, the client-certificate subject, or `User:ANONYMOUS`) is
  recorded per connection and logged, so ACLs can be added later; nothing checks it yet.
- **Deviations from Kafka's authenticator:** SaslHandshake v0 (raw tokens after the handshake)
  is advertised but answered UNSUPPORTED_VERSION; sessions are never re-authenticated (KIP-368, `session_lifetime_ms = 0`), and
  dropping a user does not close its open connections; SCRAM ignores SASLprep (as Kafka does),
  rejects channel binding and delegation tokens; users are managed with SQL functions
  (`pg_kafka_create_user` etc.) rather than AlterUserScramCredentials; listener settings apply
  when the worker restarts, not on reload.
- **Loud by default:** the worker logs a startup `WARNING` for every listener bound to a
  non-loopback address that does not authenticate clients (PLAINTEXT, or SSL without a client
  CA), and for SASL/PLAIN offered on an exposed SASL_PLAINTEXT listener.

## Compatibility Testing

### Tested Clients

| Client | Version | Status | Notes |
|--------|---------|--------|-------|
| kcat | 1.7.0+ | ✅ Works | Producer and consumer tested |
| rdkafka (Rust) | 0.36+ | ✅ Works | Full E2E test suite (`kafka_test/`) |

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

> **Shadow Mode is implemented** (Phase 11 — external Kafka forwarding, per-topic config, SASL/SSL) and is no longer future work. See `KAFKA_PROTOCOL_COVERAGE.md` and the `SHADOW_MODE_*` docs. Known correctness limitations (durable forwarding, replay idempotency, the license gate) are tracked as SH-1..16 in `archive/AUDIT-2026-06.md`.

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

> Exactly-once semantics (EOS) **is** implemented via the Phase 10 transaction APIs and read-committed isolation — see the Summary above and `KAFKA_PROTOCOL_COVERAGE.md`.

---

**Last Updated:** 2026-10-29
**Applies To:** pg_kafka Phase 11 Complete (Shadow Mode)
**API Coverage:** 28 of ~50 Kafka APIs (56%)
**Test Status:** unit + property + E2E, CI-gated; counts in [TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary)
