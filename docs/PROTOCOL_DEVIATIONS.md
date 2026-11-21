# Kafka Protocol Deviations

This document lists intentional deviations from the official Kafka protocol specification.

## Summary

`pg_kafka` implements a **subset** of the Kafka wire protocol (<!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen-->). The following deviations are intentional design decisions that optimize for PostgreSQL's strengths while maintaining compatibility with standard Kafka clients.

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
- Errors are not returned to the client (per acks=0 contract); as in Kafka, any error
  (a partition error, a corrupt batch, a failed commit) **closes the connection** after the
  partitions that succeeded are committed, so the client refreshes its metadata
- `acks` other than -1, 0 and 1 is answered `INVALID_REQUIRED_ACKS` (21) and nothing is
  written, as in Kafka, after the checks Kafka makes first: a first batch it cannot size
  (CORRUPT_MESSAGE) or a transactional batch without a transactional id
  (TRANSACTIONAL_ID_AUTHORIZATION_FAILED) on every partition, then per partition an unknown
  topic or partition (UNKNOWN_TOPIC_OR_PARTITION) and data that is not one RecordBatch v2
  or zstd before v7 (INVALID_RECORD, UNSUPPORTED_COMPRESSION_TYPE); every other partition is
  INVALID_REQUIRED_ACKS whatever its records hold (they are not decoded)
- Long-polling consumers are notified if write succeeds
- Idempotent sequences and transactions are checked as with acks >= 1, as in Kafka: an
  out-of-sequence or refused transactional batch is an error, which closes the connection

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
- ✅ **Inbound (Producer → Server)**: pg_kafka reads the RecordBatch v2 itself, with Kafka 3.7's
  record grammar (varints of at most 5 bytes, the timestamp delta a varlong of at most 10)
- ✅ **Outbound (Server → Consumer)**: Configurable via `pg_kafka.compression_type` GUC
- ✅ **Supported codecs**: none, gzip, snappy, lz4, zstd

**Configuration:**
```sql
-- In postgresql.conf (postmaster-level GUC, requires a server restart; default: none)
pg_kafka.compression_type = 'gzip'
```

**Client Impact:**
- Clients can use any compression type (gzip, snappy, lz4, zstd)
- Server decompresses messages before storage. A request's records may take at most 100 MB
  of memory once decoded, across its partitions: their decoded bytes plus a fixed cost per
  record (88 bytes) and per header (48 bytes), charged from the declared record count before
  anything decodes. A partition that does not fit in what its request may still hold is
  answered MESSAGE_TOO_LARGE (10) on its own and nothing of it is written; the request's
  other partitions are written, as Kafka answers a batch past `message.max.bytes`. Kafka
  streams validation and has no such limit: it accepts such a partition when its batch is
  within `message.max.bytes` (1 MiB by default; a gzip batch of 150 MB of zeros is some
  150 KB), and pg_kafka has no `message.max.bytes`, so it accepts an uncompressed batch over
  1 MiB that Kafka answers MESSAGE_TOO_LARGE. A batch whose declared records alone take
  more than 100 MB (over ~1.1 million records), or snappy data declaring more than 100 MB
  decoded, is refused MESSAGE_TOO_LARGE before anything decodes; Kafka answers a
  compressed one INVALID_RECORD when its records do not match the count (UNKNOWN_SERVER_ERROR
  for snappy data that does not decompress). An uncompressed batch whose records section is
  too short to hold its declared count (at 7 bytes a record) is INVALID_RECORD, as in Kafka.
  A partition refused for any reason, even after it decoded, no longer counts against the
  partitions after it.
- A request whose records take more than 2 MiB decoded (a default-size 1 MiB request that
  is compressed, or of records under some 200 bytes) decodes off the network I/O threads, at
  most two such decodes at a time. A request that finds both taken waits its turn (in arrival
  order, one request per connection at a time; the connection reads nothing more meanwhile),
  and so does any request that finds the 512 MiB of decoded records waiting for the database
  full, until room comes free. The wait is at most half the request's `timeout_ms` and at most
  10 s (Java and librdkafka send their `request.timeout.ms`, 30 s by default, so the answer
  reaches them first); past it the request is answered REQUEST_TIMED_OUT (7) on every
  partition and nothing is written. Clients retry it (an idempotent producer keeps its order;
  a non-idempotent one with several requests in flight may reorder, as on any retriable
  error). Kafka has no such limit on decoding: when its request queue is full
  (`queued.max.requests`, 500) or it lacks memory for requests (`queued.max.request.bytes`,
  unbounded by default) it stops reading connections, and its clients time out on their own. An acks=0 request that times out closes its connection.
- The decoding work of a request is bounded as well as its memory: every byte its decode
  charged counts, given back or not, so partitions refused after they decoded (INVALID_TIMESTAMP,
  a record count that does not match, MESSAGE_TOO_LARGE at the limit) cannot make one request
  decode without end. On a network I/O thread a request may decode 6 MiB (three times the
  2 MiB it may hold there); past that it decodes again off the I/O threads. There it may
  decode 400 MB (four times the 100 MB it may hold: what a request that fits takes, and room
  for refused partitions besides); past that its other partitions are refused
  MESSAGE_TOO_LARGE without decoding (UNKNOWN_TOPIC_OR_PARTITION for those of an unknown
  topic). Kafka decodes every partition on its request handler threads; only a request of
  several partitions that each decode large batches and are refused is answered differently.
- As in Kafka 3.7 (`KafkaApis.handleProduceRequest` collects a request's partitions in maps),
  a partition named more than once in a request, in one topic entry or in several, is
  answered once, for its last entry; the earlier entries are neither decoded nor written.
  Kafka also answers the error of an earlier entry that fails `ProduceRequest.validateRecords`
  (data that is not one RecordBatch v2, zstd before v7) while it writes the last one;
  pg_kafka answers and writes the last entry only.
- zstd frames may declare a window of at most 2^27 bytes (128 MiB), as in Kafka; a decoder's
  window counts toward that limit as it fills (as much again as the decoded bytes, up to the
  window). A larger window is CORRUPT_MESSAGE (Kafka 3.7: UNKNOWN_SERVER_ERROR). zstd on
  Produce v3-v6 is UNSUPPORTED_COMPRESSION_TYPE (76), as in Kafka (KIP-110).
- Each partition's data must be exactly one RecordBatch v2, checked as Kafka 3.7 checks it
  and answered with the same codes:
  - INVALID_RECORD: a legacy MessageSet (magic 0/1); empty or cut-short data; a second
    complete batch; a base offset other than 0; a record count of 0 or less, or one that
    does not match lastOffsetDelta + 1 or the records (too few or too many); a control
    batch; a producer id with a negative base sequence; a record with bytes past its
    fields, a varint longer than 5 bytes, a varlong longer than 10, or a negative header
    count or header key length.
  - CORRUPT_MESSAGE: a bad CRC; a batch length under 14, or a v2 batch length shorter than
    its header; a magic byte outside 0-2 (on the batch or on data after it). Unknown codec
    ids are CORRUPT_MESSAGE too (Kafka 3.7: UNKNOWN_SERVER_ERROR).
  - INVALID_TIMESTAMP (32): a batch that sets the LogAppendTime timestamp type with a
    maxTimestamp of -1, which Kafka refuses on a CreateTime topic (the only kind pg_kafka
    has). With a maxTimestamp the batch is accepted and its records keep their own
    timestamps, as Kafka stores it (as CreateTime).
  - A batch that fails several checks gets the code of the one Kafka checks first: the
    framing above, the base offset, the header size and CRC, the codec, the header fields,
    the records, then the LogAppendTime type.
  - Data after the batch too short to hold another batch is ignored, as Kafka trims it.
  - As in Kafka, a negative key, value or header value length is null (a null header
    value is stored empty) and duplicate header keys are stored, in order (see §7 for
    what a fetch returns). A header key that is not UTF-8 is accepted and stored with
    replacement characters, what a Java client reads it as; Kafka stores its bytes.

  As in Kafka 3.7, each partition is answered on its own: a partition whose data fails a
  check gets that code and nothing of it is written, while the request's other partitions
  are written (an acks=0 request with a refused partition writes the others, then closes
  its connection). A refused partition of a topic that does not exist, or outside its
  topic, is UNKNOWN_TOPIC_OR_PARTITION whatever its data, and the topic is not created
  for it. A refused partition leaves its idempotent producer's sequence and its
  transaction as they were. Only a first batch whose length (under 14) or magic byte
  (outside 0-2) cannot be read fails the whole request, CORRUPT_MESSAGE on every
  partition, as Kafka reads every partition's first batch before it looks at any
  partition (it stops at the first transactional batch, and so does pg_kafka); past a
  partition's first batch, the same data refuses that partition only. A request whose
  first transactional batch comes without a transactional id is answered
  TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) on every partition and nothing of it is
  written (an acks=0 one closes its connection), as Kafka refuses it. Where Kafka 3.7
  answers UNKNOWN_SERVER_ERROR (an unknown codec id, a zstd window past 2^27, data that
  does not decompress) pg_kafka answers CORRUPT_MESSAGE on that partition; Kafka fails the
  whole request for an unknown codec id on Produce v3-v6.
- Server compresses responses based on GUC setting

### 3. Idempotent Producer ✅ Fully Supported (Phase 9)

**Status:** Fully Implemented
**Kafka Behavior:** `enable.idempotence=true` prevents duplicate messages
**pg_kafka Behavior:** Full idempotency support with sequence validation

**Implementation:**
- ✅ InitProducerId API allocates producer IDs with epoch
- ✅ Sequence number tracking per topic-partition, Kafka's model: the last 5
  batches of every producer on every partition are remembered (`kafka.producer_batches`:
  first/last sequence and base offset, durable, so it survives worker restarts)
- ✅ Duplicate detection: a retry of one of those 5 batches (same epoch, same sequence
  range) is acknowledged with error code 0, **its original base offset**, and nothing is
  re-inserted, as in Kafka (DUPLICATE_SEQUENCE_NUMBER is defined but, as in Kafka, never
  returned)
- ✅ Anything else out of sequence (a gap, an overlap, a range at or below the last
  sequence that was never written, a retry older than the 5 cached batches) is
  OUT_OF_ORDER_SEQUENCE_NUMBER (45); sequences wrap from 2147483647 to 0
- ✅ Producer state is per partition, as in Kafka: a producer's first batch on a partition
  where it has no state is accepted at any sequence and tracked from there
- ✅ Epochs: a Produce with an epoch older than the producer's current one gets
  INVALID_PRODUCER_EPOCH (47), what Kafka 3.7 answers on Produce for idempotent and
  transactional producers alike; a higher epoch (an idempotent producer bumping its own
  epoch, KIP-360, as the Java client does) becomes the producer's epoch, and on every
  partition where the producer wrote at an earlier epoch its first batch at the new epoch
  must start at sequence 0 (any sequence on a partition where it has no state)
- ✅ A partition whose records are null is refused UNKNOWN_SERVER_ERROR (-1), as Kafka 3.7
  answers it, and the request's other partitions are written (with acks=0 the refusal
  closes the connection); no producer state is taken from it, so the producer's next
  batch there is checked against its earlier batches only
- ✅ PostgreSQL advisory locks for sequence serialization
- ✅ A producer the broker holds no state for (a producer id with no `kafka.producer_ids`
  row: removed by the retention sweep after 7 idle days, lost with DROP EXTENSION, or never
  issued here) is accepted as Kafka 3.7 accepts it (KIP-360): its batch is written at
  whatever sequence it carries, the producer is recorded at the epoch it presents, and its
  later batches are deduplicated and sequence-checked from there, on every partition.
  InitProducerId never hands out a producer id that is recorded this way.
- ✅ Producer ids are never reused by one installation, and start at the time the
  extension was created (milliseconds since the Unix epoch), so an extension dropped and
  created again issues ids above the earlier installation's (unless that one issued
  more ids than milliseconds passed between its creation and the new one's). A producer
  id that a producer unknown to the broker presents is outside this guarantee: it is the
  client's.
- **Deviation:** the epoch is kept per producer, not per partition: a batch at an epoch
  older than the producer's current one is INVALID_PRODUCER_EPOCH on every partition.
  Kafka compares the epoch with the producer's state on the batch's partition only and
  accepts an older epoch on a partition where the producer has no state.
- Produce responses report `log_append_time_ms = -1` (topics use CreateTime)

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
- ✅ A transactional Produce is checked per partition, as Kafka 3.7 verifies the
  transactional batches of a request with its transaction coordinator: a partition whose
  batch is transactional is refused INVALID_PRODUCER_ID_MAPPING (49) when the
  transactional id is unknown or another producer id owns it, INVALID_PRODUCER_EPOCH (47)
  for another epoch, and INVALID_TXN_STATE (48) when the transaction does not hold the
  partition; a plain or idempotent batch in the same request is written as it would be
  without the transactional id (committed, outside the transaction). As in Kafka 3.7,
  transactional batches of two producer ids in one request fail it with
  INVALID_PRODUCER_ID_MAPPING on every partition, plain ones included, and nothing is
  written; and a non-transactional batch of a producer whose open transaction has written
  to the partition is INVALID_TXN_STATE (48), with or without a transactional id in the
  request, after the epoch and sequence checks (a partition the transaction only added
  takes it, committed). **Deviation:** transactional batches of one producer id at two
  epochs are checked per epoch (the current epoch's written, the other refused); Kafka
  verifies all of them with the epoch of whichever partition its request map yields first,
  so it refuses or accepts them all. AddPartitionsToTxn,
  AddOffsetsToTxn, TxnOffsetCommit and EndTxn answer another producer id, and a
  transactional id without state, INVALID_PRODUCER_ID_MAPPING too, as Kafka does. **Deviation:** a transactional id whose
  transaction is not Ongoing (none was started with AddPartitionsToTxn, or it is complete)
  refuses its transactional batches INVALID_TXN_STATE; Kafka answers so for a partition
  the transaction does not hold, which is the same case for every client.
- ✅ A fenced producer epoch (another epoch than the transactional id's current one) is
  answered by request version as Kafka 3.7 answers it (KIP-588): PRODUCER_FENCED (90) on
  AddPartitionsToTxn, AddOffsetsToTxn and EndTxn from v2 and on InitProducerId from v4,
  INVALID_PRODUCER_EPOCH (47) on the versions before, and INVALID_PRODUCER_EPOCH on every
  TxnOffsetCommit and Produce version
- ✅ InitProducerId refuses a transaction timeout <= 0 or above
  `pg_kafka.transaction_max_timeout_ms` (Kafka's `transaction.max.timeout.ms`, default
  900000) with INVALID_TRANSACTION_TIMEOUT (50)
- ✅ InitProducerId with the producer id and epoch a client presents (v3+, KIP-360), as
  Kafka 3.7 answers it: the transactional id's current producer at its current epoch is
  bumped to the next epoch; a retry of that bump (the epoch before it) gets the same
  epoch back without another bump; any other presented id or epoch (a fenced zombie) is
  fenced (PRODUCER_FENCED, INVALID_PRODUCER_EPOCH before v4) and changes nothing. A presented id without an epoch, or the
  reverse, is INVALID_REQUEST (42). An InitProducerId presenting none (-1/-1) fences
  the previous producer, after which its retry is fenced too. An idempotent producer
  always gets a new producer id. **Deviation:** a retry of a re-init that moved the
  producer to a new id (epoch 32767 exhausted) is PRODUCER_FENCED; Kafka also
  remembers the previous producer id.
- ✅ The transaction timeout (`transaction.timeout.ms`) runs from the start of each
  transaction, as Kafka's does; the timeout sweep aborts an expired one and fences its
  producer with the next epoch (a new producer id when the epoch is exhausted)
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

- ✅ Kafka 3.7's JoinGroup checks: a member whose protocol type differs from the group's, that
  shares no protocol with every other member, or that sends an empty protocol list (or an
  empty protocol type for a new group) gets INCONSISTENT_GROUP_PROTOCOL and the group is
  untouched; a JoinGroup with a non-empty member id the group does not know (evicted, or lost in
  a worker restart) gets UNKNOWN_MEMBER_ID and the client rejoins with an empty id. The group
  protocol is chosen by Kafka's member vote (each member votes for its most preferred protocol
  that all members support; ties go to the leader's preference, where Kafka's order is
  unspecified). JoinGroup error responses before v7 carry protocol_name "" (non-nullable there).
- ✅ OffsetCommit follows Kafka 3.7: generation -1 (simple consumers, `kafka-consumer-groups
  --reset-offsets`) is accepted only for an unknown or Empty group (any member id); a group with
  members answers UNKNOWN_MEMBER_ID to a non-member and ILLEGAL_GENERATION to a wrong
  generation (also -1 from a member); a generation >= 0 for a group the coordinator does not
  know gets ILLEGAL_GENERATION. (TxnOffsetCommit keeps its own rules.)
- ✅ Groups known only by their committed offsets (none of their members joined since the
  worker started, e.g. after a restart, or the group was reclaimed from memory) are reported by
  ListGroups and DescribeGroups as Empty groups with protocol type "consumer", so
  `kafka-consumer-groups --list/--describe` shows them with offsets and lag. (Kafka keeps the
  group's actual protocol type; an offsets-only group here is always "consumer", which also
  covers groups that only ever committed as simple consumers, for which Kafka reports "".)
- Resource bounds (deviation: Kafka's `group.max.size` defaults to unlimited and it has no
  group-count cap): `pg_kafka.group_max_size` (default 1000) members per group, beyond which a
  new member gets GROUP_MAX_SIZE_REACHED; `pg_kafka.max_consumer_groups` (default 100000) groups
  in memory, beyond which the least recently active Empty group is forgotten (its offsets stay)
  or, if none is Empty, a JoinGroup for a new group gets COORDINATOR_NOT_AVAILABLE; a member's
  protocol list is limited to 64 protocols and 1 MiB (Kafka bounds it by the 1 MiB
  `__consumer_offsets` record), beyond which JoinGroup gets INVALID_REQUEST.

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
- ✅ Record headers returned to consumers
- ✅ Empty fetch responses (returns empty bytes)
- ✅ Partition watermarks (high watermark, log start offset)
- ✅ Byte-budget fetch sizing — small-message fetches fill `max_bytes` via bounded
  iterative queries instead of capping at a fixed row count
- ✅ Request-level `max_bytes` (KIP-74): partitions are filled in request order until
  the response budget is spent; the first partition with data always returns at least
  one message, later partitions return only what still fits (sizes are estimated as
  key + value + 64 bytes per record, the same accounting `partition_max_bytes` uses)
- ✅ Long polling (max_wait_ms/min_bytes support). A waiting fetch is re-run only
  when a Produce to one of its partitions (or, for `read_committed`, any transaction
  end: EndTxn, an abort by fencing InitProducerId or by the transaction timeout)
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

**Deviation: server-side read_committed filtering**

Real Kafka returns *all* records (including aborted ones) to a `read_committed`
consumer along with control batches and an `aborted_transactions` list, and the
client filters aborted ranges itself (the wire contract for Fetch ≥ v4).
pg_kafka instead filters **server-side**: pending/aborted rows are excluded by
the fetch query, results are clamped to the Last Stable Offset, and the
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
  the end and the lag goes to 0. An open transaction still blocks at the LSO.
- Fetched records are re-encoded, grouped into one RecordBatch per run of consecutive
  offsets written by the same producer id/epoch. Batches of transactional rows carry their producer
  id/epoch and the transactional flag; other batches `producer_id = -1`. The base
  sequence is sent as -1, `partition_leader_epoch` is 0, and batch boundaries may differ
  from the original produce.
- The data a client receives is already clean, so read_committed correctness holds for
  clients that do their own abort filtering too (verified with the Java consumer and
  librdkafka); what differs from Kafka is the byte-level batch layout and that
  `read_uncommitted` consumers do not see aborted records once the cleanup deleted them.
- Headers are stored as an ordered array of pairs, duplicate keys included, but a fetch
  returns duplicate header keys collapsed to the last value, in the position of the first:
  the kafka-protocol record type the fetch encodes with holds headers in a map. Kafka's
  wire format permits duplicate keys.

Related minor deviations: `throttle_time_ms` is always 0 (no quota system) and
`log_append_time_ms` is always -1 (LogAppendTime timestamp type is not
honored; producer CreateTime timestamps are preserved).

### 8. ListOffsets ✅ Implemented

**Status:** Fully implemented
**Kafka Behavior:** Returns offsets by timestamp or special values
**pg_kafka Behavior:** Supports earliest (-2), latest (-1), max timestamp (-3) and timestamp lookup

**Implementation:**
- ✅ Returns earliest offset (first message in partition; -4 EARLIEST_LOCAL is the same)
- ✅ Returns latest offset: the high watermark, or the last stable offset for a
  `read_committed` request
- ✅ MAX_TIMESTAMP (-3, KIP-734): the offset and timestamp of the record with the largest
  timestamp (the lowest offset among equal ones). Kafka 3.7.0 itself answers the
  last offset of that record's batch (KAFKA-16310, fixed in later releases)
- ✅ Timestamp-based lookup — returns the earliest offset whose record timestamp is ≥ the requested time (record timestamps are persisted)
- ✅ Timestamp lookups are bounded like Kafka's: a match at or past the high watermark
  (`read_committed`: the LSO) is offset -1; aborted records are never a match
- ✅ A partition the topic does not have is UNKNOWN_TOPIC_OR_PARTITION

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
- ✅ Topics have 1 to `pg_kafka.max_partitions_per_topic` partitions (default 10000, at most 100000; set via CreateTopics/CreatePartitions or, for auto-created topics, `pg_kafka.default_partitions` capped at the maximum). A larger count is refused with INVALID_PARTITIONS and `kafka.topics` carries a `CHECK (partitions BETWEEN 1 AND 100000)`; a stored count outside that range (only possible by bypassing the constraint) is reported by Metadata as UNKNOWN_SERVER_ERROR for that topic and ignored by the server-side assignors instead of being allocated, so a corrupt count cannot make the worker allocate without bound. Kafka itself has no per-topic cap.
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

- **CreateTopics ignores topic configs.** Per-topic `configs` (e.g. `retention.ms`) and manual replica assignments are accepted but not applied. Set `retention.ms` afterwards with IncrementalAlterConfigs.
- **CreateTopics validation follows Kafka 3.7** (checked against a real 3.7 broker): a name repeated in one request gets a single INVALID_REQUEST "Duplicate topic name." result and is not created; a name that collides with an existing topic once `.` and `_` are treated as equal (`a.b` vs `a_b`) gets INVALID_TOPIC_EXCEPTION "Topic 'a_b' collides with existing topic: a.b"; `replication_factor` must be 1 or -1 (the default): 0 or below -1 gets INVALID_REPLICATION_FACTOR "Replication factor must be larger than 0, or -1 to use the default value.", above 1 gets INVALID_REPLICATION_FACTOR (single broker), and the response reports 1. Remaining differences: two colliding names created in the *same* request are refused here (Kafka 3.7 creates both, checking collisions only against existing topics), and topics auto-created by Metadata/Produce are not collision-checked (that would cost a query per produce).
- **CreateTopics default partition count.** `num_partitions = -1` ("broker default", what most admin clients send) creates a topic with `pg_kafka.default_partitions` partitions (capped at `pg_kafka.max_partitions_per_topic`), the same default auto-created topics get. Any other value below 1, such as `0`, is rejected with INVALID_PARTITIONS, as is any count above `pg_kafka.max_partitions_per_topic` (CreateTopics and CreatePartitions; Kafka has no such cap).
- **DeleteGroups** deletes an Empty group's committed offsets (also a group known only by its offsets, see §5) and answers GROUP_ID_NOT_FOUND for a group that has neither members nor offsets, as Kafka does.
- **CreatePartitions** does not itself trigger a rebalance; consumers pick up the new partitions on their next metadata refresh.
- **DescribeConfigs** supports only TOPIC resources and reports only `retention.ms` and `cleanup.policy` (always `delete`; there is no log compaction). BROKER and other resource types return INVALID_REQUEST. Versions v1-v4 only (the kafka-protocol crate does not implement v0).
- **IncrementalAlterConfigs** accepts only SET/DELETE of `retention.ms` on TOPIC resources. Any other key or operation (APPEND/SUBTRACT) fails the whole resource with INVALID_CONFIG rather than being silently accepted.
- **AlterConfigs (33)** is not implemented.

## API Version Support

### Supported APIs (<!--gen:api-count-->28<!--/gen--> total)

These ranges are both advertised in ApiVersions and enforced on inbound requests (each API's `MIN_VERSION`/`MAX_VERSION` in `src/kafka/api/`); a request above its range gets UNSUPPORTED_VERSION in the requested version (see "Error responses" above).

| API Key | Name | Versions | Status |
|---------|------|----------|--------|
| 0 | Produce | <!--gen:api-versions:Produce-->3-9<!--/gen--> | ✅ Full support (with key-based routing) |
| 1 | Fetch | <!--gen:api-versions:Fetch-->4-11<!--/gen--> | ✅ Full support (v12+ not advertised; v13+ uses topic IDs) |
| 2 | ListOffsets | <!--gen:api-versions:ListOffsets-->1-7<!--/gen--> | ✅ Implemented (incl. timestamp lookup) |
| 3 | Metadata | <!--gen:api-versions:Metadata-->0-10<!--/gen--> | ✅ Full support (v10 `topic_id` is the zero UUID) |
| 8 | OffsetCommit | <!--gen:api-versions:OffsetCommit-->2-8<!--/gen--> | ✅ Full support |
| 9 | OffsetFetch | <!--gen:api-versions:OffsetFetch-->1-7<!--/gen--> | ✅ Full support |
| 10 | FindCoordinator | <!--gen:api-versions:FindCoordinator-->0-3<!--/gen--> | ✅ Full support |
| 11 | JoinGroup | <!--gen:api-versions:JoinGroup-->0-7<!--/gen--> | ✅ Full support |
| 12 | Heartbeat | <!--gen:api-versions:Heartbeat-->0-4<!--/gen--> | ✅ Full support |
| 13 | LeaveGroup | <!--gen:api-versions:LeaveGroup-->0-4<!--/gen--> | ✅ Full support |
| 14 | SyncGroup | <!--gen:api-versions:SyncGroup-->0-4<!--/gen--> | ✅ Full support |
| 15 | DescribeGroups | <!--gen:api-versions:DescribeGroups-->0-5<!--/gen--> | ✅ Full support |
| 16 | ListGroups | <!--gen:api-versions:ListGroups-->0-4<!--/gen--> | ✅ Full support |
| 17 | SaslHandshake | <!--gen:api-versions:SaslHandshake-->0-1<!--/gen--> | ✅ Network thread; v0 is advertised (librdkafka requires it) but answered UNSUPPORTED_VERSION, see "Network Security Posture" |
| 18 | ApiVersions | <!--gen:api-versions:ApiVersions-->0-3<!--/gen--> | ✅ Full support |
| 19 | CreateTopics | <!--gen:api-versions:CreateTopics-->2-5<!--/gen--> | ✅ Supported (topic configs ignored, see §13) |
| 20 | DeleteTopics | <!--gen:api-versions:DeleteTopics-->1-4<!--/gen--> | ✅ Full support |
| 21 | DeleteRecords | <!--gen:api-versions:DeleteRecords-->0-2<!--/gen--> | ✅ Full support |
| 22 | InitProducerId | <!--gen:api-versions:InitProducerId-->0-4<!--/gen--> | ✅ Full support |
| 24 | AddPartitionsToTxn | <!--gen:api-versions:AddPartitionsToTxn-->0-3<!--/gen--> | ✅ Full support |
| 25 | AddOffsetsToTxn | <!--gen:api-versions:AddOffsetsToTxn-->0-3<!--/gen--> | ✅ Full support |
| 26 | EndTxn | <!--gen:api-versions:EndTxn-->0-3<!--/gen--> | ✅ Full support |
| 28 | TxnOffsetCommit | <!--gen:api-versions:TxnOffsetCommit-->0-3<!--/gen--> | ✅ Full support |
| 32 | DescribeConfigs | <!--gen:api-versions:DescribeConfigs-->1-4<!--/gen--> | ✅ Minimal (TOPIC resources, see §13) |
| 36 | SaslAuthenticate | <!--gen:api-versions:SaslAuthenticate-->0-2<!--/gen--> | ✅ PLAIN, SCRAM-SHA-256, SCRAM-SHA-512; `session_lifetime_ms` = 0 (no re-authentication) |
| 37 | CreatePartitions | <!--gen:api-versions:CreatePartitions-->0-2<!--/gen--> | ✅ Full support |
| 42 | DeleteGroups | <!--gen:api-versions:DeleteGroups-->0-2<!--/gen--> | ✅ Full support |
| 44 | IncrementalAlterConfigs | <!--gen:api-versions:IncrementalAlterConfigs-->0-1<!--/gen--> | ✅ Minimal (`retention.ms` only, see §13) |

**Note:** OffsetFetch v8+ uses a different (multi-group) response format. We support <!--gen:api-vversions:OffsetFetch-->v1-v7<!--/gen-->.

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

## Acknowledgment Durability

**Status:** Kafka-conformant as of the response-after-commit barrier.

For `acks >= 1`, a produce (or offset-commit/admin) response is delivered to
the client **only after the backing PostgreSQL transaction has committed** —
the ack implies the write is durable (WAL-flushed) and visible to any other
PostgreSQL connection. (`acks=0` remains
fire-and-forget with no response frame and no durability claim — see §1.)

## Shadow Forwarding Delivery Semantics

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
  not re-claimed at lease expiry — the DB thread tracks dispatched
  forwards and the outbox claim skips them until the ack resolves or the
  entry ages out (`INFLIGHT_EVICT_AFTER`, 150 s, sized above the
  forwarder's 120 s delivery timeout). Duplicates therefore occur only on genuine ack
  *loss*, not ack *latency*. Local storage and `kafka.shadow_tracking`
  finalization remain exactly-once in all modes.

## Network Security Posture

**Status:** Authentication available (TLS, SASL); **authorization not implemented**. Full
reference: [AUTHENTICATION.md](AUTHENTICATION.md).

- **Default:** one PLAINTEXT listener on `127.0.0.1:9092` (`pg_kafka.host`, `pg_kafka.port`,
  `pg_kafka.security_protocol = 'PLAINTEXT'`) with no authentication, exactly as before. Any
  client that can open a TCP connection to a PLAINTEXT listener can read and write any topic.
- **Listener security protocols** as in Kafka: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`,
  for the primary listener and for any `pg_kafka.additional_listeners`
  (`'SASL_SSL://0.0.0.0:9093@broker.example.com:9093,...'`). Each listener advertises its own
  address in Metadata/FindCoordinator: the part after `@` (`pg_kafka.advertised_address` for the
  primary listener), else its bind host (localhost for 0.0.0.0 / ::). TLS is rustls; mutual TLS when `pg_kafka.ssl_ca_file` is set.
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
  CA), for SASL/PLAIN offered on an exposed SASL_PLAINTEXT listener, and for an exposed
  listener that would advertise a loopback address.
- **Network edge limits:** connections are capped per listener
  (`pg_kafka.max_connections`, default 1024; Kafka's cap is broker-wide with per-listener
  overrides) and per IP address across listeners (`pg_kafka.max_connections_per_ip`, default 256,
  where Kafka's default is unlimited); over a cap the socket is closed at accept. Idle
  connections close after `pg_kafka.connections_max_idle_ms` (600000, as Kafka), a request frame
  must arrive within `pg_kafka.request_read_timeout_ms` (60000; Kafka has no such deadline) of its
  first byte, and the pre-authentication phase (TLS + SASL) has 10 s from accept. Memory for an
  incoming frame is reserved as its bytes arrive, under a 512 MiB budget for frames over 1 MiB
  (Kafka's `queued.max.request.bytes` is unbounded by default); a connection whose frame would
  exceed the budget is closed. Responses are not size-capped (they are bounded by the request's
  `max_bytes`). `TCP_NODELAY` is set on every socket. SASL credential lookups are rate limited
  per IP address (10/s, burst 20; Kafka has none). A connection closed for a bad frame, a timeout
  or an acks=0 error first delivers the responses of every request it read before (up to 30 s).
  Decoded Produce records waiting for the DB thread hold at most 512 MiB together, and decodes
  of more than 2 MiB run off the I/O threads, at most two at a time; a request that finds no
  room waits for it, up to 10 s, and is then answered REQUEST_TIMED_OUT (see §2). A request's
  frame counts against the frame budget while it waits, as an incomplete frame does (one the
  exhausted budget cannot take is answered REQUEST_TIMED_OUT at once).

## Compatibility Testing

### Tested Clients

| Client | Version | Status | Notes |
|--------|---------|--------|-------|
| kcat | 1.7.0+ | ✅ Works | Producer and consumer tested |
| rdkafka (Rust) | 0.36+ | ✅ Works | Full E2E test suite (`kafka_test/`) |
| Apache Kafka Java clients and tools | 3.7.0 | ✅ Checked in CI | `interop/run-java-checks.sh`: verifiable producer/consumers through rebalances, consumer-groups, get-offsets, transactions (perf-test, TransactionalMessageCopier), SASL SCRAM/PLAIN, Kafka Streams; checks still failing are marked XFAIL with the defect |

Every behavioural difference from Apache Kafka 3.7 that `interop/diff/run.py` (the differential
harness: the same raw requests to pg_kafka and to Kafka, responses compared) observes is either
documented in this file (the harness cites the section) or listed there as a known bug.

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

> **Shadow Mode is implemented** (Phase 11: external Kafka forwarding, per-topic config, SASL/SSL). See `KAFKA_PROTOCOL_COVERAGE.md` and the `SHADOW_MODE_*` docs; delivery semantics are under "Shadow Forwarding Delivery Semantics" above.

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
**API Coverage:** <!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen-->
**Test Status:** unit + property + E2E, CI-gated; counts in [TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary)
