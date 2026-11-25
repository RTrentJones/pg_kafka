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
  reaches them first), counted from when the request's frame was read, whatever it waited for
  before its records (room for the rest of the decoded request, below); past it the request is
  answered REQUEST_TIMED_OUT (7) on every partition and nothing is written. Clients retry it (an idempotent producer keeps its order;
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
- The decoding time of a request is bounded too, checked between 16 KiB chunks of compressed
  input (inside a zstd frame too, between its blocks) and between partitions, for every
  codec: compressed input can cost much while decoding to nothing (gzip of empty
  dynamic-Huffman blocks costs some 200 ns per byte, so a 94 MB batch of them would decode
  for 15 s). The time counted is the decoding thread's CPU time, so a busy machine slows a
  decode down without refusing it. A decode on a network I/O thread stops after 20 ms and
  decodes again off the I/O threads (a legitimate 1 MB request of gzip-compressed 1 KB
  records of hex digits, compressed about 1.75:1, decodes in about 5 ms; 6 MiB of their
  output, the work limit, in about 50 ms). Off them it stops after 2 s: the partition
  decoding and those after it are answered REQUEST_TIMED_OUT (7), which clients retry (a
  request at the 100 MB limit, 85 MB of such records, decodes in about 0.5 s). Measured on a
  release build; Kafka has no such limit.
- As in Kafka 3.7 (`KafkaApis.handleProduceRequest` collects a request's partitions in maps),
  a partition named more than once in a request, in one topic entry or in several, is
  answered once and written at most once: the last of its entries that
  `ProduceRequest.validateRecords` accepts (one RecordBatch v2, no zstd before v7) is decoded
  and written, the others are neither; if any of its entries fails that check, the last such
  failure is its answer, whether or not another entry was written (a valid entry and one of
  two batches, in either order: the valid one written, INVALID_RECORD answered). The first
  batch of every entry, kept or not, is read before (a transactional batch without a
  transactional id, a batch whose length cannot be read, fail the whole request). Checked
  against a 3.7 broker. Differences: a partition of a topic that does not exist is
  auto-created by its written entry while answered UNKNOWN_TOPIC_OR_PARTITION for its
  failed one (Kafka creates nothing).
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
- ✅ Sequence checks are serialized by the worker's single database thread (every batch of
  every producer is checked and recorded there, one at a time), not by a lock
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
- **Deviation:** a producer id that a transactional id owns takes no epoch it presents
  itself: any batch of it, transactional or not, at an epoch other than its current one is
  INVALID_PRODUCER_EPOCH, a higher epoch too (its epoch moves only with InitProducerId).
  Kafka 3.7 (`ProducerAppendInfo.checkProducerEpoch`) refuses only a lower epoch, and
  accepts a non-transactional batch at a higher one as the producer's new epoch. The one
  exception is a partition its open transaction has written to, where a higher epoch is
  answered as Kafka answers it (§4).
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
  written; only partitions that pass the metadata check count (a batch to a partition
  outside its topic is UNKNOWN_TOPIC_OR_PARTITION and the owner's batches are written; a
  topic that did not exist does not count either, but Produce auto-creates it (§10) and then
  refuses the other producer's batch there on its own, 49, where Kafka answers 3); and a non-transactional batch of a producer whose open transaction has written
  to the partition is INVALID_TXN_STATE (48), with or without a transactional id in the
  request, after the epoch and sequence checks (one at a bumped epoch must start at
  sequence 0, OUT_OF_ORDER_SEQUENCE_NUMBER otherwise, and moves no stored epoch). On a
  partition the transaction only added, a non-transactional batch at the current epoch is
  written, committed; one at a bumped epoch is INVALID_PRODUCER_EPOCH (§3: a producer id
  that a transactional id owns takes no epoch it presents; Kafka 3.7 writes it). **Deviation:** transactional batches of one producer id at two
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
  always gets a new producer id. While the transactional id has a transaction Ongoing, an
  InitProducerId that would bump it (the current epoch presented, the epoch before the last
  bump, or none) aborts that transaction under the next epoch and is answered
  CONCURRENT_TRANSACTIONS (51), as in Kafka 3.7; the client retries, and a retry presenting
  the same epoch is then fenced while one presenting none gets the epoch after the fenced one.
  (The Java client and librdkafka end a transaction with EndTxn before they bump its epoch,
  and retry CONCURRENT_TRANSACTIONS.) **Deviation:** a retry of a re-init that moved the
  producer to a new id (epoch 32767 exhausted) is PRODUCER_FENCED; Kafka also
  remembers the previous producer id. **Deviation:** an InitProducerId presenting the
  current producer id with any other epoch (older than the one before the last bump, or
  newer than the current one) while a transaction is Ongoing is PRODUCER_FENCED and leaves
  that transaction open; Kafka 3.7 checks only the producer id there and aborts the
  owner's transaction (CONCURRENT_TRANSACTIONS) on behalf of such a zombie.
- ✅ The transaction timeout (`transaction.timeout.ms`) runs from the start of each
  transaction, as Kafka's does; the timeout sweep aborts an expired one and fences its
  producer with the next epoch (a new producer id when the epoch is exhausted)
- ✅ AddOffsetsToTxn includes consumer offsets in transaction: it registers the group in
  the open transaction (`kafka.txn_groups`, cleared with `kafka.txn_partitions`)
- ✅ EndTxn commits or aborts atomically. EndTxn of a transaction that added nothing since
  InitProducerId (Empty) is INVALID_TXN_STATE (48), commit or abort, as in Kafka 3.7 (the Java
  client and librdkafka send no EndTxn for such a transaction); a retried EndTxn whose
  transaction already ended the way it asks is NONE
- ✅ TxnOffsetCommit commits offsets within transaction context; for a group the
  transaction did not add with AddOffsetsToTxn it is INVALID_TXN_STATE (48) on every
  partition and version and stores nothing, as in Kafka 3.7. **Deviation:** Kafka verifies
  the group's `__consumer_offsets` partition, so a group sharing that partition with an
  added one is accepted there; pg_kafka checks the group itself. As in Kafka 3.7, the
  transaction is checked before the group member: a fenced or unknown producer, a
  transaction that is not ongoing and a group not added win over a stale generation or an
  unknown member id (UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION only once the transaction checks
  out). Whatever refuses the commit, a partition that does not exist is
  UNKNOWN_TOPIC_OR_PARTITION (Kafka filters those out before it asks the coordinator) and
  the refusal's code goes only to the partitions that exist. The member checks are Kafka
  3.7's (checked against a 3.7 broker): a group without members (unknown, or Empty) takes
  generation -1 whatever the member id; a group with members takes generation -1 only with
  an empty member id, and answers UNKNOWN_MEMBER_ID to a member id it does not know and
  ILLEGAL_GENERATION to one of its members; a generation >= 0 must be the current one, of a
  member of the group. The group's state is not checked: a member's current-generation
  commit is accepted while the group is PreparingRebalance or CompletingRebalance (where a
  plain OffsetCommit gets REBALANCE_IN_PROGRESS)
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
  leaves, or the sweep evicts the members that did not sync, below), and
  UNKNOWN_MEMBER_ID if the member itself is removed. Like a waiting joiner, a member
  whose SyncGroup is held is exempt from its session timeout. Server-side
  assignment (a leader sending empty assignments) is computed only for the leader.
- ✅ As in Kafka 3.0+ (`pendingSyncMembers`, checked against a 3.7 broker): every member of
  a new generation must send its SyncGroup within the group's rebalance timeout of the
  join's completion. The sweep removes those that have not, though they heartbeat and
  whether the group is still CompletingRebalance or already Stable (the leader's SyncGroup
  came), and the others rebalance (their Heartbeat gets REBALANCE_IN_PROGRESS).
- ✅ DescribeGroups reports the protocol and each member's metadata and assignment only for a
  Stable group; in PreparingRebalance and CompletingRebalance the protocol is "" and the
  members carry neither, as Kafka 3.7's `GroupMetadata.summary` does. **Deviation:** every
  member's `client_host` is "localhost"; Kafka reports the member's address (for example
  "/127.0.0.1"). The DB-thread handler is not given the connection's peer address.
- ✅ ListGroups' `states_filter` (v4) matches state names case-sensitively, as Kafka 3.7
  does (`kafka-consumer-groups --state` sends the canonical names).
- ✅ FindCoordinator answers a `key_type` other than 0 (group) and 1 (transaction)
  INVALID_REQUEST (42), as Kafka 3.7 does.

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
  As in Kafka 3.7 (checked against a 3.7 broker), a JoinGroup error answered at once carries the
  request's member id: INVALID_GROUP_ID, INVALID_SESSION_TIMEOUT, UNKNOWN_MEMBER_ID and
  INCONSISTENT_GROUP_PROTOCOL of a known member echo it (a new member's is ""), and
  GROUP_MAX_SIZE_REACHED, which only a new member gets, carries none. A held JoinGroup answered
  later with an error carries no member id, as Kafka's does: UNKNOWN_MEMBER_ID when its member
  is removed while it waits (a LeaveGroup naming it, or DeleteGroups), and the
  REBALANCE_IN_PROGRESS that answers a held JoinGroup replaced by the member's resend (Kafka
  leaves the replaced request unanswered).
- ✅ OffsetCommit follows Kafka 3.7: generation -1 (simple consumers, `kafka-consumer-groups
  --reset-offsets`) is accepted only for an unknown or Empty group (any member id); a group with
  members answers UNKNOWN_MEMBER_ID to a non-member and ILLEGAL_GENERATION to a wrong
  generation (also -1 from a member); a generation >= 0 for a group without members gets
  UNKNOWN_MEMBER_ID when the group has committed offsets (Kafka holds it as an Empty group,
  also when none of its members joined since the worker started) and ILLEGAL_GENERATION when
  it has neither members nor offsets. A refused commit answers a partition that does not
  exist UNKNOWN_TOPIC_OR_PARTITION, its code going only to the partitions that exist, as Kafka
  filters those out before it asks the coordinator. (TxnOffsetCommit keeps its own rules.)
- ✅ The empty group id follows Kafka 3.7 (`GroupCoordinator.isValidGroupId`, checked against a
  3.7 broker): OffsetCommit, OffsetFetch, DescribeGroups and DeleteGroups accept `""` like any
  other group id, and FindCoordinator and AddOffsetsToTxn take it too; JoinGroup, SyncGroup,
  Heartbeat, LeaveGroup and TxnOffsetCommit answer INVALID_GROUP_ID (24) and change nothing.
  JoinGroup's answer echoes the request's member id with generation -1; LeaveGroup v3+ answers
  at the top level with no member results; TxnOffsetCommit answers every partition, before
  any producer or transaction check, except a partition that does not exist, which is
  UNKNOWN_TOPIC_OR_PARTITION.
- ✅ Groups known only by their committed offsets (none of their members joined since the
  worker started, e.g. after a restart, or the group was reclaimed from memory) are reported by
  ListGroups and DescribeGroups as Empty groups with protocol type "consumer", so
  `kafka-consumer-groups --list/--describe` shows them with offsets and lag. (Kafka keeps the
  group's actual protocol type; an offsets-only group here is always "consumer", which also
  covers groups that only ever committed as simple consumers, for which Kafka reports "".) A
  group with only a pending transactional offset (a TxnOffsetCommit whose transaction is still
  open, which in Kafka 3.7 creates the group) is such a group too, for DeleteGroups as well:
  deleting it, or any group, also drops its pending transactional offsets, so the
  transaction's commit stores none for it, as in Kafka. (When that transaction aborts, such a
  group is gone at once; Kafka keeps it Empty until its ~10-minute offsets cleanup.)
- Resource bounds (deviation: Kafka's `group.max.size` defaults to unlimited and it has no
  group-count cap): `pg_kafka.group_max_size` (default 1000) members per group, beyond which a
  new member gets GROUP_MAX_SIZE_REACHED; `pg_kafka.max_consumer_groups` (default 100000) groups
  in memory, beyond which the least recently active Empty group is forgotten (its offsets stay)
  or, if none is Empty, a JoinGroup for a new group gets COORDINATOR_NOT_AVAILABLE; a member's
  protocol list is limited to 64 protocols and 1 MiB (Kafka bounds it by the 1 MiB
  `__consumer_offsets` record), beyond which JoinGroup gets INVALID_REQUEST; and
  `pg_kafka.max_group_memory` (default 128MB) bounds the member state of all groups together
  (member ids, protocols and their metadata, assignments and the copies kept for the sticky
  assignor; a group's copies of its leader's id and of its protocol name count as the longest
  id and protocol name of its members, since a leave can make any of them the leader or
  complete a join phase with any of their protocols, and nothing is refused then). Kafka has no such bound (its heap is the limit), but without it the caps above
  still allowed about 1e14 bytes, held up to the 30-minute session timeout by members that
  never sync; an allocation failure aborts the worker and sends Postgres into crash
  recovery. A JoinGroup whose member state does not fit, or a leader SyncGroup whose
  assignments do not, gets COORDINATOR_NOT_AVAILABLE (15), retriable, as for a full
  `max_consumer_groups`: the memory frees as members leave or time out, and before refusing,
  the least recently active Empty groups (no members; their committed offsets stay, so they
  remain listed as Empty groups) are forgotten until the state fits, as at the group-count cap,
  unless forgetting all of them would not make room. (Kafka removes an Empty group without
  offsets at its ~10-minute offsets cleanup; an Empty group here otherwise stays in memory for
  24 hours.) The budget is the broker's, not the group's (GROUP_MAX_SIZE_REACHED names the group's size and is fatal to
  the Java consumer). A refused JoinGroup creates no group; a refused SyncGroup stores
  nothing and the group stays in its sync phase. A rejoin that holds no more than before is
  always admitted. `rebalance_timeout_ms` is not bounded, as in Kafka.

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
- ✅ Supports offset metadata field: metadata longer than 4096 characters (Kafka's default
  `offset.metadata.max.bytes`, a length in UTF-16 code units) is answered
  OFFSET_METADATA_TOO_LARGE (12) on its partition by OffsetCommit and TxnOffsetCommit, and
  the request's other partitions are committed, as in Kafka 3.7. OffsetFetch answers ""
  for metadata committed as null and for a partition without an offset, as Kafka does
- ✅ OffsetFetch of a topic that does not exist (never created, or deleted) answers its
  partitions offset -1 with NONE, as Kafka 3.7 does on every version, like any partition
  without a committed offset (UNKNOWN_TOPIC_OR_PARTITION there made the Java consumer's
  poll() throw for a consumer that assign()s such a topic)
- **Deviation:** the committed leader epoch (OffsetCommit v6+) is not stored, and
  OffsetFetch v5+ answers `committed_leader_epoch` -1; Kafka echoes the committed one. A
  client that sees -1 skips its leader-epoch validation of the fetched position (KIP-320),
  which on a single broker whose leader epoch never changes validates nothing anyway.

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
- ✅ Partition watermarks (high watermark, log start offset) and the last stable offset (the
  first offset of an open transaction, else the high watermark), reported for both isolation
  levels as Kafka does; a read_uncommitted consumer still reads to the high watermark
- ✅ Byte-budget fetch sizing — small-message fetches fill `max_bytes` via bounded
  iterative queries instead of capping at a fixed row count
- ✅ Request-level `max_bytes` (KIP-74): partitions are filled in request order until
  the response budget is spent, each up to its own `partition_max_bytes`; the first
  partition with data always returns at least one message, later partitions return only
  what still fits. Both limits count what a record adds to the response: its key, value and
  header keys and values uncompressed, plus 84 bytes per record and 10 per header for its
  framing (the most an uncompressed record batch spends on them: a record alone in its batch,
  as read_committed skipping aborted offsets or alternating producers leave records, carries
  the whole 61-byte batch header and up to 23 bytes of its own), so the records returned stay
  within both limits, but for the first one above; the database reads (and holds) no more than
  that of a partition, and a later partition whose first record is larger than its limit does
  not read that record (sizes are stored with each record at Produce, so a fetch reads the key,
  value and headers only of the records it returns). Kafka counts
  the bytes of the record batches as stored, compressed when the producer compressed them,
  so a compressible topic returns fewer records per Fetch than Kafka would, never more
  than the limits
- ✅ Broker-side response cap (`pg_kafka.fetch_max_bytes`, default 55 MiB, Kafka's broker
  `fetch.max.bytes`): a request's `max_bytes` is lowered to it and its `min_bytes` to the
  result, as Kafka 3.7's `KafkaApis.handleFetchRequest` does, so a `max_bytes` of `i32::MAX`
  returns at most the cap; each partition reads up to its `partition_max_bytes` within what is
  left, and the first record of the first partition with data is returned whole even when it
  is larger (KIP-74; Kafka returns the first record batch). A long poll answers once the bytes
  available reach `min_bytes`, counted as Kafka 3.7's `DelayedFetch` counts them: per
  partition, what lies between its fetch offset and its readable end, at most its
  `partition_max_bytes` (a partition that returned everything up to its readable end counts
  the bytes it returned; one cut short or not read counts its whole `partition_max_bytes`). `max_wait_ms` is not capped, as in
  Kafka; a connection waiting on one is closed by the idle timeout once responses behind it
  wait unread (Network Security Posture)
- ✅ Long polling (max_wait_ms/min_bytes support). A waiting fetch is re-run only
  when a Produce to one of its partitions (or, for `read_committed`, any transaction
  end: EndTxn, an abort by fencing InitProducerId or by the transaction timeout)
  signals new data, at `max_wait_ms`, or at the slow safety-net interval
  `pg_kafka.fetch_poll_interval_ms` (default 1000 ms) — idle consumers do not
  generate repeated fetch work on the DB thread

**What's Missing:**
- ❌ Fetch sessions (always uses fetch session ID 0)
- At most 64 long-poll fetches may wait concurrently per connection; a fetch over that cap is answered immediately (possibly empty) instead of waiting for `max_wait_ms`
- At most 128 requests per connection may be in flight (read but not yet answered on the socket); beyond that the broker stops reading from the connection until responses are consumed (TCP backpressure), much as Kafka mutes a connection while it has an outstanding request. Kafka reads a connection's next request only once the previous response is sent, so it holds one response per connection; pg_kafka bounds what the in-flight requests' responses hold together by a per-connection budget of `pg_kafka.fetch_max_bytes`: a Fetch reserves its `max_bytes` before it reads records (a long poll before each attempt), a response counts at its encoded size once it arrives and is given back once written, and while the connection's unwritten responses hold more than the budget it reads no further request. Any other response's size is known only once it arrives, so the reader also stops while 8 requests other than Fetch and Heartbeat wait for their response (Kafka waits for one), and only a Fetch read while the connection holds nothing else and waits for no other response returns a first record larger than its limits (any other Fetch returns at most its `max_bytes` of records, and its first record once it reads alone). The oldest request whose response is not written never waits for room, so a connection holds at most the budget, plus that request's reservation, plus the responses of at most 8 requests other than Fetch, plus one first record larger than its Fetch's limits (and each Fetch response's partition entries, which `max_bytes` does not count, as in Kafka)

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
  the end and the lag goes to 0. An open transaction still blocks at the LSO. The
  control batch counts against `partition_max_bytes` and `max_bytes` at its encoded size
  (about 80 bytes), as Kafka counts the abort markers it returns: the first partition of a
  response with data or such a batch returns it whatever its size, a later partition only
  when it fits what is left of both limits (otherwise nothing, and its consumer fetches
  the same offset again).
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
  (`read_committed`: the LSO) is offset -1
- ⚠️ **Deviation:** aborted records are never a match of a timestamp or MAX_TIMESTAMP
  lookup, at either isolation level; the answer is the next record that matches. Kafka 3.7
  (`FileRecords.searchForTimestamp`, `LogSegment.findOffsetByTimestamp`) searches the batches
  without a transactional filter, so it can answer an aborted record's offset and timestamp.
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
**pg_kafka Behavior:** There is no broker-level switch. Topics are always auto-created on first produce. A Metadata request also auto-creates requested topics unless the client sends `allow_auto_topic_creation=false` (Metadata v4+), in which case a missing topic is reported as UNKNOWN_TOPIC_OR_PARTITION. An auto-created topic is answered at once with its partitions (Kafka 3.7 answers UNKNOWN_TOPIC_OR_PARTITION until its controller has created it, and the client retries).

As in Kafka 3.7 (checked against a 3.7 broker): a name Kafka does not accept (longer than 249 characters, a character outside `[a-zA-Z0-9._-]`, `.` or `..`) is never created; Metadata reports it INVALID_TOPIC_EXCEPTION with or without auto-creation, and Produce answers UNKNOWN_TOPIC_OR_PARTITION, as for any partition that does not exist. Metadata answers each distinct requested name once, however often the request repeats it.

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
- Idempotent/transactional producers (`producer_id >= 0`) cannot use partition -1: a batch's sequence numbers are tracked per partition, so splitting it by key would corrupt sequence tracking. Such a batch is rejected with UNKNOWN_TOPIC_OR_PARTITION (real Kafka does not route -1 at all and answers every -1 entry so; clients always pick the partition). So is a plain -1 batch in a topic of the request that also has an idempotent or transactional batch: its records would join that batch's partition and be written under its producer state (sequence-checked as part of it, or pending in its transaction). A routed batch is answered on the partitions it was written to, not on -1.

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
- **IncrementalAlterConfigs** accepts only SET/DELETE of `retention.ms` on TOPIC resources. Any other key or operation (APPEND/SUBTRACT) fails the whole resource with INVALID_CONFIG rather than being silently accepted. A resource that names one key twice is INVALID_REQUEST (42) "Error due to duplicate config keys", and then a SET, APPEND or SUBTRACT without a value is INVALID_REQUEST whatever the key; both are checked first, before the resource type and the topic lookup (an unknown topic is then INVALID_REQUEST, not UNKNOWN_TOPIC_OR_PARTITION), and nothing of the resource is applied, as in Kafka 3.7.
- **DescribeConfigs config types** are Kafka 3.7's: `retention.ms` LONG (5), `cleanup.policy` LIST (7).
- **AlterConfigs (33)** is not implemented.
- **Repeated entries follow Kafka 3.7** (checked against a 3.7 broker). OffsetCommit, TxnOffsetCommit, DeleteRecords, OffsetFetch and Fetch act on a partition the request names more than once a single time, with its last entry, and answer it once, under one entry per distinct topic; ListOffsets answers every entry of a repeated partition INVALID_REQUEST; DeleteTopics and CreatePartitions answer a repeated name with one INVALID_REQUEST "Duplicate topic name." entry; IncrementalAlterConfigs answers every entry of a repeated resource INVALID_REQUEST "Each resource must appear at most once."; DeleteGroups deletes and answers a repeated group once; DescribeConfigs and AddPartitionsToTxn answer every entry. Whatever a request repeats, its topics (and, for DescribeGroups/DeleteGroups, its groups' committed offsets) are looked up in one statement, so a small request cannot hold the DB thread, which serves every client, for one round trip per entry. Remaining difference: entries are answered in the order first named; Kafka answers merged entries in no particular order.

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

**Status:** At-least-once, in commit order per local partition, by design. Full description:
[SHADOW_MODE_FLOW_DIAGRAM.md](SHADOW_MODE_FLOW_DIAGRAM.md).

Shadow forwarding to an external Kafka cluster goes through a durable outbox
(`kafka.shadow_tracking`): a produce writes its records locally and, in the same transaction, one
pending outbox row per forwarded record. The worker's outbox tick claims due rows after the commit
and hands them to the forwarder on the network thread, which produces them with an idempotent
librdkafka producer and answers every record with an ack.

- **One path for sync and async.** `sync_mode` changes only when the client gets its response:
  a sync-mode response is held (by the forwarder, never by the worker) until its records are
  delivered or `pg_kafka.shadow_sync_timeout_ms` (default 5 s) passes; on timeout the records stay
  pending and are forwarded like any other. Both modes write the same outbox rows and are
  dispatched by the same claim. Nothing is sent before the local commit, so a rolled-back request
  never leaves a copy on the external broker. Some sync-mode responses are not held (an
  idempotent producer's retry of an appended batch, records beyond 10 000 sync rows in one
  group-commit batch, transactional records): see "Sync vs Async Forwarding" there.
- **At-least-once.** The idempotent producer suppresses duplicates of its own retries within one
  producer session. A record can land twice when it is re-sent by a new producer (rebuilt after a
  fatal error, a settings change or the 120 s delivery timeout: the old producer's purged
  in-flight requests may still have been written), or when its ack is lost (the worker restarted
  before applying it, or the row's in-flight entry was evicted after 150 s): the claim then takes
  the row again and counts the lost attempt. Acks are never dropped on the way back (the
  forwarder keeps those the bounded channel cannot take yet), and a row in flight is not claimed
  again while the worker tracks it (no lease: a slow ack never causes a re-send). Consumers of the
  external topic that need exactly-once semantics must deduplicate, for example on an
  application id.
- **Order.** The forwarder sends the records of a local partition in local-offset order among
  those in the outbox, never one ahead of an earlier one's retry. A transaction's records enter
  the outbox only when it commits, so records of different producers are forwarded in commit
  order: a later offset written meanwhile by another producer can go out before an open
  transaction's earlier offsets (a read_committed consumer of the local topic stops at the open
  transaction and reads them in offset order). The external partition
  is chosen by key (`murmur2_random`, the Java client's partitioner; random for a null key), so
  the order the external topic keeps is per external partition: the first copies of the records
  of one local partition that land in the same external partition (for keyed records, those of
  one key) are in sending order there (duplicates may follow them). Records with different keys, or null
  keys, may land in different external partitions with no order between them. A dead-lettered
  record that is replayed later lands after the records that followed it.
- **Dead letters.** A row that fails `pg_kafka.shadow_max_retries` delivery attempts stops being
  forwarded until it is replayed (`kafka.replay_shadow_messages`) or the cap is raised. Only
  the record a partition waits on is charged an attempt when nothing is sent for reasons outside
  it (the 120 s delivery timeout, or shadow settings no producer can be built from, such as an
  unknown `shadow_security_protocol`); the records behind it wait uncounted, so a bad setting
  dead-letters each partition's records one at a time, not a whole backlog at once.
- **Local state is exact.** The local write and the outbox row commit together; a delivered row
  records its external offset once (a duplicate ack matches nothing).

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
  where Kafka's default is unlimited); over a cap the socket is closed at accept. A
  connection closes after `pg_kafka.connections_max_idle_ms` (600000, as Kafka) without a byte
  read or written, once nothing it sent is still being answered or responses wait for it to
  read them: one being written, or answered ones waiting behind an earlier request such as a
  long poll (a client that stops reading: as Kafka's idle expiry closes a muted connection whose
  response does not go out). A request still being answered with nothing answered behind it
  (a long-poll Fetch, a JoinGroup waiting for its group) keeps it open; Kafka's idle expiry
  closes such a connection too. A request frame must arrive within
  `pg_kafka.request_read_timeout_ms` (60000; Kafka has no such deadline) of its first byte, and the pre-authentication phase (TLS + SASL) has 10 s from accept. Memory for an
  incomplete incoming frame is reserved as its bytes arrive, under a 512 MiB budget for frames over
  1 MiB (Kafka's `queued.max.request.bytes` is unbounded by default); a connection whose frame
  would exceed the budget is closed. That budget covers only the raw bytes of frames still
  arriving: what a request decodes to is bounded separately. A frame's size bounds its decoded
  form poorly (a Metadata v9 topic of an empty name is 2 bytes on the wire and some 90 decoded, a
  DeleteGroups group name 1 byte and some 60; with their vectors' growth a decode of such entries
  holds 40 to 100 times its frame), so every request's decode is metered as it allocates: a
  decode may hold at most 2 MiB on an I/O thread, past which it runs again off the I/O threads
  (at most two at a time) under the request limit of **64 MiB**; it holds at most about three
  times its limit at once before it stops. A request that decodes to more than 64 MiB (besides
  a Produce's records, bounded below) closes the connection without a response, as Kafka closes
  the connection for a frame over `socket.request.max.bytes` (100 MB): Kafka accepts such a
  request if its frame is under 100 MB, pg_kafka does not. 64 MiB is what a Metadata request of
  some 300,000 topic names of 50 bytes decodes to, or a Fetch of some 500,000 partitions, far
  beyond what clients send. Decoded requests then hold at most 256 MiB together until the
  DB thread has run them (not until their responses are written: the responses a connection
  whose client stops reading holds are bounded by its response budget, `pg_kafka.fetch_max_bytes`
  (§7), until the idle timeout), 32 MiB of it
  kept for requests of at most 64 KiB (Heartbeats, ApiVersions, commits), so a few large
  requests cannot lock control traffic out; a decoded request that finds no room is freed and
  decodes again once there is, waiting up to 10 s (a Produce: half its `timeout_ms`, at most
  10 s, from when its frame was read; its connection reads nothing meanwhile), after which the
  connection closes, a Produce's too. A Fetch response holds at most the request's `max_bytes`,
  lowered to `pg_kafka.fetch_max_bytes`, of uncompressed record bytes, and its first record
  whole when that is larger (§7); a large response is written from its own buffer,
  and a connection's read buffer starts small again after a large frame, so neither keeps the
  largest frame's memory for the connection's life (Kafka allocates per receive and send).
  `TCP_NODELAY` is set on every socket. SASL credential lookups are rate limited
  per IP address (10/s, burst 20; Kafka has none). A connection closed for a bad frame, a timeout
  or an acks=0 error first delivers the responses of every request it read before (up to 30 s).
  Decoded Produce records waiting for the DB thread hold at most 512 MiB together (besides the
  256 MiB of decoded requests), and record decodes of more than 2 MiB run off the I/O threads,
  at most two at a time; a Produce whose records find no room waits for it, at most half its
  `timeout_ms` and 10 s from when its frame was read, and is then answered REQUEST_TIMED_OUT
  (see §2). A request's frame counts against the frame budget while it waits (for a
  large-decode slot, or for room in either budget), as an incomplete frame does; one the
  exhausted budget cannot take is refused at once: a Produce waiting for its records' room is
  answered REQUEST_TIMED_OUT, any other wait (a Produce's included, before its records)
  closes the connection.

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
