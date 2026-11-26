# ADR-001: Partitioning and Retention Strategy

**Status:** Accepted and implemented in 0.1.0. The sections from "Summary" on are the
original 2026-01 proposal, kept for the record; where they differ, "As built" below wins.
**Date:** 2026-01-02
**Last Reviewed:** 2026-11-24
**Context:** Based on PostgreSQL queue/pubsub benchmark findings

## As built (0.1.0)

`kafka.messages` is `PARTITION BY RANGE (created_at)`. Retention drops a whole leaf
("time partition", the analogue of a Kafka log segment) instead of deleting it row by
row, wherever every row in the leaf is expired under its own topic's rule.

### Layout

- **Leaves**: width `pg_kafka.message_partition_minutes`
  (default 1440 = one day, 5 minutes to 30 days, SIGHUP), aligned to UTC multiples of the
  width, named `kafka.messages_pYYYYMMDD_HHMMSS` after their (UTC) lower bound.
- **`kafka.messages_default`**, the DEFAULT leaf, catches any row whose leaf does not exist
  (worker late, or a row written out of band with an old/future `created_at`). A Produce
  therefore never fails because maintenance was late. It is normally empty.
- **Maintenance** (`kafka.ensure_message_partitions(width, ahead)`; the worker calls it at
  start and every 60 s sweep interval, operators via `SELECT pg_kafka_run_partition_maintenance()`)
  creates the leaf for now and the next two widths, and gives every row in the DEFAULT leaf a
  proper leaf (create the table standalone, move the rows with one `DELETE ... RETURNING` /
  `INSERT`, attach it: Postgres refuses `CREATE ... PARTITION OF` while the default holds rows
  for the range). Only gaps are filled, so changing the width never produces overlapping leaves;
  existing leaves keep their bounds.
- **Catalog listing:** `kafka.message_partitions()` returns every leaf with its bounds
  (parsed from `pg_get_expr(relpartbound)` with DateStyle pinned so they round-trip).
- **Monitoring:** `kafka.metrics` sums `reltuples` / `pg_table_size` over the leaves (the
  partitioned parent stores nothing) and reports `messages.time_partitions`.

### Uniqueness

Postgres only accepts a unique constraint on a partitioned table if it includes the partition
key, so the parent has no primary key and no `UNIQUE (global_offset)`. What actually makes
offsets unique has always been the allocator: `partition_offset` comes from
`kafka.partition_offsets.next_offset`, read and advanced under the per-partition advisory lock
and the counter row's lock in the same transaction, and never decremented;
`global_offset` comes from one sequence. As a backstop every leaf carries
`UNIQUE (topic_id, partition_id, partition_offset)` and `UNIQUE (global_offset)` btrees
(created by `kafka.create_message_partition` for every leaf, by bootstrap for the DEFAULT leaf,
and inherited from the old primary key / unique constraint by the upgraded leaf). A duplicate
inside one leaf is still rejected; a duplicate across leaves would need the counter to go
backwards, which nothing does.

Fetch, ListOffsets and the HWM/LSO probes do not name `created_at`, so they cannot prune: the
planner runs a MergeAppend (or one LIMIT 1 probe per leaf for MIN/MAX) over every leaf's unique
btree. The cost is one btree descent per leaf, which stays cheap while the leaf count is
bounded, and it is bounded only by a retention window: about window / width + 3 + DEFAULT,
e.g. 11 leaves for 7-day retention with daily leaves, where the window is the longest that
applies to any topic (a topic with `retention.ms = -1` keeps every leaf it writes to). With the
defaults (`pg_kafka.message_retention_hours = 0` and no topic setting `retention.ms`) no row
ever expires, so only empty past leaves are dropped: the leaf count grows by one per width
(one a day) for as long as data arrives, and with it the per-leaf probes of every Fetch,
ListOffsets and HWM lookup and the planning cost below. A long-lived deployment should set
`pg_kafka.message_retention_hours` (or `retention.ms` on every topic). Very small widths with
long retention multiply the leaf count and should be avoided.

Planning is the larger cost: planning a statement over a partitioned table expands every leaf,
so with 11 leaves a one-shot `MAX(partition_offset)` probe cost 0.75 ms against 0.10 ms on the
unpartitioned table (pgbench, simple protocol), almost all of it planning. The hot read
statements in `storage/postgres/log.rs` (produce's base-offset probe, Fetch, the partition-bounds,
HWM and earliest-offset probes) therefore run through prepared plans kept for the life of the
backend (`select_cached`, `SPI_keepplan`; the plan cache re-plans them when a leaf is created
or dropped). With cached plans, at 11 leaves: Fetch of 100 rows 0.20 ms vs 0.135 ms, MAX probe
0.19 ms vs 0.05 ms. The plans execute with `read_only = false` so each takes a fresh READ
COMMITTED snapshot like the one-shot statements: executed read-only, a cached plan reused a
snapshot older than the group-commit batch's latest requests and Fetch reported a stale
high watermark (caught by `test_fetch_bounds_from_single_statement`).

### Kafka-visible behaviour

- **Unchanged:** offsets, HWM, log start, Fetch, ListOffsets (earliest/latest/timestamp),
  DeleteRecords, transactions, shadow outbox/replay. No UPDATE the extension issues changes
  `created_at` (EndTxn only sets `txn_state`), so no row ever moves between leaves.
- **Retention granularity (the one deliberate change):** a topic whose window is at least one
  width is expired a leaf at a time, like Kafka segment retention (a segment goes once its
  newest record is past `retention.ms`): its rows are removed once the leaf's upper bound is past
  the window, so a row lives at most `window + width`. A topic whose window is shorter than one
  width keeps exact row-by-row expiry (as before 0.1.0), so a short `retention.ms` is not
  stretched to a day. Rows still in the DEFAULT leaf are always expired row by row.

### The sweep (`PostgresStore::run_retention_sweep`)

A pass is a list of steps, each working in *turns* of one bounded statement (10,000 rows, one
leaf, one probe or one drop); `retention::run_pass` schedules them (below, "Cost"). Before the
steps the pass reads the topics' windows, the leaves and, in one statement, which past leaves
that no topic can wholly expire are empty (a LIMIT 1 probe per leaf, pruned to it at run time).
What the probes find is carried from pass to pass in `retention::FoundWork` (per backend, a hint
re-checked every pass and, for a drop, under its lock), so the step that does it can come in any
later pass.

0. **Aborted rows.** Aborted transactional rows past the grace (60 s), leaf by leaf, through
   the partial index `idx_messages_txn_aborted` (created_at) WHERE txn_state = 'aborted' (every
   leaf has it: it is a partitioned index), one 10,000-row batch a turn, a leaf until caught
   up. Each leaf costs at least one probe of that index, so the leaves rotate: each pass starts
   after the last leaf the previous pass worked on (not at the oldest again).
1. **Contents probes.** For every past leaf (upper bound <= now) where some topic can be wholly
   expired (upper bound + the shortest finite window <= now),
   `kafka.message_partition_contents(leaf)` gives the (topic, partition) pairs present (a loose
   index scan, one descent per pair) and whether a pending transactional row is there, one leaf
   a turn, at most 64 per pass, rotating over the candidates. `retention::classify_leaf`
   decides: **Drop** when every topic present is wholly expired (`upper + window <= now`) and
   nothing is pending; **Trim** when some topics are wholly expired but the leaf must stay (a
   longer or infinite window, or a pending row): those topics' rows in that leaf get the
   bounded row delete; **Keep** otherwise. The decision replaces what was found in that leaf
   before. Empty past leaves (from the check before the steps) are found drops too. With no
   finite window anywhere (the default), no leaf is probed.
2. **Row expiry.** Its items, per (topic, leaf): the found trims, and the row-level items, a
   topic whose window is shorter than one width in the leaf holding its cutoff (`now -
   window`) and in the newest leaf wholly behind it (which the cutoff has just left), and
   every topic with a window in the DEFAULT leaf (unless it is empty), none in a leaf found
   droppable. Older leaves are wholly expired for such a topic, so they are probe candidates
   and its rows there are found trims: the row-level items are at most two per topic,
   whatever the number of leaves (they were one per topic and past leaf, millions for
   thousands of topics over a year of daily leaves, each a DELETE turn every rotation). One 10,000-row batch a turn, an item until caught up; a trim is done (forgotten)
   when its batch comes back short. Each statement is confined to one leaf (the leaf's bounds
   make the planner prune to it, and `tableoid` keeps `ctid` unambiguous; the DEFAULT leaf is
   addressed directly). The items rotate by (topic, leaf upper bound): an item with a large
   backlog takes one turn per rotation (plus whatever budget the others leave) instead of
   every pass. Trims the probes find during a pass join its rotation at the end.
3. **Drops** of the found leaves, one a turn, at most 64 per pass, rotating, through
   `kafka.drop_message_partition(leaf, expired_topics)`, which re-checks under `ACCESS
   EXCLUSIVE` on `kafka.messages` that the leaf holds only listed topics and no pending row, so
   a concurrent writer cannot lose a row. A leaf the re-check refuses is forgotten until a probe
   finds it droppable again. The step stops for the pass after a drop that ran out of a lock
   wait (`lock_timed_out`): each call can wait its 2 s `lock_timeout` for any session holding a
   lock on `kafka.messages` or a leaf (a reader idle in transaction after a SELECT, a long
   query, an autovacuum or ANALYZE of a leaf), and that session blocks the next drop just as
   long. That leaf stays found and the next pass starts after it. The 2 s is the functions' own
   `lock_timeout`, not `pg_kafka.worker_lock_timeout_ms` (default 5 s, sized for request
   batches), so one blocked drop stalls the worker at most 2 s. A drop's lock is held until the
   pass commits. The re-check is only sound at READ COMMITTED, where it reads a snapshot taken
   after the lock wait. A REPEATABLE READ or SERIALIZABLE snapshot predates the wait, so a row
   committed while the drop waited for its writer would be invisible to the re-check and
   dropped with the leaf, after its producer was acked. So `drop_message_partition` refuses at
   those levels (a WARNING, `dropped = false`), and a sweep running at one (a manual
   `pg_kafka_run_retention_sweep()` in such a transaction, or under such a
   `default_transaction_isolation`) skips its drop step with one WARNING: its found leaves stay
   found, and its other steps run as usual (a row delete removes only rows its snapshot sees,
   and a concurrent change to one of them fails the statement with a serialization error). The
   worker sets `default_transaction_isolation = 'read committed'` for its own session, so a
   cluster default of another level does not stop its drops.
4. **Log start.** `log_start_offset` of every touched (topic, partition) advances to the oldest
   surviving offset or the HWM, only forward (GREATEST), exactly as for row deletes: before
   each turn of the orphaned-outbox reaper and at the end of the pass. Offsets are assigned in
   `created_at` order, so a dropped leaf is an offset prefix of each partition it held; the only
   transient exception is a topic whose older rows sit in a non-droppable leaf awaiting a Trim,
   and those rows are expired too (the log start stays at the oldest surviving row).
5. **Auxiliary tables**, one 10,000-row batch a turn, each step until a batch comes back short:
   terminal transactions (through a partial index on `last_updated_at` of the terminal rows;
   their first turn comes before the producer prune's, whose candidates they reference) and
   stale producers (through an index on `(last_active_at, producer_id)`, resuming after the last
   candidate the previous batch examined, since the ones it keeps stay idle by that column; the
   batch names its rows by `= ANY (ARRAY(...))`, so each candidate costs index probes of
   `kafka.producer_ids`, of `kafka.transactions` through an index on `producer_id` and of
   `kafka.producer_sequences`, never a read of those tables); outbox rows delivered more than a
   day ago (walked per partition in offset order through `idx_shadow_tracking_delivered`, one
   index step per due row; a partition with none due costs one descent); and shadow-outbox rows
   below their partition's log start (their message is gone: one primary-key range per
   partition; its turns follow the log-start update, so the rows of the messages this pass
   removed go in the same pass when the budget allows, and DeleteRecords' at the next).

**Cost.** The sweep runs every 60 s on the worker's single DB thread whatever the retention
settings, and nothing else is served while it runs. So every step is index-driven and runs
bounded statements, and `retention::run_pass` schedules their turns in two phases: (1) every
step with work takes one turn, in the order above, whatever the budget; (2) while the pass is
within `RETENTION_PASS_BUDGET` (1.5 s) and some step has work, the step with work that has
spent the least time this pass takes the next turn. The guarantees, proved in the module docs
of `storage/retention.rs` and checked by its scheduling tests (random steps and budgets,
item lists that change between passes, and a model of probes, found work and row expiry over
thousands of passes):

- **Every item of every step is reached within a bounded number of passes, at any budget >=
  0 (the test-only `pg_kafka.test_retention_pass_budget_ms = 0` included), whatever the other
  steps' backlogs.** Phase 1 gives every step with work a turn every pass. A rotating step's
  items persist between passes and each pass serves at least the first after its cursor, so
  with `N` items each gets a turn within `N` passes and one needing `b` batches is done within
  `b * N`. A trim or drop in one of `P` probe candidates is found within `P` passes of becoming
  due (an empty leaf: the next pass), a found trim done within `b * E` more (`E` expiry items),
  a found drop within `D` more (`D` found drops; one more per lock timeout of that leaf). The
  prunes and reapers take their due rows oldest first, one batch a turn.
- **A pass costs at most the budget plus one turn per step** (in fact
  `max(budget + one turn, one turn per step)`), one of them a leaf drop that can wait 2 s for
  its lock, plus the fixed reads before the steps and the final log-start update; with nothing
  to do it reads a few index pages.
- **No budget is left idle while a step has work.** What one step does not use goes to the
  others, and a step with work all pass gets at least `budget / 8 - one turn` of it (8 steps),
  so a client-driven backlog in one step (aborted rows, producer ids) cannot crowd the others
  out, and a step alone with work gets the whole budget.
- **Found work does not depend on the pass that found it.** A trim stays found until its delete
  catches up, a drop until it is tried; both are forgotten when the leaf goes, a new probe of
  it decides otherwise, or (a trim) the topic's window no longer expires the whole leaf.
  (When a trim existed only in the pass whose probe reached its leaf, the probe and expiry
  rotations had to meet in that pass, and on a spent budget they phase-locked: a long-window
  topic's rows in a leaf kept by an infinite topic stayed fetchable for many rotations, or
  forever.)

Each step resumes where the last pass left it, by the identity of its items (a leaf's upper
bound, a (topic, leaf) pair, a producer's `(last_active_at, producer_id)`), not their position,
so leaves dropped or created between passes do not shift it. (Before the budget, the aborted-row
DELETE read every block of every leaf: a 20 ms Metadata probe saw a 200-300 ms stall every
minute with 3,000,000 stored rows; every stale producer id deleted read all of
`kafka.transactions` for its foreign key, 18.6 s for 40,000 ids; and a producer-prune batch
joined to its candidates read `kafka.producer_ids` and `kafka.transactions` whole, 115 ms a
batch with 220,000 ids. PERFORMANCE.md "The sweep's cost" has the numbers.)

A pending transactional row blocks its leaf's drop until the transaction ends (the transaction
timeout bounds that). `pg_kafka_run_retention_sweep()` reports the drops as `dropped_partitions`.

### Concurrency with the worker and with manual maintenance

Maintenance can run in the worker and, at the same time, in any superuser session
(`pg_kafka_run_partition_maintenance()`, `pg_kafka_run_retention_sweep()`; EXECUTE on both is
revoked from PUBLIC, and a role granted it also needs the privileges their SQL needs). Who may
call them matters: a call holds `ACCESS SHARE` on the extension object while it waits for the
maintenance advisory lock (below), and any role can hold that advisory lock. A role that could
call them would, holding `pg_advisory_lock(8099547369649367905)` in one session and calling the
sweep with `lock_timeout = 0` in another, keep `DROP EXTENSION` waiting for as long as it liked,
and every worker transaction (whose extension lock queues behind the DROP) with it. The rules:

* **Everything that locks member tables takes the extension lock first.** `DROP EXTENSION
  pg_kafka` takes `ACCESS EXCLUSIVE` on the extension object and then locks its members
  one by one (the schema `kafka`, then `kafka.topics`, ..., `kafka.messages`), and partition
  DDL locks the extension too (`CREATE TABLE` in schema `kafka` locks the schema, `ALTER
  EXTENSION ... ADD/DROP TABLE` the extension). A pass that held a member table when DROP
  started (maintenance takes `ACCESS SHARE` on `kafka.messages` first, a sweep deletes rows)
  and then needed one DROP already held formed a lock cycle, and Postgres aborted DROP or
  the pass with `deadlock detected`. So every worker transaction and both SQL functions
  (`pg_kafka_run_partition_maintenance()`, `pg_kafka_run_retention_sweep()`) start with
  `ACCESS SHARE` on the extension object (`worker::txn::lock_extension`), before any table
  lock: a pass either waits for a DROP in progress (bounded by the session's `lock_timeout`;
  if the extension is gone afterwards, the function fails) or makes DROP wait until it
  commits; the pass's own `ALTER EXTENSION ... ADD/DROP TABLE` (`SHARE UPDATE EXCLUSIVE`)
  then never waits for DROP. The SQL functions do not take the schema lock: they serialize
  with the worker's maintenance through the maintenance lock, and a move takes it EXCLUSIVE
  as below. `kafka.ensure_message_partitions` and `kafka.drop_message_partition` called
  directly (only superusers and `pgkafka_worker` may) cannot take the extension lock from
  SQL, so they keep the deadlock with a concurrent DROP EXTENSION: operators call the
  `pg_kafka_run_*` functions instead.
* **Moving rows out of the DEFAULT leaf never overlaps a worker transaction.** The move
  (`DELETE` from `kafka.messages_default` + `INSERT` into the new leaf, in
  `create_message_partition`) would otherwise race the worker's UPDATEs of `kafka.messages`
  (EndTxn's `txn_state` change, the timeout/fencing aborts, DeleteRecords): an UPDATE that
  meets a row the move deleted skips it once the move commits and never sees its copy,
  leaving a committed transaction `pending` forever. So every worker transaction
  starts by taking the pg_kafka schema advisory lock (key 8099547369649367906) in SHARE
  mode (`worker::txn::lock_schema`), and a pass that has rows to move takes it EXCLUSIVE.
  `ensure_message_partitions` queues for it once, bounded by its `lock_timeout`, when
  `kafka.messages_default` holds rows, before its first leaf locks any table: a pass that
  creates several leaves holds `kafka.messages` `ACCESS EXCLUSIVE` from the first to commit,
  and queueing for the schema lock after that would wait for a worker transaction that
  waits for `kafka.messages`, a lock cycle. `create_message_partition` then only tries the
  lock (`pg_try_advisory_xact_lock`, which succeeds at once for the pass that holds it): a
  leaf whose move finds it taken (the wait timed out, or the pass found DEFAULT empty and
  did not queue) is not created, and the next pass retries; the pass's other leaves are.
  Rows can still reach DEFAULT while `create_message_partition` waits for its table locks
  (`CREATE ... PARTITION OF` would then fail with `check_violation`), so a leaf that had
  none to move checks again once it holds `kafka.messages`, when none can arrive, and
  moves the rows on the same terms. That check reads the transaction's snapshot: a manual
  pass under `REPEATABLE READ` or `SERIALIZABLE` does not see rows committed during the
  wait, so a `check_violation` from the leaf's DDL skips the leaf (a WARNING, as for a lock
  timeout) and a later pass moves the rows. Trying the schema lock while holding the tables
  waits for nothing, so it cannot close a cycle. The upgrade scripts take the same lock
  first. No other path waits for the schema lock after a table lock.
* **Sweeps and partition maintenance never overlap.** A retention sweep both deletes rows
  (the aborted-row cleanup, the expired rows), which leaves its transaction holding
  `ROW EXCLUSIVE` on `kafka.messages` and the row locks of its deletes, and drops leaves,
  which needs `ACCESS EXCLUSIVE` there, in one transaction and in either order: the drop step
  is the fourth of the pass's eight steps, after the first deletes, and the budget-driven
  second phase can run more deletes after a drop, while the transaction still holds that
  `ACCESS EXCLUSIVE`. Two sweeps interleaving those steps form a lock cycle
  (one waits for `ACCESS EXCLUSIVE` behind the other's `ROW EXCLUSIVE`, the other waits for
  the first's deleted rows or for the maintenance lock) and Postgres aborts one with
  `deadlock detected`. So every sweep takes the partition-maintenance advisory lock
  (8099547369649367905) before its first statement and holds it to commit, as
  `ensure_message_partitions` and `drop_message_partition` do: sweeps and maintenance run one
  at a time. `pg_kafka_run_retention_sweep()` queues for the lock (bounded by the session's
  `lock_timeout`; it holds nothing else yet). The worker never waits for it, because its
  transaction holds the schema lock in SHARE mode, which a manual pass holding the
  maintenance lock may be waiting for (moving rows out of DEFAULT): its sweep is skipped with
  a WARNING while another session holds the lock, and the SQL functions only poll for it
  (`kafka.try_maintenance_lock()`, `pg_try_advisory_xact_lock` until their `lock_timeout`),
  never queue. Produce and fetch never take this lock, so they are not serialized with
  maintenance beyond the table locks below.
* **Partition DDL takes its table locks without deadlocking with worker transactions.**
  Creating or dropping a leaf locks `kafka.messages` (`ACCESS EXCLUSIVE`) and, to clone or
  drop the leaf's `topic_id` foreign key, `kafka.topics` (`SHARE ROW EXCLUSIVE`). A worker
  transaction locks these two tables in either order (a group-commit batch can fetch from
  one topic and then auto-create another, whose produce upserts `kafka.topics`), so no fixed
  order avoids a cycle with it. `kafka.lock_message_tables(leaf)` takes them (topics, then
  `ONLY kafka.messages`, then the leaf a drop removes and `kafka.messages_default`, whose
  partition constraint both DDLs change) in attempts of at most 50 ms each, well below
  `deadlock_timeout`, in a subtransaction whose failure releases them all, and retries with a random pause until the
  caller's `lock_timeout` (2 s in the partition functions) has passed. A worker transaction
  therefore waits at most one attempt, and one that holds a lock the DDL needs simply makes
  it retry. No other leaf is locked: a `LOCK TABLE` without `ONLY` locks every leaf, and a
  `SHARE UPDATE EXCLUSIVE` holder on any of them (autovacuum, `VACUUM`, `ANALYZE`) made every
  creation and drop time out while it ran. A vacuum of the leaf being dropped or of the
  DEFAULT leaf still makes that DDL wait (and time out after its 2 s, retried next pass).
* **Every lock wait in maintenance is bounded and handled.** A session that ran maintenance
  and stayed open makes the next pass skip with a WARNING instead of stalling it (and, for
  the worker, the whole broker). `drop_message_partition` takes its table locks inside a
  `lock_not_available` handler, so a timeout skips that leaf (and, in a sweep, the pass's
  remaining drops) instead of rolling back the caller's whole sweep. Any ERROR in the worker's maintenance (a lock timeout, a deadlock
  with a client's own transaction) rolls back that task's subtransaction only; the worker
  logs a WARNING, keeps serving, and the next interval retries.
* **A leaf dropped concurrently is "not dropped", not an error.** When two sweeps pick
  the same leaf, the second waits for the first's locks; afterwards
  `drop_message_partition` (and `message_partition_contents`) find the leaf gone and
  return `dropped = false` (NULL contents) instead of erroring on the vanished OID.
* **A manual sweep decides on committed rows, not on its snapshot.** The worker's own sweep
  runs on the DB thread between requests, but `pg_kafka_run_retention_sweep()` runs beside
  the worker's transactions, at READ COMMITTED a fresh snapshot per statement. A row lock
  taken on a row updated since the statement's snapshot re-evaluates only the locking
  query's own WHERE on the committed version; its CTEs and subqueries keep the statement's
  snapshot. So each such statement either names its rows by `ctid` (the terminal
  transaction, outbox and message batches: a concurrently updated row has a new `ctid`
  and is left for the next pass), or repeats on the row itself every condition a worker
  write can change: the producer prune's lock re-checks `last_active_at` against the
  window, so a producer that InitProducerId or an epoch bump revived and committed after
  the prune's snapshot is kept. An idempotent Produce writes the sequence and batch rows,
  which the prune's subqueries read from their snapshot, so it also moves
  `last_active_at` whenever that is more than half the window old (one UPDATE per producer
  per 3.5 days): a producer the prune can take is always one whose Produce writes its row
  first. A Produce still open when the prune reaches its producer holds the row, which the
  prune skips; one that reaches the row after the prune deleted it waits for the prune,
  finds no row and starts the producer afresh (adopted at its batch's sequence, as a
  producer the broker holds no state for), so the batch it acks is recorded and its retry
  is deduplicated. The log-start update after a delete first locks the affected
  `kafka.partition_offsets` rows, waiting (by polling, see below) for a Produce holding
  them (it inserts its records, then advances the counter, and commits), and only then
  reads the oldest surviving offset with a new snapshot, so the log start never passes a
  record the sweep did not delete (`kafka_test/src/part/sweep_races.rs`).
* **A manual sweep never waits in the lock manager once it holds a row.** The pass is one
  transaction and holds every row it deleted or locked until it commits, while the worker's
  group commit holds the rows of every request of a batch until one commit and the shadow
  forwarder's acks update outbox rows in arrival order. A sweep statement that waited for
  one of their rows while the sweep held a row they then needed closed a lock cycle, and
  the deadlock detector failed a client's request or the sweep: the counter upsert at the
  end of a pass waited for a Produce whose batch next refreshed a producer the prune had
  deleted (or deleted rows the expiry had deleted, as DeleteRecords), and the orphaned-
  outbox reaper waited for an ack batch whose next ack was a row it had reaped. So every
  row delete and lock of a pass skips the rows others hold (`FOR UPDATE SKIP LOCKED`:
  message and outbox batches, the producer and transaction prunes), and those rows go to a
  later pass. The offset counters cannot be skipped once a turn deleted rows of their
  partitions: each turn locks them right after its delete, in the same subtransaction, the
  free ones `SKIP LOCKED` and each held one by polling `FOR UPDATE NOWAIT` (every 20 ms, for
  at most 1 s), every attempt after checking that no session waits for the sweep. If the
  counter stays held, or a session waits for the sweep (it may hold the counter until it
  gets a row the sweep holds), the turn's delete is rolled back with it and the pass ends,
  so its commit frees what that session waits for. Drops queue for their table locks
  (above), so a pass drops leaves only before it holds any row: the drops come first in a
  pass, and once a turn has deleted a row the remaining ones wait for the next pass. A
  worker statement can still wait for a row the sweep holds until the sweep commits, as
  for any transaction, but never as the closing edge of a cycle.
* **Maintenance locks the parent before any leaf.** Every worker statement on
  `kafka.messages` locks the parent first and then its leaves in bound order, and
  `ensure_message_partitions` takes `ACCESS SHARE` on `kafka.messages` before it reads
  `kafka.messages_default`: a leaf locked without the parent would close a cycle with a
  session that holds the parent and waits for that leaf.
* **Your own DDL on `kafka.messages`: lock the parent first.** `CREATE TRIGGER`,
  `ALTER TABLE` and the like lock the parent before the leaves, but `DROP TRIGGER` on a
  partitioned table takes only `ACCESS SHARE` on the parent, then `ACCESS EXCLUSIVE` on
  each leaf (DEFAULT first, then newest first), so it can deadlock with any worker read
  that spans leaves (Fetch, the outbox claim), and Postgres may abort the worker's request
  rather than the DDL. Run such DDL as `BEGIN; LOCK TABLE kafka.messages IN ACCESS
  EXCLUSIVE MODE; ...; COMMIT;`, which waits for the worker instead (as
  `kafka_test/src/shadow/rollback.rs` does).
* The worker's own lock waits are bounded by `pg_kafka.worker_lock_timeout_ms` (default
  5 s), which applies to these locks too.

### Least privilege

The worker runs as `pgkafka_worker`, which has no DDL rights. All partition DDL runs inside
SECURITY DEFINER functions owned by the extension owner, each with `SET search_path = pg_catalog,
pg_temp`, schema-qualified names and `lock_timeout = 2s` around the DDL (a lock that cannot be had
in time is a WARNING and a retry next pass, never a stalled worker). EXECUTE is revoked from PUBLIC
and granted to `pgkafka_worker` only for `ensure_message_partitions`, `message_partition_contents`
and `drop_message_partition`; `create_message_partition` and the lock helpers
`try_maintenance_lock` and `lock_message_tables` are internal. Each function refuses any
relation that is not a leaf of `kafka.messages` (and `drop` refuses the DEFAULT leaf), so the
grant is no wider than the worker's existing INSERT/DELETE on `kafka.messages`: it can create
leaves of that table and drop leaves whose rows it may delete anyway. The worker reads and writes
leaves only through `kafka.messages`, plus `SELECT, DELETE` on `kafka.messages_default` for the
row delete there. `storage/privileges.rs` lists the function grants (`WORKER_FUNCTION_GRANTS`) and
the startup role check verifies them with `has_function_privilege`.

### Upgrade from 0.0.0

`ALTER EXTENSION pg_kafka UPDATE` renames the populated table to `kafka.messages_legacy`
(renaming its primary key / unique index / constraints to the per-leaf names), creates the
partitioned parent (same `global_offset` sequence, now owned by the parent) with the DEFAULT leaf,
and attaches the old table as one leaf `[MINVALUE, upgrade time)`; the rows are not copied into
a new table. The bound is `GREATEST(now(), MAX(created_at)) + 1 µs`: a 0.0.0 row dated after the
upgrade extends the legacy leaf to that time, maintenance creates no leaf before it (it fills only
gaps), every Produce until then lands in the legacy leaf, and leaf-granular retention of what it
holds waits until that bound is past (docs/UPGRADING.md says how to check for such rows first). Under ACCESS EXCLUSIVE it rewrites the old table once (filling `record_bytes`, the
size a fetch sums, for every row: a full read with each row's headers detoasted, a full write and
the rebuild of its three indexes, the two unique ones and the 0.0.0 pending-row index adopted at
ATTACH as a leaf of `idx_messages_txn_pending`, each a full read of the new copy) and reads it six more times (MAX(created_at) for the bound,
the attach's bound check, the BRIN, `idx_messages_txn_aborted`,
`idx_messages_txn_pending_producer` and `idx_messages_timestamp` builds). The legacy leaf is then retained and dropped like any past leaf.
The views that read the table are recreated on the parent. A new binary
running before the update (docs/UPGRADING.md step 2) detects the older schema and runs no
maintenance at all (no partition maintenance, no retention sweep, no transaction timeouts):
it answers every request with KAFKA_STORAGE_ERROR and re-checks the schema every 500 ms until
the update commits. Only `pg_kafka_run_retention_sweep()` called from SQL in that window expires
rows, row by row on the unpartitioned table. `scripts/check-upgrade.sh` compares
the upgraded and fresh schemas with leaves normalized (each leaf's index set and grants must be
identical; leaf names and counts are data-dependent), then checks that maintenance works on both,
that the legacy leaf drops whole without losing the sequence, and that `pg_dump` emits no kafka
table.

### pg_dump / restore

Every leaf is made a member of the extension (`ALTER EXTENSION pg_kafka ADD TABLE`, and `DROP`
before dropping it), like the tables `CREATE EXTENSION` made. pg_dump therefore treats leaves
exactly as it always treated `kafka.messages`: it dumps `CREATE EXTENSION pg_kafka` and no kafka
table or data (pg_kafka never marked its tables with `pg_extension_config_dump`). A restore gets a
fresh, empty schema with the DEFAULT leaf, and the worker creates the time leaves. If leaves were
not members, pg_dump would emit them, their `ATTACH` and their rows, and the restore would fail on
the foreign key to the (not dumped) `kafka.topics`. Back up pg_kafka data physically (base
backup + WAL), as before. `pg_upgrade` (binary-upgrade dump of extension members) was not tested.

### PostgreSQL versions

Only features present since PG11-12 are used (declarative RANGE partitioning with DEFAULT,
partitioned BRIN/partial indexes, FKs from partitioned tables, `pg_partition_tree`,
`ATTACH PARTITION`, run-time partition pruning); no catalog column or syntax newer than PG14.
Measured on PG15; CI runs the E2E suite (time partitions included) on PG14-18.

### Measured (PG15 sandbox, 1,000,000 expired + 100,000 retained rows, one topic, retention 1 day)

| | before (row DELETE sweep) | after (partition drop) |
|---|---|---|
| sweep passes until caught up | 3 | 1 (+1 no-op) |
| wall time of the sweep calls | 2.94 s | 0.35 s |
| rows deleted / leaves dropped | 1,000,000 / - | 0 / 3 |
| WAL written by the sweep | 240.5 MB | 0.16 MB |
| dead tuples left | 1,002,360 | 0 |
| following VACUUM | 0.37 s, 59 MB WAL | 0.04 s, 8 kB WAL |
| `kafka.messages` size afterwards | 248 MB (space kept) | 17 MB |

`performance` E2E category (rdkafka clients, msgs/s; three runs each, noisy shared 4-core box):

| | produce | consume | produce P50 |
|---|---|---|---|
| before (unpartitioned) | 120-140 | 433-435 | 6.7-8.0 ms |
| after, 4 leaves | 142-145 | 417-434 | 6.8-6.9 ms |
| after, 11 leaves | 138-142 | 416-420 | 7.0-7.1 ms |
| after, 11 leaves, before plan caching | 123-127 | 416-418 | 7.5-7.7 ms |


## Summary

This document outlines the architectural decision for implementing table partitioning and retention policies in `pg_kafka`. It addresses the critical performance issue of unbounded table growth while acknowledging the tradeoffs involved.

## Problem Statement

The current Phase 2 implementation stores all messages in a single `kafka.messages` table with no retention mechanism. This leads to:

- **Unbounded Growth:** Table grows indefinitely (1M → 10M → 100M+ rows)
- **Performance Degradation:** Query latency increases linearly with table size
- **Index Bloat:** Indexes become multi-GB and slow down even indexed queries
- **Expensive Maintenance:** `VACUUM` operations take hours on large tables
- **Eventual Failure:** Disk exhaustion causes database crash

**Benchmark Evidence:**
The [pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark) demonstrates that PostgreSQL can handle 1M+ reads/sec, but performance degrades significantly beyond 50M rows even with proper indexing.

## Decision

Implement **time-based table partitioning** with **configurable retention policies** in Phase 3.

### Architecture

```sql
-- Partitioned parent table
CREATE TABLE kafka.messages (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    global_offset BIGSERIAL,
    partition_offset BIGINT NOT NULL,
    key BYTEA,
    value BYTEA,
    headers JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Child partitions (one per day)
CREATE TABLE kafka.messages_2026_01_01 PARTITION OF kafka.messages
    FOR VALUES FROM ('2026-01-01') TO ('2026-01-02');

CREATE TABLE kafka.messages_2026_01_02 PARTITION OF kafka.messages
    FOR VALUES FROM ('2026-01-02') TO ('2026-01-03');
-- ... etc
```

### Retention Policy

```sql
-- Configurable via GUC
pg_kafka.retention_days = 7  -- Default: 7 days

-- Automatic partition management
SELECT kafka.ensure_next_partition();  -- Creates tomorrow's partition
SELECT kafka.drop_old_partitions();     -- Drops partitions older than retention_days
```

### Background Worker Integration

A background worker task runs daily to:
1. Create the next day's partition (proactive)
2. Drop partitions older than `retention_days`

## Tradeoffs

### ✅ Benefits

1. **Constant Query Performance**
   - Each partition stays small (e.g., 1 day of data)
   - Query latency remains <10ms regardless of total historical volume
   - Partition pruning skips irrelevant partitions automatically

2. **O(1) Data Deletion**
   - `DROP TABLE` is instant (vs hours for `DELETE FROM ... WHERE`)
   - No `VACUUM` bloat from deletions
   - Disk space immediately reclaimed

3. **Predictable Resource Usage**
   - Disk usage bounded by retention window
   - Each partition's maintenance cost is constant
   - No runaway table growth

4. **Better Maintenance**
   - `VACUUM` per partition completes in seconds (vs hours)
   - Smaller indexes fit in memory
   - Easier to backup/restore individual time ranges

### ❌ Drawbacks

1. **Loss of Long-Term Storage**
   - **Critical:** Old messages are permanently deleted after retention period
   - Cannot replay historical data beyond retention window
   - No way to recover dropped partitions (they're gone forever)

2. **Operational Complexity**
   - Requires partition management background worker
   - More moving parts (partition creation/dropping logic)
   - Migration from non-partitioned to partitioned schema needed

3. **Schema Inflexibility**
   - Partition key (`created_at`) cannot be changed without full rebuild
   - Changing partition interval (daily → hourly) requires migration
   - All queries must be aware of partition key for optimal performance

4. **Not True Kafka Semantics**
   - Real Kafka: Retention by log segments (can be very long, e.g., months/years)
   - `pg_kafka`: Aggressive retention required for performance (typically days)
   - Users migrating from real Kafka may expect longer retention

5. **Global Offset Gaps**
   - `global_offset` (BIGSERIAL) doesn't reset per partition
   - Dropped partitions leave permanent gaps in the sequence
   - Acceptable for Kafka clients (they tolerate gaps), but may confuse users

## Alternatives Considered

### Alternative 1: Keep Unbounded Table (Status Quo)

**Rejected:** Benchmark data proves this hits performance cliff at ~50M rows. Not viable for production.

### Alternative 2: External Archival (Messages Table + Archive Table)

```sql
-- Active messages (last N days)
CREATE TABLE kafka.messages_active (...);

-- Archive table (compressed, read-only)
CREATE TABLE kafka.messages_archive (...) WITH (fillfactor = 100);

-- Background worker moves old data to archive
INSERT INTO kafka.messages_archive SELECT * FROM kafka.messages_active WHERE ...;
DELETE FROM kafka.messages_active WHERE ...;
```

**Pros:**
- Preserves long-term data in archive
- Active table stays small

**Cons:**
- `DELETE` from active table causes massive bloat (same problem as unbounded growth)
- Complex query logic (must `UNION` active + archive for historical queries)
- Archive table still grows unbounded (just slower)

**Decision:** Rejected due to `DELETE` bloat issue. Partitioning is cleaner.

### Alternative 3: Columnar Storage for Old Partitions

Use PostgreSQL's `cstore_fdw` or similar for archival partitions:

```sql
-- Recent partition: Standard heap storage
CREATE TABLE kafka.messages_2026_01_01 PARTITION OF kafka.messages ...;

-- Old partition: Columnar storage (10x compression, read-only)
CREATE FOREIGN TABLE kafka.messages_2025_12_01
    SERVER cstore_server
    OPTIONS (compression 'pglz');
```

**Pros:**
- Preserves historical data with 90% less disk
- Transparent to queries (still looks like one table)

**Cons:**
- Requires external extension (`cstore_fdw`)
- Adds operational complexity
- Read-only (no updates/deletes on archived partitions)

**Decision:** Defer to Phase 4+. Consider as future enhancement, but implement basic partitioning first.

### Alternative 4: Configurable Retention Per Topic

```sql
-- Different retention per topic
CREATE TABLE kafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    partitions INT DEFAULT 1,
    retention_days INT DEFAULT 7  -- Per-topic override
);
```

**Pros:**
- Flexibility: Critical topics get longer retention
- Granular control

**Cons:**
- Complex partition management (need per-topic partition sets)
- Queries across topics become harder to optimize
- Much more complex background worker logic

**Decision:** Defer to Phase 3.5. Start with global retention, add per-topic later if needed.

## Implementation Recommendations

### Phase 3.1: Core Partitioning

1. Create migration script: `sql/migrate_to_partitioned.sql`
2. Implement partition management functions:
   - `kafka.ensure_next_partition()` - Creates tomorrow's partition
   - `kafka.drop_old_partitions()` - Drops old partitions
3. Add GUC parameter: `pg_kafka.retention_days`
4. Update background worker to run partition management daily

### Phase 3.2: Monitoring & Safety

1. Add partition count metrics:
   ```sql
   SELECT COUNT(*) AS partition_count
   FROM pg_tables
   WHERE schemaname = 'kafka' AND tablename LIKE 'messages_%';
   ```

2. Log partition operations:
   ```
   INFO: Created partition messages_2026_01_03
   INFO: Dropped partition messages_2025_12_26 (1,234,567 rows)
   ```

3. Add safety checks:
   - Warn if partition count exceeds expected (indicates stalled cleanup)
   - Prevent dropping partitions if retention_days = 0 (infinite retention)

### Phase 3.3: Documentation

1. Clearly document the **data loss** aspect:
   > ⚠️ **WARNING:** pg_kafka automatically deletes messages older than `pg_kafka.retention_days`.
   > This is PERMANENT. If you need long-term storage, use Shadow Mode to replicate to real Kafka.

2. Provide guidance on choosing retention period:
   - Dev/test: 1-3 days sufficient
   - Production: 7-30 days typical
   - Warning: >30 days may degrade performance (large partitions)

3. Document recovery options:
   - Use WAL archiving for disaster recovery
   - Use Shadow Mode replication for long-term storage
   - Regular backups via `pg_dump` (per partition)

## Success Metrics

### Performance Targets

| Metric | Target (Phase 3) |
|--------|-----------------|
| Query latency (p50) | <5ms regardless of total historical rows |
| Query latency (p99) | <20ms regardless of total historical rows |
| VACUUM time per partition | <30 seconds |
| Disk usage | Bounded by `retention_days * daily_write_rate` |

### Operational Targets

| Metric | Target |
|--------|--------|
| Partition creation success rate | 100% (no missed days) |
| Partition dropping success rate | 100% (cleanup always runs) |
| Partition count stability | Constant (equals `retention_days + 1`) |

## User Migration Guide

### For Existing Deployments

**If you have <1M messages (small dataset):**
1. Run migration script during maintenance window
2. Verify data integrity
3. Resume operations
4. **Downtime:** ~5 minutes

**If you have >1M messages (large dataset):**
1. Enable Shadow Mode replication to external Kafka (preserves long-term data)
2. Create partitioned table alongside existing table
3. Backfill recent data (last `retention_days`) to new table
4. Switch writes to partitioned table
5. Verify dual-write working
6. Drop old table after retention period
7. **Downtime:** Zero (dual-write strategy)

### Configuration Examples

```sql
-- Development (short retention, saves disk)
pg_kafka.retention_days = 1

-- Production (balanced)
pg_kafka.retention_days = 7

-- Long-term storage (WARNING: may impact performance)
pg_kafka.retention_days = 30

-- Infinite retention (NOT RECOMMENDED: performance cliff inevitable)
pg_kafka.retention_days = 0  -- Disables auto-dropping
```

## When NOT to Use pg_kafka

Based on this retention limitation:

### ❌ Use Real Kafka Instead If:

1. **Long-term Event Sourcing**
   - Need to replay events from months/years ago
   - Event log is the source of truth (cannot delete)

2. **Compliance Requirements**
   - Legal requirement to retain messages >30 days
   - Audit logs that must be immutable

3. **Massive Scale**
   - >1M messages/sec sustained
   - Multi-datacenter replication
   - Need Kafka's distributed architecture

### ✅ pg_kafka is Good For:

1. **Recent Event Processing**
   - Real-time dashboards (last 7 days)
   - Alert systems (recent data only)
   - Session tracking (short-lived data)

2. **Development/Testing**
   - Local Kafka replacement for dev environments
   - CI/CD pipelines (ephemeral data)

3. **Migration Path**
   - Start with pg_kafka, grow into real Kafka
   - Use Shadow Mode for zero-downtime cutover

## Open Questions

1. **Should we support partition-by-size instead of partition-by-time?**
   - Pro: More predictable partition sizes
   - Con: Harder to implement, non-standard

2. **Should retention be configurable at topic level?**
   - Pro: Flexibility (critical topics get longer retention)
   - Con: Complexity (per-topic partition management)

3. **Should we compress old partitions instead of dropping them?**
   - Pro: Preserves data, saves disk
   - Con: Requires extensions like `cstore_fdw`

**Decision:** Revisit in Phase 3.5 after core partitioning is stable.

## References

- [pg-queue-pubsub-benchmark](https://github.com/stanislavkozlovski/pg-queue-pubsub-benchmark) - Performance validation
- [PostgreSQL Partitioning Docs](https://www.postgresql.org/docs/current/ddl-partitioning.html)
- [Kafka Log Retention](https://kafka.apache.org/documentation/#retention) - For comparison

---

**Next Steps:**
- [x] Implement partition management functions (0.1.0, see "As built")
- [ ] Test the 0.0.0 upgrade with 10M+ rows (checked with small data in `scripts/check-upgrade.sh`)
- [x] Document data loss behavior in user-facing docs ([PERFORMANCE.md](../PERFORMANCE.md#retention--lifecycle-gucs-shipped))
- [x] Add monitoring for partition operations (`kafka.metrics` `messages.time_partitions`, worker log lines)
