# Upgrading pg_kafka

pg_kafka is a versioned Postgres extension. The extension version is the crate version
in `Cargo.toml` (`pg_kafka.control` uses `@CARGO_VERSION@`). Every schema change ships
as an upgrade script `sql/pg_kafka--<from>--<to>.sql`; `cargo pgrx install` and
`cargo pgrx package` copy those scripts next to the generated install script.

## Procedure

1. Build and install the new version (or deploy the package from `cargo pgrx package`):

   ```bash
   cargo pgrx install --release --pg-config "$(which pg_config)"
   ```

2. Restart Postgres so the new shared library (and its background worker) is loaded. The
   worker of a new binary does **not** serve Kafka clients against the previous schema:
   the 0.1.0 library's request paths use objects the 0.0.0 schema lacks (every
   idempotent Produce reads `kafka.producer_batches`, transactions use
   `kafka.txn_partitions` and `kafka.txn_groups`, SASL logins read `kafka.sasl_credentials` and
   `kafka.sasl_server_secret`, the shadow outbox needs
   `kafka.shadow_tracking.attempt_open`, maintenance and the metrics flush need the time
   partitions and `kafka.broker_api_stats` / `kafka.broker_gauges`). At start it checks
   for each of them and, on an older schema, logs **one** WARNING (`running against an
   older pg_kafka schema (version 0.0.0, library 0.1.0): answering Kafka requests with
   KAFKA_STORAGE_ERROR and running no maintenance until ALTER EXTENSION pg_kafka UPDATE
   (missing: ...)`). Until step 3, clients see what they see while the extension is
   dropped: the Kafka port accepts connections and every request the worker serves
   (ApiVersions and Metadata included) is answered with the retriable
   `KAFKA_STORAGE_ERROR` (56). On a SASL listener the network thread still answers
   ApiVersions and SaslHandshake, but the credential lookup of the SaslAuthenticate is
   refused, and the broker then closes the connection without answering it: the
   credentials were not checked, so it is not reported as `SASL_AUTHENTICATION_FAILED`
   (which the Java client treats as fatal). Kafka clients reconnect after a disconnect
   and retry a retriable error, so producers queue their records (up to their own
   `delivery.timeout.ms` / `message.timeout.ms`) and consumers stall; nothing is
   written. A SASL client logs a disconnect on every reconnect attempt
   (`reconnect.backoff.ms`, up to `reconnect.backoff.max.ms`) until step 3. Keep the window between the restart and step 3
   short. The worker runs no maintenance in the meantime;
   `pg_kafka_run_retention_sweep()` still works from SQL (row by row on the
   unpartitioned 0.0.0 `kafka.messages`). Until step 3 runs, PUBLIC can still
   execute it (the 0.0.0 schema predates the REVOKE), so any role could hold the
   extension's lock in it and make `DROP EXTENSION` wait; step 3 revokes it and is
   not blocked by such a caller. Run step 3 promptly, or `REVOKE EXECUTE ON FUNCTION
   pg_kafka_run_retention_sweep(int, bigint) FROM PUBLIC` by hand first.

3. In the database named by `pg_kafka.database`, run:

   ```sql
   ALTER EXTENSION pg_kafka UPDATE;          -- to the default (newest) version
   SELECT extversion FROM pg_extension WHERE extname = 'pg_kafka';
   ```

4. Nothing else is needed for 0.1.0: while the schema is older the worker re-checks it
   every 500 ms, so within half a second of the update's commit it logs `pg_kafka
   schema is now version 0.1.0: serving Kafka clients`, switches to
   `pg_kafka.run_as_role` if the role now has its privileges, runs partition
   maintenance and serves clients, without a restart. Requests clients retried during
   the window then succeed (an idempotent producer's queued records are acked once, in
   order).

Upgrade scripts take locks on the tables they alter. Run them in a quiet period on a
large `kafka.messages`. A running worker does not deadlock with them: every upgrade
script starts with `SELECT pg_advisory_xact_lock(8099547369649367906)`, the pg_kafka
schema lock that every worker transaction holds in SHARE mode, so the script waits for
the worker's current transaction and the worker's next transactions wait (up to
`pg_kafka.worker_lock_timeout_ms`, default 5 s, then their requests get a retriable
error) until the upgrade commits. The script's wait is usually short, a request batch
(up to 50 ms of handlers plus its commit), but the worker's maintenance runs in
transactions too and can hold the lock for seconds: a retention sweep stops its steps
once the pass has used its 1.5 s budget, each step after at least one bounded statement
(10,000 rows or one leaf), and of its leaf drops, each of which can wait up to 2 s for
its lock, it stops after the first that times out, so a pass holds the lock for about
1.5 s plus one statement per step plus one 2 s drop wait at worst; partition
maintenance moves rows out of the DEFAULT leaf in one transaction. The
upgrade itself holds the lock for as long as it runs (see step 1 of 0.0.0 -> 0.1.0), so
on a large `kafka.messages` requests fail retriably for most of it.

`DROP EXTENSION pg_kafka` while the worker runs is safe too: every worker transaction
first takes a share lock on the extension object, which DROP EXTENSION locks before any
member table, so the two cannot deadlock. So do `pg_kafka_run_partition_maintenance()`
and `pg_kafka_run_retention_sweep()` (ADR-001, "Concurrency with the worker and with manual
maintenance"); call them in a transaction of their own. Once the extension is gone the worker keeps
the Kafka port open, answers every request with the retriable `KAFKA_STORAGE_ERROR`,
and resumes (partition maintenance, shadow configuration, role) as soon as `CREATE
EXTENSION pg_kafka` has run, without a restart.

## 0.0.0 -> 0.1.0

`sql/pg_kafka--0.0.0--0.1.0.sql`:

- converts `kafka.messages` to a table partitioned by `created_at` (ADR-001): the
  populated 0.0.0 table becomes the leaf `kafka.messages_legacy` covering
  `[MINVALUE, upgrade time)` without copying rows (it keeps its primary key and unique
  index as the per-leaf unique indexes and its `global_offset` sequence, now owned by
  the new parent), a DEFAULT leaf `kafka.messages_default` is added, the views
  `kafka.shadow_status` and `kafka.metrics` are recreated on the new parent, and the
  SECURITY DEFINER partition functions are created. The legacy leaf is dropped by the
  retention sweep once all its rows are expired. This reads the old table four times
  under ACCESS EXCLUSIVE (bound, attach check, BRIN and aborted-row index builds);
- drops `idx_messages_topic_partition_offset`, which duplicated the `kafka.messages`
  primary key;
- adds `idx_messages_created_at_brin` (BRIN on `created_at`, `autosummarize = on`) for
  the retention sweep;
- adds `idx_messages_txn_aborted` (partial, `created_at` of aborted rows) and
  `idx_shadow_tracking_delivered` (partial, delivered outbox rows), so the retention
  sweep's aborted-row cleanup and outbox reaping stop reading whole tables;
- adds `pg_kafka_run_partition_maintenance()`, and revokes EXECUTE on it and on
  `pg_kafka_run_retention_sweep()` from PUBLIC (superusers only, as in a fresh install;
  ADR-001, "Concurrency with the worker and with manual maintenance");
- creates the NOLOGIN roles `pgkafka_reader` and `pgkafka_worker` if they do not exist,
  revokes every privilege 0.0.0 granted to PUBLIC on the kafka schema (including
  EXECUTE on `kafka.replay_shadow_messages`), grants read-only access to
  `pgkafka_reader` and the worker's privileges to `pgkafka_worker`;
- brings older 0.0.0 builds to the newest 0.0.0 layout first: it re-applies the
  additive columns they may lack (`kafka.topics.retention_ms`,
  `kafka.partition_offsets.log_start_offset`, `kafka.messages.timestamp_ms`, the latter
  as -1, "no record timestamp", so fetches fall back to `created_at`) and converts the
  naive `TIMESTAMP` columns of builds before the TIMESTAMPTZ change
  (`kafka.messages.created_at`, `kafka.transactions.started_at`,
  `kafka.shadow_tracking.forwarded_at`) to `TIMESTAMPTZ`, reading the stored values in
  the upgrading session's `TimeZone` (set it to the server's zone if the session's
  differs). Converting `kafka.messages.created_at` rewrites the table, one more full
  read and write under the same lock. Layouts older than these are not supported;
- adds `kafka.producer_ids.last_epoch` (the epoch an InitProducerId retry may present);
- adds indexes on the referencing columns of foreign keys whose parent rows are deleted
  or updated: `idx_consumer_offsets_topic`, `idx_producer_sequences_topic`,
  `idx_txn_pending_offsets_topic` and `idx_txn_partitions_topic` (`topic_id`, for the
  cascade of DeleteTopics) and `idx_transactions_producer` (`kafka.transactions.producer_id`,
  for the retention sweep's producer prune and the RV-13 producer-id reallocation);
- adds `idx_producer_ids_last_active` (`last_active_at`, `producer_id`) and
  `idx_transactions_terminal` (partial, `last_updated_at` of terminal transactions) for
  the retention sweep's producer and transaction prunes.

After the upgrade:

- **Readers.** Roles that read `kafka.*` directly (dashboards, monitoring using
  `kafka.metrics`, ad-hoc queries) need `GRANT pgkafka_reader TO <role>;`. Superusers
  are unaffected.
- **Worker role.** `pg_kafka.run_as_role` defaults to `pgkafka_worker`. With that
  default nothing is restarted: once the worker sees the updated schema (step 4) it logs
  `background worker now runs as role 'pgkafka_worker'`. To keep the old behaviour (run
  as the bootstrap superuser) set `pg_kafka.run_as_role = ''`, or name another role with
  the same privileges. The setting is read only at server start (a reload does not
  change it), so set it in `postgresql.conf` before the restart of step 2.
- **Roles are cluster-wide.** They are not dropped by `DROP EXTENSION`; drop them by
  hand once no database uses pg_kafka.

## Checking an upgrade path

`scripts/check-upgrade.sh` (run in CI after the E2E tests) installs the frozen 0.0.0
install script from `sql/upgrade-test/`, creates one database at 0.0.0 with data and
upgrades it, creates another with a fresh install, and fails unless the two kafka
schemas (columns, indexes, ACLs, views, roles, partitioning; leaves are compared by
shape since their names are data-dependent) are identical, the data survived, partition
maintenance works on both, the legacy leaf drops whole and `pg_dump` emits no kafka table:

```bash
PG_CONFIG=$(which pg_config) PGHOST=localhost PGPORT=28814 PGUSER=postgres \
  scripts/check-upgrade.sh
```

## Adding the next version

1. Bump `version` in `Cargo.toml`.
2. Change `sql/bootstrap.sql` (a fresh install) and write
   `sql/pg_kafka--<old>--<new>.sql` with the same changes for existing installs.
3. Start the upgrade script with `SELECT pg_advisory_xact_lock(8099547369649367906);`
   (the pg_kafka schema lock, see above), before any statement that locks a table.
4. Copy the old version's generated install script (`$(pg_config
   --sharedir)/extension/pg_kafka--<old>.sql` from an install of the old release)
   into `sql/upgrade-test/` and point `scripts/check-upgrade.sh` at it, so CI checks
   the new path.
