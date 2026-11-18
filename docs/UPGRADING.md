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

2. Restart Postgres so the new shared library (and its background worker) is loaded.
   The worker of a new binary runs fine against the previous schema until step 3:
   features that need new objects stay as they were, and the 0.1.0 role switch falls
   back to the bootstrap superuser with a WARNING until the role exists.

3. In the database named by `pg_kafka.database`, run:

   ```sql
   ALTER EXTENSION pg_kafka UPDATE;          -- to the default (newest) version
   SELECT extversion FROM pg_extension WHERE extname = 'pg_kafka';
   ```

4. Restart Postgres again (or terminate the `pg_kafka_listener` backend) if the
   upgrade added something the worker only picks up at startup. For 0.1.0 this is the
   switch to `pg_kafka.run_as_role`.

Upgrade scripts take locks on the tables they alter. Run them in a quiet period on a
large `kafka.messages`.

## 0.0.0 -> 0.1.0

`sql/pg_kafka--0.0.0--0.1.0.sql`:

- drops `idx_messages_topic_partition_offset`, which duplicated the `kafka.messages`
  primary key;
- adds `idx_messages_created_at_brin` (BRIN on `created_at`, `autosummarize = on`) for
  the retention sweep (building it reads the table once);
- creates the NOLOGIN roles `pgkafka_reader` and `pgkafka_worker` if they do not exist,
  revokes every privilege 0.0.0 granted to PUBLIC on the kafka schema (including
  EXECUTE on `kafka.replay_shadow_messages`), grants read-only access to
  `pgkafka_reader` and the worker's privileges to `pgkafka_worker`;
- re-applies the two additive columns older 0.0.0 builds may lack
  (`kafka.topics.retention_ms`, `kafka.partition_offsets.log_start_offset`).

After the upgrade:

- **Readers.** Roles that read `kafka.*` directly (dashboards, monitoring using
  `kafka.metrics`, ad-hoc queries) need `GRANT pgkafka_reader TO <role>;`. Superusers
  are unaffected.
- **Worker role.** `pg_kafka.run_as_role` defaults to `pgkafka_worker`. After the
  restart in step 4 the worker logs `background worker now runs as role
  'pgkafka_worker'`. Set `pg_kafka.run_as_role = ''` to keep the old behaviour (run as
  the bootstrap superuser), or name another role with the same privileges.
- **Roles are cluster-wide.** They are not dropped by `DROP EXTENSION`; drop them by
  hand once no database uses pg_kafka.

## Checking an upgrade path

`scripts/check-upgrade.sh` (run in CI after the E2E tests) installs the frozen 0.0.0
install script from `sql/upgrade-test/`, creates one database at 0.0.0 with data and
upgrades it, creates another with a fresh install, and fails unless the two kafka
schemas (columns, indexes, ACLs, roles) are identical and the data survived:

```bash
PG_CONFIG=$(which pg_config) PGHOST=localhost PGPORT=28814 PGUSER=postgres \
  scripts/check-upgrade.sh
```

## Adding the next version

1. Bump `version` in `Cargo.toml`.
2. Change `sql/bootstrap.sql` (a fresh install) and write
   `sql/pg_kafka--<old>--<new>.sql` with the same changes for existing installs.
3. Copy the old version's generated install script (`$(pg_config
   --sharedir)/extension/pg_kafka--<old>.sql` from an install of the old release)
   into `sql/upgrade-test/` and point `scripts/check-upgrade.sh` at it, so CI checks
   the new path.
