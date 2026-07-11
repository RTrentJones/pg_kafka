-- Optional autovacuum tuning for pg_kafka tables
--
-- DR-5 (DEEP-REVIEW-2026-07): this script targets the tables by their actual churn
-- profile. kafka.messages is append-mostly (UPDATE only when a transaction aborts,
-- DELETE only via retention/cleanup sweeps), so its bloat pressure is modest; the
-- genuinely high-churn tables are the small counters that take an UPDATE/UPSERT on
-- every produce or commit:
--
--   - kafka.partition_offsets   UPDATE on every produce (per topic/partition)
--   - kafka.producer_sequences  UPDATE per idempotent batch
--   - kafka.consumer_offsets    UPSERT per offset commit
--   - kafka.shadow_tracking     INSERT + UPDATE per forwarded message (shadow mode)
--
-- Because those tables are tiny but rewritten constantly, the default 20% scale
-- factor lets them accumulate hundreds of dead tuples per live row between vacuums,
-- which inflates the index lookups on the hot produce/commit paths. Threshold-based
-- settings (zero scale factor + explicit threshold) keep them tight.
--
-- Usage:
--   1. For high-throughput production: Run this script after extension installation
--   2. For low-throughput dev/test: Skip this script (use default autovacuum settings)
--
-- To apply:
--   psql -d your_database -f sql/tune_autovacuum.sql
--
-- To revert to defaults:
--   ALTER TABLE <table> RESET (autovacuum_vacuum_scale_factor,
--                              autovacuum_vacuum_threshold,
--                              autovacuum_analyze_scale_factor,
--                              autovacuum_analyze_threshold,
--                              autovacuum_vacuum_cost_delay);

-- Hot counter tables: vacuum aggressively by threshold, not table fraction.
ALTER TABLE kafka.partition_offsets SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 500,       -- vacuum after ~500 dead tuples
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 500,
    autovacuum_vacuum_cost_delay = 2
);

ALTER TABLE kafka.producer_sequences SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 500,
    autovacuum_vacuum_cost_delay = 2
);

ALTER TABLE kafka.consumer_offsets SET (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000,
    autovacuum_vacuum_cost_delay = 2
);

ALTER TABLE kafka.shadow_tracking SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 2
);

-- kafka.messages: mostly append-only. A moderate scale factor keeps the visibility
-- map fresh for index-only scans and reclaims abort-marked rows after the cleanup
-- sweep deletes them, without paying constant full-table vacuum cost.
ALTER TABLE kafka.messages SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_vacuum_cost_delay = 2
);

-- Optional: Increase autovacuum workers if you have many topics/partitions
-- (Requires postgresql.conf change and restart)
-- autovacuum_max_workers = 6  -- Default is 3

-- Verify settings applied
SELECT
    relname,
    reloptions
FROM pg_class
WHERE relnamespace = 'kafka'::regnamespace
  AND reloptions IS NOT NULL
ORDER BY relname;
