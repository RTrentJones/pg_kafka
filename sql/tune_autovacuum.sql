-- Optional autovacuum tuning for pg_kafka tables
--
-- This script applies aggressive autovacuum settings to kafka.messages table.
-- These settings are recommended for high-throughput message workloads (>10k msgs/sec).
--
-- Usage:
--   1. For high-throughput production: Run this script after extension installation
--   2. For low-throughput dev/test: Skip this script (use default autovacuum settings)
--
-- You can also adjust these values based on your specific workload:
--   - Higher scale_factor = less frequent vacuum (lower overhead, more bloat)
--   - Lower scale_factor = more frequent vacuum (higher overhead, less bloat)
--
-- To apply:
--   psql -d your_database -f sql/tune_autovacuum.sql
--
-- To revert to defaults:
--   ALTER TABLE kafka.messages RESET (autovacuum_vacuum_scale_factor);
--   ALTER TABLE kafka.messages RESET (autovacuum_analyze_scale_factor);
--   ALTER TABLE kafka.messages RESET (autovacuum_vacuum_cost_delay);

-- Aggressive autovacuum for kafka.messages table
-- Recommended for workloads with >10,000 inserts/sec
ALTER TABLE kafka.messages SET (
    autovacuum_vacuum_scale_factor = 0.05,   -- Vacuum after 5% of rows change (vs default 0.2)
    autovacuum_analyze_scale_factor = 0.05,  -- Analyze after 5% of rows change (vs default 0.1)
    autovacuum_vacuum_cost_delay = 2         -- Lower delay = more aggressive vacuum (vs default 20)
);

-- Optional: Increase autovacuum workers if you have many topics/partitions
-- (Requires postgresql.conf change and restart)
-- autovacuum_max_workers = 6  -- Default is 3

-- Verify settings applied
SELECT
    relname,
    reloptions
FROM pg_class
WHERE relname = 'messages' AND relnamespace = 'kafka'::regnamespace;
