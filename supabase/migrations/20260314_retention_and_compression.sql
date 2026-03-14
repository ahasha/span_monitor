-- Ensure TimescaleDB functions are reachable regardless of session search_path
SET search_path = extensions, public;

-- Reduce raw data retention from 7 days to 3 days
SELECT remove_retention_policy('public.branch_energy', if_exists => true);
SELECT remove_retention_policy('public.main_energy', if_exists => true);

SELECT add_retention_policy('public.branch_energy', INTERVAL '3 days', if_not_exists => true);
SELECT add_retention_policy('public.main_energy', INTERVAL '3 days', if_not_exists => true);

-- Enable columnar compression on raw tables
ALTER TABLE public.branch_energy
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'time DESC',
        timescaledb.compress_segmentby = 'branch_id'
    );

ALTER TABLE public.main_energy
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'time DESC'
    );

-- Compress chunks older than 1 day (background job runs automatically)
SELECT add_compression_policy('public.branch_energy', INTERVAL '1 day', if_not_exists => true);
SELECT add_compression_policy('public.main_energy', INTERVAL '1 day', if_not_exists => true);
