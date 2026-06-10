-- Ensure TimescaleDB functions are reachable regardless of session search_path
SET search_path = extensions, public;

-- Raw hypertables were created with the default 7-day chunk interval.
-- Retention and compression act on whole chunks, so 7-day chunks meant
-- "3-day retention" kept up to ~10 days of data and the open chunk
-- (up to ~350 MB of branch_energy) was never compressed — the cause of
-- the May 2026 free-tier disk quota breach.
-- Applies to newly created chunks only; tables were empty at migration time.
SELECT set_chunk_time_interval('public.branch_energy', INTERVAL '1 day');
SELECT set_chunk_time_interval('public.main_energy', INTERVAL '1 day');
