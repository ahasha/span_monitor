# Design: Database Maintenance Scripts

**Date:** 2026-03-14
**Status:** Approved

## Context

The Supabase free tier has a 500 MB database size limit and a ~1.1 GB disk size limit. The project is approaching both. Investigation revealed:

- `branch_energy` raw table: ~252 MB for just 5 days (~50 MB/day), driven by 1-second polling across ~32 circuits
- `main_energy` raw table: ~14 MB for 5 days
- `branch_energy_hourly` cagg: ~43 MB for 18 months of history
- `main_energy_hourly` cagg: ~1.7 MB for 18 months
- WAL written cumulatively: ~14 GB (high-frequency inserts), 80 MB currently on disk

The hourly aggregates are the long-term historical record and grow slowly (~2.5 MB/month). The raw tables are the dominant storage cost and have no long-term use case — the existing 7-day retention is insufficient; 3 days keeps steady-state raw data around 150 MB.

No long-term use case exists for the second-scale raw data. The hourly aggregates are the authoritative historical record.

## Deliverables

1. A one-time SQL migration to shorten retention and enable compression
2. `archive.py` — incremental export of hourly aggregates to local parquet
3. `maintain.py` — diagnostic report and optional manual compression trigger

---

## Section 1: SQL Migration

**File:** `supabase/migrations/20260314_retention_and_compression.sql`

Changes:
- Reduce the `drop_chunks` retention policy on `branch_energy` and `main_energy` from 7 days to 3 days
- Add a `compress_chunks` compression policy on both raw tables: compress chunks older than 1 day

Deployed once via `supabase db push`. After that, TimescaleDB background jobs manage both retention and compression automatically.

**Why compression matters:** TimescaleDB columnar compression typically achieves 10–20x reduction on time-series data. At 50 MB/day uncompressed, a 3-day window of compressed raw data could drop from ~150 MB to ~10–15 MB.

---

## Section 2: `archive.py`

### Purpose

Incrementally export `branch_energy_hourly` and `main_energy_hourly` to local parquet files for offline analysis. Raw second-scale tables can optionally be downloaded but are not the default.

### Connection

Direct PostgreSQL via `psycopg2` (already a project dependency). Requires one new env var in `.env`:

```
DATABASE_URL=postgresql://postgres:{PASSWORD}@db.{PROJECT_ID}.supabase.co:5432/postgres
```

Available from Supabase dashboard under Project Settings → Database.

### CLI

```
python archive.py [OPTIONS]

Options:
  --output-dir PATH    Directory for parquet files. [default: ./archive]
  --full               Ignore existing parquet state; download entire history.
  --table TEXT         Table to archive. May be specified multiple times.
                       Choices: branch_energy_hourly, main_energy_hourly,
                                branch_energy, main_energy
                       [default: branch_energy_hourly, main_energy_hourly]
  --help
```

### Behavior

**Incremental (default):**
1. For each target table, check if a local parquet file exists at `{output_dir}/{table}.parquet`
2. If it exists, use Polars to read `max(time)` from the file
3. Query rows where `time >= max_local_time` via psycopg2 (inclusive, to catch updates to the partial last hour)
4. Construct a Polars DataFrame from cursor results
5. Drop any rows from the existing parquet where `time >= max_local_time`, concat the freshly fetched rows, and overwrite the file (ensures the last partial hourly bucket is always corrected on the next run)
6. If no local file exists, treat as a fresh full download

**Note on raw tables with `--table branch_energy` / `--table main_energy`:** Incremental is supported but these tables are not intended for recurring archival (no long-term use case). The expected use case is a one-time or occasional full download (`--full`). Incremental will work correctly but may fetch large volumes per run given the 1-second granularity.

**Full (`--full`):**
- Skip the max-timestamp check; fetch entire table history
- Overwrite any existing parquet file

**File layout:**
```
archive/
  branch_energy_hourly.parquet
  main_energy_hourly.parquet
  branch_energy.parquet          # only if --table branch_energy
  main_energy.parquet            # only if --table main_energy
```

### Notes

- Use Polars for all DataFrame operations (no pandas/ibis)
- Preserve timezone info on timestamp columns
- Log row counts and time ranges fetched each run

---

## Section 3: `maintain.py`

### Purpose

Print a diagnostic report of current database storage, and optionally trigger TimescaleDB compression on eligible uncompressed chunks.

**Note on VACUUM:** Since both raw tables are insert-only and TimescaleDB's retention policy works by dropping entire chunk tables (not row-level deletes), `VACUUM` has no meaningful effect and is not exposed as an option.

### CLI

```
python maintain.py [OPTIONS]

Options:
  --compress    Manually trigger the TimescaleDB compression background job.
  --help
```

### Diagnostic Report (always runs)

Queries and prints:

| Metric | Source |
|---|---|
| Total DB size | `pg_database_size()` |
| Per-table size (raw hypertables) | `extensions.hypertable_size()` |
| Per-table size (continuous aggregates) | `extensions.hypertable_size()` on materialized hypertable |
| Uncompressed vs compressed chunk counts | `timescaledb_information.chunks` (schema-qualify explicitly; may not be in default search path) |
| Date range of raw data | `min(time)` / `max(time)` on raw tables |
| Date range of hourly aggregates | `min(time)` / `max(time)` on cagg views |

Output is printed to stdout as a formatted report. Uses `logging` consistent with the project style.

### `--compress`

Looks up the compression policy job IDs for `branch_energy` and `main_energy` via:

```sql
SELECT j.id
FROM timescaledb_information.jobs j
JOIN timescaledb_information.hypertables h
  ON j.hypertable_name = h.hypertable_name
WHERE j.proc_name = 'policy_compression'
  AND h.hypertable_name IN ('branch_energy', 'main_energy')
```

Then calls `SELECT run_job(<id>)` for each. Reprints the diagnostic report after completion so before/after sizes are visible in the same run.

---

## Environment Variables

| Var | Used by | Notes |
|---|---|---|
| `SPAN_IP` | `main.py` | Existing |
| `SPAN_API_KEY` | `main.py` | Existing |
| `SUPABASE_URL` | `main.py` | Existing (REST API) |
| `SUPABASE_KEY` | `main.py` | Existing (REST API) |
| `DATABASE_URL` | `archive.py`, `maintain.py` | New; direct PostgreSQL connection string |

---

## Dependencies

Packages to add to `pyproject.toml`:
- `polars` — new (replaces pandas/ibis for data work)
- `click` — new (CLI framework)

Already present:
- `psycopg2`
- `python-dotenv`

---

## Out of Scope

- Archiving raw second-scale data on a recurring basis (no long-term use case identified)
- Scheduling (user will invoke scripts manually or via cron)
- Modifying `main.py` polling frequency or schema
