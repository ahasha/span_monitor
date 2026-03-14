# DB Maintenance Scripts Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce and manage Supabase storage by shortening raw data retention, enabling compression, archiving hourly aggregates to local parquet, and providing a diagnostic/maintenance CLI.

**Architecture:** Three deliverables: a SQL migration applied once to Supabase, `archive.py` that incrementally exports hourly aggregate views to date-stamped parquet files in a local directory, and `maintain.py` that reports storage diagnostics and optionally triggers TimescaleDB compression. Both scripts connect via direct PostgreSQL (`psycopg2` + `DATABASE_URL` env var).

**Tech Stack:** Python 3.11, psycopg2, polars, click, python-dotenv, TimescaleDB (via Supabase)

**Spec:** `docs/superpowers/specs/2026-03-14-db-maintenance-scripts-design.md`

---

## Chunk 1: Dependencies and SQL Migration

### Task 1: Add polars and click dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] Add `polars` and `click` to `pyproject.toml` under `[tool.poetry.dependencies]`. Leave `pandas` in place — it is used by `supabase_query.ipynb`, `database_exploration.ipynb`, and `span.ipynb`.

```toml
polars = "^1.0"
click = "^8.1"
```

- [ ] Run `poetry install` and confirm no conflicts:

```bash
poetry install
```

Expected: resolves and installs without errors.

- [ ] Commit:

```bash
git commit -a -m "$(cat <<'EOF'
Add polars and click dependencies
EOF
)"
```

---

### Task 2: Write the SQL migration

**Files:**
- Create: `supabase/migrations/20260314_retention_and_compression.sql`

- [ ] Create the migration file with the following content. This file is idempotent: it removes existing retention policies before adding new ones, and uses `if_not_exists => true` on the compression and retention policies to guard against re-runs. `remove_retention_policy` uses `if_exists` (not `if_not_exists`) — this is the correct parameter name for that function.

```sql
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
```

- [ ] Apply via the Supabase MCP tool (`mcp__supabase__apply_migration`) or via `supabase db push` from the CLI. Verify no errors.

- [ ] Commit:

```bash
git commit -a -m "$(cat <<'EOF'
Add migration: 3-day retention and compression policy on raw tables
EOF
)"
```

---

## Chunk 2: archive.py

### Task 3: Write tests for pure archive logic

**Files:**
- Create: `tests/test_archive.py`

These tests cover the two pure functions that contain real logic: `find_max_local_time` (reads max timestamp across parquet files in a directory) and `make_filename` (generates a dated filename from a DataFrame). DB interaction is not tested here.

- [ ] Create `tests/test_archive.py`:

```python
import polars as pl
import pytest
from datetime import datetime, timezone
from pathlib import Path


# Helpers imported after archive.py exists:
# from archive import find_max_local_time, make_filename


def make_hourly_df(times: list[datetime]) -> pl.DataFrame:
    return pl.DataFrame({"time": times, "value": [1.0] * len(times)})


class TestFindMaxLocalTime:
    def test_returns_none_when_no_files(self, tmp_path):
        from archive import find_max_local_time
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result is None

    def test_returns_max_across_single_file(self, tmp_path):
        from archive import find_max_local_time
        t1 = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        df = make_hourly_df([t1, t2])
        df.write_parquet(tmp_path / "branch_energy_hourly_20260101_20260101.parquet")
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result == t2

    def test_returns_max_across_multiple_files(self, tmp_path):
        from archive import find_max_local_time
        t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        t3 = datetime(2026, 3, 1, tzinfo=timezone.utc)
        make_hourly_df([t1, t2]).write_parquet(
            tmp_path / "branch_energy_hourly_20260101_20260201.parquet"
        )
        make_hourly_df([t3]).write_parquet(
            tmp_path / "branch_energy_hourly_20260301_20260301.parquet"
        )
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result == t3

    def test_ignores_files_for_other_tables(self, tmp_path):
        from archive import find_max_local_time
        t = datetime(2026, 3, 1, tzinfo=timezone.utc)
        make_hourly_df([t]).write_parquet(
            tmp_path / "main_energy_hourly_20260301_20260301.parquet"
        )
        result = find_max_local_time(tmp_path, "branch_energy_hourly")
        assert result is None


class TestMakeFilename:
    def test_formats_date_range_from_dataframe(self):
        from archive import make_filename
        t1 = datetime(2024, 8, 26, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 14, tzinfo=timezone.utc)
        df = make_hourly_df([t1, t2])
        result = make_filename("branch_energy_hourly", df)
        assert result == "branch_energy_hourly_20240826_20260314.parquet"

    def test_single_day_range(self):
        from archive import make_filename
        t = datetime(2026, 3, 14, tzinfo=timezone.utc)
        df = make_hourly_df([t])
        result = make_filename("main_energy_hourly", df)
        assert result == "main_energy_hourly_20260314_20260314.parquet"
```

- [ ] Run to confirm they fail cleanly (module not found):

```bash
poetry run pytest tests/test_archive.py -v
```

Expected: `ModuleNotFoundError: No module named 'archive'`

---

### Task 4: Implement find_max_local_time and make_filename

**Files:**
- Create: `archive.py`

- [ ] Create `archive.py` with just the two functions under test (no CLI yet). All imports — including `click` and `psycopg2` added in Task 5 — go at the top here so the file never needs import reordering:

```python
import logging
import os
from datetime import datetime
from pathlib import Path

import click
import polars as pl
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

VALID_TABLES = [
    "branch_energy_hourly",
    "main_energy_hourly",
    "branch_energy",
    "main_energy",
]


def find_max_local_time(output_dir: Path, table: str) -> datetime | None:
    """Return max(time) across all parquet files for table, or None if none exist."""
    files = sorted(output_dir.glob(f"{table}_*.parquet"))
    if not files:
        return None
    max_time = (
        pl.scan_parquet(files)
        .select(pl.col("time").max())
        .collect()["time"][0]
    )
    return max_time


def make_filename(table: str, df: pl.DataFrame) -> str:
    """Generate a dated filename from min/max time of fetched data."""
    min_date = df["time"].min().strftime("%Y%m%d")
    max_date = df["time"].max().strftime("%Y%m%d")
    return f"{table}_{min_date}_{max_date}.parquet"
```

- [ ] Run tests and confirm they pass:

```bash
poetry run pytest tests/test_archive.py -v
```

Expected: all 6 tests PASS.

- [ ] Commit:

```bash
git commit -a -m "$(cat <<'EOF'
Add archive.py with find_max_local_time and make_filename, with tests
EOF
)"
```

---

### Task 5: Implement fetch_table and the Click CLI

**Files:**
- Modify: `archive.py`

- [ ] Add `fetch_table` and the Click CLI to `archive.py`. Append below the existing functions (imports are already at the top from Task 4):

```python
def fetch_table(
    conn: psycopg2.extensions.connection,
    table: str,
    after: datetime | None,
) -> pl.DataFrame | None:
    """Fetch rows from table between after (exclusive) and the current complete hour.

    Returns None if no rows are available.
    """
    if after is not None:
        query = f"""
            SELECT * FROM public.{table}
            WHERE time > %s AND time < date_trunc('hour', now())
            ORDER BY time
        """
        params: tuple = (after,)
    else:
        query = f"""
            SELECT * FROM public.{table}
            WHERE time < date_trunc('hour', now())
            ORDER BY time
        """
        params = ()

    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description is None:
            return None
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if not rows:
        return None

    data = {col: [row[i] for row in rows] for i, col in enumerate(cols)}
    return pl.DataFrame(data)


@click.command()
@click.option(
    "--output-dir",
    default="./archive",
    type=click.Path(),
    help="Directory for parquet output files.",
    show_default=True,
)
@click.option(
    "--full",
    is_flag=True,
    help="Ignore local archive state and download entire history.",
)
@click.option(
    "--table",
    "tables",
    multiple=True,
    type=click.Choice(VALID_TABLES),
    default=["branch_energy_hourly", "main_energy_hourly"],
    show_default=True,
    help="Table to archive. May be specified multiple times.",
)
def main(output_dir: str, full: bool, tables: tuple[str, ...]) -> None:
    """Incrementally archive Supabase hourly aggregate tables to local parquet files."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        for table in tables:
            after = None if full else find_max_local_time(output_path, table)
            if after is not None:
                logger.info(f"{table}: fetching rows after {after}")
            else:
                logger.info(f"{table}: fetching full history")

            df = fetch_table(conn, table, after)
            if df is None or df.is_empty():
                logger.info(f"{table}: already up to date, nothing to write")
                continue

            filename = make_filename(table, df)
            out_path = output_path / filename
            df.write_parquet(out_path)
            logger.info(f"{table}: wrote {len(df)} rows ({df['time'].min()} – {df['time'].max()}) to {out_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] Smoke-test the CLI help (no DB required):

```bash
poetry run python archive.py --help
```

Expected output includes `--output-dir`, `--full`, `--table` options with descriptions.

- [ ] Smoke-test against the real database (requires `DATABASE_URL` in `.env`). Do a dry run by checking `--full` on `main_energy_hourly` only (smaller table):

```bash
poetry run python archive.py --full --table main_energy_hourly --output-dir ./archive_test
```

Expected: logs rows written, file appears at `archive_test/main_energy_hourly_YYYYMMDD_YYYYMMDD.parquet`.

- [ ] Verify the file loads correctly:

```bash
poetry run python -c "import polars as pl; print(pl.scan_parquet('archive_test/*.parquet').collect())"
```

Expected: DataFrame with correct columns and row count.

- [ ] Commit:

```bash
git commit -a -m "$(cat <<'EOF'
Add fetch_table and Click CLI to archive.py
EOF
)"
```

---

## Chunk 3: maintain.py

### Task 6: Implement maintain.py

**Files:**
- Create: `maintain.py`

No unit tests for `maintain.py` — it is entirely DB interaction and report printing. Verified by running against the real database.

- [ ] Create `maintain.py`:

```python
import logging
import os

import click
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ["DATABASE_URL"])


def print_report(conn: psycopg2.extensions.connection) -> None:
    """Query and print a storage diagnostic report."""
    with conn.cursor() as cur:
        # Total DB size
        cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
        total_size = cur.fetchone()[0]

        # Raw hypertable sizes
        cur.execute("""
            SELECT hypertable_name,
                   pg_size_pretty(extensions.hypertable_size(
                       format('public.%%I', hypertable_name)::regclass
                   )) AS total_size
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = 'public'
            ORDER BY hypertable_name
        """)
        raw_sizes = cur.fetchall()

        # Continuous aggregate sizes
        cur.execute("""
            SELECT view_name,
                   pg_size_pretty(extensions.hypertable_size(
                       format('%%I.%%I',
                           materialization_hypertable_schema,
                           materialization_hypertable_name
                       )::regclass
                   )) AS cagg_size
            FROM timescaledb_information.continuous_aggregates
            ORDER BY view_name
        """)
        cagg_sizes = cur.fetchall()

        # Chunk compression stats
        cur.execute("""
            SELECT hypertable_name,
                   COUNT(*) FILTER (WHERE is_compressed) AS compressed,
                   COUNT(*) FILTER (WHERE NOT is_compressed) AS uncompressed
            FROM timescaledb_information.chunks
            WHERE hypertable_schema = 'public'
            GROUP BY hypertable_name
            ORDER BY hypertable_name
        """)
        chunk_stats = cur.fetchall()

        # Date ranges for raw tables
        cur.execute("""
            SELECT 'branch_energy' AS tbl, min(time) AS oldest, max(time) AS newest
            FROM public.branch_energy
            UNION ALL
            SELECT 'main_energy', min(time), max(time)
            FROM public.main_energy
            UNION ALL
            SELECT 'branch_energy_hourly', min(time), max(time)
            FROM public.branch_energy_hourly
            UNION ALL
            SELECT 'main_energy_hourly', min(time), max(time)
            FROM public.main_energy_hourly
        """)
        date_ranges = cur.fetchall()

    print("\n=== Supabase Storage Report ===")
    print(f"Total DB size: {total_size}\n")

    print("Raw hypertables:")
    for name, size in raw_sizes:
        print(f"  {name}: {size}")

    print("\nContinuous aggregates:")
    for name, size in cagg_sizes:
        print(f"  {name}: {size}")

    print("\nChunk compression:")
    for name, compressed, uncompressed in chunk_stats:
        print(f"  {name}: {compressed} compressed, {uncompressed} uncompressed")

    print("\nDate ranges:")
    for tbl, oldest, newest in date_ranges:
        print(f"  {tbl}: {oldest} – {newest}")
    print()


def run_compression(conn: psycopg2.extensions.connection) -> None:
    """Trigger TimescaleDB compression background jobs for raw tables."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT j.id, j.hypertable_name
            FROM timescaledb_information.jobs j
            JOIN timescaledb_information.hypertables h
              ON j.hypertable_name = h.hypertable_name
            WHERE j.proc_name = 'policy_compression'
              AND h.hypertable_name IN ('branch_energy', 'main_energy')
        """)
        jobs = cur.fetchall()

    if not jobs:
        logger.warning("No compression jobs found for branch_energy / main_energy. "
                       "Was the migration applied?")
        return

    with conn.cursor() as cur:
        for job_id, table_name in jobs:
            logger.info(f"Running compression job {job_id} for {table_name}...")
            cur.execute("SELECT run_job(%s)", (job_id,))
        conn.commit()

    logger.info("Compression jobs complete.")


@click.command()
@click.option(
    "--compress",
    is_flag=True,
    help="Trigger TimescaleDB compression jobs for raw tables.",
)
def main(compress: bool) -> None:
    """Print Supabase storage diagnostics and optionally trigger compression."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = get_connection()
    try:
        print_report(conn)
        if compress:
            run_compression(conn)
            print("--- After compression ---")
            print_report(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] Smoke-test the CLI help:

```bash
poetry run python maintain.py --help
```

Expected: `--compress` option described.

- [ ] Run the diagnostic report against the real database:

```bash
poetry run python maintain.py
```

Expected: prints storage report with DB size, per-table sizes, chunk stats, date ranges. No errors.

- [ ] Commit:

```bash
git commit -a -m "$(cat <<'EOF'
Add maintain.py: storage diagnostics and compression trigger
EOF
)"
```

---

### Task 7: End-to-end archive run and cleanup

- [ ] Run a full archive of both default tables:

```bash
poetry run python archive.py --full --output-dir ./archive
```

Expected: two parquet files created in `./archive/`, one per table.

- [ ] Verify files load cleanly together:

```bash
poetry run python -c "
import polars as pl
from pathlib import Path
for f in sorted(Path('archive').glob('*.parquet')):
    df = pl.read_parquet(f)
    print(f, df.shape, df['time'].min(), '-', df['time'].max())
"
```

- [ ] Add `archive/` to `.gitignore` (parquet data files should not be committed):

```
archive/
```

- [ ] Run the compression-enabled maintain to confirm the migration took effect:

```bash
poetry run python maintain.py --compress
```

Expected: prints before report, logs compression jobs running, prints after report showing compressed chunk counts increased.

- [ ] Final commit:

```bash
git commit -a -m "$(cat <<'EOF'
Add archive/ to gitignore; verify end-to-end archive and compression
EOF
)"
```
