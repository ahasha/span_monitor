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


def find_max_local_time(table_dir: Path) -> datetime | None:
    """Return max(time) across all parquet files in table_dir, or None if none exist."""
    files = sorted(table_dir.glob("*.parquet"))
    if not files:
        return None
    max_time = (
        pl.scan_parquet(files)
        .select(pl.col("time").max())
        .collect()["time"][0]
    )
    return max_time


def make_filename(df: pl.DataFrame) -> str:
    """Generate a dated filename from min/max time of fetched data."""
    min_date = df["time"].min().strftime("%Y%m%d")
    max_date = df["time"].max().strftime("%Y%m%d")
    return f"{min_date}_{max_date}.parquet"


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
    help="Root directory for parquet output. Each table gets a subdirectory.",
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

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        for table in tables:
            table_dir = output_path / table
            table_dir.mkdir(parents=True, exist_ok=True)

            after = None if full else find_max_local_time(table_dir)
            if after is not None:
                logger.info(f"{table}: fetching rows after {after}")
            else:
                logger.info(f"{table}: fetching full history")

            df = fetch_table(conn, table, after)
            if df is None or df.is_empty():
                logger.info(f"{table}: already up to date, nothing to write")
                continue

            filename = make_filename(df)
            out_path = table_dir / filename
            df.write_parquet(out_path)
            logger.info(f"{table}: wrote {len(df)} rows ({df['time'].min()} – {df['time'].max()}) to {out_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
