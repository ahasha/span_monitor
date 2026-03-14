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
