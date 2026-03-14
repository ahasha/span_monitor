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

        # Date ranges for raw tables and caggs
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
        logger.warning(
            "No compression jobs found for branch_energy / main_energy. "
            "Was the migration applied?"
        )
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
