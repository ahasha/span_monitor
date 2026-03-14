import logging
import os

import click
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """Return DATABASE_URL, constructing it from SUPABASE_ID + SUPABASE_PWD if not set."""
    if url := os.environ.get("DATABASE_URL"):
        return url
    project_id = os.environ["SUPABASE_ID"]
    password = os.environ["SUPABASE_PWD"]
    return f"postgresql://postgres:{password}@db.{project_id}.supabase.co:5432/postgres"


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(get_database_url())


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
                       ('public.' || quote_ident(hypertable_name))::regclass
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
                       (quote_ident(materialization_hypertable_schema)
                        || '.' || quote_ident(materialization_hypertable_name))::regclass
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
    """Compress all eligible uncompressed chunks on raw tables older than 1 day."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT chunk_schema, chunk_name, hypertable_name
            FROM timescaledb_information.chunks
            WHERE hypertable_name IN ('branch_energy', 'main_energy')
              AND NOT is_compressed
              AND range_end < now() - INTERVAL '1 day'
            ORDER BY hypertable_name, range_start
        """)
        chunks = cur.fetchall()

    if not chunks:
        logger.info("No uncompressed chunks eligible for compression.")
        return

    with conn.cursor() as cur:
        for schema, chunk, table_name in chunks:
            chunk_ref = f"{schema}.{chunk}"
            logger.info(f"Compressing chunk {chunk_ref} ({table_name})...")
            cur.execute(
                "SELECT compress_chunk(%s::regclass)",
                (chunk_ref,),
            )
        conn.commit()

    logger.info(f"Compressed {len(chunks)} chunk(s).")


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
