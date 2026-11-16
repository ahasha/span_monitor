"""Compare a 2.0.0 dry-run log against the rows 1.x is writing to Supabase.

Run the new monitor in dry-run mode while the Home Assistant app (1.x)
keeps writing, saving its output:

    SPAN_DRY_RUN=1 HEALTH_PORT=8199 uv run python main.py 2>&1 | tee dryrun.log

then, any time within 3 days (raw-table retention):

    uv run python compare_dry_run.py dryrun.log

Read-only. It runs two kinds of SELECT against Supabase, both limited to the
dry run's time window (padded by the match tolerance): `main_energy` rows and
`branch_energy` rows, paged 1000 at a time. Nothing is written.

Each dry-run tick is matched to the nearest v1 tick within --tolerance
seconds. v1 stored a 2-pole circuit as one row per breaker space; those legs
are summed per circuit (via branch_to_circuit.csv) before comparing with the
single 2.0.0 row. Differences are dry-run minus v1.
"""
import ast
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click
import dotenv
import polars as pl

dotenv.load_dotenv()

MAIN_FIELDS = [
    "instant_grid_power_w",
    "main_meter_consumed_energy_wh",
    "main_meter_produced_energy_wh",
    "feed_through_power_w",
    "feed_through_consumed_energy_wh",
    "feed_through_produced_energy_wh",
]
BRANCH_FIELDS = ["instant_power_w", "imported_active_energy_wh", "exported_active_energy_wh"]

MAIN_RE = re.compile(r"DRY RUN main=(\{.*\}) branches=")
BRANCH_RE = re.compile(
    r"DRY RUN branch (\d+): relay=(\S+) power_w=(\S+) imported_wh=(\S+) exported_wh=(\S+)")


def _num(s: str) -> float | None:
    return None if s == "None" else float(s)


def _utc(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(UTC)


def parse_dry_run_log(lines) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (main rows, branch rows) from a dry-run log.

    Branch lines carry no timestamp; each belongs to the tick of the main
    line before it.
    """
    mains, branches, tick = [], [], None
    for line in lines:
        if m := MAIN_RE.search(line):
            row = ast.literal_eval(m.group(1))
            tick = _utc(row["time"])
            mains.append({"time": tick, **{f: row.get(f) for f in MAIN_FIELDS}})
        elif (b := BRANCH_RE.search(line)) and tick is not None:
            branches.append({
                "time": tick,
                "branch_id": int(b.group(1)),
                "instant_power_w": _num(b.group(3)),
                "imported_active_energy_wh": _num(b.group(4)),
                "exported_active_energy_wh": _num(b.group(5)),
            })
    main_schema = {"time": pl.Datetime("us", "UTC"), **{f: pl.Float64 for f in MAIN_FIELDS}}
    branch_schema = {"time": pl.Datetime("us", "UTC"), "branch_id": pl.Int64,
                     **{f: pl.Float64 for f in BRANCH_FIELDS}}
    return pl.DataFrame(mains, schema=main_schema), pl.DataFrame(branches, schema=branch_schema)


def circuit_keys(mapping: pl.DataFrame) -> pl.DataFrame:
    """branch_id -> circuit_key (the circuit's lowest space, which 2.0.0 writes under) and name."""
    return (
        mapping.with_columns(pl.col("branch_id").min().over("circuit_id").alias("circuit_key"))
        .select(pl.col("branch_id").cast(pl.Int64), pl.col("circuit_key").cast(pl.Int64), "name")
    )


def v1_branches_by_circuit(v1: pl.DataFrame, keys: pl.DataFrame) -> pl.DataFrame:
    """Sum v1's per-space rows into one row per circuit per tick. Unmapped spaces are dropped."""
    return (
        v1.join(keys.select("branch_id", "circuit_key"), on="branch_id", how="inner")
        .group_by("time", "circuit_key")
        .agg(pl.col(f).sum() for f in BRANCH_FIELDS)
        .sort("circuit_key", "time")
    )


def _diffs(joined: pl.DataFrame, fields: list[str]) -> pl.DataFrame:
    return joined.with_columns((pl.col(f) - pl.col(f"{f}_v1")).alias(f"{f}_diff") for f in fields)


def compare_main(dry: pl.DataFrame, v1: pl.DataFrame, tolerance_s: float) -> pl.DataFrame:
    v1 = v1.select("time", *MAIN_FIELDS).rename({f: f"{f}_v1" for f in MAIN_FIELDS})
    joined = dry.sort("time").join_asof(
        v1.with_columns(pl.col("time").alias("time_v1")).sort("time"),
        on="time", strategy="nearest", tolerance=timedelta(seconds=tolerance_s),
    ).filter(pl.col("time_v1").is_not_null())
    return _diffs(joined, MAIN_FIELDS)


def compare_branches(dry: pl.DataFrame, v1_circuits: pl.DataFrame, keys: pl.DataFrame,
                     tolerance_s: float) -> pl.DataFrame:
    names = keys.select("circuit_key", "name").unique("circuit_key")
    v1 = v1_circuits.rename({f: f"{f}_v1" for f in BRANCH_FIELDS}).with_columns(
        pl.col("time").alias("time_v1"))
    joined = (
        dry.rename({"branch_id": "circuit_key"}).sort("time")
        .join_asof(v1.sort("time"), on="time", by="circuit_key", strategy="nearest",
                   tolerance=timedelta(seconds=tolerance_s), check_sortedness=False)
        .filter(pl.col("time_v1").is_not_null())
        .join(names, on="circuit_key", how="left")
        .sort("circuit_key", "time")
    )
    return _diffs(joined, BRANCH_FIELDS)


def counter_excess(df: pl.DataFrame, diff_col: str, power_cols: list[str], counter_tol: float) -> pl.Series:
    """Wh by which each counter difference exceeds what timing explains.

    The two readings are taken a few seconds apart, and a circuit drawing P
    watts moves its counter by P * seconds / 3600 Wh in that time; P is the
    larger of the two power readings, since load can change in between.
    counter_tol covers the rest: both sources report counters in 0.5 Wh
    steps, and the row timestamps are only approximately when each value was
    read. 0 means within tolerance.
    """
    seconds_apart = (pl.col("time") - pl.col("time_v1")).dt.total_microseconds().abs() / 1e6
    power = pl.max_horizontal(pl.col(c).abs().fill_null(0) for c in power_cols)
    allowed = counter_tol + power * seconds_apart / 3600
    return df.select(
        (pl.col(diff_col).abs() - allowed).clip(lower_bound=0).alias("excess")
    )["excess"]


def fetch_v1(supabase, table: str, columns: list[str], start: datetime, end: datetime) -> pl.DataFrame:
    rows, page = [], 1000
    while True:
        batch = (
            supabase.table(table).select(",".join(columns))
            .gte("time", start.isoformat()).lte("time", end.isoformat())
            .order("time").range(len(rows), len(rows) + page - 1)
            .execute().data
        )
        rows.extend(batch)
        if len(batch) < page:
            break
    df = pl.DataFrame(rows, infer_schema_length=None)
    if df.is_empty():
        return df
    return df.with_columns(
        pl.col("time").map_elements(_utc, return_dtype=pl.Datetime("us", "UTC")),
        *(pl.col(c).cast(pl.Float64) for c in columns if c not in ("time", "branch_id")),
    )


def _flag(ok: bool) -> str:
    return "ok   " if ok else "CHECK"


COUNTER_POWER = {
    "main_meter_consumed_energy_wh": ["instant_grid_power_w", "instant_grid_power_w_v1"],
    "main_meter_produced_energy_wh": ["instant_grid_power_w", "instant_grid_power_w_v1"],
    # On this panel the feed-through counters climb at the whole-house rate
    # (observed 2026-10-21: ~7 Wh in 4 s at 7 kW grid, 20-70 W feed-through
    # power), identically in v1 and 2.0.0, so grid power is what scales them.
    "feed_through_consumed_energy_wh": ["instant_grid_power_w", "instant_grid_power_w_v1"],
    "feed_through_produced_energy_wh": ["instant_grid_power_w", "instant_grid_power_w_v1"],
}


def report_main(m: pl.DataFrame, counter_tol: float) -> None:
    click.echo(f"\n== main_energy: {m.height} matched ticks ==")
    click.echo(f"{'field':34} {'dry mean':>14} {'v1 mean':>14} {'mean diff':>11} {'max |diff|':>11}")
    for f in MAIN_FIELDS:
        d = m[f"{f}_diff"].drop_nulls()
        if d.is_empty():
            click.echo(f"{f:34} (no values)")
            continue
        max_abs = d.abs().max()
        if f.endswith("_wh"):
            ok = counter_excess(m, f"{f}_diff", COUNTER_POWER[f], counter_tol).max() == 0
        else:  # power: noisy tick to tick, so judge the mean
            ok = abs(d.mean()) <= max(20.0, 0.1 * abs(m[f"{f}_v1"].mean()))
        click.echo(f"{_flag(ok)} {f:28} {m[f].mean():14.2f} {m[f'{f}_v1'].mean():14.2f} "
                   f"{d.mean():11.2f} {max_abs:11.2f}")


def report_branches(b: pl.DataFrame, dry: pl.DataFrame, keys: pl.DataFrame, counter_tol: float) -> None:
    click.echo(f"\n== branch_energy by circuit: {b['time'].n_unique()} matched ticks ==")
    click.echo(f"{'circuit':44} {'n':>4} {'dry W':>9} {'v1 W':>9} {'mean|dW|':>9} "
               f"{'max|d imp|':>10} {'max|d exp|':>10}")
    b = b.with_columns(
        counter_excess(b, "imported_active_energy_wh_diff", ["instant_power_w", "instant_power_w_v1"], counter_tol).alias("x_imp"),
        counter_excess(b, "exported_active_energy_wh_diff", ["instant_power_w", "instant_power_w_v1"], counter_tol).alias("x_exp"),
    )
    summary = (
        b.group_by("circuit_key", "name").agg(
            pl.len().alias("n"),
            pl.col("instant_power_w").mean().alias("dry_w"),
            pl.col("instant_power_w_v1").mean().alias("v1_w"),
            pl.col("instant_power_w_diff").abs().mean().alias("mad_w"),
            pl.col("imported_active_energy_wh_diff").abs().max().alias("d_imp"),
            pl.col("exported_active_energy_wh_diff").abs().max().alias("d_exp"),
            pl.max_horizontal(pl.col("x_imp").max(), pl.col("x_exp").max()).alias("excess"),
        ).sort("circuit_key")
    )
    for r in summary.iter_rows(named=True):
        counters_ok = (r["excess"] or 0) == 0
        power_ok = r["mad_w"] <= max(20.0, 0.1 * abs(r["v1_w"]))
        label = f"{r['circuit_key']:>2} {r['name']}"[:38]
        click.echo(f"{_flag(counters_ok and power_ok)} {label:38} {r['n']:4d} {r['dry_w']:9.1f} "
                   f"{r['v1_w']:9.1f} {r['mad_w']:9.1f} {r['d_imp']:10.2f} {r['d_exp']:10.2f}")

    unmatched = set(dry["branch_id"].unique()) - set(b["circuit_key"].unique())
    if unmatched:
        click.echo(f"\nDry-run branch ids with no v1 match: {sorted(unmatched)} "
                   "(not in branch_to_circuit.csv, or no v1 tick within tolerance)")
    unmapped = sorted(set(range(1, 33)) - set(keys["branch_id"]))
    click.echo(f"v1 spaces not in branch_to_circuit.csv, excluded: {unmapped}")


@click.command()
@click.argument("log_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--mapping", "mapping_path", default=str(Path(__file__).parent / "branch_to_circuit.csv"),
              show_default=True)
@click.option("--tolerance", default=3.0, show_default=True,
              help="Max seconds between a dry-run tick and the v1 tick it is compared with.")
@click.option("--counter-tol", default=2.0, show_default=True,
              help="Wh of counter difference allowed beyond what the power drawn between "
                   "the two readings explains.")
def main(log_path, mapping_path, tolerance, counter_tol):
    from supabase import create_client

    dry_main, dry_branches = parse_dry_run_log(Path(log_path).read_text().splitlines())
    if dry_main.is_empty():
        raise click.ClickException("No 'DRY RUN main=' lines found. Was the monitor run with SPAN_DRY_RUN=1?")
    start = dry_main["time"].min() - timedelta(seconds=tolerance)
    end = dry_main["time"].max() + timedelta(seconds=tolerance)
    click.echo(f"Dry run: {dry_main.height} ticks, {start:%Y-%m-%d %H:%M:%S} to {end:%H:%M:%S} UTC")

    supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    v1_main = fetch_v1(supabase, "main_energy", ["time", *MAIN_FIELDS], start, end)
    v1_branch = fetch_v1(supabase, "branch_energy", ["time", "branch_id", *BRANCH_FIELDS], start, end)
    click.echo(f"v1 (Supabase): {v1_main.height} main_energy rows, {v1_branch.height} branch_energy rows")
    if v1_main.is_empty() or v1_branch.is_empty():
        raise click.ClickException("No v1 rows in that window. Is the Home Assistant app writing? "
                                   "Raw rows are only kept for 3 days.")

    keys = circuit_keys(pl.read_csv(mapping_path))
    report_main(compare_main(dry_main, v1_main, tolerance), counter_tol)
    report_branches(
        compare_branches(dry_branches, v1_branches_by_circuit(v1_branch, keys), keys, tolerance),
        dry_branches, keys, counter_tol,
    )
    click.echo("\nDiffs are dry-run minus v1. Counters are flagged only beyond the energy drawn "
               "between the two readings (power x seconds apart / 3600) plus --counter-tol. "
               "Power is flagged when the mean |diff| exceeds max(20 W, 10%); a cycling load "
               "(oven, water heater) needs a longer run to average out.")


if __name__ == "__main__":
    main()
