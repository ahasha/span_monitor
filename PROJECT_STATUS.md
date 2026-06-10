# Project Status: Heat Pump vs. Oil Cost Analysis

*Written 2026-06-10. Covers this repo (`span_monitor`) and `~/repos/heat_pump_analysis`.*

**TLDR:** The data collection infrastructure is solid and mature — SPAN panel logging, database maintenance, and parquet archiving all work. The analysis itself is at the exploratory stage: one scatter plot of heat pump consumption vs. degree days exists, but no regression fit yet, and no model on the oil side at all. The biggest immediate issues are that the analysis datasets are stale (they end September 2025) and the pipeline from database to analysis repo is entirely manual.

## What's done

### Data collection (`span_monitor`)

- **`main.py`** — the monitor service. Polls the SPAN panel API every 5 seconds, writes aggregate data to `main_energy` and per-circuit data to `branch_energy` in Supabase (TimescaleDB), with exponential-backoff retry. `run.sh` runs it under `caffeinate`.
- **Database design** — raw hypertables with hourly continuous aggregates (`main_energy_hourly`, `branch_energy_hourly`). In March 2026, after hitting the Supabase free-tier size limit, a migration cut raw retention to 3 days and enabled columnar compression (`supabase/migrations/20260314_retention_and_compression.sql`). The hourly aggregates are the permanent historical record (~18 months of history as of March 2026, growing ~2.5 MB/month).
- **`maintain.py`** — prints a storage diagnostic report (table sizes, chunk compression, date ranges) and can manually trigger compression (`--compress`).
- **`archive.py`** — incrementally exports the hourly aggregate tables to local parquet files (dated, per-table subdirectories), resuming from the max timestamp already on disk. Has tests in `tests/test_archive.py`.
- **`branch_to_circuit.csv`** — maps panel branch IDs to circuit names, including the key one: **"HVAC / Subpanel"**, which identifies heat pump load.

### Data acquisition notebooks (`span_monitor/notebooks/`)

| Notebook | What it does |
|---|---|
| `span.ipynb` | Original prototyping: SPAN API registration/auth, schema design, and the circuit-metadata extraction that produced `branch_to_circuit.csv` |
| `supabase_query.ipynb` | The key extraction step: joins `branch_energy_hourly` to circuit names via ibis, filters to "HVAC / Subpanel", writes `heat_pumps_hourly_average_instant_power.parquet` |
| `solaredge_api.ipynb` | Pulls daily solar production 2020–2025 from the SolarEdge monitoring API → `solar_panels_daily.parquet` |
| `database_exploration.ipynb` | Quick sanity plot of hourly grid power |
| `scratch.ipynb` | Ad-hoc API poking |

### Analysis (`~/repos/heat_pump_analysis`)

A young repo (2 commits) using uv and DVC for data versioning:

- **`oil_consumption_history.csv`** — oil deliveries Nov 2018 – Dec 2023, with gallons, kWh-equivalent, cost, unit price, and MA average price. This is the pre-heat-pump baseline; Dec 2023 marks the end of the oil era.
- **`KOWD_HDD_65F.csv` / `KOWD_CDD_68F.csv`** — daily heating/cooling degree days from degreedays.net for Norwood Memorial Airport (KOWD), Sep 2023 – Sep 2025.
- **`heat_pumps_hourly_average_instant_power.parquet`** — heat pump hourly power, Aug 2024 – Sep 2025 (hand-copied from the span_monitor extraction).
- **`power_by_load.ipynb`** — aggregates heat pump power to daily kWh, joins to HDD/CDD, assigns heating mode (Oct–Apr) vs. cooling mode (May–Sep) by month, and produces an Altair scatter plot of kWh vs. degree days colored by mode. This is the current frontier of the analysis.

## What remains to be done

1. **Refresh the data.** Analytical datasets end ~September 2025, so the winter of 2025–26 — the most valuable heating season of SPAN data — isn't in the analysis yet. Run `archive.py` (or rerun the `supabase_query.ipynb` extraction), re-download HDD/CDD CSVs through the present, and update the DVC-tracked files. Run `maintain.py` first to confirm the monitor has been logging continuously.

2. **Fit the heat-pump-side regression.** Daily heat pump kWh regressed on HDD (and separately CDD), e.g. with statsmodels. The intercept gives non-weather baseload on that circuit; the HDD slope gives kWh per heating degree day. Open issues: the "HVAC / Subpanel" circuit may include non-heat-pump loads, and the month-based heating/cooling split misassigns shoulder days that have both HDD and CDD.

3. **Build the oil-side counterfactual model.** Nothing exists yet. Natural approach: for each delivery interval in `oil_consumption_history.csv`, sum HDD between delivery dates and regress gallons on accumulated HDD. That yields gallons-per-HDD, applied to heat-pump-era degree days to get counterfactual gallons. Gap: HDD data starts Sep 2023 but oil deliveries span 2018–2023, so historical KOWD degree days back to 2018 are needed (degreedays.net provides this).

4. **Price both sides.** The oil CSV has prices through 2023; current oil prices are needed for the counterfactual. On the electric side, the actual $/kWh rate is needed — and the solar data matters here, since net metering changes the marginal cost of heat pump electricity. The solar parquet exists but is integrated into nothing.

5. **Automate the pipeline.** Current flow: notebook in span_monitor → parquet in repo root → hand-copy to heat_pump_analysis → DVC-track. DVC is initialized but has no pipeline stages. A small script in heat_pump_analysis that pulls directly from Supabase (or from `archive.py` output) and does the circuit join would make refreshes routine.

## Suggested order of attack

Refresh data first (winter 2025–26 roughly doubles the heating-season sample). Then the oil regression — self-contained and zero progress so far. Then the heat pump regression, then pricing. Automation whenever the manual steps get annoying.

**Structural caveat:** the comparison is gallons-per-HDD × current oil price vs. heat-pump-kWh-per-HDD × marginal electricity cost. If the old oil boiler also supplied domestic hot water, the oil regression's intercept (non-weather oil use) must be handled deliberately rather than dropped, or the counterfactual will understate oil costs.
