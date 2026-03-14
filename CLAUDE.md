# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A background service that polls a SPAN electrical panel API every second and logs energy data to Supabase (TimescaleDB backend). The `tesla-sdk/` subdirectory is a bundled OAuth2 SDK for Tesla's API, used for potential integration with Tesla energy products.

## Development Commands

This project uses Poetry (not `uv`):

```bash
# Install dependencies
poetry install

# Run the monitor
poetry run python main.py

# Run as background service (prevents laptop sleep via caffeinate)
./run.sh
```

## Environment Variables

Required in a `.env` file:
- `SPAN_IP` — Local IP address of the SPAN panel
- `SPAN_API_KEY` — API key for the SPAN panel
- `SUPABASE_URL` — Supabase project URL
- `SUPABASE_KEY` — Supabase service role key

## Architecture

**`main.py`** is the entire service. It:
1. Polls `http://{SPAN_IP}/api/v1/panel` every second
2. Extracts aggregate meter data and per-circuit (branch) data
3. Inserts two rows per tick into Supabase: one into `main_energy`, one per circuit into `branch_energy`
4. Wraps API calls with `@retry_on_connection_error()` — exponential backoff for network resilience

**Database** (Supabase + TimescaleDB):
- `main_energy` — aggregate grid/panel data, compressed hypertable
- `branch_energy` — per-circuit measurements, compressed hypertable
- Continuous aggregate materialized views: `branch_energy_hourly`, `main_energy_hourly`
- Raw data retention: 1 week; hourly aggregates persist indefinitely
- Schema in `database_setup.sql` and `supabase/migrations/`

**`tesla-sdk/`** — Custom OAuth2 SDK for Tesla's owner API. Uses PKCE-based OAuth2 flow with token caching. Classes: `Client` (auth), `Account`, `Vehicle`, `Energy`. Not currently used by the monitor service.

## Notes

- `run.sh` is hardcoded to the local machine path — update if running elsewhere
- `poetry.lock` is committed; use `poetry install` not `pip install`
- The `scratch.ipynb` and `span.ipynb` notebooks are for ad-hoc analysis of the logged data
