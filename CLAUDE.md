# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A background service that polls a SPAN electrical panel API every 5 seconds and logs energy data to Supabase (TimescaleDB backend). The `tesla-sdk/` subdirectory is a bundled OAuth2 SDK for Tesla's API, used for potential integration with Tesla energy products.

## Development Commands

This project uses `uv`:

```bash
# Install dependencies
uv sync

# Run the monitor
uv run python main.py

# Run the tests
uv run pytest

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

**`main.py`** — the service entry point. It:
1. Polls `http://{SPAN_IP}/api/v1/panel` every `POLL_INTERVAL` seconds (default 5)
2. Extracts aggregate meter data and per-circuit (branch) data
3. Inserts one row into `main_energy` and one row per circuit into `branch_energy`
4. Counts a tick as successful only when the panel returned 200 **and** both inserts landed
5. Wraps API calls with `@retry_on_connection_error()` — exponential backoff for network resilience

**`health.py`** — `HealthState` (thread-safe record of time since the last successful
write) and a `/healthz` endpoint on port 8099. Returns 503 once writes go stale, which
is what Home Assistant's watchdog uses to restart a silently-stalled container.

**`notify.py`** — best-effort outbound notifications: a healthchecks.io dead-man switch
ping and a `sensor.span_monitor` status entity. Nothing here may raise, block, or retry
indefinitely; a monitor that can take down the service is worse than none.

**Database** (Supabase + TimescaleDB):
- `main_energy` — aggregate grid/panel data, compressed hypertable
- `branch_energy` — per-circuit measurements, compressed hypertable
- Continuous aggregate materialized views: `branch_energy_hourly`, `main_energy_hourly`
- Raw data retention: 1 week; hourly aggregates persist indefinitely
- Schema in `database_setup.sql` and `supabase/migrations/`

**`tesla-sdk/`** — Custom OAuth2 SDK for Tesla's owner API. Uses PKCE-based OAuth2 flow with token caching. Classes: `Client` (auth), `Account`, `Vehicle`, `Energy`. Not currently used by the monitor service.

## Notes

- Two deployment paths, mutually exclusive: the Home Assistant app (`config.yaml` +
  `addon-run.sh`, normal operation) and macOS (`run.sh`, development/fallback).
  Running both at once doubles the rows per tick and corrupts the hourly aggregates.
- `run.sh` is hardcoded to the local machine path — update if running elsewhere
- `uv.lock` is committed; use `uv sync` not `pip install`
- The `scratch.ipynb` and `span.ipynb` notebooks are for ad-hoc analysis of the logged data
