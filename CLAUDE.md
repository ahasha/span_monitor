# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A background service that reads a SPAN electrical panel through the SPAN API (its local MQTT/Homie broker) and logs energy data to Supabase (TimescaleDB backend) every 5 seconds. The `tesla-sdk/` subdirectory is a bundled OAuth2 SDK for Tesla's API, used for potential integration with Tesla energy products.

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
- `SPAN_PASSPHRASE` — the panel's SPAN API passphrase (`hopPassphrase`, from the SPAN Home app)
- `SUPABASE_URL` — Supabase project URL
- `SUPABASE_KEY` — Supabase service role key

Optional: `SPAN_AUTH_CACHE` (default `~/.span_monitor_auth.json`; HA uses `/data/span_auth.json`),
`MQTT_MAX_SILENCE` (seconds without any MQTT message before ticks fail, default 120),
`SPAN_DRY_RUN=1` (log rows, insert nothing, no heartbeat ping).

## Architecture

**`main.py`** — the service entry point. It:
1. Registers with the panel once (`span_auth.py`) and starts the MQTT client (`span_mqtt.py`)
2. Every `POLL_INTERVAL` seconds (default 5), turns the current `HomieSnapshot` into rows (`extract.py`)
3. Inserts one row into `main_energy` and one row per circuit into `branch_energy`
4. Counts a tick as successful only when the snapshot was ready **and** both inserts landed
5. Wraps network calls with `@retry_on_connection_error()` — capped linear backoff for network resilience

**`span_auth.py`** — `GET /api/v2/status`, `POST /api/v2/auth/register` with the passphrase,
CA-cert download. Caches the broker password (0600) so restarts don't register new clients.

**`span_mqtt.py`** — paho client (TLS to port 8883, auto-reconnect) subscribed to `ebus/5/#`.
A rejected cached password clears the cache and exits the process so the restart re-registers.

**`homie.py`** — `HomieSnapshot`, a thread-safe last-known-value store. Since firmware r202639
the panel does not republish unchanged values, so liveness comes from the connection, the
panel's `$state`, and whether *any* message arrives, never from per-value age.

**`extract.py`** — the v1-era column mapping (evidence in
`docs/superpowers/plans/2026-10-21-span-api-mqtt-migration.md`). Each circuit is written once,
under its lowest breaker space as `branch_id`; v1 wrote 2-pole circuits as one row per space.

**`span_capture.py`** — one-shot LAN tool that captured `tests/fixtures/` (not shipped).

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
- Raw data retention: 3 days; hourly aggregates persist indefinitely
- Schema in `database_setup.sql` and `supabase/migrations/`

**`tesla-sdk/`** — Custom OAuth2 SDK for Tesla's owner API. Uses PKCE-based OAuth2 flow with token caching. Classes: `Client` (auth), `Account`, `Vehicle`, `Energy`. Not currently used by the monitor service.

## Notes

- Two deployment paths, mutually exclusive: the Home Assistant app (`config.yaml` +
  `addon-run.sh`, normal operation) and macOS (`run.sh`, development/fallback).
  Running both at once doubles the rows per tick and corrupts the hourly aggregates.
- `run.sh` is hardcoded to the local machine path — update if running elsewhere
- `uv.lock` is committed; use `uv sync` not `pip install`
- The `scratch.ipynb` and `span.ipynb` notebooks are for ad-hoc analysis of the logged data
