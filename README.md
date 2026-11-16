# SPAN monitor

A background service that reads a SPAN electrical panel through the SPAN API
(the panel's local MQTT broker) and logs energy data to Supabase (TimescaleDB)
every few seconds.

## Two deployment paths — run only one at a time

| Path | Entrypoint | Use when |
|---|---|---|
| Home Assistant app | `addon-run.sh` (via `config.yaml`) | Normal operation. Always-on, self-healing, alerting. |
| macOS | `run.sh` | Development, or as a fallback if the Pi is down. |

**Running both at once corrupts data.** Two writers produce double the rows per
tick at near-identical timestamps, and the hourly continuous aggregates built
from those rows are the permanent historical record.

### Home Assistant app

Requires Home Assistant OS with Supervisor, on `aarch64`. Install by cloning
this repository into `/addons` on the Home Assistant host — the Studio Code
Server or Advanced SSH app both give you a terminal there:

```bash
git clone https://github.com/ahasha/span_monitor /addons/span-monitor
```

Then **Settings → Apps → ⟳ refresh → Local apps → SPAN Monitor → Install**,
fill in the configuration, and enable *Start on boot* and *Watchdog*.

To update: `git pull` in that directory, then **Rebuild** on the app page.

See `DOCS.md` for the configuration reference and alerting setup.

### macOS

Requires a `.env` with `SPAN_IP`, `SPAN_PASSPHRASE`, `SUPABASE_URL`, `SUPABASE_KEY`.
Credentials obtained from the panel are cached in `~/.span_monitor_auth.json`
(override with `SPAN_AUTH_CACHE`). Set `SPAN_DRY_RUN=1` to log rows without
inserting them.

```bash
uv sync
./run.sh
```

## Other scripts

- `maintain.py` — storage diagnostics and manual compression
- `archive.py` — incremental parquet export of the hourly aggregates
- `compare_dry_run.py` — compare a 2.0.0 dry-run log with the rows the live app is writing (read-only)

Both need the `tools` dependency group, installed by default with `uv sync`.
