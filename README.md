# SPAN monitor

A background service that polls a SPAN electrical panel every few seconds and
logs energy data to Supabase (TimescaleDB).

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

Requires a `.env` with `SPAN_IP`, `SPAN_API_KEY`, `SUPABASE_URL`, `SUPABASE_KEY`.

```bash
uv sync
./run.sh
```

## Other scripts

- `maintain.py` — storage diagnostics and manual compression
- `archive.py` — incremental parquet export of the hourly aggregates

Both need the `tools` dependency group, installed by default with `uv sync`.
