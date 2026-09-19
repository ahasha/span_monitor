# Design: Home Assistant App for Continuous SPAN Monitoring

**Date:** 2026-09-19
**Status:** Approved

## Context

`main.py` currently runs on Alex's MacBook via `run.sh` (`caffeinate -i -s nohup uv run python main.py &`). This produces data gaps whenever the laptop sleeps, reboots, loses Wi-Fi, or leaves the house. Since the SPAN panel is only reachable on the home LAN, the monitor must run on an always-on device inside the house.

The target host is a Raspberry Pi running Home Assistant OS:

```
Version                core-2026.9.3
Installation type      Home Assistant OS
Supervisor             true
Container architecture aarch64
Operating system       6.18.39-haos-raspi
Timezone               America/New_York
```

Supervisor is present, so the monitor can be packaged as a Home Assistant **app** (the feature formerly called "add-ons"; renamed in the UI, unchanged in `config.yaml` schema). Supervisor then provides container lifecycle management, a generated configuration UI, log capture, boot-time start, crash restart, and an HTTP watchdog — all of which would otherwise have to be built by hand.

Studio Code Server is already installed and running on the Pi. It mounts `/addons` read-write and provides a terminal with `git`, so no additional SSH or Samba app is required.

### Requirements

1. Run continuously on the Pi, surviving reboots and power cuts without manual intervention.
2. Self-heal from as many failure modes as possible without human action.
3. Alert Alex when — and only when — manual intervention is genuinely required.
4. Expose a health/status entity in Home Assistant. **No** energy data in HA; Supabase remains the single source of truth for the analysis pipeline.
5. Preserve the ability to run the monitor on macOS.

### Non-goals

- Publishing SPAN energy data to HA as entities or to the HA Energy dashboard (explicitly deferred; it is a second output path with its own failure modes).
- Multi-architecture images. The only target is `aarch64`.
- CI/CD and a published app repository. See "Future work".

## Approach

A **local Home Assistant app**, installed by cloning this repository into `/addons/span-monitor` on the Pi. Supervisor builds the image locally from the repo's `Dockerfile`.

Two alternatives were considered and rejected:

- **App repository with CI-built images in GHCR.** Updates become an "Update" button with no shell access. Rejected for now as infrastructure serving a single installation of a single app; the layout below keeps it a small follow-on.
- **Plain Docker container beside HA.** Rejected because HA OS is a locked appliance: the Docker socket is deliberately awkward to reach, hand-started containers are invisible to Supervisor, and an OS update can remove them. Config management, logging, and watchdog would all have to be rebuilt.

### Defense in depth

Four self-healing layers plus one alerting layer, innermost outward:

| Layer | Mechanism | Catches |
|---|---|---|
| 1 | `@retry_on_connection_error` + catch-all in the poll loop (**exists today**) | Transient network and API errors |
| 2 | `/healthz` endpoint + Supervisor `watchdog` | Process alive but writing nothing |
| 3 | Supervisor `boot: auto` + crash restart | Crashes, reboots, power cuts |
| 4 | healthchecks.io dead-man switch | Everything above having failed: Pi dead, LAN down, ISP down |
| 5 | `sensor.span_monitor` in HA | At-a-glance status on a dashboard (diagnostic, not alerting) |

Layer 4 is the only one that survives the box being gone, which is why alerting lives off-premises rather than in an HA automation.

---

## Section 1: Repository layout and packaging

### New files at the repository root

Supervisor discovers local apps one level deep — `/addons/<folder>/config.yaml` — so the manifest must sit at the repo root rather than in a subdirectory. This also makes the Docker build context the repo root, which is what allows the image to copy `main.py`, `pyproject.toml`, and `uv.lock`.

```
config.yaml        # app manifest: options schema, watchdog, boot policy
Dockerfile         # aarch64 HA base-python image + uv sync
addon-run.sh       # container entrypoint: options.json -> env vars -> exec python
DOCS.md            # setup text shown in the app's Documentation tab
```

**`run.sh` is left untouched** and remains the macOS launcher. The HA convention of naming the entrypoint `run.sh` is only a convention; `CMD` may point anywhere. Naming the container entrypoint `addon-run.sh` preserves both deployment paths without collision.

### Dependency split

The image must not contain `pandas`, `polars`, `psycopg2-binary`, or `ipykernel`. Those belong to `archive.py`, `maintain.py`, and the notebooks; on a Pi they would dominate build time for no benefit.

```toml
dependencies = ["requests", "python-dotenv", "supabase", "httpx"]   # monitor runtime only

[dependency-groups]
dev = ["pytest", "pytest-mock"]
tools = ["click", "psycopg2-binary", "polars"]              # archive.py, maintain.py
notebooks = ["ipykernel", "pandas"]

[tool.uv]
default-groups = ["dev", "tools", "notebooks"]
```

`default-groups` keeps the macOS workflow byte-identical: `uv sync` still installs everything. The container runs `uv sync --frozen --no-default-groups`, installing three packages instead of a scientific stack. `uv.lock` is regenerated once and committed.

Exact version constraints carry over unchanged from the current `dependencies` list.

`httpx` is added explicitly. `main.py` already imports it for `httpx.TransportError` but it is not declared — it arrives transitively via `supabase`. A transitive import that the code depends on directly is a latent breakage, and a slim image is exactly where it would surface.

### Dockerfile

Single-architecture, so the base image is hardcoded:

```dockerfile
FROM ghcr.io/home-assistant/aarch64-base-python:3.13-alpine3.23
```

`build.yaml` is **not** used — it is deprecated, and base-image configuration now lives in the Dockerfile. Multi-arch support later is an `ARG BUILD_FROM`.

The build installs `uv`, runs `uv sync --frozen --no-default-groups`, copies `main.py` and the new modules, and sets `CMD ["/addon-run.sh"]`.

### `config.yaml`

```yaml
name: SPAN Monitor
version: "1.0.0"
slug: span_monitor
description: Polls a SPAN electrical panel and logs energy data to Supabase
arch: [aarch64]
startup: application
boot: auto
init: false
homeassistant_api: true
watchdog: "http://[HOST]:[PORT:8099]/healthz"
ports:
  8099/tcp: 8099
options:
  span_ip: ""
  span_api_key: ""
  supabase_url: ""
  supabase_key: ""
  healthcheck_url: ""
  poll_interval: 5
  stale_threshold: 300
schema:
  span_ip: str
  span_api_key: password
  supabase_url: url
  supabase_key: password
  healthcheck_url: "str?"
  poll_interval: int(1,60)
  stale_threshold: int(60,3600)
```

`init: false` because the HA base images ship s6-overlay. Secrets use the `password` type so the HA UI masks them.

**Open implementation detail:** the docs state `watchdog` does not require a `ports` entry, and that `[PORT:8099]` resolves to the effective mapped port. The behaviour when a port is mapped to `null` (not exposed) is unconfirmed. The port is therefore exposed on 8099, which also allows `curl` from the LAN while debugging. If exposure proves unnecessary during implementation, map it to `null` and confirm the watchdog still fires.

### Configuration flow

`addon-run.sh` reads `/data/options.json` via `bashio::config` and exports `SPAN_IP`, `SPAN_API_KEY`, `SUPABASE_URL`, `SUPABASE_KEY`, `HEALTHCHECK_URL`, `POLL_INTERVAL`, and `STALE_THRESHOLD`, then `exec`s Python.

Python keeps using `os.getenv` and `dotenv.load_dotenv()` exactly as today. The four existing variables are read unchanged, so the `.env` path continues to work for macOS runs and Python needs no awareness of Home Assistant.

Three variables are new. `main.py` currently hardcodes `time.sleep(5)`; it will instead read:

| Variable | Default when unset | Effect |
|---|---|---|
| `POLL_INTERVAL` | `5` | Seconds between polls |
| `STALE_THRESHOLD` | `300` | Seconds before `/healthz` reports 503 |
| `HEALTHCHECK_URL` | unset | No heartbeat pinging |

Every default reproduces today's macOS behaviour exactly, so an unmodified `.env` keeps working.

### Logging

`main.py` currently writes `span.log` via a `FileHandler` every 5 seconds. On a Pi's SD card this is needless write wear, and Supervisor already captures and rotates stdout.

The `FileHandler` becomes conditional: enabled only when `SPAN_LOG_FILE` is set (as `run.sh` will do for macOS), stdout-only otherwise. The `StreamHandler` is unconditional.

---

## Section 2: Health, alerting, and status

### Prerequisite bug fix

`insert_data` catches `APIError`, logs it, and returns normally. If Supabase begins rejecting writes — expired key, schema drift, tier limit — the loop logs `OK 200: 1234 W` every 5 seconds indefinitely while nothing reaches the database.

This is exactly the silent failure the feature exists to detect, and no liveness-based watchdog would catch it. Therefore:

- `insert_data` returns a boolean (or raises) indicating whether **both** inserts succeeded.
- The poll loop marks a tick successful only when the SPAN request returned 200 **and** both inserts landed.
- A partial insert (main succeeded, branches failed) counts as a failed tick.

Health, heartbeat, and status all key off this definition of success. Without the fix they are decorative.

### `health.py`

- `HealthState` — thread-safe (`threading.Lock`) holder for `last_success_ts` and `consecutive_errors`, with `record_success()` and `record_failure()`.
- `start_health_server(state, port, stale_threshold)` — a stdlib `http.server` on a daemon thread serving `/healthz`:
  - **200** when `now - last_success_ts <= stale_threshold`
  - **503** otherwise, including before the first successful write
  - JSON body with `last_success_ts`, `seconds_since_success`, and `consecutive_errors` for debugging

Supervisor's `watchdog` polls this and restarts the container on 503.

### `notify.py`

- `ping_healthcheck(url, state)` — GET the healthchecks.io ping URL after each successful tick.
- `publish_ha_sensor(state)` — POST to `http://supervisor/core/api/states/sensor.span_monitor` with `Authorization: Bearer ${SUPERVISOR_TOKEN}`, enabled by `homeassistant_api: true`. State is seconds since last successful write; attributes carry `consecutive_errors` and last error text. No MQTT broker required.

**Hard rule:** neither function may stall or kill the poll loop.

- Short timeouts (5s).
- Every exception caught and logged, never propagated.
- **Never** wrapped in `@retry_on_connection_error`. That decorator retries forever, which is correct for the data path and catastrophic for a heartbeat.
- Both are no-ops when their configuration is absent: no `HEALTHCHECK_URL` means no pinging; no `SUPERVISOR_TOKEN` means no sensor. This keeps macOS runs working unchanged.

A monitoring system that can take down the thing it monitors is worse than no monitoring.

### Threshold tuning

Restarting the container does not fix a Supabase outage, so an aggressive `stale_threshold` would produce a restart loop during one.

| Setting | Default | Rationale |
|---|---|---|
| `poll_interval` | 5s | Unchanged from today |
| `stale_threshold` | 300s | Long enough for existing backoff (max 60s) to ride out transient outages; short enough to catch a genuine stall quickly |
| healthchecks.io period | 5 min | Pings arrive every ~5s, so this is very tolerant |
| healthchecks.io grace | 5 min | Alert fires ~10 min into a total failure |

All are configurable without a rebuild. The residual risk is accepted: a prolonged Supabase outage produces watchdog restarts that do not help but also do not harm, and the healthchecks.io alert fires correctly regardless.

### healthchecks.io setup

Manual, one-time, done by Alex: create a check, set period and grace as above, configure email to `alex@hashadatascience.com`, and paste the resulting ping URL into the app's `healthcheck_url` option. The URL is a single config value, so switching services later is a configuration change, not a code change.

---

## Section 3: Testing

TDD, extending the existing `test_main.py` and `conftest.py`.

| Test | Asserts |
|---|---|
| `HealthState` fresh | `seconds_since_success` small after `record_success()` |
| `HealthState` stale | Aged beyond threshold reports stale |
| `/healthz` healthy | Returns 200 with a fresh state |
| `/healthz` stale | Returns 503 with an aged state |
| `/healthz` cold start | Returns 503 before any successful write |
| `ping_healthcheck` resilience | A raising or timing-out HTTP call does not propagate |
| `publish_ha_sensor` resilience | Same, and a no-op without `SUPERVISOR_TOKEN` |
| `insert_data` failure | Returns failure when the branch insert raises `APIError` |
| `insert_data` partial | Main success + branch failure counts as a failed tick |

Time-dependent tests inject a clock rather than sleeping.

**Not unit tested:** the container build and bashio wiring. Cross-building `aarch64` on macOS is slow emulation, and a successful cross-build would not prove the bashio option plumbing works. These are verified on the Pi during cutover.

---

## Section 4: Cutover

Ordered, because concurrent writers produce roughly double the rows per tick at near-identical timestamps, which would silently corrupt the hourly continuous aggregates — the permanent historical record.

1. **Stop the macOS monitor.** Confirm no `python main.py` process remains.
2. **Clone on the Pi.** In the Studio Code Server terminal:
   `git clone https://github.com/ahasha/span_monitor /addons/span-monitor`
3. **Install.** Apps → refresh → Local apps → SPAN Monitor → Install.
4. **Configure.** Fill the five values in the Configuration tab. Enable Start on boot and Watchdog.
5. **Start**, then verify three independent signals:
   - App log shows `OK 200: … W` lines
   - healthchecks.io shows a recent ping
   - `sensor.span_monitor` exists in Developer Tools → States
6. **Verify a single writer.** Query `main_energy` for rows per minute over the last 10 minutes. At a 5s interval expect ~12/min; ~24/min means both writers are live.
7. **Document.** `README.md` notes that the two deployment paths are mutually exclusive.

**Rollback:** stop the app in the HA UI and run `./run.sh` on the Mac. Because `run.sh` and the `.env` path are untouched, rollback is immediate and requires no code changes.

---

## Future work

Not in scope, recorded so the layout above stays justified:

- **App repository + CI** (approach B). Requires moving the manifest files into a subdirectory, adding `repository.yaml`, and a GitHub Actions workflow publishing `aarch64` images to GHCR. Updates then become an Update button. Deferred until update friction justifies it.
- **MQTT discovery** for SPAN energy data as HA entities and the Energy dashboard.
- **Timestamp inconsistency** in `insert_data`: `main_energy` uses naive `datetime.utcnow().isoformat()` while `branch_energy` uses the timezone-aware `now`. Unrelated to this work and deliberately left alone to avoid mixing concerns, but it should be fixed.
