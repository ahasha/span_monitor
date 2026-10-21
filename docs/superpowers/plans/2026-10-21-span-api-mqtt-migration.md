# SPAN API (MQTT/Homie) Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the deprecated `GET /api/v1/panel` poll (removed 2026-12-31, issue #3) with the supported SPAN API — an MQTT/Homie subscription — while keeping the `main_energy` / `branch_energy` tables, their hourly continuous aggregates, and every downstream consumer working unchanged.

**Architecture:** A background paho-mqtt client subscribes to `ebus/5/#` on the panel's TLS broker (port 8883) and folds every message into a thread-safe `HomieSnapshot` (last-known value per topic, retained-message semantics). The existing 5-second tick loop stays: instead of an HTTP GET, `poll_once` reads the snapshot, converts it into the same two row shapes as today (`snapshot_to_rows`), and inserts them. Credentials come from a one-time `POST /api/v2/auth/register` with the panel's `hopPassphrase`, cached on disk so restarts don't register new clients.

**Tech Stack:** Python 3.11+/3.13 (Docker), `paho-mqtt>=2.1,<3`, `requests`, `supabase`, pytest + pytest-mock, uv.

**Spec:** GitHub issue ahasha/span_monitor#3, plus SPAN's docs at https://github.com/spanio/SPAN-API-Client-Docs (read at firmware r202639): `README.md` (auth, TLS, v1 sunset table), `docs/public/mqtt-topic-reference.md`, `docs/public/power-and-energy-conventions.md`, `CHANGELOG.md`.

## Background the implementer needs

There is **no v2 REST replacement** for `/api/v1/panel`. SPAN's migration table maps it to "Subscribe `<serial>/#`" — i.e. the data moves to MQTT. Key facts from SPAN's docs:

- Broker: `mqtts://<SPAN_IP>:8883`, username = panel serial, password = `ebusBrokerPassword` from `POST /api/v2/auth/register` (body `{"name": ..., "hopPassphrase": ...}`). TLS uses a panel-generated self-signed CA available unauthenticated at `GET /api/v2/certificate/ca`; the server cert's SANs include the panel's IPs, so connecting by IP verifies normally.
- `GET /api/v2/status` (unauthenticated) returns `serialNumber`.
- Topics: `ebus/5/<device-id>/<node>/<property>`, plus `ebus/5/<device-id>/$state` and `.../$description` (JSON; has `type` and `children`). Device IDs are opaque: **classify by `$description.type`, never by parsing the ID.**
- Relevant devices: panel (`energy.ebus.device.distribution-enclosure`), lugs (`energy.ebus.device.lugs`, `info/direction` = `UPSTREAM`/`DOWNSTREAM`), circuits (`energy.ebus.device.circuit`, one per circuit — **not per breaker space**; `info/spaces` = `"5,7"` for a 2-pole).
- Since r202639, a retained value **is not republished when unchanged**. A circuit sitting at 0 W sends nothing. Last-known-value is the correct reading; per-property age is not a liveness signal.
- If the panel's `$state` is `lost`, every child is effectively lost.
- Sign convention (circuits): load = **negative** `active-power`, accumulates `exported-energy`. Matches what the v1 data already does — `heat_pump_analysis/extract_heat_pump_power.py` computes `-sum(avg_instant_power_w)`.
- Downstream lugs (`lugs-dn`) sign runs opposite to every other meter. v1 `feedthroughPowerW` "shares the downstream-lugs sign", so it maps straight across.

### Field mapping (v1 → MQTT)

`P` = panel device, `UP` = lugs with direction UPSTREAM, `DN` = lugs with direction DOWNSTREAM, `C` = a circuit. **All rows confirmed against live data in Task 1 (2026-10-21) — evidence below the table.**

| Table.column | v1 source | MQTT source |
|---|---|---|
| main_energy.relay_state | `mainRelayState` | `P/status/relay` (`UNKNOWN` → NULL) |
| main_energy.main_meter_produced_energy_wh | `mainMeterEnergy.producedEnergyWh` | `UP/meter/exported-energy` |
| main_energy.main_meter_consumed_energy_wh | `mainMeterEnergy.consumedEnergyWh` | `UP/meter/imported-energy` |
| main_energy.instant_grid_power_w | `instantGridPowerW` | `UP/meter/active-power` |
| main_energy.feed_through_power_w | `feedthroughPowerW` | `DN/meter/active-power` |
| main_energy.feed_through_produced_energy_wh | `feedthroughEnergy.producedEnergyWh` | `DN/meter/exported-energy` |
| main_energy.feed_through_consumed_energy_wh | `feedthroughEnergy.consumedEnergyWh` | `DN/meter/imported-energy` |
| main_energy.grid_sample_start_ms / _end_ms, dsm_grid_state, dsm_state, current_run_config | v1 only | NULL (no equivalent) |
| branch_energy.branch_id | `branches[].id` (one row per space) | `min(C/info/spaces)` (one row per circuit) |
| branch_energy.relay_state | `relayState` | `C/switch/relay` (`UNKNOWN` → NULL) |
| branch_energy.instant_power_w | `instantPowerW` | `C/meter/active-power` |
| branch_energy.imported_active_energy_wh | `importedActiveEnergyWh` | `C/meter/imported-energy` |
| branch_energy.exported_active_energy_wh | `exportedActiveEnergyWh` | `C/meter/exported-energy` |
| branch_energy.measure_start_ts_ms / measure_duration_ms / is_measure_valid | v1 only | NULL |

**Task 1 evidence** (panel firmware spanos3/r202639/03, v1 and MQTT read within ~1 s; fixtures in `tests/fixtures/`):

| Column | v1 | MQTT |
|---|---|---|
| main_meter_consumed_energy_wh | 10959377.0 | UP imported-energy 10959377.0 |
| main_meter_produced_energy_wh | 0.119 | UP exported-energy 0.1 |
| instant_grid_power_w | 1244.96 | UP active-power 1252.7 (`power-flows/grid` read −5: **not** a substitute) |
| feed_through_consumed_energy_wh | 10948657.98 | DN imported-energy 10948658.0 |
| feed_through_produced_energy_wh | 10890975.95 | DN exported-energy 10890975.6 |
| feed_through_power_w | 16.6 | DN active-power 3.3 (same sign; small, noisy value) |
| branch imported/exported (all 25 circuits) | sum over the circuit's spaces | equal to ≤0.2 Wh, e.g. HVAC 2593126.1 vs 2593125.9 |
| branch instant_power_w | sum over spaces | same sign, within a few W, e.g. HVAC −834.3 vs −835.0 |

Also confirmed: every circuit's `info/spaces` equals its branch ids in `branch_to_circuit.csv` (25/25), so Decision 1 holds. Main relay and all circuit relays `CLOSED` in both.

Observations that don't change the mapping:
- v1 had 32 branch rows; MQTT has 25 circuits covering 31 spaces. Space 32 is empty (−0.05 W) and stops getting rows.
- Circuits that v1 reports at only a few watts (|W| ≲ 4) read exactly `0.0` on MQTT (10 circuits; about 15 W in absolute terms, about 8 W of net load, with v1 noise in both directions). The panel appears to zero small readings or not republish them. Energy counters are unaffected; hourly average power for near-idle circuits will read slightly lower after cutover.
- The panel also publishes a `bess`, a `mid`, and a `pv` device. Nothing in the current schema needs them. v1's `dsmGridState`/`dsmState` (`DSM_GRID_UP` / `DSM_ON_GRID`) loosely correspond to MID `grid/islanding-state`, but use a different vocabulary, so they stay NULL as planned.

### Decisions taken in this plan (confirm during review)

1. **No schema change.** A circuit's row is written under the *lowest* space it occupies as `branch_id`. A 2-pole circuit that v1 wrote as two half-rows (spaces 5 and 7) becomes one whole-circuit row at `branch_id = 5`; space 7 stops receiving rows. Any query that sums a circuit's **power** across branch_ids (the HVAC extraction does exactly this) stays correct across the cutover. Energy counters are per-row cumulative, so a summed counter stays continuous only if no hourly bucket mixes v1 and 2.0.0 rows (the aggregate takes `max()` per branch; a mixed bucket double-counts the second leg — about 1.3 MWh for HVAC). Task 8 therefore cuts over on an hour boundary; a query that reads one leg of a 2-pole circuit in isolation would see a step change. Alternative rejected: a new `circuit_energy` table + aggregate keyed on circuit UUID — cleaner long-term, but forces migrations and edits in `archive.py` and `heat_pump_analysis` before the deadline.
2. **Credentials: configure the `hopPassphrase`, register once, cache the result.** The cache lives at `SPAN_AUTH_CACHE` (HA: `/data/span_auth.json`, persistent across restarts; macOS default `~/.span_monitor_auth.json`). Re-register only when the cache is missing or the broker rejects the cached password. Alternative rejected: configuring `ebusBrokerPassword` directly — fewer moving parts, but SPAN says the two values "will likely" diverge and a passphrase regeneration would require a manual reconfigure.
3. **Keep the tick loop.** The MQTT client only maintains state; inserts still happen once per `POLL_INTERVAL`, so row density, aggregates, and the health/heartbeat/sensor layers are untouched.
4. **Cutover by dry run, not by running in parallel.** CLAUDE.md warns that two writers corrupt the hourly aggregates. Task 6 adds `SPAN_DRY_RUN=1` (build rows, log them, insert nothing) so the new path can run next to the live v1 service and its output can be compared.

## Global Constraints

- Python `>=3.11` (pyproject); the container runs 3.13 on aarch64 Alpine — `paho-mqtt` is pure Python, so no wheels issue.
- New runtime dependency: `paho-mqtt>=2.1,<3` only. Use the v2 callback API (`mqtt.CallbackAPIVersion.VERSION2`).
- Use `uv add` / `uv sync`, never `pip install`; commit `uv.lock`.
- Tests live at repo root as `test_<module>.py` (existing convention), run with `uv run pytest`.
- Nothing in the monitor may raise out of `poll_once` or block the tick loop (existing contract, `main.py` docstrings).
- Supabase schema changes, if any become necessary, go through `supabase/migrations/` + `supabase db push`, never ad-hoc SQL. This plan expects none.
- v1 sunset: **2026-12-31**. The cutover (Task 8) should land by mid-December to leave slack.
- Never log the passphrase, broker password, or access token.

## Review Focus

1. **`UNKNOWN` relay state** — the DB enum `relay_state_type` is `('CLOSED','OPEN')`; inserting `UNKNOWN` fails the *whole* batch insert and zeroes a tick. Expected: mapped to NULL. (Test in Task 4.)
2. **Startup before the retained tree has arrived** — the first tick can fire before the lugs or circuits have published. Expected: tick counted as a failure ("snapshot not ready"), no partial row inserted. (Test in Task 4 / Task 6.)
3. **Panel `$state` = `lost`/`disconnected`, or broker connection drops** — the snapshot still holds stale last-known values. Expected: ticks fail, `/healthz` goes 503 after `STALE_THRESHOLD`, values are not written as if fresh. (Tests in Tasks 3 and 6.)
4. **Circuits with no `info/spaces`** (e.g. firmware-added `Commissioned PV System` virtual circuits) or a non-integer spaces string. Expected: skipped with a warning, other circuits still written. (Test in Task 4.)
5. **Broker rejects the cached password** (passphrase regenerated in the app). Expected: cache discarded, one re-registration attempt, reconnect — not a tight loop of registrations. (Test in Task 2 / Task 5.)

---

## File Structure

| File | Responsibility |
|---|---|
| `span_capture.py` (create, Task 1) | One-shot LAN tool: capture a v1 `/panel` JSON and an MQTT retained dump side by side. Not shipped in the image. |
| `span_auth.py` (create) | `GET /api/v2/status`, `POST /api/v2/auth/register`, CA cert download, credential cache. |
| `homie.py` (create) | `HomieSnapshot`: thread-safe last-known-value store fed by raw MQTT messages; readiness/liveness queries. No paho import. |
| `span_mqtt.py` (create) | `SpanMqtt`: paho client wiring (TLS, auth, reconnect, subscribe) that feeds a `HomieSnapshot`. |
| `extract.py` (create) | `snapshot_to_rows(snapshot) -> (main_row, branch_rows)`: the v1→MQTT mapping table above, as a pure function. |
| `main.py` (modify) | Drop the HTTP poll; `poll_once` reads the snapshot; `insert_data` takes prebuilt rows; dry-run mode. |
| `config.yaml`, `addon-run.sh`, `Dockerfile` (modify) | `span_passphrase` replaces `span_api_key`; ship new modules; auth cache in `/data`. |
| `CLAUDE.md`, `README.md`, `DOCS.md` (modify) | Env vars, architecture, cutover notes. |
| `tests/fixtures/span_v1_panel.json`, `tests/fixtures/span_mqtt_dump.json` (create, Task 1) | Real captured data, used by Task 4's regression test. |

---

### Task 1: Discovery spike — capture live data and confirm the mapping

This is the only task that needs the physical panel and its `hopPassphrase` (SPAN Home app → panel settings, or press the door switch 3× and use `span-auth`). It runs on the LAN. **Alex runs it, or explicitly authorizes the executor to.**

**Files:**
- Create: `span_capture.py`
- Create: `tests/fixtures/span_v1_panel.json`, `tests/fixtures/span_mqtt_dump.json`
- Modify: this plan's mapping table (resolve every ⚠)

**Interfaces:**
- Consumes: `span_auth.register` / `fetch_ca_cert` from Task 2 — so do Task 2 first, or inline the two HTTP calls here. (Recommended order: Task 2, then Task 1.)
- Produces: the two fixture files with the shapes below.

`span_mqtt_dump.json` shape: `{"captured_at": "<iso>", "messages": {"<full topic>": "<payload string>", ...}}`.

- [ ] **Step 1: Write the capture tool**

```python
"""One-shot LAN capture: v1 /panel JSON + MQTT retained dump, taken together.

Used once to confirm the v1 -> MQTT field mapping and to produce real test
fixtures. Not part of the service image.

    uv run python span_capture.py --seconds 20
"""
import json
import os
import ssl
import time
from datetime import UTC, datetime
from pathlib import Path

import click
import dotenv
import paho.mqtt.client as mqtt
import requests

from span_auth import load_or_register

dotenv.load_dotenv()
FIXTURES = Path(__file__).parent / "tests" / "fixtures"


@click.command()
@click.option("--seconds", default=20, show_default=True, help="How long to collect MQTT messages.")
def main(seconds):
    ip = os.environ["SPAN_IP"]
    creds = load_or_register(ip, os.environ["SPAN_PASSPHRASE"],
                             Path(os.getenv("SPAN_AUTH_CACHE", "~/.span_monitor_auth.json")).expanduser())

    messages: dict[str, str] = {}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"span_capture-{int(time.time())}")
    client.username_pw_set(creds.serial_number, creds.broker_password)
    client.tls_set(ca_certs=str(creds.ca_cert_path), tls_version=ssl.PROTOCOL_TLS_CLIENT)
    client.on_connect = lambda c, u, f, rc, p: c.subscribe("ebus/5/#")
    client.on_message = lambda c, u, m: messages.__setitem__(m.topic, m.payload.decode("utf-8", "replace"))
    client.connect(ip, creds.mqtt_port)
    client.loop_start()
    time.sleep(seconds)

    # Take the v1 snapshot at the end, when MQTT holds the freshest values.
    v1 = requests.get(f"http://{ip}/api/v1/panel",
                      headers={"Authorization": f"Bearer {os.environ['SPAN_API_KEY']}"}, timeout=10)
    captured_at = datetime.now(UTC).isoformat()
    client.loop_stop()
    client.disconnect()

    FIXTURES.mkdir(parents=True, exist_ok=True)
    (FIXTURES / "span_v1_panel.json").write_text(json.dumps(v1.json(), indent=2))
    (FIXTURES / "span_mqtt_dump.json").write_text(
        json.dumps({"captured_at": captured_at, "messages": messages}, indent=2))
    click.echo(f"v1 HTTP {v1.status_code}, Deprecation={v1.headers.get('Deprecation')}; "
               f"{len(messages)} MQTT topics captured")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it on the LAN**

Run: `uv run python span_capture.py --seconds 20`
Expected: `v1 HTTP 200, Deprecation=true; <N> MQTT topics captured` with N in the hundreds. If `GET /api/v2/status` 404s, the panel firmware predates SPAN API — stop and update firmware before continuing.

- [ ] **Step 3: Resolve each ⚠ row**

For each provisional row compare the v1 value with the proposed MQTT value. Cumulative energy counters should agree to within a few Wh (both are monotonic counters read seconds apart); if the "produced"/"consumed" pairing is swapped, the values will be off by orders of magnitude, which makes the check unambiguous. Instantaneous power should agree in **sign** and roughly in magnitude. Also confirm, for every row of `branch_to_circuit.csv`, that the circuit UUID's `info/spaces` contains that `branch_id` (this validates Decision 1's assumption that v1 branch ids are breaker-space numbers).

Edit the mapping table in this file: remove each ⚠, swap any pairing that turned out reversed, and note the evidence (the two numbers) in a line under the table. **If any pairing changes, update `MAPPING` in Task 4 before implementing it.**

- [ ] **Step 4: Scrub and commit**

The dump contains the panel serial, Wi-Fi SSID, and postal code. Before committing, replace the serial everywhere (topic paths and payloads) with `nt-0000-test0`, and blank `status/wifi-ssid` and `status/postal-code` payloads:

```bash
uv run python - <<'EOF'
import json, re
from pathlib import Path
p = Path("tests/fixtures/span_mqtt_dump.json")
d = json.loads(p.read_text())
serial = next(v for k, v in d["messages"].items() if k.endswith("/info/serial-number"))
scrub = lambda s: s.replace(serial, "nt-0000-test0")
d["messages"] = {scrub(k): ("" if re.search(r"/status/(wifi-ssid|postal-code)$", k) else scrub(v))
                 for k, v in d["messages"].items()}
p.write_text(json.dumps(d, indent=2))
v = Path("tests/fixtures/span_v1_panel.json"); v.write_text(scrub(v.read_text()))
EOF
grep -c nt-0000-test0 tests/fixtures/span_mqtt_dump.json
git add span_capture.py tests/fixtures/ docs/superpowers/plans/2026-10-21-span-api-mqtt-migration.md
git commit -m "Capture live v1 and MQTT panel data; confirm field mapping"
```

---

### Task 2: `span_auth.py` — registration, CA cert, credential cache

**Files:**
- Create: `span_auth.py`
- Test: `test_span_auth.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class SpanCredentials: serial_number: str; broker_password: str; mqtt_port: int; ca_cert_path: Path`
  - `load_or_register(ip: str, passphrase: str, cache_path: Path) -> SpanCredentials` — returns cached creds if the cache exists and its serial matches the panel; otherwise registers and writes the cache.
  - `invalidate(cache_path: Path) -> None` — delete the cache (used by Task 5 on auth rejection).
  - `class SpanAuthError(Exception)`

- [ ] **Step 1: Add the dependency**

```bash
uv add 'paho-mqtt>=2.1,<3'
```

- [ ] **Step 2: Write the failing tests**

```python
import json
from unittest.mock import Mock

import pytest

import span_auth
from span_auth import SpanAuthError, invalidate, load_or_register

PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
REGISTER_OK = {
    "accessToken": "tok", "tokenType": "bearer", "iatMs": 1, "hostname": "span-x.local",
    "serialNumber": "nt-0000-test0", "hopPassphrase": "pass",
    "ebusBrokerUsername": "nt-0000-test0", "ebusBrokerPassword": "brokerpw",
    "ebusBrokerHost": "span-x.local", "ebusBrokerMqttsPort": 8883,
    "ebusBrokerWsPort": 9001, "ebusBrokerWssPort": 9002,
}


def _resp(status, json_body=None, text=""):
    r = Mock(status_code=status, text=text)
    r.json.return_value = json_body
    return r


@pytest.fixture
def http(mocker):
    get = mocker.patch("span_auth.requests.get")
    post = mocker.patch("span_auth.requests.post")

    def fake_get(url, **kw):
        if url.endswith("/api/v2/status"):
            return _resp(200, {"serialNumber": "nt-0000-test0"})
        if url.endswith("/api/v2/certificate/ca"):
            return _resp(200, text=PEM)
        raise AssertionError(url)

    get.side_effect = fake_get
    post.return_value = _resp(200, REGISTER_OK)
    return get, post


def test_registers_and_caches_when_no_cache(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    creds = load_or_register("10.0.0.5", "pass", cache)

    assert creds.serial_number == "nt-0000-test0"
    assert creds.broker_password == "brokerpw"
    assert creds.mqtt_port == 8883
    assert creds.ca_cert_path.read_text() == PEM
    body = post.call_args.kwargs["json"]
    assert body["hopPassphrase"] == "pass"
    assert body["name"].startswith("span_monitor-")
    assert json.loads(cache.read_text())["broker_password"] == "brokerpw"
    assert oct(cache.stat().st_mode & 0o777) == "0o600"


def test_reuses_cache_without_registering(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    load_or_register("10.0.0.5", "pass", cache)
    post.reset_mock()

    creds = load_or_register("10.0.0.5", "pass", cache)

    post.assert_not_called()
    assert creds.broker_password == "brokerpw"


def test_reregisters_when_cached_serial_differs(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    cache.write_text(json.dumps({"serial_number": "other-panel", "broker_password": "x", "mqtt_port": 8883}))

    load_or_register("10.0.0.5", "pass", cache)

    post.assert_called_once()


def test_register_rejected_raises_without_leaking_secret(tmp_path, http):
    _, post = http
    post.return_value = _resp(422, {"detail": "bad credential"}, text="bad credential")

    with pytest.raises(SpanAuthError) as exc:
        load_or_register("10.0.0.5", "s3cret", tmp_path / "auth.json")
    assert "s3cret" not in str(exc.value)
    assert "422" in str(exc.value)


def test_register_without_broker_password_raises(tmp_path, http):
    _, post = http
    post.return_value = _resp(200, {**REGISTER_OK, "ebusBrokerPassword": None})

    with pytest.raises(SpanAuthError):
        load_or_register("10.0.0.5", "pass", tmp_path / "auth.json")


def test_rejects_non_pem_ca(tmp_path, http):
    get, _ = http
    get.side_effect = lambda url, **kw: (_resp(200, {"serialNumber": "nt-0000-test0"})
                                         if url.endswith("/status") else _resp(200, text="<html>"))
    with pytest.raises(SpanAuthError):
        load_or_register("10.0.0.5", "pass", tmp_path / "auth.json")


def test_invalidate_removes_cache_and_tolerates_absence(tmp_path):
    cache = tmp_path / "auth.json"
    cache.write_text("{}")
    invalidate(cache)
    assert not cache.exists()
    invalidate(cache)  # no error
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest test_span_auth.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'span_auth'`

- [ ] **Step 4: Implement**

```python
"""SPAN API credentials: register with the panel's hopPassphrase, cache the result.

Registration creates a named client on the panel, so it happens once and the
result is cached; a restart (or a watchdog-triggered container restart) must
not add another client. The cache holds the broker password, so it is written
0600 and never logged.
"""
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

TIMEOUT = 10


class SpanAuthError(Exception):
    pass


@dataclass(frozen=True)
class SpanCredentials:
    serial_number: str
    broker_password: str
    mqtt_port: int
    ca_cert_path: Path


def _get_serial(ip: str) -> str:
    r = requests.get(f"http://{ip}/api/v2/status", timeout=TIMEOUT)
    if r.status_code != 200:
        raise SpanAuthError(f"GET /api/v2/status returned {r.status_code}; does the panel firmware support SPAN API?")
    return r.json()["serialNumber"]


def _fetch_ca_cert(ip: str, dest: Path) -> Path:
    r = requests.get(f"http://{ip}/api/v2/certificate/ca", timeout=TIMEOUT)
    if r.status_code != 200 or "BEGIN CERTIFICATE" not in r.text:
        raise SpanAuthError(f"Could not download the panel CA certificate (HTTP {r.status_code})")
    dest.write_text(r.text)
    return dest


def _register(ip: str, passphrase: str) -> dict:
    # A fresh name each time: re-registering an existing name is rejected, and
    # we only get here when the cache is missing or invalid.
    body = {"name": f"span_monitor-{int(time.time())}",
            "description": "span_monitor energy logger",
            "hopPassphrase": passphrase}
    r = requests.post(f"http://{ip}/api/v2/auth/register", json=body, timeout=TIMEOUT)
    if r.status_code != 200:
        raise SpanAuthError(f"POST /api/v2/auth/register returned {r.status_code}: {r.text[:200]}")
    out = r.json()
    if not out.get("ebusBrokerPassword"):
        raise SpanAuthError("Registration succeeded but returned no ebusBrokerPassword")
    return out


def _write_cache(cache_path: Path, data: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(cache_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    os.chmod(cache_path, 0o600)


def _read_cache(cache_path: Path) -> dict | None:
    try:
        return json.loads(cache_path.read_text())
    except (OSError, ValueError):
        return None


def load_or_register(ip: str, passphrase: str, cache_path: Path) -> SpanCredentials:
    serial = _get_serial(ip)
    ca_path = _fetch_ca_cert(ip, cache_path.with_suffix(".ca.crt"))

    cached = _read_cache(cache_path)
    if cached and cached.get("serial_number") == serial and cached.get("broker_password"):
        logger.info("Using cached SPAN API credentials")
    else:
        logger.info("Registering span_monitor as a SPAN API client")
        out = _register(ip, passphrase)
        cached = {"serial_number": out["serialNumber"],
                  "broker_password": out["ebusBrokerPassword"],
                  "mqtt_port": out["ebusBrokerMqttsPort"]}
        _write_cache(cache_path, cached)

    return SpanCredentials(serial_number=cached["serial_number"],
                           broker_password=cached["broker_password"],
                           mqtt_port=int(cached.get("mqtt_port", 8883)),
                           ca_cert_path=ca_path)


def invalidate(cache_path: Path) -> None:
    cache_path.unlink(missing_ok=True)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest test_span_auth.py -v`
Expected: 7 passed

- [ ] **Step 6: Commit**

```bash
git add span_auth.py test_span_auth.py pyproject.toml uv.lock
git commit -m "Add SPAN API v2 registration with cached credentials"
```

---

### Task 3: `homie.py` — thread-safe snapshot of the Homie device tree

**Files:**
- Create: `homie.py`
- Test: `test_homie.py`

**Interfaces:**
- Produces:
  - `HomieSnapshot(root_device_id: str, clock=time.monotonic)`
  - `.ingest(topic: str, payload: str) -> None` — called from the MQTT thread for every message.
  - `.set_connected(connected: bool) -> None`
  - `.devices_of_type(type_suffix: str) -> list[str]` — device IDs whose `$description.type` is `energy.ebus.device.<type_suffix>`, restricted to the tree reachable from the root via `children`.
  - `.get(device_id: str, node: str, prop: str) -> str | None`
  - `.unready_reason(max_silence: float) -> str | None` — `None` when usable, otherwise a human-readable reason (`"not connected"`, `"panel $state is lost"`, `"no MQTT message for 130s"`).

- [ ] **Step 1: Write the failing tests**

```python
import json

from homie import HomieSnapshot

ROOT = "nt-0000-test0"


def desc(type_suffix, children=()):
    return json.dumps({"homie": "5.0", "type": f"energy.ebus.device.{type_suffix}", "children": list(children)})


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def ready_snapshot(clock=None):
    s = HomieSnapshot(ROOT, clock=clock or FakeClock())
    s.set_connected(True)
    s.ingest(f"ebus/5/{ROOT}/$state", "ready")
    s.ingest(f"ebus/5/{ROOT}/$description", desc("distribution-enclosure", [f"{ROOT}-lugs-up", "c1", "bess1"]))
    s.ingest(f"ebus/5/{ROOT}-lugs-up/$description", desc("lugs"))
    s.ingest("ebus/5/c1/$description", desc("circuit"))
    s.ingest("ebus/5/bess1/$description", desc("bess", ["bess1-mid"]))
    s.ingest("ebus/5/bess1-mid/$description", desc("mid"))
    return s


def test_get_returns_last_value():
    s = ready_snapshot()
    s.ingest("ebus/5/c1/meter/active-power", "-120.5")
    s.ingest("ebus/5/c1/meter/active-power", "-130.0")
    assert s.get("c1", "meter", "active-power") == "-130.0"
    assert s.get("c1", "meter", "imported-energy") is None


def test_devices_of_type_walks_tree_recursively():
    s = ready_snapshot()
    assert s.devices_of_type("circuit") == ["c1"]
    assert s.devices_of_type("mid") == ["bess1-mid"]   # grandchild of the panel


def test_devices_outside_root_tree_are_ignored():
    s = ready_snapshot()
    s.ingest("ebus/5/stranger/$description", desc("circuit"))
    assert s.devices_of_type("circuit") == ["c1"]


def test_malformed_description_is_ignored():
    s = ready_snapshot()
    s.ingest("ebus/5/c1/$description", "{not json")
    assert s.devices_of_type("circuit") == ["c1"]  # previous description kept


def test_unready_when_disconnected():
    s = ready_snapshot()
    s.set_connected(False)
    assert s.unready_reason(60) == "not connected"


def test_unready_when_panel_lost():
    s = ready_snapshot()
    s.ingest(f"ebus/5/{ROOT}/$state", "lost")
    assert s.unready_reason(60) == "panel $state is lost"


def test_unready_before_panel_state_arrives():
    s = HomieSnapshot(ROOT, clock=FakeClock())
    s.set_connected(True)
    assert s.unready_reason(60) == "panel $state is unknown"


def test_unready_after_silence():
    clock = FakeClock()
    s = ready_snapshot(clock)
    clock.t += 61
    assert s.unready_reason(60) == "no MQTT message for 61s"
    s.ingest("ebus/5/c1/meter/active-power", "1")
    assert s.unready_reason(60) is None


def test_non_ebus_topics_ignored():
    s = ready_snapshot()
    s.ingest("something/else", "x")
    s.ingest("ebus/5/c1", "x")  # too short to be a property
    assert s.get("c1", "meter", "active-power") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest test_homie.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homie'`

- [ ] **Step 3: Implement**

```python
"""Last-known-value store for a Homie 5 / eBus device tree.

The panel publishes retained values and, since firmware r202639, does not
republish a value that has not changed. So the latest value seen for a topic
IS the current value, however old the message is; liveness has to come from
the connection, the panel's $state, and whether anything at all is arriving.

ingest() runs on paho's network thread; every reader runs on the main thread.
"""
import json
import threading
import time

PREFIX = "ebus/5/"
TYPE_PREFIX = "energy.ebus.device."


class HomieSnapshot:
    def __init__(self, root_device_id: str, clock=time.monotonic):
        self._root = root_device_id
        self._clock = clock
        self._lock = threading.Lock()
        self._values: dict[tuple[str, str, str], str] = {}
        self._state: dict[str, str] = {}
        self._descriptions: dict[str, dict] = {}
        self._connected = False
        self._last_message = None

    def set_connected(self, connected: bool) -> None:
        with self._lock:
            self._connected = connected

    def ingest(self, topic: str, payload: str) -> None:
        if not topic.startswith(PREFIX):
            return
        parts = topic[len(PREFIX):].split("/")
        with self._lock:
            self._last_message = self._clock()
            if len(parts) == 2 and parts[1] == "$state":
                self._state[parts[0]] = payload
            elif len(parts) == 2 and parts[1] == "$description":
                try:
                    self._descriptions[parts[0]] = json.loads(payload)
                except ValueError:
                    pass
            elif len(parts) == 3 and not parts[1].startswith("$") and not parts[2].startswith("$"):
                self._values[(parts[0], parts[1], parts[2])] = payload

    def get(self, device_id: str, node: str, prop: str) -> str | None:
        with self._lock:
            return self._values.get((device_id, node, prop))

    def devices_of_type(self, type_suffix: str) -> list[str]:
        wanted = TYPE_PREFIX + type_suffix
        with self._lock:
            found, seen, queue = [], set(), [self._root]
            while queue:
                dev = queue.pop(0)
                if dev in seen:
                    continue
                seen.add(dev)
                d = self._descriptions.get(dev) or {}
                if dev != self._root and d.get("type") == wanted:
                    found.append(dev)
                queue.extend(d.get("children") or [])
            return found

    def unready_reason(self, max_silence: float) -> str | None:
        with self._lock:
            if not self._connected:
                return "not connected"
            state = self._state.get(self._root)
            if state is None:
                return "panel $state is unknown"
            if state != "ready":
                return f"panel $state is {state}"
            silence = self._clock() - self._last_message
            if silence > max_silence:
                return f"no MQTT message for {silence:.0f}s"
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest test_homie.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add homie.py test_homie.py
git commit -m "Add thread-safe Homie snapshot store"
```

---

### Task 4: `extract.py` — snapshot → `main_energy` / `branch_energy` rows

**Files:**
- Create: `extract.py`
- Test: `test_extract.py`

**Interfaces:**
- Consumes: `HomieSnapshot.devices_of_type`, `.get` (Task 3); fixture `tests/fixtures/span_mqtt_dump.json` (Task 1).
- Produces:
  - `class SnapshotIncomplete(Exception)` — raised when required data is missing; message says what.
  - `snapshot_to_rows(snapshot: HomieSnapshot, now: str) -> tuple[dict, list[dict]]` — `now` is the ISO timestamp used for every row (both tables share one timestamp, fixing the current mismatch where `main_energy` used `datetime.utcnow()`).

Row keys are exactly the current column names (see the mapping table). **If Task 1 changed any pairing, edit the marked lines below to match before running the tests.**

- [ ] **Step 1: Write the failing tests**

```python
import json
from pathlib import Path

import pytest

from extract import SnapshotIncomplete, snapshot_to_rows
from homie import HomieSnapshot
from test_homie import ROOT, desc

NOW = "2026-11-01T12:00:00+00:00"
FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "span_mqtt_dump.json"


def build(circuits=None, dn=True, relay="CLOSED"):
    s = HomieSnapshot(ROOT)
    s.set_connected(True)
    circuits = circuits if circuits is not None else {"c1": {"spaces": "5,7", "relay": "CLOSED"}}
    children = [f"{ROOT}-lugs-up"] + ([f"{ROOT}-lugs-dn"] if dn else []) + list(circuits)
    s.ingest(f"ebus/5/{ROOT}/$state", "ready")
    s.ingest(f"ebus/5/{ROOT}/$description", desc("distribution-enclosure", children))
    s.ingest(f"ebus/5/{ROOT}/status/relay", relay)
    for lug, direction, power, imp, exp in [("up", "UPSTREAM", "850.5", "1000", "200"),
                                            ("dn", "DOWNSTREAM", "40.0", "300", "10")]:
        if lug == "dn" and not dn:
            continue
        d = f"{ROOT}-lugs-{lug}"
        s.ingest(f"ebus/5/{d}/$description", desc("lugs"))
        s.ingest(f"ebus/5/{d}/info/direction", direction)
        s.ingest(f"ebus/5/{d}/meter/active-power", power)
        s.ingest(f"ebus/5/{d}/meter/imported-energy", imp)
        s.ingest(f"ebus/5/{d}/meter/exported-energy", exp)
    for cid, c in circuits.items():
        s.ingest(f"ebus/5/{cid}/$description", desc("circuit"))
        if c.get("spaces") is not None:
            s.ingest(f"ebus/5/{cid}/info/spaces", c["spaces"])
        s.ingest(f"ebus/5/{cid}/switch/relay", c.get("relay", "CLOSED"))
        s.ingest(f"ebus/5/{cid}/meter/active-power", c.get("power", "-500.0"))
        s.ingest(f"ebus/5/{cid}/meter/imported-energy", "1.5")
        s.ingest(f"ebus/5/{cid}/meter/exported-energy", "9000.25")
    return s


def test_main_row_mapping():
    main, _ = snapshot_to_rows(build(), NOW)
    assert main == {
        "time": NOW,
        "relay_state": "CLOSED",
        "main_meter_produced_energy_wh": 200.0,   # UP exported
        "main_meter_consumed_energy_wh": 1000.0,  # UP imported
        "instant_grid_power_w": 850.5,
        "feed_through_power_w": 40.0,
        "feed_through_produced_energy_wh": 10.0,  # DN exported
        "feed_through_consumed_energy_wh": 300.0, # DN imported
        "grid_sample_start_ms": None, "grid_sample_end_ms": None,
        "dsm_grid_state": None, "dsm_state": None, "current_run_config": None,
    }


def test_branch_row_uses_lowest_space_and_one_row_per_circuit():
    _, branches = snapshot_to_rows(build(), NOW)
    assert branches == [{
        "time": NOW, "branch_id": 5, "relay_state": "CLOSED",
        "instant_power_w": -500.0,
        "imported_active_energy_wh": 1.5, "exported_active_energy_wh": 9000.25,
        "measure_start_ts_ms": None, "measure_duration_ms": None, "is_measure_valid": None,
    }]


def test_unknown_relay_becomes_null():
    main, branches = snapshot_to_rows(build(relay="UNKNOWN",
                                            circuits={"c1": {"spaces": "3", "relay": "UNKNOWN"}}), NOW)
    assert main["relay_state"] is None
    assert branches[0]["relay_state"] is None


def test_circuit_without_spaces_is_skipped(caplog):
    _, branches = snapshot_to_rows(build(circuits={"c1": {"spaces": "3"},
                                                   "virt": {"spaces": None},
                                                   "bad": {"spaces": "x"}}), NOW)
    assert [b["branch_id"] for b in branches] == [3]
    assert "virt" in caplog.text and "bad" in caplog.text


def test_missing_downstream_lugs_gives_null_feedthrough():
    main, _ = snapshot_to_rows(build(dn=False), NOW)
    assert main["feed_through_power_w"] is None
    assert main["feed_through_produced_energy_wh"] is None


def test_missing_upstream_lugs_is_incomplete():
    s = build()
    s._descriptions.pop(f"{ROOT}-lugs-up")
    with pytest.raises(SnapshotIncomplete, match="upstream lugs"):
        snapshot_to_rows(s, NOW)


def test_no_circuits_is_incomplete():
    with pytest.raises(SnapshotIncomplete, match="circuit"):
        snapshot_to_rows(build(circuits={}), NOW)


def test_circuit_missing_power_is_incomplete():
    s = build()
    s._values.pop(("c1", "meter", "active-power"))
    with pytest.raises(SnapshotIncomplete, match="c1"):
        snapshot_to_rows(s, NOW)


@pytest.mark.skipif(not FIXTURE.exists(), reason="Task 1 fixture not captured")
def test_real_capture_matches_v1_shape():
    dump = json.loads(FIXTURE.read_text())["messages"]
    root = next(k.split("/")[2] for k in dump if k.endswith("/info/serial-number"))
    s = HomieSnapshot(root)
    s.set_connected(True)
    for topic, payload in dump.items():
        s.ingest(topic, payload)
    main, branches = snapshot_to_rows(s, NOW)

    v1 = json.loads((FIXTURE.parent / "span_v1_panel.json").read_text())
    assert main["instant_grid_power_w"] == pytest.approx(v1["instantGridPowerW"], abs=500)
    assert main["main_meter_consumed_energy_wh"] == pytest.approx(
        v1["mainMeterEnergy"]["consumedEnergyWh"], rel=1e-3)
    # Every circuit in branch_to_circuit.csv produces exactly one row.
    import csv
    with open(Path(__file__).parent / "branch_to_circuit.csv") as f:
        circuit_first_space = {}
        for r in csv.DictReader(f):
            circuit_first_space[r["circuit_id"]] = min(int(r["branch_id"]),
                                                       circuit_first_space.get(r["circuit_id"], 99))
    assert {b["branch_id"] for b in branches} >= set(circuit_first_space.values())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest test_extract.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'extract'`

- [ ] **Step 3: Implement**

```python
"""Turn a HomieSnapshot into the main_energy / branch_energy rows v1 produced.

The column names are the v1-era schema, kept so the hourly continuous
aggregates and every downstream consumer keep working. The field mapping and
the evidence for it are in
docs/superpowers/plans/2026-10-21-span-api-mqtt-migration.md.

A circuit is written under the lowest breaker space it occupies. v1 wrote a
2-pole circuit as one row per space; the SPAN API meters the circuit as a
whole, so the second space simply stops receiving rows. Sums over a
circuit's branch_ids are unaffected.
"""
import logging

from homie import HomieSnapshot

logger = logging.getLogger(__name__)

RELAY_STATES = {"OPEN", "CLOSED"}  # the relay_state_type enum; anything else -> NULL


class SnapshotIncomplete(Exception):
    pass


def _float(value: str | None) -> float | None:
    return None if value is None else float(value)


def _relay(value: str | None) -> str | None:
    return value if value in RELAY_STATES else None


def _lugs(snapshot: HomieSnapshot, direction: str) -> str | None:
    for dev in snapshot.devices_of_type("lugs"):
        if snapshot.get(dev, "info", "direction") == direction:
            return dev
    return None


def _first_space(spaces: str | None) -> int | None:
    try:
        return min(int(s) for s in spaces.split(","))
    except (AttributeError, ValueError):
        return None


def snapshot_to_rows(snapshot: HomieSnapshot, now: str) -> tuple[dict, list[dict]]:
    up = _lugs(snapshot, "UPSTREAM")
    if up is None or snapshot.get(up, "meter", "active-power") is None:
        raise SnapshotIncomplete("upstream lugs not yet published")
    dn = _lugs(snapshot, "DOWNSTREAM")
    meter = lambda dev, prop: None if dev is None else _float(snapshot.get(dev, "meter", prop))

    root = snapshot._root
    main_row = {
        "time": now,
        "relay_state": _relay(snapshot.get(root, "status", "relay")),
        "main_meter_produced_energy_wh": meter(up, "exported-energy"),
        "main_meter_consumed_energy_wh": meter(up, "imported-energy"),
        "instant_grid_power_w": meter(up, "active-power"),
        "feed_through_power_w": meter(dn, "active-power"),
        "feed_through_produced_energy_wh": meter(dn, "exported-energy"),
        "feed_through_consumed_energy_wh": meter(dn, "imported-energy"),
        "grid_sample_start_ms": None,
        "grid_sample_end_ms": None,
        "dsm_grid_state": None,
        "dsm_state": None,
        "current_run_config": None,
    }

    circuits = snapshot.devices_of_type("circuit")
    if not circuits:
        raise SnapshotIncomplete("no circuit devices published yet")

    branch_rows = []
    for cid in circuits:
        branch_id = _first_space(snapshot.get(cid, "info", "spaces"))
        if branch_id is None:
            logger.warning(f"Skipping circuit {cid}: no usable info/spaces")
            continue
        power = snapshot.get(cid, "meter", "active-power")
        if power is None:
            raise SnapshotIncomplete(f"circuit {cid} has no active-power yet")
        branch_rows.append({
            "time": now,
            "branch_id": branch_id,
            "relay_state": _relay(snapshot.get(cid, "switch", "relay")),
            "instant_power_w": float(power),
            "imported_active_energy_wh": meter(cid, "imported-energy"),
            "exported_active_energy_wh": meter(cid, "exported-energy"),
            "measure_start_ts_ms": None,
            "measure_duration_ms": None,
            "is_measure_valid": None,
        })
    branch_rows.sort(key=lambda r: r["branch_id"])
    return main_row, branch_rows
```

Then make `root` public instead of reaching into `_root`: add to `HomieSnapshot` in `homie.py`

```python
    @property
    def root(self) -> str:
        return self._root
```

and change `root = snapshot._root` to `root = snapshot.root` in `extract.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest test_extract.py test_homie.py -v`
Expected: all pass (the fixture test passes if Task 1 ran, otherwise is reported as skipped).

- [ ] **Step 5: Commit**

```bash
git add extract.py homie.py test_extract.py
git commit -m "Map Homie snapshot to existing main/branch energy rows"
```

---

### Task 5: `span_mqtt.py` — paho client that feeds the snapshot

**Files:**
- Create: `span_mqtt.py`
- Test: `test_span_mqtt.py`

**Interfaces:**
- Consumes: `SpanCredentials`, `invalidate` (Task 2); `HomieSnapshot.ingest` / `.set_connected` (Task 3).
- Produces:
  - `SpanMqtt(ip: str, creds: SpanCredentials, snapshot: HomieSnapshot, on_auth_failure: Callable[[], None], client_factory=None)`
  - `.start() -> None` — `connect_async` + `loop_start`; returns immediately; paho reconnects on its own with 1–60 s backoff.
  - `.stop() -> None`
  - The paho callbacks are methods `_on_connect`, `_on_disconnect`, `_on_message` so tests can call them directly without a broker.

- [ ] **Step 1: Write the failing tests**

```python
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from homie import HomieSnapshot
from span_auth import SpanCredentials
from span_mqtt import SpanMqtt

CREDS = SpanCredentials("nt-0000-test0", "brokerpw", 8883, Path("/tmp/ca.crt"))


def make(on_auth_failure=None):
    client = Mock()
    snap = HomieSnapshot("nt-0000-test0")
    m = SpanMqtt("10.0.0.5", CREDS, snap, on_auth_failure or Mock(), client_factory=lambda: client)
    return m, client, snap


def rc(value, failure=False):
    return SimpleNamespace(value=value, is_failure=failure)


def test_start_configures_tls_auth_and_connects():
    m, client, _ = make()
    m.start()
    client.username_pw_set.assert_called_once_with("nt-0000-test0", "brokerpw")
    assert client.tls_set.call_args.kwargs["ca_certs"] == "/tmp/ca.crt"
    client.reconnect_delay_set.assert_called_once_with(min_delay=1, max_delay=60)
    client.connect_async.assert_called_once_with("10.0.0.5", 8883, keepalive=60)
    client.loop_start.assert_called_once()


def test_connect_subscribes_and_marks_connected():
    m, client, snap = make()
    m._on_connect(client, None, None, rc(0), None)
    client.subscribe.assert_called_once_with("ebus/5/#", qos=0)
    assert snap.unready_reason(60) == "panel $state is unknown"  # connected, nothing received yet


def test_message_is_ingested():
    m, client, snap = make()
    msg = SimpleNamespace(topic="ebus/5/c1/meter/active-power", payload=b"-12.5")
    m._on_message(client, None, msg)
    assert snap.get("c1", "meter", "active-power") == "-12.5"


def test_undecodable_payload_does_not_raise():
    m, client, snap = make()
    m._on_message(client, None, SimpleNamespace(topic="ebus/5/c1/meter/active-power", payload=b"\xff\xfe"))


def test_disconnect_marks_not_connected():
    m, client, snap = make()
    m._on_connect(client, None, None, rc(0), None)
    m._on_disconnect(client, None, None, rc(0), None)
    assert snap.unready_reason(60) == "not connected"


def test_auth_rejection_calls_handler_once():
    handler = Mock()
    m, client, _ = make(handler)
    # MQTT v3.1.1 "not authorized" (5) and v5 "bad user name or password" (134 / 0x86)
    m._on_connect(client, None, None, rc(5, failure=True), None)
    m._on_connect(client, None, None, rc(134, failure=True), None)
    handler.assert_called_once()
    client.subscribe.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest test_span_mqtt.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'span_mqtt'`

- [ ] **Step 3: Implement**

```python
"""MQTT connection to the SPAN panel's eBus broker, feeding a HomieSnapshot.

paho runs its own network thread and reconnects on its own. On every
(re)connect the panel republishes its whole retained tree, so the snapshot
refills without any action here. Callbacks must never raise: an exception in
a paho callback is swallowed by paho at best and kills its thread at worst.
"""
import logging
import ssl
import time
from typing import Callable

import paho.mqtt.client as mqtt

from homie import HomieSnapshot
from span_auth import SpanCredentials

logger = logging.getLogger(__name__)

AUTH_FAILURE_CODES = {4, 5, 134, 135}  # v3.1.1 bad creds / not authorized; v5 equivalents


class SpanMqtt:
    def __init__(self, ip: str, creds: SpanCredentials, snapshot: HomieSnapshot,
                 on_auth_failure: Callable[[], None], client_factory=None):
        self._ip = ip
        self._creds = creds
        self._snapshot = snapshot
        self._on_auth_failure = on_auth_failure
        self._auth_failed = False
        factory = client_factory or (lambda: mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=f"span_monitor-{int(time.time())}"))
        self._client = factory()
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def start(self) -> None:
        self._client.username_pw_set(self._creds.serial_number, self._creds.broker_password)
        self._client.tls_set(ca_certs=str(self._creds.ca_cert_path), tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._client.connect_async(self._ip, self._creds.mqtt_port, keepalive=60)
        self._client.loop_start()

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self._snapshot.set_connected(False)
            logger.error(f"MQTT connect refused: {reason_code}")
            if reason_code.value in AUTH_FAILURE_CODES and not self._auth_failed:
                self._auth_failed = True
                try:
                    self._on_auth_failure()
                except Exception:
                    logger.exception("Auth-failure handler raised")
            return
        logger.info("MQTT connected; subscribing to ebus/5/#")
        self._snapshot.set_connected(True)
        client.subscribe("ebus/5/#", qos=0)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._snapshot.set_connected(False)
        logger.warning(f"MQTT disconnected: {reason_code}")

    def _on_message(self, client, userdata, msg):
        try:
            self._snapshot.ingest(msg.topic, msg.payload.decode("utf-8"))
        except Exception:
            logger.exception(f"Failed to ingest {msg.topic}")
```

The auth-failure handler (wired in Task 6) invalidates the cache and exits the process non-zero. Rationale: re-registering *inside* a paho callback would block its network thread, and process exit is already the recovery path under both deployments (Supervisor restarts the container; `run.sh` loops). On restart, the missing cache triggers exactly one fresh registration. `_auth_failed` makes sure a reconnect storm can't call it twice.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest test_span_mqtt.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add span_mqtt.py test_span_mqtt.py
git commit -m "Add paho MQTT client feeding the Homie snapshot"
```

---

### Task 6: Rewire `main.py` onto the snapshot; add dry-run

**Files:**
- Modify: `main.py` (remove `get_span_response`, the v1 URL/headers; change `insert_data` and `poll_once`; new `__main__` wiring)
- Modify: `test_main.py` (replace v1-shaped fixtures and `poll_once` tests)

**Interfaces:**
- Consumes: `load_or_register`, `invalidate`, `SpanAuthError` (Task 2); `HomieSnapshot` (Task 3); `snapshot_to_rows`, `SnapshotIncomplete` (Task 4); `SpanMqtt` (Task 5).
- Produces (new signatures):
  - `insert_data(main_row: dict, branch_rows: list[dict], supabase) -> bool` (still `@retry_on_connection_error()`).
  - `poll_once(snapshot, supabase, state, heartbeat, sensor, healthcheck_url, clock_floor=None, max_silence=120.0, dry_run=False) -> bool`

- [ ] **Step 1: Read the current tests**

Run: `grep -n "def test_\|get_span_response\|mock_span_data\|poll_once(" test_main.py`
Every test that builds a v1 dict, mocks `get_span_response`, or calls `poll_once` with `url, headers` is rewritten in Step 2. Tests for `retry_on_connection_error` and `get_newest_row_time` are unchanged.

- [ ] **Step 2: Write the failing tests**

Replace the `mock_span_data` fixture and the `insert_data` / `poll_once` tests in `test_main.py` with:

```python
from homie import HomieSnapshot
from test_extract import build

MAIN_ROW = {"time": "t", "relay_state": "CLOSED", "instant_grid_power_w": 500.0}
BRANCH_ROWS = [{"time": "t", "branch_id": 5, "instant_power_w": -100.0}]


@pytest.fixture
def deps():
    state = Mock()
    heartbeat = Mock(**{"ready.return_value": False})
    sensor = Mock(**{"ready.return_value": False})
    return state, heartbeat, sensor


def test_insert_data_inserts_both_tables():
    supabase = Mock()
    assert insert_data(MAIN_ROW, BRANCH_ROWS, supabase) is True
    supabase.table.assert_any_call("main_energy")
    supabase.table.assert_any_call("branch_energy")
    supabase.table("main_energy").insert.assert_any_call(MAIN_ROW)
    supabase.table("branch_energy").insert.assert_any_call(BRANCH_ROWS)


def test_insert_data_partial_failure_is_failure():
    supabase = Mock()
    supabase.table.return_value.insert.return_value.execute.side_effect = [
        None, APIError({"message": "boom"})]
    assert insert_data(MAIN_ROW, BRANCH_ROWS, supabase) is False


def test_poll_once_inserts_snapshot_rows(mocker, deps):
    state, heartbeat, sensor = deps
    ins = mocker.patch("main.insert_data", return_value=True)
    assert main.poll_once(build(), Mock(), state, heartbeat, sensor, None) is True
    main_row, branch_rows, _ = ins.call_args.args
    assert main_row["time"] == branch_rows[0]["time"]   # one timestamp per tick
    state.record_success.assert_called_once()


def test_poll_once_unready_snapshot_fails_without_insert(mocker, deps):
    state, heartbeat, sensor = deps
    ins = mocker.patch("main.insert_data")
    snap = build()
    snap.set_connected(False)
    assert main.poll_once(snap, Mock(), state, heartbeat, sensor, None) is False
    ins.assert_not_called()
    state.record_failure.assert_called_once_with("SPAN snapshot not ready: not connected")


def test_poll_once_incomplete_snapshot_fails_without_insert(mocker, deps):
    state, heartbeat, sensor = deps
    ins = mocker.patch("main.insert_data")
    assert main.poll_once(build(circuits={}), Mock(), state, heartbeat, sensor, None) is False
    ins.assert_not_called()
    assert "circuit" in state.record_failure.call_args.args[0]


def test_poll_once_dry_run_logs_and_does_not_insert(mocker, deps, caplog):
    state, heartbeat, sensor = deps
    ins = mocker.patch("main.insert_data")
    caplog.set_level("INFO")
    assert main.poll_once(build(), Mock(), state, heartbeat, sensor, None, dry_run=True) is True
    ins.assert_not_called()
    assert "DRY RUN" in caplog.text


def test_poll_once_clock_guard_still_refuses(mocker, deps):
    state, heartbeat, sensor = deps
    ins = mocker.patch("main.insert_data")
    future = datetime.now(UTC) + timedelta(hours=1)
    assert main.poll_once(build(), Mock(), state, heartbeat, sensor, None, clock_floor=future) is False
    ins.assert_not_called()


def test_poll_once_never_raises(mocker, deps):
    state, heartbeat, sensor = deps
    mocker.patch("main.snapshot_to_rows", side_effect=RuntimeError("boom"))
    assert main.poll_once(build(), Mock(), state, heartbeat, sensor, None) is False
    state.record_failure.assert_called_once_with("boom")
```

Also remove `get_span_response` from the `from main import (...)` list at the top of `test_main.py`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest test_main.py -v`
Expected: FAIL — `insert_data()` takes the wrong arguments / `poll_once()` has the wrong signature.

- [ ] **Step 4: Implement the `main.py` changes**

Replace imports at the top (drop `requests` only if nothing else uses it — `retry_on_connection_error` still lists `requests.exceptions`, keep it):

```python
from extract import SnapshotIncomplete, snapshot_to_rows
from homie import HomieSnapshot
from span_auth import SpanAuthError, invalidate, load_or_register
from span_mqtt import SpanMqtt
from pathlib import Path
import sys
```

Delete `get_span_response`. Replace `insert_data` with:

```python
@retry_on_connection_error()
def insert_data(main_row: dict, branch_rows: list[dict], supabase: Client) -> bool:
    """
    Insert one tick of panel data into 'main_energy' and 'branch_energy'.

    Returns True only if both inserts succeeded. A partial write counts as
    a failure: the health, heartbeat, and status layers all key off this
    value, so reporting success when rows did not land would recreate the
    silent failure this return value exists to expose.
    """
    main_ok = False
    try:
        supabase.table("main_energy").insert(main_row).execute()
        main_ok = True
    except APIError as e:
        logger.error(f"Error inserting main_energy: {e}")
        logger.error(main_row)

    branch_ok = False
    try:
        supabase.table("branch_energy").insert(branch_rows).execute()
        branch_ok = True
    except APIError as e:
        logger.error(f"Error inserting branch_energy: {e}")
        logger.error(branch_rows)

    return main_ok and branch_ok
```

Replace the body of `poll_once` above the post-tick notification block (keep the docstring's clock-floor paragraph and the notification block verbatim; update the first paragraph to say "the SPAN snapshot was ready" instead of "SPAN answered 200"):

```python
def poll_once(
    snapshot: HomieSnapshot, supabase, state, heartbeat, sensor, healthcheck_url,
    clock_floor: datetime | None = None, max_silence: float = 120.0, dry_run: bool = False,
) -> bool:
    success = False
    try:
        reason = snapshot.unready_reason(max_silence)
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        if reason is not None:
            logger.error(f"SPAN snapshot not ready: {reason}")
            state.record_failure(f"SPAN snapshot not ready: {reason}")
        elif clock_floor is not None and now_dt < clock_floor:
            logger.error(
                f"System clock ({now}) is behind the newest stored row "
                f"({clock_floor.isoformat()}); refusing to insert until "
                "the clock catches up"
            )
            state.record_failure("clock behind newest stored row")
        else:
            try:
                main_row, branch_rows = snapshot_to_rows(snapshot, now)
            except SnapshotIncomplete as e:
                logger.warning(f"Snapshot incomplete: {e}")
                state.record_failure(f"snapshot incomplete: {e}")
            else:
                if dry_run:
                    logger.info(f"DRY RUN main={main_row} branches={len(branch_rows)} "
                                f"sum_branch_w={sum(b['instant_power_w'] for b in branch_rows):.1f}")
                    success = True
                elif insert_data(main_row, branch_rows, supabase):
                    logger.info(f"OK: {main_row['instant_grid_power_w']} W")
                    success = True
                else:
                    logger.error("Insert failed: data did not reach the database")
                    state.record_failure("insert failed")
    except Exception as e:
        logger.exception("Unexpected error in poll loop, continuing...")
        state.record_failure(str(e))

    # ... existing post-tick notification try/except block, unchanged ...
    return success
```

Replace the SPAN-specific part of `__main__` (the `span_bearer_token` / `url` / `headers` lines) with:

```python
    span_ip = os.environ["SPAN_IP"]
    span_passphrase = os.environ["SPAN_PASSPHRASE"]
    auth_cache = Path(os.getenv("SPAN_AUTH_CACHE", "~/.span_monitor_auth.json")).expanduser()
    max_silence = float(os.getenv("MQTT_MAX_SILENCE", "120"))
    dry_run = os.getenv("SPAN_DRY_RUN", "") == "1"
    if dry_run:
        logger.warning("SPAN_DRY_RUN=1: rows are logged, not inserted")
```

and, after the health server, throttles, and `clock_floor` are set up (unchanged), before the loop:

```python
    # Registration failures are fatal at startup: without credentials there is
    # nothing to monitor, and a crash-looping container is visible where a
    # silently idle one is not.
    try:
        creds = load_or_register(span_ip, span_passphrase, auth_cache)
    except (SpanAuthError, OSError, requests.exceptions.RequestException) as e:
        logger.error(f"SPAN API authentication failed: {e}")
        sys.exit(1)

    def on_auth_failure():
        logger.error("Broker rejected cached credentials; clearing cache and exiting to re-register")
        invalidate(auth_cache)
        os._exit(1)  # from paho's thread; sys.exit would only end that thread

    snapshot = HomieSnapshot(creds.serial_number)
    mqtt_client = SpanMqtt(span_ip, creds, snapshot, on_auth_failure)
    mqtt_client.start()
```

Loop body becomes `poll_once(snapshot, supabase, state, heartbeat, sensor, healthcheck_url, clock_floor, max_silence, dry_run)`, and the `finally:` block calls `mqtt_client.stop()` before logging.

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -v`
Expected: all pass. `grep -n "api/v1\|SPAN_API_KEY\|get_span_response" main.py` returns nothing.

- [ ] **Step 6: Smoke test against the real panel in dry-run (LAN, alongside the live v1 service)**

```bash
SPAN_DRY_RUN=1 SPAN_PASSPHRASE=... uv run python main.py
```

Expected within ~10 s: `MQTT connected`, then a `DRY RUN main={...}` line every 5 s. Stop with Ctrl-C. Note: the health server binds 8099; if the v1 service runs on the same host, add `HEALTH_PORT=8199`.

- [ ] **Step 7: Commit**

```bash
git add main.py test_main.py
git commit -m "Read panel data from the SPAN API MQTT snapshot instead of /api/v1/panel"
```

---

### Task 7: Deployment config, image, and docs

**Files:**
- Modify: `config.yaml`, `addon-run.sh`, `Dockerfile`, `CLAUDE.md`, `README.md`, `DOCS.md`, `run.sh` (only if it references `SPAN_API_KEY`)

- [ ] **Step 1: `config.yaml`**

Bump `version: "2.0.0"` (breaking config change). In `options` replace `span_api_key: ""` with `span_passphrase: ""` and add `dry_run: false`; in `schema` replace `span_api_key: password` with `span_passphrase: password` and add `dry_run: bool`.

- [ ] **Step 2: `addon-run.sh`**

Replace the `SPAN_API_KEY` export with:

```bash
export SPAN_PASSPHRASE="$(bashio::config 'span_passphrase')"
# /data persists across restarts and upgrades, so the panel registration
# happens once rather than on every container start.
export SPAN_AUTH_CACHE="/data/span_auth.json"
if bashio::config.true 'dry_run'; then
    export SPAN_DRY_RUN=1
    bashio::log.warning "Dry run: rows will be logged, not inserted"
fi
```

and change `span_api_key` to `span_passphrase` in the required-options loop.

- [ ] **Step 3: `Dockerfile`**

```dockerfile
COPY main.py health.py notify.py homie.py extract.py span_auth.py span_mqtt.py ./
```

- [ ] **Step 4: Verify the image builds and imports**

Run: `docker build --platform linux/arm64 -t span_monitor:test . && docker run --rm --platform linux/arm64 --entrypoint python3 span_monitor:test -c "import main"`
Expected: build succeeds; the import exits 0. (If Docker/QEMU isn't available locally, note that and rely on the HA build in Task 8.)

- [ ] **Step 5: Docs**

- `CLAUDE.md` / `README.md` / `DOCS.md`: environment variables become `SPAN_IP`, `SPAN_PASSPHRASE` (the panel's hopPassphrase from the SPAN Home app), `SUPABASE_URL`, `SUPABASE_KEY`; optional `SPAN_AUTH_CACHE`, `MQTT_MAX_SILENCE`, `SPAN_DRY_RUN`. Architecture section: "Subscribes to the panel's eBus MQTT broker (SPAN API) and writes the latest snapshot every `POLL_INTERVAL` seconds" with one line each for `homie.py`, `extract.py`, `span_auth.py`, `span_mqtt.py`. Note the 2-pole `branch_id` change and the NULL v1-only columns, with the cutover date.
- `DOCS.md` (the HA app docs page): upgrade note — "2.0.0 replaces *SPAN API key* with *SPAN passphrase*. Find it in the SPAN Home app. Set it before starting 2.0.0."

- [ ] **Step 6: Commit**

```bash
git add config.yaml addon-run.sh Dockerfile CLAUDE.md README.md DOCS.md run.sh
git commit -m "Ship SPAN API MQTT client in the Home Assistant app (2.0.0)"
```

---

### Task 8: Cutover and post-cutover verification (operational, Alex)

No code. Do this before **2026-12-15**.

- [ ] **Step 1:** HA has a single app slot, so 2.0.0 can't dry-run there while 1.x keeps writing. Instead, run the dry-run on the Mac (Task 6 Step 6) for at least an hour while 1.x keeps writing from HA, and compare its logged `instant_grid_power_w` and `sum_branch_w` against the live rows:

```sql
-- what 1.x wrote over the same hour, for eyeballing against the DRY RUN log
select time, instant_grid_power_w from main_energy order by time desc limit 20;
select time, sum(instant_power_w) from branch_energy group by time order by time desc limit 20;
```

Expected: same sign, magnitudes within a few percent.

- [ ] **Step 2:** Cut over on an hour boundary, so no hourly bucket holds both v1 per-leg counters and 2.0.0 whole-circuit counters. Shortly before HH:00, **stop** the 1.x app. Pull and rebuild to 2.0.0, set `span_passphrase` (the first start without it fails with "Required option 'span_passphrase' is not set"), keep `dry_run: false`, and **start it after HH:00**. Record the UTC cutover hour. The few minutes of gap only lower that hour's `num_measurements`.
- [ ] **Step 3:** After 2 hours, check continuity in the hourly aggregate — no gap, no doubled `num_measurements`, and the HVAC series (`branch_id in (5, 7)`) continuous when summed:

```sql
select time, num_measurements from main_energy_hourly order by time desc limit 4;
select time, sum(avg_instant_power_w) from branch_energy_hourly
 where branch_id in (5, 7) group by time order by time desc limit 6;
```

```sql
-- Energy counters summed over each 2-pole circuit must not jump at the cutover hour.
select time, sum(exported_active_energy_wh) as hvac_exported_wh
  from branch_energy_hourly
 where branch_id in (5, 7) and time > now() - interval '6 hours'
 group by time order by time;
```

Expected: `num_measurements` around 650 (1.x ticks every ~5.47 s) before the cutover and somewhat higher after (2.0.0 skips the panel HTTP request; the dry run ticked every 5.0 s), not a doubling; HVAC power sum continuous across the cutover hour; `hvac_exported_wh` keeps rising by roughly one hour's consumption (a jump of ~1.3 MWh means an hour mixed v1 and 2.0.0 rows — record that hour as bad for energy columns in heat_pump_analysis).
- [ ] **Step 4:** Add a one-line note with the cutover timestamp to `heat_pump_analysis` (its README or handoff) so the 2-pole split change is on record next to the analysis that consumes the data. Close issue #3 referencing the merge commit.
