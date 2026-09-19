# Home Assistant App Packaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package the SPAN monitor as a local Home Assistant app so it runs continuously on a Raspberry Pi, self-heals from failure, and raises an off-premises alert when it genuinely needs a human.

**Architecture:** The existing `main.py` poll loop gains a definition of "successful tick" (SPAN returned 200 **and** both Supabase inserts landed). Two new modules hang off that signal: `health.py` serves `/healthz` so Supervisor's watchdog can restart a stalled container, and `notify.py` pings healthchecks.io as a dead-man switch and publishes a status sensor to Home Assistant. Four new root-level files (`config.yaml`, `Dockerfile`, `addon-run.sh`, `DOCS.md`) make the repo directly installable as a local app.

**Tech Stack:** Python 3.11+ (3.13 in the container), uv, requests, httpx, supabase-py, Home Assistant Supervisor app API, bashio, Docker (aarch64 Alpine)

**Spec:** `docs/superpowers/specs/2026-09-19-homeassistant-addon-design.md`

## Global Constraints

- **Target architecture is `aarch64` only.** No multi-arch builds.
- **Base image:** `ghcr.io/home-assistant/aarch64-base-python:3.13-alpine3.23`, hardcoded in the Dockerfile. `build.yaml` is deprecated and MUST NOT be created.
- **`run.sh` must not be deleted or repurposed.** It remains the macOS launcher. The container entrypoint is `addon-run.sh`.
- **The manifest files live at the repository root**, not in a subdirectory. Supervisor scans `/addons/<folder>/config.yaml` one level deep, and the Docker build context must include `main.py`, `pyproject.toml`, and `uv.lock`.
- **No Python code may know it is running under Home Assistant.** All HA-specific wiring happens in `addon-run.sh`; Python reads plain environment variables via `os.getenv`.
- **Every default must reproduce today's macOS behaviour exactly.** An unmodified `.env` with the four existing variables must keep working with no new configuration.
- **Neither `notify.py` function may stall or kill the poll loop.** 5-second timeouts, every exception caught and logged, and never wrapped in `@retry_on_connection_error` (which retries forever).
- **Supabase remains the only destination for energy data.** No SPAN measurements are published to Home Assistant — only the monitor's own health.
- **Tests run with `uv run pytest` from the repo root.** `conftest.py` puts the repo root on `sys.path`; top-level modules are imported by bare name (`import main`, `import health`).

---

## Chunk 1: Runtime hardening

### Task 1: Split dependencies so the container image stays slim

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock` (regenerated)

**Interfaces:**
- Consumes: nothing
- Produces: a `dependencies` list containing only monitor runtime packages, and the dependency groups `dev`, `tools`, `notebooks`

The container must not install `pandas`, `polars`, `psycopg2-binary`, or `ipykernel` — those belong to `archive.py`, `maintain.py`, and the notebooks. On a Pi they would dominate build time for no benefit.

`httpx` is added explicitly: `main.py` already does `import httpx` for `httpx.TransportError`, but it is undeclared and arrives transitively through `supabase`. That is survivable in a fat environment and a live hazard in a deliberately slim one.

- [ ] **Step 1: Rewrite the dependency declarations**

Replace the `dependencies` list and `[dependency-groups]` block in `pyproject.toml` with:

```toml
dependencies = [
    "requests>=2.32.3,<3",
    "python-dotenv>=1.0.1,<2",
    "supabase>=2.7.3,<3",
    "httpx>=0.28,<1",
]

[dependency-groups]
dev = [
    "pytest>=9.0.3,<10",
    "pytest-mock>=3.15.1,<4",
]
tools = [
    "click>=8.1,<9",
    "psycopg2-binary>=2.9.9,<3",
    "polars>=1.0,<2",
]
notebooks = [
    "ipykernel>=6.29.5,<7",
    "pandas>=2.2.2,<3",
]

[tool.uv]
package = false
default-groups = ["dev", "tools", "notebooks"]
```

`default-groups` is what keeps the macOS workflow byte-identical — a bare `uv sync` still installs everything. The container will run `uv sync --frozen --no-default-groups`.

- [ ] **Step 2: Regenerate the lockfile and verify the local environment is unchanged**

```bash
uv sync
```

Expected: resolves without conflicts. `uv.lock` is modified.

- [ ] **Step 3: Verify the container's dependency subset resolves to a small set**

```bash
uv export --frozen --no-default-groups --no-hashes | head -30
```

Expected: `requests`, `httpx`, `python-dotenv`, `supabase` and their transitive dependencies, and NO `pandas`, `polars`, `psycopg2-binary`, `ipykernel`, or `pytest`.

If `--no-default-groups` is rejected as an unknown flag, run `uv self update` — the Dockerfile in Task 10 depends on that flag, so an older uv would fail the image build too.

- [ ] **Step 4: Verify the existing test suite still passes**

```bash
uv run pytest -q
```

Expected: all tests pass. `archive.py` tests need `polars` and `psycopg2`, which are still present locally via `default-groups`.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "Split dependencies so the container image stays slim"
```

---

### Task 2: Make `insert_data` report whether the data actually landed

**Files:**
- Modify: `main.py` (the `insert_data` function)
- Test: `test_main.py`

**Interfaces:**
- Consumes: nothing
- Produces: `insert_data(data: dict, now: str, supabase: Client) -> bool` — `True` only when **both** the `main_energy` and `branch_energy` inserts succeed

`insert_data` currently catches `APIError`, logs it, and returns `None` either way. If Supabase starts rejecting writes — expired key, schema drift, tier limit — the loop logs `OK 200: 1234 W` every 5 seconds forever while nothing reaches the database.

This is precisely the silent failure the whole feature exists to detect. Every later task keys off this return value; without it the health endpoint, the heartbeat, and the sensor are all decorative.

Note that `test_insert_data_api_error` already exists and asserts the current logging behaviour. It is **updated, not deleted** — the logging assertion stays valid, and a return-value assertion is added.

- [ ] **Step 1: Write the failing tests**

Append to `test_main.py`:

```python
def test_insert_data_returns_true_on_success(mock_span_data):
    mock_supabase = Mock()
    mock_supabase.table.return_value.insert.return_value.execute.return_value = None

    now = datetime.now(UTC).isoformat()
    assert insert_data(mock_span_data, now, mock_supabase) is True


def test_insert_data_returns_false_when_branch_insert_fails(mock_span_data):
    """Main succeeds, branches fail: a partial write is still a failed tick."""
    mock_supabase = Mock()
    error = APIError({"message": "boom", "code": 500, "hint": "", "details": ""})
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = [
        None,    # main_energy succeeds
        error,   # branch_energy fails
    ]

    now = datetime.now(UTC).isoformat()
    assert insert_data(mock_span_data, now, mock_supabase) is False


def test_insert_data_returns_false_when_main_insert_fails(mock_span_data):
    mock_supabase = Mock()
    error = APIError({"message": "boom", "code": 500, "hint": "", "details": ""})
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = [
        error,   # main_energy fails
        None,    # branch_energy succeeds
    ]

    now = datetime.now(UTC).isoformat()
    assert insert_data(mock_span_data, now, mock_supabase) is False
```

Also update the existing `test_insert_data_api_error` so its final line asserts the return value. Change the body's last block to:

```python
    with patch('main.logger.error') as mock_logger_error:
        assert insert_data(mock_span_data, now, mock_supabase) is False
        mock_logger_error.assert_any_call(
            "Error inserting data: {'message': 'Test error', 'code': 402, 'hint': 'Blah', 'details': 'blah'}"
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_main.py -k insert_data -v
```

Expected: the three new tests FAIL with `assert None is True` / `assert None is False`.

- [ ] **Step 3: Implement the return value**

In `main.py`, change `insert_data` so each `try` block records its outcome and the function returns their conjunction. Replace the two bare `try`/`except APIError` blocks with this structure (the insert payloads themselves are unchanged):

```python
@retry_on_connection_error()
def insert_data(data: dict, now: str, supabase: Client) -> bool:
    """
    Insert one tick of panel data into 'main_energy' and 'branch_energy'.

    Returns True only if both inserts succeeded. A partial write counts as
    a failure: the health, heartbeat, and status layers all key off this
    value, so reporting success when rows did not land would recreate the
    silent failure this return value exists to expose.
    """
    main_ok = False
    logger.debug("Inserting data to main_energy")
    try:
        supabase.table("main_energy").insert(
            {
                # ... payload unchanged ...
            }
        ).execute()
        main_ok = True
    except APIError as e:
        logger.error(f"Error inserting data: {e}")
        logger.error(data)

    insert_records = [
        # ... unchanged ...
    ]

    branch_ok = False
    try:
        supabase.table("branch_energy").insert(insert_records).execute()
        branch_ok = True
    except APIError as e:
        logger.error(f"Error inserting data: {e}")
        logger.error(data)

    return main_ok and branch_ok
```

Keep the existing payload dictionaries exactly as they are — only the success tracking and the return are new. Drop the unused `response = ` assignments.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest test_main.py -v
```

Expected: all tests PASS, including the updated `test_insert_data_api_error`.

- [ ] **Step 5: Commit**

```bash
git add main.py test_main.py
git commit -m "Report insert success from insert_data

A swallowed APIError made a failing write indistinguishable from a
successful one, so the loop would log OK forever while nothing reached
the database. Health, heartbeat, and status all key off this value."
```

---

### Task 3: `HealthState` — the shared definition of "is this thing working"

**Files:**
- Create: `health.py`
- Test: `test_health.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `HealthState(stale_threshold: float = 300.0, clock: Callable[[], float] = time.monotonic)`
  - `.record_success() -> None`
  - `.record_failure(error: str = "") -> None`
  - `.seconds_since_success() -> float | None` — `None` before the first success
  - `.is_healthy() -> bool`
  - `.snapshot() -> dict` with keys `healthy`, `seconds_since_success`, `consecutive_errors`, `last_error`

The clock is injected so tests can advance time without sleeping. `time.monotonic` is used rather than wall-clock time because this measures elapsed duration, which must not jump when NTP corrects the Pi's clock.

The object is mutated by the poll loop and read by the HTTP server thread, so every access takes a lock.

- [ ] **Step 1: Write the failing tests**

Create `test_health.py`:

```python
import health
from health import HealthState


class FakeClock:
    """Manually advanced clock so tests never sleep."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_cold_start_is_unhealthy():
    """Before the first successful write there is nothing to trust."""
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    assert state.is_healthy() is False
    assert state.seconds_since_success() is None


def test_fresh_success_is_healthy():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(10.0)

    assert state.is_healthy() is True
    assert state.seconds_since_success() == 10.0


def test_success_older_than_threshold_is_unhealthy():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(301.0)

    assert state.is_healthy() is False


def test_exactly_at_threshold_is_still_healthy():
    """The boundary is inclusive, so a tick landing exactly on it is fine."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(300.0)

    assert state.is_healthy() is True


def test_failures_accumulate_and_success_resets_them():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_failure("first")
    state.record_failure("second")
    assert state.snapshot()["consecutive_errors"] == 2
    assert state.snapshot()["last_error"] == "second"

    state.record_success()
    assert state.snapshot()["consecutive_errors"] == 0


def test_failures_do_not_make_a_fresh_state_unhealthy():
    """Errors alone don't mean unhealthy — the retry layer is expected to
    hit transient errors. Only sustained absence of success does."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(5.0)
    state.record_failure("transient")

    assert state.is_healthy() is True


def test_snapshot_shape():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_success()

    assert state.snapshot() == {
        "healthy": True,
        "seconds_since_success": 0.0,
        "consecutive_errors": 0,
        "last_error": "",
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_health.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'health'`.

- [ ] **Step 3: Implement `HealthState`**

Create `health.py`:

```python
"""Health tracking and the /healthz endpoint Supervisor's watchdog polls."""

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

logger = logging.getLogger(__name__)

DEFAULT_STALE_THRESHOLD = 300.0


class HealthState:
    """Thread-safe record of whether data is currently reaching the database.

    Mutated by the poll loop and read by the HTTP server thread, so every
    access takes the lock. The clock is injected for testing and defaults to
    time.monotonic: this measures elapsed duration, which must not jump if
    NTP corrects the system clock.
    """

    def __init__(
        self,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.stale_threshold = stale_threshold
        self._clock = clock
        self._lock = threading.Lock()
        self._last_success: float | None = None
        self._consecutive_errors = 0
        self._last_error = ""

    def record_success(self) -> None:
        with self._lock:
            self._last_success = self._clock()
            self._consecutive_errors = 0
            self._last_error = ""

    def record_failure(self, error: str = "") -> None:
        with self._lock:
            self._consecutive_errors += 1
            self._last_error = error

    def seconds_since_success(self) -> float | None:
        with self._lock:
            return self._seconds_since_success_locked()

    def _seconds_since_success_locked(self) -> float | None:
        if self._last_success is None:
            return None
        return self._clock() - self._last_success

    def is_healthy(self) -> bool:
        with self._lock:
            elapsed = self._seconds_since_success_locked()
            return elapsed is not None and elapsed <= self.stale_threshold

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = self._seconds_since_success_locked()
            return {
                "healthy": elapsed is not None and elapsed <= self.stale_threshold,
                "seconds_since_success": elapsed,
                "consecutive_errors": self._consecutive_errors,
                "last_error": self._last_error,
            }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest test_health.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add health.py test_health.py
git commit -m "Add HealthState, the shared definition of a working monitor"
```

---

### Task 4: `/healthz` endpoint for Supervisor's watchdog

**Files:**
- Modify: `health.py`
- Test: `test_health.py`

**Interfaces:**
- Consumes: `HealthState` from Task 3
- Produces:
  - `health_response(state: HealthState) -> tuple[int, bytes]` — pure function returning `(status_code, json_body)`
  - `start_health_server(state: HealthState, port: int) -> HTTPServer` — starts a daemon thread and returns the server (port `0` picks a free port, and `server.server_address[1]` reveals it)

Splitting the pure `health_response` from the socket plumbing is what makes the status-code logic cheap to test exhaustively. One integration test then proves the handler is actually wired to it.

Supervisor restarts the container when this returns 503. The body is for a human reading `curl` output during debugging.

- [ ] **Step 1: Write the failing tests**

Append to `test_health.py`:

```python
import json

import requests

from health import health_response, start_health_server


def test_health_response_ok_when_fresh():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()

    status, body = health_response(state)

    assert status == 200
    assert json.loads(body)["healthy"] is True


def test_health_response_503_when_stale():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(301.0)

    status, body = health_response(state)

    assert status == 503
    assert json.loads(body)["seconds_since_success"] == 301.0


def test_health_response_503_on_cold_start():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())

    status, body = health_response(state)

    assert status == 503
    assert json.loads(body)["seconds_since_success"] is None


def test_health_server_serves_healthz():
    """Integration: proves the handler is wired to health_response."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    server = start_health_server(state, port=0)
    try:
        port = server.server_address[1]
        response = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=5)
        assert response.status_code == 200
        assert response.json()["healthy"] is True

        clock.advance(301.0)
        response = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=5)
        assert response.status_code == 503
    finally:
        server.shutdown()
        server.server_close()


def test_health_server_404s_unknown_paths():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    server = start_health_server(state, port=0)
    try:
        port = server.server_address[1]
        response = requests.get(f"http://127.0.0.1:{port}/nope", timeout=5)
        assert response.status_code == 404
    finally:
        server.shutdown()
        server.server_close()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_health.py -k "health_response or health_server" -v
```

Expected: FAIL with `ImportError: cannot import name 'health_response' from 'health'`.

- [ ] **Step 3: Implement the endpoint**

Append to `health.py`:

```python
def health_response(state: HealthState) -> tuple[int, bytes]:
    """Return (status_code, json_body) describing current health.

    200 means data reached the database recently. 503 means it did not, and
    Supervisor's watchdog will restart the container. Cold start is 503:
    a process that has never succeeded has not earned the benefit of doubt.
    """
    snapshot = state.snapshot()
    status = 200 if snapshot["healthy"] else 503
    return status, json.dumps(snapshot).encode("utf-8")


class _HealthHandler(BaseHTTPRequestHandler):
    state: HealthState  # injected by start_health_server

    def do_GET(self):  # noqa: N802 - name mandated by BaseHTTPRequestHandler
        if self.path != "/healthz":
            self.send_response(404)
            self.end_headers()
            return

        status, body = health_response(self.state)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """Silence per-request stdout logging.

        The watchdog polls this endpoint continuously; logging every probe
        would bury the actual monitor output and churn the Pi's SD card.
        """
        return


def start_health_server(state: HealthState, port: int) -> HTTPServer:
    """Serve /healthz on a daemon thread and return the server.

    Pass port=0 to bind a free port, then read server.server_address[1].
    The thread is a daemon so it never blocks interpreter shutdown.
    """
    handler = type("BoundHealthHandler", (_HealthHandler,), {"state": state})
    server = HTTPServer(("0.0.0.0", port), handler)
    thread = threading.Thread(
        target=server.serve_forever, name="health-server", daemon=True
    )
    thread.start()
    logger.info(f"Health endpoint listening on port {server.server_address[1]}")
    return server
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest test_health.py -v
```

Expected: all 12 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add health.py test_health.py
git commit -m "Serve /healthz so Supervisor's watchdog can restart a stalled monitor"
```

---

### Task 5: `Throttle` — rate-limit the outbound notifications

**Files:**
- Create: `notify.py`
- Test: `test_notify.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `Throttle(interval: float, clock: Callable[[], float] = time.monotonic)`
  - `.ready() -> bool` — returns `True` at most once per `interval`, and records the time when it does

**Why this exists (a refinement on the spec).** The spec says to ping healthchecks.io "after each successful write". Taken literally at a 5-second poll interval that is 17,280 pings per day, which would hit healthchecks.io rate limits, and an equal volume of `sensor.span_monitor` writes would churn Home Assistant's recorder database on the Pi's SD card — the exact wear the spec elsewhere sets out to avoid.

Throttling changes nothing about detection power: healthchecks.io is configured with a 5-minute period and 5-minute grace, so one ping per minute is already 5x more often than required.

`ready()` deliberately mutates on read. That keeps the call site a single `if`, which is what stops a caller from checking and then forgetting to record.

- [ ] **Step 1: Write the failing tests**

Create `test_notify.py`:

```python
from notify import Throttle


class FakeClock:
    """Manually advanced clock so tests never sleep."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_throttle_allows_first_call():
    assert Throttle(60.0, clock=FakeClock()).ready() is True


def test_throttle_blocks_second_immediate_call():
    throttle = Throttle(60.0, clock=FakeClock())
    assert throttle.ready() is True
    assert throttle.ready() is False


def test_throttle_allows_again_after_interval():
    clock = FakeClock()
    throttle = Throttle(60.0, clock=clock)
    assert throttle.ready() is True

    clock.advance(59.0)
    assert throttle.ready() is False

    clock.advance(1.0)
    assert throttle.ready() is True


def test_throttle_zero_interval_always_ready():
    """interval=0 disables throttling, for tests and debugging."""
    throttle = Throttle(0.0, clock=FakeClock())
    assert throttle.ready() is True
    assert throttle.ready() is True
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_notify.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'notify'`.

- [ ] **Step 3: Implement `Throttle`**

Create `notify.py`:

```python
"""Outbound notifications: the healthchecks.io dead-man switch and the
Home Assistant status sensor.

Every function here is best-effort. A monitoring system that can take down
the thing it monitors is worse than no monitoring, so nothing in this module
raises, blocks for long, or retries indefinitely.
"""

import logging
import os
import time
from typing import Callable

import requests

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 5.0
SUPERVISOR_API = "http://supervisor/core/api"
DEFAULT_ENTITY_ID = "sensor.span_monitor"


class Throttle:
    """Allow an action at most once per interval.

    ready() mutates on read: it records the time whenever it returns True.
    That keeps call sites to a single `if`, so a caller cannot check and
    then forget to record.
    """

    def __init__(self, interval: float, clock: Callable[[], float] = time.monotonic):
        self.interval = interval
        self._clock = clock
        self._last: float | None = None

    def ready(self) -> bool:
        now = self._clock()
        if self._last is not None and now - self._last < self.interval:
            return False
        self._last = now
        return True
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest test_notify.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add notify.py test_notify.py
git commit -m "Add Throttle to rate-limit outbound notifications

At a 5s poll interval an unthrottled heartbeat would be 17k pings/day and
an equal volume of recorder writes on the Pi's SD card."
```

---

### Task 6: `ping_healthcheck` — the dead-man switch

**Files:**
- Modify: `notify.py`
- Test: `test_notify.py`

**Interfaces:**
- Consumes: nothing
- Produces: `ping_healthcheck(url: str | None, timeout: float = TIMEOUT_SECONDS) -> bool` — `True` if the ping was delivered, `False` on any failure or when `url` is falsy

This is the only alerting layer that survives the Pi being dead, the LAN being down, or the house losing power — because the alarm lives off-premises. It must therefore be maximally boring: one GET, short timeout, and no failure mode that can propagate.

It must **not** be decorated with `@retry_on_connection_error`. That decorator retries forever, which is correct for the data path and catastrophic here — a healthchecks.io outage would freeze the poll loop permanently.

- [ ] **Step 1: Write the failing tests**

Append to `test_notify.py`:

```python
from unittest.mock import Mock

import requests

from notify import ping_healthcheck


def test_ping_healthcheck_gets_the_url(mocker):
    mock_get = mocker.patch("notify.requests.get")
    mock_get.return_value = Mock(status_code=200)

    assert ping_healthcheck("https://hc-ping.com/abc") is True
    mock_get.assert_called_once_with("https://hc-ping.com/abc", timeout=5.0)


def test_ping_healthcheck_noop_without_url():
    """No configured URL means no heartbeat - the macOS path stays unchanged."""
    assert ping_healthcheck(None) is False
    assert ping_healthcheck("") is False


def test_ping_healthcheck_swallows_connection_errors(mocker):
    mocker.patch(
        "notify.requests.get",
        side_effect=requests.exceptions.ConnectionError("no route to host"),
    )
    assert ping_healthcheck("https://hc-ping.com/abc") is False


def test_ping_healthcheck_swallows_timeouts(mocker):
    mocker.patch(
        "notify.requests.get",
        side_effect=requests.exceptions.Timeout("too slow"),
    )
    assert ping_healthcheck("https://hc-ping.com/abc") is False


def test_ping_healthcheck_swallows_unexpected_exceptions(mocker):
    """Belt and braces: nothing from this call may reach the poll loop."""
    mocker.patch("notify.requests.get", side_effect=ValueError("surprise"))
    assert ping_healthcheck("https://hc-ping.com/abc") is False


def test_ping_healthcheck_reports_false_on_error_status(mocker):
    mocker.patch("notify.requests.get", return_value=Mock(status_code=404))
    assert ping_healthcheck("https://hc-ping.com/abc") is False
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_notify.py -k ping -v
```

Expected: FAIL with `ImportError: cannot import name 'ping_healthcheck' from 'notify'`.

- [ ] **Step 3: Implement `ping_healthcheck`**

Append to `notify.py`:

```python
def ping_healthcheck(url: str | None, timeout: float = TIMEOUT_SECONDS) -> bool:
    """GET the healthchecks.io ping URL. Returns True if delivered.

    Deliberately NOT wrapped in @retry_on_connection_error: that decorator
    retries forever, so an outage at the ping service would freeze the poll
    loop. A missed heartbeat is self-correcting - the next tick sends another,
    and the configured grace period is far longer than the ping interval.
    """
    if not url:
        return False

    try:
        response = requests.get(url, timeout=timeout)
    except Exception as e:
        logger.warning(f"Heartbeat ping failed: {e}")
        return False

    if response.status_code != 200:
        logger.warning(f"Heartbeat ping returned {response.status_code}")
        return False

    return True
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest test_notify.py -v
```

Expected: all 10 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add notify.py test_notify.py
git commit -m "Add healthchecks.io dead-man switch ping"
```

---

### Task 7: `publish_ha_sensor` — status entity in Home Assistant

**Files:**
- Modify: `notify.py`
- Test: `test_notify.py`

**Interfaces:**
- Consumes: `HealthState` from Task 3
- Produces: `publish_ha_sensor(state: HealthState, entity_id: str = DEFAULT_ENTITY_ID, timeout: float = TIMEOUT_SECONDS) -> bool`

Posts to `http://supervisor/core/api/states/<entity_id>` with `Authorization: Bearer ${SUPERVISOR_TOKEN}`, which `homeassistant_api: true` in `config.yaml` enables. No MQTT broker is involved.

Reading `SUPERVISOR_TOKEN` from the environment is what keeps Python ignorant of Home Assistant: on macOS the variable is absent, the function no-ops, and nothing changes.

The state value is seconds since last success. `unknown` is sent before the first success, because HA renders a missing numeric state badly and `unknown` is the honest answer.

- [ ] **Step 1: Write the failing tests**

Append to `test_notify.py`:

```python
from health import HealthState
from notify import publish_ha_sensor


def test_publish_ha_sensor_noop_without_token(mocker, monkeypatch):
    """On macOS there is no Supervisor, so this must do nothing at all."""
    monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
    mock_post = mocker.patch("notify.requests.post")

    state = HealthState(clock=FakeClock())
    assert publish_ha_sensor(state) is False
    mock_post.assert_not_called()


def test_publish_ha_sensor_posts_state_and_attributes(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mock_post = mocker.patch("notify.requests.post", return_value=Mock(status_code=200))

    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(12.0)

    assert publish_ha_sensor(state) is True

    args, kwargs = mock_post.call_args
    assert args[0] == "http://supervisor/core/api/states/sensor.span_monitor"
    assert kwargs["headers"]["Authorization"] == "Bearer tok123"
    assert kwargs["timeout"] == 5.0
    assert kwargs["json"]["state"] == 12.0
    assert kwargs["json"]["attributes"]["consecutive_errors"] == 0
    assert kwargs["json"]["attributes"]["healthy"] is True


def test_publish_ha_sensor_reports_unknown_before_first_success(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mock_post = mocker.patch("notify.requests.post", return_value=Mock(status_code=200))

    publish_ha_sensor(HealthState(clock=FakeClock()))

    assert mock_post.call_args.kwargs["json"]["state"] == "unknown"


def test_publish_ha_sensor_swallows_errors(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mocker.patch(
        "notify.requests.post",
        side_effect=requests.exceptions.ConnectionError("supervisor unreachable"),
    )

    state = HealthState(clock=FakeClock())
    state.record_success()
    assert publish_ha_sensor(state) is False


def test_publish_ha_sensor_swallows_unexpected_exceptions(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mocker.patch("notify.requests.post", side_effect=ValueError("surprise"))

    state = HealthState(clock=FakeClock())
    state.record_success()
    assert publish_ha_sensor(state) is False
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_notify.py -k publish -v
```

Expected: FAIL with `ImportError: cannot import name 'publish_ha_sensor' from 'notify'`.

- [ ] **Step 3: Implement `publish_ha_sensor`**

Append to `notify.py`:

```python
def publish_ha_sensor(
    state: "HealthState",
    entity_id: str = DEFAULT_ENTITY_ID,
    timeout: float = TIMEOUT_SECONDS,
) -> bool:
    """Publish monitor health to Home Assistant via the Supervisor proxy.

    No-ops when SUPERVISOR_TOKEN is absent, which is how the macOS path stays
    identical: Python never needs to know whether it is running under Home
    Assistant. Enabled by homeassistant_api: true in config.yaml.

    Reports only the monitor's own health. SPAN energy data goes to Supabase
    and nowhere else.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return False

    snapshot = state.snapshot()
    elapsed = snapshot["seconds_since_success"]
    payload = {
        "state": "unknown" if elapsed is None else elapsed,
        "attributes": {
            "friendly_name": "SPAN Monitor",
            "unit_of_measurement": "s",
            "icon": "mdi:flash",
            "healthy": snapshot["healthy"],
            "consecutive_errors": snapshot["consecutive_errors"],
            "last_error": snapshot["last_error"],
        },
    }

    try:
        response = requests.post(
            f"{SUPERVISOR_API}/states/{entity_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
    except Exception as e:
        logger.warning(f"Could not publish status sensor: {e}")
        return False

    if response.status_code not in (200, 201):
        logger.warning(f"Status sensor POST returned {response.status_code}")
        return False

    return True
```

Add the import for the type hint at the top of `notify.py`, below the existing imports:

```python
from health import HealthState
```

Then change the annotation `state: "HealthState"` to `state: HealthState`.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run pytest test_notify.py -v
```

Expected: all 15 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add notify.py test_notify.py
git commit -m "Publish monitor health as an HA status sensor"
```

---

### Task 8: Wire health, heartbeat, and sensor into the poll loop

**Files:**
- Modify: `main.py`
- Modify: `run.sh`
- Test: `test_main.py`

**Interfaces:**
- Consumes: `insert_data` (Task 2), `HealthState` / `start_health_server` (Tasks 3–4), `Throttle` / `ping_healthcheck` / `publish_ha_sensor` (Tasks 5–7)
- Produces: `poll_once(url, headers, supabase, state, heartbeat, sensor, healthcheck_url) -> bool`

The infinite `while True` in `__main__` cannot be unit tested. Extracting one tick into `poll_once` makes the interesting logic — what counts as success, what gets recorded, what gets pinged — directly testable, and leaves `__main__` as pure wiring.

Config is read from environment variables whose defaults reproduce today's macOS behaviour exactly, so an unmodified `.env` keeps working.

The `FileHandler` becomes conditional. Writing `span.log` every 5 seconds on an SD card is needless wear, and Supervisor already captures and rotates stdout. `run.sh` sets `SPAN_LOG_FILE=span.log` so the macOS path is unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `test_main.py`:

```python
from health import HealthState
from notify import Throttle
from main import poll_once


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _always_ready():
    return Throttle(0.0, clock=FakeClock())


def test_poll_once_records_success_and_pings(mocker, mock_span_data):
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=True)
    mock_ping = mocker.patch("main.ping_healthcheck", return_value=True)
    mocker.patch("main.publish_ha_sensor", return_value=True)

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc") is True

    assert state.is_healthy() is True
    mock_ping.assert_called_once_with("https://hc-ping.com/abc")


def test_poll_once_failed_insert_records_failure_and_does_not_ping(mocker, mock_span_data):
    """The silent-failure case: SPAN answers 200 but nothing reaches the DB."""
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=False)
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc") is False

    assert state.is_healthy() is False
    assert state.snapshot()["consecutive_errors"] == 1
    mock_ping.assert_not_called()


def test_poll_once_non_200_records_failure(mocker):
    mocker.patch("main.get_span_response", return_value=Mock(status_code=503, text="unavailable"))
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), None) is False

    assert state.snapshot()["consecutive_errors"] == 1
    mock_ping.assert_not_called()


def test_poll_once_survives_unexpected_exceptions(mocker):
    """Nothing may escape a tick - the service must outlive any single error."""
    mocker.patch("main.get_span_response", side_effect=ValueError("surprise"))
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), None) is False
    assert state.snapshot()["consecutive_errors"] == 1


def test_poll_once_respects_heartbeat_throttle(mocker, mock_span_data):
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=True)
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    clock = FakeClock()
    heartbeat = Throttle(60.0, clock=clock)
    state = HealthState(clock=clock)

    for _ in range(3):
        poll_once("http://x", {}, Mock(), state, heartbeat, _always_ready(), "https://hc-ping.com/abc")

    assert mock_ping.call_count == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest test_main.py -k poll_once -v
```

Expected: FAIL with `ImportError: cannot import name 'poll_once' from 'main'`.

- [ ] **Step 3: Rewrite the logging setup, add config, and extract `poll_once`**

In `main.py`, replace the `logging.basicConfig(...)` block at the top with:

```python
# Log to stdout always; add a file only when asked. Supervisor captures and
# rotates stdout, and writing a line every few seconds to a Raspberry Pi's
# SD card is needless wear. run.sh sets SPAN_LOG_FILE for the macOS path.
_handlers = [logging.StreamHandler()]
if _log_file := os.getenv("SPAN_LOG_FILE"):
    _handlers.append(logging.FileHandler(_log_file))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=_handlers,
)
```

Note `dotenv.load_dotenv()` must run **before** this block so `SPAN_LOG_FILE` can come from `.env`. Move the `dotenv.load_dotenv()` call above the logging setup.

Add the new imports below the existing ones:

```python
from health import HealthState, start_health_server
from notify import Throttle, ping_healthcheck, publish_ha_sensor
```

Add `poll_once` after `insert_data`:

```python
def poll_once(url, headers, supabase, state, heartbeat, sensor, healthcheck_url) -> bool:
    """Run one poll/insert tick. Returns True if data reached the database.

    A tick counts as successful only when SPAN answered 200 AND both inserts
    landed. Anything less is a failure, because a tick that logs OK while the
    database stays empty is the exact failure this service exists to catch.

    Never raises: the service must outlive any single error.
    """
    success = False
    try:
        response = get_span_response(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            now = datetime.now(UTC).isoformat()
            if insert_data(data, now, supabase):
                logger.info(f"OK {response.status_code}: {data['instantGridPowerW']} W")
                success = True
            else:
                logger.error("Insert failed: data did not reach the database")
                state.record_failure("insert failed")
        else:
            logger.error(f"BAD {response.status_code}: {response.text}")
            state.record_failure(f"SPAN returned {response.status_code}")
    except Exception as e:
        # Last-resort safety net: never let an unexpected error kill the
        # service. Log it and keep polling; the next tick will retry.
        # (KeyboardInterrupt is a BaseException, so it still exits.)
        logger.exception("Unexpected error in poll loop, continuing...")
        state.record_failure(str(e))

    if success:
        state.record_success()
        if heartbeat.ready():
            ping_healthcheck(healthcheck_url)

    if sensor.ready():
        publish_ha_sensor(state)

    return success
```

Replace the body of `if __name__ == "__main__":` below the existing `supabase` client creation with:

```python
    poll_interval = float(os.getenv("POLL_INTERVAL", "5"))
    stale_threshold = float(os.getenv("STALE_THRESHOLD", "300"))
    health_port = int(os.getenv("HEALTH_PORT", "8099"))
    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL", "60"))
    sensor_interval = float(os.getenv("SENSOR_INTERVAL", "30"))
    healthcheck_url = os.getenv("HEALTHCHECK_URL")

    state = HealthState(stale_threshold=stale_threshold)
    start_health_server(state, health_port)
    heartbeat = Throttle(heartbeat_interval)
    sensor = Throttle(sensor_interval)

    try:
        while True:
            poll_once(url, headers, supabase, state, heartbeat, sensor, healthcheck_url)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.error("Interrupt received, exiting gracefully...")
    finally:
        logger.error("Connection closed")
```

Also delete the duplicated `import os` and `import time` lines at the top of the file while you are here — each appears twice.

- [ ] **Step 4: Run the full test suite**

```bash
uv run pytest -q
```

Expected: all tests PASS.

- [ ] **Step 5: Keep the macOS log file by setting `SPAN_LOG_FILE` in `run.sh`**

Replace the contents of `run.sh` with:

```bash
#!/usr/bin/env bash
# macOS launcher. The Home Assistant app uses addon-run.sh instead.
# Only one of the two may run at a time: concurrent writers double the rows
# per tick and corrupt the hourly continuous aggregates.
cd /Users/alex.hasha/repos/span_monitor && \
    SPAN_LOG_FILE=span.log caffeinate -i -s nohup uv run python main.py &
```

- [ ] **Step 6: Verify the monitor still starts locally**

```bash
timeout 20 uv run python main.py
```

Expected: `Health endpoint listening on port 8099` followed by `OK 200: … W` lines (or connection errors if the SPAN panel is unreachable from this machine — that is fine, the point is that startup works and nothing crashes). Confirm no `span.log` is created, since `SPAN_LOG_FILE` is unset.

In a second terminal while it runs:

```bash
curl -i http://127.0.0.1:8099/healthz
```

Expected: `HTTP/1.0 200 OK` with a JSON body, or 503 if no insert has succeeded yet.

- [ ] **Step 7: Commit**

```bash
git add main.py test_main.py run.sh
git commit -m "Wire health endpoint, heartbeat, and status sensor into the poll loop"
```

---

## Chunk 2: Home Assistant packaging

### Task 9: App manifest and documentation tab

**Files:**
- Create: `config.yaml`
- Create: `DOCS.md`

**Interfaces:**
- Consumes: the environment variable names from Task 8
- Produces: the option keys `span_ip`, `span_api_key`, `supabase_url`, `supabase_key`, `healthcheck_url`, `poll_interval`, `stale_threshold`, `heartbeat_interval` — consumed by `addon-run.sh` in Task 10

The manifest sits at the repository root because Supervisor scans `/addons/<folder>/config.yaml` exactly one level deep.

- [ ] **Step 1: Create `config.yaml`**

```yaml
name: SPAN Monitor
version: "1.0.0"
slug: span_monitor
description: Polls a SPAN electrical panel and logs energy data to Supabase
url: https://github.com/ahasha/span_monitor
arch:
  - aarch64
startup: application
boot: auto
init: false
homeassistant_api: true
watchdog: "http://[HOST]:[PORT:8099]/healthz"
ports:
  8099/tcp: 8099
ports_description:
  8099/tcp: Health endpoint (/healthz) used by the watchdog
options:
  span_ip: ""
  span_api_key: ""
  supabase_url: ""
  supabase_key: ""
  healthcheck_url: ""
  poll_interval: 5
  stale_threshold: 300
  heartbeat_interval: 60
schema:
  span_ip: str
  span_api_key: password
  supabase_url: url
  supabase_key: password
  healthcheck_url: "str?"
  poll_interval: int(1,60)
  stale_threshold: int(60,3600)
  heartbeat_interval: int(10,600)
```

`init: false` because the Home Assistant base images ship s6-overlay. The `password` schema type makes Home Assistant mask those fields in the UI.

- [ ] **Step 2: Create `DOCS.md`**

This is rendered on the app's Documentation tab.

```markdown
# SPAN Monitor

Polls a SPAN electrical panel on the local network every few seconds and writes
aggregate and per-circuit energy data to Supabase (TimescaleDB).

## Configuration

| Option | Required | Description |
|---|---|---|
| `span_ip` | yes | Local IP address of the SPAN panel, e.g. `192.168.1.50` |
| `span_api_key` | yes | Bearer token for the SPAN panel API |
| `supabase_url` | yes | Supabase project URL |
| `supabase_key` | yes | Supabase service role key |
| `healthcheck_url` | no | healthchecks.io ping URL. Leave blank to disable alerting. |
| `poll_interval` | no | Seconds between polls (default 5) |
| `stale_threshold` | no | Seconds without a successful write before the watchdog restarts the app (default 300) |
| `heartbeat_interval` | no | Minimum seconds between heartbeat pings (default 60) |

## Alerting

Create a check at <https://healthchecks.io>, set **period** to 5 minutes and
**grace** to 5 minutes, add your email as the notification method, and paste the
ping URL into `healthcheck_url`. You will be alerted roughly 10 minutes after
data stops reaching the database — including when the Pi loses power or the
internet goes down, because the alarm lives off-premises.

## Health

The app serves `/healthz` on port 8099. It returns 200 when a write succeeded
within `stale_threshold` seconds and 503 otherwise. Supervisor's watchdog polls
this and restarts the container on 503, which recovers the case where the
process is alive but silently writing nothing.

A `sensor.span_monitor` entity reports seconds since the last successful write,
with `healthy`, `consecutive_errors`, and `last_error` attributes.

## Important

Do not run the macOS launcher (`run.sh`) at the same time as this app. Two
concurrent writers produce double the rows per tick at near-identical
timestamps, which corrupts the hourly continuous aggregates that are the
permanent historical record.
```

- [ ] **Step 3: Verify the YAML parses**

```bash
uv run python -c "import yaml,sys; print(yaml.safe_load(open('config.yaml'))['slug'])"
```

Expected: prints `span_monitor`. If `yaml` is missing, use `python3 -c` with any available YAML parser, or skip — Supervisor will report parse errors at install time.

- [ ] **Step 4: Commit**

```bash
git add config.yaml DOCS.md
git commit -m "Add Home Assistant app manifest and documentation"
```

---

### Task 10: Container image and entrypoint

**Files:**
- Create: `Dockerfile`
- Create: `addon-run.sh`
- Create: `.dockerignore`

**Interfaces:**
- Consumes: the option keys from Task 9, the environment variable names from Task 8
- Produces: a container that runs `python3 /app/main.py` with all configuration exported as environment variables

`addon-run.sh` is where every Home Assistant detail lives. Python stays ignorant of HA: it reads plain environment variables and would behave identically if launched by hand.

**`build.yaml` MUST NOT be created** — it is deprecated, and base-image configuration belongs in the Dockerfile.

- [ ] **Step 1: Create `.dockerignore`**

Keeps notebooks, the Tesla SDK, git history, and parquet archives out of the build context, which matters on a Pi.

```
.git
.github
.claude
notebooks/
tesla-sdk/
docs/
supabase/
tests/
test_*.py
conftest.py
*.ipynb
*.parquet
*.csv
*.log
.venv/
__pycache__/
.env
```

- [ ] **Step 2: Create `Dockerfile`**

```dockerfile
# Single-architecture build: this app targets aarch64 (Raspberry Pi) only.
# build.yaml is deprecated; base image configuration belongs here.
FROM ghcr.io/home-assistant/aarch64-base-python:3.13-alpine3.23

# Install into the system prefix rather than a venv so the entrypoint can
# simply run python3. Never download a Python interpreter: use the image's.
ENV UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir uv

WORKDIR /app

# Dependencies first so edits to the monitor source do not invalidate this
# layer. --no-default-groups excludes dev/tools/notebooks: the image gets
# requests, httpx, python-dotenv and supabase, not the scientific stack.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups

COPY main.py health.py notify.py ./

COPY addon-run.sh /addon-run.sh
RUN chmod a+x /addon-run.sh

CMD [ "/addon-run.sh" ]
```

- [ ] **Step 3: Create `addon-run.sh`**

```bash
#!/usr/bin/with-contenv bashio
# Container entrypoint. Translates Home Assistant options into the plain
# environment variables main.py already understands, so no Python code needs
# to know it is running under Home Assistant.
#
# This is NOT run.sh — that remains the macOS launcher.
set -e

export SPAN_IP="$(bashio::config 'span_ip')"
export SPAN_API_KEY="$(bashio::config 'span_api_key')"
export SUPABASE_URL="$(bashio::config 'supabase_url')"
export SUPABASE_KEY="$(bashio::config 'supabase_key')"
export POLL_INTERVAL="$(bashio::config 'poll_interval')"
export STALE_THRESHOLD="$(bashio::config 'stale_threshold')"
export HEARTBEAT_INTERVAL="$(bashio::config 'heartbeat_interval')"
export HEALTH_PORT="8099"

# Optional: no URL means no heartbeat, and ping_healthcheck no-ops.
if bashio::config.has_value 'healthcheck_url'; then
    export HEALTHCHECK_URL="$(bashio::config 'healthcheck_url')"
    bashio::log.info "Heartbeat pings enabled"
else
    bashio::log.warning "No healthcheck_url set - you will NOT be alerted if data stops"
fi

# Fail fast and legibly rather than letting Python raise on a None URL.
for required in span_ip span_api_key supabase_url supabase_key; do
    if ! bashio::config.has_value "${required}"; then
        bashio::exit.nok "Required option '${required}' is not set"
    fi
done

bashio::log.info "Starting SPAN monitor (poll every ${POLL_INTERVAL}s)"
exec python3 /app/main.py
```

Never log the secret values — `bashio::log` output goes to the app log, which is visible in the HA UI.

- [ ] **Step 4: Verify the shell script parses**

```bash
bash -n addon-run.sh
```

Expected: no output (syntax OK). `bash` cannot *run* it — `bashio` only exists inside the container — but it catches typos before a slow Pi build.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile addon-run.sh .dockerignore
git commit -m "Add container image and Home Assistant entrypoint"
```

- [ ] **Step 6: Build on the Pi and verify**

This is the first step that cannot be verified locally. Cross-building `aarch64` on macOS is slow emulation, and a successful cross-build still would not prove the bashio wiring works.

In the Studio Code Server terminal on the Pi:

```bash
git clone https://github.com/ahasha/span_monitor /addons/span-monitor
```

Then in Home Assistant: **Settings → Apps → ⟳ (refresh, top right) → Local apps → SPAN Monitor → Install**.

Expected: the image builds and the app appears. **If the build fails on a missing wheel** (most likely `pydantic-core`, which `supabase` pulls in and which needs a musl aarch64 wheel), the fix is to switch the base image to the Debian variant — change the `FROM` line to `ghcr.io/home-assistant/aarch64-base-python:3.13` and rebuild. Record whichever base image worked in `DOCS.md`.

- [ ] **Step 7: Confirm the watchdog port mapping**

The spec flagged this as unresolved: the docs say `watchdog` does not require a `ports` entry and that `[PORT:8099]` resolves to the *effective mapped* port, but not what happens when a port is mapped to `null`.

With the app running, from any machine on the LAN:

```bash
curl -i http://homeassistant.local:8099/healthz
```

Expected: a 200 or 503 JSON response. If that works, optionally try setting `ports: {8099/tcp: null}` in `config.yaml`, rebuild, and confirm from the app's log that the watchdog still probes. Keep whichever configuration works and note the outcome in `DOCS.md`.

---

### Task 11: README and cutover

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: everything above
- Produces: documentation only

- [ ] **Step 1: Expand `README.md`**

The current README is two lines. Replace it with:

```markdown
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
```

- [ ] **Step 2: Update `CLAUDE.md`**

Two edits, both in the Architecture section.

First, replace the `**main.py** is the entire service. It:` block (the numbered list through the `@retry_on_connection_error()` line) with:

```markdown
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
```

Second, replace the `run.sh` bullet in the Notes section with:

```markdown
- Two deployment paths, mutually exclusive: the Home Assistant app (`config.yaml` +
  `addon-run.sh`, normal operation) and macOS (`run.sh`, development/fallback).
  Running both at once doubles the rows per tick and corrupts the hourly aggregates.
- `run.sh` is hardcoded to the local machine path — update if running elsewhere
```

- [ ] **Step 3: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "Document both deployment paths and the mutual-exclusion rule"
```

- [ ] **Step 4: Cut over**

Ordered deliberately — overlap corrupts the continuous aggregates.

1. **Stop the macOS monitor** and confirm nothing remains:

```bash
pkill -f "python main.py"; sleep 2; pgrep -fl "python main.py" || echo "stopped"
```

2. **Create the healthchecks.io check** (period 5 min, grace 5 min, email notification) and copy the ping URL.

3. **Configure and start the app** on the Pi, with that ping URL in `healthcheck_url`.

4. **Verify three independent signals:**
   - The app log shows `OK 200: … W` lines
   - healthchecks.io shows a recent ping and the check is green
   - `sensor.span_monitor` exists in **Developer Tools → States**

5. **Verify there is exactly one writer.** Run against Supabase:

```sql
SELECT date_trunc('minute', time) AS minute, count(*)
FROM main_energy
WHERE time > now() - interval '10 minutes'
GROUP BY 1 ORDER BY 1;
```

Expected: roughly 12 rows/minute at a 5-second interval. Roughly 24 means both writers are live — stop one immediately.

6. **Test the self-healing path.** Stop the app from the HA UI, wait for healthchecks.io to alert (~10 minutes), confirm the email arrives, then start it again and confirm the check returns to green. An untested alarm is not an alarm.

**Rollback:** stop the app in the HA UI and run `./run.sh` on the Mac. `run.sh` and the `.env` path are untouched, so rollback needs no code changes.

---

## Appendix: What is deliberately not here

- **Multi-architecture images.** `aarch64` only.
- **CI and a published app repository.** Deferred until update friction justifies it. Adding it later means moving the manifest files into a subdirectory, adding `repository.yaml`, and a GitHub Actions workflow publishing to GHCR.
- **SPAN energy data as Home Assistant entities.** Only the monitor's own health is published. Energy data goes to Supabase and nowhere else.
- **The `insert_data` timestamp inconsistency.** `main_energy` uses naive `datetime.utcnow().isoformat()` while `branch_energy` uses the timezone-aware `now`. Real, but unrelated to this work; fixing it here would mix concerns and complicate the cutover verification.
