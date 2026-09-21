"""Health tracking and the /healthz endpoint Supervisor's watchdog polls."""

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
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

    # StreamRequestHandler.setup() applies this as the connection socket's
    # timeout when it is not None. Without it, a client that opens a
    # connection and sends nothing blocks handle_one_request() in
    # rfile.readline() forever - see start_health_server for why that
    # matters on a LAN-exposed port.
    timeout = 5

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

    Uses ThreadingHTTPServer, not HTTPServer: the port is LAN-exposed, and a
    plain HTTPServer handles one request at a time, so a single client that
    opens a connection and sends nothing (a network scanner, a router
    discovery sweep, a sleeping laptop's half-open socket) would wedge every
    other request behind it - and Supervisor's watchdog would then restart a
    monitor that is working perfectly.
    """
    handler = type("BoundHealthHandler", (_HealthHandler,), {"state": state})
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    thread = threading.Thread(
        target=server.serve_forever, name="health-server", daemon=True
    )
    thread.start()
    logger.info(f"Health endpoint listening on port {server.server_address[1]}")
    return server
