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
