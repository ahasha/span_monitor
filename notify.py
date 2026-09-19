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
