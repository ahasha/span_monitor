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

from health import HealthState

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


def publish_ha_sensor(
    state: HealthState,
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
