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
    broker_username: str


def _get_serial(ip: str) -> str:
    r = requests.get(f"http://{ip}/api/v2/status", timeout=TIMEOUT)
    if r.status_code != 200:
        raise SpanAuthError(f"GET /api/v2/status returned {r.status_code}; does the panel firmware support SPAN API?")
    return r.json()["serialNumber"]


def _fetch_ca_cert(ip: str, dest: Path) -> Path:
    r = requests.get(f"http://{ip}/api/v2/certificate/ca", timeout=TIMEOUT)
    if r.status_code != 200 or "BEGIN CERTIFICATE" not in r.text:
        raise SpanAuthError(f"Could not download the panel CA certificate (HTTP {r.status_code})")
    dest.parent.mkdir(parents=True, exist_ok=True)
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
                  "broker_username": out["ebusBrokerUsername"],
                  "broker_password": out["ebusBrokerPassword"],
                  "mqtt_port": out["ebusBrokerMqttsPort"],
                  "registered_at": time.time()}
        _write_cache(cache_path, cached)

    return SpanCredentials(serial_number=cached["serial_number"],
                           broker_password=cached["broker_password"],
                           mqtt_port=int(cached.get("mqtt_port", 8883)),
                           ca_cert_path=ca_path,
                           # Caches written before broker_username was stored:
                           # SPAN documents the username as the panel serial.
                           broker_username=cached.get("broker_username") or cached["serial_number"])


def invalidate(cache_path: Path, min_age_seconds: float = 0) -> bool:
    """Delete cached credentials unless they were registered under min_age_seconds ago.

    A broker that refuses credentials minutes after issuing them is lagging
    or still booting, not rejecting a stale password. Deleting the cache then
    would register another client on every restart, and each one is a
    permanent entry on the panel. Undated caches count as old. Returns True
    if the cache was deleted.
    """
    cached = _read_cache(cache_path)
    if cached is None and not cache_path.exists():
        return False
    registered_at = (cached or {}).get("registered_at")
    if registered_at is not None and time.time() - registered_at < min_age_seconds:
        return False
    cache_path.unlink(missing_ok=True)
    return True
