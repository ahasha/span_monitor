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
                  "broker_password": out["ebusBrokerPassword"],
                  "mqtt_port": out["ebusBrokerMqttsPort"]}
        _write_cache(cache_path, cached)

    return SpanCredentials(serial_number=cached["serial_number"],
                           broker_password=cached["broker_password"],
                           mqtt_port=int(cached.get("mqtt_port", 8883)),
                           ca_cert_path=ca_path)


def invalidate(cache_path: Path) -> None:
    cache_path.unlink(missing_ok=True)
