"""SPAN API credentials: register with the panel's hopPassphrase, cache the result.

Registration creates a named client on the panel, so it happens once and the
result is cached; a restart (or a watchdog-triggered container restart) must
not add another client. The cache holds the broker password, so it is written
0600 and never logged.

The panel's CA certificate can only be fetched over plain HTTP, so it is
trusted once, at registration, and pinned in the cache. SPAN keeps the CA
stable (server certificates renew and follow IP changes, the CA does not) and
advises investigating before trusting a changed one. So a changed CA is
reported, never adopted, unless trust_new_ca is set.
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


def _fetch_ca_cert(ip: str) -> str:
    r = requests.get(f"http://{ip}/api/v2/certificate/ca", timeout=TIMEOUT)
    if r.status_code != 200 or "BEGIN CERTIFICATE" not in r.text:
        raise SpanAuthError(f"Could not download the panel CA certificate (HTTP {r.status_code})")
    return r.text


def _warn_if_ca_changed(ip: str, pinned: str) -> None:
    """Best effort: a changed CA would otherwise surface only as endless TLS
    failures with no stated cause. Never blocks startup."""
    try:
        served = _fetch_ca_cert(ip)
    except (requests.exceptions.RequestException, SpanAuthError) as e:
        logger.warning(f"Could not compare the panel CA certificate with the pinned one: {e}")
        return
    if served.strip() != pinned.strip():
        logger.error(
            "The panel's CA certificate differs from the one pinned at registration; "
            "MQTT connections will fail TLS verification. SPAN advises investigating "
            "before trusting a changed CA. If the change is expected (e.g. a panel "
            "reset), start once with SPAN_TRUST_NEW_CA=1 (Home Assistant option "
            "trust_new_panel_ca) to pin the new one."
        )


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


def load_or_register(ip: str, passphrase: str, cache_path: Path,
                     trust_new_ca: bool = False) -> SpanCredentials:
    serial = _get_serial(ip)

    cached = _read_cache(cache_path)
    if cached and cached.get("serial_number") == serial and cached.get("broker_password"):
        logger.info("Using cached SPAN API credentials")
        if trust_new_ca or not cached.get("ca_pem"):
            if trust_new_ca:
                logger.warning("SPAN_TRUST_NEW_CA set: pinning the CA certificate the panel serves now")
            else:
                logger.info("Pinning the panel CA certificate (cache predates CA pinning)")
            cached["ca_pem"] = _fetch_ca_cert(ip)
            _write_cache(cache_path, cached)
        else:
            _warn_if_ca_changed(ip, cached["ca_pem"])
    else:
        # Fetch the CA first: if it fails, no client has been created.
        ca_pem = _fetch_ca_cert(ip)
        logger.info("Registering span_monitor as a SPAN API client")
        out = _register(ip, passphrase)
        cached = {"serial_number": out["serialNumber"],
                  "broker_username": out["ebusBrokerUsername"],
                  "broker_password": out["ebusBrokerPassword"],
                  "mqtt_port": out["ebusBrokerMqttsPort"],
                  "registered_at": time.time(),
                  "ca_pem": ca_pem}
        _write_cache(cache_path, cached)

    # paho needs a file; it is always rewritten from the pinned copy.
    ca_path = cache_path.with_suffix(".ca.crt")
    ca_path.parent.mkdir(parents=True, exist_ok=True)
    ca_path.write_text(cached["ca_pem"])

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
