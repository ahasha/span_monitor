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

# v3.1.1 CONNACK codes (bad credentials, not authorized) and the v5 reason
# codes paho 2 reports them as.
AUTH_FAILURE_CODES = {4, 5, 134, 135}


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
