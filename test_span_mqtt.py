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
    # paho 2 reports a v3.1.1 refusal as reason code 134 (bad credentials) or 135 (not authorized)
    m._on_connect(client, None, None, rc(134, failure=True), None)
    m._on_connect(client, None, None, rc(135, failure=True), None)
    handler.assert_called_once()
    client.subscribe.assert_not_called()


def test_non_auth_refusal_does_not_call_handler():
    handler = Mock()
    m, client, snap = make(handler)
    m._on_connect(client, None, None, rc(136, failure=True), None)  # server unavailable
    handler.assert_not_called()
    assert snap.unready_reason(60) == "not connected"
