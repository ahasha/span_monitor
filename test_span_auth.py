import json
from unittest.mock import Mock

import pytest

import span_auth
from span_auth import SpanAuthError, invalidate, load_or_register

PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
REGISTER_OK = {
    "accessToken": "tok", "tokenType": "bearer", "iatMs": 1, "hostname": "span-x.local",
    "serialNumber": "nt-0000-test0", "hopPassphrase": "pass",
    "ebusBrokerUsername": "nt-0000-test0", "ebusBrokerPassword": "brokerpw",
    "ebusBrokerHost": "span-x.local", "ebusBrokerMqttsPort": 8883,
    "ebusBrokerWsPort": 9001, "ebusBrokerWssPort": 9002,
}


def _resp(status, json_body=None, text=""):
    r = Mock(status_code=status, text=text)
    r.json.return_value = json_body
    return r


@pytest.fixture
def http(mocker):
    get = mocker.patch("span_auth.requests.get")
    post = mocker.patch("span_auth.requests.post")

    def fake_get(url, **kw):
        if url.endswith("/api/v2/status"):
            return _resp(200, {"serialNumber": "nt-0000-test0"})
        if url.endswith("/api/v2/certificate/ca"):
            return _resp(200, text=PEM)
        raise AssertionError(url)

    get.side_effect = fake_get
    post.return_value = _resp(200, REGISTER_OK)
    return get, post


def test_registers_and_caches_when_no_cache(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    creds = load_or_register("10.0.0.5", "pass", cache)

    assert creds.serial_number == "nt-0000-test0"
    assert creds.broker_password == "brokerpw"
    assert creds.mqtt_port == 8883
    assert creds.ca_cert_path.read_text() == PEM
    body = post.call_args.kwargs["json"]
    assert body["hopPassphrase"] == "pass"
    assert body["name"].startswith("span_monitor-")
    assert json.loads(cache.read_text())["broker_password"] == "brokerpw"
    assert oct(cache.stat().st_mode & 0o777) == "0o600"


def test_reuses_cache_without_registering(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    load_or_register("10.0.0.5", "pass", cache)
    post.reset_mock()

    creds = load_or_register("10.0.0.5", "pass", cache)

    post.assert_not_called()
    assert creds.broker_password == "brokerpw"


def test_reregisters_when_cached_serial_differs(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    cache.write_text(json.dumps({"serial_number": "other-panel", "broker_password": "x", "mqtt_port": 8883}))

    load_or_register("10.0.0.5", "pass", cache)

    post.assert_called_once()


def test_register_rejected_raises_without_leaking_secret(tmp_path, http):
    _, post = http
    post.return_value = _resp(422, {"detail": "bad credential"}, text="bad credential")

    with pytest.raises(SpanAuthError) as exc:
        load_or_register("10.0.0.5", "s3cret", tmp_path / "auth.json")
    assert "s3cret" not in str(exc.value)
    assert "422" in str(exc.value)


def test_register_without_broker_password_raises(tmp_path, http):
    _, post = http
    post.return_value = _resp(200, {**REGISTER_OK, "ebusBrokerPassword": None})

    with pytest.raises(SpanAuthError):
        load_or_register("10.0.0.5", "pass", tmp_path / "auth.json")


def test_rejects_non_pem_ca(tmp_path, http):
    get, _ = http
    get.side_effect = lambda url, **kw: (_resp(200, {"serialNumber": "nt-0000-test0"})
                                         if url.endswith("/status") else _resp(200, text="<html>"))
    with pytest.raises(SpanAuthError):
        load_or_register("10.0.0.5", "pass", tmp_path / "auth.json")


def test_invalidate_removes_cache_and_tolerates_absence(tmp_path):
    cache = tmp_path / "auth.json"
    cache.write_text("{}")
    invalidate(cache)
    assert not cache.exists()
    invalidate(cache)  # no error
