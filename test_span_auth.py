import json
from pathlib import Path
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
    # Plain HTTP (the CA download) goes through requests.get; HTTPS through the
    # pinned-CA helpers. One mock each, so call order and URLs stay inspectable.
    get = mocker.patch("span_auth.requests.get")
    mocker.patch("span_auth._https_get", get)
    post = mocker.patch("span_auth._https_post")

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


def test_cache_records_broker_username_and_registration_time(tmp_path, http, mocker):
    _, post = http
    post.return_value = _resp(200, {**REGISTER_OK, "ebusBrokerUsername": "broker-user"})
    mocker.patch("span_auth.time.time", return_value=1_000_000.0)
    cache = tmp_path / "auth.json"

    creds = load_or_register("10.0.0.5", "pass", cache)

    assert creds.broker_username == "broker-user"
    cached = json.loads(cache.read_text())
    assert cached["broker_username"] == "broker-user"
    assert cached["registered_at"] == 1_000_000.0


def test_legacy_cache_without_username_falls_back_to_serial(tmp_path, http):
    cache = tmp_path / "auth.json"
    cache.write_text(json.dumps({"serial_number": "nt-0000-test0", "broker_password": "pw", "mqtt_port": 8883}))
    creds = load_or_register("10.0.0.5", "pass", cache)
    assert creds.broker_username == "nt-0000-test0"


def test_invalidate_keeps_freshly_registered_credentials(tmp_path, mocker):
    """A refusal right after registration is the broker lagging or booting, not a
    bad password: deleting the cache would register another client every restart."""
    cache = tmp_path / "auth.json"
    cache.write_text(json.dumps({"registered_at": 1000.0}))
    mocker.patch("span_auth.time.time", return_value=1000.0 + 60)

    assert invalidate(cache, min_age_seconds=600) is False
    assert cache.exists()


def test_invalidate_removes_old_or_undated_credentials(tmp_path, mocker):
    mocker.patch("span_auth.time.time", return_value=1000.0 + 601)
    old = tmp_path / "old.json"
    old.write_text(json.dumps({"registered_at": 1000.0}))
    undated = tmp_path / "undated.json"
    undated.write_text(json.dumps({"broker_password": "pw"}))

    assert invalidate(old, min_age_seconds=600) is True
    assert invalidate(undated, min_age_seconds=600) is True
    assert not old.exists() and not undated.exists()


OTHER_PEM = "-----BEGIN CERTIFICATE-----\nOTHER\n-----END CERTIFICATE-----\n"


def _serve_ca(http, pem):
    get, _ = http
    get.side_effect = lambda url, **kw: (_resp(200, {"serialNumber": "nt-0000-test0"})
                                         if url.endswith("/api/v2/status") else _resp(200, text=pem))


def test_registration_pins_ca_in_cache(tmp_path, http):
    cache = tmp_path / "auth.json"
    creds = load_or_register("10.0.0.5", "pass", cache)
    assert json.loads(cache.read_text())["ca_pem"] == PEM
    assert creds.ca_cert_path.read_text() == PEM


def test_pinned_ca_is_used_even_if_panel_serves_a_different_one(tmp_path, http, caplog):
    cache = tmp_path / "auth.json"
    load_or_register("10.0.0.5", "pass", cache)
    _serve_ca(http, OTHER_PEM)

    creds = load_or_register("10.0.0.5", "pass", cache)

    assert creds.ca_cert_path.read_text() == PEM
    assert json.loads(cache.read_text())["ca_pem"] == PEM
    assert "SPAN_TRUST_NEW_CA" in caplog.text


def test_ca_comparison_failure_does_not_block_startup(tmp_path, http):
    import requests
    cache = tmp_path / "auth.json"
    load_or_register("10.0.0.5", "pass", cache)
    get, _ = http
    get.side_effect = lambda url, **kw: (_resp(200, {"serialNumber": "nt-0000-test0"})
                                         if url.endswith("/api/v2/status")
                                         else (_ for _ in ()).throw(requests.exceptions.ConnectionError()))

    creds = load_or_register("10.0.0.5", "pass", cache)

    assert creds.ca_cert_path.read_text() == PEM


def test_pinned_ca_file_is_rewritten_from_cache(tmp_path, http):
    cache = tmp_path / "auth.json"
    creds = load_or_register("10.0.0.5", "pass", cache)
    creds.ca_cert_path.unlink()
    _serve_ca(http, OTHER_PEM)

    creds = load_or_register("10.0.0.5", "pass", cache)

    assert creds.ca_cert_path.read_text() == PEM


def test_legacy_cache_without_ca_pins_it_once(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    cache.write_text(json.dumps({"serial_number": "nt-0000-test0", "broker_password": "pw", "mqtt_port": 8883}))

    load_or_register("10.0.0.5", "pass", cache)

    post.assert_not_called()
    assert json.loads(cache.read_text())["ca_pem"] == PEM
    assert json.loads(cache.read_text())["broker_password"] == "pw"


def test_trust_new_ca_repins_without_registering(tmp_path, http):
    _, post = http
    cache = tmp_path / "auth.json"
    load_or_register("10.0.0.5", "pass", cache)
    post.reset_mock()
    _serve_ca(http, OTHER_PEM)

    creds = load_or_register("10.0.0.5", "pass", cache, trust_new_ca=True)

    post.assert_not_called()
    assert creds.ca_cert_path.read_text() == OTHER_PEM
    assert json.loads(cache.read_text())["ca_pem"] == OTHER_PEM


def test_serial_check_and_registration_use_https_verified_with_ca(tmp_path, http):
    get, post = http
    seen = {}
    real_get = get.side_effect

    def spy_get(url, **kw):
        if url.endswith("/api/v2/status"):
            seen["status_url"], seen["status_verify"] = url, kw.get("verify")
            seen["ca_at_status"] = Path(kw["verify"]).read_text() if kw.get("verify") else None
        return real_get(url, **kw)

    get.side_effect = spy_get
    cache = tmp_path / "auth.json"
    creds = load_or_register("10.0.0.5", "pass", cache)

    assert seen["status_url"] == "https://10.0.0.5/api/v2/status"
    assert seen["status_verify"] == str(creds.ca_cert_path)
    assert seen["ca_at_status"] == PEM          # CA written before it is relied on
    assert post.call_args.args[0] == "https://10.0.0.5/api/v2/auth/register"
    assert post.call_args.kwargs["verify"] == str(creds.ca_cert_path)
    # Only the CA download itself is plain HTTP.
    http_urls = [c.args[0] for c in get.call_args_list if c.args[0].startswith("http://")]
    assert http_urls == ["http://10.0.0.5/api/v2/certificate/ca"]


def test_tls_verification_failure_is_an_auth_error_not_a_retry(tmp_path, http):
    import requests
    get, post = http
    cache = tmp_path / "auth.json"
    load_or_register("10.0.0.5", "pass", cache)
    post.reset_mock()

    def fake_get(url, **kw):
        if url.startswith("https://"):
            raise requests.exceptions.SSLError("certificate verify failed")
        return _resp(200, text=PEM)

    get.side_effect = fake_get
    with pytest.raises(SpanAuthError, match="SPAN_TRUST_NEW_CA"):
        load_or_register("10.0.0.5", "pass", cache)
    post.assert_not_called()


def test_tls_failure_during_registration_never_sends_passphrase(tmp_path, http):
    import requests
    get, post = http
    get.side_effect = lambda url, **kw: (_ for _ in ()).throw(requests.exceptions.SSLError("bad"))  \
        if url.startswith("https://") else _resp(200, text=PEM)

    with pytest.raises(SpanAuthError):
        load_or_register("10.0.0.5", "pass", tmp_path / "auth.json")
    post.assert_not_called()


# --- Real TLS against certificates shaped like the panel's ---------------------
# SPAN's self-signed CA and server certificates carry no Authority/Subject Key
# Identifier. Python 3.13 + urllib3 >= 2.3 turn on VERIFY_X509_STRICT, which
# rejects them ("Missing Authority Key Identifier"); 2.0.0 failed exactly this
# way on the Home Assistant image. These tests use a local HTTPS server.

import datetime
import ipaddress
import ssl
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


def _name(cn):
    return x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SPAN.io"),
                      x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _panel_like_pki(tmp_path, ca_cn="nt-0000-test0 CA"):
    """CA + server cert for 127.0.0.1 with no key identifiers, like the panel's."""
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (x509.CertificateBuilder().subject_name(_name(ca_cn)).issuer_name(_name(ca_cn))
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(days=1))
          .not_valid_after(now + datetime.timedelta(days=30))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False),
                         critical=True)
          .sign(ca_key, hashes.SHA256()))
    srv_key = ec.generate_private_key(ec.SECP256R1())
    srv = (x509.CertificateBuilder().subject_name(_name("span-test.local")).issuer_name(ca.subject)
           .public_key(srv_key.public_key()).serial_number(x509.random_serial_number())
           .not_valid_before(now - datetime.timedelta(days=1))
           .not_valid_after(now + datetime.timedelta(days=30))
           .add_extension(x509.KeyUsage(True, False, True, False, False, False, False, False, False),
                          critical=True)
           .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
                          critical=False)
           .sign(ca_key, hashes.SHA256()))
    d = tmp_path / ca_cn.replace(" ", "_")
    d.mkdir()
    (d / "ca.pem").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (d / "srv.pem").write_bytes(srv.public_bytes(serialization.Encoding.PEM))
    (d / "srv.key").write_bytes(srv_key.private_bytes(serialization.Encoding.PEM,
                                                      serialization.PrivateFormat.PKCS8,
                                                      serialization.NoEncryption()))
    return d


@pytest.fixture
def panel_https(tmp_path):
    d = _panel_like_pki(tmp_path)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({"serialNumber": "nt-0000-test0"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(d / "srv.pem", d / "srv.key")
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{server.server_address[1]}", d / "ca.pem"
    server.shutdown()


@pytest.mark.skipif(sys.version_info < (3, 13), reason="urllib3 enables X509 strict mode on 3.13+")
def test_plain_requests_rejects_panel_like_certs_on_py313(panel_https):
    """Documents the failure 2.0.0 hit, so the test below proves something."""
    import requests
    host, ca = panel_https
    with pytest.raises(requests.exceptions.SSLError, match="Key Identifier"):
        requests.get(f"https://{host}/api/v2/status", verify=str(ca), timeout=5)


def test_serial_check_accepts_panel_like_certs(panel_https):
    host, ca = panel_https
    assert span_auth._get_serial(host, ca) == "nt-0000-test0"


def test_serial_check_still_rejects_a_different_ca(panel_https, tmp_path):
    host, _ = panel_https
    other = _panel_like_pki(tmp_path, ca_cn="impostor CA") / "ca.pem"
    with pytest.raises(SpanAuthError, match="TLS verification"):
        span_auth._get_serial(host, other)


def test_panel_context_trusts_only_the_pinned_ca(tmp_path):
    d = _panel_like_pki(tmp_path)
    ctx = span_auth._panel_ssl_context(str(d / "ca.pem"))
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname
    assert not ctx.verify_flags & ssl.VERIFY_X509_STRICT
    assert len(ctx.get_ca_certs()) == 1
