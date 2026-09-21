from health import HealthState


class FakeClock:
    """Manually advanced clock so tests never sleep."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_cold_start_is_unhealthy():
    """Before the first successful write there is nothing to trust."""
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    assert state.is_healthy() is False
    assert state.seconds_since_success() is None


def test_fresh_success_is_healthy():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(10.0)

    assert state.is_healthy() is True
    assert state.seconds_since_success() == 10.0


def test_success_older_than_threshold_is_unhealthy():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(301.0)

    assert state.is_healthy() is False


def test_exactly_at_threshold_is_still_healthy():
    """The boundary is inclusive, so a tick landing exactly on it is fine."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(300.0)

    assert state.is_healthy() is True


def test_failures_accumulate_and_success_resets_them():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_failure("first")
    state.record_failure("second")
    assert state.snapshot()["consecutive_errors"] == 2
    assert state.snapshot()["last_error"] == "second"

    state.record_success()
    assert state.snapshot()["consecutive_errors"] == 0


def test_failures_do_not_make_a_fresh_state_unhealthy():
    """Errors alone don't mean unhealthy — the retry layer is expected to
    hit transient errors. Only sustained absence of success does."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(5.0)
    state.record_failure("transient")

    assert state.is_healthy() is True


def test_snapshot_shape():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_success()

    assert state.snapshot() == {
        "healthy": True,
        "seconds_since_success": 0.0,
        "consecutive_errors": 0,
        "last_error": "",
    }


import json

import requests

from health import health_response, start_health_server


def test_health_response_ok_when_fresh():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()

    status, body = health_response(state)

    assert status == 200
    assert json.loads(body)["healthy"] is True


def test_health_response_503_when_stale():
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(301.0)

    status, body = health_response(state)

    assert status == 503
    assert json.loads(body)["seconds_since_success"] == 301.0


def test_health_response_503_on_cold_start():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())

    status, body = health_response(state)

    assert status == 503
    assert json.loads(body)["seconds_since_success"] is None


def test_health_server_serves_healthz():
    """Integration: proves the handler is wired to health_response."""
    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    server = start_health_server(state, port=0)
    try:
        port = server.server_address[1]
        response = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=5)
        assert response.status_code == 200
        assert response.json()["healthy"] is True

        clock.advance(301.0)
        response = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=5)
        assert response.status_code == 503
    finally:
        server.shutdown()
        server.server_close()


def test_health_server_404s_unknown_paths():
    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    server = start_health_server(state, port=0)
    try:
        port = server.server_address[1]
        response = requests.get(f"http://127.0.0.1:{port}/nope", timeout=5)
        assert response.status_code == 404
    finally:
        server.shutdown()
        server.server_close()


def test_health_server_survives_a_stalled_connection():
    """A client that opens a TCP connection and sends nothing must not be
    able to wedge the server for everyone else - a network scanner or a
    half-open socket is normal on a LAN-exposed port. Regression test for
    the watchdog restarting a monitor that is working perfectly."""
    import socket

    state = HealthState(stale_threshold=300.0, clock=FakeClock())
    state.record_success()
    server = start_health_server(state, port=0)
    port = server.server_address[1]

    stalled = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        stalled.connect(("127.0.0.1", port))
        # Deliberately send nothing and leave the connection open.

        response = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=5)
        assert response.status_code == 200
    finally:
        stalled.close()
        server.shutdown()
        server.server_close()
