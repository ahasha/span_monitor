from notify import Throttle


class FakeClock:
    """Manually advanced clock so tests never sleep."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_throttle_allows_first_call():
    assert Throttle(60.0, clock=FakeClock()).ready() is True


def test_throttle_blocks_second_immediate_call():
    throttle = Throttle(60.0, clock=FakeClock())
    assert throttle.ready() is True
    assert throttle.ready() is False


def test_throttle_allows_again_after_interval():
    clock = FakeClock()
    throttle = Throttle(60.0, clock=clock)
    assert throttle.ready() is True

    clock.advance(59.0)
    assert throttle.ready() is False

    clock.advance(1.0)
    assert throttle.ready() is True


def test_throttle_zero_interval_always_ready():
    """interval=0 disables throttling, for tests and debugging."""
    throttle = Throttle(0.0, clock=FakeClock())
    assert throttle.ready() is True
    assert throttle.ready() is True


from unittest.mock import Mock

import requests

from notify import ping_healthcheck


def test_ping_healthcheck_gets_the_url(mocker):
    mock_get = mocker.patch("notify.requests.get")
    mock_get.return_value = Mock(status_code=200)

    assert ping_healthcheck("https://hc-ping.com/abc") is True
    mock_get.assert_called_once_with("https://hc-ping.com/abc", timeout=5.0)


def test_ping_healthcheck_noop_without_url():
    """No configured URL means no heartbeat - the macOS path stays unchanged."""
    assert ping_healthcheck(None) is False
    assert ping_healthcheck("") is False


def test_ping_healthcheck_swallows_connection_errors(mocker):
    mocker.patch(
        "notify.requests.get",
        side_effect=requests.exceptions.ConnectionError("no route to host"),
    )
    assert ping_healthcheck("https://hc-ping.com/abc") is False


def test_ping_healthcheck_swallows_timeouts(mocker):
    mocker.patch(
        "notify.requests.get",
        side_effect=requests.exceptions.Timeout("too slow"),
    )
    assert ping_healthcheck("https://hc-ping.com/abc") is False


def test_ping_healthcheck_swallows_unexpected_exceptions(mocker):
    """Belt and braces: nothing from this call may reach the poll loop."""
    mocker.patch("notify.requests.get", side_effect=ValueError("surprise"))
    assert ping_healthcheck("https://hc-ping.com/abc") is False


def test_ping_healthcheck_reports_false_on_error_status(mocker):
    mocker.patch("notify.requests.get", return_value=Mock(status_code=404))
    assert ping_healthcheck("https://hc-ping.com/abc") is False


from health import HealthState
from notify import publish_ha_sensor


def test_publish_ha_sensor_noop_without_token(mocker, monkeypatch):
    """On macOS there is no Supervisor, so this must do nothing at all."""
    monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
    mock_post = mocker.patch("notify.requests.post")

    state = HealthState(clock=FakeClock())
    assert publish_ha_sensor(state) is False
    mock_post.assert_not_called()


def test_publish_ha_sensor_posts_state_and_attributes(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mock_post = mocker.patch("notify.requests.post", return_value=Mock(status_code=200))

    clock = FakeClock()
    state = HealthState(stale_threshold=300.0, clock=clock)
    state.record_success()
    clock.advance(12.0)

    assert publish_ha_sensor(state) is True

    args, kwargs = mock_post.call_args
    assert args[0] == "http://supervisor/core/api/states/sensor.span_monitor"
    assert kwargs["headers"]["Authorization"] == "Bearer tok123"
    assert kwargs["timeout"] == 5.0
    assert kwargs["json"]["state"] == 12.0
    assert kwargs["json"]["attributes"]["consecutive_errors"] == 0
    assert kwargs["json"]["attributes"]["healthy"] is True


def test_publish_ha_sensor_reports_unknown_before_first_success(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mock_post = mocker.patch("notify.requests.post", return_value=Mock(status_code=200))

    publish_ha_sensor(HealthState(clock=FakeClock()))

    assert mock_post.call_args.kwargs["json"]["state"] == "unknown"


def test_publish_ha_sensor_swallows_errors(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mocker.patch(
        "notify.requests.post",
        side_effect=requests.exceptions.ConnectionError("supervisor unreachable"),
    )

    state = HealthState(clock=FakeClock())
    state.record_success()
    assert publish_ha_sensor(state) is False


def test_publish_ha_sensor_swallows_unexpected_exceptions(mocker, monkeypatch):
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mocker.patch("notify.requests.post", side_effect=ValueError("surprise"))

    state = HealthState(clock=FakeClock())
    state.record_success()
    assert publish_ha_sensor(state) is False


def test_publish_ha_sensor_swallows_state_errors(mocker, monkeypatch):
    """The contract is that NOTHING escapes - including a failure in the
    state object itself, not just in the network call."""
    monkeypatch.setenv("SUPERVISOR_TOKEN", "tok123")
    mock_post = mocker.patch("notify.requests.post")
    broken_state = Mock()
    broken_state.snapshot.side_effect = RuntimeError("lock timeout")

    assert publish_ha_sensor(broken_state) is False
    mock_post.assert_not_called()
