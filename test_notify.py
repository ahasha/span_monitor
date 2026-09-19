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
