import pytest
from unittest.mock import Mock, patch
from datetime import datetime, UTC
import requests
import httpx
from postgrest import APIError
import main
from main import (
    retry_on_connection_error,
    get_span_response,
    insert_data
)

# Sample test data
@pytest.fixture
def mock_span_data():
    return {
        'mainRelayState': True,
        'mainMeterEnergy': {
            'producedEnergyWh': 1000,
            'consumedEnergyWh': 2000
        },
        'instantGridPowerW': 500,
        'feedthroughPowerW': 300,
        'feedthroughEnergy': {
            'producedEnergyWh': 800,
            'consumedEnergyWh': 1200
        },
        'gridSampleStartMs': 1000,
        'gridSampleEndMs': 2000,
        'dsmGridState': 'NORMAL',
        'dsmState': 'ACTIVE',
        'currentRunConfig': 'DEFAULT',
        'branches': [
            {
                'id': 1,
                'relayState': True,
                'instantPowerW': 100,
                'importedActiveEnergyWh': 500,
                'exportedActiveEnergyWh': 200,
                'measureStartTsMs': 1000,
                'measureDurationMs': 1000,
                'isMeasureValid': True
            }
        ]
    }

def test_retry_decorator(mocker):
    mock_func = Mock(side_effect=[
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ConnectTimeout,
        "success"
    ])
    mocker.patch('time.sleep')

    decorated_func = retry_on_connection_error(backoff_in_seconds=0)(mock_func)
    result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 3

def test_retry_decorator_retries_indefinitely(mocker):
    failures = [requests.exceptions.ConnectTimeout] * 20
    mock_func = Mock(side_effect=failures + ["success"])
    mock_sleep = mocker.patch('time.sleep')

    decorated_func = retry_on_connection_error(
        max_backoff_seconds=60, backoff_in_seconds=5
    )(mock_func)
    result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 21
    # Backoff grows linearly then caps at max_backoff_seconds
    waits = [call.args[0] for call in mock_sleep.call_args_list]
    assert waits[:3] == [5, 10, 15]
    assert waits[-1] == 60

@pytest.mark.parametrize("exc", [
    httpx.WriteError("broken pipe"),
    httpx.ReadError("connection reset"),
    httpx.ConnectError("connection refused"),
    httpx.RemoteProtocolError("server disconnected"),
])
def test_retry_decorator_recovers_from_httpx_transport_errors(mocker, exc):
    mock_func = Mock(side_effect=[exc, "success"])
    mocker.patch('time.sleep')

    decorated_func = retry_on_connection_error(backoff_in_seconds=0)(mock_func)
    result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 2


def test_get_span_response():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "ok"}

    with patch('requests.get', return_value=mock_response):
        response = get_span_response("http://test-url", headers={})

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_insert_data_success(mock_span_data):
    mock_supabase = Mock()
    mock_supabase.table.return_value.insert.return_value.execute.return_value = None

    # Test successful insertion
    now = datetime.now(UTC).isoformat()
    insert_data(mock_span_data, now, mock_supabase)

    # Verify main_energy table insert was called
    mock_supabase.table.assert_any_call("main_energy")
    # Verify branch_energy table insert was called
    mock_supabase.table.assert_any_call("branch_energy")


def test_insert_data_api_error(mock_span_data):
    mock_supabase = Mock()
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = APIError(
        {
            "message": "Test error",
            "code": 402,
            "hint": "Blah",
            "details": "blah",
        }
    )

    # Test handling of API error
    now = datetime.now(UTC).isoformat()
    with patch('main.logger.error') as mock_logger_error:
        assert insert_data(mock_span_data, now, mock_supabase) is False
        mock_logger_error.assert_any_call(
            "Error inserting data: {'message': 'Test error', 'code': 402, 'hint': 'Blah', 'details': 'blah'}"
        )


def test_insert_data_returns_true_on_success(mock_span_data):
    mock_supabase = Mock()
    mock_supabase.table.return_value.insert.return_value.execute.return_value = None

    now = datetime.now(UTC).isoformat()
    assert insert_data(mock_span_data, now, mock_supabase) is True


def test_insert_data_returns_false_when_branch_insert_fails(mock_span_data):
    """Main succeeds, branches fail: a partial write is still a failed tick."""
    mock_supabase = Mock()
    error = APIError({"message": "boom", "code": 500, "hint": "", "details": ""})
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = [
        None,    # main_energy succeeds
        error,   # branch_energy fails
    ]

    now = datetime.now(UTC).isoformat()
    assert insert_data(mock_span_data, now, mock_supabase) is False


def test_insert_data_returns_false_when_main_insert_fails(mock_span_data):
    mock_supabase = Mock()
    error = APIError({"message": "boom", "code": 500, "hint": "", "details": ""})
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = [
        error,   # main_energy fails
        None,    # branch_energy succeeds
    ]

    now = datetime.now(UTC).isoformat()
    assert insert_data(mock_span_data, now, mock_supabase) is False


def test_get_span_response_connection_error(mocker):
    mocker.patch('time.sleep')
    mock_response = Mock()
    mock_response.status_code = 200
    with patch('requests.get', side_effect=[
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ConnectionError,
        mock_response,
    ]):
        response = get_span_response("http://test-url", headers={})

    assert response.status_code == 200

from health import HealthState
from notify import Throttle
from main import poll_once


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _always_ready():
    return Throttle(0.0, clock=FakeClock())


def test_poll_once_records_success_and_pings(mocker, mock_span_data):
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=True)
    mock_ping = mocker.patch("main.ping_healthcheck", return_value=True)
    mocker.patch("main.publish_ha_sensor", return_value=True)

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc") is True

    assert state.is_healthy() is True
    mock_ping.assert_called_once_with("https://hc-ping.com/abc")


def test_poll_once_failed_insert_records_failure_and_does_not_ping(mocker, mock_span_data):
    """The silent-failure case: SPAN answers 200 but nothing reaches the DB."""
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=False)
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc") is False

    assert state.is_healthy() is False
    assert state.snapshot()["consecutive_errors"] == 1
    mock_ping.assert_not_called()


def test_poll_once_non_200_records_failure(mocker):
    mocker.patch("main.get_span_response", return_value=Mock(status_code=503, text="unavailable"))
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), None) is False

    assert state.snapshot()["consecutive_errors"] == 1
    mock_ping.assert_not_called()


def test_poll_once_survives_unexpected_exceptions(mocker):
    """Nothing may escape a tick - the service must outlive any single error."""
    mocker.patch("main.get_span_response", side_effect=ValueError("surprise"))
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), None) is False
    assert state.snapshot()["consecutive_errors"] == 1


def test_poll_once_survives_notification_failure(mocker, mock_span_data):
    """Notification/publishing work happens after the tick already
    succeeded; a failure there must not escape poll_once or flip the
    verdict it already earned."""
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=True)
    mocker.patch("main.ping_healthcheck", return_value=True)
    mocker.patch("main.publish_ha_sensor", side_effect=RuntimeError("boom"))

    state = HealthState(clock=FakeClock())
    result = poll_once("http://x", {}, Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc")

    assert result is True
    assert state.is_healthy() is True


def test_poll_once_respects_heartbeat_throttle(mocker, mock_span_data):
    mocker.patch("main.get_span_response", return_value=Mock(status_code=200, json=Mock(return_value=mock_span_data)))
    mocker.patch("main.insert_data", return_value=True)
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    clock = FakeClock()
    heartbeat = Throttle(60.0, clock=clock)
    state = HealthState(clock=clock)

    for _ in range(3):
        poll_once("http://x", {}, Mock(), state, heartbeat, _always_ready(), "https://hc-ping.com/abc")

    assert mock_ping.call_count == 1
