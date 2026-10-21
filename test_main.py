import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, UTC
import requests
import httpx
from postgrest import APIError
import main
from main import (
    retry_on_connection_error,
    insert_data,
    get_newest_row_time,
)
from test_extract import build

MAIN_ROW = {"time": "t", "relay_state": "CLOSED", "instant_grid_power_w": 500.0}
BRANCH_ROWS = [{"time": "t", "branch_id": 5, "instant_power_w": -100.0}]


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



def test_insert_data_success():
    mock_supabase = Mock()
    mock_supabase.table.return_value.insert.return_value.execute.return_value = None

    # Test successful insertion
    insert_data(MAIN_ROW, BRANCH_ROWS, mock_supabase)

    # Verify main_energy table insert was called
    mock_supabase.table.assert_any_call("main_energy")
    # Verify branch_energy table insert was called
    mock_supabase.table.assert_any_call("branch_energy")


def test_insert_data_api_error():
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
    with patch('main.logger.error') as mock_logger_error:
        assert insert_data(MAIN_ROW, BRANCH_ROWS, mock_supabase) is False
        mock_logger_error.assert_any_call(
            "Error inserting main_energy: {'message': 'Test error', 'code': 402, 'hint': 'Blah', 'details': 'blah'}"
        )


def test_insert_data_returns_true_on_success():
    mock_supabase = Mock()
    mock_supabase.table.return_value.insert.return_value.execute.return_value = None

    assert insert_data(MAIN_ROW, BRANCH_ROWS, mock_supabase) is True


def test_insert_data_returns_false_when_branch_insert_fails():
    """Main succeeds, branches fail: a partial write is still a failed tick."""
    mock_supabase = Mock()
    error = APIError({"message": "boom", "code": 500, "hint": "", "details": ""})
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = [
        None,    # main_energy succeeds
        error,   # branch_energy fails
    ]

    assert insert_data(MAIN_ROW, BRANCH_ROWS, mock_supabase) is False


def test_insert_data_returns_false_when_main_insert_fails():
    mock_supabase = Mock()
    error = APIError({"message": "boom", "code": 500, "hint": "", "details": ""})
    mock_supabase.table.return_value.insert.return_value.execute.side_effect = [
        error,   # main_energy fails
        None,    # branch_energy succeeds
    ]

    assert insert_data(MAIN_ROW, BRANCH_ROWS, mock_supabase) is False



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


def test_poll_once_records_success_and_pings(mocker):
    mocker.patch("main.insert_data", return_value=True)
    mock_ping = mocker.patch("main.ping_healthcheck", return_value=True)
    mocker.patch("main.publish_ha_sensor", return_value=True)

    state = HealthState(clock=FakeClock())
    assert poll_once(build(), Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc") is True

    assert state.is_healthy() is True
    mock_ping.assert_called_once_with("https://hc-ping.com/abc")


def test_poll_once_failed_insert_records_failure_and_does_not_ping(mocker):
    """The silent-failure case: SPAN answers 200 but nothing reaches the DB."""
    mocker.patch("main.insert_data", return_value=False)
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once(build(), Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc") is False

    assert state.is_healthy() is False
    assert state.snapshot()["consecutive_errors"] == 1
    mock_ping.assert_not_called()


def test_poll_once_unready_snapshot_records_failure(mocker):
    """The panel or broker is unreachable: nothing is inserted, nothing pinged."""
    mock_insert = mocker.patch("main.insert_data")
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")
    snap = build()
    snap.set_connected(False)

    state = HealthState(clock=FakeClock())
    assert poll_once(snap, Mock(), state, _always_ready(), _always_ready(), None) is False

    assert state.snapshot()["consecutive_errors"] == 1
    assert state.snapshot()["last_error"] == "SPAN snapshot not ready: not connected"
    mock_insert.assert_not_called()
    mock_ping.assert_not_called()


def test_poll_once_survives_unexpected_exceptions(mocker):
    """Nothing may escape a tick - the service must outlive any single error."""
    mocker.patch("main.snapshot_to_rows", side_effect=ValueError("surprise"))
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once(build(), Mock(), state, _always_ready(), _always_ready(), None) is False
    assert state.snapshot()["consecutive_errors"] == 1


def test_poll_once_survives_notification_failure(mocker):
    """Notification/publishing work happens after the tick already
    succeeded; a failure there must not escape poll_once or flip the
    verdict it already earned."""
    mocker.patch("main.insert_data", return_value=True)
    mocker.patch("main.ping_healthcheck", return_value=True)
    mocker.patch("main.publish_ha_sensor", side_effect=RuntimeError("boom"))

    state = HealthState(clock=FakeClock())
    result = poll_once(build(), Mock(), state, _always_ready(), _always_ready(), "https://hc-ping.com/abc")

    assert result is True
    assert state.is_healthy() is True


def _mock_newest_row_query(mock_supabase, rows):
    (
        mock_supabase.table.return_value.select.return_value.order.return_value
        .limit.return_value.execute.return_value
    ) = Mock(data=rows)


def test_get_newest_row_time_returns_parsed_datetime():
    mock_supabase = Mock()
    _mock_newest_row_query(mock_supabase, [{"time": "2026-01-01T12:30:00+00:00"}])

    result = get_newest_row_time(mock_supabase)

    assert result == datetime(2026, 1, 1, 12, 30, 0, tzinfo=UTC)
    mock_supabase.table.assert_called_once_with("main_energy")
    mock_supabase.table.return_value.select.return_value.order.assert_called_once_with(
        "time", desc=True
    )
    mock_supabase.table.return_value.select.return_value.order.return_value.limit.assert_called_once_with(1)


def test_get_newest_row_time_fails_open_on_query_error():
    """Fails OPEN: an unreachable database must not stop the monitor."""
    mock_supabase = Mock()
    (
        mock_supabase.table.return_value.select.return_value.order.return_value
        .limit.return_value.execute.side_effect
    ) = RuntimeError("connection refused")

    assert get_newest_row_time(mock_supabase) is None


def test_get_newest_row_time_returns_none_when_table_empty():
    mock_supabase = Mock()
    _mock_newest_row_query(mock_supabase, [])

    assert get_newest_row_time(mock_supabase) is None


def test_poll_once_clock_behind_floor_skips_insert_and_fails(mocker):
    mock_insert = mocker.patch("main.insert_data")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    clock_floor = datetime.now(UTC) + timedelta(hours=3)

    result = poll_once(
        build(), Mock(), state, _always_ready(), _always_ready(), None,
        clock_floor=clock_floor,
    )

    assert result is False
    mock_insert.assert_not_called()
    assert state.snapshot()["consecutive_errors"] == 1
    assert state.snapshot()["last_error"] == "clock behind newest stored row"


def test_poll_once_clock_after_floor_inserts_normally(mocker):
    mock_insert = mocker.patch("main.insert_data", return_value=True)
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    clock_floor = datetime.now(UTC) - timedelta(hours=3)

    result = poll_once(
        build(), Mock(), state, _always_ready(), _always_ready(), None,
        clock_floor=clock_floor,
    )

    assert result is True
    mock_insert.assert_called_once()


def test_poll_once_clock_floor_none_disables_guard(mocker):
    mock_insert = mocker.patch("main.insert_data", return_value=True)
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())

    result = poll_once(
        build(), Mock(), state, _always_ready(), _always_ready(), None,
        clock_floor=None,
    )

    assert result is True
    mock_insert.assert_called_once()


def test_poll_once_respects_heartbeat_throttle(mocker):
    mocker.patch("main.insert_data", return_value=True)
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    clock = FakeClock()
    heartbeat = Throttle(60.0, clock=clock)
    state = HealthState(clock=clock)

    for _ in range(3):
        poll_once(build(), Mock(), state, heartbeat, _always_ready(), "https://hc-ping.com/abc")

    assert mock_ping.call_count == 1


def test_poll_once_inserts_snapshot_rows_with_one_timestamp(mocker):
    mock_insert = mocker.patch("main.insert_data", return_value=True)
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once(build(), Mock(), state, _always_ready(), _always_ready(), None) is True

    main_row, branch_rows, _ = mock_insert.call_args.args
    assert main_row["instant_grid_power_w"] == 850.5
    assert [b["branch_id"] for b in branch_rows] == [5]
    assert main_row["time"] == branch_rows[0]["time"]


def test_poll_once_incomplete_snapshot_records_failure(mocker):
    """Startup: the retained tree has not fully arrived. No partial row."""
    mock_insert = mocker.patch("main.insert_data")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    assert poll_once(build(circuits={}), Mock(), state, _always_ready(), _always_ready(), None) is False

    mock_insert.assert_not_called()
    assert "circuit" in state.snapshot()["last_error"]


def test_poll_once_dry_run_logs_and_does_not_insert(mocker, caplog):
    mock_insert = mocker.patch("main.insert_data")
    mocker.patch("main.publish_ha_sensor")
    caplog.set_level("INFO")

    state = HealthState(clock=FakeClock())
    assert poll_once(build(), Mock(), state, _always_ready(), _always_ready(), None, dry_run=True) is True

    mock_insert.assert_not_called()
    assert "DRY RUN" in caplog.text


def test_insert_data_inserts_prebuilt_rows():
    mock_supabase = Mock()
    assert insert_data(MAIN_ROW, BRANCH_ROWS, mock_supabase) is True
    mock_supabase.table.return_value.insert.assert_any_call(MAIN_ROW)
    mock_supabase.table.return_value.insert.assert_any_call(BRANCH_ROWS)


def test_poll_once_dry_run_does_not_ping_heartbeat(mocker):
    """A dry run writes nothing, so it must not tell the dead-man switch
    that data is landing."""
    mocker.patch("main.insert_data")
    mock_ping = mocker.patch("main.ping_healthcheck")
    mocker.patch("main.publish_ha_sensor")

    state = HealthState(clock=FakeClock())
    poll_once(build(), Mock(), state, _always_ready(), _always_ready(),
              "https://hc-ping.com/abc", dry_run=True)

    mock_ping.assert_not_called()
