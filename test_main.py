import pytest
from unittest.mock import Mock, patch
from datetime import datetime, UTC
import requests
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
        insert_data(mock_span_data, now, mock_supabase)
        mock_logger_error.assert_any_call(
            "Error inserting data: {'message': 'Test error', 'code': 402, 'hint': 'Blah', 'details': 'blah'}"
        )


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