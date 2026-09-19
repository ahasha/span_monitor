import requests
import dotenv
import os
from datetime import datetime
from datetime import UTC
import time
import logging
from supabase import create_client, Client
from postgrest import APIError
import httpx
from health import HealthState, start_health_server
from notify import Throttle, ping_healthcheck, publish_ha_sensor

dotenv.load_dotenv()

# Log to stdout always; add a file only when asked. Supervisor captures and
# rotates stdout, and writing a line every few seconds to a Raspberry Pi's
# SD card is needless wear. run.sh sets SPAN_LOG_FILE for the macOS path.
_handlers = [logging.StreamHandler()]
if _log_file := os.getenv("SPAN_LOG_FILE"):
    _handlers.append(logging.FileHandler(_log_file))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=_handlers,
)

logger = logging.getLogger(__name__)


def retry_on_connection_error(max_backoff_seconds=60, backoff_in_seconds=5):
    # httpx.TransportError is the common ancestor of WriteError (broken pipe),
    # ReadError, ConnectError, the timeout errors, and RemoteProtocolError, so it
    # covers every transient transport failure raised by the Supabase httpx client.
    connection_errors = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        httpx.TransportError,
    )
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except connection_errors as e:
                    retries += 1
                    wait_time = min(retries * backoff_in_seconds, max_backoff_seconds)
                    logger.error(f"Attempt {retries} failed: {str(e)}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        return wrapper
    return decorator


@retry_on_connection_error()
def get_span_response(url: str, headers: dict):
    response = requests.get(url, headers=headers, timeout=10)
    return response


@retry_on_connection_error()
def insert_data(data: dict, now: str, supabase: Client) -> bool:
    """
    Insert one tick of panel data into 'main_energy' and 'branch_energy'.

    Returns True only if both inserts succeeded. A partial write counts as
    a failure: the health, heartbeat, and status layers all key off this
    value, so reporting success when rows did not land would recreate the
    silent failure this return value exists to expose.
    """
    main_ok = False
    logger.debug("Inserting data to main_energy")
    try:
        (
            supabase.table("main_energy")
            .insert(
                {
                    "time": datetime.utcnow().isoformat(),
                    "relay_state": data['mainRelayState'],
                    "main_meter_produced_energy_wh": data['mainMeterEnergy']['producedEnergyWh'],
                    "main_meter_consumed_energy_wh": data['mainMeterEnergy']['consumedEnergyWh'],
                    "instant_grid_power_w": data['instantGridPowerW'],
                    "feed_through_power_w": data['feedthroughPowerW'],
                    "feed_through_produced_energy_wh": data['feedthroughEnergy']['producedEnergyWh'],
                    "feed_through_consumed_energy_wh": data['feedthroughEnergy']['consumedEnergyWh'],
                    "grid_sample_start_ms": data['gridSampleStartMs'],
                    "grid_sample_end_ms": data['gridSampleEndMs'],
                    "dsm_grid_state": data['dsmGridState'],
                    "dsm_state": data['dsmState'],
                    "current_run_config": data['currentRunConfig']
                })
            .execute()
        )
        main_ok = True
    except APIError as e:
        logger.error(f"Error inserting data: {e}")
        logger.error(data)

    insert_records = [
        {
            "time": now,
            "branch_id": branch['id'],
            "relay_state": branch['relayState'],
            "instant_power_w": branch['instantPowerW'],
            "imported_active_energy_wh": branch['importedActiveEnergyWh'],
            "exported_active_energy_wh": branch['exportedActiveEnergyWh'],
            "measure_start_ts_ms": branch['measureStartTsMs'],
            "measure_duration_ms": branch['measureDurationMs'],
            "is_measure_valid": branch['isMeasureValid']
        }
        for branch in data['branches']
    ]
    branch_ok = False
    try:
        (
            supabase.table("branch_energy")
            .insert(insert_records)
            .execute()
        )
        branch_ok = True
    except APIError as e:
        logger.error(f"Error inserting data: {e}")
        logger.error(data)

    return main_ok and branch_ok


def poll_once(url, headers, supabase, state, heartbeat, sensor, healthcheck_url) -> bool:
    """Run one poll/insert tick. Returns True if data reached the database.

    A tick counts as successful only when SPAN answered 200 AND both inserts
    landed. Anything less is a failure, because a tick that logs OK while the
    database stays empty is the exact failure this service exists to catch.

    Never raises: the service must outlive any single error.
    """
    success = False
    try:
        response = get_span_response(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            now = datetime.now(UTC).isoformat()
            if insert_data(data, now, supabase):
                logger.info(f"OK {response.status_code}: {data['instantGridPowerW']} W")
                success = True
            else:
                logger.error("Insert failed: data did not reach the database")
                state.record_failure("insert failed")
        else:
            logger.error(f"BAD {response.status_code}: {response.text}")
            state.record_failure(f"SPAN returned {response.status_code}")
    except Exception as e:
        # Last-resort safety net: never let an unexpected error kill the
        # service. Log it and keep polling; the next tick will retry.
        # (KeyboardInterrupt is a BaseException, so it still exits.)
        logger.exception("Unexpected error in poll loop, continuing...")
        state.record_failure(str(e))

    # Post-tick notification work (heartbeat ping, status sensor) must never
    # be able to flip the verdict this function already earned. notify.py
    # self-guards today, but the docstring promises "never raises"
    # unconditionally, and under Supervisor an escape here means a
    # container restart; on the macOS fallback path there is no watchdog at
    # all, so the process would die silently.
    try:
        if success:
            state.record_success()
            if heartbeat.ready():
                ping_healthcheck(healthcheck_url)

        if sensor.ready():
            publish_ha_sensor(state)
    except Exception:
        logger.exception("Post-tick notification failed")

    return success


if __name__ == "__main__":
    span_bearer_token = os.getenv("SPAN_API_KEY")
    span_ip = os.getenv("SPAN_IP")

    url = f"http://{span_ip}/api/v1/panel"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {span_bearer_token}"
    }
    # TimescaleDB in supabase connection details
    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)

    poll_interval = float(os.getenv("POLL_INTERVAL", "5"))
    stale_threshold = float(os.getenv("STALE_THRESHOLD", "300"))
    health_port = int(os.getenv("HEALTH_PORT", "8099"))
    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL", "60"))
    sensor_interval = float(os.getenv("SENSOR_INTERVAL", "30"))
    healthcheck_url = os.getenv("HEALTHCHECK_URL")

    state = HealthState(stale_threshold=stale_threshold)
    start_health_server(state, health_port)
    heartbeat = Throttle(heartbeat_interval)
    sensor = Throttle(sensor_interval)

    try:
        while True:
            poll_once(url, headers, supabase, state, heartbeat, sensor, healthcheck_url)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.error("Interrupt received, exiting gracefully...")
    finally:
        logger.error("Connection closed")


