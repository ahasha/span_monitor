import requests
import dotenv
import os
import psycopg2
from datetime import datetime
from datetime import UTC
import time
import logging
import os
from supabase import create_client, Client
from postgrest import APIError
import httpx
import time

# loglevel info, log to a file
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("span.log"),
        logging.StreamHandler(),
    ])

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

def retry_on_connection_error(max_retries=3, backoff_in_seconds=5):
    connection_errors = (requests.exceptions.ConnectTimeout, httpx.RemoteProtocolError)
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except connection_errors as e:
                    retries += 1
                    wait_time = retries * backoff_in_seconds
                    logger.error(f"Attempt {retries}/{max_retries} failed: {str(e)}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
            raise RuntimeError(f"Function failed after {max_retries} attempts. Last error: {str(e)}")
        return wrapper
    return decorator


@retry_on_connection_error()
def get_span_response(url: str, headers: dict):
    response = requests.get(url, headers=headers)
    return response


@retry_on_connection_error()
def insert_data(data: dict, now: str, supabase: Client):
    """
    Inserts data into the 'main_energy' and 'branch_energy' tables in Supabase.

    Args:
        data (dict): A dictionary containing the data to be inserted.
        supabase (Client): An instance of the Supabase client.

    Returns:
        None
    """

    logger.debug("Inserting data to main_energy")
    try:
        response = (
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
    try:
        response = (
            supabase.table("branch_energy")
            .insert(insert_records)
            .execute()
        )
    except APIError as e:
        logger.error(f"Error inserting data: {e}")
        logger.error(data)


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

    try:
        while True:
            response = get_span_response(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                now = datetime.now(UTC).isoformat()
                logger.info(f"OK {response.status_code}: {data['instantGridPowerW']} W")
                insert_data(data, now, supabase)
            else:
                logger.error(f"BAD {response.status_code}: {response.text}")

            time.sleep(1)  # Poll every second
    except KeyboardInterrupt:
        logger.error("Interrupt received, exiting gracefully...")
    finally:
        logger.error("Connection closed")


