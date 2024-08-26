import requests
import dotenv
import os
import psycopg2
from datetime import datetime
import time
import logging
import os
from supabase import create_client, Client
from postgrest import APIError

# loglevel info, log to a file
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("span.log"),
        logging.StreamHandler(),
    ])

logger = logging.getLogger(__name__)
dotenv.load_dotenv()


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
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                now = datetime.utcnow().isoformat()
                logger.info(f"OK {response.status_code}: {data['instantGridPowerW']} W")
                insert_data(data, now, supabase)
            else:
                logger.error(f"BAD {response.status_code}: {response.text}")

            time.sleep(1)  # Poll every second
    except KeyboardInterrupt:
        logger.error("Interrupt received, exiting gracefully...")
    finally:
        logger.error("Connection closed")


