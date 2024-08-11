import requests
import dotenv
import os
import psycopg2
from datetime import datetime
import time
import logging

# loglevel info, log to a file
logging.basicConfig(filename='span.log', level=logging.INFO)
logger = logging.getLogger(__name__)

dotenv.load_dotenv()


def insert_data(data, conn):
    cursor = conn.cursor()
    main_energy_insert = """
        INSERT INTO main_energy (
            time, relay_state, main_meter_produced_energy_wh,
            main_meter_consumed_energy_wh, instant_grid_power_w,
            feed_through_power_w,
            feed_through_produced_energy_wh, feed_through_consumed_energy_wh,
            grid_sample_start_ms, grid_sample_end_ms, dsm_grid_state,
            dsm_state, current_run_config
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    cursor.execute(main_energy_insert,
                   (
                       datetime.now(),
                       data['mainRelayState'],
                       data['mainMeterEnergy']['producedEnergyWh'],
                       data['mainMeterEnergy']['consumedEnergyWh'],
                       data['instantGridPowerW'],
                       data['feedthroughPowerW'],
                       data['feedthroughEnergy']['producedEnergyWh'],
                       data['feedthroughEnergy']['consumedEnergyWh'],
                       data['gridSampleStartMs'],
                       data['gridSampleEndMs'],
                       data['dsmGridState'],
                       data['dsmState'],
                       data['currentRunConfig']
                   ))

    for branch in data['branches']:
        cursor.execute("""
            INSERT INTO branch_energy (
                time, branch_id, relay_state, instant_power_w,
                imported_active_energy_wh, exported_active_energy_wh,
                measure_start_ts_ms, measure_duration_ms, is_measure_valid
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            datetime.now(),
            branch['id'],
            branch['relayState'],
            branch['instantPowerW'],
            branch['importedActiveEnergyWh'],
            branch['exportedActiveEnergyWh'],
            branch['measureStartTsMs'],
            branch['measureDurationMs'],
            branch['isMeasureValid']
        ))
    conn.commit()


if __name__ == "__main__":
    span_bearer_token = os.getenv("SPAN_API_KEY")
    span_ip = os.getenv("SPAN_IP")

    url = f"http://{span_ip}/api/v1/panel"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {span_bearer_token}"
    }
    # TimescaleDB connection details
    conn = psycopg2.connect(
        dbname="energy",
        user="postgres",  # or your PostgreSQL username
        password="abc123",
        host="localhost",
        port="5432"
    )

    try:
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                insert_data(data, conn)
            else:
                conn.close()
                raise requests.exceptions.HTTPError(response.text)

            logger.info(f"OK {response.status_code}: {data['instantGridPowerW']} W")
            time.sleep(1)  # Poll every second
    except KeyboardInterrupt:
        logger.error("Interrupt received, exiting gracefully...")
    finally:
        conn.close()
        logger.error("Connection closed")


