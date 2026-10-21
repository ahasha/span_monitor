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
import sys
from pathlib import Path
from extract import SnapshotIncomplete, snapshot_to_rows
from homie import HomieSnapshot
from span_auth import SpanAuthError, invalidate, load_or_register
from span_mqtt import SpanMqtt
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
def insert_data(main_row: dict, branch_rows: list[dict], supabase: Client) -> bool:
    """
    Insert one tick of panel data into 'main_energy' and 'branch_energy'.

    Returns True only if both inserts succeeded. A partial write counts as
    a failure: the health, heartbeat, and status layers all key off this
    value, so reporting success when rows did not land would recreate the
    silent failure this return value exists to expose.
    """
    main_ok = False
    try:
        supabase.table("main_energy").insert(main_row).execute()
        main_ok = True
    except APIError as e:
        logger.error(f"Error inserting main_energy: {e}")
        logger.error(main_row)

    branch_ok = False
    try:
        supabase.table("branch_energy").insert(branch_rows).execute()
        branch_ok = True
    except APIError as e:
        logger.error(f"Error inserting branch_energy: {e}")
        logger.error(branch_rows)

    return main_ok and branch_ok


def get_newest_row_time(supabase: Client) -> datetime | None:
    """Return the newest timestamp already in main_energy, or None if unknown.

    Fails OPEN: any error returns None, which disables the clock guard. No
    data at all is a worse outcome than data with a slightly suspect
    timestamp, so an unreachable database must not stop the monitor.
    """
    try:
        response = (
            supabase.table("main_energy")
            .select("time")
            .order("time", desc=True)
            .limit(1)
            .execute()
        )
        rows = response.data
        if not rows:
            return None
        parsed = datetime.fromisoformat(rows[0]["time"])
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed
    except Exception as e:
        logger.warning(f"Could not determine newest stored row time: {e}")
        return None


def poll_once(
    snapshot: HomieSnapshot, supabase, state, heartbeat, sensor, healthcheck_url,
    clock_floor: datetime | None = None, max_silence: float = 120.0, dry_run: bool = False,
) -> bool:
    """Run one poll/insert tick. Returns True if data reached the database.

    A tick counts as successful only when the SPAN snapshot was ready AND
    both inserts landed. Anything less is a failure, because a tick that logs OK while the
    database stays empty is the exact failure this service exists to catch.

    clock_floor, when set, is the newest timestamp already stored in
    main_energy at process startup. If the system clock is behind it, the
    tick is refused rather than inserted - a Raspberry Pi with no
    battery-backed RTC can boot with a clock hours behind real time, and a
    row timestamped in the past would corrupt the permanent hourly
    continuous aggregates. It is captured once at startup and never updated
    from our own writes; see get_newest_row_time and the callers in
    __main__ for why a rolling high-water mark would be worse than the bug
    it fixes.

    Never raises: the service must outlive any single error.
    """
    success = False
    try:
        reason = snapshot.unready_reason(max_silence)
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        if reason is not None:
            logger.error(f"SPAN snapshot not ready: {reason}")
            state.record_failure(f"SPAN snapshot not ready: {reason}")
        elif clock_floor is not None and now_dt < clock_floor:
            logger.error(
                f"System clock ({now}) is behind the newest stored row "
                f"({clock_floor.isoformat()}); refusing to insert until "
                "the clock catches up"
            )
            state.record_failure("clock behind newest stored row")
        else:
            try:
                main_row, branch_rows = snapshot_to_rows(snapshot, now)
            except SnapshotIncomplete as e:
                logger.warning(f"Snapshot incomplete: {e}")
                state.record_failure(f"snapshot incomplete: {e}")
            else:
                if dry_run:
                    logger.info(
                        f"DRY RUN main={main_row} branches={len(branch_rows)} "
                        f"sum_branch_w={sum(b['instant_power_w'] for b in branch_rows):.1f}"
                    )
                    success = True
                elif insert_data(main_row, branch_rows, supabase):
                    logger.info(f"OK: {main_row['instant_grid_power_w']} W")
                    success = True
                else:
                    logger.error("Insert failed: data did not reach the database")
                    state.record_failure("insert failed")
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
            # A dry run writes nothing; pinging would tell the dead-man
            # switch that data is landing when it is not.
            if not dry_run and heartbeat.ready():
                ping_healthcheck(healthcheck_url)

        if sensor.ready():
            publish_ha_sensor(state)
    except Exception:
        logger.exception("Post-tick notification failed")

    return success


if __name__ == "__main__":
    span_ip = os.environ["SPAN_IP"]
    span_passphrase = os.environ["SPAN_PASSPHRASE"]
    auth_cache = Path(os.getenv("SPAN_AUTH_CACHE", "~/.span_monitor_auth.json")).expanduser()
    max_silence = float(os.getenv("MQTT_MAX_SILENCE", "120"))
    dry_run = os.getenv("SPAN_DRY_RUN", "") == "1"
    if dry_run:
        logger.warning("SPAN_DRY_RUN=1: rows are logged, not inserted")

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

    # Captured once, never updated from our own writes - see get_newest_row_time
    # and poll_once's docstring for why a rolling high-water mark would be worse
    # than the clock-skew bug it guards against.
    clock_floor = get_newest_row_time(supabase)
    if clock_floor is not None:
        logger.info(f"Clock guard armed: refusing writes timestamped before {clock_floor.isoformat()}")
    else:
        logger.info("Clock guard disabled: no newest-row timestamp available at startup")

    # After a power cut the Pi can boot before the panel answers, so network
    # errors are retried (with the same backoff as every other SPAN call).
    # A rejected passphrase is fatal: retrying cannot fix it, and a
    # crash-looping container is visible where a silently idle one is not.
    try:
        creds = retry_on_connection_error()(load_or_register)(span_ip, span_passphrase, auth_cache)
    except SpanAuthError as e:
        logger.error(f"SPAN API authentication failed: {e}")
        sys.exit(1)

    def on_auth_failure():
        if invalidate(auth_cache, min_age_seconds=600):
            logger.error("Broker rejected cached credentials; cleared the cache, exiting to re-register")
        else:
            logger.error("Broker rejected credentials registered under 10 minutes ago; "
                         "keeping them and exiting to retry rather than registering again")
        os._exit(1)  # runs on paho's thread, where sys.exit would only end that thread

    snapshot = HomieSnapshot(creds.serial_number)
    mqtt_client = SpanMqtt(span_ip, creds, snapshot, on_auth_failure)
    mqtt_client.start()

    try:
        while True:
            poll_once(
                snapshot, supabase, state, heartbeat, sensor, healthcheck_url,
                clock_floor, max_silence, dry_run,
            )
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.error("Interrupt received, exiting gracefully...")
    finally:
        mqtt_client.stop()
        logger.error("Connection closed")
