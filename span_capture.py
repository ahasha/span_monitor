"""One-shot LAN capture: v1 /panel JSON + MQTT retained dump, taken together.

Used once to confirm the v1 -> MQTT field mapping and to produce real test
fixtures. Not part of the service image.

    uv run python span_capture.py --seconds 20
"""
import json
import os
import ssl
import time
from datetime import UTC, datetime
from pathlib import Path

import click
import dotenv
import paho.mqtt.client as mqtt
import requests

from span_auth import load_or_register

dotenv.load_dotenv()
FIXTURES = Path(__file__).parent / "tests" / "fixtures"


@click.command()
@click.option("--seconds", default=20, show_default=True, help="How long to collect MQTT messages.")
def main(seconds):
    ip = os.environ["SPAN_IP"]
    creds = load_or_register(ip, os.environ["SPAN_PASSPHRASE"],
                             Path(os.getenv("SPAN_AUTH_CACHE", "~/.span_monitor_auth.json")).expanduser())

    messages: dict[str, str] = {}
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"span_capture-{int(time.time())}")
    client.username_pw_set(creds.broker_username, creds.broker_password)
    client.tls_set(ca_certs=str(creds.ca_cert_path), tls_version=ssl.PROTOCOL_TLS_CLIENT)
    client.on_connect = lambda c, u, f, rc, p: (click.echo(f"MQTT connect: {rc}"), c.subscribe("ebus/5/#"))
    client.on_message = lambda c, u, m: messages.__setitem__(m.topic, m.payload.decode("utf-8", "replace"))
    client.connect(ip, creds.mqtt_port)
    client.loop_start()
    time.sleep(seconds)

    # Take the v1 snapshot at the end, when MQTT holds the freshest values.
    v1 = requests.get(f"http://{ip}/api/v1/panel",
                      headers={"Authorization": f"Bearer {os.environ['SPAN_API_KEY']}"}, timeout=10)
    captured_at = datetime.now(UTC).isoformat()
    client.loop_stop()
    client.disconnect()

    FIXTURES.mkdir(parents=True, exist_ok=True)
    (FIXTURES / "span_v1_panel.json").write_text(json.dumps(v1.json(), indent=2))
    (FIXTURES / "span_mqtt_dump.json").write_text(
        json.dumps({"captured_at": captured_at, "messages": messages}, indent=2))
    click.echo(f"v1 HTTP {v1.status_code}, Deprecation={v1.headers.get('Deprecation')}; "
               f"{len(messages)} MQTT topics captured")


if __name__ == "__main__":
    main()
