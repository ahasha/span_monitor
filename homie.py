"""Last-known-value store for a Homie 5 / eBus device tree.

The panel publishes retained values and, since firmware r202639, does not
republish a value that has not changed. So the latest value seen for a topic
IS the current value, however old the message is; liveness has to come from
the connection, the panel's $state, and whether anything at all is arriving.

ingest() runs on paho's network thread; every reader runs on the main thread.
"""
import json
import threading
import time

PREFIX = "ebus/5/"
TYPE_PREFIX = "energy.ebus.device."


class HomieSnapshot:
    def __init__(self, root_device_id: str, clock=time.monotonic):
        self._root = root_device_id
        self._clock = clock
        self._lock = threading.Lock()
        self._values: dict[tuple[str, str, str], str] = {}
        self._state: dict[str, str] = {}
        self._descriptions: dict[str, dict] = {}
        self._connected = False
        self._last_message = None

    def set_connected(self, connected: bool) -> None:
        with self._lock:
            self._connected = connected

    def ingest(self, topic: str, payload: str) -> None:
        if not topic.startswith(PREFIX):
            return
        parts = topic[len(PREFIX):].split("/")
        with self._lock:
            self._last_message = self._clock()
            if len(parts) == 2 and parts[1] == "$state":
                self._state[parts[0]] = payload
            elif len(parts) == 2 and parts[1] == "$description":
                try:
                    self._descriptions[parts[0]] = json.loads(payload)
                except ValueError:
                    pass
            elif len(parts) == 3 and not parts[1].startswith("$") and not parts[2].startswith("$"):
                self._values[(parts[0], parts[1], parts[2])] = payload

    def get(self, device_id: str, node: str, prop: str) -> str | None:
        with self._lock:
            return self._values.get((device_id, node, prop))

    def devices_of_type(self, type_suffix: str) -> list[str]:
        wanted = TYPE_PREFIX + type_suffix
        with self._lock:
            found, seen, queue = [], set(), [self._root]
            while queue:
                dev = queue.pop(0)
                if dev in seen:
                    continue
                seen.add(dev)
                d = self._descriptions.get(dev) or {}
                if dev != self._root and d.get("type") == wanted:
                    found.append(dev)
                queue.extend(d.get("children") or [])
            return found

    def unready_reason(self, max_silence: float) -> str | None:
        with self._lock:
            if not self._connected:
                return "not connected"
            state = self._state.get(self._root)
            if state is None:
                return "panel $state is unknown"
            if state != "ready":
                return f"panel $state is {state}"
            silence = self._clock() - self._last_message
            if silence > max_silence:
                return f"no MQTT message for {silence:.0f}s"
            return None
