import json

from homie import HomieSnapshot

ROOT = "nt-0000-test0"


def desc(type_suffix, children=()):
    return json.dumps({"homie": "5.0", "type": f"energy.ebus.device.{type_suffix}", "children": list(children)})


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def ready_snapshot(clock=None):
    s = HomieSnapshot(ROOT, clock=clock or FakeClock())
    s.set_connected(True)
    s.ingest(f"ebus/5/{ROOT}/$state", "ready")
    s.ingest(f"ebus/5/{ROOT}/$description", desc("distribution-enclosure", [f"{ROOT}-lugs-up", "c1", "bess1"]))
    s.ingest(f"ebus/5/{ROOT}-lugs-up/$description", desc("lugs"))
    s.ingest("ebus/5/c1/$description", desc("circuit"))
    s.ingest("ebus/5/bess1/$description", desc("bess", ["bess1-mid"]))
    s.ingest("ebus/5/bess1-mid/$description", desc("mid"))
    return s


def test_get_returns_last_value():
    s = ready_snapshot()
    s.ingest("ebus/5/c1/meter/active-power", "-120.5")
    s.ingest("ebus/5/c1/meter/active-power", "-130.0")
    assert s.get("c1", "meter", "active-power") == "-130.0"
    assert s.get("c1", "meter", "imported-energy") is None


def test_devices_of_type_walks_tree_recursively():
    s = ready_snapshot()
    assert s.devices_of_type("circuit") == ["c1"]
    assert s.devices_of_type("mid") == ["bess1-mid"]   # grandchild of the panel


def test_devices_outside_root_tree_are_ignored():
    s = ready_snapshot()
    s.ingest("ebus/5/stranger/$description", desc("circuit"))
    assert s.devices_of_type("circuit") == ["c1"]


def test_malformed_description_is_ignored():
    s = ready_snapshot()
    s.ingest("ebus/5/c1/$description", "{not json")
    assert s.devices_of_type("circuit") == ["c1"]  # previous description kept


def test_unready_when_disconnected():
    s = ready_snapshot()
    s.set_connected(False)
    assert s.unready_reason(60) == "not connected"


def test_unready_when_panel_lost():
    s = ready_snapshot()
    s.ingest(f"ebus/5/{ROOT}/$state", "lost")
    assert s.unready_reason(60) == "panel $state is lost"


def test_unready_before_panel_state_arrives():
    s = HomieSnapshot(ROOT, clock=FakeClock())
    s.set_connected(True)
    assert s.unready_reason(60) == "panel $state is unknown"


def test_unready_after_silence():
    clock = FakeClock()
    s = ready_snapshot(clock)
    clock.t += 61
    assert s.unready_reason(60) == "no MQTT message for 61s"
    s.ingest("ebus/5/c1/meter/active-power", "1")
    assert s.unready_reason(60) is None


def test_non_ebus_topics_ignored():
    s = ready_snapshot()
    s.ingest("something/else", "x")
    s.ingest("ebus/5/c1", "x")  # too short to be a property
    assert s.get("c1", "meter", "active-power") is None
