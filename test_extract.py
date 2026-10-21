import csv
import json
from pathlib import Path

import pytest

from extract import SnapshotIncomplete, snapshot_to_rows
from homie import HomieSnapshot
from test_homie import ROOT, desc

NOW = "2026-11-01T12:00:00+00:00"
FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "span_mqtt_dump.json"


def build(circuits=None, dn=True, relay="CLOSED"):
    s = HomieSnapshot(ROOT)
    s.set_connected(True)
    circuits = circuits if circuits is not None else {"c1": {"spaces": "5,7", "relay": "CLOSED"}}
    children = [f"{ROOT}-lugs-up"] + ([f"{ROOT}-lugs-dn"] if dn else []) + list(circuits)
    s.ingest(f"ebus/5/{ROOT}/$state", "ready")
    s.ingest(f"ebus/5/{ROOT}/$description", desc("distribution-enclosure", children))
    s.ingest(f"ebus/5/{ROOT}/status/relay", relay)
    for lug, direction, power, imp, exp in [("up", "UPSTREAM", "850.5", "1000", "200"),
                                            ("dn", "DOWNSTREAM", "40.0", "300", "10")]:
        if lug == "dn" and not dn:
            continue
        d = f"{ROOT}-lugs-{lug}"
        s.ingest(f"ebus/5/{d}/$description", desc("lugs"))
        s.ingest(f"ebus/5/{d}/info/direction", direction)
        s.ingest(f"ebus/5/{d}/meter/active-power", power)
        s.ingest(f"ebus/5/{d}/meter/imported-energy", imp)
        s.ingest(f"ebus/5/{d}/meter/exported-energy", exp)
    for cid, c in circuits.items():
        s.ingest(f"ebus/5/{cid}/$description", desc("circuit"))
        if c.get("spaces") is not None:
            s.ingest(f"ebus/5/{cid}/info/spaces", c["spaces"])
        s.ingest(f"ebus/5/{cid}/switch/relay", c.get("relay", "CLOSED"))
        s.ingest(f"ebus/5/{cid}/meter/active-power", c.get("power", "-500.0"))
        s.ingest(f"ebus/5/{cid}/meter/imported-energy", "1.5")
        s.ingest(f"ebus/5/{cid}/meter/exported-energy", "9000.25")
    return s


def test_main_row_mapping():
    main, _ = snapshot_to_rows(build(), NOW)
    assert main == {
        "time": NOW,
        "relay_state": "CLOSED",
        "main_meter_produced_energy_wh": 200.0,   # UP exported
        "main_meter_consumed_energy_wh": 1000.0,  # UP imported
        "instant_grid_power_w": 850.5,
        "feed_through_power_w": 40.0,
        "feed_through_produced_energy_wh": 10.0,  # DN exported
        "feed_through_consumed_energy_wh": 300.0, # DN imported
        "grid_sample_start_ms": None, "grid_sample_end_ms": None,
        "dsm_grid_state": None, "dsm_state": None, "current_run_config": None,
    }


def test_branch_row_uses_lowest_space_and_one_row_per_circuit():
    _, branches = snapshot_to_rows(build(), NOW)
    assert branches == [{
        "time": NOW, "branch_id": 5, "relay_state": "CLOSED",
        "instant_power_w": -500.0,
        "imported_active_energy_wh": 1.5, "exported_active_energy_wh": 9000.25,
        "measure_start_ts_ms": None, "measure_duration_ms": None, "is_measure_valid": None,
    }]


def test_unknown_relay_becomes_null():
    main, branches = snapshot_to_rows(build(relay="UNKNOWN",
                                            circuits={"c1": {"spaces": "3", "relay": "UNKNOWN"}}), NOW)
    assert main["relay_state"] is None
    assert branches[0]["relay_state"] is None


def test_circuit_without_spaces_is_skipped(caplog):
    _, branches = snapshot_to_rows(build(circuits={"c1": {"spaces": "3"},
                                                   "virt": {"spaces": None},
                                                   "bad": {"spaces": "x"}}), NOW)
    assert [b["branch_id"] for b in branches] == [3]
    assert "virt" in caplog.text and "bad" in caplog.text


def test_missing_downstream_lugs_gives_null_feedthrough():
    main, _ = snapshot_to_rows(build(dn=False), NOW)
    assert main["feed_through_power_w"] is None
    assert main["feed_through_produced_energy_wh"] is None


def test_missing_upstream_lugs_is_incomplete():
    s = build()
    s._descriptions.pop(f"{ROOT}-lugs-up")
    with pytest.raises(SnapshotIncomplete, match="upstream lugs"):
        snapshot_to_rows(s, NOW)


def test_no_circuits_is_incomplete():
    with pytest.raises(SnapshotIncomplete, match="circuit"):
        snapshot_to_rows(build(circuits={}), NOW)


def test_circuit_missing_power_is_incomplete():
    s = build()
    s._values.pop(("c1", "meter", "active-power"))
    with pytest.raises(SnapshotIncomplete, match="c1"):
        snapshot_to_rows(s, NOW)


@pytest.mark.skipif(not FIXTURE.exists(), reason="Task 1 fixture not captured")
def test_real_capture_matches_v1_shape():
    dump = json.loads(FIXTURE.read_text())["messages"]
    # The capture has several devices with serial numbers (panel, battery,
    # MID); find the panel by its type, not by key order.
    root = next(k.split("/")[2] for k, v in dump.items()
                if k.endswith("/$description") and "distribution-enclosure" in json.loads(v)["type"])
    s = HomieSnapshot(root)
    s.set_connected(True)
    for topic, payload in dump.items():
        s.ingest(topic, payload)
    main, branches = snapshot_to_rows(s, NOW)

    v1 = json.loads((FIXTURE.parent / "span_v1_panel.json").read_text())
    assert main["instant_grid_power_w"] == pytest.approx(v1["instantGridPowerW"], abs=500)
    assert main["main_meter_consumed_energy_wh"] == pytest.approx(
        v1["mainMeterEnergy"]["consumedEnergyWh"], rel=1e-3)
    # Every circuit in branch_to_circuit.csv produces exactly one row.
    with open(Path(__file__).parent / "branch_to_circuit.csv") as f:
        circuit_first_space = {}
        for r in csv.DictReader(f):
            circuit_first_space[r["circuit_id"]] = min(int(r["branch_id"]),
                                                       circuit_first_space.get(r["circuit_id"], 99))
    assert {b["branch_id"] for b in branches} >= set(circuit_first_space.values())
