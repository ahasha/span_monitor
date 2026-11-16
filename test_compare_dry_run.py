from datetime import datetime, UTC

import polars as pl
import pytest

from compare_dry_run import (
    circuit_keys,
    compare_branches,
    compare_main,
    parse_dry_run_log,
    v1_branches_by_circuit,
)

LOG = """\
2026-10-21 16:47:29,266 ERROR SPAN snapshot not ready: not connected
2026-10-21 16:47:34,275 INFO DRY RUN main={'time': '2026-10-21T20:47:34.270000+00:00', 'relay_state': 'CLOSED', 'main_meter_produced_energy_wh': 0.1, 'main_meter_consumed_energy_wh': 1000.0, 'instant_grid_power_w': 850.5, 'feed_through_power_w': 3.3, 'feed_through_produced_energy_wh': 10.0, 'feed_through_consumed_energy_wh': 300.0, 'grid_sample_start_ms': None, 'grid_sample_end_ms': None, 'dsm_grid_state': None, 'dsm_state': None, 'current_run_config': None} branches=2 sum_branch_w=-840.0
2026-10-21 16:47:34,275 INFO DRY RUN branch 5: relay=CLOSED power_w=-835.0 imported_wh=90.4 exported_wh=2593125.9
2026-10-21 16:47:34,275 INFO DRY RUN branch 14: relay=None power_w=-5.0 imported_wh=None exported_wh=100.0
2026-10-21 16:47:39,286 INFO DRY RUN main={'time': '2026-10-21T20:47:39.280000+00:00', 'relay_state': 'CLOSED', 'main_meter_produced_energy_wh': 0.1, 'main_meter_consumed_energy_wh': 1001.0, 'instant_grid_power_w': 860.0, 'feed_through_power_w': 3.0, 'feed_through_produced_energy_wh': 10.0, 'feed_through_consumed_energy_wh': 300.0, 'grid_sample_start_ms': None, 'grid_sample_end_ms': None, 'dsm_grid_state': None, 'dsm_state': None, 'current_run_config': None} branches=1 sum_branch_w=-830.0
2026-10-21 16:47:39,286 INFO DRY RUN branch 5: relay=CLOSED power_w=-830.0 imported_wh=90.4 exported_wh=2593127.1
"""


def ts(s):
    return datetime.fromisoformat(s).astimezone(UTC)


def test_parse_dry_run_log_reads_main_and_branch_rows():
    main, branches = parse_dry_run_log(LOG.splitlines())

    assert main.height == 2
    assert main["time"][0] == ts("2026-10-21T20:47:34.270000+00:00")
    assert main["instant_grid_power_w"].to_list() == [850.5, 860.0]

    # Branch lines carry no timestamp of their own; they belong to the
    # preceding main line's tick.
    assert branches.height == 3
    assert branches.filter(pl.col("branch_id") == 14)["time"][0] == ts("2026-10-21T20:47:34.270000+00:00")
    assert branches.filter(pl.col("branch_id") == 14)["imported_active_energy_wh"][0] is None
    assert branches.filter(pl.col("branch_id") == 5)["exported_active_energy_wh"].to_list() == [2593125.9, 2593127.1]


def test_parse_ignores_branch_lines_before_first_main_line():
    _, branches = parse_dry_run_log(["x INFO DRY RUN branch 5: relay=CLOSED power_w=1 imported_wh=1 exported_wh=1"])
    assert branches.height == 0


def test_circuit_keys_map_every_space_to_the_lowest_space_of_its_circuit():
    mapping = pl.DataFrame({"branch_id": [5, 7, 14], "circuit_id": ["hvac", "hvac", "fridge"],
                            "name": ["HVAC", "HVAC", "Fridge"]})
    keys = circuit_keys(mapping)
    assert dict(zip(keys["branch_id"], keys["circuit_key"])) == {5: 5, 7: 5, 14: 14}
    assert dict(zip(keys["circuit_key"], keys["name"])) == {5: "HVAC", 14: "Fridge"}


def test_v1_legs_are_summed_per_circuit_and_unmapped_spaces_dropped():
    keys = circuit_keys(pl.DataFrame({"branch_id": [5, 7], "circuit_id": ["hvac", "hvac"], "name": ["HVAC", "HVAC"]}))
    t = ts("2026-10-21T20:47:33+00:00")
    v1 = pl.DataFrame({
        "time": [t, t, t], "branch_id": [5, 7, 32],
        "instant_power_w": [-400.0, -434.0, -0.05],
        "imported_active_energy_wh": [40.0, 50.4, 1.0],
        "exported_active_energy_wh": [1_000_000.0, 1_593_125.0, 261.0],
    })
    out = v1_branches_by_circuit(v1, keys)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["circuit_key"] == 5
    assert row["instant_power_w"] == pytest.approx(-834.0)
    assert row["exported_active_energy_wh"] == pytest.approx(2_593_125.0)


def test_compare_main_matches_nearest_tick_within_tolerance():
    dry, _ = parse_dry_run_log(LOG.splitlines())
    v1 = pl.DataFrame({
        "time": [ts("2026-10-21T20:47:33+00:00"), ts("2026-10-21T20:47:50+00:00")],
        "instant_grid_power_w": [845.0, 900.0],
        "main_meter_consumed_energy_wh": [999.5, 1003.0],
        "main_meter_produced_energy_wh": [0.1, 0.1],
        "feed_through_power_w": [3.0, 3.0],
        "feed_through_consumed_energy_wh": [300.0, 300.0],
        "feed_through_produced_energy_wh": [10.0, 10.0],
    })
    out = compare_main(dry, v1, tolerance_s=5)
    # 20:47:34 matches 20:47:33 (1 s); 20:47:39 is >5 s from both v1 ticks.
    assert out.height == 1
    assert out["instant_grid_power_w_diff"][0] == pytest.approx(5.5)
    assert out["main_meter_consumed_energy_wh_diff"][0] == pytest.approx(0.5)


def test_compare_branches_matches_per_circuit():
    keys = circuit_keys(pl.DataFrame({"branch_id": [5, 7], "circuit_id": ["hvac", "hvac"], "name": ["HVAC", "HVAC"]}))
    _, dry = parse_dry_run_log(LOG.splitlines())
    t = ts("2026-10-21T20:47:35+00:00")
    v1 = v1_branches_by_circuit(pl.DataFrame({
        "time": [t, t], "branch_id": [5, 7],
        "instant_power_w": [-400.0, -433.0],
        "imported_active_energy_wh": [40.0, 50.4],
        "exported_active_energy_wh": [1_000_000.0, 1_593_125.5],
    }), keys)
    out = compare_branches(dry, v1, keys, tolerance_s=3)
    # Branch 14 has no v1 circuit in the mapping; only HVAC matches.
    assert out["circuit_key"].to_list() == [5]
    assert out["name"][0] == "HVAC"
    assert out["instant_power_w_diff"][0] == pytest.approx(-2.0)
    assert out["exported_active_energy_wh_diff"][0] == pytest.approx(0.4, abs=1e-6)


def test_counter_tolerance_allows_for_energy_used_between_readings():
    from compare_dry_run import counter_excess
    t = ts("2026-10-21T20:00:00+00:00")
    df = pl.DataFrame({
        "time": [t, t],
        "time_v1": [t - __import__("datetime").timedelta(seconds=3.9)] * 2,
        "c_diff": [-7.5, -7.5],
        "p": [6000.0, 0.0],
        "p_v1": [7000.0, 0.0],   # the larger reading counts: 7 kW for 3.9 s is 7.6 Wh
    })
    excess = counter_excess(df, "c_diff", ["p", "p_v1"], counter_tol=1.0)
    assert excess.to_list() == pytest.approx([0.0, 6.5], abs=0.01)
