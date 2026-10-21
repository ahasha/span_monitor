"""Turn a HomieSnapshot into the main_energy / branch_energy rows v1 produced.

The column names are the v1-era schema, kept so the hourly continuous
aggregates and every downstream consumer keep working. The field mapping and
the evidence for it are in
docs/superpowers/plans/2026-10-21-span-api-mqtt-migration.md.

A circuit is written under the lowest breaker space it occupies. v1 wrote a
2-pole circuit as one row per space; the SPAN API meters the circuit as a
whole, so the second space simply stops receiving rows. Sums over a
circuit's branch_ids are unaffected.
"""
import logging

from homie import HomieSnapshot

logger = logging.getLogger(__name__)

RELAY_STATES = {"OPEN", "CLOSED"}  # the relay_state_type enum; anything else -> NULL


class SnapshotIncomplete(Exception):
    pass


def _float(value: str | None) -> float | None:
    return None if value is None else float(value)


def _relay(value: str | None) -> str | None:
    return value if value in RELAY_STATES else None


def _lugs(snapshot: HomieSnapshot, direction: str) -> str | None:
    for dev in snapshot.devices_of_type("lugs"):
        if snapshot.get(dev, "info", "direction") == direction:
            return dev
    return None


def _first_space(spaces: str | None) -> int | None:
    if not spaces:
        return None
    try:
        return min(int(s) for s in spaces.split(","))
    except ValueError:
        return None


def snapshot_to_rows(snapshot: HomieSnapshot, now: str) -> tuple[dict, list[dict]]:
    up = _lugs(snapshot, "UPSTREAM")
    if up is None or snapshot.get(up, "meter", "active-power") is None:
        raise SnapshotIncomplete("upstream lugs not yet published")
    dn = _lugs(snapshot, "DOWNSTREAM")

    def meter(dev, prop):
        return None if dev is None else _float(snapshot.get(dev, "meter", prop))

    main_row = {
        "time": now,
        "relay_state": _relay(snapshot.get(snapshot.root, "status", "relay")),
        "main_meter_produced_energy_wh": meter(up, "exported-energy"),
        "main_meter_consumed_energy_wh": meter(up, "imported-energy"),
        "instant_grid_power_w": meter(up, "active-power"),
        "feed_through_power_w": meter(dn, "active-power"),
        "feed_through_produced_energy_wh": meter(dn, "exported-energy"),
        "feed_through_consumed_energy_wh": meter(dn, "imported-energy"),
        "grid_sample_start_ms": None,
        "grid_sample_end_ms": None,
        "dsm_grid_state": None,
        "dsm_state": None,
        "current_run_config": None,
    }

    circuits = snapshot.devices_of_type("circuit")
    if not circuits:
        raise SnapshotIncomplete("no circuit devices published yet")

    branch_rows = []
    for cid in circuits:
        branch_id = _first_space(snapshot.get(cid, "info", "spaces"))
        if branch_id is None:
            logger.warning(f"Skipping circuit {cid}: no usable info/spaces")
            continue
        power = snapshot.get(cid, "meter", "active-power")
        if power is None:
            raise SnapshotIncomplete(f"circuit {cid} has no active-power yet")
        branch_rows.append({
            "time": now,
            "branch_id": branch_id,
            "relay_state": _relay(snapshot.get(cid, "switch", "relay")),
            "instant_power_w": float(power),
            "imported_active_energy_wh": meter(cid, "imported-energy"),
            "exported_active_energy_wh": meter(cid, "exported-energy"),
            "measure_start_ts_ms": None,
            "measure_duration_ms": None,
            "is_measure_valid": None,
        })
    branch_rows.sort(key=lambda r: r["branch_id"])
    return main_row, branch_rows
