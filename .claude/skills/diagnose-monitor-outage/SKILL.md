---
name: diagnose-monitor-outage
description: Use when the SPAN monitor's down-detector (healthchecks.io) alert fires, the Home Assistant watchdog keeps restarting the add-on, /healthz returns 503, or main_energy/branch_energy has stopped receiving rows.
---

# Diagnosing a down or stale SPAN monitor

Read-only checks, in order. Narrate each before running it.

1. **Is data actually missing?** Query `max(time)` and recent row counts on
   `main_energy` and `branch_energy`. Both stopping together means the whole
   tick is failing, not one insert.
2. **What does the add-on think?** `curl -sS http://<ha-host>:8099/healthz`
   (use `-sS`: plain `-s` prints nothing when the host doesn't answer).
   - `seconds_since_success: null` AND `consecutive_errors: 0` means
     `poll_once` is stuck inside `@retry_on_connection_error`, which retries
     forever without calling `state.record_failure`. Watchdog restarts can't
     help: the new process hits the same wall.
   - Non-zero `consecutive_errors` with a `last_error` means ticks complete
     but fail; read the error.
3. **Is the panel reachable at `SPAN_IP`?** `curl -sS http://<span-ip>/api/v1/status`
   (unauthenticated, read-only).
4. **If not, has the panel moved?** It is Wi-Fi only (`eth0Link: false` is
   normal) and its address is pinned by a DHCP reservation on the router,
   added after it once moved overnight without a reboot. Check the
   reservation, then find its current IP over mDNS:
   `dns-sd -B _http._tcp local.` (it advertises `span-<serial>-HTTP-API`),
   then `dns-sd -G v4 span-<serial>.local`.

Never read `.env`; it holds secrets and is blocked for this project.
