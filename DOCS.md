# SPAN Monitor

Polls a SPAN electrical panel on the local network every few seconds and writes
aggregate and per-circuit energy data to Supabase (TimescaleDB).

## Configuration

| Option | Required | Description |
|---|---|---|
| `span_ip` | yes | Local IP address of the SPAN panel, e.g. `192.168.1.50` |
| `span_api_key` | yes | Bearer token for the SPAN panel API |
| `supabase_url` | yes | Supabase project URL |
| `supabase_key` | yes | Supabase service role key |
| `healthcheck_url` | no | healthchecks.io ping URL. Leave blank to disable alerting. |
| `poll_interval` | no | Seconds between polls (default 5) |
| `stale_threshold` | no | Seconds without a successful write before the watchdog restarts the app (default 300) |
| `heartbeat_interval` | no | Minimum seconds between heartbeat pings (default 60) |
| `sensor_interval` | no | Minimum seconds between status sensor updates (default 30) |

## Alerting

Create a check at <https://healthchecks.io>, set **period** to 5 minutes and
**grace** to 5 minutes, add your email as the notification method, and paste the
ping URL into `healthcheck_url`. You will be alerted roughly 10 minutes after
data stops reaching the database — including when the Pi loses power or the
internet goes down, because the alarm lives off-premises.

## Health

The app serves `/healthz` on port 8099. It returns 200 when a write succeeded
within `stale_threshold` seconds and 503 otherwise. Supervisor's watchdog polls
this and restarts the container on 503, which recovers the case where the
process is alive but silently writing nothing.

A `sensor.span_monitor` entity reports seconds since the last successful write,
with `healthy`, `consecutive_errors`, and `last_error` attributes.

## Important

Do not run the macOS launcher (`run.sh`) at the same time as this app. Two
concurrent writers produce double the rows per tick at near-identical
timestamps, which corrupts the hourly continuous aggregates that are the
permanent historical record.
