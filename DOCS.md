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
with `healthy`, `consecutive_errors`, and `last_error` attributes. If the poll
loop wedges, this sensor stops updating rather than going unavailable, so a
frozen value is itself a symptom, not a clean "no data" state. The
healthchecks.io alert configured above, not the sensor, is the real alarm -
it fires from the dead-man switch regardless of whether the sensor update
also stalled.

## Security

The four credentials (`span_api_key`, `supabase_url`, `supabase_key`,
`healthcheck_url`) are stored by Supervisor in `/data/options.json` in
plaintext, and that file is included in Home Assistant backups. Backups are
commonly synced to cloud storage or a NAS, so password-protect your Home
Assistant backups accordingly.

Using a Supabase key scoped to inserts on `main_energy` and `branch_energy`
is strongly preferable to reusing the service-role key: the service-role key
grants unrestricted access to the full history, so if `options.json` or a
backup is ever exposed, a scoped key limits the damage to future inserts
rather than the entire dataset.

## Important

Do not run the macOS launcher (`run.sh`) at the same time as this app. Two
concurrent writers produce double the rows per tick at near-identical
timestamps, which corrupts the hourly continuous aggregates that are the
permanent historical record.
