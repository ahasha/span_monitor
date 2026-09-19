#!/usr/bin/with-contenv bashio
# Container entrypoint. Translates Home Assistant options into the plain
# environment variables main.py already understands, so no Python code needs
# to know it is running under Home Assistant.
#
# This is NOT run.sh — that remains the macOS launcher.
set -e

export SPAN_IP="$(bashio::config 'span_ip')"
export SPAN_API_KEY="$(bashio::config 'span_api_key')"
export SUPABASE_URL="$(bashio::config 'supabase_url')"
export SUPABASE_KEY="$(bashio::config 'supabase_key')"
export POLL_INTERVAL="$(bashio::config 'poll_interval')"
export STALE_THRESHOLD="$(bashio::config 'stale_threshold')"
export HEARTBEAT_INTERVAL="$(bashio::config 'heartbeat_interval')"
export SENSOR_INTERVAL="$(bashio::config 'sensor_interval')"
export HEALTH_PORT="8099"

# Optional: no URL means no heartbeat, and ping_healthcheck no-ops.
if bashio::config.has_value 'healthcheck_url'; then
    export HEALTHCHECK_URL="$(bashio::config 'healthcheck_url')"
    bashio::log.info "Heartbeat pings enabled"
else
    bashio::log.warning "No healthcheck_url set - you will NOT be alerted if data stops"
fi

# Fail fast and legibly rather than letting Python raise on a None URL.
for required in span_ip span_api_key supabase_url supabase_key; do
    if ! bashio::config.has_value "${required}"; then
        bashio::exit.nok "Required option '${required}' is not set"
    fi
done

bashio::log.info "Starting SPAN monitor (poll every ${POLL_INTERVAL}s)"
exec python3 /app/main.py
