#!/usr/bin/env bash
# macOS launcher. The Home Assistant app uses addon-run.sh instead.
# Only one of the two may run at a time: concurrent writers double the rows
# per tick and corrupt the hourly continuous aggregates.
cd /Users/alex.hasha/repos/span_monitor && \
    SPAN_LOG_FILE=span.log caffeinate -i -s nohup uv run python main.py &
