#!/usr/bin/env bash
set -euo pipefail

project_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
lock_file="${TMPDIR:-/tmp}/vn-climate-risk-monitor-weather.lock"

cd "$project_dir"
exec flock --nonblock "$lock_file" uv run run-open-meteo-pipeline --execute "$@"
