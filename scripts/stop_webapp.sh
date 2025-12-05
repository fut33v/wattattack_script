#!/usr/bin/env bash
# Stop local webapp started via dev script (kills uvicorn webapp.main:app).
set -euo pipefail

pids=$(pgrep -f "uvicorn webapp.main:app" || true)
if [ -z "$pids" ]; then
  echo "webapp is not running"
  exit 0
fi

echo "$pids" | xargs kill
echo "Stopped webapp (PIDs: $pids)"
