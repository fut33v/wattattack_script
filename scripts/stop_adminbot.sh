#!/usr/bin/env bash
# Stop local adminbot started via dev script (kills `python -m adminbot`).
set -euo pipefail

pids=$(pgrep -fi "adminbot" || true)
if [ -z "$pids" ]; then
  echo "adminbot is not running"
  exit 0
fi

echo "Stopping adminbot PIDs: $pids"
echo "$pids" | xargs kill
sleep 0.5

remaining=$(pgrep -fi "adminbot" || true)
if [ -n "$remaining" ]; then
  echo "Force killing remaining PIDs: $remaining"
  echo "$remaining" | xargs kill -9 || true
else
  echo "Stopped adminbot"
fi
