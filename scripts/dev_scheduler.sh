#!/usr/bin/env bash
# Local dev launcher for the scheduler against the Dockerized Postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

. "$ROOT/scripts/dev_env.sh"

# Stop previous scheduler instance to avoid duplicate runs
pkill -fi "scheduler" >/dev/null 2>&1 || true

STATE_DIR="${STATE_DIR:-$ROOT/notifier_state}"
mkdir -p "$STATE_DIR"

if [ ! -x venv/bin/python ]; then
  echo "venv not found; create it with: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

exec venv/bin/python -m scheduler \
  --interval "${WATTATTACK_INTERVAL_SECONDS:-300}" \
  --notifier-args \
  --accounts "$WATTATTACK_ACCOUNTS_FILE" \
  --state "$STATE_DIR/state.json"
