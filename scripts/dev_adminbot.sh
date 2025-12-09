#!/usr/bin/env bash
# Local dev launcher for adminbot against the Dockerized Postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

. "$ROOT/scripts/dev_env.sh"

# Stop any existing adminbot processes to avoid Telegram polling conflicts.
pkill -fi "adminbot" >/dev/null 2>&1 || true

if [ ! -x venv/bin/python ]; then
  echo "venv not found; create it with: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

exec venv/bin/python -m adminbot
