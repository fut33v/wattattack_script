#!/usr/bin/env bash
# Local dev launcher for clientbot against the Dockerized Postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-5432}"
export DB_NAME="${DB_NAME:-wattattack}"
export DB_USER="${DB_USER:-wattattack}"
export DB_PASSWORD="${DB_PASSWORD:-wattattack}"

if [ ! -x venv/bin/python ]; then
  echo "venv not found; create it with: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

exec venv/bin/python -m clientbot.main
