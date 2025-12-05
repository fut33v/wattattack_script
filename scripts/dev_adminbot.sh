#!/usr/bin/env bash
# Local dev launcher for adminbot against the Dockerized Postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Stop any existing adminbot processes to avoid Telegram polling conflicts.
pkill -fi "adminbot" >/dev/null 2>&1 || true

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if command -v dotenv >/dev/null 2>&1; then
  # Prefer python -m dotenv (from python-dotenv) if available for .env parsing
  export $(dotenv -f .env list | xargs)
fi

# Force local DB for dev runs regardless of .env contents
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME="${DB_NAME:-wattattack}"
export DB_USER="${DB_USER:-wattattack}"
export DB_PASSWORD="${DB_PASSWORD:-wattattack}"
export WATTATTACK_ACCOUNTS_FILE="${WATTATTACK_ACCOUNTS_FILE:-$ROOT/accounts.json}"

export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-5432}"
export DB_NAME="${DB_NAME:-wattattack}"
export DB_USER="${DB_USER:-wattattack}"
export DB_PASSWORD="${DB_PASSWORD:-wattattack}"
export WATTATTACK_ACCOUNTS_FILE="${WATTATTACK_ACCOUNTS_FILE:-$ROOT/accounts.json}"

if [ ! -x venv/bin/python ]; then
  echo "venv not found; create it with: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

exec venv/bin/python -m adminbot
