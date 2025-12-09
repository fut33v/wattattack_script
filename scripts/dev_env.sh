#!/usr/bin/env bash
# Shared dev environment loader: pulls tokens from .env but forces local DB defaults.
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

if [ -f "$ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
  if command -v dotenv >/dev/null 2>&1; then
    # Prefer python-dotenv if available for more robust parsing
    export $(dotenv -f "$ROOT/.env" list | xargs)
  fi
fi

# Force local Postgres for dev runs to avoid hitting remote DBs accidentally.
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME="${DB_NAME:-wattattack}"
export DB_USER="${DB_USER:-wattattack}"
export DB_PASSWORD="${DB_PASSWORD:-wattattack}"

# Shared accounts file path
export WATTATTACK_ACCOUNTS_FILE="${WATTATTACK_ACCOUNTS_FILE:-$ROOT/accounts.json}"
