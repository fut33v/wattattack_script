#!/usr/bin/env bash
# Local dev launcher for the FastAPI webapp with reload enabled.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

. "$ROOT/scripts/dev_env.sh"

# Stop previous webapp dev server to free the port
pkill -fi "uvicorn webapp.main:app" >/dev/null 2>&1 || true

if [ ! -x venv/bin/python ]; then
  echo "venv not found; create it with: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

exec venv/bin/python -m uvicorn webapp.main:app --reload --host 0.0.0.0 --port "${PORT:-3002}"
