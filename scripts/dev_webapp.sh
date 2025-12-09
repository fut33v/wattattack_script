#!/usr/bin/env bash
# Local dev launcher for the FastAPI webapp with reload enabled.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

. "$ROOT/scripts/dev_env.sh"

# Stop previous webapp dev server to free the port
PORT="${PORT:-3002}"
pkill -fi "uvicorn webapp.main:app" >/dev/null 2>&1 || true

# Kill any leftover listener on the target port (best-effort)
if command -v lsof >/dev/null 2>&1; then
  PID_ON_PORT=$(lsof -ti tcp:"${PORT}" -sTCP:LISTEN 2>/dev/null | head -n1)
  if [ -n "${PID_ON_PORT:-}" ]; then
    echo "Stopping process ${PID_ON_PORT} listening on port ${PORT}"
    kill "${PID_ON_PORT}" 2>/dev/null || true
    sleep 0.5
  fi
fi

if [ ! -x venv/bin/python ]; then
  echo "venv not found; create it with: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

exec venv/bin/python -m uvicorn webapp.main:app --reload --host 0.0.0.0 --port "${PORT}"
