# Repository Guidelines

## Project Structure & Module Organization
Core Telegram bot logic lives in `adminbot/`, `krutilkafitbot/`, and `clientbot/`, each exposing a `__main__` so you can run them with `python -m <package>`. The scheduler/notification loop resides in `scheduler/`, while backend APIs, templates, and auth helpers sit under `webapp/` with the React/Vite SPA in `webapp/frontend/`. Database helpers are centralized in `repositories/`, CSV/CLI utilities in `scripts/`, and seed assets in `data/` plus `accounts.sample.json`. The lightweight integration test harness currently lives at `test_activity_tracking.py`.

## Build, Test, and Development Commands
- `python -m venv venv && source venv/bin/activate && pip install -r requirements.txt` — create a 3.11+ environment for bots, schedulers, and the FastAPI service.
- `python -m adminbot` / `python -m krutilkafitbot` / `python -m scheduler` — run the corresponding Telegram entry points with your `.env` loaded.
- `uvicorn webapp.main:app --reload` — launch the API + server-rendered assets; mount the SPA build from `webapp/frontend/dist`.
- `cd webapp/frontend && npm install && npm run dev` for iterative UI work, and `npm run build` before packaging Docker images.
- `docker compose up -d --build` or `./start.sh` — spin up Postgres, bots, scheduler, and the web app exactly as production expects.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation, descriptive snake_case functions, and module-level `log = logging.getLogger(__name__)` rather than ad-hoc prints. Keep env and token constants uppercase (see `adminbot/main.py:BOT_TOKEN_ENV`) and return plain dict payloads like the repository helpers do. FastAPI endpoints favor explicit Pydantic-like typing even when returning dicts; mirror that approach. Frontend code is TypeScript-first with PascalCase component files (`webapp/frontend/src/components/DataGrid.tsx`) and React Query hooks under `lib/`. Keep JSX lean, colocate styles in `src/styles`, and use ESLint (`npx eslint src --max-warnings=0`) before committing UI changes.

## Testing Guidelines
Grow coverage by mirroring `test_activity_tracking.py`: place new tests alongside the feature or under a future `tests/` package using the `test_*.py` naming scheme so `python -m pytest` can discover them. Stub external services (WattAttack API, Telegram, Postgres) via fixtures or temporary containers instead of hitting production endpoints. When adding scheduler or bot features, include a minimal reproduction script plus logging assertions so regressions are visible in CI logs. Frontend work should include at least a screenshot or a manual checklist until Vitest/RTL coverage is added.

## Commit & Pull Request Guidelines
History shows short, imperative commits such as `schedule fix` or `docker compose`; keep messages under ~60 characters and focus on what the change does. Every PR should include: (1) a concise summary plus linked issue, (2) notes on new env vars, migrations, or scripts, (3) test evidence (`pytest`, CLI transcript, or manual steps), and (4) UI screenshots/GIFs for visible changes. Request review from someone owning the affected bot or web surface, and avoid bundling unrelated refactors with feature work.

## Security & Configuration Tips
Do not commit filled `accounts.json`, `.env`, or bot tokens; rely on `accounts.sample.json` and document secrets in team vaults. Required settings include `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_IDS`, the login bot credentials, and `DATABASE_URL` consumed by `repositories/db_utils.py`. The default Postgres in `docker-compose.yml` listens on `localhost:55432`; point tests to that instance or to a disposable container rather than production. Rotate tokens promptly if they leak in logs, and keep long-running services behind the `docker compose` stack so credentials only live in container env vars.
