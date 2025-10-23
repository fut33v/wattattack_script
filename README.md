# WattAttack Bot Suite

## Overview
This repository contains a Telegram bot and supporting utilities for managing WattAttack athlete accounts. The bot automates tasks such as applying client data (from a PostgreSQL-backed client database) to WattAttack user profiles, downloading FIT activity files for new workouts, and providing account information on demand. Supporting scripts handle CSV imports of client information and direct profile updates via the WattAttack web API.

## Components
- **wattattack_bot.py** – Telegram bot that interacts with WattAttack accounts:
  - `/start`, `/help` – navigation/help.
  - `/recent <n>` – fetch recent workouts for a selected account.
  - `/latest` – download latest workout FIT for each account.
  - `/account <id>` – show current WattAttack profile data.
  - `/client <query>` – search clients by name/surname.
  - `/setclient <id>` – apply selected client’s data to a WattAttack account.
  - `/uploadclients [truncate]` – import clients from CSV (reply or inline); supports truncation.
  - `/admins`, `/addadmin`, `/removeadmin` – manage bot administrators.
  - Inline menus for account/client selection and ad-hoc text client search (admins only).
- **wattattack_notifier.py** – Periodic notifier that watches WattAttack for new activities and sends FIT files with metadata to admins.
- **wattattack_activities.py** – API wrapper for WattAttack endpoints (`/auth/login`, `/activities`, `/athlete/update`, `/user/update`, `/auth/check`).
- **load_clients.py** – CLI loader to import clients from CSV into PostgreSQL (with optional `--truncate`).
- **admin_repository.py** – Admin management (table creation, seeding from env, CRUD helpers).
- **client_repository.py** – Client access helpers (list, search, get by id). 
- **db_utils.py** – PostgreSQL connection helpers.
- **wattattack_profile_set.py** – CLI tool for updating WattAttack profile values (name, weight, FTP, etc.).

## Features
- **Admin-only controls**: All commands, callbacks, and text handlers require admin status. Admins are stored centrally in the DB; `/addadmin` and `/removeadmin` manipulate them at runtime. Seed list comes from `TELEGRAM_ADMIN_IDS`.
- **Client management**: CSV import normalizes name, gender, weight, height, FTP, pedals, goals, etc. `/client` and plain text search locate clients; `/setclient` applies their data to accounts (keeping mandatory fields like `birthDate` and gender). `/account` fetches up-to-date profile info from WattAttack.
- **Activity notifications**: Notifier sends FIT files and metadata when new workouts appear. Admin list reused from the DB.
- **Docker-ready**: `docker-compose.yml` provides services for bot (`bot`), scheduler (`scheduler`), and Postgres (`db`, exposed on host port 55432). Volume `postgres_data` persists DB state.

## Setup
1. Install dependencies: `pip install -r requirements.txt` (Python 3.11+).
2. Copy `.env.example` to `.env` and configure:
   - `TELEGRAM_BOT_TOKEN`, optional `TELEGRAM_ADMIN_IDS` seed.
   - `WATTATTACK_ACCOUNTS_FILE` (JSON with email/password/base_url per account).
   - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.
   - Optional timeouts/page size (see `.env.example`).
3. Start services: `docker-compose up -d db bot scheduler` (or run bot locally via `python wattattack_bot.py`).
4. Import clients: `python3 load_clients.py --truncate` or send CSV via `/uploadclients` command.
5. Configure bot commands in BotFather:
   ```
   start - показать список аккаунтов
   help - подсказать доступные команды
   recent - последние N активностей для аккаунта
   latest - последняя активность каждого аккаунта
   setclient - применить данные клиента к аккаунту
   account - показать текущие данные аккаунта
   client - найти клиента по БД
   uploadclients - загрузить CSV клиентов
   admins - показать список администраторов
   addadmin - добавить администратора
   removeadmin - удалить администратора
   ```

## Usage Notes
- **Administrators**: Only admins can invoke commands or interact with inline keyboards. Non-admins receive “Недостаточно прав”.
- **Client CSV**: Ensure the columns match `load_clients.py` mapping (e.g., “Имя”, “Фамилия”, “ПОЛ”, “Ваш вес”, etc.). `/uploadclients truncate` replaces all entries; otherwise rows are upserted.
- **Profile updates**: WattAttack requires `birthDate` and gender; bot defaults to `2000-01-01` if missing. Logs include payload and response for visibility.
- **Notifier**: Draws admin IDs from DB (`admins` table). If empty, it logs an error at startup.
- **Security**: `.env` and `accounts.json` are gitignored; keep credentials safe. PostgreSQL credentials supplied via env variables.

## Utilities
- `load_clients.py` – standalone importer (CLI). Returns counts of inserted/updated rows.
- `wattattack_profile_set.py` – direct CLI profile updater (authentication required).
- `wattattack_profile_debug.py` – helper to inspect `/api/v1/athlete` vs `/cabinet` data.

## Future Enhancements
- Additional validation or persona fields if WattAttack adds more profile requirements.
- Schema migrations can be handled via a migration tool if functionality expands.

