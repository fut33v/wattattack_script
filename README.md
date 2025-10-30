# WattAttack Bot Suite

## Overview
This repository contains a Telegram bot and supporting utilities for managing WattAttack athlete accounts, а также веб-приложение «Крутилка» для администраторов. The bot automates tasks such as applying client data (from a PostgreSQL-backed client database) to WattAttack user profiles, downloading FIT activity files for new workouts, and providing account information on demand. Supporting scripts handle CSV imports of client information and direct profile updates via the WattAttack web API.

## Components
- **wattattackbot/** – Telegram bot package that interacts with WattAttack accounts:
  - `/start`, `/help` – navigation/help.
  - `/recent <n>` – fetch recent workouts for a selected account.
  - `/latest` – download latest workout FIT for each account.
  - `/account <id>` – show current WattAttack profile data.
  - `/bikes [поиск]` – list available bikes from the shared inventory (supports search term).
  - `/client <query>` – search clients by name/surname.
  - `/setclient <id>` – apply selected client’s data to a WattAttack account.
  - `/uploadclients [truncate]` – import clients from CSV (reply or inline); supports truncation.
  - `/uploadbikes [truncate]` – import bicycles from CSV (reply or inline); supports truncation.
  - `/uploadstands [truncate]` – import trainer inventory from CSV (reply or inline); supports truncation.
  - `/uploadworkout [all|account…]` – upload a ZWO workout file into one or more WattAttack libraries (parsing + metrics).
  - `/admins`, `/addadmin`, `/removeadmin` – manage bot administrators.
  - Inline menus for account/client selection and ad-hoc text client search (admins only).
- **wattattackscheduler/** – Scheduler loop and notifier CLI that watch WattAttack for new activities and send FIT files with metadata to admins.
- **wattattack_activities.py** – API wrapper for WattAttack endpoints (`/auth/login`, `/activities`, `/athlete/update`, `/user/update`, `/auth/check`, `/workouts/user-create`).
- **wattattack_workouts.py** – ZWO workout parser, sanitizer, chart/metrics calculator, and payload builder for library uploads.
- **scripts/load_clients.py** – CLI loader to import clients from CSV into PostgreSQL (with optional `--truncate`).
- **scripts/load_bikes.py** – CLI loader for the bicycle inventory CSV (with optional `--truncate`).
- **scripts/load_trainers.py** – CLI loader for the trainer inventory CSV (with optional `--truncate`).
- **repositories/admin_repository.py** – Admin management (table creation, seeding from env, CRUD helpers).
- **repositories/client_repository.py** – Client access helpers (list, search, get by id).
- **repositories/bikes_repository.py** – Bicycle inventory access helpers (table creation, search/list helpers).
- **repositories/trainers_repository.py** – Trainer inventory access helpers (table creation, search/list helpers).
- **repositories/db_utils.py** – PostgreSQL connection helpers.
- **scripts/wattattack_profile_set.py** – CLI tool for updating WattAttack profile values (name, weight, FTP, etc.).
- **krutilkavnbot/** – Telegram бот для клиентов: авторизация по фамилии и привязка Telegram-пользователей к записям клиентов.
- **webapp/frontend/** – одностраничное приложение «Крутилка» на React + Vite с React Query и современным UI для работы с API.
- **webapp/** – FastAPI бэкенд (API + Telegram login) и современный React/Vite фронтенд для управления клиентами, велосипедами, тренажерами, связками и администраторами.

## Features
- **Admin-only controls**: All commands, callbacks, and text handlers require admin status. Admins are stored centrally in the DB; `/addadmin` and `/removeadmin` manipulate them at runtime. Seed list comes from `TELEGRAM_ADMIN_IDS`.
- **Client management**: CSV import normalizes name, gender, weight, height, FTP, pedals, goals, etc. `/client` and plain text search locate clients; `/setclient` applies their data to accounts (keeping mandatory fields like `birthDate` and gender). Карточка клиента включает отдельную кнопку для просмотра подобранных велосипедов и станков (учитываются рост, оси и кассеты). `/stands` выводит учёт станков и позволяет редактировать оси/кассеты из бота, `/bikes` показывает парк велосипедов и даёт менять допустимые значения роста. `/account` fetches up-to-date profile info from WattAttack.
- **Workout uploads**: `/uploadworkout` parses ZWO files server-side, builds chart data and advanced power metrics (IF, NP, TSS, zone breakdown) using the athlete’s FTP when available, and pushes the workout into the selected WattAttack account(s).
- **Activity notifications**: Notifier sends FIT files and metadata when new workouts appear. Admin list reused from the DB.
- **Docker-ready**: `docker-compose.yml` provides services for bot (`bot`), scheduler (`scheduler`), and Postgres (`db`, exposed on host port 55432). Volume `postgres_data` persists DB state.

## Setup
1. Install backend dependencies: `pip install -r requirements.txt` (Python 3.11+).
2. Install frontend dependencies: `cd webapp/frontend && npm install` (Node.js 18+), затем вернитесь в корень `cd ../..`.
3. Copy `.env.example` to `.env` and configure:
   - `TELEGRAM_BOT_TOKEN`, optional `TELEGRAM_ADMIN_IDS` seed.
   - `KRUTILKAVN_BOT_TOKEN` for the greeting bot (use `KRUTILKAVN_GREETING` to override the default message).
   - `WATTATTACK_ACCOUNTS_FILE` (JSON with email/password/base_url per account).
   - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.
   - Optional timeouts/page size (see `.env.example`).
   - `TELEGRAM_LOGIN_BOT_USERNAME` — username логин-бота без `@` (он же используется для виджета авторизации).
   - `TELEGRAM_LOGIN_BOT_TOKEN` — токен логин-бота (по умолчанию берётся `KRUTILKAVN_BOT_TOKEN`, затем `TELEGRAM_BOT_TOKEN`, если переменная не задана).
   - `WEBAPP_SECRET_KEY` — случайная строка для подписи cookie-сессий.
   - `WEBAPP_BASE_URL` (опция) — базовый URL, с которого доступно приложение (используется в ссылках).
   - `WEBAPP_CLIENTS_PAGE_SIZE` (опция) — размер страницы списка клиентов в вебе (по умолчанию 50).
4. Start services:
   - Backend/API: `docker-compose up -d db webapp` (или `uvicorn webapp.main:app --reload`) — сервис отдаёт API и собранную SPA «Крутилка».
   - Frontend (разработка): `cd webapp/frontend && npm run dev` — Vite поднимет SPA «Крутилка» на `http://localhost:5173` и проксирует вызовы к `:8000`.
   - Telegram-боты: `docker-compose up -d bot scheduler krutilkavnbot` (как и прежде).
   - Для production-образа `docker-compose up -d db webapp` автоматически соберёт фронтенд внутри Dockerfile (используется Node-стадия).
5. Import reference data:
   - Clients: `python -m scripts.load_clients --truncate` or send CSV via `/uploadclients`.
   - Bikes: `python -m scripts.load_bikes --truncate` to load the inventory CSV from `data/`.
   - Stands: `python -m scripts.load_trainers --truncate` to import the trainer inventory.
6. Configure bot commands in BotFather:
   ```
   start - показать список аккаунтов
   help - подсказать доступные команды
   recent - последние N активностей для аккаунта
   latest - последняя активность каждого аккаунта
   setclient - применить данные клиента к аккаунту
   account - показать текущие данные аккаунта
   bikes - показать доступные велосипеды
   stands - показать доступные станки
   client - найти клиента по БД
   uploadclients - загрузить CSV клиентов
   uploadbikes - загрузить CSV велосипедов
   uploadstands - загрузить CSV станков
   uploadworkout - загрузить тренировку ZWO в библиотеку
   admins - показать список администраторов
   addadmin - добавить администратора
   removeadmin - удалить администратора
   ```

## Usage Notes
- **Administrators**: Only admins can invoke commands or interact with inline keyboards. Non-admins receive “Недостаточно прав”.
- **Client linking**: `krutilkavnbot` запоминает выбранного клиента в таблице `client_links` (создаётся автоматически). Пользователь может повторно пройти авторизацию и выбрать другую запись в любой момент — привязка вступает в силу только после подтверждения администратором.
- **Client onboarding**: Если клиент не найден по фамилии или пользователь выбрал «Создать новую запись», бот проводит через мини-анкету (имя, фамилия, вес, рост, пол, FTP, педали, цель) и автоматически создаёт запись в базе; имя, фамилия, вес и рост обязательны, FTP по умолчанию 150, пол выбирается кнопками «М/Ж», педали — из фиксированного списка, необязательные поля (FTP и цель) можно пропустить кнопкой «ОК».

## Web Admin (SPA «Крутилка»)
- **Авторизация** выполняется через Telegram Login виджет. После успешного входа backend устанавливает cookie-сессию, а SPA работает через REST API (`/api/*`).
- **API эндпоинты**: `/api/clients`, `/api/bikes`, `/api/trainers`, `/api/client-links`, `/api/admins`, `/api/summary`, `/api/session`.
- **Разработка**: запускайте фронтенд (`npm run dev`) и бэкенд (`uvicorn webapp.main:app --reload`) параллельно. Vite проксирует запросы `/api`, `/auth`, `/logout` на порт 8000.
- **Production**: `docker-compose up webapp` соберёт React-приложение «Крутилка», положит статические файлы в `webapp/frontend/dist` и обслужит их через FastAPI (`/app/*`).
- **Client CSV**: Ensure the columns match `scripts/load_clients.py` mapping (e.g., “Имя”, “Фамилия”, “ПОЛ”, “Ваш вес”, etc.). `/uploadclients truncate` replaces all entries; otherwise rows are upserted.
- **Bike inventory**: The CSV should match `scripts/load_bikes.py` expectations (ID, название, владелец, размер, рост от/до, передачи, эксцентрик/ось, кассета). Trainer CSV should follow `scripts/load_trainers.py` mapping (код, модель, отображаемое имя, хозяин, оси, кассета, комментарий). Use `/bikes <поиск>` для велосипедов и `/stands <поиск>` для станков; по кнопке станка можно отредактировать тип оси и кассету. Карточки клиента автоматически показывают совместимые велосипеды и станки.
- **CSV uploads**: Команды `/uploadclients`, `/uploadbikes`, `/uploadstands` принимают CSV как документ (можно переслать, ответить на файл, использовать `truncate` для полной перезагрузки).
- **Inventory editing**: Команды `/bikes` и `/stands` выводят списки с кнопками для редактирования допустимого роста велосипедов и параметров станков прямо в Telegram.
- **Profile updates**: WattAttack requires `birthDate` and gender; bot defaults to `2000-01-01` if missing. Logs include payload and response for visibility.
- **Notifier**: Draws admin IDs from DB (`admins` table). If empty, it logs an error at startup.
- **Security**: `.env` and `accounts.json` are gitignored; keep credentials safe. PostgreSQL credentials supplied via env variables.

## Utilities
- `scripts/load_clients.py` – standalone importer (CLI). Returns counts of inserted/updated rows.
- `scripts/load_bikes.py` – bicycle inventory importer (CLI) with optional truncation.
- `scripts/load_trainers.py` – trainer inventory importer (CLI) with optional truncation.
- `scripts/wattattack_profile_set.py` – direct CLI profile updater (authentication required).
- `scripts/wattattack_profile_debug.py` – helper to inspect `/api/v1/athlete` vs `/cabinet` data.

## Future Enhancements
- Additional validation or persona fields if WattAttack adds more profile requirements.
- Schema migrations can be handled via a migration tool if functionality expands.
