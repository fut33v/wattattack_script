# Straver service

Отдельный сервис, который забирает на себя авторизацию в Strava и загрузку тренировок с другого IP.

## Запуск

```bash
cd straver
docker compose up -d --build
```

По умолчанию API доступен на `http://localhost:8098`.

## Переменные окружения

- `STRAVA_CLIENT_ID` / `STRAVA_CLIENT_SECRET` — креды приложения Strava (обязательные).
- `STRAVA_REDIRECT_URI` — URL коллбэка на стороне Straver (`/strava/callback`).
- `STRAVER_DATABASE_URL` — строка подключения к Postgres (по умолчанию внутри compose).
- `STRAVER_INTERNAL_SECRET` — shared secret для внутренних методов `/internal/*`.
- `TELEGRAM_BOT_TOKEN` — токен бота, чтобы отправлять пользователю сообщение об успешной привязке (опционально).
- `TELEGRAM_LOGIN_BOT_USERNAME` — username логин-бота для редиректа после успешной авторизации (опционально).

## Основные ручки

- `GET /strava/authorize?state=<tg_user_id>` — редирект на Strava OAuth.
- `GET /strava/callback` — получает код, сохраняет токены в своей БД.
- `POST /internal/strava/status` — отдает статус по списку user_id (требуется `X-Internal-Secret`).
- `POST /internal/strava/upload` — принимает файл тренировки и грузит в Strava от имени пользователя.
- `POST /internal/strava/disconnect` — удаляет сохраненные токены пользователя.
