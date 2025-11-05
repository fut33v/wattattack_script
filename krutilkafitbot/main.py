#!/usr/bin/env python3
"""Telegram bot that focuses on WattAttack activity downloads."""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    is_admin as db_is_admin,
)
from wattattack_activities import WattAttackClient
from adminbot.accounts import (
    AccountConfig,
    load_accounts,
    normalize_account_id,
    resolve_account_identifier,
)

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "KRUTILKAFIT_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_RECENT_LIMIT = int(os.environ.get("WATTATTACK_RECENT_LIMIT", "5"))
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))


ACCOUNT_REGISTRY: Dict[str, AccountConfig] = {}


def is_admin_user(user) -> bool:
    if user is None:
        return False
    return db_is_admin(getattr(user, "id", None), getattr(user, "username", None))


def ensure_admin_message(update: Update) -> bool:
    if not update.message:
        return False
    if is_admin_user(update.message.from_user):
        return True
    try:
        update.message.reply_text("🚫 Недостаточно прав для выполнения команды.")
    except Exception:
        pass
    return False


async def ensure_admin_callback(query) -> bool:
    user = getattr(query, "from_user", None)
    if is_admin_user(user):
        return True
    await query.edit_message_text("🚫 Недостаточно прав для выполнения действия.")
    return False


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    await update.message.reply_text(
        "📂 Выберите аккаунт, чтобы получить последние активности:",
        reply_markup=build_accounts_keyboard(DEFAULT_RECENT_LIMIT),
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    message = (
        "ℹ️ Команды krutilkafitbot:\n"
        "/start — показать список аккаунтов\n"
        "/recent <число> — выбрать аккаунт и получить последние N активностей\n"
        "/latest — выслать последнюю активность по каждому аккаунту"
    )
    await update.message.reply_text(message)


async def recent_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    limit = DEFAULT_RECENT_LIMIT
    if context.args:
        try:
            limit = max(1, int(context.args[0]))
        except ValueError:
            await update.message.reply_text("ℹ️ Нужно указать число активностей, например: /recent 5")
            return

    await update.message.reply_text(
        "📂 Выберите аккаунт:",
        reply_markup=build_accounts_keyboard(limit),
    )


async def latest_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    user = update.effective_user
    LOGGER.info(
        "/latest requested by user id=%s username=%s",
        getattr(user, "id", None),
        getattr(user, "username", None),
    )
    await update.message.reply_text("⏳ Собираю последние активности по аккаунтам...")

    cache = context.user_data.setdefault("account_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        context.user_data["account_cache"] = cache

    for account_id, account in ACCOUNT_REGISTRY.items():
        LOGGER.info("Processing latest activities for account_id=%s name=%s", account_id, account.name)
        try:
            activities, profile = await fetch_recent_activities(account_id, 1)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch latest activity for %s", account_id)
            await update.message.reply_text(
                f"⚠️ {account.name}: ошибка получения данных — {exc}",
            )
            continue

        cache[account_id] = {"activities": activities, "profile": profile}

        if not activities:
            await update.message.reply_text(f"ℹ️ {account.name}: активностей пока нет.")
            continue

        activity = activities[0]
        caption = format_activity_meta(activity, account.name, profile)
        fit_id = activity.get("fitFileId")
        if fit_id:
            try:
                temp_path = await download_fit_tempfile(account_id, str(fit_id))
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Fit download failed for %s", account_id)
                await update.message.reply_text(
                    f"⚠️ {account.name}: не удалось скачать FIT — {exc}",
                )
                continue

            filename = f"activity_{activity.get('id')}.fit"
            with temp_path.open("rb") as file_handle:
                await update.message.reply_document(
                    file_handle,
                    filename=filename,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
            temp_path.unlink(missing_ok=True)
            try:
                temp_path.parent.rmdir()
            except OSError:
                LOGGER.debug("Temp directory not removed: %s", temp_path.parent)
        else:
            await update.message.reply_text(
                f"ℹ️ {account.name}: FIT недоступен\n{caption}",
                parse_mode=ParseMode.HTML,
            )


def build_accounts_keyboard(limit: int) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for account_id, account in ACCOUNT_REGISTRY.items():
        buttons.append(
            [
                InlineKeyboardButton(
                    text=account.name,
                    callback_data=f"acct|{account_id}|{limit}",
                )
            ]
        )
    return InlineKeyboardMarkup(buttons)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if not query.data:
        return

    if not await ensure_admin_callback(query):
        return

    parts = query.data.split("|")
    action = parts[0]

    if action == "acct" and len(parts) >= 3:
        account_id = parts[1]
        try:
            limit = max(1, int(parts[2]))
        except ValueError:
            limit = DEFAULT_RECENT_LIMIT
        await send_recent_activities(query, context, account_id, limit)
    elif action == "fit" and len(parts) >= 3:
        account_id = parts[1]
        activity_id = parts[2]
        await send_fit_file(query, context, account_id, activity_id)
    elif action == "noop":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:  # noqa: BLE001
            LOGGER.debug("noop action ignored for message %s", query.message)
    else:
        await query.edit_message_text("❓ Неизвестное действие.")


async def send_recent_activities(query, context, account_id: str, limit: int) -> None:
    if account_id not in ACCOUNT_REGISTRY:
        await query.edit_message_text("⚠️ Аккаунт не найден.")
        return

    account = ACCOUNT_REGISTRY[account_id]

    try:
        LOGGER.info(
            "Fetching recent activities via callback for account_id=%s limit=%d",
            account_id,
            limit,
        )
        activities, profile = await fetch_recent_activities(account_id, limit)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch activities")
        await query.edit_message_text(f"❌ Ошибка: {exc}")
        return

    cache = context.user_data.setdefault("account_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        context.user_data["account_cache"] = cache
    cache[account_id] = {"activities": activities, "profile": profile}

    text_lines = [f"<b>📈 {account.name}</b>"]
    text_lines.append(f"🏁 Последние {min(limit, len(activities))} активностей:")
    keyboard_rows: List[List[InlineKeyboardButton]] = []

    for idx, activity in enumerate(activities[:limit], start=1):
        description = format_activity_line(idx, activity)
        text_lines.append(description)

        fit_id = activity.get("fitFileId")
        if fit_id:
            button = InlineKeyboardButton(
                text=f"Скачать #{idx}",
                callback_data=f"fit|{account_id}|{activity.get('id')}",
            )
            keyboard_rows.append([button])

    if not keyboard_rows:
        keyboard_rows.append([InlineKeyboardButton(text="🚫 FIT недоступен", callback_data="noop")])

    await query.edit_message_text(
        "\n".join(text_lines),
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


def format_activity_line(index: int, activity: Dict[str, Any]) -> str:
    date_str = format_start_time(activity)
    distance = activity.get("distance", 0) or 0
    try:
        distance_km = float(distance) / 1000
    except (TypeError, ValueError):
        distance_km = 0.0
    duration = format_duration(activity.get("elapsedTime"))
    name = activity.get("mapNameRu") or activity.get("name") or "Без названия"

    return f"<b>{index}.</b> 🚴‍♂️ {name} — {distance_km:.1f} км, {duration}, {date_str}"


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "?"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}ч {minutes:02d}м"
    return f"{minutes}м {seconds:02d}с"


def format_start_time(activity: Dict[str, Any]) -> str:
    start_time = activity.get("startTime")
    if not start_time:
        return "?"
    try:
        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        dt = dt + timedelta(hours=3)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return str(start_time)


def format_activity_meta(
    activity: Dict[str, Any],
    account_name: Optional[str] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> str:
    name = activity.get("mapNameRu") or activity.get("name") or "Без названия"
    date_str = format_start_time(activity)
    distance = activity.get("distance", 0) or 0
    try:
        distance_km = float(distance) / 1000
    except (TypeError, ValueError):
        distance_km = 0.0
    duration = format_duration(activity.get("elapsedTime"))
    elevation = activity.get("totalElevationGain")
    power_avg = activity.get("averageWatts")
    cadence_avg = activity.get("averageCadence")
    heartrate_avg = activity.get("averageHeartrate")

    lines: List[str] = []
    if account_name:
        lines.append(f"<b>📈 {account_name}</b>")
    lines.append(f"<b>🚴‍♂️ {name}</b>")
    athlete_name = extract_athlete_name(profile) if profile else ""
    if athlete_name:
        lines.append(f"👤 Атлет: {athlete_name}")
    gender = extract_athlete_field(profile, "gender") if profile else ""
    if gender:
        gender_symbol = "🚹" if str(gender).upper().startswith("M") else "🚺"
        lines.append(f"{gender_symbol} Пол: {'М' if str(gender).upper().startswith('M') else 'Ж'}")
    weight = extract_athlete_field(profile, "weight") if profile else ""
    if weight:
        lines.append(f"⚖️ Вес: {weight} кг")
    ftp_value = extract_athlete_field(profile, "ftp") if profile else ""
    if ftp_value:
        lines.append(f"⚡ FTP: {ftp_value} Вт")
    lines.append(f"📅 Дата: {date_str}")
    lines.append(f"🛣️ Дистанция: {distance_km:.1f} км")
    lines.append(f"⏱️ Время: {duration}")
    if elevation is not None:
        lines.append(f"⛰️ Набор высоты: {elevation} м")
    if power_avg:
        lines.append(f"⚡ Средняя мощность: {power_avg} Вт")
    if cadence_avg:
        lines.append(f"🔄 Средний каденс: {cadence_avg} об/мин")
    if heartrate_avg:
        lines.append(f"❤️ Средний пульс: {heartrate_avg} уд/мин")

    return "\n".join(lines)


def extract_athlete_name(profile: Dict[str, Any]) -> str:
    candidate = profile
    if isinstance(profile.get("user"), dict):
        candidate = profile["user"]
    elif isinstance(profile.get("athlete"), dict):
        candidate = profile["athlete"]

    first = candidate.get("firstName") if isinstance(candidate, dict) else None
    last = candidate.get("lastName") if isinstance(candidate, dict) else None

    parts = [str(part) for part in [first, last] if part]
    if parts:
        return " ".join(parts)

    if isinstance(candidate, dict):
        for key in ("nickname", "name", "displayName"):
            value = candidate.get(key)
            if value:
                return str(value)
    return ""


def extract_athlete_field(profile: Dict[str, Any], field: str) -> str:
    containers = []
    if isinstance(profile.get("athlete"), dict):
        containers.append(profile["athlete"])
    if isinstance(profile.get("user"), dict):
        containers.append(profile["user"])
    containers.append(profile)

    for container in containers:
        if isinstance(container, dict):
            value = container.get(field)
            if value is None and field == "birthDate":
                value = container.get("birth_date")
            if value not in (None, ""):
                return str(value)
    return ""


def _coerce_timestamp(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return None


def _activity_timestamp(activity: Dict[str, Any]) -> float:
    keys = (
        "startTime",
        "start_time",
        "startDate",
        "start_date",
        "startTimestamp",
        "createdAt",
        "created_at",
        "updatedAt",
        "updated_at",
    )
    for key in keys:
        ts = _coerce_timestamp(activity.get(key))
        if ts is not None:
            return ts

    ts = _coerce_timestamp(activity.get("id"))
    if ts is not None:
        return ts

    return float("-inf")


async def fetch_recent_activities(
    account_id: str, limit: int
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    account = ACCOUNT_REGISTRY[account_id]

    def worker() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)
        fetch_limit = max(limit * 3, DEFAULT_RECENT_LIMIT * 2, 30)
        activities, metadata = client.fetch_activity_feed(
            limit=fetch_limit,
            timeout=DEFAULT_TIMEOUT,
        )
        LOGGER.info(
            "Account %s fetched %d activities (limit=%d). metadata_keys=%s",
            account_id,
            len(activities),
            limit,
            list(metadata.keys()),
        )
        strategy = metadata.get("_pagination_strategy")
        if strategy:
            LOGGER.info(
                "Account %s used pagination strategy %s (page_size=%s)",
                account_id,
                strategy,
                metadata.get("_pagination_page_size"),
            )
        if activities:
            sample = [
                {
                    "id": item.get("id"),
                    "startTime": item.get("startTime") or item.get("start_time"),
                    "createdAt": item.get("createdAt") or item.get("created_at"),
                    "fitFileId": item.get("fitFileId"),
                }
                for item in activities[: min(3, len(activities))]
            ]
            LOGGER.info("Account %s first activities (raw order) %s", account_id, sample)
        activities = sorted(
            activities,
            key=_activity_timestamp,
            reverse=True,
        )
        if activities:
            sample_sorted = [
                {
                    "id": item.get("id"),
                    "startTime": item.get("startTime") or item.get("start_time"),
                    "createdAt": item.get("createdAt") or item.get("created_at"),
                    "fitFileId": item.get("fitFileId"),
                }
                for item in activities[: min(3, len(activities))]
            ]
            LOGGER.info("Account %s first activities (sorted) %s", account_id, sample_sorted)

        profile: Dict[str, Any] = {}
        try:
            profile = client.fetch_profile(timeout=DEFAULT_TIMEOUT)
            if not isinstance(profile, dict):
                profile = {}
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch profile for %s: %s", account_id, exc)

        try:
            auth_info = client.auth_check(timeout=DEFAULT_TIMEOUT)
            if isinstance(auth_info, dict) and isinstance(auth_info.get("user"), dict):
                profile.setdefault("user", auth_info["user"])
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch auth info for %s: %s", account_id, exc)

        return activities[:limit], profile

    return await asyncio.to_thread(worker)


async def send_fit_file(query, context, account_id: str, activity_id: str) -> None:
    cache = context.user_data.setdefault("account_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        context.user_data["account_cache"] = cache
    account_cache: Dict[str, Any] = cache.get(account_id, {}) if isinstance(cache, dict) else {}
    activities: List[Dict[str, Any]] = (
        account_cache.get("activities", []) if isinstance(account_cache, dict) else []
    )
    profile: Optional[Dict[str, Any]] = (
        account_cache.get("profile") if isinstance(account_cache, dict) else None
    )
    if profile is not None and not isinstance(profile, dict):
        profile = None
    account = ACCOUNT_REGISTRY.get(account_id)

    activity = None
    for item in activities:
        if str(item.get("id")) == str(activity_id):
            activity = item
            break

    if activity is None:
        try:
            LOGGER.info(
                "Cache miss for activity_id=%s (account=%s); refreshing recent activities",
                activity_id,
                account_id,
            )
            activities, profile = await fetch_recent_activities(account_id, DEFAULT_RECENT_LIMIT)
        except Exception as exc:  # noqa: BLE001
            await query.edit_message_text(f"❌ Ошибка обновления списка: {exc}")
            return
        cache[account_id] = {"activities": activities, "profile": profile}
        for item in activities:
            if str(item.get("id")) == str(activity_id):
                activity = item
                break

    if activity is None:
        await query.edit_message_text("🔍 Активность не найдена.")
        return

    fit_id = activity.get("fitFileId")
    if not fit_id:
        caption = format_activity_meta(
            activity,
            account.name if account else None,
            profile,
        )
        await query.edit_message_text(
            "ℹ️ Для этой активности нет FIT файла.\n\n" + caption,
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        temp_path = await download_fit_tempfile(account_id, str(fit_id))
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Fit download failed")
        await query.edit_message_text(f"❌ Ошибка скачивания: {exc}")
        return

    filename = f"activity_{activity_id}.fit"
    caption = format_activity_meta(
        activity,
        account.name if account else None,
        profile,
    )
    with temp_path.open("rb") as file_handle:
        await query.message.reply_document(
            file_handle,
            filename=filename,
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
    temp_path.unlink(missing_ok=True)
    try:
        temp_path.parent.rmdir()
    except OSError:
        LOGGER.debug("Temp directory not removed: %s", temp_path.parent)


async def download_fit_tempfile(account_id: str, fit_id: str) -> Path:
    account = ACCOUNT_REGISTRY[account_id]
    temp_dir = Path(tempfile.mkdtemp(prefix="wattattack_"))
    temp_path = temp_dir / f"{fit_id}.fit"

    def worker() -> None:
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)
        client.download_fit_file(fit_id, temp_path, timeout=DEFAULT_TIMEOUT)

    await asyncio.to_thread(worker)
    return temp_path


async def noop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    if not await ensure_admin_callback(query):
        return
    await query.answer("Нет доступных файлов")


async def on_error(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Unhandled exception during update", exc_info=context.error)


def build_application(token: str) -> Application:
    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(CommandHandler("recent", recent_handler))
    application.add_handler(CommandHandler("latest", latest_handler))
    application.add_handler(CallbackQueryHandler(callback_handler))

    application.add_error_handler(on_error)
    return application


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    token = os.environ.get(BOT_TOKEN_ENV)
    if not token:
        raise SystemExit(
            "KRUTILKAFIT_BOT_TOKEN не задан. Установите переменную окружения и повторите запуск."
        )

    ensure_admin_table()
    seed_admins_from_env()

    accounts_path = Path(os.environ.get(ACCOUNTS_ENV, DEFAULT_ACCOUNTS_PATH))
    global ACCOUNT_REGISTRY
    ACCOUNT_REGISTRY = load_accounts(accounts_path)

    LOGGER.info("Loaded %d WattAttack accounts", len(ACCOUNT_REGISTRY))

    application = build_application(token)
    application.run_polling()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
