#!/usr/bin/env python3
"""Telegram bot that exposes WattAttack activities for multiple accounts."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_RECENT_LIMIT = int(os.environ.get("WATTATTACK_RECENT_LIMIT", "5"))
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))


@dataclass(frozen=True)
class AccountConfig:
    identifier: str
    name: str
    email: str
    password: str
    base_url: str = DEFAULT_BASE_URL


ACCOUNT_REGISTRY: Dict[str, AccountConfig] = {}


def load_accounts(config_path: Path) -> Dict[str, AccountConfig]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Accounts config file not found: {config_path}. "
            "Create it from the sample template."
        )

    raw_data = json.loads(config_path.read_text(encoding="utf-8"))
    accounts: Dict[str, AccountConfig] = {}

    for entry in raw_data:
        identifier = entry["id"]
        accounts[identifier] = AccountConfig(
            identifier=identifier,
            name=entry.get("name", identifier),
            email=entry["email"],
            password=entry["password"],
            base_url=entry.get("base_url", DEFAULT_BASE_URL),
        )

    if not accounts:
        raise ValueError("Accounts list is empty")

    return accounts


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "Выберите аккаунт WattAttack:",
        reply_markup=build_accounts_keyboard(DEFAULT_RECENT_LIMIT),
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    message = (
        "Использование:\n"
        "/start — показать список аккаунтов\n"
        "/recent <число> — предложить последние N активностей всех аккаунтов"
    )
    await update.message.reply_text(message)


async def recent_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    limit = DEFAULT_RECENT_LIMIT
    if context.args:
        try:
            limit = max(1, int(context.args[0]))
        except ValueError:
            await update.message.reply_text("Нужно указать число активностей, например: /recent 5")
            return

    await update.message.reply_text(
        "Выберите аккаунт:",
        reply_markup=build_accounts_keyboard(limit),
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
    else:
        await query.edit_message_text("Неизвестное действие.")


async def send_recent_activities(query, context, account_id: str, limit: int) -> None:
    if account_id not in ACCOUNT_REGISTRY:
        await query.edit_message_text("Аккаунт не найден.")
        return

    try:
        activities = await fetch_recent_activities(account_id, limit)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch activities")
        await query.edit_message_text(f"Ошибка: {exc}")
        return

    cache = context.user_data.setdefault("activities", {})
    cache[account_id] = activities

    text_lines = [f"Последние {min(limit, len(activities))} активностей:"]
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
        keyboard_rows.append([InlineKeyboardButton(text="FIT недоступен", callback_data="noop")])

    await query.edit_message_text(
        "\n".join(text_lines),
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


def format_activity_line(index: int, activity: Dict[str, Any]) -> str:
    start_time = activity.get("startTime")
    if start_time:
        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        date_str = dt.strftime("%Y-%m-%d %H:%M")
    else:
        date_str = "?"

    distance = activity.get("distance", 0) or 0
    distance_km = float(distance) / 1000 if distance else 0
    duration = format_duration(activity.get("elapsedTime"))
    name = activity.get("mapNameRu") or activity.get("name") or "Без названия"

    return f"<b>{index}.</b> {name} — {distance_km:.1f} км, {duration}, {date_str}"


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "?"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}ч {minutes:02d}м"
    return f"{minutes}м {seconds:02d}с"


async def fetch_recent_activities(account_id: str, limit: int) -> List[Dict[str, Any]]:
    account = ACCOUNT_REGISTRY[account_id]

    def worker() -> List[Dict[str, Any]]:
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)
        payload = client.fetch_activities(timeout=DEFAULT_TIMEOUT)
        return payload.get("activities", [])[:limit]

    return await asyncio.to_thread(worker)


async def send_fit_file(query, context, account_id: str, activity_id: str) -> None:
    cache = context.user_data.setdefault("activities", {})
    activities: List[Dict[str, Any]] = cache.get(account_id, [])

    activity = None
    for item in activities:
        if str(item.get("id")) == str(activity_id):
            activity = item
            break

    if activity is None:
        try:
            activities = await fetch_recent_activities(account_id, DEFAULT_RECENT_LIMIT)
        except Exception as exc:  # noqa: BLE001
            await query.edit_message_text(f"Ошибка обновления списка: {exc}")
            return
        cache[account_id] = activities
        for item in activities:
            if str(item.get("id")) == str(activity_id):
                activity = item
                break

    if activity is None:
        await query.edit_message_text("Активность не найдена.")
        return

    fit_id = activity.get("fitFileId")
    if not fit_id:
        await query.edit_message_text("Для этой активности нет FIT файла.")
        return

    try:
        temp_path = await download_fit_tempfile(account_id, fit_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Fit download failed")
        await query.edit_message_text(f"Ошибка скачивания: {exc}")
        return

    filename = f"activity_{activity_id}.fit"
    with temp_path.open("rb") as file_handle:
        await query.message.reply_document(file_handle, filename=filename)
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
    await update.callback_query.answer("Нет доступных файлов")


async def on_error(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Unhandled exception during update", exc_info=context.error)


def build_application(token: str) -> Application:
    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(CommandHandler("recent", recent_handler))
    application.add_handler(CallbackQueryHandler(noop_handler, pattern="^noop$"))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_error_handler(on_error)

    return application


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)

    token = os.environ.get(BOT_TOKEN_ENV)
    if not token:
        raise SystemExit(
            "TELEGRAM_BOT_TOKEN не задан. Установите переменную окружения и повторите запуск."
        )

    accounts_path = Path(os.environ.get(ACCOUNTS_ENV, DEFAULT_ACCOUNTS_PATH))
    global ACCOUNT_REGISTRY
    ACCOUNT_REGISTRY = load_accounts(accounts_path)

    LOGGER.info("Loaded %d WattAttack accounts", len(ACCOUNT_REGISTRY))

    application = build_application(token)
    application.run_polling()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
