"""Handlers and helpers for account commands."""
from __future__ import annotations

import asyncio
import html
import logging
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from adminbot.accounts import AccountConfig, normalize_account_id as normalize_account_id_value
from wattattack_activities import WattAttackClient

LOGGER = logging.getLogger(__name__)

_account_registry: Mapping[str, AccountConfig] = {}
_ensure_admin_message = None
_resolve_account_identifier = None
_format_account_list = None
_default_timeout: float = 30.0


def configure_accounts_view(
    *,
    account_registry: Mapping[str, AccountConfig],
    ensure_admin_message,
    resolve_account_identifier,
    format_account_list,
    default_timeout: float,
) -> None:
    global _account_registry, _ensure_admin_message, _resolve_account_identifier, _format_account_list, _default_timeout
    _account_registry = account_registry
    _ensure_admin_message = ensure_admin_message
    _resolve_account_identifier = resolve_account_identifier
    _format_account_list = format_account_list
    _default_timeout = default_timeout


def register_account_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("account", account_handler))


def _extract_athlete_name(profile: Dict[str, Any]) -> str:
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


def _extract_athlete_field(profile: Dict[str, Any], field: str) -> str:
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


def fetch_account_information(account_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    account = _account_registry[account_id]
    client = WattAttackClient(account.base_url)
    client.login(account.email, account.password, timeout=_default_timeout)

    profile: Dict[str, Any] = {}
    try:
        profile = client.fetch_profile(timeout=_default_timeout)
        if not isinstance(profile, dict):
            profile = {}
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch profile for %s: %s", account_id, exc)
        profile = {}

    auth_user: Dict[str, Any] = {}
    try:
        auth_info = client.auth_check(timeout=_default_timeout)
        if isinstance(auth_info, dict) and isinstance(auth_info.get("user"), dict):
            auth_user = auth_info["user"]
            profile.setdefault("user", auth_user)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch auth info for %s: %s", account_id, exc)

    return profile, auth_user


def format_account_details(
    account_id: str,
    profile: Dict[str, Any],
    auth_user: Dict[str, Any],
) -> str:
    account = _account_registry[account_id]
    lines = [f"<b>👤 {account.name}</b> ({account_id})"]

    first = auth_user.get("firstName") if auth_user else None
    last = auth_user.get("lastName") if auth_user else None
    if first or last:
        name_str = " ".join(part for part in [first, last] if part)
        if name_str:
            lines.append(f"🧑 Имя: {name_str}")
    else:
        name = _extract_athlete_name(profile)
        if name:
            lines.append(f"🧑 Имя: {name}")

    gender = _extract_athlete_field(profile, "gender")
    if gender:
        gender_symbol = "🚹" if gender.upper().startswith("M") else "🚺"
        lines.append(f"{gender_symbol} Пол: {'М' if gender.upper().startswith('M') else 'Ж'} ({gender})")

    weight = _extract_athlete_field(profile, "weight")
    if weight:
        try:
            lines.append(f"⚖️ Вес: {float(weight):g} кг")
        except (TypeError, ValueError):
            lines.append(f"⚖️ Вес: {weight} кг")

    height = _extract_athlete_field(profile, "height")
    if height:
        try:
            lines.append(f"📏 Рост: {float(height):g} см")
        except (TypeError, ValueError):
            lines.append(f"📏 Рост: {height} см")

    ftp = _extract_athlete_field(profile, "ftp")
    if ftp:
        try:
            lines.append(f"⚡ FTP: {int(float(ftp))} Вт")
        except (TypeError, ValueError):
            lines.append(f"⚡ FTP: {ftp} Вт")

    return "\n".join(lines)


async def show_account_selection(
    *, message: Optional[Message] = None, query=None, kind: str = "account", account_id: Optional[str] = None
) -> None:
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for account_id in sorted(_account_registry):
        alias = normalize_account_id_value(account_id)
        label = f"{alias} — {_account_registry[account_id].name}"
        if kind == "workout":
            callback = f"workout_select|{account_id}"
        else:
            callback = f"account_show|{account_id}"

        keyboard_rows.append([InlineKeyboardButton(text=label, callback_data=callback)])

    if kind == "account":
        keyboard_rows.append(
            [InlineKeyboardButton(text="Все аккаунты", callback_data="account_show|ALL")]
        )
    if kind == "workout":
        keyboard_rows.append(
            [InlineKeyboardButton(text="Все аккаунты", callback_data="workout_select|ALL")]
        )
        text = "📤 Выберите аккаунт для загрузки тренировки:"
    else:
        text = "📊 Выберите аккаунт для просмотра данных:"
    keyboard_rows.append([InlineKeyboardButton(text="↩️ В меню", callback_data="menu|start")])
    markup = InlineKeyboardMarkup(keyboard_rows)
    if query:
        await query.edit_message_text(text, reply_markup=markup)
    elif message:
        await message.reply_text(text, reply_markup=markup)


async def show_account_via_callback(query, account_id: str) -> None:
    account = _resolve_account_identifier(account_id)
    if account is None:
        await query.edit_message_text("⚠️ Аккаунт не найден.")
        return

    try:
        profile, auth_user = await asyncio.to_thread(fetch_account_information, account)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch account info for %s", account)
        await query.edit_message_text(f"❌ Ошибка получения данных: {exc}")
        return

    text = format_account_details(account, profile, auth_user)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text="← Выбор аккаунта",
                        callback_data="select_accounts|account",
                    )
                ]
            ]
        ),
    )


async def show_all_accounts_via_callback(query) -> None:
    if not _account_registry:
        await query.edit_message_text("⚠️ Аккаунты не настроены.")
        return

    summaries: List[str] = []
    for account_id in sorted(_account_registry):
        account = _account_registry[account_id]
        try:
            profile, auth_user = await asyncio.to_thread(fetch_account_information, account_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch account info for %s", account_id)
            summaries.append(
                f"<b>👤 {account.name}</b> ({account_id})\n"
                f"⚠️ Ошибка получения данных: {html.escape(str(exc))}"
            )
            continue
        summaries.append(format_account_details(account_id, profile, auth_user))

    text = "\n\n".join(summaries) if summaries else "⚠️ Аккаунты не настроены."
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton(text="← Выбор аккаунта", callback_data="select_accounts|account")]]
        ),
    )


async def account_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    if not context.args:
        await show_account_selection(message=update.message, kind="account")
        return

    raw_account_id = context.args[0]
    account_id = _resolve_account_identifier(raw_account_id)
    if account_id is None:
        await show_account_selection(message=update.message, kind="account")
        return

    try:
        profile, auth_user = await asyncio.to_thread(fetch_account_information, account_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch account info for %s", account_id)
        await update.message.reply_text(f"⚠️ Ошибка получения данных: {exc}")
        return

    text = format_account_details(account_id, profile, auth_user)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def handle_account_callback(action: str, parts: List[str], query, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if action == "select_accounts" and len(parts) >= 2:
        kind = parts[1]
        await show_account_selection(query=query, kind=kind)
        return True
    if action == "account_show" and len(parts) >= 2:
        target = parts[1]
        if target.upper() == "ALL":
            await show_all_accounts_via_callback(query)
        else:
            await show_account_via_callback(query, target)
        return True
    return False
