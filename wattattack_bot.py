#!/usr/bin/env python3
"""Telegram bot that exposes WattAttack activities for multiple accounts."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from client_repository import (
    count_clients,
    get_client,
    list_clients,
    search_clients,
    update_client_fields,
)
from admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    list_admins as db_list_admins,
    add_admin as db_add_admin,
    remove_admin as db_remove_admin,
    is_admin as db_is_admin,
)
from bikes_repository import (
    ensure_bikes_table,
    list_bikes,
    search_bikes,
    bikes_count,
    find_bikes_for_height,
    get_bike,
    update_bike_fields,
)
from trainers_repository import (
    ensure_trainers_table,
    list_trainers,
    search_trainers,
    trainers_count,
    get_trainer,
    update_trainer_fields,
)
from load_clients import load_clients_from_csv_bytes
from load_bikes import load_bikes_from_csv_bytes
from load_trainers import load_trainers_from_csv_bytes
from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_RECENT_LIMIT = int(os.environ.get("WATTATTACK_RECENT_LIMIT", "5"))
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
CLIENTS_PAGE_SIZE = int(os.environ.get("CLIENTS_PAGE_SIZE", "6"))
DEFAULT_CLIENT_FTP = int(os.environ.get("WATTATTACK_DEFAULT_FTP", "150"))

PENDING_UPLOAD_KEY = "pending_inventory_upload"
PENDING_TRAINER_EDIT_KEY = "pending_trainer_edit"
PENDING_BIKE_EDIT_KEY = "pending_bike_edit"
UPLOAD_COMMAND_TYPES = {
    "/uploadclients": "clients",
    "/uploadbikes": "bikes",
    "/uploadstands": "stands",
}


@dataclass(frozen=True)
class AccountConfig:
    identifier: str
    name: str
    email: str
    password: str
    base_url: str = DEFAULT_BASE_URL


ACCOUNT_REGISTRY: Dict[str, AccountConfig] = {}


def normalize_account_id(value: str) -> str:
    return value.replace("_", "").lower()


def resolve_account_identifier(raw_id: str) -> Optional[str]:
    if raw_id in ACCOUNT_REGISTRY:
        return raw_id
    target = normalize_account_id(raw_id)
    for account_id in ACCOUNT_REGISTRY:
        if normalize_account_id(account_id) == target:
            return account_id
    return None


def format_account_list() -> str:
    lines: List[str] = []
    for key in sorted(ACCOUNT_REGISTRY):
        alias = normalize_account_id(key)
        account_name = ACCOUNT_REGISTRY[key].name
        lines.append(f"{alias} ({key}) — {account_name}")
    return "\n".join(lines)


def format_admin_list(admins: List[Dict[str, Any]]) -> str:
    if not admins:
        return "Администраторы не настроены."
    lines = [format_admin_record(admin) for admin in admins]
    return "\n".join(lines)


def format_admin_record(record: Dict[str, Any]) -> str:
    display_name = record.get("display_name")
    username = record.get("username")
    tg_id = record.get("tg_id")

    parts: List[str] = []
    if display_name:
        parts.append(str(display_name))
    if username:
        handle = username if username.startswith("@") else f"@{username}"
        parts.append(handle)
    if tg_id:
        parts.append(f"id={tg_id}")
    return " ".join(parts) if parts else f"id={tg_id}" if tg_id else str(record.get("id"))


def _set_pending_upload(user_data: Dict, upload_type: str, truncate: bool) -> None:
    user_data[PENDING_UPLOAD_KEY] = {"type": upload_type, "truncate": truncate}


def _pop_pending_upload(user_data: Dict) -> Optional[Dict[str, Any]]:
    value = user_data.get(PENDING_UPLOAD_KEY)
    if value is not None:
        user_data.pop(PENDING_UPLOAD_KEY, None)
    return value


def _normalize_tokens(value: str) -> List[str]:
    tokens = [token for token in re.split(r"[,\s/;]+", value.strip()) if token]
    return tokens


def _format_trainer_code(code: Optional[str]) -> str:
    if not code:
        return ""
    value = str(code).strip()
    match = re.match(r"^([^\d]*)(\d+)(.*)$", value)
    if not match:
        return value
    prefix, digits, suffix = match.groups()
    padded = digits.zfill(2)
    return f"{prefix}{padded}{suffix}"


def _parse_axle_types(value: Any) -> set[str]:
    if not value:
        return set()
    if isinstance(value, str):
        return {token.upper() for token in _normalize_tokens(value)}
    return set()


def _parse_cassette_values(value: Any) -> set[int]:
    if not value:
        return set()
    if isinstance(value, (int, float)):
        return {int(round(float(value)))}
    tokens = re.split(r"[^\d]+", str(value))
    cassette_values = set()
    for token in tokens:
        if not token:
            continue
        try:
            cassette_values.add(int(token))
        except ValueError:
            continue
    return cassette_values


def _load_trainer_inventory() -> List[Dict[str, Any]]:
    ensure_trainers_table()
    return list_trainers()


def _is_trainer_compatible(bike: Dict[str, Any], trainer: Dict[str, Any]) -> bool:
    bike_axles = _parse_axle_types(bike.get("axle_type"))
    trainer_axles = _parse_axle_types(trainer.get("axle_types"))
    if bike_axles and trainer_axles and not (bike_axles & trainer_axles):
        return False
    if bike_axles and not trainer_axles:
        # Trainer axle types unknown, assume compatible
        pass

    bike_cassettes = _parse_cassette_values(bike.get("cassette"))
    trainer_cassettes = _parse_cassette_values(trainer.get("cassette"))
    if bike_cassettes:
        if trainer_cassettes and not (bike_cassettes & trainer_cassettes):
            return False
        # If trainer cassette unknown, assume compatibility
    return True


def _build_trainer_suggestions(
    bikes: List[Dict[str, Any]], trainers: List[Dict[str, Any]]
) -> Dict[int, List[Dict[str, Any]]]:
    suggestions: Dict[int, List[Dict[str, Any]]] = {}
    for bike in bikes:
        bike_id = bike.get("id")
        if not isinstance(bike_id, int):
            continue
        bike_axles = _parse_axle_types(bike.get("axle_type"))
        matches: List[tuple] = []
        for trainer in trainers:
            if not _is_trainer_compatible(bike, trainer):
                continue
            trainer_axles = _parse_axle_types(trainer.get("axle_types"))
            shared_axles = bike_axles & trainer_axles if bike_axles and trainer_axles else set()
            matches.append(
                (
                    trainer,
                    -len(shared_axles),  # More shared axles first
                    0 if trainer_axles else 1,  # Known axle types preferred
                    trainer.get("code") or "",
                )
            )
        matches.sort(key=lambda item: (item[1], item[2], item[3]))
        suggestions[bike_id] = [item[0] for item in matches]
    return suggestions


def _format_decimal_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return format(value.normalize(), "f").rstrip("0").rstrip(".")
    if isinstance(value, (int, float)):
        number = float(value)
        if number.is_integer():
            return str(int(number))
        return f"{number:.1f}".rstrip("0").rstrip(".")
    try:
        number = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.1f}".rstrip("0").rstrip(".")


def _parse_height_cm(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _load_bike_suggestions(height_cm: float, limit: int) -> List[Dict[str, Any]]:
    ensure_bikes_table()
    return find_bikes_for_height(height_cm, limit)


async def get_bike_suggestions_for_client(
    client_record: Dict[str, Any], limit: int = 5
) -> Tuple[List[Dict[str, Any]], Optional[float], List[Dict[str, Any]]]:
    height_cm = _parse_height_cm(client_record.get("height"))
    if height_cm is None:
        return [], None, []
    try:
        bikes = await asyncio.to_thread(_load_bike_suggestions, height_cm, limit)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "Failed to load bikes for client %s (height=%s): %s",
            client_record.get("id"),
            height_cm,
            exc,
        )
        return [], height_cm, []

    trainers: List[Dict[str, Any]] = []
    if bikes:
        try:
            trainers = await asyncio.to_thread(_load_trainer_inventory)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to load trainers inventory: %s", exc)
            trainers = []
    return bikes, height_cm, trainers


def format_bike_record(record: Dict[str, Any]) -> str:
    title = html.escape(record.get("title") or "Без названия")
    lines = [f"🚲 <b>{title}</b>"]

    owner = record.get("owner")
    if owner:
        lines.append(f"• Владелец: {html.escape(str(owner))}")

    size_label = (record.get("size_label") or "").strip()
    frame_size = (record.get("frame_size_cm") or "").strip()
    if size_label and frame_size:
        numeric = frame_size.replace(" ", "").replace(",", ".")
        suffix = " см" if numeric.replace(".", "", 1).isdigit() else ""
        lines.append(
            f"• Размер: {html.escape(size_label)} (труба {html.escape(frame_size)}{suffix})"
        )
    elif size_label:
        lines.append(f"• Размер: {html.escape(size_label)}")
    elif frame_size:
        numeric = frame_size.replace(" ", "").replace(",", ".")
        suffix = " см" if numeric.replace(".", "", 1).isdigit() else ""
        lines.append(f"• Труба: {html.escape(frame_size)}{suffix}")

    height_min = _format_decimal_value(record.get("height_min_cm"))
    height_max = _format_decimal_value(record.get("height_max_cm"))
    if height_min and height_max:
        lines.append(f"• Рост: {height_min}–{height_max} см")
    elif height_min:
        lines.append(f"• Рост от {height_min} см")
    elif height_max:
        lines.append(f"• Рост до {height_max} см")

    technical_parts: List[str] = []
    gears = record.get("gears")
    if gears:
        technical_parts.append(f"Передачи: {html.escape(str(gears))}")
    axle = record.get("axle_type")
    if axle:
        technical_parts.append(f"Ось: {html.escape(str(axle))}")
    cassette = record.get("cassette")
    if cassette:
        technical_parts.append(f"Кассета: {html.escape(str(cassette))}")
    if technical_parts:
        lines.append(f"• {'; '.join(technical_parts)}")

    return "\n".join(lines)


def format_bike_suggestion(
    record: Dict[str, Any],
    trainers: Optional[List[Dict[str, Any]]] = None,
) -> str:
    title = html.escape(record.get("title") or "Без названия")
    details: List[str] = []

    size_label = (record.get("size_label") or "").strip()
    frame_size = (record.get("frame_size_cm") or "").strip()
    size_parts: List[str] = []
    if size_label:
        size_parts.append(html.escape(size_label))
    if frame_size:
        numeric = frame_size.replace(" ", "").replace(",", ".")
        suffix = " см" if numeric.replace(".", "", 1).isdigit() else ""
        size_parts.append(f"{html.escape(frame_size)}{suffix}")
    if size_parts:
        details.append(" / ".join(size_parts))

    height_min = _format_decimal_value(record.get("height_min_cm"))
    height_max = _format_decimal_value(record.get("height_max_cm"))
    if height_min and height_max:
        details.append(f"{height_min}–{height_max} см")
    elif height_min:
        details.append(f"от {height_min} см")
    elif height_max:
        details.append(f"до {height_max} см")

    axle = record.get("axle_type")
    if axle:
        details.append(f"ось {html.escape(str(axle))}")
    cassette = record.get("cassette")
    if cassette:
        details.append(f"кассета {html.escape(str(cassette))}")
    gears = record.get("gears")
    if gears:
        details.append(f"{html.escape(str(gears))} передач")

    descriptor = "; ".join(details)
    lines = [f"• <b>{title}</b>" + (f" — {descriptor}" if descriptor else "")]

    if trainers is not None:
        if trainers:
            lines.append("    ↳ Станки:")
            for trainer in trainers:
                lines.append(f"        • {format_trainer_summary(trainer)}")
        else:
            lines.append("    ↳ Станки: нет совместимых станков")

    return "\n".join(lines)


def format_bike_button_label(record: Dict[str, Any]) -> str:
    title = (record.get("title") or "Без названия").strip()
    size_label = (record.get("size_label") or "").strip()
    heights = []
    for key in ("height_min_cm", "height_max_cm"):
        value = record.get(key)
        formatted = _format_decimal_value(value)
        if formatted:
            heights.append(formatted)
    height_part = "-".join(heights) + " см" if heights else ""
    parts = [title]
    if size_label:
        parts.append(size_label)
    if height_part:
        parts.append(height_part)
    return " · ".join(part for part in parts if part)


def format_bike_details(
    record: Dict[str, Any],
    trainers: Optional[List[Dict[str, Any]]] = None,
) -> str:
    title = html.escape(record.get("title") or "Без названия")
    lines = [f"🚲 <b>{title}</b>"]

    owner = record.get("owner")
    if owner:
        lines.append(f"• Владелец: {html.escape(str(owner))}")

    size_label = (record.get("size_label") or "").strip()
    frame_size = (record.get("frame_size_cm") or "").strip()
    if size_label:
        lines.append(f"• Размер: {html.escape(size_label)}")
    if frame_size:
        numeric = frame_size.replace(" ", "").replace(",", ".")
        suffix = " см" if numeric.replace(".", "", 1).isdigit() else ""
        lines.append(f"• Труба: {html.escape(frame_size)}{suffix}")

    height_min = _format_decimal_value(record.get("height_min_cm"))
    height_max = _format_decimal_value(record.get("height_max_cm"))
    if height_min and height_max:
        lines.append(f"• Рост: {height_min}–{height_max} см")
    elif height_min:
        lines.append(f"• Рост от {height_min} см")
    elif height_max:
        lines.append(f"• Рост до {height_max} см")

    axle = record.get("axle_type")
    if axle:
        lines.append(f"• Ось: {html.escape(str(axle))}")
    cassette = record.get("cassette")
    if cassette:
        lines.append(f"• Кассета: {html.escape(str(cassette))}")
    gears = record.get("gears")
    if gears:
        lines.append(f"• Передачи: {html.escape(str(gears))}")

    if trainers is not None:
        if trainers:
            lines.append("")
            lines.append("🛠 Совместимые станки:")
            for trainer in trainers:
                lines.append(f"• {format_trainer_summary(trainer)}")
        else:
            lines.append("")
            lines.append("🛠 Совместимые станки: нет записей")

    return "\n".join(lines)


def build_bike_info_markup(bike_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="📏 Рост от",
                    callback_data=f"bike_edit|height_min_cm|{bike_id}",
                ),
                InlineKeyboardButton(
                    text="📏 Рост до",
                    callback_data=f"bike_edit|height_max_cm|{bike_id}",
                ),
            ],
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="noop")],
        ]
    )


def build_bike_edit_markup(bike_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="↩️ Назад",
                    callback_data=f"bike_info|{bike_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data=f"bike_edit_cancel|{bike_id}",
                ),
            ]
        ]
    )


def format_trainer_record(record: Dict[str, Any]) -> str:
    code = _format_trainer_code(record.get("code"))
    title = record.get("title") or ""
    display = record.get("display_name") or ""

    header_parts = [part for part in [code.strip(), title.strip()] if part]
    if not header_parts and display:
        header_parts.append(display.strip())
    header = " — ".join(header_parts) if header_parts else (display.strip() or "Без названия")

    lines = [f"🛠 <b>{html.escape(header)}</b>"]

    if display:
        lines.append(f"• Отображается как: {html.escape(display)}")

    owner = record.get("owner")
    if owner:
        lines.append(f"• Владелец: {html.escape(str(owner))}")

    axle_types = record.get("axle_types")
    if axle_types:
        lines.append(f"• Оси: {html.escape(str(axle_types))}")

    cassette = record.get("cassette")
    if cassette:
        lines.append(f"• Кассета: {html.escape(str(cassette))}")

    notes = record.get("notes")
    if notes:
        lines.append(f"• Комментарий: {html.escape(str(notes))}")

    return "\n".join(lines)


def format_trainer_button_label(record: Dict[str, Any]) -> str:
    code = _format_trainer_code(record.get("code"))
    display = (record.get("display_name") or record.get("title") or "").strip()
    if code and display and display.lower() != code.lower():
        return f"{code} · {display}"
    return code or display or f"id={record.get('id')}"


def format_trainer_summary(record: Dict[str, Any]) -> str:
    code_raw = record.get("code")
    code_display = _format_trainer_code(code_raw) if code_raw else None
    name_raw = (
        record.get("display_name")
        or record.get("title")
        or code_display
        or "Без названия"
    )
    details: List[str] = []
    axle_types = record.get("axle_types")
    if axle_types:
        details.append(f"оси {html.escape(str(axle_types))}")
    cassette = record.get("cassette")
    if cassette:
        details.append(f"кассета {html.escape(str(cassette))}")
    code_part = html.escape(code_display) if code_display else ""
    name_part = html.escape(str(name_raw))

    if code_display:
        if name_raw and isinstance(name_raw, str) and name_raw.strip().lower() != code_display.strip().lower():
            base = f"{code_part} — {name_part}"
        else:
            base = code_part
    else:
        base = name_part

    if details:
        return f"{base} ({'; '.join(details)})"
    return base


def trainer_display_name(record: Dict[str, Any]) -> str:
    display = (record.get("display_name") or "").strip()
    if display:
        return display
    code = _format_trainer_code(record.get("code"))
    if code:
        return code
    title = (record.get("title") or "").strip()
    if title:
        return title
    return f"id={record.get('id')}"


def format_trainer_details(record: Dict[str, Any]) -> str:
    code = _format_trainer_code(record.get("code"))
    title = (record.get("title") or "").strip()
    display = (record.get("display_name") or "").strip()

    header_parts: List[str] = []
    if code:
        header_parts.append(code)
    if title and title.lower() != code.lower():
        header_parts.append(title)
    header = " — ".join(header_parts) if header_parts else (display or "Без названия")

    lines = [f"🛠 <b>{html.escape(header)}</b>"]
    if display:
        lines.append(f"• Отображается как: {html.escape(display)}")
    owner = record.get("owner")
    if owner:
        lines.append(f"• Владелец: {html.escape(str(owner))}")
    axle = record.get("axle_types")
    if axle:
        lines.append(f"• Оси: {html.escape(str(axle))}")
    cassette = record.get("cassette")
    if cassette:
        lines.append(f"• Кассета: {html.escape(str(cassette))}")
    notes = record.get("notes")
    if notes:
        lines.append(f"• Комментарий: {html.escape(str(notes))}")
    return "\n".join(lines)


def build_trainer_info_markup(trainer_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="🔧 Оси",
                    callback_data=f"trainer_edit|axle_types|{trainer_id}",
                ),
                InlineKeyboardButton(
                    text="⚙️ Кассета",
                    callback_data=f"trainer_edit|cassette|{trainer_id}",
                ),
            ],
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="noop")],
        ]
    )


def build_trainer_edit_markup(trainer_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="↩️ Назад",
                    callback_data=f"trainer_info|{trainer_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data=f"trainer_edit_cancel|{trainer_id}",
                ),
            ]
        ]
    )


def parse_admin_identifier(value: str) -> Tuple[Optional[int], Optional[str]]:
    value = value.strip()
    if not value:
        return None, None
    if value.startswith("@"):
        value = value[1:]
    if value.isdigit():
        return int(value), None
    return None, value


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


async def process_clients_document(
    document, message: Message, truncate: bool = False
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download CSV file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        inserted, updated = await asyncio.to_thread(
            load_clients_from_csv_bytes, bytes(data), truncate
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import clients")
        await message.reply_text(f"❌ Ошибка импорта: {exc}")
        return

    await message.reply_text(
        "✅ Импорт завершён. Добавлено: {0}, обновлено: {1}.".format(inserted, updated)
    )


async def process_bikes_document(
    document, message: Message, truncate: bool = False
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download bikes CSV file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        inserted, updated = await asyncio.to_thread(
            load_bikes_from_csv_bytes,
            bytes(data),
            truncate,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import bikes")
        await message.reply_text(f"❌ Ошибка импорта велосипедов: {exc}")
        return

    await message.reply_text(
        "✅ Велосипеды импортированы. Добавлено: {0}, обновлено: {1}.".format(
            inserted, updated
        )
    )


async def process_trainers_document(
    document, message: Message, truncate: bool = False
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download trainers CSV file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        inserted, updated = await asyncio.to_thread(
            load_trainers_from_csv_bytes,
            bytes(data),
            truncate,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import trainers")
        await message.reply_text(f"❌ Ошибка импорта станков: {exc}")
        return

    await message.reply_text(
        "✅ Станки импортированы. Добавлено: {0}, обновлено: {1}.".format(
            inserted, updated
        )
    )


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
    if not ensure_admin_message(update):
        return
    await update.message.reply_text(
        "📋 Выберите аккаунт WattAttack:",
        reply_markup=build_accounts_keyboard(DEFAULT_RECENT_LIMIT),
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    message = (
        "ℹ️ Использование:\n"
        "/start — показать список аккаунтов\n"
        "/recent <число> — предложить последние N активностей выбранного аккаунта\n"
        "/latest — скачать последнюю активность по каждому аккаунту\n"
        "/setclient <аккаунт> — применить данные клиента из базы\n"
        "/account <аккаунт> — показать текущие данные аккаунта\n"
        "/bikes [поиск] — показать доступные велосипеды\n"
        "/stands [поиск] — показать доступные станки\n"
        "/client <имя/фамилия> — найти клиента по БД\n"
        "/uploadclients [truncate] — загрузить CSV клиентов\n"
        "/uploadbikes [truncate] — загрузить CSV велосипедов\n"
        "/uploadstands [truncate] — загрузить CSV станков\n"
        "/admins — показать список администраторов\n"
        "/addadmin <id|@user> — добавить администратора (можно ответом на сообщение)\n"
        "/removeadmin <id|@user> — удалить администратора"
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

    await update.message.reply_text("⏳ Собираю последние активности по аккаунтам...")

    cache = context.user_data.setdefault("account_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        context.user_data["account_cache"] = cache

    for account_id, account in ACCOUNT_REGISTRY.items():
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


async def admins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    try:
        admins = await asyncio.to_thread(db_list_admins)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load admins")
        await update.message.reply_text(f"❌ Ошибка получения списка администраторов: {exc}")
        return

    message = (
        "👥 Администраторы:\n" + format_admin_list(admins)
        if admins
        else "⚠️ Администраторы не настроены."
    )
    await update.message.reply_text(message)


async def addadmin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    identifier: Optional[str] = None
    display_name: Optional[str] = None
    target_user = None

    if context.args:
        identifier = context.args[0]
        if len(context.args) > 1:
            display_name = " ".join(context.args[1:])

    if not identifier and update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        identifier = str(target_user.id)
        display_name = display_name or target_user.full_name

    if not identifier:
        await update.message.reply_text(
            "ℹ️ Укажите ID или @username (можно ответить на сообщение пользователя)."
        )
        return

    tg_id, username = parse_admin_identifier(identifier)

    if target_user is not None:
        tg_id = target_user.id
        username = target_user.username
        display_name = display_name or target_user.full_name

    try:
        created, record = await asyncio.to_thread(
            db_add_admin,
            tg_id=tg_id,
            username=username,
            display_name=display_name,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to add admin")
        await update.message.reply_text(f"❌ Ошибка добавления администратора: {exc}")
        return

    status = "Добавлен" if created else "Обновлён"
    summary = format_admin_record(record)
    await update.message.reply_text(f"✅ {status} администратор: {summary}")


async def removeadmin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    identifier: Optional[str] = None
    target_user = None

    if context.args:
        identifier = context.args[0]

    if not identifier and update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        identifier = str(target_user.id)

    if not identifier:
        await update.message.reply_text(
            "ℹ️ Укажите ID или @username (можно ответить на сообщение администратора)."
        )
        return

    tg_id, username = parse_admin_identifier(identifier)
    if target_user is not None:
        tg_id = target_user.id
        username = target_user.username

    if tg_id is None and (username is None or not username):
        await update.message.reply_text("⚠️ Некорректный идентификатор администратора.")
        return

    try:
        removed = await asyncio.to_thread(
            db_remove_admin,
            tg_id=tg_id,
            username=username,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to remove admin")
        await update.message.reply_text(f"❌ Ошибка удаления администратора: {exc}")
        return

    if removed:
        await update.message.reply_text("🗑️ Администратор удалён.")
    else:
        await update.message.reply_text("🔍 Администратор не найден.")


async def uploadclients_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    truncate = False
    if context.args:
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in context.args)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_clients_document(
            update.message.reply_to_message.document,
            update.message,
            truncate=truncate,
        )
        return

    _set_pending_upload(context.user_data, "clients", truncate)
    await update.message.reply_text(
        "📄 Пришлите CSV файл (как документ). Можно указать /uploadclients truncate для полной перезагрузки."
    )


async def uploadbikes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    truncate = False
    if context.args:
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in context.args)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_bikes_document(
            update.message.reply_to_message.document,
            update.message,
            truncate=truncate,
        )
        return

    _set_pending_upload(context.user_data, "bikes", truncate)
    await update.message.reply_text(
        "📄 Пришлите CSV файл (как документ). Можно указать /uploadbikes truncate для полной перезагрузки."
    )


async def uploadstands_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    truncate = False
    if context.args:
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in context.args)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_trainers_document(
            update.message.reply_to_message.document,
            update.message,
            truncate=truncate,
        )
        return

    _set_pending_upload(context.user_data, "stands", truncate)
    await update.message.reply_text(
        "📄 Пришлите CSV файл (как документ). Можно указать /uploadstands truncate для полной перезагрузки."
    )


async def setclient_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    if not context.args:
        await show_account_selection(message=update.message)
        return

    raw_account_id = context.args[0]
    account_id = resolve_account_identifier(raw_account_id)
    if account_id is None:
        account_list = format_account_list()
        await update.message.reply_text(
            f"⚠️ Аккаунт {raw_account_id} не найден. Доступные аккаунты:\n{account_list}"
        )
        return

    await show_client_page(account_id, page=0, message=update.message)


async def account_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    if not context.args:
        await show_account_selection(message=update.message, kind="account")
        return

    raw_account_id = context.args[0]
    account_id = resolve_account_identifier(raw_account_id)
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


async def client_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    if not context.args:
        await update.message.reply_text(
            "ℹ️ Укажите имя или фамилию, например: /client Иван"
        )
        return

    term = " ".join(context.args).strip()
    await process_client_search(update.message, term)


async def bikes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    search_term = " ".join(context.args).strip() if context.args else ""

    try:
        await asyncio.to_thread(ensure_bikes_table)
        if search_term:
            bikes = await asyncio.to_thread(search_bikes, search_term, 30)
            total_count = len(bikes)
        else:
            bikes = await asyncio.to_thread(list_bikes, 50)
            total_count = await asyncio.to_thread(bikes_count)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch bikes")
        await update.message.reply_text(
            f"❌ Ошибка получения списка велосипедов: {exc}"
        )
        return

    if not bikes:
        if search_term:
            await update.message.reply_text(
                f"🚫 Велосипеды по запросу «{search_term}» не найдены."
            )
        else:
            await update.message.reply_text("🚫 В базе нет доступных велосипедов.")
        return

    header_lines: List[str] = []
    if search_term:
        header_lines.append(
            f"🔍 Найдено {total_count} велосипедов по запросу «{html.escape(search_term)}»."
        )
    else:
        header_lines.append(f"🚲 В базе велосипедов: {total_count}.")
        if total_count > len(bikes):
            header_lines.append(f"Показаны первые {len(bikes)} записей.")
        header_lines.append("Используйте /bikes &lt;поиск&gt; для фильтрации.")

    body = "\n\n".join(format_bike_record(record) for record in bikes)
    text = "\n\n".join(header_lines + [body])
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def stands_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)

    search_term = " ".join(context.args).strip() if context.args else ""

    try:
        await asyncio.to_thread(ensure_trainers_table)
        if search_term:
            trainers = await asyncio.to_thread(search_trainers, search_term, 30)
            total_count = len(trainers)
        else:
            trainers = await asyncio.to_thread(list_trainers, 50)
            total_count = await asyncio.to_thread(trainers_count)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch trainers")
        await update.message.reply_text(
            f"❌ Ошибка получения списка станков: {exc}"
        )
        return

    if not trainers:
        if search_term:
            await update.message.reply_text(
                f"🚫 Станки по запросу «{search_term}» не найдены."
            )
        else:
            await update.message.reply_text("🚫 В базе нет станков.")
        return

    header_lines: List[str] = []
    if search_term:
        header_lines.append(
            f"🔍 Найдено {total_count} станков по запросу «{html.escape(search_term)}»."
        )
    else:
        header_lines.append(f"🛠 Всего станков: {total_count}.")
        if total_count > len(trainers):
            header_lines.append(f"Показаны первые {len(trainers)} записей.")
        header_lines.append("Используйте /stands &lt;поиск&gt; для фильтрации.")

    body = "\n\n".join(format_trainer_record(record) for record in trainers)
    text = "\n\n".join(header_lines + [body, "Выберите станок для настройки."])

    keyboard_rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=format_trainer_button_label(record),
                callback_data=f"trainer_info|{record['id']}",
            )
        ]
        for record in trainers
    ]
    keyboard_rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="noop")])

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
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
    elif action == "setclient" and len(parts) >= 3:
        account_id = parts[1]
        try:
            client_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await assign_client_to_account(query, context, account_id, client_id)
    elif action == "setclient_page" and len(parts) >= 3:
        account_id = parts[1]
        try:
            page = max(0, int(parts[2]))
        except ValueError:
            page = 0
        await show_client_page(account_id, page, query=query)
    elif action == "select_accounts" and len(parts) >= 2:
        kind = parts[1]
        await show_account_selection(query=query, kind=kind)
    elif action == "account_show" and len(parts) >= 2:
        account_id = parts[1]
        await show_account_via_callback(query, account_id)
    elif action == "client_info" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await show_client_info(query, context, client_id)
    elif action == "client_edit" and len(parts) >= 3:
        field = parts[1]
        try:
            client_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await start_client_edit(query, context, client_id, field)
    elif action == "client_edit_cancel" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await cancel_client_edit(query, context, client_id)
    elif action == "client_bikes" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await render_client_bike_suggestions(
            context,
            query.message.chat_id,
            query.message.message_id,
            client_id,
        )
    elif action == "trainer_info" and len(parts) >= 2:
        try:
            trainer_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор станка.")
            return
        await show_trainer_info(query, context, trainer_id)
    elif action == "trainer_edit" and len(parts) >= 3:
        field = parts[1]
        try:
            trainer_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор станка.")
            return
        await start_trainer_edit(query, context, trainer_id, field)
    elif action == "trainer_edit_cancel" and len(parts) >= 2:
        try:
            trainer_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор станка.")
            return
        await cancel_trainer_edit(query, context, trainer_id)
    elif action == "noop":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:  # noqa: BLE001
            LOGGER.debug("noop action ignored for message %s", query.message)
        return
    else:
        await query.edit_message_text("❓ Неизвестное действие.")


async def send_recent_activities(query, context, account_id: str, limit: int) -> None:
    if account_id not in ACCOUNT_REGISTRY:
        await query.edit_message_text("⚠️ Аккаунт не найден.")
        return

    account = ACCOUNT_REGISTRY[account_id]

    try:
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

    lines = []
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


def split_full_name(full_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if not parts:
        return None, None
    first = parts[0]
    last = " ".join(parts[1:]) or None
    return first, last


def apply_client_profile(account_id: str, client_record: Dict[str, Any]) -> None:
    account = ACCOUNT_REGISTRY[account_id]
    client = WattAttackClient(account.base_url)
    client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)

    existing_profile: Dict[str, Any] = {}
    try:
        existing_profile = client.fetch_profile(timeout=DEFAULT_TIMEOUT)
        if not isinstance(existing_profile, dict):
            existing_profile = {}
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch current profile for %s: %s", account_id, exc)
        existing_profile = {}

    athlete_section = existing_profile.get("athlete") if isinstance(existing_profile, dict) else {}
    if not isinstance(athlete_section, dict):
        athlete_section = {}

    first = client_record.get("first_name") or None
    last = client_record.get("last_name") or None
    if not first and not last:
        first, last = split_full_name(client_record.get("full_name", ""))

    user_payload: Dict[str, Any] = {}
    if first:
        user_payload["firstName"] = str(first)
    if last:
        user_payload["lastName"] = str(last)

    profile_payload: Dict[str, Any] = {}
    weight = client_record.get("weight")
    height = client_record.get("height")
    ftp = client_record.get("ftp")
    gender_value = client_record.get("gender")

    if weight is not None:
        try:
            profile_payload["weight"] = float(weight)
        except (TypeError, ValueError):
            pass
    if height is not None:
        try:
            profile_payload["height"] = float(height)
        except (TypeError, ValueError):
            pass
    ftp_value = ftp
    if ftp_value is None:
        ftp_value = DEFAULT_CLIENT_FTP
    elif isinstance(ftp_value, str):
        ftp_value = ftp_value.strip()
        if not ftp_value:
            ftp_value = DEFAULT_CLIENT_FTP
    try:
        profile_payload["ftp"] = int(float(ftp_value))
    except (TypeError, ValueError):
        profile_payload["ftp"] = DEFAULT_CLIENT_FTP
    if gender_value:
        gender_norm = str(gender_value).strip().lower()
        if gender_norm in {"m", "male", "м", "муж", "мужской"}:
            profile_payload["gender"] = "male"
        elif gender_norm in {"f", "female", "ж", "жен", "женский"}:
            profile_payload["gender"] = "female"

    # Preserve existing required fields to avoid validation errors
    if "birthDate" not in profile_payload and athlete_section.get("birthDate"):
        profile_payload["birthDate"] = athlete_section.get("birthDate")
    if "gender" not in profile_payload and athlete_section.get("gender"):
        profile_payload["gender"] = athlete_section.get("gender")
    if not profile_payload.get("birthDate"):
        profile_payload["birthDate"] = "2000-01-01"

    if user_payload:
        LOGGER.info("Updating user %s with payload: %s", account_id, user_payload)
        client.update_user(user_payload, timeout=DEFAULT_TIMEOUT)
        LOGGER.info("User update for %s completed", account_id)
    if profile_payload:
        LOGGER.info("Updating athlete %s with payload: %s", account_id, profile_payload)
        response = client.update_profile(profile_payload, timeout=DEFAULT_TIMEOUT)
        LOGGER.info("Athlete update for %s response: %s", account_id, response)


def format_client_summary(client_record: Dict[str, Any]) -> str:
    full_name = client_record.get("full_name")
    first_name = client_record.get("first_name")
    last_name = client_record.get("last_name")
    if first_name or last_name:
        header = " ".join(part for part in [first_name, last_name] if part).strip()
    else:
        header = full_name or ""
    header = header or "Без имени"
    lines = [f"<b>👤 {header}</b>"]
    gender_value = client_record.get("gender")
    if gender_value:
        gender_norm = str(gender_value).strip().lower()
        if gender_norm.startswith("m"):
            lines.append(f"🚹 Пол: М ({gender_value})")
        elif gender_norm.startswith("f"):
            lines.append(f"🚺 Пол: Ж ({gender_value})")
        else:
            lines.append(f"🚻 Пол: {gender_value}")
    weight = client_record.get("weight")
    if weight is not None:
        try:
            lines.append(f"⚖️ Вес: {float(weight):g} кг")
        except (TypeError, ValueError):
            pass
    height = client_record.get("height")
    if height is not None:
        try:
            lines.append(f"📏 Рост: {float(height):g} см")
        except (TypeError, ValueError):
            pass
    ftp = client_record.get("ftp")
    if ftp is not None:
        try:
            lines.append(f"⚡ FTP: {int(float(ftp))} Вт")
        except (TypeError, ValueError):
            pass
    if client_record.get("goal"):
        lines.append(f"🎯 Цель: {client_record['goal']}")
    return "\n".join(lines)


def format_client_button_label(client_record: Dict[str, Any]) -> str:
    first_name = client_record.get("first_name") or ""
    last_name = client_record.get("last_name") or ""
    full_name = (first_name + " " + last_name).strip() or client_record.get("full_name") or "Без имени"
    ftp = client_record.get("ftp")
    if ftp:
        try:
            return f"{full_name} (FTP {int(float(ftp))})"
        except (TypeError, ValueError):
            pass
    return full_name


def format_client_details(
    client_record: Dict[str, Any],
    bike_suggestions: Optional[List[Dict[str, Any]]] = None,
    height_cm: Optional[float] = None,
    trainer_suggestions: Optional[Dict[int, List[Dict[str, Any]]]] = None,
) -> str:
    summary = format_client_summary(client_record)
    lines = [summary]
    has_goal_in_summary = any("Цель:" in line for line in summary.splitlines())

    pedals = client_record.get("pedals")
    if pedals:
        lines.append(f"🚴‍♂️ Педали: {pedals}")
    goal = client_record.get("goal")
    if goal and not has_goal_in_summary:
        lines.append(f"🎯 Цель: {goal}")
    saddle = client_record.get("saddle_height")
    if saddle:
        lines.append(f"📐 Высота седла: {saddle}")
    bike = client_record.get("favorite_bike")
    if bike:
        lines.append(f"🚲 Любимый велосипед: {bike}")
    submitted = client_record.get("submitted_at")
    if submitted:
        if isinstance(submitted, datetime):
            submitted_str = submitted.strftime("%Y-%m-%d %H:%M")
        else:
            submitted_str = str(submitted)
        lines.append(f"🗓️ Анкета заполнена: {submitted_str}")

    return "\n".join(lines)


CLIENT_EDIT_FIELDS: Dict[str, Dict[str, str]] = {
    "ftp": {
        "label": "⚡ FTP",
        "prompt": "Введите новое значение FTP в ваттах (например, 250).",
    },
    "weight": {
        "label": "⚖️ Вес",
        "prompt": "Введите вес в килограммах (например, 72.5).",
    },
    "favorite_bike": {
        "label": "🚲 Любимый велосипед",
        "prompt": "Введите название любимого велосипеда.",
    },
    "pedals": {
        "label": "🚴‍♂️ Педали",
        "prompt": "Введите информацию о педалях.",
    },
}


BIKE_EDIT_FIELDS: Dict[str, Dict[str, str]] = {
    "height_min_cm": {
        "label": "📏 Рост от",
        "prompt": "Введите минимальный рост в сантиметрах (например, 165). Оставьте пустым для очистки.",
    },
    "height_max_cm": {
        "label": "📏 Рост до",
        "prompt": "Введите максимальный рост в сантиметрах (например, 185). Оставьте пустым для очистки.",
    },
}


TRAINER_EDIT_FIELDS: Dict[str, Dict[str, str]] = {
    "axle_types": {
        "label": "🔧 Оси",
        "prompt": "Введите типы осей через запятую (например, ЭКС, ОСЬ). Оставьте пустым для очистки.",
    },
    "cassette": {
        "label": "⚙️ Кассета",
        "prompt": "Введите доступные кассеты (например, 10/11). Оставьте пустым для очистки.",
    },
}


def client_display_name(record: Dict[str, Any]) -> str:
    first = record.get("first_name")
    last = record.get("last_name")
    if first or last:
        return " ".join(part for part in [first, last] if part).strip()
    return record.get("full_name") or f"id={record.get('id')}"


def build_client_info_markup(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="⚡ Изменить FTP",
                    callback_data=f"client_edit|ftp|{client_id}",
                ),
                InlineKeyboardButton(
                    text="⚖️ Изменить вес",
                    callback_data=f"client_edit|weight|{client_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🚲 Любимый велосипед",
                    callback_data=f"client_edit|favorite_bike|{client_id}",
                ),
                InlineKeyboardButton(
                    text="🚴‍♂️ Педали",
                    callback_data=f"client_edit|pedals|{client_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🚴 Подбор велосипедов",
                    callback_data=f"client_bikes|{client_id}",
                )
            ],
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="noop")],
        ]
    )


def build_client_edit_markup(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="↩️ Назад",
                    callback_data=f"client_info|{client_id}",
                ),
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"client_edit_cancel|{client_id}"),
            ]
        ]
    )


def parse_client_edit_value(field: str, raw_value: str) -> object:
    value = (raw_value or "").strip()
    if field == "ftp":
        normalized = value.replace(",", ".")
        ftp_value = int(float(normalized))
        if ftp_value <= 0:
            raise ValueError("Введите положительное число (Вт).")
        return ftp_value
    if field == "weight":
        normalized = value.replace(",", ".")
        weight_value = float(normalized)
        if weight_value <= 0:
            raise ValueError("Введите положительное число (кг).")
        return weight_value
    if field in {"favorite_bike", "pedals"}:
        if not value:
            raise ValueError("Значение не должно быть пустым.")
        return value
    raise ValueError("Unsupported field.")


def parse_bike_edit_value(field: str, raw_value: str) -> object:
    value = (raw_value or "").strip()
    if field in {"height_min_cm", "height_max_cm"}:
        if not value:
            return None
        normalized = value.replace(",", ".")
        number = float(normalized)
        if number <= 0:
            raise ValueError("Введите положительное число (см).")
        return number
    raise ValueError("Unsupported field.")


def parse_trainer_edit_value(field: str, raw_value: str) -> object:
    value = (raw_value or "").strip()
    if field == "axle_types":
        tokens = _normalize_tokens(value)
        if not tokens:
            return None
        return ", ".join(token.upper() for token in tokens)
    if field == "cassette":
        digits = re.findall(r"\d+", value)
        if digits:
            return "/".join(digits)
        if not value:
            return None
        raise ValueError("Введите одно или несколько чисел (например, 10/11).")
    raise ValueError("Unsupported field.")


async def render_client_info_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    client_id: int,
) -> None:
    try:
        record = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s", client_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения данных клиента: {exc}",
        )
        return

    if not record:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔍 Клиент не найден.",
        )
        return

    bike_suggestions, height_cm, trainer_inventory = await get_bike_suggestions_for_client(record)
    trainer_map = (
        _build_trainer_suggestions(bike_suggestions, trainer_inventory)
        if bike_suggestions and trainer_inventory
        else None
    )
    text = format_client_details(record, bike_suggestions, height_cm, trainer_map)
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_client_info_markup(client_id),
    )


async def render_client_bike_suggestions(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    client_id: int,
) -> None:
    try:
        record = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s", client_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения данных клиента: {exc}",
        )
        return

    if not record:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔍 Клиент не найден.",
        )
        return

    bike_suggestions, height_cm, trainer_inventory = await get_bike_suggestions_for_client(record)
    trainer_map = (
        _build_trainer_suggestions(bike_suggestions, trainer_inventory)
        if bike_suggestions and trainer_inventory
        else None
    )

    if not bike_suggestions:
        height_label = _format_decimal_value(height_cm) or f"{height_cm:g}" if height_cm else None
        if height_label:
            text = f"🚴 Для роста {height_label} см подходящие велосипеды не найдены."
        else:
            text = "🚴 Рост клиента не указан, подбор невозможен."
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(text="↩️ Назад", callback_data=f"client_info|{client_id}")]]
            ),
        )
        return

    detail_blocks: List[str] = []
    for bike_record in bike_suggestions:
        trainers_for_bike = (
            trainer_map.get(bike_record.get("id"), []) if trainer_map else None
        )
        detail_blocks.append(format_bike_suggestion(bike_record, trainers_for_bike))

    text = "\n\n".join(detail_blocks)
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text="↩️ Назад",
                        callback_data=f"client_info|{client_id}",
                    ),
                    InlineKeyboardButton(text="❌ Закрыть", callback_data="noop"),
                ]
            ]
        ),
    )


async def start_client_edit(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    field: str,
) -> None:
    metadata = CLIENT_EDIT_FIELDS.get(field)
    if metadata is None:
        await query.answer("Поле недоступно для редактирования.", show_alert=True)
        return

    try:
        record = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s", client_id)
        await query.edit_message_text(f"❌ Ошибка получения данных клиента: {exc}")
        return

    if not record:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    bike_suggestions, height_cm, trainer_inventory = await get_bike_suggestions_for_client(record)
    trainer_map = (
        _build_trainer_suggestions(bike_suggestions, trainer_inventory)
        if bike_suggestions and trainer_inventory
        else None
    )
    details_text = format_client_details(record, bike_suggestions, height_cm, trainer_map)
    display_name = client_display_name(record)
    prompt = metadata["prompt"]
    text = (
        f"{details_text}\n\n"
        f"✏️ <i>{html.escape(prompt)}</i>\n"
        f"👤 <i>Клиент: {html.escape(display_name)}</i>"
    )

    context.user_data["pending_client_edit"] = {
        "client_id": client_id,
        "field": field,
        "chat_id": query.message.chat_id,
        "message_id": query.message.message_id,
        "label": metadata["label"],
        "client_name": display_name,
    }

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_client_edit_markup(client_id),
    )


async def cancel_client_edit(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
) -> None:
    pending = context.user_data.get("pending_client_edit")
    if (
        pending
        and pending.get("chat_id") == query.message.chat_id
        and pending.get("message_id") == query.message.message_id
    ):
        context.user_data.pop("pending_client_edit", None)

    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
    )


async def process_pending_client_edit(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    pending: Dict[str, Any],
) -> bool:
    field = pending.get("field")
    client_id = pending.get("client_id")

    if field not in CLIENT_EDIT_FIELDS or not isinstance(client_id, int):
        context.user_data.pop("pending_client_edit", None)
        await message.reply_text("⚠️ Изменение этого поля недоступно.")
        return True

    metadata = CLIENT_EDIT_FIELDS[field]

    try:
        new_value = parse_client_edit_value(field, message.text or "")
    except Exception as exc:  # noqa: BLE001
        await message.reply_text(f"⚠️ {exc}")
        return True

    try:
        await asyncio.to_thread(update_client_fields, client_id, **{field: new_value})
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to update client %s field %s", client_id, field)
        await message.reply_text(f"❌ Не удалось обновить данные: {exc}")
        return True

    context.user_data.pop("pending_client_edit", None)

    client_name = pending.get("client_name")
    if client_name:
        await message.reply_text(f"✅ {metadata['label']} для {client_name} обновлено.")
    else:
        await message.reply_text(f"✅ {metadata['label']} обновлено.")

    chat_id = pending.get("chat_id")
    message_id = pending.get("message_id")
    if isinstance(chat_id, int) and isinstance(message_id, int):
        await render_client_info_message(context, chat_id, message_id, client_id)

    return True


async def render_bike_info_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    bike_id: int,
) -> None:
    try:
        record = await asyncio.to_thread(get_bike, bike_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load bike %s", bike_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения данных велосипеда: {exc}",
        )
        return

    if not record:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔍 Велосипед не найден.",
        )
        return

    trainers: Optional[List[Dict[str, Any]]] = None
    try:
        trainer_inventory = await asyncio.to_thread(_load_trainer_inventory)
        suggestions = _build_trainer_suggestions([record], trainer_inventory)
        trainers = suggestions.get(record.get("id"), []) if suggestions else []
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to load trainers for bike %s: %s", bike_id, exc)
        trainers = None

    text = format_bike_details(record, trainers)
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_bike_info_markup(bike_id),
    )


async def show_bike_info(query, context: ContextTypes.DEFAULT_TYPE, bike_id: int) -> None:
    context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)
    await render_bike_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        bike_id,
    )


async def start_bike_edit(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    bike_id: int,
    field: str,
) -> None:
    metadata = BIKE_EDIT_FIELDS.get(field)
    if metadata is None:
        await query.answer("Поле недоступно для редактирования.", show_alert=True)
        return

    try:
        record = await asyncio.to_thread(get_bike, bike_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load bike %s", bike_id)
        await query.edit_message_text(f"❌ Ошибка получения данных велосипеда: {exc}")
        return

    if not record:
        await query.edit_message_text("🔍 Велосипед не найден.")
        return

    trainers: Optional[List[Dict[str, Any]]] = None
    try:
        trainer_inventory = await asyncio.to_thread(_load_trainer_inventory)
        suggestions = _build_trainer_suggestions([record], trainer_inventory)
        trainers = suggestions.get(record.get("id"), []) if suggestions else []
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to load trainers for bike %s: %s", bike_id, exc)
        trainers = None

    details_text = format_bike_details(record, trainers)
    prompt = metadata["prompt"]
    bike_name = (record.get("title") or f"id={record.get('id')}").strip()

    context.user_data[PENDING_BIKE_EDIT_KEY] = {
        "bike_id": bike_id,
        "field": field,
        "chat_id": query.message.chat_id,
        "message_id": query.message.message_id,
        "label": metadata["label"],
        "bike_name": bike_name,
    }

    await query.edit_message_text(
        f"{details_text}\n\n✏️ <i>{html.escape(prompt)}</i>\n🚲 <i>Велосипед: {html.escape(bike_name)}</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_bike_edit_markup(bike_id),
    )


async def cancel_bike_edit(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    bike_id: int,
) -> None:
    pending = context.user_data.get(PENDING_BIKE_EDIT_KEY)
    if (
        pending
        and pending.get("chat_id") == query.message.chat_id
        and pending.get("message_id") == query.message.message_id
    ):
        context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)

    await render_bike_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        bike_id,
    )


async def process_pending_bike_edit(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    pending: Dict[str, Any],
) -> bool:
    field = pending.get("field")
    bike_id = pending.get("bike_id")

    if field not in BIKE_EDIT_FIELDS or not isinstance(bike_id, int):
        context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)
        await message.reply_text("⚠️ Изменение этого поля недоступно.")
        return True

    metadata = BIKE_EDIT_FIELDS[field]

    try:
        new_value = parse_bike_edit_value(field, message.text or "")
    except Exception as exc:  # noqa: BLE001
        await message.reply_text(f"⚠️ {exc}")
        return True

    try:
        await asyncio.to_thread(update_bike_fields, bike_id, **{field: new_value})
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to update bike %s field %s", bike_id, field)
        await message.reply_text(f"❌ Не удалось обновить данные: {exc}")
        return True

    context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)

    bike_name = pending.get("bike_name")
    if bike_name:
        await message.reply_text(f"✅ {metadata['label']} для {bike_name} обновлены.")
    else:
        await message.reply_text(f"✅ {metadata['label']} обновлены.")

    chat_id = pending.get("chat_id")
    message_id = pending.get("message_id")
    if isinstance(chat_id, int) and isinstance(message_id, int):
        await render_bike_info_message(context, chat_id, message_id, bike_id)

    return True


async def render_trainer_info_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    trainer_id: int,
) -> None:
    try:
        record = await asyncio.to_thread(get_trainer, trainer_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load trainer %s", trainer_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения данных станка: {exc}",
        )
        return

    if not record:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔍 Станок не найден.",
        )
        return

    text = format_trainer_details(record)
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_trainer_info_markup(trainer_id),
    )


async def show_trainer_info(query, context: ContextTypes.DEFAULT_TYPE, trainer_id: int) -> None:
    context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)
    await render_trainer_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        trainer_id,
    )


async def start_trainer_edit(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    trainer_id: int,
    field: str,
) -> None:
    metadata = TRAINER_EDIT_FIELDS.get(field)
    if metadata is None:
        await query.answer("Поле недоступно для редактирования.", show_alert=True)
        return

    try:
        record = await asyncio.to_thread(get_trainer, trainer_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load trainer %s", trainer_id)
        await query.edit_message_text(f"❌ Ошибка получения данных станка: {exc}")
        return

    if not record:
        await query.edit_message_text("🔍 Станок не найден.")
        return

    details_text = format_trainer_details(record)
    prompt = metadata["prompt"]
    trainer_name = trainer_display_name(record)

    context.user_data[PENDING_TRAINER_EDIT_KEY] = {
        "trainer_id": trainer_id,
        "field": field,
        "chat_id": query.message.chat_id,
        "message_id": query.message.message_id,
        "label": metadata["label"],
        "trainer_name": trainer_name,
    }

    await query.edit_message_text(
        f"{details_text}\n\n✏️ <i>{html.escape(prompt)}</i>\n🛠 <i>Станок: {html.escape(trainer_name)}</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_trainer_edit_markup(trainer_id),
    )


async def cancel_trainer_edit(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    trainer_id: int,
) -> None:
    pending = context.user_data.get(PENDING_TRAINER_EDIT_KEY)
    if (
        pending
        and pending.get("chat_id") == query.message.chat_id
        and pending.get("message_id") == query.message.message_id
    ):
        context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)

    await render_trainer_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        trainer_id,
    )


async def process_pending_trainer_edit(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    pending: Dict[str, Any],
) -> bool:
    field = pending.get("field")
    trainer_id = pending.get("trainer_id")

    if field not in TRAINER_EDIT_FIELDS or not isinstance(trainer_id, int):
        context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)
        await message.reply_text("⚠️ Изменение этого поля недоступно.")
        return True

    metadata = TRAINER_EDIT_FIELDS[field]

    try:
        new_value = parse_trainer_edit_value(field, message.text or "")
    except Exception as exc:  # noqa: BLE001
        await message.reply_text(f"⚠️ {exc}")
        return True

    try:
        await asyncio.to_thread(update_trainer_fields, trainer_id, **{field: new_value})
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to update trainer %s field %s", trainer_id, field)
        await message.reply_text(f"❌ Не удалось обновить данные: {exc}")
        return True

    context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)

    trainer_name = pending.get("trainer_name")
    if trainer_name:
        await message.reply_text(f"✅ {metadata['label']} для {trainer_name} обновлены.")
    else:
        await message.reply_text(f"✅ {metadata['label']} обновлены.")

    chat_id = pending.get("chat_id")
    message_id = pending.get("message_id")
    if isinstance(chat_id, int) and isinstance(message_id, int):
        await render_trainer_info_message(context, chat_id, message_id, trainer_id)

    return True
def fetch_account_information(account_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    account = ACCOUNT_REGISTRY[account_id]
    client = WattAttackClient(account.base_url)
    client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)

    profile: Dict[str, Any] = {}
    try:
        profile = client.fetch_profile(timeout=DEFAULT_TIMEOUT)
        if not isinstance(profile, dict):
            profile = {}
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch profile for %s: %s", account_id, exc)
        profile = {}

    auth_user: Dict[str, Any] = {}
    try:
        auth_info = client.auth_check(timeout=DEFAULT_TIMEOUT)
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
    account = ACCOUNT_REGISTRY[account_id]
    lines = [f"<b>👤 {account.name}</b> ({account_id})"]

    first = auth_user.get("firstName") if auth_user else None
    last = auth_user.get("lastName") if auth_user else None
    if first or last:
        name_str = " ".join(part for part in [first, last] if part)
        if name_str:
            lines.append(f"🧑 Имя: {name_str}")
    else:
        name = extract_athlete_name(profile)
        if name:
            lines.append(f"🧑 Имя: {name}")

    email = auth_user.get("email") if auth_user else None
    if email:
        lines.append(f"✉️ Email: {email}")

    gender = extract_athlete_field(profile, "gender")
    if gender:
        gender_symbol = "🚹" if gender.upper().startswith("M") else "🚺"
        lines.append(f"{gender_symbol} Пол: {'М' if gender.upper().startswith('M') else 'Ж'} ({gender})")

    weight = extract_athlete_field(profile, "weight")
    if weight:
        try:
            lines.append(f"⚖️ Вес: {float(weight):g} кг")
        except (TypeError, ValueError):
            lines.append(f"⚖️ Вес: {weight} кг")

    height = extract_athlete_field(profile, "height")
    if height:
        try:
            lines.append(f"📏 Рост: {float(height):g} см")
        except (TypeError, ValueError):
            lines.append(f"📏 Рост: {height} см")

    ftp = extract_athlete_field(profile, "ftp")
    if ftp:
        try:
            lines.append(f"⚡ FTP: {int(float(ftp))} Вт")
        except (TypeError, ValueError):
            lines.append(f"⚡ FTP: {ftp} Вт")

    birth_date = extract_athlete_field(profile, "birthDate")
    if birth_date:
        lines.append(f"🎂 Дата рождения: {birth_date}")

    return "\n".join(lines)


async def fetch_recent_activities(
    account_id: str, limit: int
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    account = ACCOUNT_REGISTRY[account_id]

    def worker() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)
        payload = client.fetch_activities(timeout=DEFAULT_TIMEOUT)
        activities = payload.get("activities", [])
        if not isinstance(activities, list):
            activities = []

        profile: Dict[str, Any] = {}
        try:
            profile = client.fetch_profile(timeout=DEFAULT_TIMEOUT)
            if not isinstance(profile, dict):
                profile = {}
            athlete_name = extract_athlete_name(profile)
            LOGGER.info(
                "Fetched profile for %s: %s (keys=%s)",
                account_id,
                athlete_name or "<unknown>",
                list(profile.keys()),
            )
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


async def show_client_page(
    account_id: str,
    page: int,
    *,
    message: Optional[Message] = None,
    query=None,
) -> None:
    if account_id not in ACCOUNT_REGISTRY:
        text = "⚠️ Аккаунт не найден."
        if query:
            await query.edit_message_text(text)
        elif message:
            await message.reply_text(text)
        return

    try:
        total = await asyncio.to_thread(count_clients)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to count clients")
        text = f"❌ Ошибка чтения базы клиентов: {exc}"
        if query:
            await query.edit_message_text(text)
        elif message:
            await message.reply_text(text)
        return

    if total <= 0:
        text = "📭 Список клиентов пуст."
        if query:
            await query.edit_message_text(text)
        elif message:
            await message.reply_text(text)
        return

    page_size = CLIENTS_PAGE_SIZE
    max_page = max(0, (total - 1) // page_size)
    page = max(0, min(page, max_page))
    offset = page * page_size

    try:
        clients = await asyncio.to_thread(list_clients, page_size, offset)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load clients from DB")
        text = f"❌ Ошибка чтения базы клиентов: {exc}"
        if query:
            await query.edit_message_text(text)
        elif message:
            await message.reply_text(text)
        return

    keyboard_rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=client["full_name"],
                callback_data=f"setclient|{account_id}|{client['id']}",
            )
        ]
        for client in clients
    ]

    nav_row: List[InlineKeyboardButton] = [
        InlineKeyboardButton(
            text="← Выбор аккаунта",
            callback_data="select_accounts|setclient",
        )
    ]
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                text="← Назад",
                callback_data=f"setclient_page|{account_id}|{page-1}",
            )
        )
    if page < max_page:
        nav_row.append(
            InlineKeyboardButton(
                text="Вперёд →",
                callback_data=f"setclient_page|{account_id}|{page+1}",
            )
        )
    if nav_row:
        keyboard_rows.append(nav_row)

    text = (
        f"👥 Выберите клиента для применения данных к {ACCOUNT_REGISTRY[account_id].name}:\n"
        f"📄 Страница {page + 1} из {max_page + 1} (всего {total})"
    )

    markup = InlineKeyboardMarkup(keyboard_rows)
    if query:
        await query.edit_message_text(text, reply_markup=markup)
    elif message:
        await message.reply_text(text, reply_markup=markup)


async def show_account_selection(
    *, message: Optional[Message] = None, query=None, kind: str = "setclient", account_id: Optional[str] = None
) -> None:
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for account_id in sorted(ACCOUNT_REGISTRY):
        alias = normalize_account_id(account_id)
        label = f"{alias} — {ACCOUNT_REGISTRY[account_id].name}"
        if kind == "setclient":
            callback = f"setclient_page|{account_id}|0"
        else:
            callback = f"account_show|{account_id}"

        keyboard_rows.append([InlineKeyboardButton(text=label, callback_data=callback)])

    if kind == "setclient":
        text = "👤 Выберите аккаунт для применения данных клиента:"
    else:
        text = "📊 Выберите аккаунт для просмотра данных:"
    markup = InlineKeyboardMarkup(keyboard_rows)
    if query:
        await query.edit_message_text(text, reply_markup=markup)
    elif message:
        await message.reply_text(text, reply_markup=markup)


async def assign_client_to_account(query, context, account_id: str, client_id: int) -> None:
    if account_id not in ACCOUNT_REGISTRY:
        await query.edit_message_text("⚠️ Аккаунт не найден.")
        return

    try:
        client_record = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch client %s", client_id)
        await query.edit_message_text(f"❌ Ошибка чтения клиента: {exc}")
        return

    if not client_record:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    try:
        await asyncio.to_thread(apply_client_profile, account_id, client_record)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to apply client %s to %s", client_id, account_id)
        await query.edit_message_text(f"❌ Ошибка применения данных: {exc}")
        return

    summary = format_client_summary(client_record)
    cache = context.user_data.get("account_cache")
    if isinstance(cache, dict):
        cache.pop(account_id, None)
    await query.edit_message_text(
        f"✅ Данные клиента применены к {ACCOUNT_REGISTRY[account_id].name}:\n{summary}",
        parse_mode=ParseMode.HTML,
    )


async def show_account_via_callback(query, account_id: str) -> None:
    account = resolve_account_identifier(account_id)
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


async def show_client_info(query, context: ContextTypes.DEFAULT_TYPE, client_id: int) -> None:
    context.user_data.pop("pending_client_edit", None)
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
    )


async def text_search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    if update.message.text.startswith("/"):
        return
    if not ensure_admin_message(update):
        return
    bike_pending = context.user_data.get(PENDING_BIKE_EDIT_KEY)
    if bike_pending:
        handled = await process_pending_bike_edit(update.message, context, bike_pending)
        if handled:
            return
    trainer_pending = context.user_data.get(PENDING_TRAINER_EDIT_KEY)
    if trainer_pending:
        handled = await process_pending_trainer_edit(update.message, context, trainer_pending)
        if handled:
            return
    pending = context.user_data.get("pending_client_edit")
    if pending:
        handled = await process_pending_client_edit(update.message, context, pending)
        if handled:
            return
    await process_client_search(update.message, update.message.text)


async def document_upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return
    if not ensure_admin_message(update):
        return

    document = update.message.document
    caption = update.message.caption or ""
    command = None
    args: List[str] = []

    stripped = caption.strip()
    if stripped.startswith("/"):
        parts = stripped.split()
        command = parts[0].lower()
        args = parts[1:]

    upload_type: Optional[str] = None
    truncate = False

    if command and command in UPLOAD_COMMAND_TYPES:
        upload_type = UPLOAD_COMMAND_TYPES[command]
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in args)
        _pop_pending_upload(context.user_data)
    else:
        pending = _pop_pending_upload(context.user_data)
        if pending:
            upload_type = pending.get("type")
            truncate = pending.get("truncate", False)

    if upload_type == "clients":
        await process_clients_document(document, update.message, truncate)
    elif upload_type == "bikes":
        await process_bikes_document(document, update.message, truncate)
    elif upload_type == "stands":
        await process_trainers_document(document, update.message, truncate)
    else:
        await update.message.reply_text(
            "ℹ️ Чтобы импортировать данные, используйте /uploadclients, /uploadbikes, /uploadstands "
            "или добавьте команду в подпись к файлу."
        )


async def process_client_search(message: Message, term: str) -> None:
    term = (term or "").strip()
    if not term:
        await message.reply_text("⚠️ Запрос не должен быть пустым.")
        return
    if len(term) < 2:
        await message.reply_text("ℹ️ Уточните запрос (минимум 2 символа).")
        return

    try:
        results = await asyncio.to_thread(search_clients, term, 15)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to search clients")
        await message.reply_text(f"❌ Ошибка поиска клиентов: {exc}")
        return

    if not results:
        await message.reply_text("🔍 Ничего не найдено. Уточните запрос.")
        return

    if len(results) == 1:
        record = results[0]
        bike_suggestions, height_cm, trainer_inventory = await get_bike_suggestions_for_client(record)
        trainer_map = (
            _build_trainer_suggestions(bike_suggestions, trainer_inventory)
            if bike_suggestions and trainer_inventory
            else None
        )
        await message.reply_text(
            format_client_details(record, bike_suggestions, height_cm, trainer_map),
            parse_mode=ParseMode.HTML,
            reply_markup=build_client_info_markup(record["id"]),
        )
        return

    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for record in results:
        label = format_client_button_label(record)
        keyboard_rows.append(
            [InlineKeyboardButton(text=label, callback_data=f"client_info|{record['id']}")]
        )

    header = f"📋 Найдено {len(results)} совпадений. Выберите клиента:"
    if len(results) >= 15:
        header += "\nℹ️ Показаны первые 15, уточните запрос для более точного результата."

    await message.reply_text(
        header,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def send_fit_file(query, context, account_id: str, activity_id: str) -> None:
    cache = context.user_data.setdefault("account_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        context.user_data["account_cache"] = cache
    account_cache: Dict[str, Any] = cache.get(account_id, {}) if isinstance(cache, dict) else {}
    activities: List[Dict[str, Any]] = account_cache.get("activities", []) if isinstance(account_cache, dict) else []
    profile: Optional[Dict[str, Any]] = account_cache.get("profile") if isinstance(account_cache, dict) else None
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
    application.add_handler(CommandHandler("account", account_handler))
    application.add_handler(CommandHandler("client", client_handler))
    application.add_handler(CommandHandler("bikes", bikes_handler))
    application.add_handler(CommandHandler("stands", stands_handler))
    application.add_handler(CommandHandler("setclient", setclient_handler))
    application.add_handler(CommandHandler("admins", admins_handler))
    application.add_handler(CommandHandler("addadmin", addadmin_handler))
    application.add_handler(CommandHandler("removeadmin", removeadmin_handler))
    application.add_handler(CommandHandler("uploadclients", uploadclients_handler))
    application.add_handler(CommandHandler("uploadbikes", uploadbikes_handler))
    application.add_handler(CommandHandler("uploadstands", uploadstands_handler))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), text_search_handler))
    csv_filter = (filters.Document.MimeType("text/csv") | filters.Document.FileExtension("csv"))
    application.add_handler(
        MessageHandler(csv_filter, document_upload_handler)
    )
    application.add_handler(CallbackQueryHandler(noop_handler, pattern="^noop$"))
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
            "TELEGRAM_BOT_TOKEN не задан. Установите переменную окружения и повторите запуск."
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
