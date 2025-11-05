#!/usr/bin/env python3
"""Telegram bot for managing WattAttack profiles, clients, and inventory."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from repositories.client_repository import (
    count_clients,
    create_client,
    get_client,
    get_clients_stats,
    list_clients,
    search_clients,
    update_client_fields,
)
from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    list_admins as db_list_admins,
    add_admin as db_add_admin,
    remove_admin as db_remove_admin,
    is_admin as db_is_admin,
)
from repositories.bikes_repository import (
    ensure_bikes_table,
    list_bikes,
    search_bikes,
    bikes_count,
    find_bikes_for_height,
    get_bike,
    update_bike_fields,
)
from repositories.trainers_repository import (
    ensure_trainers_table,
    list_trainers,
    search_trainers,
    trainers_count,
    get_trainer,
    update_trainer_fields,
)
from scripts.load_clients import load_clients_from_csv_bytes
from scripts.load_bikes import load_bikes_from_csv_bytes
from scripts.load_trainers import load_trainers_from_csv_bytes
from wattattack_activities import WattAttackClient
from wattattack_workouts import (
    build_workout_payload,
    calculate_workout_metrics,
    parse_zwo_workout,
    zwo_to_chart_data,
)
from adminbot.accounts import (
    AccountConfig,
    format_account_list as format_account_list_from_registry,
    load_accounts,
    normalize_account_id as normalize_account_id_value,
    resolve_account_identifier as resolve_account_identifier_value,
    resolve_account_tokens as resolve_account_tokens_value,
)

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
CLIENTS_PAGE_SIZE = int(os.environ.get("CLIENTS_PAGE_SIZE", "6"))
DEFAULT_CLIENT_FTP = int(os.environ.get("WATTATTACK_DEFAULT_FTP", "150"))
CLIENT_BIKE_PICK_PAGE_SIZE = int(os.environ.get("CLIENT_BIKE_PAGE_SIZE", "6"))

PEDAL_OPTIONS: List[Tuple[str, str]] = [
    ("топталки (под кроссовки)", "platform"),
    ("контакты шоссе Look", "road_look"),
    ("контакты шоссе Shimano", "road_shimano"),
    ("контакты MTB Shimano", "mtb_shimano"),
    ("принесу свои", "own"),
]
PEDAL_OPTION_LABEL_BY_CODE: Dict[str, str] = {code: label for label, code in PEDAL_OPTIONS}

(
    NEWCLIENT_FIRST_NAME,
    NEWCLIENT_LAST_NAME,
    NEWCLIENT_WEIGHT,
    NEWCLIENT_HEIGHT,
    NEWCLIENT_GENDER,
    NEWCLIENT_FTP,
    NEWCLIENT_PEDALS,
    NEWCLIENT_GOAL,
) = range(8)

NEWCLIENT_FORM_KEY = "adminbot:newclient_form"


def _newclient_get_form(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    form = context.user_data.get(NEWCLIENT_FORM_KEY)
    if form is None:
        form = {}
        context.user_data[NEWCLIENT_FORM_KEY] = form
    return form


def _newclient_reset_form(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(NEWCLIENT_FORM_KEY, None)


def _parse_optional_float_value(text: str) -> Tuple[bool, Optional[float]]:
    normalized = text.replace(",", ".").strip()
    if not normalized:
        return True, None
    try:
        return True, float(normalized)
    except ValueError:
        return False, None


def _parse_positive_float_value(text: str) -> Tuple[bool, Optional[float]]:
    ok, value = _parse_optional_float_value(text)
    if not ok or value is None:
        return False, None
    if value <= 0:
        return False, None
    return True, value


def _format_optional_number(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:g}"

PENDING_UPLOAD_KEY = "pending_inventory_upload"
PENDING_TRAINER_EDIT_KEY = "pending_trainer_edit"
PENDING_BIKE_EDIT_KEY = "pending_bike_edit"
PENDING_WORKOUT_UPLOAD_KEY = "pending_workout_upload"
PENDING_COMBINATE_KEY = "pending_combinate"
UPLOAD_COMMAND_TYPES = {
    "/uploadclients": "clients",
    "/uploadbikes": "bikes",
    "/uploadstands": "stands",
}
WORKOUT_UPLOAD_COMMAND = "/uploadworkout"


ACCOUNT_REGISTRY: Dict[str, AccountConfig] = {}


def normalize_account_id(value: str) -> str:
    return normalize_account_id_value(value)


def resolve_account_identifier(raw_id: str) -> Optional[str]:
    return resolve_account_identifier_value(ACCOUNT_REGISTRY, raw_id)


def format_account_list() -> str:
    return format_account_list_from_registry(ACCOUNT_REGISTRY)


def resolve_account_tokens(tokens: Iterable[str]) -> Tuple[List[str], List[str]]:
    return resolve_account_tokens_value(ACCOUNT_REGISTRY, tokens)


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


def _set_pending_workout_upload(user_data: Dict[str, Any], account_ids: List[str]) -> None:
    unique_ids = list(dict.fromkeys(account_ids))
    user_data[PENDING_WORKOUT_UPLOAD_KEY] = {"account_ids": unique_ids}


def _pop_pending_workout_upload(user_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    value = user_data.get(PENDING_WORKOUT_UPLOAD_KEY)
    if value is not None:
        user_data.pop(PENDING_WORKOUT_UPLOAD_KEY, None)
    return value


def _set_pending_combinate(user_data: Dict[str, Any], chat_id: int) -> None:
    user_data[PENDING_COMBINATE_KEY] = {"chat_id": chat_id}


def _clear_pending_combinate(user_data: Dict[str, Any]) -> None:
    user_data.pop(PENDING_COMBINATE_KEY, None)


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


def _format_number(value: Optional[float]) -> str:
    if value is None:
        return ""
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.1f}".rstrip("0").rstrip(".")


def _format_metric_range(label: str, minimum: Optional[float], maximum: Optional[float], unit: str) -> str:
    if minimum is None and maximum is None:
        return f"{label}: нет данных."
    if minimum is None:
        return f"{label}: до {_format_number(maximum)} {unit}"
    if maximum is None:
        return f"{label}: от {_format_number(minimum)} {unit}"
    if abs(maximum - minimum) < 1e-6:
        return f"{label}: {_format_number(maximum)} {unit}"
    return f"{label}: {_format_number(minimum)}–{_format_number(maximum)} {unit}"


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


def _bike_height_bounds(bike: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    return _parse_height_cm(bike.get("height_min_cm")), _parse_height_cm(bike.get("height_max_cm"))


def _bike_height_distance(bike: Dict[str, Any], rider_height: float) -> float:
    min_height, max_height = _bike_height_bounds(bike)
    if min_height is not None and rider_height < min_height:
        return float(min_height - rider_height)
    if max_height is not None and rider_height > max_height:
        return float(rider_height - max_height)
    if min_height is not None and max_height is not None:
        midpoint = (min_height + max_height) / 2.0
        return abs(midpoint - rider_height)
    if min_height is not None:
        return abs(min_height - rider_height)
    if max_height is not None:
        return abs(max_height - rider_height)
    return 0.0


def _bike_height_matches(bike: Dict[str, Any], rider_height: float) -> bool:
    min_height, max_height = _bike_height_bounds(bike)
    if min_height is not None and rider_height < min_height:
        return False
    if max_height is not None and rider_height > max_height:
        return False
    return True


def _split_html_message(text: str, limit: int = 4000) -> List[str]:
    """Split long HTML messages without breaking mid-line."""
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    buffer = ""
    for line in text.splitlines():
        candidate = f"{buffer}\n{line}" if buffer else line
        if len(candidate) <= limit:
            buffer = candidate
            continue
        if buffer:
            chunks.append(buffer)
            buffer = line
        else:
            # Single line longer than limit; hard split.
            start = 0
            while start < len(line):
                end = min(start + limit, len(line))
                chunks.append(line[start:end])
                start = end
            buffer = ""
    if buffer:
        chunks.append(buffer)
    return chunks


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
    title = (record.get("title") or "").strip()
    display = (record.get("display_name") or "").strip()
    if code and title and title.lower() != code.lower():
        return f"{code} · {title}"
    if title:
        return title
    if code:
        return code
    return display or f"id={record.get('id')}"


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
            [
                InlineKeyboardButton(
                    text="↩️ К списку",
                    callback_data="stands_list",
                ),
                InlineKeyboardButton(text="❌ Закрыть", callback_data="stands_close"),
            ],
        ]
    )


def build_trainer_edit_markup(trainer_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(
                text="↩️ Назад",
                callback_data=f"trainer_info|{trainer_id}",
            )
        ]]
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


async def process_workout_document(
    document,
    message: Message,
    account_ids: List[str],
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download workout file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    raw_bytes = bytes(data)
    text: Optional[str] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = raw_bytes.decode("utf-8", errors="ignore")

    try:
        workout = await asyncio.to_thread(parse_zwo_workout, text)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to parse ZWO workout")
        await message.reply_text(f"❌ Ошибка обработки ZWO: {exc}")
        return

    try:
        chart_data = await asyncio.to_thread(zwo_to_chart_data, workout)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to build chart data")
        await message.reply_text(f"❌ Ошибка генерации графика тренировки: {exc}")
        return

    if not account_ids:
        await message.reply_text("ℹ️ Не указан ни один аккаунт для загрузки.")
        return

    results: List[Tuple[str, bool, str]] = []
    for account_id in account_ids:
        if account_id not in ACCOUNT_REGISTRY:
            results.append((account_id, False, "Аккаунт не найден"))
            continue
        success, info = await upload_workout_to_account(account_id, workout, chart_data)
        results.append((account_id, success, info))

    workout_name = workout.get("name") or (document.file_name or "тренировка")
    header = f"📤 Загрузка тренировки «{workout_name}»:"
    lines = [header]
    for account_id, success, info in results:
        account = ACCOUNT_REGISTRY.get(account_id)
        account_label = account.name if account else account_id
        prefix = "✅" if success else "❌"
        lines.append(f"{prefix} {account_label}: {info}")

    await message.reply_text("\n".join(lines))


async def upload_workout_to_account(
    account_id: str,
    workout: Dict[str, Any],
    chart_data: List[Dict[str, Any]],
) -> Tuple[bool, str]:
    def worker() -> Dict[str, Any]:
        account = ACCOUNT_REGISTRY[account_id]
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=DEFAULT_TIMEOUT)

        ftp: Optional[float] = None
        try:
            profile = client.fetch_profile(timeout=DEFAULT_TIMEOUT)
            ftp_raw = extract_athlete_field(profile, "ftp")
            if ftp_raw not in (None, "", "—"):
                ok, ftp_value = _parse_optional_float(str(ftp_raw))
                if ok and ftp_value is not None and ftp_value > 0:
                    ftp = ftp_value
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch FTP for %s: %s", account_id, exc)

        metrics = calculate_workout_metrics(workout, ftp)
        payload = build_workout_payload(workout, chart_data, metrics)
        return client.upload_workout(payload, timeout=DEFAULT_TIMEOUT)

    try:
        response = await asyncio.to_thread(worker)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to upload workout for %s", account_id)
        return False, str(exc)

    message = ""
    workout_info: Dict[str, Any] = {}
    if isinstance(response, dict):
        message = response.get("message") or ""
        workout_obj = response.get("workout")
        if isinstance(workout_obj, dict):
            workout_info = workout_obj

    if not message:
        message = "Загружено"
    workout_id = workout_info.get("id") if isinstance(workout_info, dict) else None
    if workout_id:
        message = f"{message} (ID {workout_id})"

    return True, message


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    await update.message.reply_text(
        "👋 Этот бот управляет профилями WattAttack и клиентской базой. "
        "Для скачивания активностей используйте бота krutilkafitbot.",
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    message = (
        "ℹ️ Использование:\n"
        "/start — краткое описание возможностей\n"
        "/setclient <аккаунт> — применить данные клиента из базы\n"
        "/account <аккаунт> — показать текущие данные аккаунта\n"
        "/combinate — подобрать велосипеды и станки; фамилии пришлите отдельным сообщением\n"
        "/bikes [поиск] — показать доступные велосипеды\n"
        "/stands [поиск] — показать доступные станки\n"
        "/client <имя/фамилия> — найти клиента по БД\n"
        "/stats — статистика по клиентской базе\n"
        "/uploadclients [truncate] — загрузить CSV клиентов\n"
        "/uploadbikes [truncate] — загрузить CSV велосипедов\n"
        "/uploadstands [truncate] — загрузить CSV станков\n"
        "/uploadworkout [all|аккаунт] — загрузить тренировку ZWO в библиотеку\n"
        "/newclient — создать новую запись клиента в базе\n"
        "/admins — показать список администраторов\n"
        "/addadmin <id|@user> — добавить администратора (можно ответом на сообщение)\n"
        "/removeadmin <id|@user> — удалить администратора"
        "\n\nДля выгрузки активностей и FIT файлов используйте бота krutilkafitbot."
    )
    await update.message.reply_text(message)


async def _newclient_send_gender_prompt(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("М", callback_data="newclient:gender:male"),
                InlineKeyboardButton("Ж", callback_data="newclient:gender:female"),
            ]
        ]
    )
    await context.bot.send_message(
        chat_id,
        "👤 Выберите пол клиента:",
        reply_markup=keyboard,
    )


async def _newclient_send_ftp_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Оставить 150", callback_data="newclient:ftp:skip")]]
    )
    await context.bot.send_message(
        chat_id,
        "⚡ Введите FTP клиента в ваттах.\n"
        "Отправьте число (например, 215) или нажмите «Оставить 150», чтобы использовать значение по умолчанию.",
        reply_markup=keyboard,
    )


async def _newclient_send_pedals_prompt(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"newclient:pedals:{code}")]
        for label, code in PEDAL_OPTIONS
    ]
    await context.bot.send_message(
        chat_id,
        "🚴 Выберите тип педалей из доступных вариантов:",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _newclient_send_goal_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Пропустить", callback_data="newclient:goal:skip")]]
    )
    await context.bot.send_message(
        chat_id,
        "🎯 Укажите цель клиента (например, подготовка к старту).\n"
        "Если цель не нужна, отправьте пустое сообщение или нажмите «Пропустить».",
        reply_markup=keyboard,
    )


async def _newclient_finalize_creation(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    send_message: Callable[[str], Awaitable[Any]],
) -> int:
    form = _newclient_get_form(context)

    first_name = (form.get("first_name") or "").strip()
    last_name = (form.get("last_name") or "").strip()
    weight = form.get("weight")
    height = form.get("height")
    gender = form.get("gender")
    ftp = form.get("ftp", float(DEFAULT_CLIENT_FTP))
    pedals = form.get("pedals")
    goal = form.get("goal")

    missing: List[str] = []
    if not first_name:
        missing.append("имя")
    if not last_name:
        missing.append("фамилия")
    if weight is None:
        missing.append("вес")
    if height is None:
        missing.append("рост")
    if gender not in {"male", "female"}:
        missing.append("пол")
    if not pedals:
        missing.append("педали")

    if missing:
        await send_message(
            "⚠️ Анкета заполнена не полностью. Завершите команду и начните заново: "
            + ", ".join(missing)
        )
        _newclient_reset_form(context)
        return ConversationHandler.END

    try:
        client = await asyncio.to_thread(
            create_client,
            first_name=first_name,
            last_name=last_name,
            weight=weight,
            height=height,
            gender=gender,
            ftp=ftp,
            pedals=pedals,
            goal=goal,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to create client %s %s", first_name, last_name)
        await send_message(f"❌ Не удалось создать запись: {exc}")
        _newclient_reset_form(context)
        return ConversationHandler.END

    summary_lines = [
        "🆕 Создан клиент:",
        f"• Имя: {first_name}",
        f"• Фамилия: {last_name}",
        f"• Пол: {'М' if gender == 'male' else 'Ж'}",
        f"• Вес: {_format_optional_number(weight)} кг",
        f"• Рост: {_format_optional_number(height)} см",
        f"• FTP: {_format_optional_number(ftp)} Вт",
        f"• Педали: {pedals}",
        f"• Цель: {goal or '—'}",
    ]
    await send_message("\n".join(summary_lines))

    client_label = client_display_name(client)
    await context.bot.send_message(
        chat_id,
        f"✅ Запись сохранена: {client_label} (ID {client['id']}).",
    )

    _newclient_reset_form(context)
    return ConversationHandler.END


async def newclient_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return ConversationHandler.END
    if not ensure_admin_message(update):
        return ConversationHandler.END

    _newclient_reset_form(context)
    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP)

    await update.message.reply_text(
        "🆕 Создание новой анкеты клиента.\n"
        "🖊️ Введите имя клиента (команда /cancel для отмены)."
    )
    return NEWCLIENT_FIRST_NAME


async def newclient_first_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return NEWCLIENT_FIRST_NAME
    if not ensure_admin_message(update):
        return ConversationHandler.END

    value = update.message.text.strip()
    if not value:
        await update.message.reply_text("⚠️ Имя не должно быть пустым. Введите имя клиента.")
        return NEWCLIENT_FIRST_NAME

    form = _newclient_get_form(context)
    form["first_name"] = value
    await update.message.reply_text("🧾 Введите фамилию клиента.")
    return NEWCLIENT_LAST_NAME


async def newclient_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return NEWCLIENT_LAST_NAME
    if not ensure_admin_message(update):
        return ConversationHandler.END

    value = update.message.text.strip()
    if not value:
        await update.message.reply_text("⚠️ Фамилия не должна быть пустой. Введите фамилию клиента.")
        return NEWCLIENT_LAST_NAME

    form = _newclient_get_form(context)
    form["last_name"] = value
    await update.message.reply_text(
        "⚖️ Введите вес клиента в килограммах (например, 72.5)."
    )
    return NEWCLIENT_WEIGHT


async def newclient_weight(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return NEWCLIENT_WEIGHT
    if not ensure_admin_message(update):
        return ConversationHandler.END

    ok, weight = _parse_positive_float_value(update.message.text or "")
    if not ok or weight is None:
        await update.message.reply_text(
            "⚠️ Вес должен быть положительным числом (например, 72.5). Повторите ввод."
        )
        return NEWCLIENT_WEIGHT

    form = _newclient_get_form(context)
    form["weight"] = weight
    await update.message.reply_text(
        "📏 Введите рост клиента в сантиметрах (например, 178)."
    )
    return NEWCLIENT_HEIGHT


async def newclient_height(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return NEWCLIENT_HEIGHT
    if not ensure_admin_message(update):
        return ConversationHandler.END

    ok, height = _parse_positive_float_value(update.message.text or "")
    if not ok or height is None:
        await update.message.reply_text(
            "⚠️ Рост должен быть положительным числом (например, 178). Повторите ввод."
        )
        return NEWCLIENT_HEIGHT

    form = _newclient_get_form(context)
    form["height"] = height
    await _newclient_send_gender_prompt(context, update.message.chat_id)
    return NEWCLIENT_GENDER


async def newclient_gender_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_GENDER
    if not await ensure_admin_callback(query):
        return NEWCLIENT_GENDER

    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_GENDER
    gender = parts[2]
    if gender not in {"male", "female"}:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_GENDER

    form = _newclient_get_form(context)
    form["gender"] = gender

    label = "М" if gender == "male" else "Ж"
    try:
        await query.edit_message_text(f"👤 Пол: {label}")
    except Exception:
        LOGGER.debug("Failed to edit gender selection message", exc_info=True)

    await _newclient_send_ftp_prompt(context, query.message.chat_id)
    return NEWCLIENT_FTP


async def newclient_gender_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is not None:
        await message.reply_text("👤 Пожалуйста, выберите пол с помощью кнопок.")
    return NEWCLIENT_GENDER


async def newclient_ftp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return NEWCLIENT_FTP
    if not ensure_admin_message(update):
        return ConversationHandler.END

    ok, ftp = _parse_optional_float_value(update.message.text or "")
    if not ok or (ftp is not None and ftp <= 0):
        await update.message.reply_text(
            "⚠️ Введите положительное число (например, 215) или нажмите кнопку, чтобы оставить значение по умолчанию."
        )
        return NEWCLIENT_FTP

    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP) if ftp is None else ftp
    await _newclient_send_pedals_prompt(context, update.message.chat_id)
    return NEWCLIENT_PEDALS


async def newclient_skip_ftp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_FTP
    if not await ensure_admin_callback(query):
        return NEWCLIENT_FTP

    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP)
    try:
        await query.edit_message_text("ℹ️ FTP оставлен по умолчанию (150).")
    except Exception:
        LOGGER.debug("Failed to edit FTP skip message", exc_info=True)

    await _newclient_send_pedals_prompt(context, query.message.chat_id)
    return NEWCLIENT_PEDALS


async def newclient_pedals_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_PEDALS
    if not await ensure_admin_callback(query):
        return NEWCLIENT_PEDALS

    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_PEDALS
    code = parts[2]
    label = PEDAL_OPTION_LABEL_BY_CODE.get(code)
    if label is None:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_PEDALS

    form = _newclient_get_form(context)
    form["pedals"] = label
    try:
        await query.edit_message_text(f"🚴 Педали: {label}")
    except Exception:
        LOGGER.debug("Failed to edit pedals selection message", exc_info=True)

    await _newclient_send_goal_prompt(context, query.message.chat_id)
    return NEWCLIENT_GOAL


async def newclient_pedals_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is not None:
        await message.reply_text("🚴 Выберите вариант педалей при помощи кнопок.")
    return NEWCLIENT_PEDALS


async def newclient_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return NEWCLIENT_GOAL
    if not ensure_admin_message(update):
        return ConversationHandler.END

    value = (update.message.text or "").strip()
    form = _newclient_get_form(context)
    form["goal"] = value or None

    return await _newclient_finalize_creation(
        context=context,
        chat_id=update.message.chat_id,
        send_message=update.message.reply_text,
    )


async def newclient_skip_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_GOAL
    if not await ensure_admin_callback(query):
        return NEWCLIENT_GOAL

    form = _newclient_get_form(context)
    form["goal"] = None
    try:
        await query.edit_message_text("ℹ️ Цель не указана.")
    except Exception:
        LOGGER.debug("Failed to edit goal skip message", exc_info=True)

    return await _newclient_finalize_creation(
        context=context,
        chat_id=query.message.chat_id,
        send_message=lambda text: context.bot.send_message(query.message.chat_id, text),
    )


async def newclient_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _newclient_reset_form(context)
    message = update.effective_message
    if message is not None:
        await message.reply_text("Создание клиента отменено.")
    return ConversationHandler.END


async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    try:
        stats = await asyncio.to_thread(get_clients_stats)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load clients stats")
        await update.message.reply_text(f"❌ Ошибка получения статистики клиентов: {exc}")
        return

    stats = stats or {}
    total = int(stats.get("total") or 0)
    lines = [
        "📊 Статистика клиентов",
        f"👥 Всего: {total}",
        _format_metric_range("📏 Рост", stats.get("min_height"), stats.get("max_height"), "см"),
        _format_metric_range("⚡ FTP", stats.get("min_ftp"), stats.get("max_ftp"), "Вт"),
    ]
    await update.message.reply_text("\n".join(lines))


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


async def uploadworkout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    tokens = context.args or []
    account_ids, missing = resolve_account_tokens(tokens)
    if missing:
        await update.message.reply_text(
            "⚠️ Не удалось распознать аккаунты: {0}".format(", ".join(missing))
        )
        return

    reply_document = None
    if update.message.reply_to_message and update.message.reply_to_message.document:
        reply_document = update.message.reply_to_message.document

    if reply_document and (reply_document.file_name or "").lower().endswith(".zwo"):
        if not account_ids:
            await update.message.reply_text(
                "ℹ️ Укажите аккаунт или all, например: /uploadworkout all (в ответ на файл)."
            )
            return
        _pop_pending_workout_upload(context.user_data)
        await process_workout_document(reply_document, update.message, account_ids)
        return

    if account_ids:
        _pop_pending_workout_upload(context.user_data)
        _set_pending_workout_upload(context.user_data, account_ids)
        if len(account_ids) == len(ACCOUNT_REGISTRY):
            target = "все аккаунты"
        elif len(account_ids) == 1:
            target = ACCOUNT_REGISTRY[account_ids[0]].name
        else:
            target = ", ".join(
                ACCOUNT_REGISTRY[acc].name for acc in account_ids if acc in ACCOUNT_REGISTRY
            )
        await update.message.reply_text(
            f"📄 Пришлите файл тренировки ZWO для загрузки в {target}."
        )
        return

    await show_account_selection(message=update.message, kind="workout")


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


async def combinate_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    _set_pending_combinate(context.user_data, update.message.chat_id)

    prompt = (
        "📝 Пришлите фамилии клиентов отдельным сообщением (по одной фамилии на строку).\n"
        "Если передумали, нажмите «Отмена»."
    )
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton(text="Отмена", callback_data="combinate_cancel")]]
    )
    await update.message.reply_text(prompt, reply_markup=markup)


async def _process_combinate_text(
    message: Message, context: ContextTypes.DEFAULT_TYPE, raw_text: str
) -> bool:
    cleaned = (raw_text or "").strip()
    if not cleaned:
        await message.reply_text("ℹ️ Укажите фамилии клиентов (по одной в строке).")
        return False

    candidate_lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    if not candidate_lines:
        await message.reply_text("ℹ️ Укажите фамилии клиентов (по одной в строке).")
        return False

    unique_terms: List[str] = list(dict.fromkeys(candidate_lines))

    selected_clients: List[Dict[str, Any]] = []
    ambiguous_terms: List[Tuple[str, List[Dict[str, Any]]]] = []
    missing_terms: List[str] = []

    for index, term in enumerate(unique_terms):
        term_lower = term.lower()
        try:
            results = await asyncio.to_thread(search_clients, term, 15)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to search clients for %s", term)
            await message.reply_text(f"❌ Ошибка поиска клиента «{term}»: {exc}")
            return False

        if not results:
            missing_terms.append(term)
            continue

        scored_results: List[Tuple[int, Dict[str, Any]]] = []
        for record in results:
            last_name = (record.get("last_name") or "").strip().lower()
            full_name = (record.get("full_name") or "").strip().lower()
            score = 3
            if last_name == term_lower or full_name == term_lower:
                score = 0
            elif term_lower and term_lower in last_name:
                score = 1
            elif term_lower and term_lower in full_name:
                score = 2
            scored_results.append((score, record))
        scored_results.sort(key=lambda item: item[0])

        if not scored_results:
            missing_terms.append(term)
            continue

        best_score = scored_results[0][0]
        best_matches = [record for score, record in scored_results if score == best_score]

        if len(best_matches) == 1:
            record = dict(best_matches[0])
            record["_requested_term"] = term
            record["_order_index"] = index
            selected_clients.append(record)
        else:
            ambiguous_terms.append((term, best_matches[:5]))

    if not selected_clients:
        parts: List[str] = []
        if missing_terms:
            missing_text = ", ".join(missing_terms)
            parts.append(f"🔍 Не найдено клиентов: {html.escape(missing_text)}.")
        if ambiguous_terms:
            lines = []
            for term, matches in ambiguous_terms:
                options = ", ".join(html.escape(client_display_name(item)) for item in matches)
                lines.append(f"• {html.escape(term)} → {options}")
            parts.append("❔ Уточните фамилии:\n" + "\n".join(lines))
        if not parts:
            parts.append("⚠️ Не удалось распознать ни одного клиента.")
        await message.reply_text("\n".join(parts), parse_mode=ParseMode.HTML)
        return False

    try:
        await asyncio.to_thread(ensure_bikes_table)
        bikes = await asyncio.to_thread(list_bikes)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load bikes for combinate command")
        await message.reply_text(f"❌ Ошибка чтения базы велосипедов: {exc}")
        return False
    if not bikes:
        await message.reply_text("⚠️ В базе нет зарегистрированных велосипедов.")
        return False

    try:
        await asyncio.to_thread(ensure_trainers_table)
        trainers = await asyncio.to_thread(list_trainers)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load trainers for combinate command")
        await message.reply_text(f"❌ Ошибка чтения базы станков: {exc}")
        return False

    trainer_suggestions_map = _build_trainer_suggestions(bikes, trainers) if trainers else {}

    clients_sorted = sorted(selected_clients, key=lambda item: item.get("_order_index", 0))
    clients_with_height: List[Tuple[Dict[str, Any], float]] = []
    for record in clients_sorted:
        height_value = _parse_height_cm(record.get("height"))
        if height_value is not None:
            clients_with_height.append((record, height_value))

    height_map: Dict[int, float] = {}
    for record, height_value in clients_with_height:
        client_id = record.get("id")
        if isinstance(client_id, int):
            height_map[client_id] = height_value

    def _format_bike_title(bike: Dict[str, Any]) -> str:
        title = html.escape(bike.get("title") or "Без названия")
        owner = bike.get("owner")
        if owner:
            return f"{title} ({html.escape(str(owner))})"
        return title

    def _format_trainer_label(trainer: Dict[str, Any]) -> str:
        code = _format_trainer_code(trainer.get("code"))
        display_name = trainer.get("display_name") or trainer.get("title")
        if code and display_name:
            return f"{code} — {display_name}"
        if code:
            return code
        if display_name:
            return str(display_name)
        return "Без названия"

    def _normalize_bike_name(value: Any) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).strip().lower())

    def _match_favorite_bike(preferred_raw: Any) -> Optional[Dict[str, Any]]:
        normalized = _normalize_bike_name(preferred_raw)
        if not normalized:
            return None

        best_match: Optional[Tuple[int, Dict[str, Any]]] = None
        for bike in bikes:
            title_norm = _normalize_bike_name(bike.get("title"))
            owner_norm = _normalize_bike_name(bike.get("owner"))
            combined_norm = _normalize_bike_name(
                f"{bike.get('title') or ''} {bike.get('owner') or ''}"
            )

            score: Optional[int] = None
            if normalized and normalized == title_norm:
                score = 0
            elif normalized and normalized == combined_norm:
                score = 1
            elif normalized and title_norm and normalized in title_norm:
                score = 2
            elif normalized and combined_norm and normalized in combined_norm:
                score = 3
            elif normalized and owner_norm and normalized == owner_norm:
                score = 4

            if score is None:
                continue

            if best_match is None or score < best_match[0]:
                best_match = (score, bike)

        return best_match[1] if best_match else None

    def _prioritize_preferred_bike(
        candidates: List[Dict[str, Any]], preferred: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if preferred is None:
            return candidates[:5]

        result: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()

        preferred_id = preferred.get("id") if isinstance(preferred, dict) else None
        if isinstance(preferred_id, int):
            seen_ids.add(preferred_id)
        result.append(preferred)

        for bike in candidates:
            bike_id = bike.get("id")
            if isinstance(bike_id, int) and bike_id in seen_ids:
                continue
            if bike is preferred:
                continue
            result.append(bike)
            if len(result) >= 5:
                break

        return result[:5]

    bike_candidates: Dict[int, List[Dict[str, Any]]] = {}
    favorite_matches: Dict[int, Dict[str, Any]] = {}
    for record, height_value in clients_with_height:
        client_id = record.get("id")
        if not isinstance(client_id, int):
            continue
        matching_bikes = [bike for bike in bikes if _bike_height_matches(bike, height_value)]
        pool = matching_bikes if matching_bikes else bikes
        sorted_candidates = sorted(
            pool,
            key=lambda bike: (
                _bike_height_distance(bike, height_value),
                1 if bike.get("position") is None else 0,
                bike.get("position") or 0,
                (bike.get("title") or "").lower(),
            ),
        )
        preferred_bike_raw = record.get("favorite_bike")
        preferred_bike = _match_favorite_bike(preferred_bike_raw) if preferred_bike_raw else None
        if preferred_bike is not None:
            favorite_matches[client_id] = preferred_bike
        bike_candidates[client_id] = _prioritize_preferred_bike(sorted_candidates, preferred_bike)

    assignments: Dict[int, Dict[str, Any]] = {}
    used_bike_ids: set[int] = set()
    for record, height_value in sorted(clients_with_height, key=lambda item: item[1], reverse=True):
        client_id = record.get("id")
        if not isinstance(client_id, int):
            continue
        preferred_bike = favorite_matches.get(client_id)
        if preferred_bike:
            bike_id = preferred_bike.get("id")
            if isinstance(bike_id, int) and bike_id not in used_bike_ids:
                assignments[client_id] = preferred_bike
                used_bike_ids.add(bike_id)
                continue

        candidates = bike_candidates.get(client_id, [])
        for bike in candidates:
            bike_id = bike.get("id")
            if isinstance(bike_id, int) and bike_id not in used_bike_ids:
                assignments[client_id] = bike
                used_bike_ids.add(bike_id)
                break

    lines: List[str] = []
    lines.append("<b>🚲 Предложение рассадки</b>")
    for record in clients_sorted:
        display_name = client_display_name(record)
        requested = record.get("_requested_term")
        header = f"<b>{html.escape(display_name)}</b>"
        if requested and requested.lower() != (display_name or "").lower():
            header += f" <i>(запрос «{html.escape(str(requested))}»)</i>"
        lines.append(header)

        client_id = record.get("id")
        height_value = height_map.get(client_id) if isinstance(client_id, int) else None
        if height_value is None:
            lines.append("• ⚠️ В анкете нет роста, подбор велосипеда невозможен.")
            continue

        lines.append(f"• 📏 Рост: {height_value:g} см")

        assigned_bike = assignments.get(client_id) if isinstance(client_id, int) else None
        preferred_bike = favorite_matches.get(client_id) if isinstance(client_id, int) else None
        assigned_is_preferred = assigned_bike is not None and assigned_bike is preferred_bike
        if assigned_bike:
            bike_title = _format_bike_title(assigned_bike)
            suffix = " (любимый)" if assigned_is_preferred else ""
            lines.append(f"• ✅ Основной вариант: {bike_title}{suffix}")

            trainer_options = trainer_suggestions_map.get(assigned_bike.get("id")) or []
            if trainer_options:
                trainer_titles = [html.escape(_format_trainer_label(option)) for option in trainer_options[:3]]
                lines.append(f"  └─ Станки: {', '.join(trainer_titles)}")
        else:
            if preferred_bike:
                preferred_title = _format_bike_title(preferred_bike)
                lines.append(f"• ⚠️ Любимый велосипед недоступен: {preferred_title}")
            else:
                lines.append("• ⚠️ Не удалось подобрать уникальный велосипед из доступных.")

        alternatives = [
            bike
            for bike in bike_candidates.get(client_id, [])
            if bike is not assignments.get(client_id)
        ]
        if alternatives:
            alt_titles = [_format_bike_title(bike) for bike in alternatives[:3]]
            lines.append(f"• 🔁 Альтернативы: {', '.join(alt_titles)}")

    if missing_terms:
        escaped = ", ".join(html.escape(term) for term in missing_terms)
        lines.append(f"\n🔍 Не найдено клиентов по запросам: {escaped}.")

    if ambiguous_terms:
        lines.append("\n❔ Уточните следующие фамилии:")
        for term, matches in ambiguous_terms:
            options = ", ".join(html.escape(client_display_name(item)) for item in matches)
            lines.append(f"• {html.escape(term)} → {options}")

    full_text = "\n".join(lines)
    for chunk in _split_html_message(full_text):
        await message.reply_text(chunk, parse_mode=ParseMode.HTML)

    return True


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

    search_term = " ".join(context.args).strip() if context.args else ""
    context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)
    context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)
    context.user_data["last_stands_search"] = search_term

    try:
        text, markup = await build_stands_view(search_term)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch trainers")
        await update.message.reply_text(
            f"❌ Ошибка получения списка станков: {exc}"
        )
        return

    if markup is None:
        await update.message.reply_text(text)
    else:
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if not query.data:
        return

    if not await ensure_admin_callback(query):
        return

    parts = query.data.split("|")
    action = parts[0]

    if action == "setclient" and len(parts) >= 3:
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
    elif action == "workout_select" and len(parts) >= 2:
        target = parts[1]
        if target.upper() == "ALL":
            account_ids = list(ACCOUNT_REGISTRY.keys())
            if not account_ids:
                await query.edit_message_text("⚠️ Аккаунты не настроены.")
                return
            label = "все аккаунты"
        else:
            account_id = resolve_account_identifier(target)
            if account_id is None:
                await query.edit_message_text("⚠️ Аккаунт не найден.")
                return
            account_ids = [account_id]
            label = ACCOUNT_REGISTRY[account_id].name
        _set_pending_workout_upload(context.user_data, account_ids)
        await query.edit_message_text(
            f"📄 Пришлите файл тренировки ZWO для загрузки в {label}."
        )
    elif action == "combinate_cancel":
        _clear_pending_combinate(context.user_data)
        await query.edit_message_text("⛔ Подбор рассадки отменён.")
    elif action == "client_close":
        pending_edit = context.user_data.get("pending_client_edit")
        if (
            pending_edit
            and pending_edit.get("chat_id") == query.message.chat_id
            and pending_edit.get("message_id") == query.message.message_id
        ):
            context.user_data.pop("pending_client_edit", None)
        try:
            await query.message.delete()
        except Exception:  # noqa: BLE001
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:  # noqa: BLE001
                LOGGER.debug("client_close action failed for message %s", query.message)
        return
    elif action == "account_show" and len(parts) >= 2:
        target = parts[1]
        if target.upper() == "ALL":
            await show_all_accounts_via_callback(query)
        else:
            await show_account_via_callback(query, target)
    elif action == "client_info" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await show_client_info(query, context, client_id)
    elif action == "client_assign_prepare" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
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

        client_name = html.escape(client_display_name(record))
        account_list = format_account_list()
        if account_list:
            body = html.escape(account_list)
        else:
            body = "Аккаунты не настроены."
        text = (
            f"👤 <b>{client_name}</b>\n"
            "Выберите аккаунт WattAttack для применения данных клиента:\n"
            f"{body}"
        )
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_client_assign_keyboard(client_id),
        )
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
    elif action == "client_favbike_page" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            page = max(0, int(parts[2]))
        except ValueError:
            await query.edit_message_text("⚠️ Некорректные параметры страницы.")
            return
        pending = context.user_data.get("pending_client_edit")
        if isinstance(pending, dict) and pending.get("client_id") == client_id:
            pending.update(
                {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id,
                    "field": "favorite_bike",
                    "mode": "picker",
                }
            )
        else:
            context.user_data["pending_client_edit"] = {
                "client_id": client_id,
                "field": "favorite_bike",
                "chat_id": query.message.chat_id,
                "message_id": query.message.message_id,
                "label": CLIENT_EDIT_FIELDS["favorite_bike"]["label"],
                "client_name": "",
                "mode": "picker",
            }
        await render_client_favorite_bike_picker(
            context,
            query.message.chat_id,
            query.message.message_id,
            client_id,
            page=page,
        )
    elif action == "client_favbike_set" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            bike_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректные параметры велосипеда.")
            return
        await set_client_favorite_bike(query, context, client_id, bike_id)
    elif action == "client_favbike_clear" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await clear_client_favorite_bike(query, context, client_id)
    elif action == "client_pedals_set" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        pedal_code = parts[2]
        await set_client_pedals(query, context, client_id, pedal_code)
    elif action == "client_pedals_clear" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await clear_client_pedals(query, context, client_id)
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
    elif action == "trainer_set_axle" and len(parts) >= 3:
        axle_value = parts[1].upper()
        if axle_value not in {"ОСЬ", "ЭКС"}:
            await query.answer("Некорректное значение", show_alert=True)
            return
        try:
            trainer_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор станка.")
            return
        try:
            await asyncio.to_thread(update_trainer_fields, trainer_id, axle_types=axle_value)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to update trainer %s axle", trainer_id)
            await query.edit_message_text(f"❌ Не удалось обновить ось: {exc}")
            return
        await query.answer("Ось обновлена")
        await show_trainer_info(query, context, trainer_id)
    elif action == "stands_list":
        search_term = context.user_data.get("last_stands_search", "")
        try:
            text, markup = await build_stands_view(search_term)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch trainers")
            await query.edit_message_text(f"❌ Ошибка получения списка станков: {exc}")
            return

        context.user_data.pop(PENDING_TRAINER_EDIT_KEY, None)
        context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)

        if markup is None:
            await query.edit_message_text(text)
        else:
            await query.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
            )
    elif action == "stands_close":
        try:
            await query.message.delete()
        except Exception:  # noqa: BLE001
            LOGGER.debug("stands_close delete failed for message %s", query.message)
        return
    elif action == "noop":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:  # noqa: BLE001
            LOGGER.debug("noop action ignored for message %s", query.message)
        return
    else:
        await query.edit_message_text("❓ Неизвестное действие.")


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
        "prompt": "Выберите велосипед из списка ниже или очистите текущий выбор.",
    },
    "pedals": {
        "label": "🚴‍♂️ Педали",
        "prompt": "Выберите тип педалей из списка ниже или очистите текущее значение.",
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
        "label": "🔧 Ось",
        "prompt": "Выберите тип оси: ОСЬ или ЭКС.",
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
                    text="👥 Посадить на аккаунт",
                    callback_data=f"client_assign_prepare|{client_id}",
                )
            ],
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
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="client_close")],
        ]
    )


def build_client_assign_keyboard(client_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for account_id in sorted(ACCOUNT_REGISTRY):
        account = ACCOUNT_REGISTRY[account_id]
        alias = normalize_account_id(account_id)
        label = f"{alias} — {account.name}"
        rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"setclient|{account_id}|{client_id}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="↩️ Назад",
                callback_data=f"client_info|{client_id}",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


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


def _format_bike_choice_label(record: Dict[str, Any]) -> str:
    title = str(record.get("title") or f"id={record.get('id')}")
    size_label = record.get("size_label") or record.get("frame_size_cm")
    owner = record.get("owner")

    suffix_parts: List[str] = []
    if size_label:
        suffix_parts.append(str(size_label))
    if owner:
        suffix_parts.append(str(owner))

    if suffix_parts:
        return f"{title} • {' / '.join(part.strip() for part in suffix_parts if part)}"
    return title


def build_client_bike_picker_markup(
    client_id: int,
    bikes: List[Dict[str, Any]],
    page: int,
    total_count: int,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    if bikes:
        for record in bikes:
            label = _format_bike_choice_label(record)
            rows.append(
                [
                    InlineKeyboardButton(
                        text=label,
                        callback_data=f"client_favbike_set|{client_id}|{record['id']}",
                    )
                ]
            )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🚫 Велосипеды не найдены",
                    callback_data="noop",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="🚫 Очистить выбор",
                callback_data=f"client_favbike_clear|{client_id}",
            )
        ]
    )

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"client_favbike_page|{client_id}|{page - 1}",
            )
        )
    if (page + 1) * CLIENT_BIKE_PICK_PAGE_SIZE < total_count:
        nav_row.append(
            InlineKeyboardButton(
                text="➡️ Далее",
                callback_data=f"client_favbike_page|{client_id}|{page + 1}",
            )
        )
    if nav_row:
        rows.append(nav_row)

    rows.append(
        [
            InlineKeyboardButton(
                text="↩️ Назад",
                callback_data=f"client_info|{client_id}",
            ),
            InlineKeyboardButton(
                text="❌ Отмена",
                callback_data=f"client_edit_cancel|{client_id}",
            ),
        ]
    )
    return InlineKeyboardMarkup(rows)


async def render_client_favorite_bike_picker(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    client_id: int,
    page: int = 0,
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
    details_text = format_client_details(record, bike_suggestions, height_cm, trainer_map)
    display_name = client_display_name(record)
    prompt = CLIENT_EDIT_FIELDS["favorite_bike"]["prompt"]

    pending = context.user_data.get("pending_client_edit")
    if isinstance(pending, dict) and pending.get("client_id") == client_id:
        pending.setdefault("client_name", display_name)
        pending["label"] = CLIENT_EDIT_FIELDS["favorite_bike"]["label"]
        pending["mode"] = "picker"

    try:
        await asyncio.to_thread(ensure_bikes_table)
        total_count = await asyncio.to_thread(bikes_count)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to prepare bikes list for client %s", client_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения списка велосипедов: {exc}",
        )
        return

    limit = max(1, CLIENT_BIKE_PICK_PAGE_SIZE)
    page = max(page, 0)
    max_page = (total_count - 1) // limit if total_count else 0
    if page > max_page:
        page = max_page
    offset = page * limit

    try:
        bikes = await asyncio.to_thread(list_bikes, limit, offset)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch bikes page (page=%s) for client %s", page, client_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения списка велосипедов: {exc}",
        )
        return

    text = (
        f"{details_text}\n\n"
        f"✏️ <i>{html.escape(prompt)}</i>\n"
        f"👤 <i>Клиент: {html.escape(display_name)}</i>"
    )

    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_client_bike_picker_markup(client_id, bikes, page, total_count),
    )


def build_client_pedals_picker_markup(client_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=label,
                callback_data=f"client_pedals_set|{client_id}|{code}",
            )
        ]
        for label, code in PEDAL_OPTIONS
    ]
    rows.append(
        [
            InlineKeyboardButton(
                text="🚫 Очистить выбор",
                callback_data=f"client_pedals_clear|{client_id}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="↩️ Назад",
                callback_data=f"client_info|{client_id}",
            ),
            InlineKeyboardButton(
                text="❌ Отмена",
                callback_data=f"client_edit_cancel|{client_id}",
            ),
        ]
    )
    return InlineKeyboardMarkup(rows)


async def render_client_pedals_picker(
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
    details_text = format_client_details(record, bike_suggestions, height_cm, trainer_map)
    display_name = client_display_name(record)
    prompt = CLIENT_EDIT_FIELDS["pedals"]["prompt"]

    pending = context.user_data.get("pending_client_edit")
    if isinstance(pending, dict) and pending.get("client_id") == client_id:
        pending.setdefault("client_name", display_name)
        pending["label"] = CLIENT_EDIT_FIELDS["pedals"]["label"]
        pending["mode"] = "picker"

    text = (
        f"{details_text}\n\n"
        f"✏️ <i>{html.escape(prompt)}</i>\n"
        f"👤 <i>Клиент: {html.escape(display_name)}</i>"
    )

    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_client_pedals_picker_markup(client_id),
    )


async def set_client_pedals(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    pedal_code: str,
) -> None:
    label = PEDAL_OPTION_LABEL_BY_CODE.get(pedal_code)
    if not label:
        await query.answer("Неизвестный вариант педалей.", show_alert=True)
        return

    try:
        await asyncio.to_thread(update_client_fields, client_id, pedals=label)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to update client %s pedals to %s", client_id, pedal_code)
        await query.edit_message_text(f"❌ Не удалось обновить педали клиента: {exc}")
        return

    pending = context.user_data.get("pending_client_edit") or {}
    client_name = pending.get("client_name")
    field_label = CLIENT_EDIT_FIELDS["pedals"]["label"]
    if client_name:
        success_text = f"✅ {field_label} для {client_name} обновлены."
    else:
        success_text = f"✅ {field_label} обновлены."
    await context.bot.send_message(chat_id=query.message.chat_id, text=success_text)

    context.user_data.pop("pending_client_edit", None)
    await query.answer("Педали обновлены.")
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
    )


async def clear_client_pedals(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
) -> None:
    try:
        await asyncio.to_thread(update_client_fields, client_id, pedals=None)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to clear pedals for client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось очистить педали клиента: {exc}")
        return

    pending = context.user_data.get("pending_client_edit") or {}
    client_name = pending.get("client_name")
    field_label = CLIENT_EDIT_FIELDS["pedals"]["label"]
    if client_name:
        success_text = f"✅ {field_label} для {client_name} очищены."
    else:
        success_text = f"✅ {field_label} очищены."
    await context.bot.send_message(chat_id=query.message.chat_id, text=success_text)

    context.user_data.pop("pending_client_edit", None)
    await query.answer("Педали очищены.")
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
    )


async def set_client_favorite_bike(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    bike_id: int,
) -> None:
    try:
        bike_record = await asyncio.to_thread(get_bike, bike_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load bike %s for client %s", bike_id, client_id)
        await query.answer("Ошибка получения велосипеда.", show_alert=True)
        return

    if not bike_record:
        await query.answer("Велосипед не найден.", show_alert=True)
        return

    bike_title = bike_record.get("title") or f"id={bike_id}"

    try:
        await asyncio.to_thread(update_client_fields, client_id, favorite_bike=bike_title)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to update client %s favorite bike to %s", client_id, bike_id)
        await query.edit_message_text(f"❌ Не удалось обновить велосипед клиента: {exc}")
        return

    pending = context.user_data.get("pending_client_edit") or {}
    client_name = pending.get("client_name")
    label = CLIENT_EDIT_FIELDS["favorite_bike"]["label"]
    if client_name:
        success_text = f"✅ {label} для {client_name} обновлён."
    else:
        success_text = f"✅ {label} обновлён."
    await context.bot.send_message(chat_id=query.message.chat_id, text=success_text)

    context.user_data.pop("pending_client_edit", None)
    await query.answer("Любимый велосипед обновлён.")
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
    )


async def clear_client_favorite_bike(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
) -> None:
    try:
        await asyncio.to_thread(update_client_fields, client_id, favorite_bike=None)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to clear favorite bike for client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось очистить велосипед клиента: {exc}")
        return

    pending = context.user_data.get("pending_client_edit") or {}
    client_name = pending.get("client_name")
    label = CLIENT_EDIT_FIELDS["favorite_bike"]["label"]
    if client_name:
        success_text = f"✅ {label} для {client_name} очищен."
    else:
        success_text = f"✅ {label} очищен."
    await context.bot.send_message(chat_id=query.message.chat_id, text=success_text)

    context.user_data.pop("pending_client_edit", None)
    await query.answer("Любимый велосипед очищен.")
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
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


async def build_stands_view(search_term: str) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    await asyncio.to_thread(ensure_trainers_table)
    if search_term:
        trainers = await asyncio.to_thread(search_trainers, search_term, 30)
        total_count = len(trainers)
    else:
        trainers = await asyncio.to_thread(list_trainers, 50)
        total_count = await asyncio.to_thread(trainers_count)

    if not trainers:
        if search_term:
            return (
                f"🚫 Станки по запросу «{html.escape(search_term)}» не найдены.",
                None,
            )
        return ("🚫 В базе нет станков.", None)

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
    keyboard_rows.append(
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="stands_close")]
    )

    return text, InlineKeyboardMarkup(keyboard_rows)


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

    text = format_client_details(record)
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
                    InlineKeyboardButton(text="❌ Закрыть", callback_data="client_close"),
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

    display_name = client_display_name(record)

    if field == "favorite_bike":
        context.user_data["pending_client_edit"] = {
            "client_id": client_id,
            "field": field,
            "chat_id": query.message.chat_id,
            "message_id": query.message.message_id,
            "label": metadata["label"],
            "client_name": display_name,
            "mode": "picker",
        }
        await render_client_favorite_bike_picker(
            context,
            query.message.chat_id,
            query.message.message_id,
            client_id,
            page=0,
        )
        return
    if field == "pedals":
        context.user_data["pending_client_edit"] = {
            "client_id": client_id,
            "field": field,
            "chat_id": query.message.chat_id,
            "message_id": query.message.message_id,
            "label": metadata["label"],
            "client_name": display_name,
            "mode": "picker",
        }
        await render_client_pedals_picker(
            context,
            query.message.chat_id,
            query.message.message_id,
            client_id,
        )
        return

    bike_suggestions, height_cm, trainer_inventory = await get_bike_suggestions_for_client(record)
    trainer_map = (
        _build_trainer_suggestions(bike_suggestions, trainer_inventory)
        if bike_suggestions and trainer_inventory
        else None
    )
    details_text = format_client_details(record, bike_suggestions, height_cm, trainer_map)
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
        "mode": "text",
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
    mode = pending.get("mode", "text")
    if mode != "text":
        return False

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
    trainer_name = trainer_display_name(record)

    if field == "axle_types":
        buttons = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text="ОСЬ",
                        callback_data=f"trainer_set_axle|ОСЬ|{trainer_id}",
                    ),
                    InlineKeyboardButton(
                        text="ЭКС",
                        callback_data=f"trainer_set_axle|ЭКС|{trainer_id}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="↩️ Назад",
                        callback_data=f"trainer_info|{trainer_id}",
                    ),
                ],
            ]
        )
        await query.edit_message_text(
            f"{details_text}\n\n✏️ <i>Выберите тип оси:</i>\n🛠 <i>Станок: {html.escape(trainer_name)}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=buttons,
        )
        return

    prompt = metadata["prompt"]

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

    return "\n".join(lines)


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
        elif kind == "workout":
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
    elif kind == "setclient":
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


async def show_all_accounts_via_callback(query) -> None:
    if not ACCOUNT_REGISTRY:
        await query.edit_message_text("⚠️ Аккаунты не настроены.")
        return

    summaries: List[str] = []
    for account_id in sorted(ACCOUNT_REGISTRY):
        account = ACCOUNT_REGISTRY[account_id]
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
    combinate_pending = context.user_data.get(PENDING_COMBINATE_KEY)
    if combinate_pending and combinate_pending.get("chat_id") == update.message.chat_id:
        success = await _process_combinate_text(update.message, context, update.message.text)
        if success:
            _clear_pending_combinate(context.user_data)
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


async def workout_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return
    if not ensure_admin_message(update):
        return

    document = update.message.document
    file_name = (document.file_name or "").lower()
    if not file_name.endswith(".zwo"):
        return

    caption = update.message.caption or ""
    command = None
    args: List[str] = []
    stripped = caption.strip()
    if stripped.startswith("/"):
        parts = stripped.split()
        command = parts[0].lower()
        args = parts[1:]

    account_ids: List[str] = []
    if command == WORKOUT_UPLOAD_COMMAND:
        account_ids, missing = resolve_account_tokens(args)
        if missing:
            await update.message.reply_text(
                "⚠️ Не удалось распознать аккаунты: {0}".format(", ".join(missing))
            )
            return
        _pop_pending_workout_upload(context.user_data)
    else:
        pending = _pop_pending_workout_upload(context.user_data)
        if pending:
            account_ids = list(pending.get("account_ids") or [])

    if not account_ids:
        await update.message.reply_text(
            "ℹ️ Сначала выберите аккаунт командой /uploadworkout."
        )
        return

    await process_workout_document(document, update.message, account_ids)


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
            "ℹ️ Чтобы импортировать данные, используйте /uploadclients, /uploadbikes, /uploadstands, "
            "/uploadworkout или добавьте команду в подпись к файлу."
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
    application.add_handler(CommandHandler("account", account_handler))
    application.add_handler(CommandHandler("combinate", combinate_handler))
    application.add_handler(CommandHandler("client", client_handler))
    application.add_handler(CommandHandler("stats", stats_handler))
    application.add_handler(CommandHandler("bikes", bikes_handler))
    application.add_handler(CommandHandler("stands", stands_handler))
    application.add_handler(CommandHandler("setclient", setclient_handler))
    application.add_handler(CommandHandler("admins", admins_handler))
    application.add_handler(CommandHandler("addadmin", addadmin_handler))
    application.add_handler(CommandHandler("removeadmin", removeadmin_handler))
    application.add_handler(CommandHandler("uploadclients", uploadclients_handler))
    application.add_handler(CommandHandler("uploadbikes", uploadbikes_handler))
    application.add_handler(CommandHandler("uploadstands", uploadstands_handler))
    application.add_handler(CommandHandler("uploadworkout", uploadworkout_handler))
    newclient_conversation = ConversationHandler(
        entry_points=[CommandHandler("newclient", newclient_start)],
        states={
            NEWCLIENT_FIRST_NAME: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_first_name)
            ],
            NEWCLIENT_LAST_NAME: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_last_name)
            ],
            NEWCLIENT_WEIGHT: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_weight)
            ],
            NEWCLIENT_HEIGHT: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_height)
            ],
            NEWCLIENT_GENDER: [
                CallbackQueryHandler(
                    newclient_gender_selection, pattern=r"^newclient:gender:(male|female)$"
                ),
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_gender_prompt),
            ],
            NEWCLIENT_FTP: [
                CallbackQueryHandler(newclient_skip_ftp, pattern=r"^newclient:ftp:skip$"),
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_ftp),
            ],
            NEWCLIENT_PEDALS: [
                CallbackQueryHandler(
                    newclient_pedals_selection, pattern=r"^newclient:pedals:[^:]+$"
                ),
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_pedals_prompt),
            ],
            NEWCLIENT_GOAL: [
                CallbackQueryHandler(newclient_skip_goal, pattern=r"^newclient:goal:skip$"),
                MessageHandler(filters.TEXT & (~filters.COMMAND), newclient_goal),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", newclient_cancel),
            CommandHandler("stop", newclient_cancel),
        ],
        allow_reentry=True,
    )
    application.add_handler(newclient_conversation)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), text_search_handler))
    workout_filter = (
        filters.Document.FileExtension("zwo") | filters.Document.FileExtension("ZWO")
    )
    application.add_handler(
        MessageHandler(workout_filter, workout_document_handler)
    )
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
