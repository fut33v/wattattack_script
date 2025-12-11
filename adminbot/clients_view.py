#!/usr/bin/env python3
"""Telegram bot for managing WattAttack profiles, clients, and inventory."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
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
from repositories.client_link_repository import link_user_to_client, get_link_by_client
from repositories.vk_client_link_repository import get_link_by_client as get_vk_link_by_client
from repositories.link_requests_repository import get_link_request, delete_link_request
from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
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
from repositories.pedals_repository import (
    ensure_pedals_table,
    list_pedals,
)
from repositories.schedule_repository import (
    book_available_reservation,
    get_reservation,
    get_slot_with_reservations,
    list_available_slots,
    list_future_reservations_for_client,
    update_reservation,
)
from repositories.layout_repository import (
    ensure_layout_table,
    list_layout_details,
    get_assignment_for_bike,
    set_bike_assignment,
    clear_bike_assignment_for_bike,
)
from wattattack_activities import WattAttackClient
from wattattack_profiles import apply_client_profile as apply_wattattack_profile
from wattattack_workouts import (
    build_workout_payload,
    calculate_workout_metrics,
    parse_zwo_workout,
    zwo_to_chart_data,
)
from adminbot import events as events_admin
from adminbot import intervals as intervals_admin
from adminbot import wizard as wizard_admin
from adminbot import menu as menu_admin
from adminbot import accounts_view
from adminbot import admins_view
from adminbot import uploads as uploads_admin
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
CLIENT_BOT_TOKEN_ENV = "KRUTILKAVN_BOT_TOKEN"
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

LOCAL_TIMEZONE = ZoneInfo(os.environ.get("WATTATTACK_LOCAL_TZ", "Europe/Moscow"))
BOOKING_LOOKAHEAD_DAYS = int(os.environ.get("ADMINBOT_BOOKING_LOOKAHEAD_DAYS", "21"))
BOOKING_LOOKAHEAD = timedelta(days=max(1, BOOKING_LOOKAHEAD_DAYS))
BOOKING_CUTOFF_MINUTES = int(os.environ.get("ADMINBOT_BOOKING_CUTOFF_MINUTES", "90"))
BOOKING_CUTOFF = timedelta(minutes=max(1, BOOKING_CUTOFF_MINUTES))
BOOKING_SLOTS_LIMIT = max(10, int(os.environ.get("ADMINBOT_BOOKING_SLOTS_LIMIT", "80")))
WEEKDAY_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
BOOKING_DAY_LIMIT = max(1, int(os.environ.get("ADMINBOT_BOOKING_DAY_LIMIT", "7")))
BOOKING_REASSIGN_LIMIT = max(3, int(os.environ.get("ADMINBOT_REASSIGN_OPTIONS", "12")))
CLIENT_BOOKINGS_LIMIT = max(5, int(os.environ.get("ADMINBOT_CLIENT_BOOKINGS_LIMIT", "10")))
_CLIENT_BOT = None
_CLIENT_BOT_WARNED = False
START_MESSAGE = (
    "👋 Это главное меню админа Крутилки.\n"
    "Выберите действие в меню ниже или используйте команды напрямую.\n"
    "Чтобы найти клиента, просто напишите его имя и фамилию."
)

def build_menu_return_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("↩️ В меню", callback_data="menu|start")]]
    )

def _get_client_bot() -> Bot | None:
    global _CLIENT_BOT, _CLIENT_BOT_WARNED
    if _CLIENT_BOT is not None:
        return _CLIENT_BOT
    token = os.environ.get(CLIENT_BOT_TOKEN_ENV)
    if not token:
        if not _CLIENT_BOT_WARNED:
            LOGGER.warning("KRUTILKAVN_BOT_TOKEN is not set; users will not be notified about link approvals")
            _CLIENT_BOT_WARNED = True
        return None
    try:
        _CLIENT_BOT = Bot(token=token)
        return _CLIENT_BOT
    except Exception:
        LOGGER.exception("Failed to init client bot for link notifications", exc_info=True)
        return None

def _format_link_client_label(client: Dict[str, Any]) -> str:
    last_name = (client.get("last_name") or "").strip()
    first_name = (client.get("first_name") or "").strip()
    full_name = (client.get("full_name") or "").strip()
    if last_name and first_name:
        display = f"{last_name} {first_name}".strip()
    elif full_name:
        display = full_name
    else:
        display = last_name or first_name or "Без имени"
    return f"{display} (ID {client.get('id')})"

async def _handle_link_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    admin_user = update.effective_user
    if query is None or admin_user is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "link":
        return
    action = parts[1]
    request_id = parts[2]

    if not db_is_admin(tg_id=admin_user.id, username=admin_user.username):
        await query.answer("Недостаточно прав.", show_alert=True)
        return

    request = get_link_request(request_id)
    if not request:
        await query.answer("Запрос не найден или уже обработан.", show_alert=True)
        try:
            await query.edit_message_text("Запрос не найден или уже обработан.")
        except Exception:
            pass
        return

    client = get_client(request["client_id"])
    if not client:
        await query.answer("Анкета не найдена.", show_alert=True)
        delete_link_request(request_id)
        try:
            await query.edit_message_text("Анкета не найдена. Запрос закрыт.")
        except Exception:
            pass
        return

    client_label = _format_link_client_label(client)
    tg_user_id = request["tg_user_id"]
    user_chat_id = request.get("user_chat_id") or tg_user_id

    if action == "approve":
        try:
            link_user_to_client(
                tg_user_id=tg_user_id,
                client_id=client["id"],
                tg_username=request.get("tg_username"),
                tg_full_name=request.get("tg_full_name"),
            )
        except Exception:
            LOGGER.exception("Failed to link user %s to client %s on approve", tg_user_id, client["id"])
            await query.answer("Не удалось привязать клиента. Попробуйте позже.", show_alert=True)
            return
        admin_text = f"✅ Привязка подтверждена.\nКлиент: {client_label}\nПользователь: id {tg_user_id}"
        user_text = (
            f"Администратор подтвердил привязку к клиенту {client_label}. "
            "Теперь вы можете пользоваться сервисом."
        )
    else:
        admin_text = f"❌ Привязка отклонена.\nКлиент: {client_label}\nПользователь: id {tg_user_id}"
        user_text = (
            f"Администратор отклонил запрос на привязку к клиенту {client_label}. "
            "Связь осталась без изменений."
        )

    delete_link_request(request_id)

    try:
        await query.edit_message_text(admin_text)
    except Exception:
        LOGGER.debug("Failed to edit approval message %s", request_id, exc_info=True)

    client_bot = _get_client_bot()
    if client_bot:
        try:
            await client_bot.send_message(chat_id=user_chat_id, text=user_text)
        except Exception:
            LOGGER.exception("Failed to notify user %s about link decision %s", tg_user_id, request_id)
    else:
        LOGGER.info("Skipping user notification for %s: client bot not configured", tg_user_id)

    await query.answer("Готово.")

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

def _local_now() -> datetime:
    return datetime.now(tz=LOCAL_TIMEZONE)

def _to_local_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(LOCAL_TIMEZONE).replace(tzinfo=None)

def _parse_date_value(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None

def _parse_time_value(value: Any) -> Optional[time]:
    if isinstance(value, time):
        return value
    if isinstance(value, datetime):
        return value.time()
    if isinstance(value, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                continue
    return None

def _slot_start_datetime(slot: Dict[str, Any]) -> Optional[datetime]:
    slot_date = _parse_date_value(slot.get("slot_date"))
    start_time_value = _parse_time_value(slot.get("start_time"))
    if slot_date is None or start_time_value is None:
        return None
    combined = datetime.combine(slot_date, start_time_value)
    return combined.replace(tzinfo=LOCAL_TIMEZONE)

def _format_time_label(value: Any) -> Optional[str]:
    parsed = _parse_time_value(value)
    if parsed is None:
        return str(value) if value is not None else None
    return parsed.strftime("%H:%M")

def _format_weekday_label(value: date) -> str:
    if not isinstance(value, date):
        return str(value)
    idx = value.weekday()
    prefix = WEEKDAY_SHORT[idx] if 0 <= idx < len(WEEKDAY_SHORT) else value.strftime("%a")
    return f"{prefix} {value.strftime('%d.%m')}"

def _format_slot_summary(slot: Dict[str, Any]) -> str:
    slot_date = _parse_date_value(slot.get("slot_date"))
    date_label = _format_weekday_label(slot_date) if slot_date else str(slot.get("slot_date"))
    start_label = _format_time_label(slot.get("start_time"))
    end_label = _format_time_label(slot.get("end_time"))
    time_part = None
    if start_label and end_label:
        time_part = f"{start_label}–{end_label}"
    elif start_label or end_label:
        time_part = start_label or end_label
    parts = [date_label]
    if time_part:
        parts.append(time_part)
    summary = " · ".join(parts)
    descriptor_raw = slot.get("label")
    descriptor = str(descriptor_raw).strip() if descriptor_raw not in (None, "") else ""
    if descriptor:
        summary = f"{summary} ({descriptor})"
    elif slot.get("session_kind") == "instructor":
        instructor = (slot.get("instructor_name") or "").strip()
        if instructor:
            summary = f"{summary} (инструктор {instructor})"
        else:
            summary = f"{summary} (с инструктором)"
    return summary

def _format_slot_button_label(slot: Dict[str, Any]) -> str:
    start_label = _format_time_label(slot.get("start_time")) or "—"
    end_label = _format_time_label(slot.get("end_time")) or ""
    if end_label:
        time_part = f"{start_label}–{end_label}"
    else:
        time_part = start_label

    descriptor_raw = slot.get("label")
    descriptor = str(descriptor_raw).strip() if descriptor_raw not in (None, "") else ""

    parts = [time_part]
    if descriptor:
        parts.append(descriptor)
    elif slot.get("session_kind") == "instructor":
        instructor = (slot.get("instructor_name") or "").strip()
        if instructor:
            parts.append(f"инструктор {instructor}")
        else:
            parts.append("с инструктором")

    free_count = slot.get("free_count")
    if isinstance(free_count, int):
        parts.append(f"мест {free_count}")

    return " · ".join(part for part in parts if part)

def _group_slots_by_day(slots: Iterable[Dict[str, Any]]) -> List[Tuple[date, List[Dict[str, Any]]]]:
    grouped: Dict[date, List[Dict[str, Any]]] = {}
    for slot in slots:
        slot_date = _parse_date_value(slot.get("slot_date"))
        if slot_date is None:
            continue
        grouped.setdefault(slot_date, []).append(slot)

    ordered: List[Tuple[date, List[Dict[str, Any]]]] = []
    for slot_date, entries in grouped.items():
        entries.sort(
            key=lambda item: (
                _parse_time_value(item.get("start_time")) or time.min,
                item.get("id") or 0,
            )
        )
        ordered.append((slot_date, entries))
    ordered.sort(key=lambda item: item[0])
    return ordered

def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _score_bike_height(bike: Dict[str, Any], client_height: Optional[float]) -> float:
    if client_height is None:
        return 120.0

    min_h = _to_float(bike.get("height_min_cm"))
    max_h = _to_float(bike.get("height_max_cm"))

    if min_h is not None and max_h is not None:
        if min_h <= client_height <= max_h:
            midpoint = (min_h + max_h) / 2
            return abs(client_height - midpoint)
        if client_height < min_h:
            return 200.0 + (min_h - client_height)
        return 200.0 + (client_height - max_h)

    if min_h is not None:
        if client_height >= min_h:
            return client_height - min_h
        return 200.0 + (min_h - client_height)

    if max_h is not None:
        if client_height <= max_h:
            return max_h - client_height
        return 200.0 + (client_height - max_h)

    return 150.0

def _match_favorite_bike_id(favorite_raw: Optional[str], bikes_map: Dict[int, Dict[str, Any]]) -> Optional[int]:
    if not favorite_raw:
        return None
    needle = favorite_raw.strip().lower()
    if not needle:
        return None

    exact_matches: List[int] = []
    partial_matches: List[int] = []

    for bike_id, bike in bikes_map.items():
        title = (bike.get("title") or "").strip().lower()
        owner = (bike.get("owner") or "").strip().lower()
        if title == needle or owner == needle:
            exact_matches.append(bike_id)
        elif needle in title or (owner and needle in owner):
            partial_matches.append(bike_id)

    if exact_matches:
        return exact_matches[0]
    if partial_matches:
        return partial_matches[0]
    return None

def _format_stand_label_for_booking(
    stand: Optional[Dict[str, Any]],
    reservation: Optional[Dict[str, Any]] = None,
) -> str:
    if stand:
        code = _format_trainer_code(stand.get("code"))
        title = (stand.get("title") or "").strip()
        display = (stand.get("display_name") or "").strip()
        if code and title and title.lower() != code.lower():
            return f"{code} · {title}"
        if title:
            return title
        if code:
            return code
        if display:
            return display
        stand_id = stand.get("id")
        if stand_id is not None:
            return f"Станок {stand_id}"
    stand_code = _format_trainer_code((reservation or {}).get("stand_code"))
    if stand_code:
        return stand_code
    if reservation and reservation.get("stand_id"):
        return f"Станок {reservation.get('stand_id')}"
    return "Станок"

def _choose_best_reservation_for_client(
    client: Dict[str, Any],
    reservations: List[Dict[str, Any]],
    *,
    stands_map: Dict[int, Dict[str, Any]],
    bikes_map: Dict[int, Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    ranked = _rank_available_reservations(client, reservations, stands_map, bikes_map)
    if not ranked:
        return None, None, None

    best_choice = ranked[0]
    return best_choice[3], best_choice[4], best_choice[5]

def _rank_available_reservations(
    client: Dict[str, Any],
    reservations: List[Dict[str, Any]],
    stands_map: Dict[int, Dict[str, Any]],
    bikes_map: Dict[int, Dict[str, Any]],
) -> List[
    Tuple[float, float, str, Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]
]:
    client_height = _to_float(client.get("height"))
    favorite_bike_id = _match_favorite_bike_id(client.get("favorite_bike"), bikes_map)
    ranked: List[
        Tuple[float, float, str, Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]
    ] = []
    for reservation in reservations:
        stand_id = reservation.get("stand_id")
        stand = stands_map.get(stand_id) if isinstance(stand_id, int) else None
        bike = None
        bike_id = None
        if stand:
            bike_id = stand.get("bike_id")
            if isinstance(bike_id, int):
                bike = bikes_map.get(bike_id)

        if bike_id is not None and favorite_bike_id is not None and bike_id == favorite_bike_id:
            score = 0.0
        elif bike is not None:
            score = 100.0 + _score_bike_height(bike, client_height)
        elif stand is not None:
            score = 600.0
        else:
            score = 900.0

        position = stand.get("position") if isinstance(stand, dict) else None
        position_score = float(position) if isinstance(position, (int, float)) else 999.0
        stand_label = _format_stand_label_for_booking(stand, reservation)
        ranked.append((score, position_score, stand_label.lower(), reservation, stand, bike, stand_label))
    ranked.sort(key=lambda item: item[:3])
    return ranked

def _format_reassign_option_label(
    base_label: str,
    stand: Optional[Dict[str, Any]],
    bike: Optional[Dict[str, Any]],
) -> str:
    bike_title = None
    if isinstance(bike, dict):
        bike_title = bike.get("title")
    if not bike_title and isinstance(stand, dict):
        bike_title = stand.get("bike_title")
    if bike_title:
        label = f"{base_label} · {bike_title}"
    else:
        label = base_label
    return _trim_label(label)

def _format_reservation_stand_label(reservation: Dict[str, Any]) -> str:
    code = _format_trainer_code(reservation.get("stand_code"))
    display = (reservation.get("stand_display_name") or "").strip()
    title = (reservation.get("stand_title") or "").strip()
    if code:
        return code
    if display:
        return display
    if title:
        return title
    stand_id = reservation.get("stand_id")
    if stand_id:
        return f"Станок {stand_id}"
    return "Станок"

def _format_reservation_button_label(reservation: Dict[str, Any]) -> str:
    slot_date = _parse_date_value(reservation.get("slot_date"))
    start_label = _format_time_label(reservation.get("start_time")) or "—"
    end_label = _format_time_label(reservation.get("end_time")) or ""
    if end_label:
        time_part = f"{start_label}–{end_label}"
    else:
        time_part = start_label
    date_label = _format_weekday_label(slot_date) if slot_date else str(reservation.get("slot_date"))
    stand_label = _format_reservation_stand_label(reservation)
    return _trim_label(f"{date_label} · {time_part} · {stand_label}")

def _format_reservation_detail(reservation: Dict[str, Any]) -> str:
    slot_summary = _format_slot_summary(reservation)
    stand_label = _format_reservation_stand_label(reservation)
    bike_title = reservation.get("bike_title")
    lines = [
        f"{html.escape(slot_summary)}",
        f"🛠 Станок: {html.escape(stand_label)}",
    ]
    if bike_title:
        lines.append(f"🚲 Велосипед: {html.escape(str(bike_title))}")
    source = reservation.get("source")
    if source:
        lines.append(f"📝 Источник: {html.escape(str(source))}")
    return "\n".join(lines)

def build_booking_success_markup(client_id: int, reservation_id: Optional[int]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if isinstance(reservation_id, int):
        rows.append(
            [
                InlineKeyboardButton(
                    text="🛠 Посадить на другой станок",
                    callback_data=f"client_schedule_reassign|{client_id}|{reservation_id}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="➕ Записать ещё",
                callback_data=f"client_schedule_refresh|{client_id}",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="↩️ К клиенту", callback_data=f"client_info|{client_id}")])
    return InlineKeyboardMarkup(rows)

def build_client_booking_detail_markup(client_id: int, reservation_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="🛠 Пересадить",
                    callback_data=f"client_schedule_reassign|{client_id}|{reservation_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Удалить бронь",
                    callback_data=f"client_booking_cancel|{client_id}|{reservation_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="↩️ Ко всем бронированиям",
                    callback_data=f"client_bookings|{client_id}",
                )
            ],
            [InlineKeyboardButton(text="↩️ К клиенту", callback_data=f"client_info|{client_id}")],
        ]
    )

async def _load_inventory_maps() -> Tuple[Dict[int, Dict[str, Any]], Dict[int, Dict[str, Any]]]:
    try:
        trainers = await asyncio.to_thread(list_trainers)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load trainers for booking selection")
        trainers = []
    stands_map = {
        row.get("id"): row
        for row in trainers
        if isinstance(row.get("id"), int)
    }

    try:
        bikes = await asyncio.to_thread(list_bikes)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load bikes for booking selection")
        bikes = []
    bikes_map = {row.get("id"): row for row in bikes if isinstance(row.get("id"), int)}
    return stands_map, bikes_map

async def show_client_bookings(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    *,
    notice: Optional[str] = None,
) -> None:
    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s bookings", client_id)
        await query.edit_message_text(f"❌ Не удалось получить данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    since = _to_local_naive(_local_now())
    try:
        bookings = await asyncio.to_thread(
            list_future_reservations_for_client,
            client_id,
            since,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load future reservations for client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось загрузить бронирования: {exc}")
        return

    rows: List[List[InlineKeyboardButton]] = []
    limited = bookings[:CLIENT_BOOKINGS_LIMIT]
    for reservation in limited:
        res_id = reservation.get("id")
        if not isinstance(res_id, int):
            continue
        label = _format_reservation_button_label(reservation)
        rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"client_booking|{client_id}|{res_id}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="🔄 Обновить",
                callback_data=f"client_bookings|{client_id}",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="↩️ К клиенту", callback_data=f"client_info|{client_id}")])

    client_label = html.escape(client_display_name(client))
    header = f"📅 Бронирования клиента <b>{client_label}</b>."

    body_lines: List[str] = []
    if bookings:
        body_lines.append(f"Найдено {len(bookings)} активных бронирований.")
        if len(bookings) > CLIENT_BOOKINGS_LIMIT:
            body_lines.append(f"Показаны первые {CLIENT_BOOKINGS_LIMIT}.")
    else:
        body_lines.append("Нет активных записей.")

    message_parts: List[str] = []
    if notice:
        message_parts.append(f"⚠️ {html.escape(notice)}")
    message_parts.append(header)
    message_parts.extend(body_lines)

    await query.edit_message_text(
        "\n".join(message_parts),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )

async def show_client_booking_detail(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    reservation_id: int,
) -> None:
    try:
        reservation = await asyncio.to_thread(get_reservation, reservation_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load reservation %s", reservation_id)
        await query.edit_message_text(f"❌ Не удалось прочитать бронь: {exc}")
        return

    if not reservation or reservation.get("client_id") != client_id:
        await query.answer("Бронь не найдена.", show_alert=True)
        await show_client_bookings(query, context, client_id, notice="Эта бронь больше недоступна.")
        return

    slot_id = reservation.get("slot_id")
    slot_details = None
    if isinstance(slot_id, int):
        try:
            slot_details = await asyncio.to_thread(get_slot_with_reservations, slot_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to load slot %s for reservation %s: %s", slot_id, reservation_id, exc)
            slot_details = None

    reservation_info = dict(reservation)
    if slot_details:
        reservation_info.setdefault("slot_date", slot_details.get("slot_date"))
        reservation_info.setdefault("start_time", slot_details.get("start_time"))
        reservation_info.setdefault("end_time", slot_details.get("end_time"))
        reservation_info.setdefault("label", slot_details.get("label"))
        reservation_info.setdefault("session_kind", slot_details.get("session_kind"))
        reservation_info.setdefault("instructor_name", slot_details.get("instructor_name"))

    detail_text = _format_reservation_detail(reservation_info)
    client = await asyncio.to_thread(get_client, client_id)
    client_label = html.escape(client_display_name(client) if client else f"id={client_id}")
    text = f"📘 Бронь #{reservation_id} для <b>{client_label}</b>.\n{detail_text}"

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_client_booking_detail_markup(client_id, reservation_id),
    )

async def cancel_client_reservation(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    reservation_id: int,
) -> None:
    try:
        reservation = await asyncio.to_thread(get_reservation, reservation_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load reservation %s for cancel", reservation_id)
        await query.edit_message_text(f"❌ Не удалось прочитать бронь: {exc}")
        return

    if not reservation or reservation.get("client_id") != client_id:
        await query.answer("Бронь не найдена.", show_alert=True)
        await show_client_bookings(query, context, client_id, notice="Эта бронь уже изменена.")
        return

    slot_id = reservation.get("slot_id")
    slot_details = None
    if isinstance(slot_id, int):
        try:
            slot_details = await asyncio.to_thread(get_slot_with_reservations, slot_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to load slot %s for cancel: %s", slot_id, exc)
            slot_details = None

    reservation_info = dict(reservation)
    if slot_details:
        reservation_info.setdefault("slot_date", slot_details.get("slot_date"))
        reservation_info.setdefault("start_time", slot_details.get("start_time"))
        reservation_info.setdefault("end_time", slot_details.get("end_time"))
        reservation_info.setdefault("label", slot_details.get("label"))
        reservation_info.setdefault("session_kind", slot_details.get("session_kind"))
        reservation_info.setdefault("instructor_name", slot_details.get("instructor_name"))

    summary = _format_slot_summary(reservation_info)

    try:
        await asyncio.to_thread(
            update_reservation,
            reservation_id,
            client_id=None,
            client_name=None,
            status="available",
            source="adminbot",
            notes="cancelled via adminbot",
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to cancel reservation %s", reservation_id)
        await query.edit_message_text(f"❌ Не удалось отменить бронь: {exc}")
        return

    notice = f"Бронь на {summary} отменена."
    await show_client_bookings(query, context, client_id, notice=notice)

PENDING_TRAINER_EDIT_KEY = "pending_trainer_edit"
PENDING_BIKE_EDIT_KEY = "pending_bike_edit"
LAST_BIKES_SEARCH_KEY = "adminbot:last_bikes_search"

ACCOUNT_REGISTRY: Dict[str, AccountConfig] = {}

def normalize_account_id(value: str) -> str:
    return normalize_account_id_value(value)

def resolve_account_identifier(raw_id: str) -> Optional[str]:
    return resolve_account_identifier_value(ACCOUNT_REGISTRY, raw_id)

def format_account_list() -> str:
    return format_account_list_from_registry(ACCOUNT_REGISTRY)

def resolve_account_tokens(tokens: Iterable[str]) -> Tuple[List[str], List[str]]:
    return resolve_account_tokens_value(ACCOUNT_REGISTRY, tokens)

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

def build_bike_info_markup(bike_id: int, has_assignment: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="🛠 Поставить на станок",
                callback_data=f"bike_assign_prepare|{bike_id}",
            )
        ]
    ]

    if has_assignment:
        rows[0].append(
            InlineKeyboardButton(
                text="🧹 Снять",
                callback_data=f"bike_assign_clear|{bike_id}",
            )
        )

    rows.append(
        [
            InlineKeyboardButton(
                text="📏 Рост от",
                callback_data=f"bike_edit|height_min_cm|{bike_id}",
            ),
            InlineKeyboardButton(
                text="📏 Рост до",
                callback_data=f"bike_edit|height_max_cm|{bike_id}",
            ),
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(text="← Список велосипедов", callback_data="bikes_list"),
            InlineKeyboardButton(text="❌ Закрыть", callback_data="bikes_close"),
        ]
    )

    return InlineKeyboardMarkup(rows)

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

def _make_reply_func(
    bot,
    chat_id: int,
    reply_to_message_id: Optional[int] = None,
) -> Callable[[str], Awaitable[Any]]:
    async def _reply(text: str) -> Any:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_to_message_id=reply_to_message_id,
        )

    return _reply

async def process_workout_bytes(
    raw_bytes: bytes,
    file_name: str,
    account_ids: List[str],
    reply_func: Callable[[str], Awaitable[Any]],
) -> None:
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
        await reply_func(f"❌ Ошибка обработки ZWO: {exc}")
        return

    try:
        chart_data = await asyncio.to_thread(zwo_to_chart_data, workout)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to build chart data")
        await reply_func(f"❌ Ошибка генерации графика тренировки: {exc}")
        return

    if not account_ids:
        await reply_func("ℹ️ Не указан ни один аккаунт для загрузки.")
        return

    results: List[Tuple[str, bool, str]] = []
    for account_id in account_ids:
        if account_id not in ACCOUNT_REGISTRY:
            results.append((account_id, False, "Аккаунт не найден"))
            continue
        success, info = await upload_workout_to_account(account_id, workout, chart_data)
        results.append((account_id, success, info))

    workout_name = workout.get("name") or (file_name or "тренировка")
    header = f"📤 Загрузка тренировки «{workout_name}»:"
    lines = [header]
    for account_id, success, info in results:
        account = ACCOUNT_REGISTRY.get(account_id)
        account_label = account.name if account else account_id
        prefix = "✅" if success else "❌"
        lines.append(f"{prefix} {account_label}: {info}")

    await reply_func("\n".join(lines))

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

    await process_workout_bytes(
        raw_bytes=bytes(data),
        file_name=document.file_name or "",
        account_ids=account_ids,
        reply_func=message.reply_text,
    )

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

def build_start_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🧙‍♂️ Волшебник", callback_data="menu|wizard"),
                InlineKeyboardButton("🆕 Создать клиента", callback_data="menu|newclient"),
            ],
            [
                InlineKeyboardButton("👤 Аккаунты", callback_data="menu|accounts"),
                InlineKeyboardButton("🚲 Велосипеды", callback_data="menu|bikes"),
            ],
            [
                InlineKeyboardButton("🛠 Станки", callback_data="menu|stands"),
                InlineKeyboardButton("📅 События", callback_data="menu|events"),
            ],
        ]
    )

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    await update.message.reply_text(
        START_MESSAGE,
        reply_markup=menu_admin.build_start_menu_keyboard(),
    )

async def events_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    if not ACCOUNT_REGISTRY:
        await update.message.reply_text("⚠️ Нет доступных WattAttack аккаунтов.")
        return
    await events_admin.start_events_flow(update, context, ACCOUNT_REGISTRY)

async def wizard_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    if not ACCOUNT_REGISTRY:
        await update.message.reply_text("⚠️ Нет доступных WattAttack аккаунтов.")
        return
    await wizard_admin.start(update, context, ACCOUNT_REGISTRY, LOCAL_TIMEZONE)

async def wizard_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return
    handled = await wizard_admin.handle_message(
        update, context, ACCOUNT_REGISTRY, LOCAL_TIMEZONE
    )
    if not handled:
        await text_search_handler(update, context)

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
    query = update.callback_query
    message = update.effective_message
    if query:
        await query.answer()
        if not await ensure_admin_callback(query):
            return ConversationHandler.END
    else:
        if not update.message:
            return ConversationHandler.END
        if not ensure_admin_message(update):
            return ConversationHandler.END

    if not message:
        return ConversationHandler.END

    _newclient_reset_form(context)
    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP)

    await message.reply_text(
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
    return

async def bikes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    search_term = " ".join(context.args).strip() if context.args else ""
    context.user_data[LAST_BIKES_SEARCH_KEY] = search_term

    try:
        text, markup = await build_bikes_view(search_term)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch bikes")
        await update.message.reply_text(
            f"❌ Ошибка получения списка велосипедов: {exc}"
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

async def layout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not ensure_admin_message(update):
        return

    try:
        text = await build_layout_overview()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch layout")
        await update.message.reply_text(f"❌ Ошибка получения расстановки: {exc}")
        return

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_return_markup(),
    )

async def build_layout_overview() -> str:
    try:
        await asyncio.to_thread(ensure_layout_table)
        await asyncio.to_thread(ensure_trainers_table)
        await asyncio.to_thread(ensure_bikes_table)
        assignments = await asyncio.to_thread(list_layout_details)
        trainers = await asyncio.to_thread(list_trainers, 100)
        bikes = await asyncio.to_thread(list_bikes, 200)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch layout")
        raise

    assignment_by_stand = {
        row.get("stand_id"): row for row in assignments if row.get("stand_id") is not None
    }
    assigned_bike_ids = {
        row.get("bike_id") for row in assignments if row.get("bike_id") is not None
    }

    lines: List[str] = ["<b>🚲 Текущая расстановка</b>"]
    if assignments:
        for row in assignments:
            stand_stub = {
                "code": row.get("stand_code"),
                "title": row.get("stand_title"),
                "display_name": row.get("stand_display"),
                "id": row.get("stand_id"),
            }
            stand_label = format_trainer_button_label(stand_stub)
            bike_title = row.get("bike_title") or f"id={row.get('bike_id')}"
            lines.append(
                f"• {html.escape(stand_label)} → {html.escape(str(bike_title))}"
            )
    else:
        lines.append("• Нет назначенных велосипедов.")

    free_stands = [
        trainer
        for trainer in trainers
        if isinstance(trainer.get("id"), int)
        and trainer.get("id") not in assignment_by_stand
    ]
    if free_stands:
        stand_labels = [format_trainer_button_label(item) for item in free_stands[:10]]
        extra = "…" if len(free_stands) > len(stand_labels) else ""
        lines.append("")
        lines.append(
            "🛠 Свободные станки: "
            + ", ".join(html.escape(label) for label in stand_labels)
            + extra
        )

    free_bikes = [
        bike for bike in bikes if isinstance(bike.get("id"), int) and bike.get("id") not in assigned_bike_ids
    ]
    if free_bikes and (free_stands or len(assignments) < len(trainers)):
        bike_labels = [_format_bike_choice_label(bike) for bike in free_bikes[:10]]
        extra = "…" if len(free_bikes) > len(bike_labels) else ""
        lines.append("")
        lines.append(
            "🚲 Свободные велосипеды: "
            + ", ".join(html.escape(label) for label in bike_labels)
            + extra
        )

    return "\n".join(lines)

async def build_pedals_overview() -> str:
    await asyncio.to_thread(ensure_pedals_table)
    pedals = await asyncio.to_thread(list_pedals)

    lines = ["🚴 Доступные педали:"]
    if pedals:
        for pedal in pedals:
            pedal_type = pedal.get("pedal_type") or ""
            type_label = PEDAL_OPTION_LABEL_BY_CODE.get(pedal_type, pedal_type)
            lines.append(f"• {pedal.get('name')} — {type_label}")
    else:
        lines.append("• Список пуст.")

    lines.append("")
    lines.append("Используйте карточку клиента, чтобы задать/очистить педали.")
    return "\n".join(lines)

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

    upload_handled = await uploads_admin.handle_upload_callback(action, parts, query, context)
    if upload_handled:
        return

    admin_handled = await admins_view.handle_admin_callback(action, parts, query, context)
    if admin_handled:
        return

    account_handled = await accounts_view.handle_account_callback(action, parts, query, context)
    if account_handled:
        return

    if action == "menu" and len(parts) >= 2:
        menu_action = parts[1]
        handled = await menu_admin.handle_menu_action(
            menu_action,
            update,
            context,
            ACCOUNT_REGISTRY,
            LOCAL_TIMEZONE,
            show_account_selection=accounts_view.show_account_selection,
            build_bikes_view=build_bikes_view,
            build_stands_view=build_stands_view,
            build_layout_overview=build_layout_overview,
            build_pedals_overview=build_pedals_overview,
            show_admin_menu=admins_view.show_admin_menu,
            start_text=START_MESSAGE,
        )
        if handled:
            return

    if action == "events":
        handled = await events_admin.handle_events_callback(
            update,
            context,
            ACCOUNT_REGISTRY,
            LOCAL_TIMEZONE,
            DEFAULT_TIMEOUT,
            parts[1:],
        )
        if handled:
            return
    elif action == "wizard":
        handled = await wizard_admin.handle_callback(
            update,
            context,
            ACCOUNT_REGISTRY,
            LOCAL_TIMEZONE,
            DEFAULT_TIMEOUT,
            DEFAULT_CLIENT_FTP,
        )
        if handled:
            return
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
    elif action == "client_edit_menu" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await show_client_edit_menu(query, context, client_id)
    elif action == "client_info" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await show_client_info(query, context, client_id)
    elif action == "client_schedule_book" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.answer("Некорректный идентификатор.", show_alert=True)
            return
        await show_client_booking_days(query, context, client_id)
    elif action == "client_schedule_refresh" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.answer("Некорректный идентификатор.", show_alert=True)
            return
        await show_client_booking_days(query, context, client_id)
    elif action == "intervals_start" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await intervals_admin.start_intervals_edit(update, context, client_id)
    elif action == "intervals_cancel" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await intervals_admin.cancel_intervals(update, context, client_id)
    elif action == "client_schedule_back" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.answer("Некорректный идентификатор.", show_alert=True)
            return
        await show_client_booking_days(query, context, client_id)
    elif action == "client_schedule_day" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.answer("Некорректный идентификатор.", show_alert=True)
            return
        date_token = parts[2]
        await show_client_booking_slots(query, context, client_id, date_token)
    elif action == "client_schedule_slot" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            slot_id = int(parts[2])
        except ValueError:
            await query.answer("Некорректные параметры.", show_alert=True)
            return
        await book_client_on_slot(query, context, client_id, slot_id)
    elif action == "client_bookings" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.answer("Некорректный идентификатор.", show_alert=True)
            return
        await show_client_bookings(query, context, client_id)
    elif action == "client_booking" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            reservation_id = int(parts[2])
        except ValueError:
            await query.answer("Некорректные параметры.", show_alert=True)
            return
        await show_client_booking_detail(query, context, client_id, reservation_id)
    elif action == "client_booking_cancel" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            reservation_id = int(parts[2])
        except ValueError:
            await query.answer("Некорректные параметры.", show_alert=True)
            return
        await cancel_client_reservation(query, context, client_id, reservation_id)
    elif action == "client_schedule_reassign" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            reservation_id = int(parts[2])
        except ValueError:
            await query.answer("Некорректные параметры.", show_alert=True)
            return
        await show_client_reassign_menu(query, context, client_id, reservation_id)
    elif action == "client_schedule_reassign_auto" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
            reservation_id = int(parts[2])
        except ValueError:
            await query.answer("Некорректные параметры.", show_alert=True)
            return
        await auto_reassign_client(query, context, client_id, reservation_id)
    elif action == "client_schedule_reassign_set" and len(parts) >= 4:
        try:
            client_id = int(parts[1])
            reservation_id = int(parts[2])
            target_reservation_id = int(parts[3])
        except ValueError:
            await query.answer("Некорректные параметры.", show_alert=True)
            return
        await set_client_reservation_stand(
            query,
            context,
            client_id,
            reservation_id,
            target_reservation_id,
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
    elif action == "uploadclients_mode" and len(parts) >= 2:
        await handle_uploadclients_mode(query, context, parts[1])
        return
    elif action == "uploadschedule_mode" and len(parts) >= 2:
        await handle_uploadschedule_mode(query, context, parts[1])
        return
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
    elif action == "client_gender_set" and len(parts) >= 3:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        gender_code = parts[2]
        await set_client_gender(query, context, client_id, gender_code)
    elif action == "client_gender_clear" and len(parts) >= 2:
        try:
            client_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор клиента.")
            return
        await clear_client_gender(query, context, client_id)
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
    elif action == "bike_info" and len(parts) >= 2:
        try:
            bike_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор велосипеда.")
            return
        await show_bike_info(query, context, bike_id)
    elif action == "bike_assign_prepare" and len(parts) >= 2:
        try:
            bike_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор велосипеда.")
            return
        await render_bike_assignment_selector(
            context,
            query.message.chat_id,
            query.message.message_id,
            bike_id,
        )
    elif action == "bike_assign_set" and len(parts) >= 3:
        try:
            bike_id = int(parts[1])
            stand_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректные параметры назначения.")
            return
        try:
            await asyncio.to_thread(
                set_bike_assignment,
                stand_id,
                bike_id,
                getattr(query.from_user, "id", None),
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to assign bike %s to stand %s", bike_id, stand_id)
            await query.edit_message_text(f"❌ Не удалось назначить велосипед: {exc}")
            return
        await query.answer("Велосипед назначен.")
        await render_bike_info_message(
            context,
            query.message.chat_id,
            query.message.message_id,
            bike_id,
        )
    elif action == "bike_assign_clear" and len(parts) >= 2:
        try:
            bike_id = int(parts[1])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный идентификатор велосипеда.")
            return
        try:
            await asyncio.to_thread(clear_bike_assignment_for_bike, bike_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to clear bike %s assignment", bike_id)
            await query.edit_message_text(f"❌ Не удалось снять велосипед со станка: {exc}")
            return
        await query.answer("Велосипед снят со станка.")
        await render_bike_info_message(
            context,
            query.message.chat_id,
            query.message.message_id,
            bike_id,
        )
    elif action == "bikes_close":
        try:
            await query.message.delete()
        except Exception:  # noqa: BLE001
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:  # noqa: BLE001
                LOGGER.debug("bikes_close action failed for message %s", query.message)
        return
    elif action == "bikes_list":
        search_term = context.user_data.get(LAST_BIKES_SEARCH_KEY, "")
        try:
            text, markup = await build_bikes_view(search_term)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to rebuild bikes view")
            await query.edit_message_text(f"❌ Ошибка получения списка велосипедов: {exc}")
            return
        if markup is None:
            await query.edit_message_text(text)
        else:
            await query.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
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

def apply_client_profile(account_id: str, client_record: Dict[str, Any]) -> None:
    account = ACCOUNT_REGISTRY[account_id]
    apply_wattattack_profile(
        account_id=account_id,
        account_label=account.name,
        email=account.email,
        password=account.password,
        base_url=account.base_url,
        client_record=client_record,
        timeout=DEFAULT_TIMEOUT,
        default_ftp=DEFAULT_CLIENT_FTP,
    )

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
    telegram_link: Optional[Dict[str, Any]] = None,
    vk_link: Optional[Dict[str, Any]] = None,
) -> str:
    summary = format_client_summary(client_record)
    lines = [summary]
    has_goal_in_summary = any("Цель:" in line for line in summary.splitlines())

    tg_username = (telegram_link or {}).get("tg_username") or (client_record.get("tg_username") or "")
    tg_username = tg_username.strip() if isinstance(tg_username, str) else ""
    tg_id = (telegram_link or {}).get("tg_user_id") or client_record.get("tg_user_id")
    if tg_username and tg_id:
        lines.append(f"💬 Telegram: @{tg_username} (id {tg_id})")
    elif tg_username:
        lines.append(f"💬 Telegram: @{tg_username}")
    elif tg_id:
        lines.append(f"💬 Telegram id {tg_id}")

    vk_username = (vk_link or {}).get("vk_username")
    vk_user_id = (vk_link or {}).get("vk_user_id")
    vk_profile_url = None
    if vk_username:
        vk_profile_url = f"https://vk.com/{vk_username}"
    elif vk_user_id:
        vk_profile_url = f"https://vk.com/id{vk_user_id}"
    if vk_profile_url:
        vk_link_safe = html.escape(vk_profile_url)
        lines.append(f"🌐 VK: <a href=\"{vk_link_safe}\">профиль</a>")

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
    "height": {
        "label": "📏 Рост",
        "prompt": "Введите рост в сантиметрах (например, 178).",
    },
    "gender": {
        "label": "🚻 Пол",
        "prompt": "Выберите пол клиента или очистите значение.",
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
                    text="📅 Записать на тренировку",
                    callback_data=f"client_schedule_book|{client_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚴 Подбор велосипедов",
                    callback_data=f"client_bikes|{client_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📅 Бронирования клиента",
                    callback_data=f"client_bookings|{client_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить",
                    callback_data=f"client_edit_menu|{client_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🏠 Главное меню",
                    callback_data="menu|start",
                )
            ],
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="client_close")],
        ]
    )

def build_client_edit_menu_markup(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="⚡ FTP",
                    callback_data=f"client_edit|ftp|{client_id}",
                ),
                InlineKeyboardButton(
                    text="⚖️ Вес",
                    callback_data=f"client_edit|weight|{client_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📏 Рост",
                    callback_data=f"client_edit|height|{client_id}",
                ),
                InlineKeyboardButton(
                    text="🚻 Пол",
                    callback_data=f"client_edit|gender|{client_id}",
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
                    text="🔑 Intervals.icu ключ",
                    callback_data=f"intervals_start|{client_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="↩️ К клиенту",
                    callback_data=f"client_info|{client_id}",
                ),
                InlineKeyboardButton(
                    text="🏠 Главное меню",
                    callback_data="menu|start",
                ),
                InlineKeyboardButton(text="❌ Закрыть", callback_data="client_close"),
            ],
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

def _format_bike_choice_label(record: Dict[str, Any]) -> str:
    title = str(record.get("title") or f"id={record.get('id')}")
    size_label = record.get("size_label") or record.get("frame_size_cm")

    if size_label:
        return f"{title} • {str(size_label).strip()}"
    return title

def _trim_label(value: str, max_len: int = 60) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "…"

def _format_bike_button_label(record: Dict[str, Any], assignment: Optional[Dict[str, Any]]) -> str:
    base = _format_bike_choice_label(record)
    if not assignment:
        return _trim_label(f"{base} • свободен")

    trainer_stub = {
        "code": assignment.get("stand_code"),
        "title": assignment.get("stand_title"),
        "display_name": assignment.get("stand_display"),
        "id": assignment.get("stand_id"),
    }
    stand_label = format_trainer_button_label(trainer_stub)
    return _trim_label(f"{base} • {stand_label}")

def _format_stand_selection_label(
    trainer_record: Dict[str, Any], assignment: Optional[Dict[str, Any]]
) -> str:
    base = format_trainer_button_label(trainer_record)
    if not assignment:
        return _trim_label(f"{base} • свободно")

    bike_title = assignment.get("bike_title") or f"id={assignment.get('bike_id')}"
    return _trim_label(f"{base} • {bike_title}")

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

async def build_bikes_view(
    search_term: str,
) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    await asyncio.to_thread(ensure_bikes_table)
    if search_term:
        bikes = await asyncio.to_thread(search_bikes, search_term, 30)
        total_count = len(bikes)
    else:
        bikes = await asyncio.to_thread(list_bikes, 50)
        total_count = await asyncio.to_thread(bikes_count)

    if not bikes:
        if search_term:
            return (
                f"🚫 Велосипеды по запросу «{html.escape(search_term)}» не найдены.",
                None,
            )
        return ("🚫 В базе нет доступных велосипедов.", None)

    await asyncio.to_thread(ensure_layout_table)
    try:
        assignments = await asyncio.to_thread(list_layout_details)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to load bike layout: %s", exc)
        assignments = []

    assignment_map = {
        row.get("bike_id"): row
        for row in assignments
        if isinstance(row.get("bike_id"), int)
    }

    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for record in bikes:
        bike_id = record.get("id")
        if not isinstance(bike_id, int):
            continue
        label = _format_bike_button_label(record, assignment_map.get(bike_id))
        keyboard_rows.append(
            [InlineKeyboardButton(text=label, callback_data=f"bike_info|{bike_id}")]
        )

    keyboard_rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="bikes_close")])

    header_lines: List[str] = []
    if search_term:
        header_lines.append(
            f"🔍 Найдено {total_count} велосипедов по запросу «{html.escape(search_term)}»."
        )
    else:
        header_lines.append(f"🚲 Всего велосипедов: {total_count}.")
        if total_count > len(bikes):
            header_lines.append(f"Показаны первые {len(bikes)} записей.")
        header_lines.append("Нажмите на велосипед, чтобы открыть карточку.")

    text = "\n".join(header_lines)
    return text, InlineKeyboardMarkup(keyboard_rows)

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

def build_client_gender_picker_markup(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="🚹 Мужской",
                    callback_data=f"client_gender_set|{client_id}|male",
                ),
                InlineKeyboardButton(
                    text="🚺 Женский",
                    callback_data=f"client_gender_set|{client_id}|female",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Очистить значение",
                    callback_data=f"client_gender_clear|{client_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="↩️ Назад",
                    callback_data=f"client_info|{client_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data=f"client_edit_cancel|{client_id}",
                ),
            ],
        ]
    )

async def render_client_gender_picker(
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
    prompt = CLIENT_EDIT_FIELDS["gender"]["prompt"]

    pending = context.user_data.get("pending_client_edit")
    if isinstance(pending, dict) and pending.get("client_id") == client_id:
        pending.setdefault("client_name", display_name)
        pending["label"] = CLIENT_EDIT_FIELDS["gender"]["label"]
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
        reply_markup=build_client_gender_picker_markup(client_id),
    )

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

async def set_client_gender(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    gender_code: str,
) -> None:
    normalized = gender_code.strip().lower()
    if normalized not in {"male", "female"}:
        await query.answer("Неизвестный вариант пола.", show_alert=True)
        return

    try:
        await asyncio.to_thread(update_client_fields, client_id, gender=normalized)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to update client %s gender to %s", client_id, gender_code)
        await query.edit_message_text(f"❌ Не удалось обновить пол клиента: {exc}")
        return

    pending = context.user_data.get("pending_client_edit") or {}
    client_name = pending.get("client_name")
    field_label = CLIENT_EDIT_FIELDS["gender"]["label"]
    gender_label = "М" if normalized == "male" else "Ж"
    if client_name:
        success_text = f"✅ {field_label} для {client_name} обновлён: {gender_label}."
    else:
        success_text = f"✅ {field_label} обновлён: {gender_label}."
    await context.bot.send_message(chat_id=query.message.chat_id, text=success_text)

    context.user_data.pop("pending_client_edit", None)
    await query.answer("Пол обновлён.")
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
    )

async def clear_client_gender(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
) -> None:
    try:
        await asyncio.to_thread(update_client_fields, client_id, gender=None)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to clear gender for client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось очистить пол клиента: {exc}")
        return

    pending = context.user_data.get("pending_client_edit") or {}
    client_name = pending.get("client_name")
    field_label = CLIENT_EDIT_FIELDS["gender"]["label"]
    if client_name:
        success_text = f"✅ {field_label} для {client_name} очищен."
    else:
        success_text = f"✅ {field_label} очищен."
    await context.bot.send_message(chat_id=query.message.chat_id, text=success_text)

    context.user_data.pop("pending_client_edit", None)
    await query.answer("Пол очищен.")
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
    if field == "height":
        normalized = value.replace(",", ".")
        height_value = float(normalized)
        if height_value <= 0:
            raise ValueError("Введите положительное число (см).")
        return height_value
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
    reply_markup: Optional[InlineKeyboardMarkup] = None,
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

    telegram_link = await asyncio.to_thread(get_link_by_client, client_id)
    vk_link = await asyncio.to_thread(get_vk_link_by_client, client_id)

    text = format_client_details(
        record,
        telegram_link=telegram_link,
        vk_link=vk_link,
    )
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup or build_client_info_markup(client_id),
    )

async def show_client_edit_menu(query, context: ContextTypes.DEFAULT_TYPE, client_id: int) -> None:
    context.user_data.pop("pending_client_edit", None)
    await render_client_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        client_id,
        reply_markup=build_client_edit_menu_markup(client_id),
    )

async def _resolve_stand_label(reservation: Dict[str, Any]) -> str:
    stand_id = reservation.get("stand_id")
    if isinstance(stand_id, int):
        try:
            trainer = await asyncio.to_thread(get_trainer, stand_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Failed to load trainer %s for booking: %s", stand_id, exc)
        else:
            if trainer:
                return format_trainer_button_label(trainer)
    stand_code = _format_trainer_code(reservation.get("stand_code"))
    if stand_code:
        return stand_code
    if stand_id:
        return f"Станок {stand_id}"
    return "Станок"

async def show_client_booking_days(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    *,
    notice: Optional[str] = None,
) -> None:
    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s for booking", client_id)
        await query.edit_message_text(f"❌ Не удалось прочитать данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    client_label = client_display_name(client)
    headline = f"📅 Выберите день для записи <b>{html.escape(client_label)}</b>."

    search_start = _local_now() + BOOKING_CUTOFF
    search_end = search_start + BOOKING_LOOKAHEAD

    try:
        slots_raw = await asyncio.to_thread(
            list_available_slots,
            _to_local_naive(search_start),
            _to_local_naive(search_end),
            BOOKING_SLOTS_LIMIT,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load available slots for admin booking")
        await query.edit_message_text(f"❌ Не удалось получить расписание: {exc}")
        return

    slots: List[Dict[str, Any]] = []
    for slot in slots_raw:
        slot_start = _slot_start_datetime(slot)
        if slot_start is None or slot_start < search_start:
            continue
        slots.append(slot)

    grouped = _group_slots_by_day(slots)
    limited = grouped[:BOOKING_DAY_LIMIT]

    lines: List[str] = []
    if notice:
        lines.append(f"⚠️ {html.escape(notice)}")
    lines.append(headline)

    markup: InlineKeyboardMarkup
    if not limited:
        horizon_label = _format_weekday_label(search_end.date())
        lines.append(f"ℹ️ Свободных мест до {html.escape(horizon_label)} не найдено.")
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(text="↩️ Назад к клиенту", callback_data=f"client_info|{client_id}")]]
        )
    else:
        rows: List[List[InlineKeyboardButton]] = []
        for slot_date, day_slots in limited:
            total_free = sum(int(slot.get("free_count") or 0) for slot in day_slots)
            label = f"{_format_weekday_label(slot_date)} · мест {total_free}"
            rows.append(
                [
                    InlineKeyboardButton(
                        text=label,
                        callback_data=f"client_schedule_day|{client_id}|{slot_date.isoformat()}",
                    )
                ]
            )
        rows.append(
            [
                InlineKeyboardButton(text="↩️ К клиенту", callback_data=f"client_info|{client_id}"),
                InlineKeyboardButton(
                    text="🔄 Обновить", callback_data=f"client_schedule_refresh|{client_id}"
                ),
            ]
        )
        markup = InlineKeyboardMarkup(rows)

    await query.edit_message_text(
        "\n\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )

async def show_client_booking_slots(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    date_token: str,
    *,
    notice: Optional[str] = None,
) -> None:
    try:
        target_date = date.fromisoformat(date_token)
    except ValueError:
        await query.answer("Некорректная дата.", show_alert=True)
        return

    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s for booking", client_id)
        await query.edit_message_text(f"❌ Не удалось прочитать данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    client_label = client_display_name(client)
    day_label = _format_weekday_label(target_date)

    start_dt_local = datetime.combine(target_date, time.min, tzinfo=LOCAL_TIMEZONE)
    end_dt_local = datetime.combine(target_date, time.max, tzinfo=LOCAL_TIMEZONE)
    cutoff_threshold = _local_now() + BOOKING_CUTOFF

    try:
        slots_raw = await asyncio.to_thread(
            list_available_slots,
            _to_local_naive(start_dt_local),
            _to_local_naive(end_dt_local),
            BOOKING_SLOTS_LIMIT,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load available slots for admin booking day %s", date_token)
        await query.edit_message_text(f"❌ Не удалось получить слоты: {exc}")
        return

    slots: List[Dict[str, Any]] = []
    for slot in slots_raw:
        slot_start = _slot_start_datetime(slot)
        if slot_start is None or slot_start < cutoff_threshold:
            continue
        slots.append(slot)

    if not slots:
        await show_client_booking_days(
            query,
            context,
            client_id,
            notice="Свободных мест на выбранный день больше нет.",
        )
        return

    rows: List[List[InlineKeyboardButton]] = []
    for slot in slots:
        slot_id = slot.get("id")
        if not isinstance(slot_id, int):
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=_format_slot_button_label(slot),
                    callback_data=f"client_schedule_slot|{client_id}|{slot_id}",
                )
            ]
        )

    if not rows:
        await show_client_booking_days(
            query,
            context,
            client_id,
            notice="Свободных мест на выбранный день больше нет.",
        )
        return

    rows.append(
        [
            InlineKeyboardButton(text="← Дни", callback_data=f"client_schedule_back|{client_id}"),
            InlineKeyboardButton(text="↩️ К клиенту", callback_data=f"client_info|{client_id}"),
        ]
    )

    lines: List[str] = []
    if notice:
        lines.append(f"⚠️ {html.escape(notice)}")
    lines.append(
        f"🗓 <b>{html.escape(day_label)}</b>\n"
        f"Выберите слот для записи {html.escape(client_label)}."
    )

    await query.edit_message_text(
        "\n\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )

async def book_client_on_slot(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    slot_id: int,
) -> None:
    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s for booking", client_id)
        await query.edit_message_text(f"❌ Не удалось прочитать данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    client_label = client_display_name(client)

    try:
        slot_details = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for booking", slot_id)
        await query.edit_message_text(f"❌ Не удалось получить детали слота: {exc}")
        return

    if not slot_details:
        await show_client_booking_days(
            query,
            context,
            client_id,
            notice="Слот больше не существует.",
        )
        return

    slot_start = _slot_start_datetime(slot_details)
    if slot_start and slot_start < _local_now():
        await show_client_booking_days(
            query,
            context,
            client_id,
            notice="Этот слот уже начался.",
        )
        return

    reservations = slot_details.get("reservations") or []
    available_reservations = [
        res for res in reservations if (res.get("status") or "").lower() == "available"
    ]
    if not available_reservations:
        await show_client_booking_days(
            query,
            context,
            client_id,
            notice="Свободных мест на этом слоте больше нет.",
        )
        return

    stands_map, bikes_map = await _load_inventory_maps()

    reservation, stand, bike = _choose_best_reservation_for_client(
        client,
        available_reservations,
        stands_map=stands_map,
        bikes_map=bikes_map,
    )
    if reservation is None:
        await show_client_booking_days(
            query,
            context,
            client_id,
            notice="Не удалось подобрать подходящий станок. Попробуйте другой слот.",
        )
        return

    booked: Optional[Dict[str, Any]] = None
    try:
        booked = await asyncio.to_thread(
            book_available_reservation,
            reservation["id"],
            client_id=client_id,
            client_name=client_label,
            status="booked",
            source="adminbot",
            notes="manual booking via adminbot",
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Booking attempt failed for reservation %s", reservation.get("id"))
        booked = None

    slot_date = _parse_date_value(slot_details.get("slot_date"))
    date_token = slot_date.isoformat() if slot_date else None

    if not booked:
        notice = "Не удалось занять место — попробуйте другой слот."
        if date_token:
            await show_client_booking_slots(
                query,
                context,
                client_id,
                date_token,
                notice=notice,
            )
        else:
            await show_client_booking_days(
                query,
                context,
                client_id,
                notice=notice,
            )
        return

    if stand:
        stand_label = _format_stand_label_for_booking(stand, booked)
    else:
        stand_label = await _resolve_stand_label(booked)
    slot_summary = _format_slot_summary(slot_details)
    bike_line = ""
    bike_title = bike.get("title") if isinstance(bike, dict) else None
    if bike_title:
        bike_line = f"\n🚲 Велосипед: {html.escape(str(bike_title))}."
    text = (
        f"✅ <b>{html.escape(client_label)}</b> записан на {html.escape(slot_summary)}.\n"
        f"🛠 Место: {html.escape(stand_label)}.{bike_line}"
    )
    markup = build_booking_success_markup(client_id, booked.get("id"))
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def show_client_reassign_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    reservation_id: int,
    *,
    notice: Optional[str] = None,
) -> None:
    try:
        reservation = await asyncio.to_thread(get_reservation, reservation_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load reservation %s", reservation_id)
        await query.edit_message_text(f"❌ Не удалось прочитать бронь: {exc}")
        return

    if not reservation:
        await query.answer("Бронь не найдена.", show_alert=True)
        return
    if reservation.get("client_id") != client_id:
        await query.answer("Эта бронь уже изменилась.", show_alert=True)
        return

    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось получить данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    slot_id = reservation.get("slot_id")
    if not isinstance(slot_id, int):
        await query.edit_message_text("⚠️ Неверные данные слота для брони.")
        return

    try:
        slot_details = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for reassign", slot_id)
        await query.edit_message_text(f"❌ Не удалось получить слот: {exc}")
        return

    if not slot_details:
        await query.edit_message_text("⚠️ Слот больше не существует.")
        return

    available_reservations = [
        row
        for row in slot_details.get("reservations") or []
        if (row.get("status") or "").lower() == "available"
    ]

    if not available_reservations:
        await query.edit_message_text(
            "⚠️ Свободных станков для этого слота пока нет.",
            reply_markup=build_booking_success_markup(client_id, reservation_id),
        )
        return

    stands_map, bikes_map = await _load_inventory_maps()
    ranked = _rank_available_reservations(client, available_reservations, stands_map, bikes_map)
    if not ranked:
        await query.edit_message_text(
            "⚠️ Не удалось подобрать подходящие станки. Попробуйте позже.",
            reply_markup=build_booking_success_markup(client_id, reservation_id),
        )
        return

    slot_summary = _format_slot_summary(slot_details)
    client_label = client_display_name(client)
    current_stand = stands_map.get(reservation.get("stand_id"))
    current_label = _format_stand_label_for_booking(current_stand, reservation)
    current_bike = None
    if isinstance(current_stand, dict):
        current_bike = current_stand.get("bike_title")
    current_line = f"🛠 Текущий станок: {html.escape(current_label)}"
    if current_bike:
        current_line += f" · {html.escape(str(current_bike))}"

    lines: List[str] = [
        f"🔁 Пересадка клиента <b>{html.escape(client_label)}</b>.",
        f"Слот: {html.escape(slot_summary)}.",
        current_line,
        "Выберите свободный станок или воспользуйтесь автоматическим подбором.",
    ]
    if notice:
        lines.insert(0, f"⚠️ {html.escape(notice)}")

    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✨ Автовыбор",
                callback_data=f"client_schedule_reassign_auto|{client_id}|{reservation_id}",
            )
        ]
    ]
    for option in ranked[:BOOKING_REASSIGN_LIMIT]:
        target_res = option[3]
        target_id = target_res.get("id")
        if not isinstance(target_id, int):
            continue
        label = _format_reassign_option_label(option[6], option[4], option[5])
        rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"client_schedule_reassign_set|{client_id}|{reservation_id}|{target_id}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="↩️ Список дней",
                callback_data=f"client_schedule_refresh|{client_id}",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="↩️ К клиенту", callback_data=f"client_info|{client_id}")])

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )

async def auto_reassign_client(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    reservation_id: int,
) -> None:
    try:
        reservation = await asyncio.to_thread(get_reservation, reservation_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load reservation %s", reservation_id)
        await query.edit_message_text(f"❌ Не удалось прочитать бронь: {exc}")
        return

    if not reservation or reservation.get("client_id") != client_id:
        await query.answer("Бронь не найдена либо уже изменена.", show_alert=True)
        return

    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось получить данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    slot_id = reservation.get("slot_id")
    if not isinstance(slot_id, int):
        await query.edit_message_text("⚠️ Неверные данные слота для брони.")
        return

    try:
        slot_details = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for reassign", slot_id)
        await query.edit_message_text(f"❌ Не удалось получить слот: {exc}")
        return

    if not slot_details:
        await query.edit_message_text("⚠️ Слот больше не существует.")
        return

    available_reservations = [
        row
        for row in slot_details.get("reservations") or []
        if (row.get("status") or "").lower() == "available"
    ]

    if not available_reservations:
        await show_client_reassign_menu(
            query,
            context,
            client_id,
            reservation_id,
            notice="Свободных станков пока нет.",
        )
        return

    stands_map, bikes_map = await _load_inventory_maps()
    ranked = _rank_available_reservations(client, available_reservations, stands_map, bikes_map)
    if not ranked:
        await show_client_reassign_menu(
            query,
            context,
            client_id,
            reservation_id,
            notice="Не удалось подобрать станки автоматически.",
        )
        return

    best_option = ranked[0]
    target_reservation = best_option[3]
    target_id = target_reservation.get("id")
    if not isinstance(target_id, int):
        await show_client_reassign_menu(
            query,
            context,
            client_id,
            reservation_id,
            notice="Некорректный станок для пересадки.",
        )
        return

    await set_client_reservation_stand(query, context, client_id, reservation_id, target_id)

async def set_client_reservation_stand(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    reservation_id: int,
    target_reservation_id: int,
) -> None:
    try:
        client = await asyncio.to_thread(get_client, client_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load client %s", client_id)
        await query.edit_message_text(f"❌ Не удалось получить данные клиента: {exc}")
        return

    if not client:
        await query.edit_message_text("🔍 Клиент не найден.")
        return

    try:
        current_res = await asyncio.to_thread(get_reservation, reservation_id)
        target_res = await asyncio.to_thread(get_reservation, target_reservation_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception(
            "Failed to load reservations %s -> %s", reservation_id, target_reservation_id
        )
        await query.edit_message_text(f"❌ Не удалось прочитать данные брони: {exc}")
        return

    if not current_res or current_res.get("client_id") != client_id:
        await query.answer("Текущая бронь не найдена или уже изменена.", show_alert=True)
        return

    if not target_res:
        await show_client_reassign_menu(
            query,
            context,
            client_id,
            reservation_id,
            notice="Целевой станок больше не существует.",
        )
        return

    if current_res.get("slot_id") != target_res.get("slot_id"):
        await query.answer("Нельзя пересаживать между разными слотами.", show_alert=True)
        return

    if (target_res.get("status") or "").lower() != "available":
        await show_client_reassign_menu(
            query,
            context,
            client_id,
            reservation_id,
            notice="Этот станок уже занят.",
        )
        return

    slot_id = current_res.get("slot_id")
    try:
        slot_details = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for reassignment", slot_id)
        await query.edit_message_text(f"❌ Не удалось получить слот: {exc}")
        return

    if not slot_details:
        await query.edit_message_text("⚠️ Слот больше не существует.")
        return

    stands_map, bikes_map = await _load_inventory_maps()
    target_stand = stands_map.get(target_res.get("stand_id"))
    target_bike = None
    if isinstance(target_stand, dict):
        bike_id = target_stand.get("bike_id")
        if isinstance(bike_id, int):
            target_bike = bikes_map.get(bike_id)

    client_label = client_display_name(client)
    try:
        booked = await asyncio.to_thread(
            book_available_reservation,
            target_reservation_id,
            client_id=client_id,
            client_name=client_label,
            status="booked",
            source="adminbot",
            notes="reassigned via adminbot",
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to book target reservation %s", target_reservation_id)
        booked = None

    if not booked:
        await show_client_reassign_menu(
            query,
            context,
            client_id,
            reservation_id,
            notice="Не удалось занять выбранный станок. Его успели занять.",
        )
        return

    try:
        await asyncio.to_thread(
            update_reservation,
            reservation_id,
            client_id=None,
            client_name=None,
            status="available",
            source="adminbot",
            notes="released via adminbot",
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to release old reservation %s: %s", reservation_id, exc)

    if target_stand:
        stand_label = _format_stand_label_for_booking(target_stand, booked)
    else:
        stand_label = await _resolve_stand_label(booked)

    slot_summary = _format_slot_summary(slot_details)
    bike_line = ""
    bike_title = None
    if isinstance(target_bike, dict) and target_bike.get("title"):
        bike_title = target_bike["title"]
    elif isinstance(target_stand, dict) and target_stand.get("bike_title"):
        bike_title = target_stand["bike_title"]
    if bike_title:
        bike_line = f"\n🚲 Велосипед: {html.escape(str(bike_title))}."

    text = (
        f"✅ <b>{html.escape(client_label)}</b> пересажен на новый станок.\n"
        f"{html.escape(slot_summary)}\n"
        f"🛠 Место: {html.escape(stand_label)}.{bike_line}"
    )
    markup = build_booking_success_markup(client_id, booked.get("id"))
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

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
    if field == "gender":
        context.user_data["pending_client_edit"] = {
            "client_id": client_id,
            "field": field,
            "chat_id": query.message.chat_id,
            "message_id": query.message.message_id,
            "label": metadata["label"],
            "client_name": display_name,
            "mode": "picker",
        }
        await render_client_gender_picker(
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

    assignment = None
    try:
        await asyncio.to_thread(ensure_layout_table)
        assignment = await asyncio.to_thread(get_assignment_for_bike, bike_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to load layout for bike %s: %s", bike_id, exc)
        assignment = None

    text = format_bike_details(record, trainers)
    if assignment and assignment.get("stand_id"):
        trainer_stub = {
            "code": assignment.get("stand_code"),
            "title": assignment.get("stand_title"),
            "display_name": assignment.get("stand_display"),
            "id": assignment.get("stand_id"),
        }
        stand_label = format_trainer_button_label(trainer_stub)
        text = f"{text}\n\n🛠 На станке: {html.escape(stand_label)}"
    else:
        text = f"{text}\n\n🛠 На станке: не назначен."

    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_bike_info_markup(bike_id, assignment is not None),
    )

async def show_bike_info(query, context: ContextTypes.DEFAULT_TYPE, bike_id: int) -> None:
    context.user_data.pop(PENDING_BIKE_EDIT_KEY, None)
    await render_bike_info_message(
        context,
        query.message.chat_id,
        query.message.message_id,
        bike_id,
    )

async def render_bike_assignment_selector(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    bike_id: int,
) -> None:
    try:
        bike = await asyncio.to_thread(get_bike, bike_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load bike %s", bike_id)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения данных велосипеда: {exc}",
        )
        return

    if not bike:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔍 Велосипед не найден.",
        )
        return

    try:
        await asyncio.to_thread(ensure_trainers_table)
        trainers = await asyncio.to_thread(list_trainers, 100)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load trainers for bike assignment")
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"❌ Ошибка получения списка станков: {exc}",
        )
        return

    await asyncio.to_thread(ensure_layout_table)
    try:
        assignments = await asyncio.to_thread(list_layout_details)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to load layout details: %s", exc)
        assignments = []

    assignment_by_stand = {
        row.get("stand_id"): row
        for row in assignments
        if isinstance(row.get("stand_id"), int)
    }
    current_assignment = None
    for row in assignments:
        if row.get("bike_id") == bike_id:
            current_assignment = row
            break

    rows: List[List[InlineKeyboardButton]] = []
    for trainer in trainers:
        stand_id = trainer.get("id")
        if not isinstance(stand_id, int):
            continue
        label = _format_stand_selection_label(trainer, assignment_by_stand.get(stand_id))
        rows.append(
            [InlineKeyboardButton(text=label, callback_data=f"bike_assign_set|{bike_id}|{stand_id}")]
        )

    if not rows:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ В базе нет станков для назначения.",
        )
        return

    if current_assignment:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🧹 Снять со станка",
                    callback_data=f"bike_assign_clear|{bike_id}",
                )
            ]
        )

    rows.append(
        [InlineKeyboardButton(text="↩️ Назад", callback_data=f"bike_info|{bike_id}")]
    )

    bike_title = bike.get("title") or f"id={bike_id}"
    text = (
        f"🛠 Выберите станок для велосипеда <b>{html.escape(str(bike_title))}</b>.\n"
        "Если нужный станок недоступен, сначала освободите его."
    )

    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
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

    handled_events = await events_admin.handle_events_text(update, context, LOCAL_TIMEZONE)
    if handled_events:
        return

    # Intervals.icu pending input (API key + athlete_id)
    handled_intervals = await intervals_admin.handle_intervals_text(update, context)
    if handled_intervals:
        return

    handled_admins = await admins_view.handle_admin_text(update, context)
    if handled_admins:
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
