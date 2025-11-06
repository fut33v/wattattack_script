"""Entry points for the Krutilka VNB Telegram bot."""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Awaitable, Callable, Dict, Final, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, User
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from repositories import bikes_repository, schedule_repository, trainers_repository
from repositories.client_repository import create_client, get_client, search_clients
from repositories.client_link_repository import (
    get_link_by_client,
    get_link_by_user,
    link_user_to_client,
)
from repositories.admin_repository import get_admin_ids, is_admin

LOGGER = logging.getLogger(__name__)

_GREETING_KEY: Final[str] = "krutilkavnbot:greeting"
_CANDIDATES_KEY: Final[str] = "krutilkavnbot:candidates"
_FORM_KEY: Final[str] = "krutilkavnbot:form"
_PENDING_APPROVALS_KEY: Final[str] = "krutilkavnbot:pending_approvals"
_LAST_SEARCH_KEY: Final[str] = "krutilkavnbot:last_name"
_BOOKING_STATE_KEY: Final[str] = "krutilkavnbot:booking"

DEFAULT_GREETING: Final[str] = "Здравствуйте!"
MAX_SUGGESTIONS: Final[int] = 6

(
    ASK_LAST_NAME,
    CONFIRM_LINK,
    FORM_FIRST_NAME,
    FORM_LAST_NAME,
    FORM_WEIGHT,
    FORM_HEIGHT,
    FORM_GENDER,
    FORM_FTP,
    FORM_PEDALS,
    FORM_GOAL,
) = range(10)

BOOK_SELECT_DAY, BOOK_SELECT_SLOT = range(100, 102)

_PEDAL_CHOICES: Final[List[Tuple[str, str]]] = [
    ("топталки (под кроссовки)", "platform"),
    ("контакты шоссе Look", "road_look"),
    ("контакты шоссе Shimano", "road_shimano"),
    ("контакты MTB Shimano", "mtb_shimano"),
    ("принесу свои", "own"),
]

_PEDAL_LABEL_BY_CODE: Final[Dict[str, str]] = {code: label for label, code in _PEDAL_CHOICES}
_GENDER_LABELS: Final[Dict[str, str]] = {"male": "М", "female": "Ж"}
_WEEKDAY_SHORT: Final[List[str]] = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_BOOKING_CUTOFF: Final[timedelta] = timedelta(minutes=90)
_LOCAL_TZ: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")


def _normalize_last_name(value: str) -> str:
    return value.strip().lower()


def _format_client_label(client: Dict[str, Any]) -> str:
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


def _format_client_display_name(client: Dict[str, Any]) -> str:
    first_name = (client.get("first_name") or "").strip()
    last_name = (client.get("last_name") or "").strip()
    full_name = (client.get("full_name") or "").strip()
    if first_name and last_name:
        return f"{first_name} {last_name}".strip()
    if full_name:
        return full_name
    return last_name or first_name or "Клиент"


def _format_day_label(value: date) -> str:
    weekday_idx = value.weekday()
    prefix = _WEEKDAY_SHORT[weekday_idx] if 0 <= weekday_idx < len(_WEEKDAY_SHORT) else value.strftime("%a")
    return f"{prefix} {value.strftime('%d.%m')}"


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return None
    return None


def _parse_time(value: Any) -> Optional[time]:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time()
            except ValueError:
                continue
    return None


def _format_time_range(start_value: Any, end_value: Any) -> str:
    start = _parse_time(start_value)
    end = _parse_time(end_value)
    if not start or not end:
        return f"{start_value}-{end_value}"
    return f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"


def _format_slot_caption(slot: Dict[str, Any]) -> str:
    label_parts: List[str] = [_format_time_range(slot.get("start_time"), slot.get("end_time"))]
    slot_label = (slot.get("label") or "").strip()
    instructor_name = (slot.get("instructor_name") or "").strip()
    if slot_label:
        label_parts.append(slot_label)
    elif slot.get("session_kind") == "instructor" and instructor_name:
        label_parts.append(f"С инструктором ({instructor_name})")
    elif slot.get("session_kind") == "instructor":
        label_parts.append("С инструктором")
    free_count = slot.get("free_count")
    if isinstance(free_count, int):
        label_parts.append(f"мест {free_count}")
    return " · ".join(label_parts)


def _format_stand_label(stand: Optional[Dict[str, Any]], reservation: Optional[Dict[str, Any]] = None) -> str:
    if stand:
        for key in ("display_name", "code", "title"):
            value = stand.get(key)
            if value:
                return str(value)
        stand_id = stand.get("id")
        if stand_id is not None:
            return f"Станок {stand_id}"
    stand_code = (reservation or {}).get("stand_code")
    if stand_code:
        return str(stand_code)
    return "Станок"


def _slot_start_datetime(slot: Dict[str, Any]) -> Optional[datetime]:
    slot_date = _parse_date(slot.get("slot_date"))
    start_time_value = _parse_time(slot.get("start_time"))
    if slot_date and start_time_value:
        combined = datetime.combine(slot_date, start_time_value)
        return combined.replace(tzinfo=_LOCAL_TZ)
    return None


def _to_local_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(_LOCAL_TZ).replace(tzinfo=None)


def _local_now() -> datetime:
    return datetime.now(tz=_LOCAL_TZ)


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


def _choose_best_reservation(
    client: Dict[str, Any],
    reservations: List[Dict[str, Any]],
    *,
    stands_map: Dict[int, Dict[str, Any]],
    bikes_map: Dict[int, Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    client_height = _to_float(client.get("height"))
    favorite_bike_id = _match_favorite_bike_id(client.get("favorite_bike"), bikes_map)

    best_choice: Optional[Tuple[float, float, str, Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = None

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
        stand_label = _format_stand_label(stand, reservation)

        candidate = (score, position_score, stand_label.lower(), reservation, stand, bike)
        if best_choice is None or candidate[:3] < best_choice[:3]:
            best_choice = candidate

    if best_choice is None:
        return None, None, None

    return best_choice[3], best_choice[4], best_choice[5]


def _get_booking_state(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    state = context.user_data.get(_BOOKING_STATE_KEY)
    if state is None:
        state = {}
        context.user_data[_BOOKING_STATE_KEY] = state
    return state


def _clear_booking_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_BOOKING_STATE_KEY, None)


def _fetch_linked_client(user_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    try:
        link = get_link_by_user(user_id)
    except Exception:
        LOGGER.exception("Failed to fetch linked client for user %s", user_id)
        return None, None
    if not link:
        return None, None

    client_id = link.get("client_id")
    if not isinstance(client_id, int):
        return link, None

    try:
        client = get_client(client_id)
    except Exception:
        LOGGER.exception("Failed to load client record %s for user %s", client_id, user_id)
        return link, None

    return link, client


def _group_slots_by_day(slots: List[Dict[str, Any]]) -> List[Tuple[date, List[Dict[str, Any]]]]:
    grouped: Dict[date, List[Dict[str, Any]]] = {}
    for slot in slots:
        slot_date = slot.get("slot_date")
        if not isinstance(slot_date, date):
            continue
        grouped.setdefault(slot_date, []).append(slot)
    for slot_list in grouped.values():
        slot_list.sort(key=lambda item: (item.get("start_time"), item.get("id")))
    return sorted(grouped.items(), key=lambda item: item[0])


def _build_day_keyboard(day_slots: List[Tuple[date, List[Dict[str, Any]]]]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for slot_date, slots in day_slots:
        total_free = sum(slot.get("free_count") or 0 for slot in slots)
        label = f"{_format_day_label(slot_date)} · свободно {total_free}"
        buttons.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"book:day:{slot_date.isoformat()}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="book:cancel")])
    return InlineKeyboardMarkup(buttons)


def _build_slot_keyboard(slots: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for slot in slots:
        caption = _format_slot_caption(slot)
        slot_id = slot.get("id")
        if not isinstance(slot_id, int):
            continue
        buttons.append(
            [
                InlineKeyboardButton(
                    text=caption,
                    callback_data=f"book:slot:{slot_id}",
                )
            ]
        )
    nav_row = [
        InlineKeyboardButton(text="← Назад", callback_data="book:back"),
        InlineKeyboardButton(text="Отмена", callback_data="book:cancel"),
    ]
    buttons.append(nav_row)
    return InlineKeyboardMarkup(buttons)


async def _present_day_selection(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    reply: Callable[[str, InlineKeyboardMarkup], Awaitable[None]],
    horizon_days: int = 21,
) -> bool:
    now_local = _local_now()
    end = now_local + timedelta(days=horizon_days)
    cutoff_threshold = now_local + _BOOKING_CUTOFF
    if end <= cutoff_threshold:
        end = cutoff_threshold + timedelta(days=1)
    try:
        slots_raw = schedule_repository.list_available_slots(
            _to_local_naive(now_local),
            _to_local_naive(end),
        )
    except Exception:
        LOGGER.exception("Failed to load available slots for booking")
        return False

    slots = []
    for slot in slots_raw:
        start_dt = _slot_start_datetime(slot)
        if start_dt is None:
            continue
        if start_dt - now_local < _BOOKING_CUTOFF:
            continue
        slots.append(slot)

    if not slots:
        return False

    grouped = _group_slots_by_day(slots)
    if not grouped:
        return False

    limited = grouped[:10]
    booking_state = _get_booking_state(context)
    booking_state["day_map"] = {day.isoformat(): day_slots for day, day_slots in limited}
    booking_state["horizon_start"] = cutoff_threshold.isoformat()
    booking_state["horizon_end"] = end

    text = "Выберите день для бронирования тренировки:"
    markup = _build_day_keyboard(limited)
    await reply(text, markup)
    return True


async def _send_day_selection_message(message, context: ContextTypes.DEFAULT_TYPE) -> bool:
    async def _reply(text: str, markup: InlineKeyboardMarkup) -> None:
        await message.reply_text(text, reply_markup=markup)

    return await _present_day_selection(context, reply=_reply)


async def _edit_day_selection_message(query, context: ContextTypes.DEFAULT_TYPE) -> bool:
    async def _reply(text: str, markup: InlineKeyboardMarkup) -> None:
        await query.edit_message_text(text, reply_markup=markup)

    return await _present_day_selection(context, reply=_reply)


async def _book_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return ConversationHandler.END

    _clear_booking_state(context)
    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start, затем повторите попытку.")
        return ConversationHandler.END

    state = _get_booking_state(context)
    state["client"] = client
    state["client_id"] = client.get("id")
    state["link"] = link

    success = await _send_day_selection_message(message, context)
    if not success or "day_map" not in state:
        await message.reply_text("Свободных слотов пока нет. Попробуйте позже.")
        _clear_booking_state(context)
        return ConversationHandler.END

    return BOOK_SELECT_DAY


async def _handle_booking_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None:
        return ConversationHandler.END
    await query.answer()
    data = query.data or ""
    try:
        _, _, date_str = data.split(":", 2)
        selected_date = date.fromisoformat(date_str)
    except (ValueError, AttributeError):
        return BOOK_SELECT_DAY

    state = _get_booking_state(context)
    day_map = state.get("day_map") or {}
    if date_str not in day_map:
        success = await _edit_day_selection_message(query, context)
        if not success:
            await query.edit_message_text("Выбор устарел. Запустите /book заново.")
            _clear_booking_state(context)
            return ConversationHandler.END
        return BOOK_SELECT_DAY
    state["selected_date"] = date_str

    start_dt_local = datetime.combine(selected_date, time.min, tzinfo=_LOCAL_TZ)
    end_dt_local = datetime.combine(selected_date, time.max, tzinfo=_LOCAL_TZ)

    try:
        slots = schedule_repository.list_available_slots(
            _to_local_naive(start_dt_local),
            _to_local_naive(end_dt_local),
        )
    except Exception:
        LOGGER.exception("Failed to load slots for %s", date_str)
        await query.edit_message_text("Не удалось загрузить список слотов. Попробуйте позже.")
        _clear_booking_state(context)
        return ConversationHandler.END

    now_check = _local_now()
    filtered_slots: List[Dict[str, Any]] = []
    for slot in slots:
        if not slot.get("free_count"):
            continue
        start_dt_candidate = _slot_start_datetime(slot)
        if start_dt_candidate is None:
            continue
        if start_dt_candidate - now_check < _BOOKING_CUTOFF:
            continue
        filtered_slots.append(slot)
    slots = filtered_slots
    if not slots:
        success = await _edit_day_selection_message(query, context)
        if not success:
            await query.edit_message_text("Свободных мест не осталось. Попробуйте позже.")
            _clear_booking_state(context)
            return ConversationHandler.END
        return BOOK_SELECT_DAY

    state["slots_map"] = {slot["id"]: slot for slot in slots if isinstance(slot.get("id"), int)}

    text = f"Свободные слоты { _format_day_label(selected_date)}:"
    markup = _build_slot_keyboard(slots)
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except Exception:
        LOGGER.exception("Failed to present slots for %s", date_str)
        _clear_booking_state(context)
        return ConversationHandler.END

    return BOOK_SELECT_SLOT


async def _handle_booking_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None:
        return ConversationHandler.END
    await query.answer()
    success = await _edit_day_selection_message(query, context)
    if not success:
        await query.edit_message_text("Свободных слотов пока нет. Попробуйте позже.")
        _clear_booking_state(context)
        return ConversationHandler.END
    return BOOK_SELECT_DAY


async def _handle_booking_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is not None:
        await query.answer("Бронирование отменено")
        try:
            await query.edit_message_text("Бронирование отменено.")
        except Exception:
            LOGGER.debug("Failed to edit cancel message", exc_info=True)
    _clear_booking_state(context)
    return ConversationHandler.END


async def _booking_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is not None:
        await message.reply_text("Бронирование отменено.")
    _clear_booking_state(context)
    return ConversationHandler.END


async def _handle_booking_slot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None:
        return ConversationHandler.END
    await query.answer()

    data = query.data or ""
    try:
        reservation_slot_id = int(data.split(":", 2)[2])
    except (ValueError, IndexError):
        return BOOK_SELECT_SLOT

    state = _get_booking_state(context)
    client = state.get("client")
    if not isinstance(client, dict) or not isinstance(client.get("id"), int):
        await query.edit_message_text("Сессия бронирования устарела. Запустите /book снова.")
        _clear_booking_state(context)
        return ConversationHandler.END

    try:
        slot = schedule_repository.get_slot_with_reservations(reservation_slot_id)
    except Exception:
        LOGGER.exception("Failed to load slot detail %s", reservation_slot_id)
        slot = None

    if not slot:
        success = await _edit_day_selection_message(query, context)
        if not success:
            await query.edit_message_text("Слот недоступен. Попробуйте начать заново.")
            _clear_booking_state(context)
            return ConversationHandler.END
        return BOOK_SELECT_DAY

    slot_start_dt = _slot_start_datetime(slot)
    if slot_start_dt is not None:
        if slot_start_dt - _local_now() < _BOOKING_CUTOFF:
            await query.answer("До начала менее 1,5 часов, слот недоступен для брони.", show_alert=True)
            success = await _edit_day_selection_message(query, context)
            if not success:
                await query.edit_message_text("Выберите другой слот при следующей попытке.")
                _clear_booking_state(context)
                return ConversationHandler.END
            return BOOK_SELECT_DAY

    reservations = slot.get("reservations") or []
    for existing in reservations:
        if existing.get("client_id") == client.get("id"):
            await query.edit_message_text("Вы уже записаны на этот слот.")
            _clear_booking_state(context)
            return ConversationHandler.END

    available_reservations = [
        res for res in reservations if (res.get("status") or "").lower() == "available"
    ]
    if not available_reservations:
        success = await _edit_day_selection_message(query, context)
        if not success:
            await query.edit_message_text("Свободных мест не осталось. Попробуйте позже.")
            _clear_booking_state(context)
            return ConversationHandler.END
        return BOOK_SELECT_DAY

    try:
        stands = trainers_repository.list_trainers()
    except Exception:
        LOGGER.exception("Failed to load trainers for booking")
        stands = []
    stands_map = {
        row.get("id"): row for row in stands if isinstance(row.get("id"), int)
    }

    try:
        bikes = bikes_repository.list_bikes()
    except Exception:
        LOGGER.exception("Failed to load bikes for booking")
        bikes = []
    bikes_map = {
        row.get("id"): row for row in bikes if isinstance(row.get("id"), int)
    }

    reservation, stand, bike = _choose_best_reservation(
        client,
        available_reservations,
        stands_map=stands_map,
        bikes_map=bikes_map,
    )
    if reservation is None:
        success = await _edit_day_selection_message(query, context)
        if not success:
            await query.edit_message_text("Не удалось подобрать свободный станок.")
            _clear_booking_state(context)
            return ConversationHandler.END
        return BOOK_SELECT_DAY

    client_display_name = _format_client_display_name(client)
    try:
        booked_row = schedule_repository.book_available_reservation(
            reservation["id"],
            client_id=client["id"],
            client_name=client_display_name,
            source="krutilkavnbot",
        )
    except Exception:
        LOGGER.exception("Failed to update reservation %s", reservation["id"])
        booked_row = None

    if not booked_row:
        await query.answer("К сожалению, место только что заняли.", show_alert=True)
        selected_date_str = state.get("selected_date")
        if selected_date_str:
            try:
                selected_date = date.fromisoformat(selected_date_str)
            except ValueError:
                selected_date = slot.get("slot_date")
        else:
            selected_date = slot.get("slot_date")

        selected_date_obj = _parse_date(selected_date)
        if selected_date_obj:
            cutoff_time = _local_now() + _BOOKING_CUTOFF
            start_dt_local = datetime.combine(selected_date_obj, time.min, tzinfo=_LOCAL_TZ)
            if start_dt_local < cutoff_time:
                start_dt_local = cutoff_time
            end_dt_local = datetime.combine(selected_date_obj, time.max, tzinfo=_LOCAL_TZ)
            if end_dt_local <= start_dt_local:
                end_dt_local = start_dt_local + timedelta(minutes=1)
            try:
                refreshed = schedule_repository.list_available_slots(
                    _to_local_naive(start_dt_local),
                    _to_local_naive(end_dt_local),
                )
            except Exception:
                LOGGER.exception("Failed to refresh slots for %s", selected_date)
                refreshed = []

            now_refresh = _local_now()
            filtered_refreshed: List[Dict[str, Any]] = []
            for item in refreshed:
                if not item.get("free_count"):
                    continue
                start_dt_candidate = _slot_start_datetime(item)
                if start_dt_candidate is None:
                    continue
                if start_dt_candidate - now_refresh < _BOOKING_CUTOFF:
                    continue
                filtered_refreshed.append(item)
            refreshed = filtered_refreshed
            if refreshed:
                state["slots_map"] = {
                    item["id"]: item for item in refreshed if isinstance(item.get("id"), int)
                }
                markup = _build_slot_keyboard(refreshed)
                text = f"Свободные слоты {_format_day_label(selected_date_obj)}:"
                await query.edit_message_text(text, reply_markup=markup)
                return BOOK_SELECT_SLOT

        success = await _edit_day_selection_message(query, context)
        if not success:
            await query.edit_message_text("Свободных мест больше нет. Попробуйте позже.")
            _clear_booking_state(context)
            return ConversationHandler.END
        return BOOK_SELECT_DAY

    stand_label = _format_stand_label(stand, reservation)
    bike_label = None
    if bike:
        bike_title = (bike.get("title") or "").strip()
        bike_owner = (bike.get("owner") or "").strip()
        if bike_owner:
            bike_label = f"{bike_title} ({bike_owner})" if bike_title else bike_owner
        else:
            bike_label = bike_title or None

    slot_label = (slot.get("label") or "").strip()
    instructor_note = ""
    if slot.get("session_kind") == "instructor":
        instructor_name = (slot.get("instructor_name") or "").strip()
        if instructor_name:
            instructor_note = f"\nИнструктор: {instructor_name}"
        else:
            instructor_note = "\nИнструктор: уточняется"

    when_label = ""
    slot_date_val = _parse_date(slot.get("slot_date"))
    start_time_val = _parse_time(slot.get("start_time"))
    if slot_date_val and start_time_val:
        when_label = f"{slot_date_val.strftime('%d.%m.%Y')} в {start_time_val.strftime('%H:%M')}"

    summary_lines = [
        "✅ Запись подтверждена!",
        f"{client_display_name}, вы записаны на {when_label}." if when_label else "Запись создана.",
        f"Станок: {stand_label}",
    ]
    if slot_label:
        summary_lines.append(f"Слот: {slot_label}")
    if bike_label:
        summary_lines.append(f"Велосипед: {bike_label}")
    if instructor_note:
        summary_lines.append(instructor_note.strip())
    summary_lines.append("До встречи в «Крутилке»!")

    try:
        await query.edit_message_text("\n".join(summary_lines))
    except Exception:
        LOGGER.debug("Failed to edit confirmation message", exc_info=True)
        try:
            await query.message.reply_text("\n".join(summary_lines))
        except Exception:
            LOGGER.exception("Failed to send booking confirmation follow-up")

    _clear_booking_state(context)
    return ConversationHandler.END


def _find_clients_by_last_name(last_name: str) -> List[Dict[str, Any]]:
    normalized = _normalize_last_name(last_name)
    results = search_clients(last_name, limit=MAX_SUGGESTIONS * 2)
    exact_matches = [
        client for client in results if _normalize_last_name(client.get("last_name") or "") == normalized
    ]
    if exact_matches:
        return exact_matches[:MAX_SUGGESTIONS]
    return results[:MAX_SUGGESTIONS]


def _store_candidates(context: ContextTypes.DEFAULT_TYPE, clients: List[Dict[str, Any]]) -> None:
    context.user_data[_CANDIDATES_KEY] = {str(client["id"]): client for client in clients}


def _get_candidates(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Dict[str, Any]]:
    return context.user_data.get(_CANDIDATES_KEY, {})


def _clear_candidates(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_CANDIDATES_KEY, None)


def _get_form(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    form = context.user_data.get(_FORM_KEY)
    if form is None:
        form = {}
        context.user_data[_FORM_KEY] = form
    return form


def _start_new_client_form(context: ContextTypes.DEFAULT_TYPE, last_name: str) -> Dict[str, Any]:
    form = {
        "last_name": last_name.strip(),
        "ftp": 150.0,
    }
    context.user_data[_FORM_KEY] = form
    return form


def _clear_form(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_FORM_KEY, None)


def _pending_approvals(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Dict[str, Any]]:
    approvals = context.application.bot_data.setdefault(_PENDING_APPROVALS_KEY, {})
    return approvals


def _store_pending_request(context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any]) -> None:
    approvals = _pending_approvals(context)
    approvals[request["request_id"]] = request


def _get_pending_request(context: ContextTypes.DEFAULT_TYPE, request_id: str) -> Optional[Dict[str, Any]]:
    approvals = _pending_approvals(context)
    return approvals.get(request_id)


def _pop_pending_request(context: ContextTypes.DEFAULT_TYPE, request_id: str) -> Optional[Dict[str, Any]]:
    approvals = _pending_approvals(context)
    return approvals.pop(request_id, None)


def _format_user_label(user: Optional[User]) -> str:
    if user is None:
        return "неизвестный пользователь"

    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    parts = [part for part in [first, last] if part]
    display = " ".join(parts) if parts else (user.username or f"id {user.id}")
    if user.username:
        handle = user.username if user.username.startswith("@") else f"@{user.username}"
        display = f"{display} ({handle})"
    return display


def _is_admin_user(user: Optional[User]) -> bool:
    tg_id = user.id if user else None
    username = user.username if user else None
    try:
        return is_admin(tg_id=tg_id, username=username)
    except Exception:
        LOGGER.exception("Failed to check admin status for user %s", tg_id)
        return False


def _parse_optional_float(text: str) -> Tuple[bool, Optional[float]]:
    value = text.replace(",", ".").strip()
    if not value or value == "-":
        return True, None
    try:
        return True, float(value)
    except ValueError:
        return False, None


def _parse_positive_float(text: str) -> Tuple[bool, Optional[float]]:
    ok, value = _parse_optional_float(text)
    if not ok or value is None:
        return False, None
    if value <= 0:
        return False, None
    return True, value


def _format_optional_number(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:g}"


def _format_gender_label(gender: Optional[str]) -> str:
    if gender is None:
        return "—"
    return _GENDER_LABELS.get(gender, gender)


def _skip_keyboard(callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("ОК", callback_data=callback)]])


async def _send_gender_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("М", callback_data="form:set:gender:male"),
                InlineKeyboardButton("Ж", callback_data="form:set:gender:female"),
            ]
        ]
    )
    await context.bot.send_message(chat_id, "👤 Выберите ваш пол:", reply_markup=keyboard)


async def _send_pedals_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"form:set:pedals:{code}")]
        for label, code in _PEDAL_CHOICES
    ]
    await context.bot.send_message(chat_id, "🚴 Выберите тип педалей:", reply_markup=InlineKeyboardMarkup(rows))


async def _send_ftp_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    await context.bot.send_message(
        chat_id,
        "⚡ Введите ваш FTP (ватты). По умолчанию 150. Чтобы оставить значение по умолчанию, нажмите «ОК».",
        reply_markup=_skip_keyboard("form:skip:ftp"),
    )


async def _send_goal_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    await context.bot.send_message(
        chat_id,
        "🎯 Опишите вашу цель (например, подготовка к старту).\n"
        "Если не хотите указывать, нажмите «ОК».",
        reply_markup=_skip_keyboard("form:skip:goal"),
    )


async def _request_admin_approval(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user: User,
    client: Dict[str, Any],
    existing: Optional[Dict[str, Any]],
    respond_initial: Callable[[str], Awaitable[Any]],
    user_chat_id: int,
    on_failure: Optional[Callable[[str], Awaitable[Any]]] = None,
) -> bool:
    try:
        admin_ids = [admin_id for admin_id in get_admin_ids() if admin_id]
    except Exception:
        LOGGER.exception("Failed to load admin IDs for approval request")
        admin_ids = []

    if not admin_ids:
        message = "Не удалось отправить запрос на подтверждение: не найдены администраторы."
        await respond_initial(message)
        if on_failure:
            await on_failure("Запрос не обработан. Обратитесь к администратору, чтобы настроить доступ.")
        else:
            await context.bot.send_message(
                user_chat_id,
                "Запрос не обработан. Обратитесь к администратору, чтобы настроить доступ.",
            )
        return False

    request_id = uuid4().hex
    tg_username = user.username if user.username else None
    tg_full_name = " ".join(filter(None, [user.first_name, user.last_name])).strip() or None

    request: Dict[str, Any] = {
        "request_id": request_id,
        "client": client,
        "user_id": user.id,
        "user_username": tg_username,
        "user_full_name": tg_full_name,
        "user_chat_id": user_chat_id,
        "existing": existing,
        "admin_messages": [],
    }
    _store_pending_request(context, request)

    await respond_initial(
        f"Запрос на привязку клиента {_format_client_label(client)} отправлен администратору. "
        "Ожидайте подтверждения.",
    )

    await _notify_admins(context, request, admin_ids)
    if not request["admin_messages"]:
        _pop_pending_request(context, request_id)
        failure_text = (
            "Не удалось отправить запрос администраторам. Попробуйте позже или свяжитесь с поддержкой."
        )
        await respond_initial(failure_text)
        if on_failure:
            await on_failure("Запрос не обработан. Сообщите об этом администратору или повторите позже.")
        else:
            await context.bot.send_message(
                user_chat_id,
                "Запрос не обработан. Сообщите об этом администратору или повторите позже.",
            )
        return False

    return True


async def _start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        LOGGER.debug("Cannot handle /start without message or user in update %s", update.update_id)
        return ConversationHandler.END

    _clear_candidates(context)
    _clear_form(context)
    greeting: str = context.application.bot_data.get(_GREETING_KEY, DEFAULT_GREETING)

    linked_client_name: Optional[str] = None
    try:
        existing = get_link_by_user(user.id)
        if existing:
            client = get_client(existing["client_id"])
            if client:
                linked_client_name = _format_client_label(client)
    except Exception:
        LOGGER.exception("Failed to check existing link for user %s", user.id)

    intro = (
        "👋 Это бот для записи в первую велостудию в Великом Новгороде «Крутилку».\n"
        "Если вы уже были у нас, мы найдём вашу анкету и привяжем её к Telegram.\n"
        "Если вы новый участник, создадим анкету прямо здесь."
    )

    if linked_client_name:
        text = (
            f"{greeting}\n\n{intro}\n\n"
            f"✅ Уже привязаны к {linked_client_name}.\n"
            "Чтобы изменить связь, отправьте свою фамилию снова.\n\n"
            "Для бронирования свободных слотов используйте команду /book."
        )
    else:
        text = (
            f"{greeting}\n\n{intro}\n\n"
            "Пожалуйста, введите свою фамилию, чтобы продолжить. "
            "После подтверждения анкеты можно будет записываться через /book."
        )

    await message.reply_text(text)
    return ASK_LAST_NAME


async def _help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        "Отправьте /start, чтобы пройти авторизацию и привязать свою анкету. "
        "Если клиента нет в базе, можно заполнить короткую анкету для новой записи. "
        "После привязки используйте /book, чтобы выбрать удобный слот и забронировать тренировку.",
    )


async def _unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text("Команда не поддерживается. Используйте /start для авторизации.")


async def _handle_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return ASK_LAST_NAME

    _clear_candidates(context)
    _clear_form(context)

    last_name = message.text.strip()
    if not last_name:
        await message.reply_text("⚠️ Пожалуйста, отправьте фамилию текстом.")
        return ASK_LAST_NAME

    context.user_data[_LAST_SEARCH_KEY] = last_name

    try:
        clients = _find_clients_by_last_name(last_name)
    except Exception:
        LOGGER.exception("Failed to search clients by last name %r", last_name)
        await message.reply_text("Не удалось выполнить поиск. Попробуйте ещё раз позже.")
        return ASK_LAST_NAME

    if not clients:
        _start_new_client_form(context, last_name)
        await message.reply_text(
            "🔎 Клиентов с такой фамилией не нашлось. Давайте создадим новую запись.\n"
            "🖊️ Введите своё имя:"
        )
        _clear_candidates(context)
        return FORM_FIRST_NAME

    _store_candidates(context, clients)

    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=_format_client_label(client),
                callback_data=f"link:{client['id']}",
            )
        ]
        for client in clients
    ]
    rows.append([InlineKeyboardButton("Создать новую запись", callback_data="new_client")])
    keyboard = InlineKeyboardMarkup(rows)
    lines = ["📋 Нашлись такие клиенты:"]
    for client in clients:
        lines.append(f"• {_format_client_label(client)}")
    if len(clients) >= MAX_SUGGESTIONS:
        lines.append("Если не нашли нужного, уточните фамилию и отправьте снова.")
    lines.append("ℹ️ Или нажмите «Создать новую запись», чтобы заполнить анкету.")

    await message.reply_text("\n".join(lines), reply_markup=keyboard)
    return CONFIRM_LINK


async def _handle_link_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        LOGGER.debug("Callback without query or user in update %s", update.update_id)
        return ASK_LAST_NAME

    await query.answer()
    data = query.data or ""
    if not data.startswith("link:"):
        await query.answer("Неизвестное действие.", show_alert=True)
        return ASK_LAST_NAME

    client_id = data.split(":", 1)[1]
    candidates = _get_candidates(context)
    client = candidates.get(client_id)
    if client is None:
        await query.answer("Список устарел. Отправьте фамилию ещё раз.", show_alert=True)
        return ASK_LAST_NAME

    try:
        existing = get_link_by_client(client["id"])
    except Exception:
        existing = None

    success = await _request_admin_approval(
        context=context,
        user=user,
        client=client,
        existing=existing,
        respond_initial=query.edit_message_text,
        user_chat_id=query.message.chat_id if query.message else user.id,
        on_failure=query.message.reply_text if query.message else None,
    )

    if success and query.message:
        await query.message.reply_text(
            "Запрос отправлен администраторам. После подтверждения привязки вы получите уведомление. "
            "Чтобы выбрать другого клиента, отправьте новую фамилию.",
        )

    return ASK_LAST_NAME


async def _handle_new_client_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return FORM_FIRST_NAME

    await query.answer()
    last_name = context.user_data.get(_LAST_SEARCH_KEY, "")

    _clear_candidates(context)
    _start_new_client_form(context, last_name)

    try:
        await query.edit_message_text("🆕 Создание новой записи.")
    except Exception:
        LOGGER.debug("Failed to edit new-client message", exc_info=True)

    prompt_lines = []
    if last_name:
        prompt_lines.append(f"✅ Фамилия сохранена: {last_name}")
    prompt_lines.append("🖊️ Введите своё имя:")
    await query.message.reply_text("\n".join(prompt_lines))
    return FORM_FIRST_NAME


async def _handle_form_first_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return FORM_FIRST_NAME

    first_name = message.text.strip()
    if not first_name:
        await message.reply_text("⚠️ Имя не должно быть пустым. Пожалуйста, введите своё имя.")
        return FORM_FIRST_NAME

    form = _get_form(context)
    form["first_name"] = first_name
    current_last_name = form.get("last_name") or ""
    if current_last_name:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Оставить без изменений", callback_data="form:keep:last_name")]]
        )
        await message.reply_text(
            f"🧾 Подтвердите свою фамилию (сейчас: {current_last_name}) или введите новую.",
            reply_markup=keyboard,
        )
    else:
        await message.reply_text("🖊️ Введите свою фамилию:")
    return FORM_LAST_NAME


async def _handle_form_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return FORM_LAST_NAME

    value = message.text.strip()
    if not value:
        await message.reply_text("⚠️ Фамилия не может быть пустой. Введите свою фамилию.")
        return FORM_LAST_NAME

    form = _get_form(context)
    form["last_name"] = value
    await message.reply_text(
        "⚖️ Введите ваш вес в килограммах (например, 72.5). Это обязательное поле."
    )
    return FORM_WEIGHT


async def _keep_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return FORM_LAST_NAME

    await query.answer()
    form = _get_form(context)
    last_name = (form.get("last_name") or "").strip()
    if not last_name:
        await query.answer("Фамилия ещё не указана.", show_alert=True)
        return FORM_LAST_NAME

    try:
        await query.edit_message_text(f"✅ Фамилия: {last_name}")
    except Exception:
        LOGGER.debug("Failed to edit keep-last-name message", exc_info=True)

    await query.message.reply_text(
        "⚖️ Введите ваш вес в килограммах (например, 72.5). Это обязательное поле."
    )
    return FORM_WEIGHT


async def _handle_form_weight(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return FORM_WEIGHT

    ok, weight = _parse_positive_float(message.text)
    if not ok or weight is None:
        await message.reply_text(
            "⚠️ Вес должен быть положительным числом (например, 72.5). Попробуйте ещё раз."
        )
        return FORM_WEIGHT

    form = _get_form(context)
    form["weight"] = weight
    await message.reply_text(
        "📏 Введите ваш рост в сантиметрах (например, 178). Это обязательное поле."
    )
    return FORM_HEIGHT


async def _handle_form_height(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return FORM_HEIGHT

    ok, height = _parse_positive_float(message.text)
    if not ok or height is None:
        await message.reply_text(
            "⚠️ Рост должен быть положительным числом (например, 178). Попробуйте ещё раз."
        )
        return FORM_HEIGHT

    form = _get_form(context)
    form["height"] = height
    await _send_gender_prompt(context, message.chat_id)
    return FORM_GENDER


async def _handle_gender_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return FORM_GENDER

    await query.answer()
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return FORM_GENDER
    gender_code = parts[3]
    if gender_code not in {"male", "female"}:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return FORM_GENDER

    form = _get_form(context)
    form["gender"] = gender_code

    label = "М" if gender_code == "male" else "Ж"
    try:
        await query.edit_message_text(f"👤 Пол: {label}")
    except Exception:
        LOGGER.debug("Failed to edit gender selection message", exc_info=True)

    await _send_ftp_prompt(context, query.message.chat_id)
    return FORM_FTP


async def _prompt_gender_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("👤 Пожалуйста, выберите пол с помощью кнопок «М» или «Ж».")
    return FORM_GENDER


async def _handle_form_ftp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return FORM_FTP

    ok, ftp = _parse_optional_float(message.text)
    if not ok or (ftp is not None and ftp <= 0):
        await message.reply_text(
            "⚠️ Не удалось распознать FTP. Введите число (например, 215) или нажмите «ОК», чтобы оставить значение по умолчанию (150)."
        )
        return FORM_FTP

    form = _get_form(context)
    form["ftp"] = 150.0 if ftp is None else ftp
    await _send_pedals_prompt(context, message.chat_id)
    return FORM_PEDALS


async def _skip_ftp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return FORM_FTP

    await query.answer()
    form = _get_form(context)
    form["ftp"] = form.get("ftp") or 150.0
    try:
        await query.edit_message_text("ℹ️ FTP оставлен по умолчанию (150).")
    except Exception:
        LOGGER.debug("Failed to edit FTP skip message", exc_info=True)
    await _send_pedals_prompt(context, query.message.chat_id)
    return FORM_PEDALS


async def _handle_form_pedals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("🚴 Пожалуйста, выберите тип педалей с помощью кнопок ниже.")
    return FORM_PEDALS


async def _handle_pedals_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.message is None:
        return FORM_PEDALS

    form = _get_form(context)
    await query.answer()
    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return FORM_PEDALS
    code = parts[3]
    label = _PEDAL_LABEL_BY_CODE.get(code)
    if label is None:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return FORM_PEDALS

    form["pedals"] = label
    try:
        await query.edit_message_text(f"🚴 Тип педалей: {label}")
    except Exception:
        LOGGER.debug("Failed to edit pedals selection message", exc_info=True)
    await _send_goal_prompt(context, query.message.chat_id)
    return FORM_GOAL


async def _handle_form_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or message.text is None:
        return FORM_GOAL

    value = message.text.strip()
    form = _get_form(context)
    form["goal"] = value or None

    return await _finalize_client_creation(
        context=context,
        user=user,
        chat_id=message.chat_id,
        send_message=message.reply_text,
    )


async def _skip_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user = update.effective_user
    if query is None or query.message is None or user is None:
        return FORM_GOAL

    await query.answer()
    form = _get_form(context)
    form["goal"] = None
    try:
        await query.edit_message_text("ℹ️ Цель не указана.")
    except Exception:
        LOGGER.debug("Failed to edit goal skip message", exc_info=True)

    return await _finalize_client_creation(
        context=context,
        user=user,
        chat_id=query.message.chat_id,
        send_message=lambda text: context.bot.send_message(query.message.chat_id, text),
    )


async def _finalize_client_creation(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    user: User,
    chat_id: int,
    send_message: Callable[[str], Awaitable[Any]],
) -> int:
    form = _get_form(context)

    first_name = (form.get("first_name") or "").strip()
    last_name = (form.get("last_name") or "").strip()
    weight = form.get("weight")
    height = form.get("height")
    gender = form.get("gender")
    ftp = form.get("ftp", 150.0)
    pedals = form.get("pedals")
    goal = form.get("goal")

    if (
        not first_name
        or not last_name
        or weight is None
        or height is None
        or pedals is None
        or gender not in {"male", "female"}
    ):
        await send_message("⚠️ Анкета заполнена не полностью. Начните заново командой /start.")
        _clear_form(context)
        return ASK_LAST_NAME

    try:
        client = create_client(
            first_name=first_name,
            last_name=last_name,
            weight=weight,
            height=height,
            gender=gender,
            ftp=ftp,
            pedals=pedals,
            goal=goal,
        )
    except Exception:
        LOGGER.exception("Failed to create client record for %s %s", first_name, last_name)
        await send_message(
            "⚠️ Не удалось создать запись. Попробуйте ещё раз позже или обратитесь к администратору."
        )
        _clear_form(context)
        return ASK_LAST_NAME

    summary_lines = [
        "📝 Анкета (проверьте данные):",
        f"• Имя: {first_name}",
        f"• Фамилия: {last_name}",
        f"• Пол: {_format_gender_label(gender)}",
        f"• Вес: {_format_optional_number(weight)} кг",
        f"• Рост: {_format_optional_number(height)} см",
        f"• FTP: {_format_optional_number(ftp)} Вт",
        f"• Педали: {pedals}",
        f"• Цель: {goal or '—'}",
    ]
    await send_message("\n".join(summary_lines))

    tg_username = user.username if user.username else None
    tg_full_name = " ".join(filter(None, [user.first_name, user.last_name])).strip() or None

    try:
        link_user_to_client(
            tg_user_id=user.id,
            client_id=client["id"],
            tg_username=tg_username,
            tg_full_name=tg_full_name,
        )
        await send_message(f"✨ Создана новая запись: {_format_client_label(client)}.")
        await send_message(
            "🔗 Ваш Telegram автоматически привязан к новой анкете. Готово!"
        )
    except Exception:
        LOGGER.exception("Failed to link new client %s to user %s", client["id"], user.id)
        await send_message(
            "⚠️ Запись создана, но привязка не удалась. Попробуйте ещё раз позже или обратитесь к администратору."
        )

    _clear_form(context)

    return ASK_LAST_NAME


async def _cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _clear_candidates(context)
    _clear_form(context)
    message = update.effective_message
    if message is not None:
        await message.reply_text("Авторизация прервана. Чтобы начать заново, используйте /start.")
    return ConversationHandler.END


async def _notify_admins(
    context: ContextTypes.DEFAULT_TYPE,
    request: Dict[str, Any],
    admin_ids: List[int],
) -> None:
    client = request["client"]
    user_id = request["user_id"]
    user_username = request.get("user_username")
    user_full_name = request.get("user_full_name")
    existing = request.get("existing")

    client_label = _format_client_label(client)
    user_label_parts: List[str] = []
    if user_full_name:
        user_label_parts.append(user_full_name)
    if user_username:
        handle = user_username if user_username.startswith("@") else f"@{user_username}"
        user_label_parts.append(handle)
    user_label_parts.append(f"id {user_id}")
    user_label = ", ".join(user_label_parts)

    lines = [
        "Новый запрос на привязку клиента.",
        f"Клиент: {client_label}",
        f"Пользователь: {user_label}",
    ]

    if existing and existing.get("tg_user_id"):
        existing_user_id = existing.get("tg_user_id")
        existing_username = existing.get("tg_username")
        existing_full_name = existing.get("tg_full_name")
        existing_parts = []
        if existing_full_name:
            existing_parts.append(existing_full_name)
        if existing_username:
            handle = existing_username if existing_username.startswith("@") else f"@{existing_username}"
            existing_parts.append(handle)
        if existing_user_id:
            existing_parts.append(f"id {existing_user_id}")
        lines.append("Предыдущая связь: " + ", ".join(existing_parts))

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Подтвердить", callback_data=f"approve:{request['request_id']}"),
                InlineKeyboardButton("Отменить", callback_data=f"reject:{request['request_id']}"),
            ]
        ]
    )

    for admin_id in admin_ids:
        try:
            message = await context.bot.send_message(
                admin_id,
                "\n".join(lines),
                reply_markup=keyboard,
            )
            request["admin_messages"].append(
                {"chat_id": message.chat_id, "message_id": message.message_id}
            )
        except Exception:
            LOGGER.exception("Failed to send approval request %s to admin %s", request["request_id"], admin_id)


async def _handle_admin_decision(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    admin_user = update.effective_user
    if query is None or admin_user is None:
        return

    await query.answer()
    data = query.data or ""
    if ":" not in data:
        return
    action, request_id = data.split(":", 1)

    request = _get_pending_request(context, request_id)
    if request is None:
        await query.answer("Запрос уже обработан.", show_alert=True)
        try:
            await query.edit_message_text("Запрос уже обработан.")
        except Exception:
            pass
        return

    if not _is_admin_user(admin_user):
        await query.answer("Недостаточно прав.", show_alert=True)
        return

    client = request["client"]
    client_label = _format_client_label(client)
    user_chat_id = request["user_chat_id"]
    user_id = request["user_id"]

    if action == "approve":
        try:
            link_user_to_client(
                tg_user_id=user_id,
                client_id=client["id"],
                tg_username=request.get("user_username"),
                tg_full_name=request.get("user_full_name"),
            )
        except Exception:
            LOGGER.exception(
                "Failed to apply approved link for request %s (user %s, client %s)",
                request_id,
                user_id,
                client["id"],
            )
            await query.answer("Не удалось привязать клиента. Попробуйте позже.", show_alert=True)
            return

        admin_text = (
            f"✅ Запрос {request_id} подтверждён администратором {_format_user_label(admin_user)}.\n"
            f"Клиент: {client_label}"
        )
        user_text = (
            f"Администратор подтвердил привязку к клиенту {client_label}. "
            "Теперь вы можете пользоваться сервисом."
        )
    else:
        admin_text = (
            f"❌ Запрос {request_id} отклонён администратором {_format_user_label(admin_user)}.\n"
            f"Клиент: {client_label}"
        )
        user_text = (
            f"Администратор отклонил запрос на привязку к клиенту {client_label}. "
            "Связь осталась без изменений."
        )

    _pop_pending_request(context, request_id)

    try:
        await query.edit_message_text(admin_text)
    except Exception:
        LOGGER.debug("Failed to edit admin decision message for request %s", request_id, exc_info=True)

    for message_ref in request.get("admin_messages", []):
        if (
            message_ref.get("chat_id") == query.message.chat_id
            and message_ref.get("message_id") == query.message.message_id
        ):
            continue
        try:
            await context.bot.edit_message_text(
                admin_text,
                chat_id=message_ref["chat_id"],
                message_id=message_ref["message_id"],
            )
        except Exception:
            LOGGER.debug("Failed to update admin message for request %s", request_id, exc_info=True)

    try:
        await context.bot.send_message(user_chat_id, user_text)
    except Exception:
        LOGGER.exception(
            "Failed to notify user %s about decision %s for request %s",
            user_id,
            action,
            request_id,
        )


def create_application(token: str, greeting: str = DEFAULT_GREETING) -> Application:
    """Create a Telegram application with surname-based client linking."""
    if not token:
        raise ValueError("Telegram bot token must be provided")

    application = Application.builder().token(token).build()
    application.bot_data[_GREETING_KEY] = greeting or DEFAULT_GREETING

    conversation = ConversationHandler(
        entry_points=[CommandHandler("start", _start_handler)],
        states={
            ASK_LAST_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_last_name),
            ],
            CONFIRM_LINK: [
                CallbackQueryHandler(_handle_link_selection, pattern=r"^link:\d+$"),
                CallbackQueryHandler(_handle_new_client_request, pattern=r"^new_client$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_last_name),
            ],
            FORM_FIRST_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_first_name),
            ],
            FORM_LAST_NAME: [
                CallbackQueryHandler(_keep_last_name, pattern=r"^form:keep:last_name$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_last_name),
            ],
            FORM_WEIGHT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_weight),
            ],
            FORM_HEIGHT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_height),
            ],
            FORM_GENDER: [
                CallbackQueryHandler(_handle_gender_selection, pattern=r"^form:set:gender:(male|female)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _prompt_gender_buttons),
            ],
            FORM_FTP: [
                CallbackQueryHandler(_skip_ftp, pattern=r"^form:skip:ftp$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_ftp),
            ],
            FORM_PEDALS: [
                CallbackQueryHandler(_handle_pedals_selection, pattern=r"^form:set:pedals:[^:]+$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_pedals),
            ],
            FORM_GOAL: [
                CallbackQueryHandler(_skip_goal, pattern=r"^form:skip:goal$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_form_goal),
            ],
        },
        fallbacks=[CommandHandler("cancel", _cancel_handler)],
        name="client_authorization",
        persistent=False,
    )

    application.add_handler(conversation)
    booking_conversation = ConversationHandler(
        entry_points=[CommandHandler("book", _book_command_handler)],
        states={
            BOOK_SELECT_DAY: [
                CallbackQueryHandler(_handle_booking_day, pattern=r"^book:day:\d{4}-\d{2}-\d{2}$"),
                CallbackQueryHandler(_handle_booking_cancel_callback, pattern=r"^book:cancel$"),
            ],
            BOOK_SELECT_SLOT: [
                CallbackQueryHandler(_handle_booking_slot, pattern=r"^book:slot:\d+$"),
                CallbackQueryHandler(_handle_booking_day, pattern=r"^book:day:\d{4}-\d{2}-\d{2}$"),
                CallbackQueryHandler(_handle_booking_back, pattern=r"^book:back$"),
                CallbackQueryHandler(_handle_booking_cancel_callback, pattern=r"^book:cancel$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", _booking_cancel_command)],
        name="schedule_booking",
        persistent=False,
    )
    application.add_handler(booking_conversation)
    application.add_handler(CommandHandler("help", _help_handler))
    application.add_handler(CallbackQueryHandler(_handle_admin_decision, pattern=r"^(approve|reject):"))
    application.add_handler(MessageHandler(filters.COMMAND, _unknown_command_handler))

    return application


__all__ = ["create_application", "DEFAULT_GREETING"]
