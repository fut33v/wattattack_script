"""Entry points for the Krutilka VNB Telegram bot."""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
import requests
from typing import Any, Awaitable, Callable, Dict, Final, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from telegram import Bot, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Update, User
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

from repositories import bikes_repository, message_repository, race_repository, schedule_repository, trainers_repository
from repositories.client_repository import create_client, get_client, search_clients, update_client_fields
from repositories.client_link_repository import (
    get_link_by_client,
    get_link_by_user,
    link_user_to_client,
    update_strava_tokens,
)
from repositories.admin_repository import get_admin_ids, is_admin
from straver_client import StraverClient

import os

LOGGER = logging.getLogger(__name__)

_GREETING_KEY: Final[str] = "krutilkavnbot:greeting"
_CANDIDATES_KEY: Final[str] = "krutilkavnbot:candidates"
_FORM_KEY: Final[str] = "krutilkavnbot:form"
_PROFILE_EDIT_FIELD_KEY: Final[str] = "krutilkavnbot:profile_edit_field"
_RELINK_MODE_KEY: Final[str] = "krutilkavnbot:relink_mode"
_FORM_STEP_KEY: Final[str] = "krutilkavnbot:form_step"
_PENDING_APPROVALS_KEY: Final[str] = "krutilkavnbot:pending_approvals"
_LAST_SEARCH_KEY: Final[str] = "krutilkavnbot:last_name"
_BOOKING_STATE_KEY: Final[str] = "krutilkavnbot:booking"
_MY_BOOKINGS_CACHE_KEY: Final[str] = "krutilkavnbot:my_bookings"
_RACE_CONTEXT_KEY: Final[str] = "krutilkavnbot:race_flow"
_STATUS_LABELS: Final[Dict[str, str]] = {
    "booked": "Записан",
    "available": "Свободно",
    "cancelled": "Отменено",
    "pending": "Ожидание",
    "waitlist": "Лист ожидания",
    "blocked": "Заблокировано",
    "legacy": "История",
    "hold": "Держим",
}

DEFAULT_GREETING: Final[str] = "Здравствуйте!"
MAX_SUGGESTIONS: Final[int] = 6
ADMIN_BOT_TOKEN_ENV: Final[str] = "TELEGRAM_BOT_TOKEN"

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
(
    RACE_SELECT_MODE,
    RACE_COLLECT_BIKE,
    RACE_COLLECT_AXLE,
    RACE_COLLECT_GEARS,
    RACE_WAITING_PROOF,
) = range(200, 205)

_RACE_AXLE_CHOICES: Final[Dict[str, str]] = {
    "thru": "На ось (thru axle)",
    "qr": "На эксцентрик",
    "unknown": "Не знаю",
}
_RACE_GEARS_CHOICES: Final[List[str]] = ["7", "8", "9", "10", "11", "12", "Не знаю"]
_RACE_MODE_CHOICES: Final[Dict[str, str]] = {
    "offline": "🏟 Оффлайн (в Крутилке)",
    "online": "💻 Онлайн (у себя дома)",
}

_ADMIN_NOTIFICATION_BOT: Optional[Bot] = None
_ADMIN_NOTIFICATION_WARNED: bool = False


def _gear_label_from_code(code: str) -> Optional[str]:
    normalized_code = (code or "").strip().lower()
    for option in _RACE_GEARS_CHOICES:
        option_code = option.lower().replace(" ", "_")
        if option_code == normalized_code:
            return option
    return None


def _get_admin_notification_bot() -> Optional[Bot]:
    global _ADMIN_NOTIFICATION_BOT, _ADMIN_NOTIFICATION_WARNED
    token = os.environ.get(ADMIN_BOT_TOKEN_ENV)
    if not token:
        if not _ADMIN_NOTIFICATION_WARNED:
            LOGGER.warning(
                "TELEGRAM_BOT_TOKEN is not configured in krutilkavnbot; admin alerts will be sent from the client bot"
            )
            _ADMIN_NOTIFICATION_WARNED = True
        return None
    if _ADMIN_NOTIFICATION_BOT is None:
        _ADMIN_NOTIFICATION_BOT = Bot(token=token)
    return _ADMIN_NOTIFICATION_BOT


async def _send_admin_notification(
    chat_id: int,
    text: str,
    *,
    context: ContextTypes.DEFAULT_TYPE | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    admin_bot = _get_admin_notification_bot()
    if admin_bot is not None:
        try:
            await admin_bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            return True
        except Exception:
            LOGGER.exception("Failed to send admin notification via adminbot", exc_info=True)
    if context is not None:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            return True
        except Exception:
            LOGGER.exception("Failed to send admin notification via krutilkavnbot fallback", exc_info=True)
    return False

_FORM_STEP_HINTS: Final[Dict[int, str]] = {
    FORM_FIRST_NAME: "Сейчас ждём ваше имя (только текст).",
    FORM_LAST_NAME: "Сейчас ждём вашу фамилию.",
    FORM_WEIGHT: "Сейчас ждём ваш вес в килограммах (например, 72.5).",
    FORM_HEIGHT: "Сейчас ждём ваш рост в сантиметрах (например, 178).",
    FORM_GENDER: "Нужно выбрать пол кнопками «М» или «Ж».",
    FORM_FTP: "Введите FTP в ваттах или нажмите «ОК», чтобы оставить 150.",
    FORM_PEDALS: "Выберите тип педалей на клавиатуре ниже.",
    FORM_GOAL: "Опишите цель или нажмите «ОК», чтобы пропустить.",
}

_PROFILE_EDIT_FIELDS: Final[Dict[str, Dict[str, str]]] = {
    "first_name": {
        "label": "Имя",
        "prompt": "🖊️ Введите новое имя (или отправьте /start, чтобы отменить).",
        "type": "text",
    },
    "last_name": {
        "label": "Фамилия",
        "prompt": "🖊️ Введите новую фамилию (или /start для отмены).",
        "type": "text",
    },
    "weight": {
        "label": "Вес",
        "prompt": "⚖️ Введите новый вес в килограммах (например, 72.5).",
        "type": "positive_float",
    },
    "height": {
        "label": "Рост",
        "prompt": "📏 Введите новый рост в сантиметрах (например, 178).",
        "type": "positive_float",
    },
    "ftp": {
        "label": "FTP",
        "prompt": "⚡ Введите новое значение FTP в ваттах (например, 220).",
        "type": "positive_float",
    },
    "gender": {
        "label": "Пол",
        "prompt": "Выберите новый пол с помощью кнопок ниже.",
        "type": "gender",
    },
    "pedals": {
        "label": "Педали",
        "prompt": "Выберите тип педалей с помощью кнопок ниже.",
        "type": "pedals",
    },
}

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


def _format_price_rub(value: Any) -> str:
    try:
        amount = int(float(value))
    except (TypeError, ValueError):
        return str(value)
    return f"{amount:,}".replace(",", " ")


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
        code_raw = stand.get("code")
        if code_raw is not None:
            code = str(code_raw).strip()
            if code:
                return code
        title_raw = stand.get("title")
        if title_raw is not None:
            title = str(title_raw).strip()
            if title:
                return title
        display_raw = stand.get("display_name")
        if display_raw is not None:
            display_name = str(display_raw).strip()
            if display_name:
                return display_name
        stand_id = stand.get("id")
        if stand_id is not None:
            return f"Станок {stand_id}"
    stand_code = (reservation or {}).get("stand_code")
    if stand_code is not None:
        code = str(stand_code).strip()
        if code:
            return code
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
    context.user_data.pop(_MY_BOOKINGS_CACHE_KEY, None)


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
        _clear_booking_state(context)
        return ConversationHandler.END

    # Booking was successful, send confirmation message
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
            instructor_note = f"\n🧑‍🏫 Инструктор: {instructor_name}"
        else:
            instructor_note = "\n🧑‍🏫 Инструктор: уточняется"

    when_label = ""
    slot_date_val = _parse_date(slot.get("slot_date"))
    start_time_val = _parse_time(slot.get("start_time"))
    if slot_date_val and start_time_val:
        when_label = f"{slot_date_val.strftime('%d.%m.%Y')} в {start_time_val.strftime('%H:%M')}"

    summary_lines = [
        "✅ Запись подтверждена!",
        f"{client_display_name}, вы записаны на {when_label}." if when_label else "Запись создана.",
        f"🏋️ Станок: {stand_label}",
    ]
    if slot_label:
        summary_lines.append(f"🕒 Слот: {slot_label}")
    if bike_label:
        summary_lines.append(f"🚲 Велосипед: {bike_label}")
    if instructor_note:
        summary_lines.append(instructor_note.strip())
    summary_lines.append("🧭 Как добраться: https://t.me/krutilkavn/9")
    summary_lines.append("🎒 Что взять с собой: https://t.me/krutilkavn/10")
    summary_lines.append("До встречи в «Крутилке»!")

    try:
        await query.edit_message_text("\n".join(summary_lines))
    except Exception:
        LOGGER.debug("Failed to edit confirmation message", exc_info=True)

    try:
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else user.id,
            text=(
                "💳 Стоимость занятия — 700 ₽.\n\n"
                "Оплата переводом по СБП на телефон\n"
                "+7 911 602 5498 (ТБАНК). Евгений Б.\n\n"
                "Пожалуйста, оплатите заранее и покажите подтверждение перед тренировкой.\n\n"
                "Если планы изменились — отправьте /cancel, чтобы освободить слот."
            ),
        )
    except Exception:
        LOGGER.debug("Failed to send payment reminder", exc_info=True)
        if query.message is not None:
            try:
                await query.message.reply_text("\n".join(summary_lines))
            except Exception:
                LOGGER.exception("Failed to send booking confirmation follow-up")
    
    # Notify admins about the new booking
    try:
        await _notify_admins_of_booking(context, client, slot, stand, bike)
    except Exception:
        LOGGER.exception("Failed to notify admins of new booking")

    _clear_booking_state(context)
    return ConversationHandler.END


async def _notify_admins_of_booking(
    context: ContextTypes.DEFAULT_TYPE,
    client: Dict[str, Any],
    slot: Dict[str, Any],
    stand: Optional[Dict[str, Any]],
    bike: Optional[Dict[str, Any]]
) -> None:
    """Send notification to all admins about a new booking."""
    try:
        admin_ids = get_admin_ids()
    except Exception:
        LOGGER.exception("Failed to load admin IDs for booking notification")
        return

    if not admin_ids:
        LOGGER.debug("No admin IDs found for booking notification")
        return

    # Format the booking details
    client_name = _format_client_display_name(client)
    
    slot_date = _parse_date(slot.get("slot_date"))
    start_time = _parse_time(slot.get("start_time"))
    
    date_str = slot_date.strftime('%d.%m.%Y') if slot_date else "неизвестная дата"
    time_str = start_time.strftime('%H:%M') if start_time else "неизвестное время"
    
    stand_label = _format_stand_label(stand, None)
    
    bike_info = ""
    if bike:
        bike_title = (bike.get("title") or "").strip()
        bike_owner = (bike.get("owner") or "").strip()
        if bike_title or bike_owner:
            bike_info = f"\n🚲 Велосипед: {bike_title} ({bike_owner})" if bike_title and bike_owner else f"\n🚲 Велосипед: {bike_title or bike_owner}"

    # Determine if it's with an instructor or self-service
    session_type = ""
    if slot.get("session_kind") == "instructor":
        instructor_name = (slot.get("instructor_name") or "").strip()
        if instructor_name:
            session_type = f"\n🧑‍🏫 С инструктором: {instructor_name}"
        else:
            session_type = "\n🧑‍🏫 С инструктором (имя уточняется)"
    else:
        session_type = "\n🔄 Самокрутка"
    
    # Create the notification message
    message = (
        f"🔔 Новая запись!\n\n"
        f"Клиент: {client_name}\n"
        f"Дата и время: {date_str} в {time_str}"
        f"{session_type}\n"
        f"🏋️ Станок: {stand_label}"
        f"{bike_info}\n\n"
        f"Запись создана через бота krutilkavnbot"
    )

    # Send notification to all admins
    for admin_id in admin_ids:
        sent = await _send_admin_notification(admin_id, message, context=context)
        if not sent:
            LOGGER.warning("Failed to send booking notification to admin %s", admin_id)


async def _send_confirmation_message(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    reservation: Dict[str, Any],
    slot: Dict[str, Any],
    stand: Optional[Dict[str, Any]],
    bike: Optional[Dict[str, Any]],
) -> int:
    client = reservation.get("client")
    client_display_name = _format_client_display_name(client)

    if not client:
        await query.edit_message_text("Клиент не найден. Попробуйте снова.")
        _clear_booking_state(context)
        return ConversationHandler.END

    if not slot:
        await query.edit_message_text("Слот не найден. Попробуйте снова.")
        _clear_booking_state(context)
        return ConversationHandler.END

    if not stand:
        await query.edit_message_text("Станок не найден. Попробуйте снова.")
        _clear_booking_state(context)
        return ConversationHandler.END

    if slot.get("slot_type") == "select":
        return BOOK_SELECT_SLOT

    success = await _edit_day_selection_message(query, context)
    if not success:
        await query.edit_message_text("Свободных мест больше нет. Попробуйте позже.")
        _clear_booking_state(context)
        return ConversationHandler.END
    return BOOK_SELECT_DAY


def _format_slot_summary(slot: Dict[str, Any]) -> str:
    slot_date = _parse_date(slot.get("slot_date"))
    date_label = _format_day_label(slot_date) if slot_date else str(slot.get("slot_date"))
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


def _format_cancellation_summary(slot: Dict[str, Any]) -> str:
    slot_date = _parse_date(slot.get("slot_date"))
    date_label = _format_day_label(slot_date) if slot_date else str(slot.get("slot_date"))
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


def _format_time_label(value: Any) -> Optional[str]:
    parsed = _parse_time(value)
    if parsed is None:
        return str(value) if value is not None else None
    return parsed.strftime("%H:%M")


def _format_reservation_summary(slot: Dict[str, Any]) -> str:
    slot_date = _parse_date(slot.get("slot_date"))
    date_label = _format_day_label(slot_date) if slot_date else str(slot.get("slot_date"))
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


async def _cancel_booking_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /cancel command to show bookings that can be cancelled."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start, затем повторите попытку.")
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await message.reply_text("Не удалось определить вашу анкету. Обратитесь к администратору.")
        return

    now_local = _local_now()
    try:
        reservations = schedule_repository.list_future_reservations_for_client(
            client_id,
            _to_local_naive(now_local),
        )
    except Exception:
        LOGGER.exception("Failed to fetch future reservations for client %s", client_id)
        await message.reply_text("Не удалось получить список записей. Попробуйте позже.")
        return

    if not reservations:
        await message.reply_text("⏳ У вас нет будущих записей для отмены.")
        return

    # Create inline keyboard with cancel buttons for each reservation
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for entry in reservations[:10]:  # Limit to first 10 reservations
        reservation_id = entry.get("id")
        if not isinstance(reservation_id, int):
            continue
            
        slot_label = _format_time_range(entry.get("start_time"), entry.get("end_time"))
        slot_date_value = _parse_date(entry.get("slot_date"))
        if slot_date_value:
            slot_label = f"{slot_date_value.strftime('%d.%m (%a)')} · {slot_label}"
        
        # Add session type info
        session_kind = entry.get("session_kind")
        instructor_name = (entry.get("instructor_name") or "").strip()
        if session_kind == "instructor":
            if instructor_name:
                slot_label += f" · Инструктор: {instructor_name}"
            else:
                slot_label += " · Инструктор: уточняется"
        else:
            slot_label += " · Самокрутка"
            
        button = InlineKeyboardButton(
            text=slot_label,
            callback_data=f"cancel_booking:{reservation_id}"
        )
        keyboard_rows.append([button])

    keyboard_rows.append([InlineKeyboardButton("Отмена", callback_data="cancel_booking_cancel")])
    
    await message.reply_text(
        "Выберите запись для отмены:",
        reply_markup=InlineKeyboardMarkup(keyboard_rows)
    )


async def _handle_cancel_booking_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle callback queries for booking cancellation."""
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    
    data = query.data or ""
    if data == "cancel_booking_cancel":
        await query.edit_message_text("Отмена записи отменена.")
        return
    
    if not data.startswith("cancel_booking:"):
        return
        
    try:
        reservation_id = int(data.split(":", 1)[1])
    except (ValueError, IndexError):
        await query.edit_message_text("Некорректный идентификатор записи.")
        return

    # Get user and client info
    user = update.effective_user
    if user is None:
        await query.edit_message_text("Не удалось определить пользователя.")
        return
        
    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await query.edit_message_text("Сначала привяжите свою анкету через /start.")
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await query.edit_message_text("Не удалось определить вашу анкету.")
        return

    # Get reservation details before cancelling
    try:
        reservation = schedule_repository.get_reservation(reservation_id)
    except Exception:
        LOGGER.exception("Failed to fetch reservation %s", reservation_id)
        await query.edit_message_text("Не удалось получить информацию о записи.")
        return

    if not reservation:
        await query.edit_message_text("Запись не найдена.")
        return

    # Check if reservation belongs to this client
    if reservation.get("client_id") != client_id:
        await query.edit_message_text("Эта запись не принадлежит вам.")
        return

    # Get slot details for notification
    slot_id = reservation.get("slot_id")
    slot_details = None
    if isinstance(slot_id, int):
        try:
            slot_details = schedule_repository.get_slot_with_reservations(slot_id)
        except Exception:
            LOGGER.warning("Failed to load slot %s for reservation %s", slot_id, reservation_id)

    # Cancel the reservation by updating its status
    try:
        cancelled_reservation = schedule_repository.update_reservation(
            reservation_id,
            client_id=None,
            client_name=None,
            status="available",
            source="krutilkavnbot",
            notes=f"cancelled via /cancel command by client {client_id}"
        )
    except Exception as exc:
        LOGGER.exception("Failed to cancel reservation %s", reservation_id)
        await query.edit_message_text(f"❌ Не удалось отменить запись: {exc}")
        return

    if not cancelled_reservation:
        await query.edit_message_text("❌ Не удалось отменить запись.")
        return

    # Prepare notification details
    reservation_info = dict(reservation)
    if slot_details:
        reservation_info.setdefault("slot_date", slot_details.get("slot_date"))
        reservation_info.setdefault("start_time", slot_details.get("start_time"))
        reservation_info.setdefault("end_time", slot_details.get("end_time"))
        reservation_info.setdefault("label", slot_details.get("label"))
        reservation_info.setdefault("session_kind", slot_details.get("session_kind"))
        reservation_info.setdefault("instructor_name", slot_details.get("instructor_name"))

    try:
        await _notify_admins_of_cancellation(context, client, reservation_info)
    except Exception:
        LOGGER.exception("Failed to notify admins about cancellation")

    # Format the cancellation message for the user
    slot_summary = _format_cancellation_summary(reservation_info)

    confirmation_lines = [
        "✅ Запись отменена.",
        f"Освобождён слот: {slot_summary}",
        "Спасибо, что предупредили. Если захотите записаться снова — используйте /book."
    ]
    confirmation_text = "\n\n".join(confirmation_lines)
    if query.message:
        await query.edit_message_text(confirmation_text)
    else:
        await context.bot.send_message(chat_id=user.id, text=confirmation_text)


def _straver_status(tg_user_id: int) -> bool:
    """Check Straver for the user's Strava connection state."""
    try:
        client = StraverClient()
        if not client.is_configured():
            return False
        status = client.connection_status([tg_user_id])
        entry = status.get(tg_user_id)
        return bool(entry and entry.get("connected"))
    except Exception:
        LOGGER.exception("Failed to fetch Straver status for user %s", tg_user_id)
        return False


async def _handle_strava_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Strava OAuth callback."""
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return

    await query.answer()
    
    # Check if user is linked to a client
    link = get_link_by_user(user.id)
    if not link:
        await query.edit_message_text("Сначала привяжите свою анкету через /start.")
        return

    # Generate Strava authorization URL
    try:
        straver = StraverClient()
        if not straver.is_configured():
            raise RuntimeError("Straver is not configured")
        auth_url = straver.build_authorize_url(state=str(user.id))
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Авторизоваться в Strava", url=auth_url)],
            [InlineKeyboardButton("Отмена", callback_data="strava_cancel")]
        ])
        
        await query.edit_message_text(
            "Чтобы подключить Strava, нажмите кнопку ниже и авторизуйтесь в своей учетной записи Strava. "
            "После этого ваши тренировки будут автоматически загружаться в Strava.",
            reply_markup=keyboard
        )
    except Exception as e:
        LOGGER.error("Failed to generate Straver auth URL: %s", e)
        await query.edit_message_text("Произошла ошибка при создании ссылки для авторизации Strava. Попробуйте позже.")


async def _handle_strava_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Strava authorization cancellation."""
    query = update.callback_query
    if query is None:
        return

    await query.answer()
    await query.edit_message_text("Подключение Strava отменено.")


async def _handle_strava_webhook(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Strava webhook with authorization code."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    # Parse the message text to extract code and state
    text = message.text or ""
    if "code=" not in text:
        await message.reply_text("Неверный формат сообщения для подключения Strava.")
        return

    # Extract code and state from the message
    try:
        # Try to find code and state in the message text
        import re
        code_match = re.search(r'code=([^&\s]+)', text)
        state_match = re.search(r'state=(\d+)', text)
        
        if not code_match or not state_match:
            await message.reply_text("Не удалось извлечь данные авторизации Strava из сообщения.")
            return
            
        code = code_match.group(1)
        state_user_id = int(state_match.group(1))
        
        # Verify that the state matches the current user
        if state_user_id != user.id:
            await message.reply_text("Ошибка проверки подлинности. Попробуйте снова.")
            return
            
    except Exception as e:
        LOGGER.error("Failed to parse Strava callback data: %s", e)
        await message.reply_text("Произошла ошибка при обработке данных авторизации Strava.")
        return
    
    straver = StraverClient()
    if not straver.is_configured():
        await message.reply_text("Сервис Straver не настроен. Попробуйте позже или свяжитесь с администраторами.")
        return

    try:
        response = requests.get(
            f"{straver.base_url}/strava/callback",
            params={"code": code, "state": str(state_user_id)},
            timeout=20,
            allow_redirects=False,
        )
        if response.status_code >= 400:
            await message.reply_text("Произошла ошибка при подключении Strava. Попробуйте снова.")
            return
        await message.reply_text(
            "✅ Стартовала привязка Strava через Straver. Если соединение успешно, вы получите подтверждение."
        )
    except Exception as e:
        LOGGER.error("Failed to hand off Strava callback to Straver: %s", e)
        await message.reply_text("Произошла ошибка при подключении Strava. Попробуйте снова.")


async def _show_profile_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user profile menu with Strava integration option."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start.")
        return

    # Check if Strava is already connected
    strava_connected = _straver_status(user.id) or bool(link.get("strava_access_token"))

    keyboard_buttons = []
    
    if strava_connected:
        keyboard_buttons.append([InlineKeyboardButton("🔄 Обновить подключение Strava", callback_data="strava_connect")])
        keyboard_buttons.append([InlineKeyboardButton("❌ Отключить Strava", callback_data="strava_disconnect")])
    else:
        keyboard_buttons.append([InlineKeyboardButton("🔌 Подключить Strava", callback_data="strava_connect")])
    
    keyboard_buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="profile_back")])

    client_name = _format_client_display_name(client)
    
    strava_status = "✅ Подключена" if strava_connected else "❌ Не подключена"
    
    profile_text = (
        f"👤 Профиль: {client_name}\n\n"
        f"🏅 Strava: {strava_status}\n\n"
        "Настройте интеграцию с Strava для автоматической загрузки ваших тренировок."
    )

    await message.reply_text(profile_text, reply_markup=InlineKeyboardMarkup(keyboard_buttons))


async def _handle_strava_disconnect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Strava disconnection."""
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return

    await query.answer()
    
    try:
        straver = StraverClient()
        if straver.is_configured():
            straver.disconnect(user.id)
        # Remove Strava tokens from the client link
        updated_link = update_strava_tokens(
            tg_user_id=user.id,
            strava_access_token=None,
            strava_refresh_token=None,
            strava_token_expires_at=None,
            strava_athlete_id=None
        )
        
        if updated_link:
            await query.edit_message_text("✅ Strava успешно отключена.")
        else:
            await query.edit_message_text("Ошибка при отключении Strava.")
    except Exception as e:
        LOGGER.error("Failed to disconnect Strava: %s", e)
        await query.edit_message_text("Произошла ошибка при отключении Strava.")


async def _strava_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /strava command to manage Strava integration."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    # Check if user is linked to a client
    link = get_link_by_user(user.id)
    if not link:
        await message.reply_text("Сначала привяжите свою анкету через /start.")
        return

    # Check if Strava is already connected
    strava_connected = _straver_status(user.id) or bool(link.get("strava_access_token"))

    keyboard_buttons = []
    
    if strava_connected:
        keyboard_buttons.append([InlineKeyboardButton("🔄 Обновить подключение Strava", callback_data="strava_connect")])
        keyboard_buttons.append([InlineKeyboardButton("❌ Отключить Strava", callback_data="strava_disconnect")])
    else:
        keyboard_buttons.append([InlineKeyboardButton("🔌 Подключить Strava", callback_data="strava_connect")])
    
    keyboard_buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="profile_back")])

    strava_status = "✅ Подключена" if strava_connected else "❌ Не подключена"
    
    strava_text = (
        f"🏅 Strava: {strava_status}\n\n"
        "Настройте интеграцию с Strava для автоматической загрузки ваших тренировок.\n\n"
        "• При подключении вы будете перенаправлены на сайт Strava для авторизации\n"
        "• После подтверждения ваши тренировки будут автоматически загружаться в Strava"
    )

    await message.reply_text(strava_text, reply_markup=InlineKeyboardMarkup(keyboard_buttons))


# Removed upload command functionality - no longer needed


async def _notify_admins_of_cancellation(
    context: ContextTypes.DEFAULT_TYPE,
    client: Dict[str, Any],
    reservation: Dict[str, Any]
) -> None:
    """Send notification to all admins about a cancelled booking."""
    try:
        admin_ids = get_admin_ids()
    except Exception:
        LOGGER.exception("Failed to load admin IDs for cancellation notification")
        return

    if not admin_ids:
        LOGGER.debug("No admin IDs found for cancellation notification")
        return

    # Format the cancellation details
    client_name = _format_client_display_name(client)
    
    slot_date = _parse_date(reservation.get("slot_date"))
    start_time = _parse_time(reservation.get("start_time"))
    
    date_str = slot_date.strftime('%d.%m.%Y') if slot_date else "неизвестная дата"
    time_str = start_time.strftime('%H:%M') if start_time else "неизвестное время"
    
    # Determine if it's with an instructor or self-service
    session_type = ""
    if reservation.get("session_kind") == "instructor":
        instructor_name = (reservation.get("instructor_name") or "").strip()
        if instructor_name:
            session_type = f"\n🧑‍🏫 С инструктором: {instructor_name}"
        else:
            session_type = "\n🧑‍🏫 С инструктором (имя уточняется)"
    else:
        session_type = "\n🔄 Самокрутка"

    # Create the notification message
    message = (
        f"🔔 Отмена записи!\n\n"
        f"Клиент: {client_name}\n"
        f"Дата и время: {date_str} в {time_str}"
        f"{session_type}\n\n"
        f"Запись отменена через бота krutilkavnbot"
    )

    # Send notification to all admins
    for admin_id in admin_ids:
        sent = await _send_admin_notification(admin_id, message, context=context)
        if not sent:
            LOGGER.warning("Failed to send cancellation notification to admin %s", admin_id)


async def _notify_admins_of_new_message(
    context: ContextTypes.DEFAULT_TYPE,
    user: User,
    message_text: str
) -> None:
    """Send notification to all admins about a new user message."""
    try:
        admin_ids = get_admin_ids()
    except Exception:
        LOGGER.exception("Failed to load admin IDs for message notification")
        return

    if not admin_ids:
        LOGGER.debug("No admin IDs found for message notification")
        return

    # Format user information
    user_parts = []
    if user.first_name:
        user_parts.append(user.first_name)
    if user.last_name:
        user_parts.append(user.last_name)
    user_display_name = " ".join(user_parts) if user_parts else "Без имени"
    
    if user.username:
        user_display = f"{user_display_name} (@{user.username})"
    else:
        user_display = user_display_name

    # Create the notification message
    notification = (
        f"✉️ Новое сообщение от пользователя!\n\n"
        f"Пользователь: {user_display}\n"
        f"ID: {user.id}\n\n"
        f"Сообщение:\n{message_text}\n\n"
        f"Ответьте пользователю напрямую в личных сообщениях — бот пока не поддерживает переписку с клиентами."
    )

    # Send notification to all admins
    for admin_id in admin_ids:
        sent = await _send_admin_notification(admin_id, notification, context=context)
        if not sent:
            LOGGER.warning("Failed to send message notification to admin %s", admin_id)


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


def _peek_form(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    form = context.user_data.get(_FORM_KEY)
    return form if isinstance(form, dict) else {}


def _set_form_step(context: ContextTypes.DEFAULT_TYPE, step: int) -> None:
    context.user_data[_FORM_STEP_KEY] = step


def _current_form_step(context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    return context.user_data.get(_FORM_STEP_KEY)


def _clear_form_step(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_FORM_STEP_KEY, None)


def _set_race_context(context: ContextTypes.DEFAULT_TYPE, payload: Dict[str, Any]) -> None:
    context.user_data[_RACE_CONTEXT_KEY] = payload


def _get_race_context(context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
    race_payload = context.user_data.get(_RACE_CONTEXT_KEY)
    return race_payload if isinstance(race_payload, dict) else None


def _clear_race_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_RACE_CONTEXT_KEY, None)


def _current_race_registration_id(context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    race_payload = _get_race_context(context) or {}
    value = race_payload.get("registration_id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_race_date_text(value: Any) -> str:
    date_value = _parse_date(value)
    if date_value:
        return date_value.strftime("%d.%m.%Y")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "уточняется"


async def _prompt_race_mode_choice(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    buttons = [
        [
            InlineKeyboardButton(
                label,
                callback_data=f"race:mode:{code}",
            )
        ]
        for code, label in _RACE_MODE_CHOICES.items()
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    await context.bot.send_message(
        chat_id,
        "Как будете участвовать?\n🏟 В Крутилке вживую или 💻 онлайн у себя дома?",
        reply_markup=keyboard,
    )


async def _prompt_race_bike_choice(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Привезу свой велосипед", callback_data="race:bike:own"),
            ],
            [
                InlineKeyboardButton("🚲 Нужен студийный велосипед", callback_data="race:bike:rent"),
            ],
        ]
    )
    await context.bot.send_message(
        chat_id,
        "Расскажите, приедете ли вы со своим велосипедом?",
        reply_markup=keyboard,
    )


async def _prompt_race_axle_choice(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    buttons = [
        [InlineKeyboardButton(label, callback_data=f"race:axle:{code}")]
        for code, label in _RACE_AXLE_CHOICES.items()
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    await context.bot.send_message(
        chat_id,
        "Какой тип крепления у вашего велосипеда?",
        reply_markup=keyboard,
    )


async def _prompt_race_gears_choice(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for option in _RACE_GEARS_CHOICES:
        code = option.lower().replace(" ", "_")
        row.append(InlineKeyboardButton(option, callback_data=f"race:gears:{code}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    keyboard = InlineKeyboardMarkup(buttons)
    await context.bot.send_message(
        chat_id,
        "Сколько передач у велосипеда?",
        reply_markup=keyboard,
    )


async def _send_race_payment_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    registration: Dict[str, Any],
) -> None:
    race_ctx = _get_race_context(context) or {}
    race_title = race_ctx.get("race_title") or "гонка"
    race_date = _format_race_date_text(race_ctx.get("race_date"))
    price_text = _format_price_rub(race_ctx.get("price_rub"))
    sbp_phone = (race_ctx.get("sbp_phone") or "").strip()
    payment_text = (race_ctx.get("payment_text") or "").strip()
    race_mode = (registration.get("race_mode") or "").strip().lower()
    mode_label = _RACE_MODE_CHOICES.get(race_mode)

    lines = [
        "Отлично! Остался последний шаг.",
        f"🏁 Гонка: {race_title}",
        f"📅 Дата: {race_date}",
        f"💰 Стоимость участия: {price_text} ₽",
    ]
    if mode_label:
        lines.append(f"🛰 Формат: {mode_label}")
    if sbp_phone:
        lines.append(f"💳 Переведите оплату по СБП на номер: {sbp_phone}")
    if payment_text:
        lines.append("")
        lines.append(payment_text)
    lines.append("")
    lines.append("После перевода пришлите сюда скриншот подтверждения, и мы передадим его администраторам.")
    if registration.get("status") == race_repository.RACE_STATUS_PENDING and registration.get("payment_submitted_at"):
        lines.append(
            "Мы уже получили от вас скриншот и отправили его на проверку. "
            "Если хотите заменить его, просто отправьте новый."
        )

    await context.bot.send_message(chat_id, "\n".join(lines))


async def _advance_race_survey(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
) -> int:
    registration_id = _current_race_registration_id(context)
    if registration_id is None:
        await context.bot.send_message(
            chat_id,
            "Не удалось продолжить регистрацию. Попробуйте команду /race ещё раз.",
        )
        return ConversationHandler.END

    record = race_repository.get_registration_by_id(registration_id)
    if not record:
        await context.bot.send_message(
            chat_id,
            "Не удалось найти вашу заявку. Попробуйте начать регистрацию заново командой /race.",
        )
        return ConversationHandler.END

    race_mode = (record.get("race_mode") or "").strip().lower()
    if race_mode not in _RACE_MODE_CHOICES:
        await _prompt_race_mode_choice(context, chat_id)
        return RACE_SELECT_MODE

    if race_mode == "online":
        await _send_race_payment_prompt(context, chat_id, record)
        return RACE_WAITING_PROOF

    bring_own_bike = record.get("bring_own_bike")
    if bring_own_bike is None:
        await _prompt_race_bike_choice(context, chat_id)
        return RACE_COLLECT_BIKE
    if bring_own_bike and not (record.get("axle_type") or "").strip():
        await _prompt_race_axle_choice(context, chat_id)
        return RACE_COLLECT_AXLE
    if bring_own_bike and not (record.get("gears_label") or "").strip():
        await _prompt_race_gears_choice(context, chat_id)
        return RACE_COLLECT_GEARS

    await _send_race_payment_prompt(context, chat_id, record)
    return RACE_WAITING_PROOF


async def _race_bike_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("Пожалуйста, используйте кнопки ниже, чтобы выбрать вариант.")
    return RACE_COLLECT_BIKE


async def _race_axle_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("Выберите подходящий вариант типа оси на клавиатуре ниже.")
    return RACE_COLLECT_AXLE


async def _race_gears_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("Укажите количество передач, выбрав один из вариантов на клавиатуре.")
    return RACE_COLLECT_GEARS


async def _race_mode_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("Выберите формат участия кнопками ниже.")
    return RACE_SELECT_MODE


async def _remind_form_progress(update: Update, context: ContextTypes.DEFAULT_TYPE, step: int) -> int:
    message = update.effective_message
    if message is None:
        return step

    form = _peek_form(context)
    hint = _FORM_STEP_HINTS.get(step, "Продолжим заполнение анкеты.")
    details: List[str] = []
    if step == FORM_FIRST_NAME:
        last_name = (form.get("last_name") or "").strip()
        if last_name:
            details.append(f"Фамилия уже сохранена: {last_name}.")
    elif step == FORM_LAST_NAME:
        first_name = (form.get("first_name") or "").strip()
        if first_name:
            details.append(f"Имя уже указано: {first_name}.")

    lines = [
        "📝 Вы уже создаёте новую анкету.",
        hint,
    ]
    if details:
        lines.extend(details)
    lines.append("Чтобы начать заново, отправьте /cancel и затем /start.")

    await message.reply_text("\n".join(lines))

    if step == FORM_GENDER:
        await _send_gender_prompt(context, message.chat_id)
    elif step == FORM_FTP:
        await _send_ftp_prompt(context, message.chat_id)
    elif step == FORM_PEDALS:
        await _send_pedals_prompt(context, message.chat_id)
    elif step == FORM_GOAL:
        await _send_goal_prompt(context, message.chat_id)

    return step


def _describe_expected_input(
    context: ContextTypes.DEFAULT_TYPE,
    user: Optional[User] = None,
) -> Optional[str]:
    edit_state = _profile_edit_state(context)
    if edit_state:
        field = edit_state["field"]
        config = _PROFILE_EDIT_FIELDS.get(field, {})
        prompt = config.get("prompt")
        if prompt:
            return prompt
        return "Отправьте новое значение или используйте /start для отмены."

    step = _current_form_step(context)
    form = _peek_form(context)
    if step is not None:
        hint = _FORM_STEP_HINTS.get(step, "")
        details: List[str] = []
        if step == FORM_FIRST_NAME:
            last_name = (form.get("last_name") or "").strip()
            if last_name:
                details.append(f"Фамилия уже сохранена: {last_name}.")
        elif step == FORM_LAST_NAME:
            first_name = (form.get("first_name") or "").strip()
            if first_name:
                details.append(f"Имя уже указано: {first_name}.")
        elif step == FORM_WEIGHT:
            details.append("Введите число, например 72.5.")
        elif step == FORM_HEIGHT:
            details.append("Введите число в сантиметрах, например 178.")
        text_parts = ["📝 Вы заполняете анкету."]
        if hint:
            text_parts.append(hint)
        if details:
            text_parts.extend(details)
        text_parts.append("Чтобы начать заново, отправьте /start.")
        return "\n".join(text_parts)

    if user is not None:
        link, _ = _fetch_linked_client(user.id)
        if link:
            if context.user_data.get(_RELINK_MODE_KEY):
                return "Отправьте фамилию клиента, чтобы привязать другую анкету."
            return None

    last_search = (context.user_data.get(_LAST_SEARCH_KEY) or "").strip()
    if last_search:
        return "Сейчас ждём вашу фамилию заново, чтобы выбрать анкету или создать новую."
    return "Сейчас бот ждёт вашу фамилию текстом, чтобы найти анкету в базе."


def _start_new_client_form(context: ContextTypes.DEFAULT_TYPE, last_name: str) -> Dict[str, Any]:
    form = {
        "last_name": last_name.strip(),
        "ftp": 150.0,
    }
    context.user_data[_FORM_KEY] = form
    return form


def _clear_form(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_FORM_KEY, None)
    _clear_form_step(context)


def _profile_edit_state(context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, str]]:
    state = context.user_data.get(_PROFILE_EDIT_FIELD_KEY)
    if isinstance(state, dict) and "field" in state:
        field = state.get("field")
        if isinstance(field, str) and field in _PROFILE_EDIT_FIELDS:
            return {"field": field}
    return None


def _set_profile_edit_field(context: ContextTypes.DEFAULT_TYPE, field: str) -> None:
    if field in _PROFILE_EDIT_FIELDS:
        context.user_data[_PROFILE_EDIT_FIELD_KEY] = {"field": field}


def _clear_profile_edit_field(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_PROFILE_EDIT_FIELD_KEY, None)


def _reset_authorization_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_candidates(context)
    _clear_form(context)
    _clear_profile_edit_field(context)
    context.user_data.pop(_LAST_SEARCH_KEY, None)


def _format_profile_summary(client: Dict[str, Any]) -> str:
    display_name = _format_client_display_name(client)
    client_id = client.get("id")
    gender_label = _format_gender_label(client.get("gender"))
    weight_label = _format_optional_number(client.get("weight"))
    height_label = _format_optional_number(client.get("height"))
    ftp_label = _format_optional_number(client.get("ftp"))
    pedals_label = (client.get("pedals") or "—").strip() or "—"
    lines = [
        f"👤 Анкета: {display_name} (ID {client_id})",
        f"Пол: {gender_label}",
        f"Вес: {weight_label} кг",
        f"Рост: {height_label} см",
        f"FTP: {ftp_label} Вт",
        f"Педали: {pedals_label}",
        "",
        "Выберите, что изменить:",
    ]
    return "\n".join(lines)


def _build_profile_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Имя", callback_data="profile:edit:first_name"),
                InlineKeyboardButton("Фамилия", callback_data="profile:edit:last_name"),
            ],
            [
                InlineKeyboardButton("Вес", callback_data="profile:edit:weight"),
                InlineKeyboardButton("Рост", callback_data="profile:edit:height"),
            ],
            [
                InlineKeyboardButton("Пол", callback_data="profile:edit:gender"),
                InlineKeyboardButton("FTP", callback_data="profile:edit:ftp"),
            ],
            [
                InlineKeyboardButton("Педали", callback_data="profile:edit:pedals"),
            ],
            [
                InlineKeyboardButton("🔄 Привязать другую анкету", callback_data="profile:relink"),
            ],
        ]
    )


async def _send_profile_menu(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    client: Dict[str, Any],
) -> None:
    summary = _format_profile_summary(client)
    await context.bot.send_message(chat_id, summary, reply_markup=_build_profile_menu_keyboard())


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


def _compose_full_name(first_name: Optional[str], last_name: Optional[str]) -> Optional[str]:
    parts = []
    first = (first_name or "").strip()
    last = (last_name or "").strip()
    if first:
        parts.append(first)
    if last:
        parts.append(last)
    full_name = " ".join(parts).strip()
    return full_name or None


def _profile_field_updates(client: Dict[str, Any], field: str, value: object) -> Dict[str, object]:
    updates: Dict[str, object] = {field: value}
    if field == "first_name":
        updates["full_name"] = _compose_full_name(value, client.get("last_name"))
    elif field == "last_name":
        updates["full_name"] = _compose_full_name(client.get("first_name"), value)
    return updates


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


async def _send_profile_gender_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("М", callback_data="profile:set:gender:male"),
                InlineKeyboardButton("Ж", callback_data="profile:set:gender:female"),
            ]
        ]
    )
    await context.bot.send_message(chat_id, "👤 Выберите новый пол:", reply_markup=keyboard)


async def _send_profile_pedals_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"profile:set:pedals:{code}")]
        for label, code in _PEDAL_CHOICES
    ]
    await context.bot.send_message(chat_id, "🚴 Выберите тип педалей:", reply_markup=InlineKeyboardMarkup(rows))


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

    current_step = _current_form_step(context)
    if current_step is not None and current_step in _FORM_STEP_HINTS:
        return await _remind_form_progress(update, context, current_step)

    _reset_authorization_flow(context)
    greeting: str = context.application.bot_data.get(_GREETING_KEY, DEFAULT_GREETING)

    linked_client: Optional[Dict[str, Any]] = None
    try:
        existing = get_link_by_user(user.id)
        if existing:
            client = get_client(existing["client_id"])
            if client:
                linked_client = client
    except Exception:
        LOGGER.exception("Failed to check existing link for user %s", user.id)

    intro = (
        "👋 Это бот для записи в первую велостудию в Великом Новгороде «Крутилку».\n"
        "Если вы уже были у нас, мы найдём вашу анкету и привяжем её к Telegram.\n"
        "Если вы новый участник, создадим анкету прямо здесь."
    )

    if linked_client:
        display_label = _format_client_label(linked_client)
        text = (
            f"{greeting}\n\n{intro}\n\n"
            f"✅ Ваш Telegram уже привязан к {display_label}.\n"
            "Используйте меню ниже, чтобы обновить данные или привязать другую анкету."
        )
        await message.reply_text(text, parse_mode=ParseMode.HTML)
        await _send_profile_menu(context, message.chat_id, linked_client)
        context.user_data[_RELINK_MODE_KEY] = False
        return ASK_LAST_NAME

    text = (
        f"{greeting}\n\n{intro}\n\n"
        "Пожалуйста, введите свою <b>ФАМИЛИЮ</b>, чтобы продолжить. "
        "После подтверждения анкеты откроются записи и история посещений."
    )

    await message.reply_text(text, parse_mode=ParseMode.HTML)
    return ASK_LAST_NAME


async def _help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        "Отправьте /start, чтобы пройти авторизацию и привязать свою анкету. "
        "Если клиента нет в базе, можно заполнить короткую анкету для новой записи. "
        "После привязки используйте /book, чтобы выбрать удобный слот, "
        "/mybookings — посмотреть будущие записи, а /history — историю визитов.",
    )


async def _unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None:
        return
    expectation = _describe_expected_input(context, user)
    lines: List[str] = []
    if expectation:
        lines.append(expectation)
    lines.append(
        "Команды:\n/start — привязать или создать анкету.\n/book — запись на ближайшие слоты.\n/mybookings — будущие посещения.\n/history — история визитов."
    )
    await message.reply_text("\n\n".join(lines))


async def _fallback_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or not message.text:
        return

    edit_state = _profile_edit_state(context)
    if edit_state:
        handled = await _process_profile_edit_text(update, context, edit_state)
        if handled:
            return
    
    # Store the message in the database
    if user is not None:
        try:
            message_repository.store_user_message(
                tg_user_id=user.id,
                message_text=message.text,
                tg_username=user.username,
                tg_full_name=f"{user.first_name or ''} {user.last_name or ''}".strip()
            )
            
            # Notify admins about the new message
            await _notify_admins_of_new_message(context, user, message.text)
        except Exception:
            LOGGER.exception("Failed to store user message")

    expectation = _describe_expected_input(context, user)
    lines: List[str] = []
    if expectation:
        lines.append(expectation)

    if user is not None:
        link, _ = _fetch_linked_client(user.id)
        if link:
            lines.append(
                "ℹ️ Переписка с оператором «Крутилки» пока доступна только через сообщения канала @krutilkavn."
            )

    lines.append(
        "Команды:\n"
        "/book — забронировать слот.\n"
        "/cancel — отменить ближайшую бронь.\n"
        "/race — регистрация на гонку.\n"
        "/mybookings — показать будущие записи.\n"
        "/history — показать историю.\n"
        "/start — открыть меню анкеты."
    )
    await message.reply_text("\n\n".join(lines))


async def _handle_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or not message.text:
        return ASK_LAST_NAME

    if message.text.startswith("/"):
        command = message.text.strip().split()[0].lower()
        if command == "/start":
            return await _start_handler(update, context)
        await message.reply_text(
            "Используйте /start для привязки анкеты, /book для новых записей, "
            "/mybookings для ближайших посещений и /history для истории бронирований."
        )
        return ASK_LAST_NAME

    relink_mode = bool(context.user_data.get(_RELINK_MODE_KEY))
    has_link = False
    if user is not None:
        link_record, _ = _fetch_linked_client(user.id)
        has_link = bool(link_record)

    if has_link and not relink_mode:
        await message.reply_text(
            "Вы уже привязаны к анкете. Используйте /start и кнопку «Привязать другую анкету», "
            "если хотите переключиться на другого клиента."
        )
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
        if has_link:
            await message.reply_text(
                "Не нашли такую фамилию. Уточните запрос или свяжитесь с администратором, "
                "если нужная анкета отсутствует."
            )
            _clear_candidates(context)
            return ASK_LAST_NAME

        _start_new_client_form(context, last_name)
        await message.reply_text(
            "🔎 Клиентов с такой фамилией не нашлось. Давайте создадим новую запись.\n"
            "🖊️ Введите своё имя:"
        )
        _set_form_step(context, FORM_FIRST_NAME)
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
    if not has_link:
        rows.append([InlineKeyboardButton("Создать новую запись", callback_data="new_client")])
    keyboard = InlineKeyboardMarkup(rows)
    lines = ["📋 Нашлись такие клиенты:"]
    for client in clients:
        lines.append(f"• {_format_client_label(client)}")
    if len(clients) >= MAX_SUGGESTIONS:
        lines.append("Если не нашли нужного, уточните фамилию и отправьте снова.")
    if not has_link:
        lines.append("ℹ️ Или нажмите «Создать новую запись», чтобы заполнить анкету.")
    else:
        lines.append("ℹ️ Данные новой анкеты нельзя создать, пока текущая связь активна.")

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
        context.user_data.pop(_RELINK_MODE_KEY, None)

    return ASK_LAST_NAME


async def _handle_new_client_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user = update.effective_user
    if query is None or query.message is None:
        return FORM_FIRST_NAME

    if user is not None:
        link, _ = _fetch_linked_client(user.id)
        if link:
            await query.answer("Вы уже привязаны к анкете. Создание новой записи недоступно.", show_alert=True)
            return ASK_LAST_NAME

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
    _set_form_step(context, FORM_FIRST_NAME)
    return FORM_FIRST_NAME


async def _handle_profile_edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or query.message is None or user is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Неизвестное действие.", show_alert=True)
        return
    field = parts[2]
    config = _PROFILE_EDIT_FIELDS.get(field)
    if config is None:
        await query.answer("Недоступно.", show_alert=True)
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await query.answer("Анкета не найдена. Используйте /start.", show_alert=True)
        return

    await query.answer()
    field_type = config.get("type")
    chat_id = query.message.chat_id
    if field_type == "gender":
        await _send_profile_gender_prompt(context, chat_id)
        return
    if field_type == "pedals":
        await _send_profile_pedals_prompt(context, chat_id)
        return

    _set_profile_edit_field(context, field)
    current_value = client.get(field)
    if isinstance(current_value, (int, float)):
        current_display = f"{current_value:g}"
    else:
        current_display = (current_value or "—").strip() or "—"
    lines = [
        config.get("prompt", "Введите новое значение."),
        f"Текущее значение: {current_display}",
    ]
    await context.bot.send_message(chat_id, "\n".join(lines))


async def _handle_profile_gender_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or query.message is None or user is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return
    gender_code = parts[3]
    if gender_code not in {"male", "female"}:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await query.answer("Анкета не найдена. Используйте /start.", show_alert=True)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await query.answer("Некорректная анкета.", show_alert=True)
        return

    try:
        update_client_fields(client_id, gender=gender_code)
    except Exception:
        LOGGER.exception("Failed to update gender for client %s", client_id)
        await query.answer("Не удалось обновить пол.", show_alert=True)
        return

    try:
        refreshed = get_client(client_id) or client
    except Exception:
        LOGGER.exception("Failed to refresh client %s after gender update", client_id)
        refreshed = client

    label = "М" if gender_code == "male" else "Ж"
    await query.message.reply_text(f"✅ Пол обновлён: {label}")
    await _send_profile_menu(context, query.message.chat_id, refreshed)


async def _handle_profile_pedals_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or query.message is None or user is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return
    code = parts[3]
    label = _PEDAL_LABEL_BY_CODE.get(code)
    if label is None:
        await query.answer("Неизвестный вариант.", show_alert=True)
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await query.answer("Анкета не найдена. Используйте /start.", show_alert=True)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await query.answer("Некорректная анкета.", show_alert=True)
        return

    try:
        update_client_fields(client_id, pedals=label)
    except Exception:
        LOGGER.exception("Failed to update pedals for client %s", client_id)
        await query.answer("Не удалось обновить педали.", show_alert=True)
        return

    try:
        refreshed = get_client(client_id) or client
    except Exception:
        LOGGER.exception("Failed to refresh client %s after pedals update", client_id)
        refreshed = client

    await query.message.reply_text(f"✅ Педали обновлены: {label}")
    await _send_profile_menu(context, query.message.chat_id, refreshed)


async def _handle_profile_relink_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or query.message is None:
        return

    await query.answer()
    _reset_authorization_flow(context)
    context.user_data[_RELINK_MODE_KEY] = True
    await query.message.reply_text(
        "Отправьте фамилию клиента, которого нужно привязать. "
        "Создание новой анкеты отключено, пока текущая связь активна."
    )


async def _process_profile_edit_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    edit_state: Dict[str, str],
) -> bool:
    message = update.effective_message
    user = update.effective_user
    if message is None or message.text is None or user is None:
        return False

    field = edit_state.get("field")
    if not field:
        _clear_profile_edit_field(context)
        return False

    config = _PROFILE_EDIT_FIELDS.get(field)
    if config is None:
        _clear_profile_edit_field(context)
        return False

    raw_value = message.text.strip()
    if not raw_value:
        await message.reply_text("⚠️ Значение не должно быть пустым.")
        return True

    field_type = config.get("type")
    if field_type == "text":
        new_value: object = raw_value
    elif field_type == "positive_float":
        ok, parsed = _parse_positive_float(raw_value)
        if not ok or parsed is None:
            await message.reply_text("⚠️ Введите положительное число (например, 72.5).")
            return True
        new_value = parsed
    else:
        return False

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Не удалось найти вашу анкету. Используйте /start.")
        _clear_profile_edit_field(context)
        return True

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await message.reply_text("Анкета недоступна. Попробуйте позже.")
        _clear_profile_edit_field(context)
        return True

    updates = _profile_field_updates(client, field, new_value)
    try:
        update_client_fields(client_id, **updates)
    except Exception:
        LOGGER.exception("Failed to update client %s field %s", client_id, field)
        await message.reply_text("❌ Не удалось сохранить значение. Попробуйте позже.")
        return True

    _clear_profile_edit_field(context)

    try:
        refreshed = get_client(client_id) or client
    except Exception:
        LOGGER.exception("Failed to refresh client %s after manual edit", client_id)
        refreshed = client

    await message.reply_text(f"✅ {config.get('label', 'Поле')} обновлено.")
    await _send_profile_menu(context, message.chat_id, refreshed)
    return True


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
    _set_form_step(context, FORM_LAST_NAME)
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
    _set_form_step(context, FORM_WEIGHT)
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
    _set_form_step(context, FORM_WEIGHT)
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
    _set_form_step(context, FORM_HEIGHT)
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
    _set_form_step(context, FORM_GENDER)
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
    _set_form_step(context, FORM_FTP)
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
    _set_form_step(context, FORM_PEDALS)
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
    _set_form_step(context, FORM_PEDALS)
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
    _set_form_step(context, FORM_GOAL)
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
        _reset_authorization_flow(context)
        return ConversationHandler.END

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

    _reset_authorization_flow(context)

    return ConversationHandler.END


async def _cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _reset_authorization_flow(context)
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
        fallbacks=[
            CommandHandler("cancel", _cancel_handler),
            CommandHandler("start", _start_handler),
        ],
        name="client_authorization",
        persistent=False,
        allow_reentry=True,
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
    race_conversation = ConversationHandler(
        entry_points=[CommandHandler("race", _race_command_handler)],
        states={
            RACE_SELECT_MODE: [
                CallbackQueryHandler(_handle_race_mode_choice, pattern=r"^race:mode:(offline|online)$"),
                MessageHandler(~filters.COMMAND, _race_mode_reminder),
            ],
            RACE_COLLECT_BIKE: [
                CallbackQueryHandler(_handle_race_bike_choice, pattern=r"^race:bike:(own|rent)$"),
                MessageHandler(~filters.COMMAND, _race_bike_reminder),
            ],
            RACE_COLLECT_AXLE: [
                CallbackQueryHandler(_handle_race_axle_choice, pattern=r"^race:axle:(thru|qr|unknown)$"),
                MessageHandler(~filters.COMMAND, _race_axle_reminder),
            ],
            RACE_COLLECT_GEARS: [
                CallbackQueryHandler(_handle_race_gears_choice, pattern=r"^race:gears:[a-z0-9_]+$"),
                MessageHandler(~filters.COMMAND, _race_gears_reminder),
            ],
            RACE_WAITING_PROOF: [
                MessageHandler((filters.PHOTO | filters.Document.IMAGE), _handle_race_payment_proof),
                MessageHandler(~filters.COMMAND, _race_prompt_payment),
            ],
        },
        fallbacks=[CommandHandler("cancel", _race_cancel_handler)],
        name="race_registration",
        persistent=False,
        allow_reentry=True,
    )
    application.add_handler(race_conversation)
    application.add_handler(CommandHandler("mybookings", _my_bookings_handler))
    application.add_handler(CommandHandler("history", _history_handler))
    application.add_handler(CommandHandler("cancel", _cancel_booking_handler))
    application.add_handler(CommandHandler("help", _help_handler))
    application.add_handler(CommandHandler("strava", _strava_command_handler))  # Add Strava command handler
    
    # Strava integration handlers
    # Strava integration handlers
    application.add_handler(CallbackQueryHandler(_handle_strava_callback, pattern=r"^strava_connect$"))
    application.add_handler(CallbackQueryHandler(_handle_strava_cancel, pattern=r"^strava_cancel$"))
    application.add_handler(CallbackQueryHandler(_handle_strava_disconnect, pattern=r"^strava_disconnect$"))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"code="), _handle_strava_webhook))
    
    # Placeholder for future document handlers
    
    application.add_handler(CallbackQueryHandler(_handle_profile_edit_callback, pattern=r"^profile:edit:[a-z_]+$"))
    application.add_handler(CallbackQueryHandler(_handle_profile_gender_selection, pattern=r"^profile:set:gender:(male|female)$"))
    application.add_handler(CallbackQueryHandler(_handle_profile_pedals_selection, pattern=r"^profile:set:pedals:[^:]+$"))
    application.add_handler(CallbackQueryHandler(_handle_profile_relink_callback, pattern=r"^profile:relink$"))
    application.add_handler(CallbackQueryHandler(_handle_race_payment_callback, pattern=r"^race_payment:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _fallback_text_handler))
    application.add_handler(CallbackQueryHandler(_handle_admin_decision, pattern=r"^(approve|reject):"))
    application.add_handler(CallbackQueryHandler(_handle_cancel_booking_callback, pattern=r"^cancel_booking:"))
    application.add_handler(MessageHandler(filters.COMMAND, _unknown_command_handler))

    return application


__all__ = ["create_application", "DEFAULT_GREETING"]


async def _race_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return ConversationHandler.END

    _clear_race_context(context)

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start, затем повторите попытку.")
        return ConversationHandler.END

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await message.reply_text("Не удалось определить вашу анкету. Обратитесь к администратору.")
        return ConversationHandler.END

    try:
        race = race_repository.get_active_race(only_future=True)
    except Exception:
        LOGGER.exception("Failed to load active race")
        race = None

    if not race:
        await message.reply_text("Сейчас нет активной регистрации на гонку. Следите за новостями!")
        return ConversationHandler.END

    race_id = race.get("id")
    if not isinstance(race_id, int):
        await message.reply_text("Не удалось определить текущую гонку. Попробуйте позже.")
        return ConversationHandler.END

    try:
        registration = race_repository.upsert_registration(
            race_id=race_id,
            client_id=client_id,
            tg_user_id=user.id,
            tg_username=user.username,
            tg_full_name=getattr(user, "full_name", None),
        )
    except Exception:
        LOGGER.exception("Failed to upsert race registration for user %s", user.id)
        await message.reply_text("Не удалось начать регистрацию на гонку. Попробуйте чуть позже.")
        return ConversationHandler.END

    registration_id = registration.get("id")
    if not isinstance(registration_id, int):
        await message.reply_text("Не удалось зафиксировать вашу заявку. Попробуйте чуть позже.")
        return ConversationHandler.END

    status = (registration.get("status") or "").lower()
    if status == race_repository.RACE_STATUS_APPROVED:
        cluster_label = (registration.get("cluster_label") or "").strip()
        cluster_text = f" Кластер: {cluster_label}." if cluster_label else ""
        race_date = _parse_date(race.get("race_date"))
        date_text = race_date.strftime("%d.%m.%Y") if race_date else race.get("race_date")
        await message.reply_text(
            f"🏁 Вы уже зарегистрированы на гонку {race.get('title')}.\n"
            f"📅 Дата: {date_text}\n"
            "✅ Оплата подтверждена."
            f"{cluster_text}"
        )
        _clear_race_context(context)
        return ConversationHandler.END

    client_label = _format_client_display_name(client)
    sbp_phone = (race.get("sbp_phone") or "").strip()
    payment_text = (race.get("payment_instructions") or "").strip()
    race_date = _parse_date(race.get("race_date"))
    date_text = race_date.strftime("%d.%m.%Y") if race_date else (race.get("race_date") or "уточняется")
    price_text = _format_price_rub(race.get("price_rub"))

    lines = [
        f"🏁 Регистрация на гонку «{race.get('title')}» открыта!",
        f"📅 Дата: {date_text}",
        f"💰 Стоимость участия: {price_text} ₽",
    ]
    if sbp_phone:
        lines.append(f"💳 Оплата по СБП на номер: {sbp_phone}")
    if race.get("clusters"):
        lines.append("📌 Кластер вам назначит администратор после проверки оплаты.")
    lines.append("")
    lines.append("Ответьте на несколько вопросов, чтобы мы подготовились к вашему старту.")

    await message.reply_text("\n".join(lines))

    _set_race_context(
        context,
        {
            "race_id": race_id,
            "race_title": race.get("title"),
            "race_date": race.get("race_date"),
            "price_rub": race.get("price_rub"),
            "sbp_phone": sbp_phone,
            "payment_text": payment_text,
            "client_id": client_id,
            "client_name": client_label,
            "registration_id": registration_id,
        },
    )
    return await _advance_race_survey(context, message.chat_id)


async def _handle_race_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return RACE_WAITING_PROOF

    race_context = _get_race_context(context)
    if not race_context:
        await message.reply_text("Сессия регистрации истекла. Отправьте команду /race заново.")
        return ConversationHandler.END

    file_id: Optional[str] = None
    file_unique_id: Optional[str] = None

    if message.photo:
        photo = message.photo[-1]
        file_id = photo.file_id
        file_unique_id = getattr(photo, "file_unique_id", None)
    elif message.document and (message.document.mime_type or "").startswith("image/"):
        document = message.document
        file_id = document.file_id
        file_unique_id = getattr(document, "file_unique_id", None)
    else:
        await message.reply_text("Пожалуйста, отправьте скриншот или фотографию платежа.")
        return RACE_WAITING_PROOF

    if not file_id:
        await message.reply_text("Не удалось обработать файл. Попробуйте ещё раз.")
        return RACE_WAITING_PROOF

    registration_id_value = race_context.get("registration_id")
    try:
        registration_id_int = int(registration_id_value)
    except (TypeError, ValueError):
        await message.reply_text("Сессия регистрации истекла. Отправьте команду /race заново.")
        _clear_race_context(context)
        return ConversationHandler.END

    try:
        record = race_repository.save_payment_proof(
            registration_id=registration_id_int,
            file_id=file_id,
            file_unique_id=file_unique_id,
            message_id=message.message_id,
        )
    except Exception:
        LOGGER.exception("Failed to save race payment proof for registration %s", race_context.get("registration_id"))
        await message.reply_text("Не удалось сохранить скриншот. Попробуйте отправить ещё раз.")
        return RACE_WAITING_PROOF

    if not record:
        await message.reply_text("Не удалось сохранить скриншот. Попробуйте отправить ещё раз.")
        return RACE_WAITING_PROOF

    caption = (
        f"🏁 Оплата гонки «{race_context.get('race_title')}»\n"
        f"Клиент: {race_context.get('client_name')} (ID {race_context.get('client_id')})\n"
        f"Telegram: {_format_user_label(user)}\n"
        f"Регистрация ID: {registration_id_int}"
    )

    admin_ids = get_admin_ids()
    approval_keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Подтвердить оплату",
                    callback_data=f"race_payment:approve:{registration_id_int}",
                )
            ]
        ]
    )
    for admin_id in admin_ids:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=file_id,
                caption=caption,
                reply_markup=approval_keyboard,
            )
        except Exception:
            LOGGER.exception("Failed to forward race payment proof to admin %s", admin_id)

    await message.reply_text(
        "Спасибо! Мы получили скриншот и передали его администраторам. "
        "После проверки оплаты вам сообщат и назначат кластер."
    )
    _clear_race_context(context)
    return ConversationHandler.END


async def _race_prompt_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message:
        await message.reply_text("Пришлите скриншот или фото перевода, чтобы завершить регистрацию.")
    return RACE_WAITING_PROOF


async def _race_cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _clear_race_context(context)
    message = update.effective_message
    if message:
        await message.reply_text("Регистрация гонки отменена. Вы можете начать заново командой /race.")
    return ConversationHandler.END


async def _handle_race_mode_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.data is None:
        return RACE_SELECT_MODE

    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 3 or parts[0] != "race" or parts[1] != "mode":
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_SELECT_MODE

    mode_code = parts[2]
    if mode_code not in _RACE_MODE_CHOICES:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_SELECT_MODE

    registration_id = _current_race_registration_id(context)
    if registration_id is None:
        await query.edit_message_text("Сессия регистрации истекла. Отправьте /race заново.")
        _clear_race_context(context)
        return ConversationHandler.END

    update_kwargs: Dict[str, Any] = {"race_mode": mode_code}
    if mode_code == "online":
        update_kwargs.update(
            bring_own_bike=None,
            axle_type=None,
            gears_label=None,
        )
    try:
        race_repository.update_registration(
            registration_id,
            **update_kwargs,
        )
    except Exception:
        LOGGER.exception("Failed to store race mode for registration %s", registration_id)
        await query.answer("Не удалось сохранить выбор. Попробуйте ещё раз.", show_alert=True)
        return RACE_SELECT_MODE

    try:
        await query.edit_message_text(f"Формат участия: {_RACE_MODE_CHOICES[mode_code]}")
    except Exception:
        LOGGER.debug("Failed to edit race mode message", exc_info=True)

    if query.message:
        return await _advance_race_survey(context, query.message.chat_id)
    return ConversationHandler.END


async def _handle_race_bike_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.data is None:
        return RACE_COLLECT_BIKE

    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_COLLECT_BIKE

    choice = parts[2]
    if choice not in {"own", "rent"}:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_COLLECT_BIKE

    registration_id = _current_race_registration_id(context)
    if registration_id is None:
        await query.edit_message_text("Сессия регистрации истекла. Отправьте /race заново.")
        _clear_race_context(context)
        return ConversationHandler.END

    bring_value = choice == "own"
    try:
        race_repository.update_registration(
            registration_id,
            bring_own_bike=bring_value,
            axle_type=None if bring_value else "Студийный велосипед",
            gears_label=None if bring_value else "Студийный велосипед",
        )
    except Exception:
        LOGGER.exception("Failed to store bike preference for race registration %s", registration_id)
        await query.answer("Не удалось сохранить выбор. Попробуйте ещё раз.", show_alert=True)
        return RACE_COLLECT_BIKE

    text = "Записали, что вы приедете со своим велосипедом." if bring_value else "Записали, что нужен студийный велосипед."
    try:
        await query.edit_message_text(text)
    except Exception:
        LOGGER.debug("Failed to edit bike choice message", exc_info=True)

    if query.message:
        return await _advance_race_survey(context, query.message.chat_id)
    return ConversationHandler.END


async def _handle_race_axle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.data is None:
        return RACE_COLLECT_AXLE

    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_COLLECT_AXLE

    choice = parts[2]
    label = _RACE_AXLE_CHOICES.get(choice)
    if label is None:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_COLLECT_AXLE

    registration_id = _current_race_registration_id(context)
    if registration_id is None:
        await query.edit_message_text("Сессия регистрации истекла. Отправьте /race заново.")
        _clear_race_context(context)
        return ConversationHandler.END

    try:
        race_repository.update_registration(
            registration_id,
            axle_type=label,
        )
    except Exception:
        LOGGER.exception("Failed to store axle type for race registration %s", registration_id)
        await query.answer("Не удалось сохранить выбор. Попробуйте ещё раз.", show_alert=True)
        return RACE_COLLECT_AXLE

    try:
        await query.edit_message_text(f"Тип крепления: {label}.")
    except Exception:
        LOGGER.debug("Failed to edit axle choice message", exc_info=True)

    if query.message:
        return await _advance_race_survey(context, query.message.chat_id)
    return ConversationHandler.END


async def _handle_race_gears_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None or query.data is None:
        return RACE_COLLECT_GEARS

    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_COLLECT_GEARS

    label = _gear_label_from_code(parts[2])
    if label is None:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return RACE_COLLECT_GEARS

    registration_id = _current_race_registration_id(context)
    if registration_id is None:
        await query.edit_message_text("Сессия регистрации истекла. Отправьте /race заново.")
        _clear_race_context(context)
        return ConversationHandler.END

    try:
        race_repository.update_registration(
            registration_id,
            gears_label=label,
        )
    except Exception:
        LOGGER.exception("Failed to store gears info for race registration %s", registration_id)
        await query.answer("Не удалось сохранить выбор. Попробуйте ещё раз.", show_alert=True)
        return RACE_COLLECT_GEARS

    try:
        await query.edit_message_text(f"Количество передач: {label}.")
    except Exception:
        LOGGER.debug("Failed to edit gears choice message", exc_info=True)

    if query.message:
        return await _advance_race_survey(context, query.message.chat_id)
    return ConversationHandler.END


async def _handle_race_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or not query.data:
        return

    await query.answer()
    user = query.from_user
    if not _is_admin_user(user):
        await query.answer("Только администраторы могут подтверждать оплату.", show_alert=True)
        return

    parts = query.data.split(":")
    if len(parts) != 3 or parts[0] != "race_payment" or parts[1] != "approve":
        await query.answer("Неизвестное действие.", show_alert=True)
        return

    try:
        registration_id = int(parts[2])
    except ValueError:
        await query.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    record = race_repository.get_registration_by_id(registration_id)
    if not record:
        await query.answer("Заявка не найдена.", show_alert=True)
        return

    current_status = (record.get("status") or "").lower()
    if current_status == race_repository.RACE_STATUS_APPROVED:
        await query.answer("Оплата уже подтверждена.", show_alert=True)
        return

    try:
        race_repository.update_registration(
            registration_id,
            status=race_repository.RACE_STATUS_APPROVED,
        )
    except Exception:
        LOGGER.exception("Failed to approve race registration %s", registration_id)
        await query.answer("Не удалось подтвердить оплату. Попробуйте позже.", show_alert=True)
        return

    race_title = None
    try:
        race = race_repository.get_race(record.get("race_id"))
        race_title = race.get("title") if race else None
    except Exception:
        race = None

    caption = query.message.caption or query.message.text or ""
    suffix = f"\n\n✅ Оплату подтвердил {_format_user_label(user)}"
    try:
        if query.message.photo:
            await query.edit_message_caption(caption + suffix, reply_markup=None)
        else:
            await query.edit_message_text(caption + suffix, reply_markup=None)
    except Exception:
        LOGGER.debug("Failed to edit race payment approval message for registration %s", registration_id, exc_info=True)

    tg_user_id = record.get("tg_user_id")
    client_name = record.get("client_name") or record.get("tg_full_name") or record.get("tg_username")
    user_text_parts = [
        "✅ Оплата за гонку подтверждена!",
    ]
    if race_title:
        user_text_parts.append(f"Гонка: {race_title}")
    if client_name:
        user_text_parts.append(f"Анкета: {client_name}")
    user_text_parts.append("Мы сообщим дополнительно, когда администратор назначит кластер.")
    if tg_user_id:
        try:
            await context.bot.send_message(tg_user_id, "\n".join(user_text_parts))
        except Exception:
            LOGGER.exception("Failed to notify user %s about approved race registration %s", tg_user_id, registration_id)

    await query.answer("Оплата подтверждена.", show_alert=False)


async def _my_bookings_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start, затем повторите попытку.")
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await message.reply_text("Не удалось определить вашу анкету. Обратитесь к администратору.")
        return

    now_local = _local_now()
    try:
        reservations = schedule_repository.list_future_reservations_for_client(
            client_id,
            _to_local_naive(now_local),
        )
    except Exception:
        LOGGER.exception("Failed to fetch future reservations for client %s", client_id)
        await message.reply_text("Не удалось получить список записей. Попробуйте позже.")
        return

    if not reservations:
        await message.reply_text("⏳ У вас нет будущих записей.")
        return

    lines: List[str] = ["🗓 Ваши ближайшие записи:"]
    for entry in reservations[:10]:
        slot_label = _format_time_range(entry.get("start_time"), entry.get("end_time"))
        slot_date_value = _parse_date(entry.get("slot_date"))
        if slot_date_value:
            slot_label = f"{slot_date_value.strftime('%d.%m (%a)')} · {slot_label}"

        parts = [f"🕒 {slot_label}"]

        session_kind = entry.get("session_kind")
        instructor_name = (entry.get("instructor_name") or "").strip()
        if session_kind == "instructor":
            if instructor_name:
                parts.append(f"🧑‍🏫 Инструктор: {instructor_name}")
            else:
                parts.append("🧑‍🏫 Инструктор: уточняется")

        label = (entry.get("label") or "").strip()
        if label:
            parts.append(label)

        stand_label = _format_stand_label(
            {
                "code": entry.get("stand_code"),
                "display_name": entry.get("stand_display_name"),
                "title": entry.get("stand_title"),
            },
            entry,
        )
        if stand_label:
            parts.append(f"🏋️ Станок: {stand_label}")

        bike_title = (entry.get("bike_title") or "").strip()
        bike_owner = (entry.get("bike_owner") or "").strip()
        if bike_title or bike_owner:
            if bike_owner:
                parts.append(
                    f"🚲 Велосипед: {bike_title} ({bike_owner})" if bike_title else f"🚲 Велосипед: {bike_owner}"
                )
            else:
                parts.append(f"🚲 Велосипед: {bike_title}")

        lines.append("\n".join(parts))

    await message.reply_text("\n\n".join(lines))


async def _history_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start, затем повторите попытку.")
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        await message.reply_text("Не удалось определить вашу анкету. Обратитесь к администратору.")
        return

    now_local = _local_now()
    try:
        reservations = schedule_repository.list_past_reservations_for_client(
            client_id,
            _to_local_naive(now_local),
            limit=10,
        )
    except Exception:
        LOGGER.exception("Failed to fetch reservation history for client %s", client_id)
        await message.reply_text("Не удалось получить историю записей. Попробуйте позже.")
        return

    if not reservations:
        await message.reply_text("📭 История бронирований пуста.")
        return

    lines: List[str] = ["📜 Последние посещения:"]
    for entry in reservations:
        slot_date_value = _parse_date(entry.get("slot_date"))
        time_range = _format_time_range(entry.get("start_time"), entry.get("end_time"))
        header = ""
        if slot_date_value:
            header = f"{slot_date_value.strftime('%d.%m.%Y (%a)')} · {time_range}"
        else:
            header = time_range

        parts = [f"🕘 {header}"]

        session_kind = entry.get("session_kind")
        instructor_name = (entry.get("instructor_name") or "").strip()
        if session_kind == "instructor":
            if instructor_name:
                parts.append(f"🧑‍🏫 Инструктор: {instructor_name}")
            else:
                parts.append("🧑‍🏫 Инструктор: уточняется")

        lines.append("\n".join(parts))

    await message.reply_text("\n\n".join(lines))
