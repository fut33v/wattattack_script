"""Event creation flow for adminbot."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone as datetime_timezone
import logging
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode

from adminbot.accounts import AccountConfig
from wattattack_activities import WattAttackClient

LOGGER = logging.getLogger(__name__)

EVENT_STATE_KEY = "events_flow_state"
DATE_BUTTONS = 7
BASE_TIME_CHOICES: Sequence[time] = tuple(
    time(hour, minute)
    for hour in range(6, 24)
    for minute in (0, 15, 30, 45)
)
TIMES_PER_PAGE = 6
DATE_PAGE_SPAN = 7
DEFAULT_LATE_JOIN_MINUTES = 15


@dataclass
class EventFlowState:
    chat_id: int
    mode: Optional[str] = None  # "ride" or "race"
    account_id: Optional[str] = None
    account_name: Optional[str] = None
    routes_payload: List[Dict[str, object]] = field(default_factory=list)
    route_id: Optional[int] = None
    route_label: Optional[str] = None
    map_label: Optional[str] = None
    selected_date: Optional[date] = None
    selected_time: Optional[time] = None
    date_offset: int = 0
    time_page: int = 0
    waiting_for_manual_time: bool = False
    waiting_for_title: bool = False
    title: Optional[str] = None
    measure_type: Optional[str] = None  # distance, duration, laps
    measure_value: Optional[float] = None
    waiting_for_measure_value: bool = False


def _get_state(context, chat_id: int) -> Optional[EventFlowState]:
    state = context.user_data.get(EVENT_STATE_KEY)
    if isinstance(state, EventFlowState) and state.chat_id == chat_id:
        return state
    return None


def _set_state(context, state: EventFlowState) -> None:
    context.user_data[EVENT_STATE_KEY] = state


def _clear_state(context) -> None:
    context.user_data.pop(EVENT_STATE_KEY, None)


def _build_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("❌ Отмена", callback_data="events|cancel")],
            [InlineKeyboardButton("↩️ В меню", callback_data="menu|start")],
        ]
    )


def _build_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🚴 Заезд", callback_data="events|type|ride"),
                InlineKeyboardButton("🏁 Гонка", callback_data="events|type|race"),
            ],
            [InlineKeyboardButton("❌ Закрыть", callback_data="events|cancel")],
            [InlineKeyboardButton("↩️ В меню", callback_data="menu|start")],
        ]
    )


async def start_events_flow(
    update: Update,
    context,
    accounts: Mapping[str, AccountConfig],
) -> None:
    message = update.effective_message
    if message is None:
        return
    state = EventFlowState(chat_id=message.chat_id)
    _set_state(context, state)
    if not accounts:
        await message.reply_text("⚠️ Нет доступных WattAttack аккаунтов для создания заезда.")
        _clear_state(context)
        return
    await message.reply_text(
        "🚀 Что создаём?\n"
        "Сначала выберите тип события — заезд или гонку.",
        reply_markup=_build_type_keyboard(),
    )


async def handle_events_callback(
    update: Update,
    context,
    accounts: Mapping[str, AccountConfig],
    timezone,
    timeout: float,
    parts: Sequence[str],
) -> bool:
    query = update.callback_query
    message = query.message
    chat_id = message.chat_id if message else update.effective_chat.id
    state = _get_state(context, chat_id)
    if not parts:
        return False
    action = parts[0]
    if action == "cancel":
        _clear_state(context)
        await query.edit_message_text("❌ Создание события отменено.")
        return True
    if state is None:
        await query.edit_message_text("ℹ️ Сессия не найдена. Отправьте /events, чтобы начать заново.")
        return True
    if action == "type":
        choice = parts[1] if len(parts) > 1 else ""
        if choice not in {"ride", "race"}:
            await query.edit_message_text("⚠️ Неизвестный тип события.")
            return True
        state.mode = choice
        label = "заезд" if choice == "ride" else "гонку"
        await query.edit_message_text(
            f"👥 Выберите WattAttack аккаунт, от имени которого создаём {label}:",
            reply_markup=_build_account_keyboard(accounts),
        )
        return True
    if action == "account":
        if len(parts) < 2:
            await query.edit_message_text("⚠️ Укажите аккаунт.", reply_markup=_build_account_keyboard(accounts))
            return True
        account_id = parts[1]
        account = accounts.get(account_id)
        if not account:
            await query.edit_message_text("⚠️ Аккаунт не найден.", reply_markup=_build_account_keyboard(accounts))
            return True
        state.account_id = account_id
        state.account_name = account.name
        state.route_id = None
        state.routes_payload = []
        state.selected_date = None
        state.selected_time = None
        state.title = None
        state.waiting_for_manual_time = False
        state.waiting_for_title = False
        state.time_page = 0
        state.measure_type = None
        state.measure_value = None
        state.waiting_for_measure_value = False
        await query.edit_message_text(
            f"🔄 Загружаю маршруты для <b>{account.name}</b>…",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_cancel_keyboard(),
        )
        try:
            routes = await _load_routes(account, timeout)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to load routes for %s", account_id)
            await query.edit_message_text(
                f"❌ Не удалось получить список маршрутов: {exc}",
                reply_markup=_build_account_keyboard(accounts),
            )
            return True
        if not routes:
            await query.edit_message_text(
                "⚠️ Список маршрутов пуст. Попробуйте позже.",
                reply_markup=_build_account_keyboard(accounts),
            )
            return True
        state.routes_payload = routes
        event_label = "гонки" if state.mode == "race" else "заезда"
        await query.edit_message_text(
            f"🗺 Выберите маршрут для {event_label} (аккаунт <b>{account.name}</b>):",
            parse_mode=ParseMode.HTML,
            reply_markup=_build_route_keyboard(routes),
        )
        return True
    if action == "route":
        if not state.routes_payload:
            await query.edit_message_text(
                "⚠️ Сначала выберите аккаунт, чтобы загрузить маршруты.",
                reply_markup=_build_account_keyboard(accounts),
            )
            return True
        if len(parts) < 2:
            await query.edit_message_text(
                "⚠️ Маршрут не выбран.",
                reply_markup=_build_route_keyboard(state.routes_payload),
            )
            return True
        try:
            route_id = int(parts[1])
        except ValueError:
            await query.edit_message_text(
                "⚠️ Некорректный идентификатор маршрута.",
                reply_markup=_build_route_keyboard(state.routes_payload),
            )
            return True
        route, map_info = _find_route(state.routes_payload, route_id)
        if not route:
            await query.edit_message_text(
                "⚠️ Маршрут не найден.",
                reply_markup=_build_route_keyboard(state.routes_payload),
            )
            return True
        state.route_id = route_id
        state.route_label = route.get("nameRu") or route.get("nameEn") or f"Маршрут {route_id}"
        state.map_label = map_info.get("nameRu") or map_info.get("nameEn") or "Маршрут"
        state.selected_date = None
        state.selected_time = None
        state.title = None
        state.waiting_for_manual_time = False
        state.waiting_for_title = False
        state.time_page = 0
        state.measure_type = None
        state.measure_value = None
        state.waiting_for_measure_value = False
        await query.edit_message_text(
            f"📅 Выберите дату старта для «{state.route_label}».",
            reply_markup=_build_date_keyboard(state, timezone),
        )
        return True
    if action == "datepage":
        direction = parts[1] if len(parts) > 1 else "next"
        if direction == "prev":
            state.date_offset = max(0, state.date_offset - DATE_PAGE_SPAN)
        else:
            state.date_offset += DATE_PAGE_SPAN
        await query.edit_message_text(
            f"📅 Выберите дату старта для «{state.route_label}».",
            reply_markup=_build_date_keyboard(state, timezone),
        )
        return True
    if action == "date":
        if len(parts) < 2:
            await query.edit_message_text(
                "⚠️ Дата не распознана.",
                reply_markup=_build_date_keyboard(state, timezone),
            )
            return True
        try:
            selected = datetime.strptime(parts[1], "%Y-%m-%d").date()
        except ValueError:
            await query.edit_message_text(
                "⚠️ Некорректная дата.",
                reply_markup=_build_date_keyboard(state, timezone),
            )
            return True
        state.selected_date = selected
        state.selected_time = None
        state.title = None
        state.waiting_for_title = False
        state.time_page = 0
        state.measure_type = None
        state.measure_value = None
        state.waiting_for_measure_value = False
        markup, has_slots = _build_time_keyboard(state, timezone)
        text = (
            f"⏰ Выберите время старта {selected.strftime('%d.%m.%Y')}:"
            if has_slots
            else "⏰ На выбранную дату доступных слотов не осталось. Выберите другую дату или укажите своё время."
        )
        await query.edit_message_text(text, reply_markup=markup)
        return True
    if action == "timepage":
        if not state.selected_date:
            await query.edit_message_text(
                "📅 Сначала выберите дату старта.",
                reply_markup=_build_date_keyboard(state, timezone),
            )
            return True
        direction = parts[1] if len(parts) > 1 else "next"
        choices = _get_time_choices(state, timezone)
        if direction == "prev":
            state.time_page = max(0, state.time_page - 1)
        else:
            max_page = max(0, (len(choices) - 1) // TIMES_PER_PAGE) if choices else 0
            state.time_page = min(max_page, state.time_page + 1)
        markup, has_slots = _build_time_keyboard(state, timezone)
        text = (
            f"⏰ Выберите время старта {state.selected_date.strftime('%d.%m.%Y') if state.selected_date else ''}:"
            if has_slots
            else "⏰ На выбранную дату доступных слотов не осталось. Выберите другую дату или укажите своё время."
        )
        await query.edit_message_text(text, reply_markup=markup)
        return True
    if action == "time":
        if not state.selected_date:
            await query.edit_message_text(
                "📅 Сначала выберите дату.",
                reply_markup=_build_date_keyboard(state, timezone),
            )
            return True
        if len(parts) < 2:
            markup, has_slots = _build_time_keyboard(state, timezone)
            text = (
                "⚠️ Укажите время старта."
                if has_slots
                else "⚠️ На выбранную дату нет доступных слотов. Выберите другую дату или задайте своё время."
            )
            await query.edit_message_text(text, reply_markup=markup)
            return True
        if parts[1] == "custom":
            state.waiting_for_manual_time = True
            state.selected_time = None
            state.waiting_for_title = False
            instruction = (
                "📝 Отправьте время старта в формате <code>YYYY-MM-DD HH:MM</code>\n"
                "Можно указать только время (<code>HH:MM</code>) — будет использована выбранная дата."
            )
            await query.edit_message_text(
                instruction,
                parse_mode=ParseMode.HTML,
                reply_markup=_build_cancel_keyboard(),
            )
            return True
        try:
            hour, minute = parts[1].split(":")
            selected_time = time(int(hour), int(minute))
        except ValueError:
            markup, has_slots = _build_time_keyboard(state, timezone)
            text = (
                "⚠️ Некорректное время."
                if has_slots
                else "⚠️ На выбранную дату нет доступных слотов. Выберите другую дату или задайте своё время."
            )
            await query.edit_message_text(text, reply_markup=markup)
            return True
        state.selected_time = selected_time
        state.waiting_for_manual_time = False
        state.title = None
        if state.mode == "race":
            state.measure_type = None
            state.measure_value = None
            state.waiting_for_measure_value = False
            state.waiting_for_title = False
            await query.edit_message_text(
                "⚙️ Выберите формат гонки:",
                reply_markup=_build_measure_type_keyboard(),
            )
            return True
        state.waiting_for_title = True
        await query.edit_message_text(
            "✍️ Отправьте название события одним сообщением.",
            reply_markup=_build_cancel_keyboard(),
        )
        return True
    if action == "measure":
        if len(parts) < 2:
            await query.edit_message_text(
                "⚠️ Выберите формат гонки.",
                reply_markup=_build_measure_type_keyboard(),
            )
            return True
        choice = parts[1]
        if choice not in {"distance", "duration", "laps"}:
            await query.edit_message_text(
                "⚠️ Неизвестный формат гонки.",
                reply_markup=_build_measure_type_keyboard(),
            )
            return True
        state.measure_type = choice
        state.measure_value = None
        state.waiting_for_measure_value = True
        await query.edit_message_text(
            _measure_prompt(choice),
            reply_markup=_build_measure_value_keyboard(),
        )
        return True
    if action == "confirm":
        if not state.account_id or not state.route_id or not state.selected_date or not state.selected_time or not state.title:
            await query.edit_message_text(
                "⚠️ Недостаточно данных для создания события.",
                reply_markup=_build_cancel_keyboard(),
            )
            return True
        if state.mode == "race" and (state.measure_type is None or state.measure_value is None):
            await query.edit_message_text(
                "⚠️ Укажите формат гонки и значение перед созданием.",
                reply_markup=_build_measure_type_keyboard(),
            )
            return True
        label = "гонку" if state.mode == "race" else "заезд"
        await query.edit_message_text(f"🚀 Создаю {label}…", reply_markup=_build_cancel_keyboard())
        await _create_event(query, context, state, accounts, timezone, timeout)
        return True
    if action == "back":
        target = parts[1] if len(parts) > 1 else ""
        if target == "time":
            state.selected_time = None
            state.title = None
            state.waiting_for_manual_time = False
            state.waiting_for_title = False
            state.measure_type = None
            state.measure_value = None
            state.waiting_for_measure_value = False
            markup, has_slots = _build_time_keyboard(state, timezone)
            text = (
                f"⏰ Выберите время старта {state.selected_date.strftime('%d.%m.%Y') if state.selected_date else ''}:"
                if has_slots
                else "⏰ На выбранную дату доступных слотов не осталось. Выберите другую дату или укажите своё время."
            )
            await query.edit_message_text(text, reply_markup=markup)
            return True
        if target == "date":
            state.selected_date = None
            state.selected_time = None
            state.title = None
            state.waiting_for_manual_time = False
            state.waiting_for_title = False
            state.time_page = 0
            state.measure_type = None
            state.measure_value = None
            state.waiting_for_measure_value = False
            await query.edit_message_text(
                f"📅 Выберите дату старта для «{state.route_label or 'маршрут'}».",
                reply_markup=_build_date_keyboard(state, timezone),
            )
            return True
        if target == "route":
            state.route_id = None
            state.selected_date = None
            state.selected_time = None
            state.title = None
            state.waiting_for_manual_time = False
            state.waiting_for_title = False
            state.time_page = 0
            state.measure_type = None
            state.measure_value = None
            state.waiting_for_measure_value = False
            await query.edit_message_text(
                "🗺 Выберите маршрут:",
                reply_markup=_build_route_keyboard(state.routes_payload),
            )
            return True
        if target == "account":
            await query.edit_message_text(
                "👥 Выберите WattAttack аккаунт:",
                reply_markup=_build_account_keyboard(accounts),
            )
            return True
        if target == "measure":
            state.measure_type = None
            state.measure_value = None
            state.waiting_for_measure_value = False
            await query.edit_message_text(
                "⚙️ Выберите формат гонки:",
                reply_markup=_build_measure_type_keyboard(),
            )
            return True
    return False


async def handle_events_text(update: Update, context, timezone) -> bool:
    message = update.message
    if message is None:
        return False
    state = _get_state(context, message.chat_id)
    if not state:
        return False
    text = (message.text or "").strip()
    if state.waiting_for_manual_time:
        success = _apply_manual_time(state, text, timezone)
        if not success:
            await message.reply_text(
                "⚠️ Не удалось распознать дату/время. Пример: <code>2025-11-23 13:30</code>.",
                parse_mode=ParseMode.HTML,
            )
            return True
        state.waiting_for_manual_time = False
        if state.mode == "race":
            state.measure_type = None
            state.measure_value = None
            state.waiting_for_measure_value = False
            state.waiting_for_title = False
            await message.reply_text(
                "⚙️ Выберите формат гонки:",
                reply_markup=_build_measure_type_keyboard(),
            )
        else:
            state.waiting_for_title = True
            await message.reply_text("✍️ Отправьте название события одним сообщением.")
        return True
    if state.waiting_for_measure_value:
        success, error = _apply_measure_value(state, text)
        if not success:
            await message.reply_text(error)
            return True
        state.waiting_for_measure_value = False
        state.waiting_for_title = True
        await message.reply_text("✍️ Отправьте название гонки одним сообщением.")
        return True
    if state.waiting_for_title:
        if not text:
            await message.reply_text("⚠️ Название не может быть пустым.")
            return True
        state.title = text
        state.waiting_for_title = False
        await _send_summary(message, state, timezone)
        return True
    return False


def _build_account_keyboard(accounts: Mapping[str, AccountConfig]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for account_id in sorted(accounts):
        button = InlineKeyboardButton(
            accounts[account_id].name,
            callback_data=f"events|account|{account_id}",
        )
        current_row.append(button)
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="events|cancel")])
    rows.append([InlineKeyboardButton("↩️ В меню", callback_data="menu|start")])
    return InlineKeyboardMarkup(rows)


def _build_route_keyboard(routes: Iterable[Dict[str, object]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for item in routes:
        map_name = item.get("nameRu") or item.get("nameEn") or "Маршрут"
        rows.append([InlineKeyboardButton(f"🗺 {map_name}", callback_data="noop")])
        for route in item.get("routes") or []:
            route_id = route.get("id")
            if route_id is None:
                continue
            distance = (route.get("distance") or 0) / 1000
            route_name = route.get("nameRu") or route.get("nameEn") or str(route_id)
            elevation = route.get("totalElevation") or 0
            elevation_text = f" · {int(elevation)} м" if elevation else ""
            label = f"{route_name} · {distance:.1f} км{elevation_text}"
            rows.append(
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"events|route|{route_id}",
                    )
                ]
            )
    rows.append(
        [
            InlineKeyboardButton("↩️ К аккаунтам", callback_data="events|back|account"),
            InlineKeyboardButton("❌ Отмена", callback_data="events|cancel"),
        ]
    )
    rows.append([InlineKeyboardButton("↩️ В меню", callback_data="menu|start")])
    return InlineKeyboardMarkup(rows)


def _build_date_keyboard(state: EventFlowState, timezone) -> InlineKeyboardMarkup:
    today = datetime.now(timezone).date()
    start = today + timedelta(days=state.date_offset)
    buttons: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for idx in range(DATE_BUTTONS):
        target_date = start + timedelta(days=idx)
        label = target_date.strftime("%a %d.%m")
        current_row.append(
            InlineKeyboardButton(
                label,
                callback_data=f"events|date|{target_date.isoformat()}",
            )
        )
        if len(current_row) == 3:
            buttons.append(current_row)
            current_row = []
    if current_row:
        buttons.append(current_row)
    nav_row: List[InlineKeyboardButton] = []
    nav_row.append(
        InlineKeyboardButton("« Назад", callback_data="events|datepage|prev")
    )
    nav_row.append(
        InlineKeyboardButton("Вперёд »", callback_data="events|datepage|next")
    )
    buttons.append(nav_row)
    buttons.append(
        [
            InlineKeyboardButton("↩️ К маршрутам", callback_data="events|back|route"),
            InlineKeyboardButton("❌ Отмена", callback_data="events|cancel"),
        ]
    )
    buttons.append([InlineKeyboardButton("↩️ В меню", callback_data="menu|start")])
    return InlineKeyboardMarkup(buttons)


def _get_time_choices(state: EventFlowState, timezone) -> List[time]:
    choices = list(BASE_TIME_CHOICES)
    if not state.selected_date:
        return choices
    today = datetime.now(timezone).date()
    if state.selected_date == today:
        now_local = datetime.now(timezone)
        choices = [
            slot
            for slot in choices
            if datetime.combine(today, slot, tzinfo=timezone) > now_local
        ]
    return choices


def _build_time_keyboard(state: EventFlowState, timezone) -> Tuple[InlineKeyboardMarkup, bool]:
    choices = _get_time_choices(state, timezone)
    if not choices:
        rows = [
            [InlineKeyboardButton("↩️ К датам", callback_data="events|back|date")],
            [InlineKeyboardButton("❌ Отмена", callback_data="events|cancel")],
            [InlineKeyboardButton("↩️ В меню", callback_data="menu|start")],
        ]
        return InlineKeyboardMarkup(rows), False

    max_page = max(0, (len(choices) - 1) // TIMES_PER_PAGE)
    if state.time_page > max_page:
        state.time_page = max_page
    rows: List[List[InlineKeyboardButton]] = []
    start_index = state.time_page * TIMES_PER_PAGE
    page_values = choices[start_index : start_index + TIMES_PER_PAGE]
    current_row: List[InlineKeyboardButton] = []
    for value in page_values:
        label = value.strftime("%H:%M")
        current_row.append(
            InlineKeyboardButton(label, callback_data=f"events|time|{label}")
        )
        if len(current_row) == 3:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append(
        [
            InlineKeyboardButton("«", callback_data="events|timepage|prev"),
            InlineKeyboardButton("📝 Своё время", callback_data="events|time|custom"),
            InlineKeyboardButton("»", callback_data="events|timepage|next"),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton("↩️ К датам", callback_data="events|back|date"),
            InlineKeyboardButton("❌ Отмена", callback_data="events|cancel"),
        ]
    )
    rows.append([InlineKeyboardButton("↩️ В меню", callback_data="menu|start")])
    return InlineKeyboardMarkup(rows), True


def _build_measure_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📏 Дистанция", callback_data="events|measure|distance"),
                InlineKeyboardButton("🕒 Время", callback_data="events|measure|duration"),
                InlineKeyboardButton("🔁 Круги", callback_data="events|measure|laps"),
            ],
            [InlineKeyboardButton("↩️ К времени", callback_data="events|back|time")],
            [InlineKeyboardButton("❌ Отмена", callback_data="events|cancel")],
            [InlineKeyboardButton("↩️ В меню", callback_data="menu|start")],
        ]
    )


def _build_measure_value_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ Формат", callback_data="events|back|measure")],
            [InlineKeyboardButton("❌ Отмена", callback_data="events|cancel")],
            [InlineKeyboardButton("↩️ В меню", callback_data="menu|start")],
        ]
    )


def _measure_prompt(measure_type: str) -> str:
    if measure_type == "distance":
        return "📏 Укажите дистанцию в километрах (например, 35 или 42.5)."
    if measure_type == "duration":
        return "🕒 Укажите продолжительность гонки в минутах (например, 60)."
    return "🔁 Укажите количество кругов (целое число, например, 5)."


def _format_measure_summary(state: EventFlowState) -> str:
    measure = state.measure_type or "distance"
    value = state.measure_value or 0
    if measure == "distance":
        return f"• Формат: {value:.1f} км"
    if measure == "duration":
        return f"• Формат: {int(value)} минут"
    return f"• Формат: {int(value)} кругов"


def _build_summary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Создать событие", callback_data="events|confirm")],
            [
                InlineKeyboardButton("🕘 Изменить время", callback_data="events|back|time"),
                InlineKeyboardButton("❌ Отмена", callback_data="events|cancel"),
            ],
            [InlineKeyboardButton("↩️ В меню", callback_data="menu|start")],
        ]
    )


async def _send_summary(target_message, state: EventFlowState, timezone) -> None:
    if not (state.account_name and state.route_label and state.selected_date and state.selected_time and state.title):
        return
    if state.mode == "race" and (state.measure_type is None or state.measure_value is None):
        return
    start_local = datetime.combine(
        state.selected_date,
        state.selected_time,
        tzinfo=timezone,
    )
    tz_label = getattr(timezone, "key", str(timezone))
    header = "гонки" if state.mode == "race" else "заезда"
    lines = [
        f"📋 Проверьте детали {header}:",
        f"• Название: <b>{state.title}</b>",
        f"• Аккаунт: {state.account_name}",
        f"• Маршрут: {state.route_label}",
        f"• Дата: {start_local.strftime('%d.%m.%Y')}",
        f"• Время: {start_local.strftime('%H:%M')} ({tz_label})",
    ]
    if state.mode == "race":
        lines.append(_format_measure_summary(state))
        lines.append("• Опции: драфтинг, изоляция, финишная таблица, позднее присоединение")
    else:
        lines.append("• Опции: драфтинг, изоляция, притягивание к лидеру, позднее присоединение")
    await target_message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=_build_summary_keyboard(),
    )


async def _create_event(query, context, state: EventFlowState, accounts, timezone, timeout: float) -> None:
    start_local = datetime.combine(
        state.selected_date,
        state.selected_time,
        tzinfo=timezone,
    )
    late_join = start_local + timedelta(minutes=DEFAULT_LATE_JOIN_MINUTES)
    route, _ = _find_route(state.routes_payload, state.route_id)
    distance_km = None
    if route and route.get("distance"):
        distance_km = max(1.0, round((route["distance"] or 0) / 1000, 1))
    account = accounts.get(state.account_id)
    if not account:
        await query.edit_message_text("⚠️ Аккаунт не найден.", reply_markup=_build_cancel_keyboard())
        return

    payload = {
        "nameRu": state.title,
        "descriptionRu": state.title,
        "routeId": state.route_id,
        "eventType": "group_race" if state.mode == "race" else "group_ride",
        "startDate": _to_api_iso(start_local),
        "isLateJoin": True,
        "isLeaderMagnet": False if state.mode == "race" else True,
        "isIsolated": True,
        "isShowResults": True,
        "isDraftingEnabled": True,
    }
    if state.mode == "race":
        measure = state.measure_type or "distance"
        payload["eventMeasureType"] = measure
        if measure == "distance":
            payload["distanceKm"] = float(state.measure_value)
        elif measure == "duration":
            payload["durationMinutes"] = int(state.measure_value)
        else:
            payload["lapsCount"] = int(state.measure_value)
    else:
        payload["eventMeasureType"] = "distance"
        if distance_km:
            payload["distanceKm"] = distance_km
    payload["lateJoinDeadline"] = _to_api_iso(late_join)

    def worker() -> Dict[str, object]:
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=timeout)
        return client.create_event(payload, timeout=timeout)

    try:
        result = await asyncio.to_thread(worker)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to create ride event for %s", account.identifier)
        await query.edit_message_text(f"❌ Не удалось создать заезд: {exc}")
        return

    event = result.get("event") if isinstance(result, dict) else None
    label = "Гонка" if state.mode == "race" else "Заезд"
    if isinstance(event, dict):
        event_id = event.get("id")
        base_url = account.base_url.rstrip("/")
        link = f"{base_url}/cabinet/events/{event_id}" if event_id else None
        text_lines = [
            f"✅ {label} создан!",
            f"ID: {event_id} ({event.get('publicId')})",
            f"Маршрут: {state.route_label}",
            f"Старт: {event.get('startDate')}",
        ]
        if link:
            text_lines.append(f"🔗 {link}")
        text = "\n".join(text_lines)
    else:
        text = f"✅ {label} создан."
    await query.edit_message_text(text)
    _clear_state(context)


async def _load_routes(account: AccountConfig, timeout: float) -> List[Dict[str, object]]:
    def worker() -> List[Dict[str, object]]:
        client = WattAttackClient(account.base_url)
        client.login(account.email, account.password, timeout=timeout)
        return client.fetch_maps_with_routes(timeout=timeout)

    return await asyncio.to_thread(worker)


def _find_route(
    routes_payload: Iterable[Dict[str, object]],
    route_id: Optional[int],
) -> Tuple[Optional[Dict[str, object]], Optional[Dict[str, object]]]:
    if route_id is None:
        return None, None
    for item in routes_payload:
        for route in item.get("routes") or []:
            if route.get("id") == route_id:
                return route, item
    return None, None


def _apply_manual_time(state: EventFlowState, text: str, timezone) -> bool:
    text = text.strip()
    parsed = _parse_datetime(text, timezone, state.selected_date)
    if not parsed:
        return False
    state.selected_date = parsed.date()
    state.selected_time = time(parsed.hour, parsed.minute)
    return True


def _apply_measure_value(state: EventFlowState, text: str) -> Tuple[bool, str]:
    if not state.measure_type:
        return False, "⚠️ Сначала выберите формат гонки."
    normalized = text.replace(",", ".").strip()
    if state.measure_type == "distance":
        try:
            value = float(normalized)
        except ValueError:
            return False, "⚠️ Укажите расстояние в километрах, например 35.5"
        if value <= 0:
            return False, "⚠️ Дистанция должна быть больше нуля."
        state.measure_value = round(value, 2)
        return True, ""
    try:
        value_int = int(float(normalized))
    except ValueError:
        if state.measure_type == "duration":
            return False, "⚠️ Укажите продолжительность в минутах, например 45"
        return False, "⚠️ Укажите количество кругов целым числом"
    if value_int <= 0:
        if state.measure_type == "duration":
            return False, "⚠️ Продолжительность должна быть больше нуля."
        return False, "⚠️ Количество кругов должно быть больше нуля."
    state.measure_value = float(value_int)
    return True, ""


def _parse_datetime(value: str, timezone, fallback_date: Optional[date]) -> Optional[datetime]:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    candidates = [value, value.replace(" ", "T")]
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone)
        else:
            dt = dt.astimezone(timezone)
        return dt
    for fmt in ("%d.%m.%Y %H:%M", "%Y.%m.%d %H:%M"):
        try:
            dt = datetime.strptime(value, fmt)
            dt = dt.replace(tzinfo=timezone)
            return dt
        except ValueError:
            continue
    if fallback_date and ":" in value:
        try:
            hour, minute = value.split(":")
            dt = datetime(
                fallback_date.year,
                fallback_date.month,
                fallback_date.day,
                int(hour),
                int(minute),
                tzinfo=timezone,
            )
            return dt
        except ValueError:
            return None
    return None


def _to_api_iso(dt: datetime) -> str:
    return dt.astimezone(datetime_timezone.utc).isoformat().replace("+00:00", "Z")
