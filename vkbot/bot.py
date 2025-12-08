"""VK bot that replies with a greeting and simple booking actions (book/profile/cancel)."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

import vk_api
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.utils import get_random_id

from booking import notifications as booking_notifications
from booking import service as booking_service
from notifications import admin as admin_notifications
from repositories import client_repository, schedule_repository
from repositories.vk_client_link_repository import get_link_by_vk_user, link_vk_user_to_client
from .formatting import (
    format_client_name,
    format_day_display,
    format_reservation_details,
    format_reservation_label,
    format_slot_label,
)
from .keyboards import build_day_keyboard, build_inline_keyboard, build_slots_keyboard
from .matchers import match_booking_text, match_cancel_text, match_day_text, match_slot_text

log = logging.getLogger(__name__)

DEFAULT_GREETING = "Привет!"
_PAGE_SIZE = 4
WHAT_TO_BRING_TEXT = (
    "⛹️‍♂️ Что взять с собой в КРУТИЛКУ ⛹️‍♀️\n\n"
    "Чтобы тренировка была комфортной, подготовьте:\n"
    "✅ Бутылку чистой воды\n"
    "✅ Полотенце\n"
    "✅ Удобную сменную обувь (кроссовки или велотуфли)\n"
    "✅ Шлёпанцы\n"
    "✅ Принадлежности для душа\n\n"
    "Собирайте рюкзак и увидимся на тренировке!"
)
HOW_TO_GET_ATTACHMENT = "video-232708853_456239017"
_LOCAL_TZ = booking_service.LOCAL_TZ
_BOOKING_CUTOFF = booking_service.BOOKING_CUTOFF
_STATE: Dict[int, Dict[str, Any]] = {}
_NEW_CLIENT_STEPS: List[Dict[str, str]] = [
    {"key": "weight", "prompt": "Введите ваш вес в кг (например, 72.5)."},
    {"key": "height", "prompt": "Введите ваш рост в см (например, 178)."},
    {"key": "ftp", "prompt": "Введите FTP в ваттах или нажмите «Пропустить»."},
    {"key": "pedals", "prompt": "Выберите тип педалей или нажмите «Пропустить»."},
    {"key": "goal", "prompt": "Опишите цель тренировок (можно коротко)."},
]
_PEDAL_OPTIONS: List[tuple[str, str]] = [
    ("топталки (под кроссовки)", "топталки"),
    ("контакты шоссе Look", "контакты look"),
    ("контакты шоссе Shimano", "контакты shimano road"),
    ("контакты MTB SPD", "контакты spd"),
    ("Пропустить", "пропустить"),
]


def run_bot(
    *,
    token: str,
    group_id: int,
    greeting: str = DEFAULT_GREETING,
    api_version: Optional[str] = None,
) -> None:
    """Start a long poll loop and reply with a greeting + booking actions."""
    session_args: Dict[str, object] = {"token": token}
    if api_version:
        session_args["api_version"] = api_version

    vk_session = vk_api.VkApi(**session_args)
    vk = vk_session.get_api()
    longpoll = VkBotLongPoll(vk_session, group_id)

    log.info("VK bot is listening on group %s with long poll", group_id)
    inline_keyboard = build_inline_keyboard()
    for event in longpoll.listen():
        if event.type == VkBotEventType.MESSAGE_EVENT:
            _handle_message_event(event, vk, greeting)
            continue

        if event.type != VkBotEventType.MESSAGE_NEW:
            continue

        message = getattr(event, "message", None)
        if not message:
            continue

        peer_id = message.get("peer_id")
        user_id = message.get("from_id")
        text = message.get("text", "")
        payload = message.get("payload")
        if peer_id is None:
            log.debug("Received message without peer_id: %s", message)
            continue

        log.info("Incoming message from peer %s: %s", peer_id, text)
        handled = _handle_stateful_message(peer_id, user_id, text.strip(), vk, payload=payload)
        if handled:
            continue

        try:
            vk.messages.send(
                peer_id=peer_id,
                random_id=get_random_id(),
                message=greeting,
                keyboard=inline_keyboard,
            )
        except Exception:
            log.exception("Failed to send greeting to peer_id %s", peer_id)


def _handle_message_event(event, vk, greeting: str) -> None:
    payload = getattr(event.object, "payload", {}) or {}
    action = payload.get("action")
    peer_id = getattr(event.object, "peer_id", None)
    user_id = getattr(event.object, "user_id", None)
    if not action or not peer_id or not user_id:
        return

    log.info("Received inline action %s from user %s in peer %s", action, user_id, peer_id)
    if action == "book":
        _start_booking(peer_id, user_id, vk)
    elif action == "profile":
        _show_profile(peer_id, user_id, vk, greeting)
    elif action == "cancel":
        _show_cancellable(peer_id, user_id, vk)
    elif action == "link_pick":
        try:
            client_id = int(payload.get("client_id"))
        except Exception:
            _send_text(vk, peer_id, "Не понял, какую анкету выбрать.")
            return
        _link_client_by_id(peer_id, client_id, vk)
    elif action == "book_day":
        date_str = payload.get("date")
        _show_slots_for_day(peer_id, user_id, date_str, vk)
    elif action == "slots_page":
        date_str = payload.get("date")
        try:
            page = int(payload.get("page", 0))
        except Exception:
            page = 0
        _show_slots_for_day(peer_id, user_id, date_str, vk, page=page)
    elif action == "slot":
        try:
            slot_id = int(payload.get("slot_id"))
        except Exception:
            _send_text(vk, peer_id, "Не понял слот. Попробуйте снова.")
            return
        _confirm_slot(peer_id, user_id, slot_id, vk)
    elif action == "cancel_reservation":
        try:
            reservation_id = int(payload.get("reservation_id"))
        except Exception:
            _send_text(vk, peer_id, "Не понял запись. Попробуйте снова.")
            return
        _cancel_reservation(peer_id, user_id, reservation_id, vk)
    elif action == "my_bookings":
        _show_future_bookings(peer_id, user_id, vk)
    elif action == "booking_details":
        try:
            reservation_id = int(payload.get("reservation_id"))
        except Exception:
            _send_text(vk, peer_id, "Не понял запись. Попробуйте снова.")
            return
        _show_booking_details(peer_id, user_id, reservation_id, vk)
    elif action == "cancel_page":
        try:
            page = int(payload.get("page", 0))
        except Exception:
            page = 0
        _show_cancellable(peer_id, user_id, vk, page=page)
    elif action == "booking_page":
        try:
            page = int(payload.get("page", 0))
        except Exception:
            page = 0
        _show_future_bookings(peer_id, user_id, vk, page=page)
    elif action == "close":
        _reset_to_main(peer_id, vk)
    elif action == "how_to_get":
        _send_video(vk, peer_id, attachment=HOW_TO_GET_ATTACHMENT, caption="Как добраться ⬆️")
        _send_text(vk, peer_id, "Меню:", keyboard=build_inline_keyboard())
    elif action == "what_to_bring":
        _send_text(vk, peer_id, WHAT_TO_BRING_TEXT, keyboard=build_inline_keyboard())

    try:
        vk.messages.sendMessageEventAnswer(
            event_id=event.object.event_id,
            user_id=user_id,
            peer_id=peer_id,
            event_data={"type": "show_snackbar", "text": "✅"},
        )
    except Exception:
        log.debug("Failed to send snackbar for %s", action, exc_info=True)


def _get_state(peer_id: int) -> Dict[str, Any]:
    return _STATE.setdefault(peer_id, {})


def _clear_state(peer_id: int) -> None:
    _STATE.pop(peer_id, None)


def _handle_stateful_message(peer_id: int, user_id: Optional[int], text: str, vk, *, payload: Any = None) -> bool:
    payload_data = None
    if isinstance(payload, str):
        try:
            payload_data = json.loads(payload)
        except Exception:
            payload_data = None
    elif isinstance(payload, dict):
        payload_data = payload

    # Plain-text shortcuts for main actions in case inline events are unavailable.
    lowered = text.lower()
    if lowered in {"забронировать", "бронь", "book"}:
        _start_booking(peer_id, user_id or peer_id, vk)
        return True
    if lowered in {"анкета", "профиль", "profile"}:
        _show_profile(peer_id, user_id or peer_id, vk, DEFAULT_GREETING)
        return True
    if lowered in {"отменить", "cancel", "отменить бронь"}:
        _show_cancellable(peer_id, user_id or peer_id, vk)
        return True
    if lowered in {"мои брони", "брони", "my bookings"}:
        _show_future_bookings(peer_id, user_id or peer_id, vk)
        return True
    if lowered in {"закрыть", "close"}:
        _reset_to_main(peer_id, vk)
        return True
    if lowered in {"как добраться", "маршрут"}:
        _send_video(vk, peer_id, attachment=HOW_TO_GET_ATTACHMENT, caption="Как добраться ⬆️")
        _send_text(vk, peer_id, "Меню:", keyboard=build_inline_keyboard())
        return True
    if lowered in {"что взять", "что взять с собой"}:
        _send_text(vk, peer_id, WHAT_TO_BRING_TEXT, keyboard=build_inline_keyboard())
        return True
    if payload_data:
        action = payload_data.get("action")
        if action == "slots_page":
            date_str = payload_data.get("date")
            try:
                page = int(payload_data.get("page", 0))
            except Exception:
                page = 0
            _show_slots_for_day(peer_id, user_id or peer_id, date_str, vk, page=page)
            return True
        if action == "cancel_page":
            try:
                page = int(payload_data.get("page", 0))
            except Exception:
                page = 0
            _show_cancellable(peer_id, user_id or peer_id, vk, page=page)
            return True
        if action == "booking_page":
            try:
                page = int(payload_data.get("page", 0))
            except Exception:
                page = 0
            _show_future_bookings(peer_id, user_id or peer_id, vk, page=page)
            return True

    state = _get_state(peer_id)
    mode = state.get("mode")
    if mode == "new_client":
        _handle_new_client_input(peer_id, text, vk)
        return True

    day_choice = match_day_text(text, day_map=state.get("day_map"), mode=mode)
    if day_choice:
        _show_slots_for_day(peer_id, user_id or peer_id, day_choice, vk)
        return True

    slot_choice = match_slot_text(text, slots_map=state.get("slots_map"), mode=mode)
    if slot_choice:
        _confirm_slot(peer_id, user_id or peer_id, slot_choice, vk)
        return True

    booking_choice = match_booking_text(text, booking_map=state.get("booking_map"), mode=mode)
    if booking_choice:
        _show_booking_details(peer_id, user_id or peer_id, booking_choice, vk)
        return True

    cancel_choice = match_cancel_text(text, cancel_map=state.get("cancel_map"), mode=mode)
    if cancel_choice:
        _cancel_reservation(peer_id, user_id or peer_id, cancel_choice, vk)
        return True
    if mode == "link_search":
        _handle_link_search(peer_id, text, vk)
        return True
    if mode == "link_pick":
        _handle_link_pick(peer_id, text, vk)
        return True
    if mode == "slot_select" and state.get("current_date"):
        if text in {"→", ">", ">>", "вперёд"}:
            _show_slots_for_day(
                peer_id,
                user_id or peer_id,
                state["current_date"],
                vk,
                page=(state.get("slots_page", 0) or 0) + 1,
            )
            return True
        if text in {"←", "<", "<<", "назад"}:
            _show_slots_for_day(
                peer_id,
                user_id or peer_id,
                state["current_date"],
                vk,
                page=(state.get("slots_page", 0) or 0) - 1,
            )
            return True
    return False


def _send_text(vk, peer_id: int, message: str, *, keyboard: Optional[str] = None) -> None:
    try:
        vk.messages.send(
            peer_id=peer_id,
            random_id=get_random_id(),
            message=message,
            keyboard=keyboard,
        )
    except Exception:
        log.exception("Failed to send message to peer %s", peer_id)


def _send_video(vk, peer_id: int, *, attachment: str, caption: str = "") -> None:
    try:
        vk.messages.send(
            peer_id=peer_id,
            random_id=get_random_id(),
            message=caption,
            attachment=attachment,
        )
    except Exception:
        log.exception("Failed to send video to peer %s", peer_id)


def _reset_to_main(peer_id: int, vk) -> None:
    """Clear state and show the main menu keyboard."""
    _clear_state(peer_id)
    _send_text(vk, peer_id, DEFAULT_GREETING, keyboard=build_inline_keyboard())


def _local_now() -> datetime:
    return booking_service.local_now()


def _get_linked_client(peer_id: int) -> tuple[Optional[Dict], Optional[Dict]]:
    link = get_link_by_vk_user(peer_id)
    client: Optional[Dict] = None
    if link and isinstance(link.get("client_id"), int):
        try:
            client = client_repository.get_client(link["client_id"])
        except Exception:
            log.exception("Failed to load client %s", link["client_id"])
    return link, client


def _prompt_link(peer_id: int, vk) -> None:
    _auto_link_or_onboard(peer_id, vk)


def _handle_link_search(peer_id: int, term: str, vk) -> None:
    candidates = _search_clients(term, require_full_match=False)
    if not candidates:
        _start_new_client_onboarding(peer_id, vk)
        return
    if len(candidates) == 1:
        _link_client(peer_id, candidates[0], vk)
        return
    _present_candidates(peer_id, candidates, vk)


def _handle_link_pick(peer_id: int, text: str, vk) -> None:
    state = _get_state(peer_id)
    candidates: List[Dict[str, Any]] = state.get("candidates") or []
    text_clean = (text or "").strip()
    idx = None
    try:
        idx = int(text_clean) - 1
    except Exception:
        idx = None

    if idx is not None:
        if idx < 0 or idx >= len(candidates):
            _send_text(vk, peer_id, "Такой номер вне списка. Попробуйте снова.")
            return
        _link_client(peer_id, candidates[idx], vk)
        return

    # Try to match by name substring
    lowered = text_clean.lower()
    matched = [c for c in candidates if lowered and lowered in format_client_name(c).lower()]
    if len(matched) == 1:
        _link_client(peer_id, matched[0], vk)
        return

    _send_text(vk, peer_id, "Пришлите номер анкеты (например, 1).")


def _link_client(peer_id: int, client: Dict[str, Any], vk) -> None:
    state = _get_state(peer_id)
    state.pop("mode", None)
    state.pop("candidates", None)
    client_id = client.get("id")
    if not isinstance(client_id, int):
        _send_text(vk, peer_id, "Не удалось привязать эту анкету.")
        return

    try:
        vk_user = _fetch_vk_user(peer_id, vk)
        full_name = " ".join(part for part in [(vk_user.get("first_name") or "").strip(), (vk_user.get("last_name") or "").strip()] if part) or None
        link_vk_user_to_client(
            vk_user_id=peer_id,
            client_id=client_id,
            vk_username=vk_user.get("screen_name"),
            vk_full_name=full_name,
        )
    except Exception:
        log.exception("Failed to link peer %s to client %s", peer_id, client_id)
        _send_text(vk, peer_id, "Не получилось привязать. Попробуйте снова.")
        return

    _clear_state(peer_id)
    _send_text(vk, peer_id, f"Привязали: {format_client_name(client)}. Теперь можно бронировать.", keyboard=build_inline_keyboard())


def _present_candidates(peer_id: int, candidates: List[Dict[str, Any]], vk, *, prompt: str = "Нашёл анкеты:") -> None:
    lines = [prompt]
    keyboard = VkKeyboard(inline=True)
    for idx, c in enumerate(candidates):
        client_id = c.get("id")
        if not isinstance(client_id, int):
            continue
        lines.append(f"• {format_client_name(c)} (id {client_id})")
        keyboard.add_button(
            format_client_name(c),
            color=VkKeyboardColor.PRIMARY,
            payload={"action": "link_pick", "client_id": client_id},
        )
        if idx != len(candidates) - 1:
            keyboard.add_line()
    lines.append("Или пришлите номер выбранной анкеты (1-5).")
    state = _get_state(peer_id)
    state["mode"] = "link_pick"
    state["candidates"] = candidates
    _send_text(vk, peer_id, "\n".join(lines), keyboard=keyboard.get_keyboard())
    state["mode"] = "link_pick"


def _search_clients(term: str, *, require_full_match: bool = False, first_name: Optional[str] = None) -> List[Dict[str, Any]]:
    try:
        candidates = client_repository.search_clients(term, limit=5)
    except Exception:
        log.exception("Failed to search clients by term %s", term)
        return []

    if not require_full_match:
        return candidates

    term_clean = term.strip().lower()
    first_clean = (first_name or "").strip().lower()
    filtered = []
    for c in candidates:
        last_name = (c.get("last_name") or "").strip().lower()
        first = (c.get("first_name") or "").strip().lower()
        if last_name != term_clean:
            continue
        if first_clean and first and first != first_clean:
            continue
        filtered.append(c)
    return filtered


def _fetch_vk_user(peer_id: int, vk) -> Dict[str, Any]:
    try:
        info = vk.users.get(user_ids=peer_id)
        if info:
            return info[0] or {}
    except Exception:
        log.debug("Failed to fetch VK user %s", peer_id, exc_info=True)
    return {}


def _auto_link_or_onboard(peer_id: int, vk) -> None:
    vk_user = _fetch_vk_user(peer_id, vk)
    last_name = (vk_user.get("last_name") or "").strip()
    first_name = (vk_user.get("first_name") or "").strip()

    if last_name:
        candidates = _search_clients(last_name, require_full_match=True, first_name=first_name)
        if len(candidates) == 1:
            _link_client(peer_id, candidates[0], vk)
            return
        if len(candidates) > 1:
            _present_candidates(peer_id, candidates, vk, prompt="Нашёл несколько совпадений:")
            return

    # No match — create new client using VK name
    if not first_name and not last_name:
        _send_text(vk, peer_id, "Не удалось прочитать имя и фамилию в профиле VK. Напишите фамилию.")
        state = _get_state(peer_id)
        state["mode"] = "link_search"
        return

    _start_new_client_onboarding(peer_id, vk)


def _link_client_by_id(peer_id: int, client_id: int, vk) -> None:
    try:
        client = client_repository.get_client(client_id)
    except Exception:
        log.exception("Failed to load client %s for linking", client_id)
        _send_text(vk, peer_id, "Не удалось загрузить анкету.")
        return

    if not client:
        _send_text(vk, peer_id, "Анкета не найдена.")
        return

    _link_client(peer_id, client, vk)


def _start_new_client_onboarding(peer_id: int, vk) -> None:
    vk_user = _fetch_vk_user(peer_id, vk)
    first_name = (vk_user.get("first_name") or "").strip() or None
    last_name = (vk_user.get("last_name") or "").strip() or None
    if not first_name and not last_name:
        _send_text(vk, peer_id, "Не удалось прочитать имя и фамилию в профиле VK. Укажите сначала фамилию.")
        state = _get_state(peer_id)
        state["mode"] = "link_search"
        return

    state = _get_state(peer_id)
    state["mode"] = "new_client"
    state["new_client_data"] = {"first_name": first_name, "last_name": last_name}
    state["new_client_step"] = 0
    _send_text(vk, peer_id, "Создаём новую анкету. Идём по шагам.")
    _send_onboarding_prompt(peer_id, vk, step_index=0)


def _handle_new_client_input(peer_id: int, text: str, vk) -> None:
    state = _get_state(peer_id)
    step_index = state.get("new_client_step", 0)
    data = state.get("new_client_data") or {}

    def _advance() -> None:
        state["new_client_step"] = step_index + 1
        if step_index + 1 < len(_NEW_CLIENT_STEPS):
            _send_onboarding_prompt(peer_id, vk, step_index=step_index + 1)
        else:
            _finalize_new_client(peer_id, vk)

    if step_index >= len(_NEW_CLIENT_STEPS):
        _finalize_new_client(peer_id, vk)
        return

    key = _NEW_CLIENT_STEPS[step_index]["key"]
    text_clean = (text or "").strip()

    if key in {"weight", "height", "ftp"}:
        if text_clean.lower() in {"пропустить", "skip", ""}:
            data[key] = None
            _advance()
            return
        try:
            value = float(text_clean.replace(",", "."))
            if value <= 0:
                raise ValueError
            data[key] = value
            _advance()
        except Exception:
            _send_text(vk, peer_id, "Введите положительное число или «Пропустить».")
        return

    if key == "pedals":
        choice = _normalize_pedals_choice(text_clean)
        if choice == "SKIP":
            data[key] = None
            _advance()
            return
        if choice is None:
            _send_onboarding_prompt(peer_id, vk, step_index=step_index)
            return
        data[key] = choice
        _advance()
        return

    if key == "goal":
        data[key] = text_clean or None
        _advance()
        return

    _advance()


def _finalize_new_client(peer_id: int, vk) -> None:
    state = _get_state(peer_id)
    data = state.get("new_client_data") or {}
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    try:
        client = client_repository.create_client(
            first_name=first_name,
            last_name=last_name,
            weight=data.get("weight"),
            height=data.get("height"),
            ftp=data.get("ftp"),
            pedals=data.get("pedals"),
            goal=data.get("goal"),
        )
    except Exception:
        log.exception("Failed to create client for peer %s", peer_id)
        _send_text(vk, peer_id, "Не удалось создать анкету. Попробуйте позже.")
        state.pop("mode", None)
        return

    # Persist notification about new client
    try:
        booking_notifications.notify_client_created(
            booking_notifications.ClientCreatedNotification(
                client_id=client.get("id") if isinstance(client.get("id"), int) else None,
                client_name=format_client_name(client),
                source="vkbot",
                payload={"peer_id": peer_id},
            )
        )
    except Exception:
        log.debug("Failed to record client_created notification", exc_info=True)

    _link_client(peer_id, client, vk)


def _send_onboarding_prompt(peer_id: int, vk, *, step_index: int) -> None:
    if step_index >= len(_NEW_CLIENT_STEPS):
        return
    prompt = _NEW_CLIENT_STEPS[step_index]["prompt"]
    key = _NEW_CLIENT_STEPS[step_index]["key"]

    keyboard = None
    if key == "ftp":
        kb = VkKeyboard(inline=False, one_time=True)
        kb.add_button("Пропустить", color=VkKeyboardColor.SECONDARY, payload={"action": "skip"})
        keyboard = kb.get_keyboard()
    elif key == "pedals":
        kb = VkKeyboard(inline=False, one_time=True)
        for idx, (label, _) in enumerate(_PEDAL_OPTIONS):
            kb.add_button(label, color=VkKeyboardColor.PRIMARY, payload={"action": "pedals"})
            if idx % 2 == 1:
                kb.add_line()
        keyboard = kb.get_keyboard()

    _send_text(vk, peer_id, prompt, keyboard=keyboard)


def _normalize_pedals_choice(text: str) -> Optional[str]:
    cleaned = (text or "").strip().lower()
    if cleaned in {"пропустить", "skip", ""}:
        return "SKIP"
    for label, value in _PEDAL_OPTIONS:
        if cleaned in label.lower() or cleaned == value.lower():
            return value
    return cleaned or None



def _filter_slots() -> List[Dict[str, Any]]:
    try:
        return booking_service.list_bookable_slots_for_horizon(now=_local_now())
    except Exception:
        log.exception("Failed to load available slots")
        return []


def _start_booking(peer_id: int, user_id: int, vk) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    slots = _filter_slots()
    if not slots:
        _send_text(vk, peer_id, "Свободных слотов пока нет. Попробуйте позже.")
        return

    # Group by day
    day_slots = booking_service.group_slots_by_day(slots)
    if not day_slots:
        _send_text(vk, peer_id, "Свободных слотов пока нет. Попробуйте позже.")
        return

    state = _get_state(peer_id)
    state["day_map"] = {day.isoformat(): slots for day, slots in day_slots}

    # If only one day, show slots right away
    if len(day_slots) == 1:
        day, _ = day_slots[0]
        _show_slots_for_day(peer_id, user_id, day.isoformat(), vk)
        return

    # Store day map in state for later validation
    state["mode"] = "day_select"

    keyboard = build_day_keyboard(day_slots[:5], add_back=True)
    _send_text(vk, peer_id, "Выберите день:", keyboard=keyboard)


def _group_slots_by_day(slots: List[Dict[str, Any]]) -> List[tuple[date, List[Dict[str, Any]]]]:
    return booking_service.group_slots_by_day(slots)


def _show_slots_for_day(peer_id: int, user_id: int, date_str: Optional[str], vk, *, page: int = 0) -> None:
    if not date_str:
        _send_text(vk, peer_id, "Не смог понять дату. Попробуйте снова.")
        return

    state = _get_state(peer_id)
    day_map = state.get("day_map") or {}
    slots_for_day = day_map.get(date_str)
    if not slots_for_day:
        try:
            # Reload fresh slots to refresh day map
            slots = _filter_slots()
            day_slots = _group_slots_by_day(slots)
            state["day_map"] = {day.isoformat(): slots for day, slots in day_slots}
            slots_for_day = state["day_map"].get(date_str)
        except Exception:
            log.exception("Failed to reload slots for day %s", date_str)
            slots_for_day = None

    if not slots_for_day:
        _send_text(vk, peer_id, "Свободных слотов на эту дату нет. Выберите другой день.")
        keyboard = build_day_keyboard(list(state.get("day_map", {}).items())[:6]) if state.get("day_map") else None
        _send_text(vk, peer_id, "Выберите день:", keyboard=keyboard)
        return

    state["mode"] = "slot_select"
    state["current_date"] = date_str
    # cache slots map for text matching
    state["slots_map"] = {slot["id"]: slot for slot in slots_for_day if isinstance(slot.get("id"), int)}
    current_page = _send_slots_keyboard(peer_id, slots_for_day, vk, date_iso=date_str, page=page)
    state["slots_page"] = current_page


def _send_slots_keyboard(peer_id: int, slots: List[Dict[str, Any]], vk, *, date_iso: str, page: int = 0) -> int:
    per_page = 5
    total_pages = (len(slots) + per_page - 1) // per_page or 1
    current_page = max(0, min(page, total_pages - 1))
    start_idx = current_page * per_page
    limited_slots = slots[start_idx : start_idx + per_page]
    try:
        keyboard = build_slots_keyboard(
            limited_slots,
            has_prev=current_page > 0,
            has_next=current_page < total_pages - 1,
            date_iso=date_iso,
            page=current_page,
        )
        _send_text(
            vk,
            peer_id,
            f"Выберите слот для бронирования (стр. {current_page + 1}/{total_pages}):",
            keyboard=keyboard,
        )
    except Exception:
        log.exception("Failed to build slots keyboard, sending text list")
        # Fallback to text list
        lines = [f"Свободные слоты (стр. {current_page + 1}/{total_pages}):"]
        for idx, slot in enumerate(limited_slots, 1):
            lines.append(f"{idx}) {format_slot_label(slot)} (id {slot.get('id')})")
        _send_text(vk, peer_id, "\n".join(lines))
    return current_page


def _confirm_slot(peer_id: int, user_id: int, slot_id: int, vk) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        _send_text(vk, peer_id, "Не удалось определить вашу анкету.")
        return

    try:
        booking_result = booking_service.book_slot(slot_id, client_id, format_client_name(client), source="vkbot")
    except booking_service.AlreadyBooked:
        _send_text(vk, peer_id, "У вас уже есть запись на этот слот.")
        return
    except booking_service.NoFreePlace:
        _send_text(vk, peer_id, "Свободных мест не осталось. Попробуйте другой слот.")
        return
    except booking_service.SlotNotFound:
        _send_text(vk, peer_id, "Слот не найден.")
        return
    except booking_service.BookingError:
        _send_text(vk, peer_id, "Не удалось записать. Попробуйте другой слот.")
        return

    client_name = format_client_name(client)
    confirmation = _format_booking_confirmation(client_name, booking_result.slot, booking_result.reservation)
    _send_text(vk, peer_id, confirmation, keyboard=build_inline_keyboard())
    try:
        slot_date = booking_result.slot.get("slot_date")
        start_time = booking_result.slot.get("start_time")
        stand_label = booking_result.reservation.get("stand_code") or booking_result.reservation.get("stand_id")
        session_kind = booking_result.slot.get("session_kind") or booking_result.slot.get("slot_type") or "self_service"
        bike_title = (booking_result.reservation.get("bike_title") or "").strip()
        bike_owner = (booking_result.reservation.get("bike_owner") or "").strip()
        bike_label = None
        if bike_title and bike_owner:
            bike_label = f"{bike_title} ({bike_owner})"
        elif bike_title or bike_owner:
            bike_label = bike_title or bike_owner
        message = booking_notifications.format_booking_created_message(
            client_name=client_name,
            slot_date=slot_date if isinstance(slot_date, date) else None,
            start_time=start_time if isinstance(start_time, time) else None,
            stand_label=str(stand_label) if stand_label else None,
            bike_label=bike_label,
            session_kind=session_kind,
            instructor_name=(booking_result.slot.get("instructor_name") or "").strip() or None,
            source="vkbot",
        )
        _fanout_telegram_notification(
            text=message,
            instructor_id=booking_result.slot.get("instructor_id"),
        )
        booking_notifications.notify_booking_created(
            booking_notifications.BookingNotification(
                client_id=client_id,
                client_name=client_name,
                slot_date=slot_date if isinstance(slot_date, date) else None,
                start_time=start_time if isinstance(start_time, time) else None,
                slot_label=(booking_result.slot.get("label") or "").strip() or None,
                stand_label=str(stand_label) if stand_label else None,
                bike_label=bike_label,
                source="vkbot",
                message_text=message,
                payload={"slot_id": booking_result.slot.get("id")},
            )
        )
    except Exception:
        log.debug("Failed to record booking notification", exc_info=True)


def _format_booking_confirmation(client_name: str, slot: Dict[str, Any], reservation: Dict[str, Any]) -> str:
    slot_date = slot.get("slot_date")
    start_time_val = slot.get("start_time")
    label = slot.get("label") or ""
    when = ""
    if isinstance(slot_date, date):
        when = slot_date.strftime("%d.%m.%Y")
    if isinstance(start_time_val, time):
        when = f"{when} в {start_time_val.strftime('%H:%M')}".strip()

    stand_label = reservation.get("stand_code") or reservation.get("stand_id") or "место"
    lines = [
        "✅ Запись подтверждена!",
        f"{client_name}, вы записаны {when}.",
        f"Станок: {stand_label}",
    ]
    if label:
        lines.append(str(label))
    return "\n".join(lines)


def _show_profile(peer_id: int, user_id: int, vk, greeting: str) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    client_name = format_client_name(client)
    text = f"{greeting}\nВаша анкета: {client_name}\nЧтобы записаться, нажмите «Забронировать»."
    _send_text(vk, peer_id, text, keyboard=build_inline_keyboard())


def _show_cancellable(peer_id: int, user_id: int, vk, *, page: int = 0) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        _send_text(vk, peer_id, "Не удалось определить вашу анкету.")
        return

    now_naive = _local_now().replace(tzinfo=None)
    try:
        reservations = schedule_repository.list_future_reservations_for_client(client_id, now_naive)
    except Exception:
        log.exception("Failed to list future reservations for client %s", client_id)
        _send_text(vk, peer_id, "Не удалось получить список записей.")
        return

    if not reservations:
        _send_text(vk, peer_id, "У вас нет будущих записей.")
        return

    state = _get_state(peer_id)
    state["mode"] = "cancel_select"
    state["cancel_map"] = {res["id"]: res for res in reservations if isinstance(res.get("id"), int)}
    state["cancel_list"] = reservations

    total_pages = max(1, (len(reservations) + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_items = reservations[start:end]

    keyboard = VkKeyboard(inline=True)
    for res in page_items:
        res_id = res.get("id")
        if not isinstance(res_id, int):
            continue
        keyboard.add_button(
            format_reservation_label(res),
            color=VkKeyboardColor.PRIMARY,
            payload={"action": "cancel_reservation", "reservation_id": res_id},
        )
        keyboard.add_line()

    if total_pages > 1:
        if page > 0:
            keyboard.add_button("←", color=VkKeyboardColor.SECONDARY, payload={"action": "cancel_page", "page": page - 1})
        if page < total_pages - 1:
            keyboard.add_button("→", color=VkKeyboardColor.SECONDARY, payload={"action": "cancel_page", "page": page + 1})
    keyboard.add_button("Закрыть", color=VkKeyboardColor.NEGATIVE, payload={"action": "close"})
    _send_text(vk, peer_id, "Выберите запись для отмены:", keyboard=keyboard.get_keyboard())


def _show_future_bookings(peer_id: int, user_id: int, vk, *, page: int = 0) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        _send_text(vk, peer_id, "Не удалось определить вашу анкету.")
        return

    now_naive = _local_now().replace(tzinfo=None)
    try:
        reservations = schedule_repository.list_future_reservations_for_client(client_id, now_naive)
    except Exception:
        log.exception("Failed to list future reservations for client %s", client_id)
        _send_text(vk, peer_id, "Не удалось получить список записей.")
        return

    if not reservations:
        _send_text(vk, peer_id, "У вас нет будущих записей.")
        return

    state = _get_state(peer_id)
    state["mode"] = "booking_list"
    state["booking_map"] = {res["id"]: res for res in reservations if isinstance(res.get("id"), int)}
    state["booking_list"] = reservations

    total_pages = max(1, (len(reservations) + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_items = reservations[start:end]

    keyboard = VkKeyboard(inline=True)
    for idx, res in enumerate(page_items):
        res_id = res.get("id")
        if not isinstance(res_id, int):
            continue
        keyboard.add_button(
            format_reservation_label(res),
            color=VkKeyboardColor.PRIMARY,
            payload={"action": "booking_details", "reservation_id": res_id},
        )
        if idx != len(page_items) - 1:
            keyboard.add_line()
    if total_pages > 1:
        if page > 0:
            keyboard.add_button("←", color=VkKeyboardColor.SECONDARY, payload={"action": "booking_page", "page": page - 1})
        if page < total_pages - 1:
            keyboard.add_button("→", color=VkKeyboardColor.SECONDARY, payload={"action": "booking_page", "page": page + 1})
    keyboard.add_button("Закрыть", color=VkKeyboardColor.NEGATIVE, payload={"action": "close"})
    _send_text(vk, peer_id, "Ваши будущие записи:", keyboard=keyboard.get_keyboard())


def _cancel_reservation(peer_id: int, user_id: int, reservation_id: int, vk) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        _send_text(vk, peer_id, "Не удалось определить вашу анкету.")
        return

    try:
        reservation = schedule_repository.get_reservation(reservation_id)
    except Exception:
        log.exception("Failed to fetch reservation %s", reservation_id)
        _send_text(vk, peer_id, "Не удалось получить запись.")
        return

    if not reservation:
        _send_text(vk, peer_id, "Запись не найдена.")
        return
    if reservation.get("client_id") != client_id:
        _send_text(vk, peer_id, "Эта запись не принадлежит вам.")
        return

    notes = f"cancelled via vkbot by client {client_id}"
    try:
        updated = schedule_repository.update_reservation(
            reservation_id,
            client_id=None,
            client_name=None,
            status="available",
            source="vkbot",
            notes=notes,
        )
    except Exception:
        log.exception("Failed to cancel reservation %s", reservation_id)
        _send_text(vk, peer_id, "Не удалось отменить запись.")
        return

    if not updated:
        _send_text(vk, peer_id, "Не удалось отменить запись.")
        return

    _send_text(vk, peer_id, "Запись отменена. Можете выбрать другой слот.", keyboard=build_inline_keyboard())
    try:
        slot_id = reservation.get("slot_id")
        slot = None
        if isinstance(slot_id, int):
            slot = schedule_repository.get_slot_with_reservations(slot_id)
        slot_date = slot.get("slot_date") if slot else reservation.get("slot_date")
        start_time = slot.get("start_time") if slot else reservation.get("start_time")
        stand_label = reservation.get("stand_code") or reservation.get("stand_id")
        message = booking_notifications.format_booking_cancelled_message(
            client_name=format_client_name(client),
            slot_date=slot_date if isinstance(slot_date, date) else None,
            start_time=start_time if isinstance(start_time, time) else None,
            stand_label=str(stand_label) if stand_label else None,
            source="vkbot",
        )
        booking_notifications.notify_booking_cancelled(
            booking_notifications.BookingNotification(
                client_id=client_id,
                client_name=format_client_name(client),
                slot_date=slot_date if isinstance(slot_date, date) else None,
                start_time=start_time if isinstance(start_time, time) else None,
                slot_label=(slot.get("label") if slot else reservation.get("label")) or None,
                stand_label=str(stand_label) if stand_label else None,
                bike_label=None,
                source="vkbot",
                message_text=message,
                payload={"slot_id": slot_id, "reservation_id": reservation_id, "instructor_id": (slot or reservation).get("instructor_id")},
            )
        )
        _fanout_telegram_notification(
            text=message,
            instructor_id=(slot or reservation).get("instructor_id"),
        )
    except Exception:
        log.debug("Failed to record cancellation notification", exc_info=True)
    # Clear cancellation state
    state = _get_state(peer_id)
    state.pop("cancel_map", None)
    if state.get("mode") == "cancel_select":
        state.pop("mode", None)


def _show_booking_details(peer_id: int, user_id: int, reservation_id: int, vk) -> None:
    link, client = _get_linked_client(peer_id)
    if not link or not client:
        _prompt_link(peer_id, vk)
        return

    client_id = client.get("id")
    if not isinstance(client_id, int):
        _send_text(vk, peer_id, "Не удалось определить вашу анкету.")
        return

    try:
        reservation = schedule_repository.get_reservation(reservation_id)
    except Exception:
        log.exception("Failed to fetch reservation %s", reservation_id)
        _send_text(vk, peer_id, "Не удалось получить запись.")
        return

    if not reservation:
        _send_text(vk, peer_id, "Запись не найдена.")
        return
    if reservation.get("client_id") != client_id:
        _send_text(vk, peer_id, "Эта запись не принадлежит вам.")
        return

    # Enrich with slot info if available
    slot_id = reservation.get("slot_id")
    if isinstance(slot_id, int):
        try:
            slot = schedule_repository.get_slot_with_reservations(slot_id)
            if slot:
                reservation = {**slot, **reservation}
        except Exception:
            log.debug("Failed to enrich reservation %s with slot %s", reservation_id, slot_id, exc_info=True)

    details = format_reservation_details(reservation, format_client_name(client))
    state = _get_state(peer_id)
    state["mode"] = "booking_details"
    keyboard = VkKeyboard(inline=True)
    keyboard.add_button("Отменить запись", color=VkKeyboardColor.NEGATIVE, payload={"action": "cancel_reservation", "reservation_id": reservation_id})
    keyboard.add_line()
    keyboard.add_button("Закрыть", color=VkKeyboardColor.SECONDARY, payload={"action": "close"})
    _send_text(vk, peer_id, details, keyboard=keyboard.get_keyboard())
def _fanout_telegram_notification(*, text: str, instructor_id: Optional[int]) -> None:
    """Send notification text to Telegram admins respecting instructor filtering."""
    admin_ids = admin_notifications.resolve_admin_chat_ids(instructor_id=instructor_id)
    if not admin_ids:
        return
    delivered = admin_notifications.notify_admins_blocking(text, admin_ids=admin_ids)
    for admin_id in admin_ids:
        if admin_id not in delivered:
            log.debug("Failed to send admin notification to %s", admin_id)
