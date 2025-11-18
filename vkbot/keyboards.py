"""VK inline keyboard builders."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Tuple

from vk_api.keyboard import VkKeyboard, VkKeyboardColor

from .formatting import format_day_display, format_slot_label, format_reservation_label


def build_inline_keyboard() -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_button("Забронировать", color=VkKeyboardColor.PRIMARY, payload={"action": "book"})
    keyboard.add_line()
    keyboard.add_button("Мои брони", color=VkKeyboardColor.PRIMARY, payload={"action": "my_bookings"})
    keyboard.add_line()
    keyboard.add_button("Отменить бронь", color=VkKeyboardColor.NEGATIVE, payload={"action": "cancel"})
    keyboard.add_line()
    keyboard.add_openlink_button("Расписание (текущая)", link="https://clc.to/krutilka_current_week")
    keyboard.add_openlink_button("Расписание (следующая)", link="https://clc.to/krutilka_next_week")
    keyboard.add_line()
    keyboard.add_button("Как добраться", color=VkKeyboardColor.SECONDARY, payload={"action": "how_to_get"})
    keyboard.add_button("Что взять", color=VkKeyboardColor.SECONDARY, payload={"action": "what_to_bring"})
    return keyboard.get_keyboard()


def build_slots_keyboard(slots: List[Dict[str, Any]]) -> str:
    keyboard = VkKeyboard(inline=True)
    for idx, slot in enumerate(slots):
        slot_id = slot.get("id")
        if not isinstance(slot_id, int):
            continue
        keyboard.add_button(
            format_slot_label(slot),
            color=VkKeyboardColor.PRIMARY,
            payload={"action": "slot", "slot_id": slot_id},
        )
        if idx != len(slots) - 1:
            keyboard.add_line()
    keyboard.add_line()
    keyboard.add_button("Отмена", color=VkKeyboardColor.NEGATIVE, payload={"action": "cancel"})
    return keyboard.get_keyboard()


def build_day_keyboard(day_slots: List[Tuple[date, List[Dict[str, Any]]]], *, add_back: bool = False) -> str:
    keyboard = VkKeyboard(inline=True)
    for idx, (day, _) in enumerate(day_slots):
        label = format_day_display(day)
        keyboard.add_button(label, color=VkKeyboardColor.PRIMARY, payload={"action": "book_day", "date": day.isoformat()})
        if idx != len(day_slots) - 1:
            keyboard.add_line()
    if add_back:
        keyboard.add_line()
        keyboard.add_button("← Назад", color=VkKeyboardColor.SECONDARY, payload={"action": "close"})
    return keyboard.get_keyboard()


def build_cancel_keyboard(reservations: List[Dict[str, Any]]) -> str:
    keyboard = VkKeyboard(inline=True)
    for res in reservations:
        res_id = res.get("id")
        if not isinstance(res_id, int):
            continue
        keyboard.add_button(
            format_reservation_label(res),
            color=VkKeyboardColor.PRIMARY,
            payload={"action": "cancel_reservation", "reservation_id": res_id},
        )
        keyboard.add_line()
    keyboard.add_button("Закрыть", color=VkKeyboardColor.NEGATIVE, payload={"action": "close"})
    return keyboard.get_keyboard()
