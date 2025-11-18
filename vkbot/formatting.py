"""Formatting helpers for VK bot messages and labels."""
from __future__ import annotations

from datetime import date, time
from typing import Any, Dict

WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def format_day_display(day: date) -> str:
    weekday = WEEKDAYS_RU[day.weekday()] if 0 <= day.weekday() < len(WEEKDAYS_RU) else day.strftime("%a")
    return f"{day.strftime('%d.%m')} ({weekday})"


def format_slot_label(slot: Dict[str, Any]) -> str:
    slot_date = slot.get("slot_date")
    start_time_val = slot.get("start_time")
    free_count = slot.get("free_count")
    total_count = slot.get("total_count")
    label = slot.get("label")
    date_part = format_day_display(slot_date) if isinstance(slot_date, date) else "Дата?"
    time_part = start_time_val.strftime("%H:%M") if isinstance(start_time_val, time) else "время?"
    parts = [f"{date_part} {time_part}"]
    if label:
        parts.append(str(label))
    if free_count is not None and total_count is not None:
        parts.append(f"{free_count}/{total_count} мест")
    return " · ".join(parts)


def format_client_name(client: Dict[str, Any]) -> str:
    for key in ("full_name", "last_name", "first_name"):
        val = (client.get(key) or "").strip()
        if val:
            return val
    return "Клиент"


def format_reservation_label(res: Dict[str, Any]) -> str:
    slot_date = res.get("slot_date")
    start_time_val = res.get("start_time")
    label = res.get("label") or ""
    date_part = slot_date.strftime("%d.%m") if isinstance(slot_date, date) else "Дата?"
    time_part = start_time_val.strftime("%H:%M") if isinstance(start_time_val, time) else "время?"
    pieces = [f"{date_part} {time_part}"]
    if label:
        pieces.append(str(label))
    return " · ".join(pieces)


def format_reservation_details(res: Dict[str, Any], client_name: str) -> str:
    slot_date = res.get("slot_date")
    start_time_val = res.get("start_time")
    end_time_val = res.get("end_time")
    label = res.get("label") or ""
    instructor = (res.get("instructor_name") or "").strip()
    stand_label = res.get("stand_display_name") or res.get("stand_title") or res.get("stand_code") or "место"

    when = ""
    if isinstance(slot_date, date):
        when = slot_date.strftime("%d.%m.%Y")
    if isinstance(start_time_val, time):
        when = f"{when} {start_time_val.strftime('%H:%M')}".strip()
    if isinstance(end_time_val, time):
        when = f"{when}–{end_time_val.strftime('%H:%M')}"

    lines = [
        "📌 Ваша запись",
        f"{client_name}",
        f"Когда: {when}" if when else "Когда: скоро",
        f"Станок: {stand_label}",
    ]
    if label:
        lines.append(f"Слот: {label}")
    if instructor:
        lines.append(f"Инструктор: {instructor}")
    return "\n".join(lines)
