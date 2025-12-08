"""Notification helper for booking events: persist + format for admin channels."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from typing import Any, Callable, Dict, Iterable, Optional

from repositories import booking_notifications_repository


@dataclass
class BookingNotification:
    client_id: Optional[int]
    client_name: Optional[str]
    slot_date: Optional[date]
    start_time: Optional[time]
    slot_label: Optional[str]
    stand_label: Optional[str]
    bike_label: Optional[str]
    source: Optional[str]
    message_text: str
    payload: Optional[Dict[str, Any]] = None


@dataclass
class ClientCreatedNotification:
    client_id: Optional[int]
    client_name: Optional[str]
    source: Optional[str]
    payload: Optional[Dict[str, Any]] = None


def format_booking_created_message(
    *,
    client_name: str,
    slot_date: Optional[date],
    start_time: Optional[time],
    stand_label: Optional[str],
    bike_label: Optional[str],
    session_kind: Optional[str] = None,
    instructor_name: Optional[str] = None,
    source: Optional[str] = None,
) -> str:
    """Format a human-readable booking notification."""
    date_part = slot_date.strftime("%d.%m.%Y") if isinstance(slot_date, date) else "дата?"
    time_part = start_time.strftime("%H:%M") if isinstance(start_time, time) else "время?"
    session_note = ""
    if session_kind == "instructor":
        if instructor_name:
            session_note = f"\n🧑‍🏫 С инструктором: {instructor_name}"
        else:
            session_note = "\n🧑‍🏫 С инструктором (уточняется)"
    elif session_kind:
        session_note = f"\n🔄 Тип: {session_kind}"
    bike_note = f"\n🚲 Велосипед: {bike_label}" if bike_label else ""
    source_note = f"\nИсточник: {source}" if source else ""
    return (
        "🔔 Новая запись!\n\n"
        f"Клиент: {client_name}\n"
        f"Когда: {date_part} в {time_part}{session_note}\n"
        f"🏋️ Станок: {stand_label or 'место'}"
        f"{bike_note}"
        f"{source_note}"
    )


def persist_booking_notification(data: BookingNotification, *, event_type: str = "booking_created") -> Dict[str, Any]:
    """Persist booking notification to DB."""
    return booking_notifications_repository.insert_notification(
        event_type=event_type,
        client_id=data.client_id,
        client_name=data.client_name,
        slot_date=data.slot_date,
        start_time=data.start_time,
        slot_label=data.slot_label,
        stand_label=data.stand_label,
        bike_label=data.bike_label,
        source=data.source,
        message_text=data.message_text,
        payload=data.payload,
    )


def notify_booking_created(
    data: BookingNotification,
    *,
    admin_senders: Optional[Iterable[Callable[[str], Any]]] = None,
) -> Dict[str, Any]:
    """Persist notification and fan out to admin senders (if provided)."""
    row = persist_booking_notification(data, event_type="booking_created")
    for sender in admin_senders or []:
        try:
            sender(data.message_text)
        except Exception:
            # swallow errors to avoid breaking caller; logging expected at caller side
            pass
    return row


def format_client_created_message(*, client_name: str, source: Optional[str] = None) -> str:
    source_note = f"\nИсточник: {source}" if source else ""
    return f"🆕 Новая анкета\nКлиент: {client_name}{source_note}"


def notify_client_created(
    data: ClientCreatedNotification,
    *,
    admin_senders: Optional[Iterable[Callable[[str], Any]]] = None,
) -> Dict[str, Any]:
    """Persist client-created notification and optionally fan out."""
    message = format_client_created_message(client_name=data.client_name or "Клиент", source=data.source)
    row = booking_notifications_repository.insert_notification(
        event_type="client_created",
        client_id=data.client_id,
        client_name=data.client_name,
        slot_date=None,
        start_time=None,
        slot_label=None,
        stand_label=None,
        bike_label=None,
        source=data.source,
        message_text=message,
        payload=data.payload,
    )
    for sender in admin_senders or []:
        try:
            sender(message)
        except Exception:
            pass
    return row


def format_booking_cancelled_message(
    *,
    client_name: str,
    slot_date: Optional[date],
    start_time: Optional[time],
    stand_label: Optional[str],
    source: Optional[str] = None,
) -> str:
    date_part = slot_date.strftime("%d.%m.%Y") if isinstance(slot_date, date) else "дата?"
    time_part = start_time.strftime("%H:%M") if isinstance(start_time, time) else "время?"
    source_note = f"\nИсточник: {source}" if source else ""
    return (
        "⚠️ Отмена записи\n\n"
        f"Клиент: {client_name}\n"
        f"Когда: {date_part} в {time_part}\n"
        f"🏋️ Станок: {stand_label or 'место'}"
        f"{source_note}"
    )


def notify_booking_cancelled(
    data: BookingNotification,
    *,
    admin_senders: Optional[Iterable[Callable[[str], Any]]] = None,
) -> Dict[str, Any]:
    """Persist cancellation notification and optionally fan out."""
    row = persist_booking_notification(data, event_type="booking_cancelled")
    for sender in admin_senders or []:
        try:
            sender(data.message_text)
        except Exception:
            pass
    return row
