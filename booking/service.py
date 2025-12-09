"""Core booking operations shared between VK bot, Telegram bot, and future clients."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from repositories import schedule_repository

log = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Europe/Moscow")
BOOKING_CUTOFF = timedelta(0)


def local_now() -> datetime:
    """Return current time in the local timezone used for scheduling."""
    return datetime.now(tz=LOCAL_TZ)


def _to_local_naive(value: datetime) -> datetime:
    """Convert a datetime to naive local time (repository expects naive)."""
    if value.tzinfo is None:
        return value
    return value.astimezone(LOCAL_TZ).replace(tzinfo=None)


def slot_start_datetime(slot: Dict[str, Any]) -> Optional[datetime]:
    """Combine slot_date and start_time into a timezone-aware datetime."""
    slot_date = slot.get("slot_date")
    start_time_val = slot.get("start_time")
    if isinstance(slot_date, date) and isinstance(start_time_val, time):
        return datetime.combine(slot_date, start_time_val, tzinfo=LOCAL_TZ)
    return None


def _filter_bookable_slots(
    slots: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    booking_cutoff: timedelta = BOOKING_CUTOFF,
) -> List[Dict[str, Any]]:
    """Remove slots that start too soon or have no free seats."""
    now = now or local_now()
    filtered: List[Dict[str, Any]] = []
    for slot in slots:
        start_dt = slot_start_datetime(slot)
        if start_dt is None:
            continue
        if start_dt - now < booking_cutoff:
            continue
        free_count = slot.get("free_count")
        if free_count is not None and free_count <= 0:
            continue
        filtered.append(slot)
    return filtered


def list_bookable_slots_between(
    start: datetime,
    end: datetime,
    *,
    now: Optional[datetime] = None,
    booking_cutoff: timedelta = BOOKING_CUTOFF,
) -> List[Dict[str, Any]]:
    """Load available slots in a time window and filter by cutoff."""
    now = now or local_now()
    try:
        slots_raw = schedule_repository.list_available_slots(
            _to_local_naive(start),
            _to_local_naive(end),
        )
    except Exception:
        log.exception("Failed to load available slots between %s and %s", start, end)
        return []
    return _filter_bookable_slots(slots_raw, now=now, booking_cutoff=booking_cutoff)


def list_bookable_slots_for_horizon(
    *,
    now: Optional[datetime] = None,
    horizon_days: int = 21,
    booking_cutoff: timedelta = BOOKING_CUTOFF,
) -> List[Dict[str, Any]]:
    """Load available slots for a horizon window, respecting booking cutoff."""
    now = now or local_now()
    horizon_end = now + timedelta(days=horizon_days)
    if horizon_end <= now + booking_cutoff:
        horizon_end = now + booking_cutoff + timedelta(days=1)
    return list_bookable_slots_between(now, horizon_end, now=now, booking_cutoff=booking_cutoff)


def group_slots_by_day(slots: List[Dict[str, Any]]) -> List[Tuple[date, List[Dict[str, Any]]]]:
    """Group slots by date and sort them."""
    grouped: Dict[date, List[Dict[str, Any]]] = {}
    for slot in slots:
        slot_date = slot.get("slot_date")
        if isinstance(slot_date, date):
            grouped.setdefault(slot_date, []).append(slot)
    result: List[Tuple[date, List[Dict[str, Any]]]] = []
    for day in sorted(grouped.keys()):
        sorted_slots = sorted(
            grouped[day],
            key=lambda s: (
                s.get("start_time") if isinstance(s.get("start_time"), time) else time.min,
                s.get("id") or 0,
            ),
        )
        result.append((day, sorted_slots))
    return result


def has_existing_booking(client_id: int, slot_id: int) -> bool:
    """Check if client already has a booking for the given slot."""
    now_naive = local_now().replace(tzinfo=None)
    try:
        reservations = schedule_repository.list_future_reservations_for_client(client_id, now_naive)
    except Exception:
        log.exception("Failed to check existing bookings for client %s", client_id)
        return False

    for res in reservations:
        if res.get("slot_id") != slot_id:
            continue
        status = (res.get("status") or "").lower()
        if status not in {"cancelled", "legacy", "blocked"}:
            return True
    return False


def pick_available_reservation(slot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first available reservation from a slot."""
    for res in slot.get("reservations") or []:
        if (res.get("status") or "").lower() == "available":
            return res
    return None


class BookingError(Exception):
    """Base booking error."""


class SlotNotFound(BookingError):
    """Slot could not be loaded."""


class NoFreePlace(BookingError):
    """No available reservations left."""


class AlreadyBooked(BookingError):
    """Client already has a booking for this slot."""


class BookingFailed(BookingError):
    """Failed to persist the booking."""


@dataclass
class BookingResult:
    slot: Dict[str, Any]
    reservation: Dict[str, Any]
    booked_row: Dict[str, Any]


def book_slot(
    slot_id: int,
    client_id: int,
    client_name: str,
    *,
    source: Optional[str] = None,
) -> BookingResult:
    """Book the first available reservation for a slot."""
    try:
        slot = schedule_repository.get_slot_with_reservations(slot_id)
    except Exception:
        log.exception("Failed to load slot %s", slot_id)
        raise SlotNotFound()

    if not slot:
        raise SlotNotFound()

    if has_existing_booking(client_id, slot_id):
        raise AlreadyBooked()

    reservation = pick_available_reservation(slot)
    if not reservation or not isinstance(reservation.get("id"), int):
        raise NoFreePlace()

    try:
        booked = schedule_repository.book_available_reservation(
            reservation["id"],
            client_id=client_id,
            client_name=client_name,
            source=source or "booking_service",
        )
    except Exception:
        log.exception("Failed to book reservation %s for client %s", reservation["id"], client_id)
        raise BookingFailed()

    if not booked:
        raise BookingFailed()

    return BookingResult(slot=slot, reservation=reservation, booked_row=booked)
