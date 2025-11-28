"""Schedule management and public schedule pages."""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Optional

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from repositories import (
    client_repository,
    instructors_repository,
    schedule_repository,
    trainers_repository,
)
from ..dependencies import require_admin, require_user
from ..utils.parsing import parse_iso_date as _parse_iso_date, to_float as _to_float
from .schedule_utils import _load_schedule_week_payload, _serialize_reservation, _serialize_slot

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(prefix="/schedule", tags=["schedule"])
public_router = APIRouter()

SCHEDULE_SESSION_KINDS = {"self_service", "instructor", "race"}
SCHEDULE_SESSION_KIND_LABELS = {
    "self_service": "Самокрутка",
    "instructor": "Инструктор",
    "race": "Гонка",
}
RESERVATION_STATUS_ALLOWED = {
    "available",
    "booked",
    "cancelled",
    "legacy",
    "hold",
    "pending",
    "waitlist",
    "blocked",
}
RESERVATION_STATUS_BOOKED = "booked"

WEEKDAY_SHORT_NAMES = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
WEEKDAY_FULL_NAMES = (
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
)


def _reservation_display_entry(reservation: dict) -> dict:
    status = str(reservation.get("status") or "").lower()
    client_name = (reservation.get("client_name") or "").strip()

    if status == "available" or (not client_name and status in {"cancelled"}):
        return {"label": "Свободно", "full_label": "Свободно", "kind": "free"}

    if client_name:
        parts = [part for part in client_name.split() if part]
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[-1]
            base_label = f"{last_name} {first_name}"
        else:
            base_label = client_name
        return {"label": base_label, "full_label": client_name, "kind": "booked"}

    return {"label": "Занято", "full_label": "Занято", "kind": "busy"}


def _serialize_slot_summary(slot: dict) -> dict:
    serialized = {
        "id": slot.get("id"),
        "week_id": slot.get("week_id"),
        "slot_date": slot.get("slot_date"),
        "start_time": slot.get("start_time"),
        "end_time": slot.get("end_time"),
        "label": slot.get("label"),
        "session_kind": slot.get("session_kind"),
        "is_cancelled": slot.get("is_cancelled", False),
        "week_start_date": slot.get("week_start_date"),
    }

    if isinstance(serialized["slot_date"], date):
        serialized["slot_date"] = serialized["slot_date"].isoformat()
    if isinstance(serialized["start_time"], time):
        serialized["start_time"] = serialized["start_time"].strftime("%H:%M")
    if isinstance(serialized["end_time"], time):
        serialized["end_time"] = serialized["end_time"].strftime("%H:%M")
    if isinstance(serialized["week_start_date"], date):
        serialized["week_start_date"] = serialized["week_start_date"].isoformat()

    if "instructor_id" in slot:
        serialized["instructorId"] = slot.get("instructor_id")
    if "instructor_name" in slot:
        serialized["instructorName"] = slot.get("instructor_name")
    return serialized


def _week_start_for_slug(slug: str) -> Optional[date]:
    if not slug:
        return None
    normalized = slug.strip().lower()
    today = date.today()
    current_week_start = today - timedelta(days=today.weekday())
    if normalized == "current_week":
        return current_week_start
    if normalized == "next_week":
        return current_week_start + timedelta(days=7)
    if normalized == "previous_week":
        return current_week_start - timedelta(days=7)

    if normalized.startswith("week_"):
        tail = normalized[5:]
        for fmt in ("%Y_%m_%d", "%d_%m_%Y", "%d_%m_%y"):
            try:
                parsed = datetime.strptime(tail, fmt).date()
                return parsed
            except ValueError:
                continue
    return None


def _format_week_slug(week_start: date | str) -> Optional[str]:
    if isinstance(week_start, str):
        try:
            week_date = datetime.strptime(week_start, "%Y-%m-%d").date()
        except ValueError:
            return None
    elif isinstance(week_start, date):
        week_date = week_start
    else:
        return None
    return f"week_{week_date:%d_%m_%y}"


def _format_week_range_label(week_start: date | str) -> Optional[str]:
    if isinstance(week_start, str):
        try:
            start_date = datetime.strptime(week_start, "%Y-%m-%d").date()
        except ValueError:
            return None
    elif isinstance(week_start, date):
        start_date = week_start
    else:
        return None
    end_date = start_date + timedelta(days=6)
    return f"{start_date:%d.%m.%Y} — {end_date:%d.%m.%Y}"


def _build_day_columns(slots: list[dict], week_start_date: str | date, instructors: list[dict]) -> list[dict]:
    if isinstance(week_start_date, str):
        try:
            start_date = datetime.strptime(week_start_date, "%Y-%m-%d").date()
        except ValueError:
            return []
    elif isinstance(week_start_date, date):
        start_date = week_start_date
    else:
        return []

    instructor_map = {}
    for instructor in instructors or []:
        instructor_id = instructor.get("id")
        if instructor_id is None:
            continue
        instructor_map[instructor_id] = instructor.get("full_name") or instructor.get("name")

    stand_order: dict[int, int] = {}
    for idx, stand in enumerate(
        sorted(
            trainers_repository.list_trainers(),
            key=lambda row: (
                _to_float(row.get("code")) is None,
                _to_float(row.get("code")) if _to_float(row.get("code")) is not None else row.get("code") or "",
                row.get("id") or 0,
            ),
        )
    ):
        if isinstance(stand.get("id"), int):
            stand_order[stand["id"]] = idx

    slots_by_date: dict[str, list[dict]] = {}
    for slot in slots or []:
        key = slot.get("slot_date") or slot.get("slotDate")
        if not key:
            continue
        slots_by_date.setdefault(key, []).append(slot)

    day_columns: list[dict] = []
    for offset in range(7):
        current_date = start_date + timedelta(days=offset)
        iso = current_date.isoformat()
        slot_list = slots_by_date.get(iso, [])
        slot_list = sorted(
            slot_list,
            key=lambda item: (
                item.get("start_time", ""),
                item.get("end_time", ""),
                item.get("id", 0),
            ),
        )

        slot_entries: list[dict] = []
        totals = {"occupied": 0, "free": 0, "slots": len(slot_list)}
        for raw_slot in slot_list:
            reservations = raw_slot.get("reservations") or []
            occupied = sum(1 for res in reservations if (res.get("status") or "").lower() != "available")
            total = len(reservations)
            free = max(total - occupied, 0)
            totals["occupied"] += occupied
            totals["free"] += free

            if stand_order:
                reservations = sorted(
                    reservations,
                    key=lambda res: stand_order.get(res.get("stand_id"), 10_000),
                )
            reservation_rows = [_reservation_display_entry(res) for res in reservations]
            session_kind = (raw_slot.get("session_kind") or "").strip() or raw_slot.get("session_kind")

            instructor_name = raw_slot.get("instructorName")
            if not instructor_name:
                instructor_id = raw_slot.get("instructorId") or raw_slot.get("instructor_id")
                if instructor_id is not None:
                    instructor_name = instructor_map.get(instructor_id)

            raw_label = (raw_slot.get("label") or "").strip()
            display_label = raw_label
            if not display_label:
                display_label = "С инструктором" if session_kind == "instructor" else "Самокрутка"
            meta_label = raw_label or SCHEDULE_SESSION_KIND_LABELS.get(session_kind, "Слот")

            slot_entries.append(
                {
                    "id": raw_slot.get("id"),
                    "start_time": raw_slot.get("start_time"),
                    "end_time": raw_slot.get("end_time"),
                    "label": display_label,
                    "instructor_name": instructor_name,
                    "reservations": reservations,
                    "reservation_rows": reservation_rows,
                    "stats": {"occupied": occupied, "free": free, "total": total},
                    "session_kind": session_kind or raw_slot.get("session_kind"),
                    "meta_label": meta_label,
                }
            )

        day_columns.append(
            {
                "iso": iso,
                "weekday_short": WEEKDAY_SHORT_NAMES[offset],
                "weekday_full": WEEKDAY_FULL_NAMES[offset],
                "label": current_date.strftime("%d.%m"),
                "slots": slot_entries,
                "totals": totals,
            }
        )

    return day_columns


@router.get("/weeks")
def api_schedule_weeks(
    page: int = 1,
    page_size: int = 20,
    user=Depends(require_user),
):
    page = max(page, 1)
    page_size = min(max(page_size, 1), 50)
    offset = (page - 1) * page_size

    rows = schedule_repository.list_weeks(limit=page_size, offset=offset)
    total = schedule_repository.count_weeks()
    total_pages = max(math.ceil(total / page_size), 1)

    return {
        "items": jsonable_encoder(rows),
        "pagination": {
            "page": page,
            "pageSize": page_size,
            "total": total,
            "totalPages": total_pages,
        },
    }


@router.post("/weeks")
async def api_create_schedule_week(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    week_start_date = _parse_iso_date("weekStartDate", payload.get("weekStartDate"))
    title_present = "title" in payload
    raw_title = payload.get("title")
    if title_present:
        if raw_title is None:
            title_value: Optional[str] = None
        elif isinstance(raw_title, str):
            title_value = raw_title.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid title")
    else:
        title_value = None

    notes_present = "notes" in payload
    raw_notes = payload.get("notes")
    if notes_present:
        if raw_notes is None:
            notes_value: Optional[str] = None
        elif isinstance(raw_notes, str):
            notes_value = raw_notes.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid notes")
    else:
        notes_value = None
    copy_from_week_id = payload.get("copyFromWeekId")
    if copy_from_week_id in (None, "", "null"):
        copy_from_week_id = None
    elif isinstance(copy_from_week_id, int):
        pass
    else:
        try:
            copy_from_week_id = int(copy_from_week_id)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid copyFromWeekId")

    replace_raw = payload.get("replace", False)
    if isinstance(replace_raw, str):
        replace_existing = replace_raw.strip().lower() in {"1", "true", "yes", "on"}
    else:
        replace_existing = bool(replace_raw)

    copied_slots = 0
    placeholders = 0
    defaults_created = 0

    existing = schedule_repository.get_week_by_start(week_start_date)
    if existing and not replace_existing and copy_from_week_id is None:
        raise HTTPException(status.HTTP_409_CONFLICT, "Week already exists")

    if existing and replace_existing:
        schedule_repository.clear_week_slots(existing["id"])
        schedule_repository.sync_week_capacity(existing["id"])
        meta_updates: dict[str, object] = {}
        if title_present:
            meta_updates["title"] = title_value
        if notes_present:
            meta_updates["notes"] = notes_value
        if meta_updates:
            schedule_repository.update_week(existing["id"], **meta_updates)
        target_week = schedule_repository.get_week(existing["id"]) or existing
        log.info("schedule: reusing week %s replace_existing=%s", target_week["id"], replace_existing)
    else:
        try:
            target_week = schedule_repository.create_week(
                week_start_date=week_start_date,
                title=title_value,
                notes=notes_value,
                copied_from_week_id=copy_from_week_id if copy_from_week_id else None,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, "Week already exists") from exc
        log.info("schedule: created week %s start=%s", target_week["id"], week_start_date)

    if copy_from_week_id:
        if not schedule_repository.get_week(copy_from_week_id):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Source week not found")
        if replace_existing:
            schedule_repository.clear_week_slots(target_week["id"])
        copied_slots, placeholders = schedule_repository.copy_slots_from_week(
            copy_from_week_id,
            target_week["id"],
        )
        schedule_repository.sync_week_capacity(target_week["id"])
    else:
        try:
            defaults_created = schedule_repository.create_default_slots_for_week(
                target_week["id"],
                force=replace_existing,
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Failed to seed default slots for week %s: %s", target_week["id"], exc)
            defaults_created = 0

    refreshed = schedule_repository.get_week(target_week["id"])
    slots = schedule_repository.list_slots_with_reservations(target_week["id"])
    serialized_slots = [_serialize_slot(slot) for slot in slots]
    trainers_repository.ensure_trainers_table()
    stands_payload = [
        {
            "id": trainer["id"],
            "code": trainer.get("code"),
            "display_name": trainer.get("display_name"),
            "title": trainer.get("title"),
        }
        for trainer in trainers_repository.list_trainers()
    ]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())
    return {
        "week": jsonable_encoder(refreshed),
        "copied": {
            "slots": copied_slots,
            "placeholders": placeholders,
        },
        "defaults": defaults_created,
        "slots": serialized_slots,
        "stands": jsonable_encoder(stands_payload),
        "instructors": instructors_payload,
    }


@router.get("/weeks/{week_id}")
def api_get_schedule_week(week_id: int, user=Depends(require_user)):
    payload = _load_schedule_week_payload(week_id)
    if not payload:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    return payload


@router.patch("/weeks/{week_id}")
async def api_update_schedule_week(week_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    updates: dict[str, object] = {}

    if "title" in payload:
        value = payload.get("title")
        if value is None:
            updates["title"] = None
        elif isinstance(value, str):
            updates["title"] = value.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid title")

    if "notes" in payload:
        value = payload.get("notes")
        if value is None:
            updates["notes"] = None
        elif isinstance(value, str):
            updates["notes"] = value.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid notes")

    if "weekStartDate" in payload:
        updates["week_start_date"] = _parse_iso_date("weekStartDate", payload["weekStartDate"])

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    try:
        record = schedule_repository.update_week(week_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Week already exists") from exc
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    return {"week": jsonable_encoder(record)}


@router.delete("/weeks/{week_id}")
def api_delete_schedule_week(week_id: int, user=Depends(require_admin)):
    if not schedule_repository.get_week(week_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    schedule_repository.delete_week(week_id)
    log.info("schedule: deleted week %s", week_id)
    return {"status": "ok"}


@router.post("/weeks/{week_id}/copy")
async def api_copy_schedule_week(week_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    source_week_id = payload.get("sourceWeekId")
    if source_week_id in (None, "", "null"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "sourceWeekId required")
    if not isinstance(source_week_id, int):
        try:
            source_week_id = int(source_week_id)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid sourceWeekId")

    replace_raw = payload.get("replace", False)
    if isinstance(replace_raw, str):
        replace_existing = replace_raw.strip().lower() in {"1", "true", "yes", "on"}
    else:
        replace_existing = bool(replace_raw)

    target = schedule_repository.get_week(week_id)
    if not target:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    if not schedule_repository.get_week(source_week_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Source week not found")

    if replace_existing:
        schedule_repository.clear_week_slots(week_id)

    copied_slots, placeholders = schedule_repository.copy_slots_from_week(source_week_id, week_id)
    schedule_repository.update_week(week_id, copied_from_week_id=source_week_id)
    schedule_repository.sync_week_capacity(week_id)
    log.info(
        "schedule: copied week %s into %s (slots=%s placeholders=%s replace=%s)",
        source_week_id,
        week_id,
        copied_slots,
        placeholders,
        replace_existing,
    )

    slots = schedule_repository.list_slots_with_reservations(week_id)
    serialized_slots = [_serialize_slot(slot) for slot in slots]
    trainers_repository.ensure_trainers_table()
    stands_payload = [
        {
            "id": trainer["id"],
            "code": trainer.get("code"),
            "display_name": trainer.get("display_name"),
            "title": trainer.get("title"),
        }
        for trainer in trainers_repository.list_trainers()
    ]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())

    return {
        "copied": {
            "slots": copied_slots,
            "placeholders": placeholders,
        },
        "slots": serialized_slots,
        "stands": jsonable_encoder(stands_payload),
        "instructors": instructors_payload,
    }


@router.post("/weeks/{week_id}/sync")
def api_sync_schedule_week(week_id: int, user=Depends(require_admin)):
    if not schedule_repository.get_week(week_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    inserted = schedule_repository.sync_week_capacity(week_id)
    if inserted:
        log.info("schedule: synced placeholders for week %s (inserted=%s)", week_id, inserted)
    slots = schedule_repository.list_slots_with_reservations(week_id)
    serialized_slots = [_serialize_slot(slot) for slot in slots]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())
    return {"inserted": inserted, "slots": serialized_slots, "instructors": instructors_payload}


@router.post("/weeks/{week_id}/fill-template")
def api_fill_week_template(week_id: int, force: bool = False, user=Depends(require_admin)):
    if not schedule_repository.get_week(week_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    try:
        created = schedule_repository.create_default_slots_for_week(week_id, force=force)
    except Exception as exc:  # pylint: disable=broad-except
        log.warning("Failed to fill template for week %s: %s", week_id, exc)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to create template slots") from exc
    log.info("schedule: fill-template week=%s force=%s created=%s", week_id, force, created)
    slots = schedule_repository.list_slots_with_reservations(week_id)
    serialized_slots = [_serialize_slot(slot) for slot in slots]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())
    return {"created": created, "slots": serialized_slots, "instructors": instructors_payload}


@router.patch("/reservations/{reservation_id}")
async def api_update_reservation(reservation_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    updates: dict[str, object] = {}

    reservation = schedule_repository.get_reservation(reservation_id)
    if not reservation:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Reservation not found")
    slot_id = reservation.get("slot_id")

    if "clientId" in payload:
        client_value = payload["clientId"]
        if client_value in (None, "", "null"):
            updates["client_id"] = None
            updates.setdefault("client_name", None)
        else:
            try:
                client_id = int(client_value)
            except (TypeError, ValueError):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid clientId")
            client_record = client_repository.get_client(client_id)
            if not client_record:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
            updates["client_id"] = client_id
            if "clientName" not in payload:
                full_name = client_record.get("full_name") or (
                    f"{client_record.get('first_name', '')} {client_record.get('last_name', '')}".strip()
                )
                updates["client_name"] = full_name or None

    if "clientName" in payload:
        client_name = payload["clientName"]
        if client_name is None:
            updates["client_name"] = None
        elif isinstance(client_name, str):
            updates["client_name"] = client_name.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid clientName")

    if "status" in payload:
        status_value = payload["status"]
        if not isinstance(status_value, str) or status_value not in RESERVATION_STATUS_ALLOWED:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid status")
        updates["status"] = status_value

    if "notes" in payload:
        notes = payload["notes"]
        if notes is None:
            updates["notes"] = None
        elif isinstance(notes, str):
            updates["notes"] = notes.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid notes")

    if "source" in payload:
        source_value = payload["source"]
        if source_value is None:
            updates["source"] = None
        elif isinstance(source_value, str):
            updates["source"] = source_value.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid source")

    if "standId" in payload:
        stand_value = payload.get("standId")
        swap_reservation_id = payload.get("swapReservationId")
        trainer_row = None
        if stand_value in (None, "", "null"):
            new_stand_id = None
        else:
            try:
                new_stand_id = int(stand_value)
            except (TypeError, ValueError):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid standId")
            trainer_row = trainers_repository.get_trainer(new_stand_id)
            if not trainer_row:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Stand not found")

        current_stand_id = reservation.get("stand_id")
        if new_stand_id != current_stand_id:
            existing_for_stand = None
            if new_stand_id is not None and slot_id is not None:
                existing_for_stand = schedule_repository.get_reservation_for_stand(slot_id, new_stand_id)
            if existing_for_stand and existing_for_stand.get("id") != reservation_id:
                if swap_reservation_id is None or swap_reservation_id != existing_for_stand.get("id"):
                    raise HTTPException(status.HTTP_409_CONFLICT, "Stand already occupied")
                current_code = reservation.get("stand_code")
                if current_stand_id is not None and current_code is None:
                    current_trainer = trainers_repository.get_trainer(current_stand_id)
                    current_code = current_trainer.get("code") if current_trainer else None
                try:
                    if current_stand_id is not None:
                        schedule_repository.update_reservation(
                            reservation_id,
                            stand_id=None,
                            stand_code=None,
                        )
                    schedule_repository.update_reservation(
                        existing_for_stand["id"],
                        stand_id=current_stand_id,
                        stand_code=current_code,
                    )
                except psycopg2.errors.UniqueViolation as exc:
                    raise HTTPException(status.HTTP_409_CONFLICT, "Stand already occupied") from exc
            updates["stand_id"] = new_stand_id
            updates["stand_code"] = trainer_row.get("code") if trainer_row else None

    if not updates and not {"clientId", "clientName"} & payload.keys():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    if "status" not in updates:
        client_id_new = updates.get("client_id", reservation.get("client_id"))
        client_name_new = updates.get("client_name", reservation.get("client_name"))
        if client_id_new is not None or (client_name_new and str(client_name_new).strip()):
            updates.setdefault("status", "booked")
        elif "client_id" in updates or "client_name" in updates:
            updates.setdefault("status", "available")

    try:
        record = schedule_repository.update_reservation(reservation_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Stand already occupied") from exc
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Reservation not found")

    slot_payload = None
    slot_meta = schedule_repository.get_slot(record["slot_id"])
    if slot_meta:
        slots = schedule_repository.list_slots_with_reservations(slot_meta["week_id"])
        for item in slots:
            if item["id"] == slot_meta["id"]:
                slot_payload = item
                break

    response = {
        "reservation": _serialize_reservation(record),
    }
    if slot_payload:
        response["slot"] = _serialize_slot(slot_payload)
    return response


@router.post("/reservations/{reservation_id}/clear")
def api_clear_reservation(reservation_id: int, user=Depends(require_admin)):
    record = schedule_repository.get_reservation(reservation_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Reservation not found")
    updates = {
        "client_id": None,
        "client_name": None,
        "status": "available",
        "notes": None,
        "source": "manual-clear",
    }
    record = schedule_repository.update_reservation(reservation_id, **updates)
    slot_payload = None
    slot_meta = schedule_repository.get_slot(record["slot_id"]) if record else None
    if slot_meta:
        slots = schedule_repository.list_slots_with_reservations(slot_meta["week_id"])
        for item in slots:
            if item["id"] == slot_meta["id"]:
                slot_payload = item
                break
    response = {"reservation": _serialize_reservation(record)}
    if slot_payload:
        response["slot"] = _serialize_slot(slot_payload)
    return response


@router.get("/notifications")
def api_get_workout_notifications(page: int = 1, user=Depends(require_user)):
    """Get list of workout notifications."""
    page = max(page, 1)
    limit = 50
    offset = (page - 1) * limit

    try:
        notifications = schedule_repository.list_workout_notifications(limit=limit, offset=offset)
        total_count = len(notifications)
        total_pages = max(math.ceil(total_count / limit), 1) if total_count > 0 else 1

        return {
            "items": jsonable_encoder(notifications),
            "pagination": {
                "page": page,
                "pageSize": limit,
                "total": total_count,
                "totalPages": total_pages,
            },
        }
    except Exception as exc:
        log.exception("Failed to fetch workout notifications")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch notifications") from exc


@router.get("/notification-settings")
def api_get_notification_settings(user=Depends(require_admin)):
    """Get workout notification settings."""
    try:
        settings = schedule_repository.get_workout_notification_settings()
        return {"settings": settings}
    except Exception as exc:
        log.exception("Failed to fetch notification settings")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch settings") from exc


@router.post("/notification-settings")
async def api_update_notification_settings(request: Request, user=Depends(require_admin)):
    """Update workout notification settings."""
    try:
        payload = await request.json()
        reminder_hours = payload.get("reminder_hours")

        if reminder_hours is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reminder_hours is required")

        if not isinstance(reminder_hours, int):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reminder_hours must be an integer")

        if reminder_hours < 1 or reminder_hours > 168:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reminder_hours must be between 1 and 168")

        success = schedule_repository.update_workout_notification_settings(reminder_hours)

        if not success:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Failed to update settings")

        return {"settings": {"reminder_hours": reminder_hours}}
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("Failed to update notification settings")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to update settings") from exc


@public_router.get("/schedule/{slug}")
def schedule_week(slug: str, request: Request):
    week_start = _week_start_for_slug(slug)
    if not week_start:
        try:
            week_start = datetime.strptime(slug, "%Y-%m-%d").date()
            week_start = week_start - timedelta(days=week_start.weekday())
        except ValueError:
            response = templates.TemplateResponse(
                "public_schedule.html",
                {
                    "request": request,
                    "error_message": "Недопустимый формат даты в URL",
                },
            )
            response.headers["Cache-Control"] = "no-store"
            return response

    share_url = f"{request.url.scheme}://{request.url.netloc}/schedule/{slug}"

    try:
        week = schedule_repository.get_week_by_start(week_start)
    except Exception as exc:  # pylint: disable=broad-except
        log.error("Failed to load week for %s: %s", week_start, exc)
        response = templates.TemplateResponse(
            "public_schedule.html",
            {
                "request": request,
                "error_message": "Не удалось загрузить расписание",
                "canonical_slug": slug,
                "share_url": share_url,
            },
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    adjacent_weeks = schedule_repository.get_adjacent_week_starts(week_start)
    prev_week_slug = _format_week_slug(adjacent_weeks.get("previous"))
    next_week_slug = _format_week_slug(adjacent_weeks.get("next"))
    week_range_label = _format_week_range_label(week["week_start_date"] if week else week_start)

    if not week:
        response = templates.TemplateResponse(
            "public_schedule.html",
            {
                "request": request,
                "error_message": "Расписание для выбранной недели не найдено",
                "week_range_label": week_range_label,
                "target_week_label": week_range_label,
                "prev_week_slug": prev_week_slug,
                "next_week_slug": next_week_slug,
                "canonical_slug": slug,
                "share_url": share_url,
                "day_columns": [],
            },
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    payload = _load_schedule_week_payload(week["id"])
    if not payload:
        response = templates.TemplateResponse(
            "public_schedule.html",
            {
                "request": request,
                "error_message": "Не удалось загрузить данные расписания",
                "week_range_label": week_range_label,
                "prev_week_slug": prev_week_slug,
                "next_week_slug": next_week_slug,
                "canonical_slug": slug,
                "share_url": share_url,
                "day_columns": [],
            },
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    day_columns = _build_day_columns(
        payload["slots"],
        week["week_start_date"],
        payload["instructors"],
    )

    context = {
        "request": request,
        "week": week,
        "day_columns": day_columns,
        "prev_week_slug": prev_week_slug,
        "next_week_slug": next_week_slug,
        "canonical_slug": slug,
        "week_range_label": week_range_label,
        "share_url": share_url,
    }

    response = templates.TemplateResponse("public_schedule.html", context)
    response.headers["Cache-Control"] = "no-store"
    return response


@public_router.get("/schedule")
def schedule_default():
    return RedirectResponse(url="/schedule/current_week", status_code=status.HTTP_307_TEMPORARY_REDIRECT)
