"""Schedule slots and reservations management routes."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder

from repositories import client_repository, instructors_repository, schedule_repository, trainers_repository
from ..dependencies import require_admin, require_user
from ..utils.parsing import parse_iso_date as _parse_iso_date, parse_iso_time as _parse_iso_time
from .schedule_utils import _serialize_slot, _serialize_reservation, _load_schedule_week_payload

router = APIRouter(tags=["schedule"])
log = logging.getLogger(__name__)

SCHEDULE_SESSION_KINDS = {"self_service", "instructor", "open"}
RESERVATION_STATUS_ALLOWED = {"available", "booked", "cancelled", "pending", "waitlist", "blocked", "legacy", "hold"}


def _week_start_for_slug(slug: str):
    from datetime import date, timedelta, datetime

    if not slug:
        return None
    normalized = slug.strip().lower()
    today = date.today()
    current_week_start = today - timedelta(days=today.weekday())
    if normalized == "current_week":
        return current_week_start
    if normalized == "next_week":
        return current_week_start + timedelta(days=7)
    if normalized == "prev_week":
        return current_week_start - timedelta(days=7)
    if normalized.startswith("week_"):
        try:
            week_date = datetime.strptime(normalized.replace("week_", ""), "%d_%m_%y").date()
            return week_date - timedelta(days=week_date.weekday())
        except ValueError:
            return None
    try:
        week_date = datetime.strptime(normalized, "%Y-%m-%d").date()
        return week_date - timedelta(days=week_date.weekday())
    except ValueError:
        return None


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

    from datetime import date, time

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


@router.post("/schedule/slots")
async def api_create_schedule_slot(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    try:
        week_id = int(payload.get("weekId"))
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid weekId")

    if not schedule_repository.get_week(week_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")

    slot_date = _parse_iso_date("slotDate", payload.get("slotDate"))
    start_time = _parse_iso_time("startTime", payload.get("startTime"))
    end_time = _parse_iso_time("endTime", payload.get("endTime"))
    if end_time <= start_time:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "endTime must be after startTime")

    label = payload.get("label")
    session_kind = payload.get("sessionKind") or "self_service"
    if session_kind not in SCHEDULE_SESSION_KINDS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid sessionKind")

    sort_index = payload.get("sortIndex")
    if sort_index is not None:
        try:
            sort_index = int(sort_index)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid sortIndex")

    notes = payload.get("notes")
    instructor_id = payload.get("instructorId")
    if instructor_id in (None, "", "null"):
        instructor_id = None
    else:
        try:
            instructor_id = int(instructor_id)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid instructorId")
        if not instructors_repository.get_instructor(instructor_id):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Instructor not found")
    if session_kind != "instructor":
        instructor_id = None
    try:
        created = schedule_repository.create_slot(
            week_id=week_id,
            slot_date=slot_date,
            start_time=start_time,
            end_time=end_time,
            label=label.strip() if isinstance(label, str) else None,
            session_kind=session_kind,
            sort_index=sort_index,
            notes=notes.strip() if isinstance(notes, str) else None,
            instructor_id=instructor_id,
        )
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Slot already exists for this time") from exc

    log.info(
        "schedule: created slot id=%s week=%s date=%s %s-%s",
        created.get("id"),
        week_id,
        slot_date,
        start_time,
        end_time,
    )
    slots = schedule_repository.list_slots_with_reservations(week_id)
    serialized_slots = [_serialize_slot(slot) for slot in slots]
    slot_payload = next((item for item in slots if item["id"] == created["id"]), created)
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())
    return {"slot": _serialize_slot(slot_payload), "slots": serialized_slots, "instructors": instructors_payload}


@router.patch("/schedule/slots/{slot_id}")
async def api_update_schedule_slot(slot_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()

    current_slot = schedule_repository.get_slot(slot_id)
    if not current_slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")

    updates: dict[str, object] = {}
    if "slotDate" in payload:
        updates["slot_date"] = _parse_iso_date("slotDate", payload["slotDate"])
    if "startTime" in payload:
        updates["start_time"] = _parse_iso_time("startTime", payload["startTime"])
    if "endTime" in payload:
        updates["end_time"] = _parse_iso_time("endTime", payload["endTime"])

    if "label" in payload:
        label = payload["label"]
        if label is None:
            updates["label"] = None
        elif isinstance(label, str):
            updates["label"] = label.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid label")

    if "sessionKind" in payload:
        session_kind = payload["sessionKind"]
        if session_kind not in SCHEDULE_SESSION_KINDS:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid sessionKind")
        updates["session_kind"] = session_kind
        if session_kind != "instructor":
            updates["instructor_id"] = None

    if "isCancelled" in payload:
        updates["is_cancelled"] = bool(payload["isCancelled"])

    if "sortIndex" in payload:
        sort_index_value = payload["sortIndex"]
        if sort_index_value is None:
            updates["sort_index"] = None
        else:
            try:
                updates["sort_index"] = int(sort_index_value)
            except (TypeError, ValueError):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid sortIndex")

    if "notes" in payload:
        notes = payload["notes"]
        if notes is None:
            updates["notes"] = None
        elif isinstance(notes, str):
            updates["notes"] = notes.strip() or None
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid notes")

    if "instructorId" in payload:
        instructor_value = payload["instructorId"]
        if instructor_value in (None, "", "null"):
            updates["instructor_id"] = None
        else:
            try:
                instructor_id = int(instructor_value)
            except (TypeError, ValueError):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid instructorId")
            if not instructors_repository.get_instructor(instructor_id):
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Instructor not found")
            updates["instructor_id"] = instructor_id

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    new_start = updates.get("start_time", current_slot["start_time"])
    new_end = updates.get("end_time", current_slot["end_time"])
    if new_end <= new_start:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "endTime must be after startTime")

    try:
        record = schedule_repository.update_slot(slot_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Slot already exists for this time") from exc
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")
    log.info("schedule: updated slot %s (%s)", slot_id, ", ".join(updates.keys()))

    slot = schedule_repository.get_slot(slot_id)
    if not slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")

    week_id = slot["week_id"]
    slots = schedule_repository.list_slots_with_reservations(week_id)
    slot_payload = next((item for item in slots if item["id"] == slot_id), slot)
    serialized_slots = [_serialize_slot(item) for item in slots]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())
    return {"slot": _serialize_slot(slot_payload), "slots": serialized_slots, "instructors": instructors_payload}


@router.delete("/schedule/slots/{slot_id}")
def api_delete_schedule_slot(slot_id: int, user=Depends(require_admin)):
    slot = schedule_repository.get_slot(slot_id)
    if not slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")
    schedule_repository.delete_slot(slot_id)
    log.info("schedule: deleted slot %s (week %s)", slot_id, slot["week_id"])
    slots = schedule_repository.list_slots_with_reservations(slot["week_id"])
    serialized_slots = [_serialize_slot(item) for item in slots]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())
    return {"slots": serialized_slots, "instructors": instructors_payload}


@router.get("/schedule/slots/{slot_id}")
def api_get_schedule_slot(slot_id: int, user=Depends(require_user)):
    slot = schedule_repository.get_slot(slot_id)
    if not slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")

    payload = _load_schedule_week_payload(slot["week_id"])
    if not payload:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")

    slot_payload = schedule_repository.get_slot_with_reservations(slot_id) or slot
    client_lookup: dict[int, dict] = {}
    for res in slot_payload.get("reservations") or []:
        client_id = res.get("client_id")
        if client_id is not None and client_id not in client_lookup:
            client_row = client_repository.get_client(client_id)
            if client_row:
                client_lookup[client_id] = client_row

    instructors_payload = payload.get("instructors") or jsonable_encoder(instructors_repository.list_instructors())
    return {
        "week": payload["week"],
        "slot": _serialize_slot(slot_payload, client_lookup),
        "stands": payload.get("stands") or [],
        "instructors": instructors_payload,
    }


@router.get("/schedule/slots/{slot_id}/copy-targets")
def api_get_slot_copy_targets(slot_id: int, user=Depends(require_admin)):
    slot = schedule_repository.get_slot(slot_id)
    if not slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")

    targets = schedule_repository.list_future_slots_for_copy(
        slot["slot_date"], slot["start_time"], exclude_slot_id=slot_id
    )
    return {"items": [_serialize_slot_summary(target) for target in targets]}


@router.post("/schedule/slots/{slot_id}/copy")
async def api_copy_slot_seating(slot_id: int, request: Request, user=Depends(require_admin)):
    source_slot = schedule_repository.get_slot(slot_id)
    if not source_slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")

    payload = await request.json()
    target_ids_raw = payload.get("targetSlotIds")
    if not isinstance(target_ids_raw, list) or not target_ids_raw:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "targetSlotIds must be a non-empty list")

    try:
        target_ids = [int(item) for item in target_ids_raw]
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid targetSlotIds") from exc

    results = []
    updated_slots: list[dict] = []

    for target_id in target_ids:
        target_slot = schedule_repository.get_slot(target_id)
        if not target_slot:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"Target slot {target_id} not found")

        if (target_slot["slot_date"], target_slot["start_time"]) < (
            source_slot["slot_date"],
            source_slot["start_time"],
        ):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Target slot must be in the future")

        copy_result = schedule_repository.copy_slot_seating(slot_id, target_id)
        slot_payload = schedule_repository.get_slot_with_reservations(target_id)
        client_lookup: dict[int, dict] = {}
        for res in slot_payload.get("reservations") or []:
            client_id = res.get("client_id")
            if client_id is not None and client_id not in client_lookup:
                client_row = client_repository.get_client(client_id)
                if client_row:
                    client_lookup[client_id] = client_row
        updated_slots.append(_serialize_slot(slot_payload, client_lookup))
        results.append(
            {
                "target_slot_id": target_id,
                "week_id": target_slot.get("week_id"),
                **copy_result,
            }
        )

    return {"results": results, "updated_slots": updated_slots}


@router.get("/schedule/week/{slug}")
def api_get_schedule_week_by_slug(slug: str, user=Depends(require_user)):
    week_start = _week_start_for_slug(slug)
    if not week_start:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid week slug")
    try:
        week = schedule_repository.get_or_create_week(week_start)
    except Exception as exc:  # pylint: disable=broad-except
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to load week") from exc
    payload = _load_schedule_week_payload(week.get("id"))
    if not payload:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    return payload


@router.patch("/schedule/reservations/{reservation_id}")
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
        updated = schedule_repository.update_reservation(reservation_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Stand already occupied") from exc
    if not updated:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Reservation not found")

    slot = schedule_repository.get_slot_with_reservations(slot_id) if slot_id else None
    if slot:
        slots = schedule_repository.list_slots_with_reservations(slot["week_id"])
        slot_payload = next((item for item in slots if item["id"] == slot_id), slot)
        serialized_slots = [_serialize_slot(item) for item in slots]
        return {
            "reservation": _serialize_reservation(updated),
            "slot": _serialize_slot(slot_payload),
            "slots": serialized_slots,
        }

    return {"reservation": _serialize_reservation(updated)}
