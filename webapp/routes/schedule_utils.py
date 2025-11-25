"""Shared helpers for schedule serialization and data loading."""
from __future__ import annotations

from datetime import date, time
from typing import Optional

from fastapi.encoders import jsonable_encoder

from repositories import client_repository, instructors_repository, schedule_repository, trainers_repository


def _serialize_reservation(reservation: dict, client_lookup: Optional[dict[int, dict]] = None) -> dict:
    serialized = dict(reservation)
    created_at = serialized.get("created_at")
    if hasattr(created_at, "isoformat"):
        serialized["created_at"] = created_at.isoformat()
    updated_at = serialized.get("updated_at")
    if hasattr(updated_at, "isoformat"):
        serialized["updated_at"] = updated_at.isoformat()
    client_id = serialized.get("client_id")
    if client_id is not None and client_lookup is not None:
        client_row = client_lookup.get(client_id)
        if client_row and client_row.get("height") is not None:
            serialized["client_height"] = client_row.get("height")
    return serialized


def _serialize_slot(slot: dict, client_lookup: Optional[dict[int, dict]] = None) -> dict:
    serialized = dict(slot)
    slot_date = serialized.get("slot_date")
    if isinstance(slot_date, date):
        serialized["slot_date"] = slot_date.isoformat()
    start_time = serialized.get("start_time")
    if isinstance(start_time, time):
        serialized["start_time"] = start_time.strftime("%H:%M")
    end_time = serialized.get("end_time")
    if isinstance(end_time, time):
        serialized["end_time"] = end_time.strftime("%H:%M")

    if "instructor_id" in serialized:
        serialized["instructorId"] = serialized.pop("instructor_id")
    if "instructor_name" in serialized:
        serialized["instructorName"] = serialized.pop("instructor_name")

    reservations = serialized.get("reservations") or []
    serialized["reservations"] = [_serialize_reservation(res, client_lookup) for res in reservations]
    return serialized


def _load_schedule_week_payload(week_id: int) -> Optional[dict]:
    week = schedule_repository.get_week(week_id)
    if not week:
        return None

    slots = schedule_repository.list_slots_with_reservations(week_id)
    if not slots:
        try:
            created = schedule_repository.create_default_slots_for_week(week_id)
        except Exception as exc:  # pylint: disable=broad-except
            return None
        if created:
            slots = schedule_repository.list_slots_with_reservations(week_id)

    trainers_repository.ensure_trainers_table()
    stands = trainers_repository.list_trainers()
    client_lookup: dict[int, dict] = {}
    for slot in slots or []:
        for res in slot.get("reservations") or []:
            client_id = res.get("client_id")
            if client_id is not None and client_id not in client_lookup:
                client_row = client_repository.get_client(client_id)
                if client_row:
                    client_lookup[client_id] = client_row
    stands_payload = [
        {
            "id": trainer["id"],
            "code": trainer.get("code"),
            "display_name": trainer.get("display_name"),
            "title": trainer.get("title"),
            "bike_id": trainer.get("bike_id"),
            "bike_title": trainer.get("bike_title"),
            "bike_owner": trainer.get("bike_owner"),
            "bike_size_label": trainer.get("bike_size_label"),
            "bike_frame_size_cm": trainer.get("bike_frame_size_cm"),
            "bike_height_min_cm": trainer.get("bike_height_min_cm"),
            "bike_height_max_cm": trainer.get("bike_height_max_cm"),
        }
        for trainer in stands
    ]

    serialized_slots = [_serialize_slot(slot, client_lookup) for slot in slots]
    instructors_payload = jsonable_encoder(instructors_repository.list_instructors())

    return {
        "week": jsonable_encoder(week),
        "slots": serialized_slots,
        "stands": jsonable_encoder(stands_payload),
        "instructors": instructors_payload,
    }
