from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder

from repositories import (
    client_link_repository,
    client_repository,
    schedule_repository,
    client_subscription_repository,
    client_balance_repository,
)
from ..config import get_settings
from ..dependencies import require_admin, require_user
from .schedule_utils import _serialize_reservation


router = APIRouter(prefix="/clients", tags=["clients"], dependencies=[Depends(require_user)])

SUBSCRIPTION_PLANS = {
    "pack_4": {"plan_name": "Абонемент на 4 занятия", "sessions_total": 4, "price_rub": 2500},
    "pack_8": {"plan_name": "Абонемент на 8 занятий", "sessions_total": 8, "price_rub": 4500},
    "pack_12": {"plan_name": "Абонемент на 12 занятий", "sessions_total": 12, "price_rub": 6000},
    "unlimited": {"plan_name": "Безлимит на месяц", "sessions_total": None, "price_rub": 7500},
    "unlimited_self": {
        "plan_name": "Безлимит на месяц самокрутки",
        "sessions_total": None,
        "price_rub": 5500,
    },
}


def _serialize_activity_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized = dict(row)
    for ts_field in ("created_at", "start_time"):
        value = serialized.get(ts_field)
        if hasattr(value, "isoformat"):
            serialized[ts_field] = value.isoformat()
    return serialized


def _serialize_reservation_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized = _serialize_reservation(row)
    slot_date = serialized.get("slot_date")
    if hasattr(slot_date, "isoformat"):
        serialized["slot_date"] = slot_date.isoformat()
    start_time = serialized.get("start_time")
    if hasattr(start_time, "strftime"):
        serialized["start_time"] = start_time.strftime("%H:%M")
    end_time = serialized.get("end_time")
    if hasattr(end_time, "strftime"):
        serialized["end_time"] = end_time.strftime("%H:%M")
    return serialized


def _serialize_subscription_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized = dict(row)
    for field in ("created_at", "valid_from", "valid_until"):
        value = serialized.get(field)
        if hasattr(value, "isoformat"):
            serialized[field] = value.isoformat()
    return serialized


def _serialize_adjustment_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized = dict(row)
    created_at = serialized.get("created_at")
    if hasattr(created_at, "isoformat"):
        serialized["created_at"] = created_at.isoformat()
    return serialized


@router.get("")
def api_clients(
    page: int = 1,
    search: str | None = None,
    sort: str | None = None,
    direction: str = "asc",
):
    settings = get_settings()
    page = max(page, 1)
    limit = settings.clients_page_size
    offset = (page - 1) * limit
    term = search.strip() if isinstance(search, str) else None

    rows = client_repository.list_clients(
        limit=limit,
        offset=offset,
        search=term,
        sort=sort,
        direction=direction,
    )
    total = client_repository.count_clients(search=term)
    total_pages = max(math.ceil(total / limit), 1)

    return {
        "items": jsonable_encoder(rows),
        "pagination": {
            "page": page,
            "pageSize": limit,
            "total": total,
            "totalPages": total_pages,
        },
    }


@router.get("/{client_id}")
def api_get_client(client_id: int):
    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"item": jsonable_encoder(record)}


@router.get("/{client_id}/activities")
def api_get_client_activities(client_id: int):
    """Return activities linked to the given client with basic stats."""
    schedule_repository.ensure_activity_ids_table()
    with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                account_id,
                activity_id,
                start_time,
                scheduled_name,
                profile_name,
                distance,
                elapsed_time,
                elevation_gain,
                created_at
            FROM seen_activity_ids
            WHERE client_id = %s
            ORDER BY start_time DESC NULLS LAST, created_at DESC
            LIMIT 200
            """,
            (client_id,),
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT
                COUNT(*) AS cnt,
                COALESCE(SUM(distance), 0) AS distance,
                COALESCE(SUM(elevation_gain), 0) AS elevation_gain,
                COALESCE(SUM(elapsed_time), 0) AS elapsed_time
            FROM seen_activity_ids
            WHERE client_id = %s
            """,
            (client_id,),
        )
        stats_row = cur.fetchone() or {}

    items = [_serialize_activity_row(row) for row in rows]
    stats = {
        "count": stats_row.get("cnt", 0),
        "distance": stats_row.get("distance", 0) or 0,
        "elevation_gain": stats_row.get("elevation_gain", 0) or 0,
        "elapsed_time": stats_row.get("elapsed_time", 0) or 0,
    }
    return {"items": items, "stats": stats}


@router.get("/{client_id}/reservations")
def api_get_client_reservations(client_id: int):
    """Return future and recent past reservations for a client."""
    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    now = datetime.now()
    upcoming = schedule_repository.list_future_reservations_for_client(client_id, now)
    past = schedule_repository.list_past_reservations_for_client(client_id, now, limit=20)

    return {
        "upcoming": [_serialize_reservation_row(row) for row in upcoming],
        "past": [_serialize_reservation_row(row) for row in past],
        "stats": {
            "upcoming": len(upcoming),
            "past": len(past),
            "total": len(upcoming) + len(past),
        },
    }


@router.get("/{client_id}/balance", dependencies=[Depends(require_admin)])
def api_get_client_balance(client_id: int):
    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    balance = client_balance_repository.get_balance(client_id)
    adjustments = client_balance_repository.list_adjustments(client_id)
    return {
        "balance": balance,
        "adjustments": [_serialize_adjustment_row(adj) for adj in adjustments],
    }


def _parse_date(value: object, field: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        try:
            return date.fromisoformat(trimmed.split("T")[0])
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc
    raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")


@router.get("/{client_id}/subscriptions", dependencies=[Depends(require_admin)])
def api_get_client_subscriptions(client_id: int):
    """Return purchased subscriptions and adjustments for a client (admins only)."""
    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    subscriptions = client_subscription_repository.list_client_subscriptions(client_id)
    total_sessions = 0
    items = []
    for sub in subscriptions:
        serialized = _serialize_subscription_row(sub)
        adjustments = [_serialize_adjustment_row(item) for item in sub.get("adjustments", [])]
        serialized["adjustments"] = adjustments
        items.append(serialized)
        remaining = sub.get("sessions_remaining")
        if isinstance(remaining, int):
            total_sessions += remaining

    return {"items": items, "totals": {"sessions_remaining": total_sessions}}


@router.post("/{client_id}/subscriptions")
async def api_create_client_subscription(client_id: int, request: Request, admin=Depends(require_admin)):
    payload = await request.json()

    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    def _parse_int(value: object, field: str, *, allow_none: bool = True) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned == "":
                return None
            try:
                return int(cleaned)
            except ValueError as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc
        if allow_none:
            return None
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")

    plan_code = (payload.get("plan_code") or "").strip()
    if not plan_code:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "plan_code is required")

    defaults = SUBSCRIPTION_PLANS.get(plan_code, {})
    plan_name = (payload.get("plan_name") or defaults.get("plan_name") or plan_code).strip()
    if not plan_name:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "plan_name is required")

    sessions_total = _parse_int(payload.get("sessions_total", defaults.get("sessions_total")), "sessions_total")
    price_rub = _parse_int(payload.get("price_rub", defaults.get("price_rub")), "price_rub")
    sessions_remaining = _parse_int(
        payload.get("sessions_remaining", payload.get("sessions_total", defaults.get("sessions_total"))),
        "sessions_remaining",
    )
    valid_from = _parse_date(payload.get("valid_from"), "valid_from")
    valid_until = _parse_date(payload.get("valid_until"), "valid_until")
    notes_value = payload.get("notes")
    notes = notes_value.strip() if isinstance(notes_value, str) else None

    try:
        subscription = client_subscription_repository.create_subscription(
            client_id=client_id,
            plan_code=plan_code,
            plan_name=plan_name,
            sessions_total=sessions_total,
            price_rub=price_rub,
            valid_from=valid_from,
            valid_until=valid_until,
            notes=notes,
            created_by=getattr(admin, "id", None),
            sessions_remaining=sessions_remaining,
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    serialized = _serialize_subscription_row(subscription)
    return {"item": serialized}


@router.delete("/{client_id}/subscriptions/{subscription_id}")
async def api_delete_client_subscription(client_id: int, subscription_id: int, admin=Depends(require_admin)):
    subscription = client_subscription_repository.get_subscription(subscription_id)
    if not subscription or subscription.get("client_id") != client_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Subscription not found")

    deleted = client_subscription_repository.delete_subscription(subscription_id)
    if not deleted:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Не удалось удалить абонемент")
    return {"status": "deleted"}


@router.post("/{client_id}/subscriptions/{subscription_id}/adjust")
async def api_adjust_client_subscription(
    client_id: int,
    subscription_id: int,
    request: Request,
    admin=Depends(require_admin),
):
    payload = await request.json()

    subscription = client_subscription_repository.get_subscription(subscription_id)
    if not subscription or subscription.get("client_id") != client_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Subscription not found")

    try:
        delta = int(payload.get("delta_sessions"))
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "delta_sessions must be an integer")
    if delta == 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "delta_sessions must be non-zero")

    reservation_id_value = payload.get("reservation_id")
    reservation_id = None
    if reservation_id_value is not None:
        try:
            reservation_id = int(reservation_id_value)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reservation_id must be an integer")
    if delta < 0 and reservation_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "reservation_id is required для списания тренировки")

    reason = payload.get("reason") or ("spend" if delta < 0 else "top-up")

    try:
        updated, adjustment = client_subscription_repository.add_adjustment(
            subscription_id=subscription_id,
            delta_sessions=delta,
            reason=reason,
            created_by=getattr(admin, "id", None),
            reservation_id=reservation_id,
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    serialized = _serialize_subscription_row(updated)
    serialized["adjustments"] = [_serialize_adjustment_row(adjustment)]
    return {"item": serialized}


@router.post("/{client_id}/balance", dependencies=[Depends(require_admin)])
async def api_adjust_client_balance(client_id: int, request: Request, admin=Depends(require_admin)):
    payload = await request.json()
    try:
        delta_rub = int(payload.get("delta_rub"))
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "delta_rub must be an integer")
    if delta_rub == 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "delta_rub must be non-zero")
    reservation_id_value = payload.get("reservation_id")
    reservation_id = None
    if reservation_id_value is not None:
        try:
            reservation_id = int(reservation_id_value)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reservation_id must be an integer")
    reason = (payload.get("reason") or "").strip() or ("spend" if delta_rub < 0 else "top-up")
    created_at_value = payload.get("created_at")
    created_at = None
    if created_at_value:
        try:
            created_at = str(created_at_value)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "created_at must be a string")
    try:
        balance, adjustment = client_balance_repository.add_adjustment(
            client_id=client_id,
            delta_rub=delta_rub,
            reason=reason,
            reservation_id=reservation_id,
            created_by=getattr(admin, "id", None),
            created_at=created_at,
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    return {
        "balance": _serialize_adjustment_row(balance),
        "adjustment": _serialize_adjustment_row(adjustment),
    }


@router.delete("/{client_id}/balance/{adjustment_id}", dependencies=[Depends(require_admin)])
async def api_delete_client_balance_adjustment(client_id: int, adjustment_id: int, admin=Depends(require_admin)):
    try:
        balance = client_balance_repository.delete_adjustment(client_id, adjustment_id)
    except ValueError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc
    return {"balance": _serialize_adjustment_row(balance)}


@router.patch("/{client_id}", dependencies=[Depends(require_admin)])
async def api_update_client(client_id: int, request: Request):
    payload = await request.json()

    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    updates: dict[str, object] = {}

    def _clean_text(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed or None
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid text value")

    def _clean_numeric(value: object, field: str) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            normalized = stripped.replace(",", ".")
            try:
                return float(normalized)
            except ValueError as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")

    if "first_name" in payload:
        updates["first_name"] = _clean_text(payload["first_name"])
    if "last_name" in payload:
        updates["last_name"] = _clean_text(payload["last_name"])
    if "weight" in payload:
        updates["weight"] = _clean_numeric(payload["weight"], "weight")
    if "height" in payload:
        updates["height"] = _clean_numeric(payload["height"], "height")
    if "ftp" in payload:
        updates["ftp"] = _clean_numeric(payload["ftp"], "ftp")
    if "favorite_bike" in payload:
        updates["favorite_bike"] = _clean_text(payload["favorite_bike"])
    if "pedals" in payload:
        updates["pedals"] = _clean_text(payload["pedals"])
    if "goal" in payload:
        updates["goal"] = _clean_text(payload["goal"])
    if "gender" in payload:
        gender_value = _clean_text(payload["gender"])
        updates["gender"] = gender_value.lower() if isinstance(gender_value, str) else gender_value
    if "saddle_height" in payload:
        updates["saddle_height"] = _clean_text(payload["saddle_height"])

    if "first_name" in updates or "last_name" in updates:
        first = updates.get("first_name", record.get("first_name"))
        last = updates.get("last_name", record.get("last_name"))
        full_name = " ".join(part for part in [first, last] if part) if (first or last) else None
        updates["full_name"] = full_name

    for key in list(updates.keys()):
        if key == "full_name":
            continue
        if updates[key] == record.get(key):
            updates.pop(key)
    if "full_name" in updates and updates["full_name"] == record.get("full_name"):
        updates.pop("full_name")

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    try:
        client_repository.update_client_fields(client_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Another client already uses this full name"
        ) from exc
    updated_record = client_repository.get_client(client_id)
    if not updated_record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found after update")

    return {"item": jsonable_encoder(updated_record)}


@router.post("", dependencies=[Depends(require_admin)])
async def api_create_client(request: Request):
    payload = await request.json()

    def _clean_text(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed or None
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid text value")

    def _clean_numeric(value: object, field: str) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            normalized = stripped.replace(",", ".")
            try:
                return float(normalized)
            except ValueError as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")

    first_name = _clean_text(payload.get("first_name"))
    last_name = _clean_text(payload.get("last_name"))
    if not first_name and not last_name:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Имя или фамилия обязательны")

    weight = _clean_numeric(payload.get("weight"), "weight")
    height = _clean_numeric(payload.get("height"), "height")
    ftp = _clean_numeric(payload.get("ftp"), "ftp")
    pedals = _clean_text(payload.get("pedals"))
    goal = _clean_text(payload.get("goal"))
    favorite_bike = _clean_text(payload.get("favorite_bike"))
    saddle_height = _clean_text(payload.get("saddle_height"))
    gender = _clean_text(payload.get("gender"))
    if isinstance(gender, str):
        gender = gender.lower()

    try:
        record = client_repository.create_client(
            first_name=first_name,
            last_name=last_name,
            gender=gender,
            weight=weight,
            height=height,
            ftp=ftp,
            pedals=pedals,
            goal=goal,
            favorite_bike=favorite_bike,
            saddle_height=saddle_height,
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Клиент с таким именем уже существует") from exc

    created = client_repository.get_client(record["id"])
    return {"item": jsonable_encoder(created or record)}


@router.delete("/{client_id}", dependencies=[Depends(require_admin)])
def api_delete_client(client_id: int):
    client_link_repository.remove_link(client_id=client_id)

    deleted = client_repository.delete_client(client_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"ok": True}
