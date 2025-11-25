"""FastAPI backend for the Крутилка admin portal."""
from __future__ import annotations

import math
import logging
import os
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import psycopg2
import requests
from straver_client import StraverClient
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Form,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer
from repositories import (
    admin_repository,
    bikes_repository,
    client_link_repository,
    client_repository,
    intervals_link_repository,
    instructors_repository,
    layout_repository,
    message_repository,
    race_repository,
    trainers_repository,
    schedule_repository,
)
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse

from .auth import TelegramAuthError, verify_telegram_payload
from .config import get_settings
from .dependencies import (
    SESSION_KEY_USER,
    get_current_user,
    is_admin_user,
    require_admin,
    require_user,
)
from .routes.activities import router as activities_router
from .routes.messaging import ensure_uploads_dir, router as messaging_router, UPLOADS_DIR as MESSAGING_UPLOADS_DIR
from .routes.vk_client_links import router as vk_client_links_router
from .routes.intervals_links import router as intervals_links_router
from .routes.races import router as races_router, _format_race_date_label
from .routes.sync import router as sync_router
from .utils.parsing import (
    parse_iso_date as _parse_iso_date,
    parse_iso_time as _parse_iso_time,
    to_float as _to_float,
)


BASE_DIR = Path(__file__).parent
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"
TEMPLATES_DIR = BASE_DIR / "templates"
log = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _safe_next(next_param: Optional[str]) -> str:
    if not next_param:
        return "/app"
    if next_param.startswith("/"):
        return next_param
    return "/app"


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


def _build_base_url(request: Request) -> str:
    settings = get_settings()
    base = settings.public_url or settings.base_url or str(request.base_url)
    return base.rstrip("/")


def _store_uploaded_image(image_upload: UploadFile, *, request: Request, image_bytes: bytes) -> str:
    """Persist uploaded image and return absolute URL for Telegram."""
    target_dir = UPLOADS_DIR / "messaging"
    target_dir.mkdir(parents=True, exist_ok=True)

    original_suffix = Path(image_upload.filename or "").suffix.lower()
    safe_suffix = original_suffix if original_suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp"} else ".jpg"
    filename = f"{uuid4().hex}{safe_suffix}"
    destination = target_dir / filename
    destination.write_bytes(image_bytes)

    public_url = f"{_build_base_url(request)}/uploads/messaging/{filename}"
    log.info("messaging: stored image %s (%s bytes) -> %s", destination, len(image_bytes), public_url)
    return public_url


api = APIRouter(prefix="/api", tags=["api"])
api.include_router(activities_router)
api.include_router(sync_router)
api.include_router(vk_client_links_router, dependencies=[Depends(require_admin)])
api.include_router(intervals_links_router, dependencies=[Depends(require_admin)])
api.include_router(races_router)
api.include_router(messaging_router)

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


def _load_schedule_week_payload(week_id: int) -> Optional[dict]:
    week = schedule_repository.get_week(week_id)
    if not week:
        return None

    slots = schedule_repository.list_slots_with_reservations(week_id)
    if not slots:
        try:
            created = schedule_repository.create_default_slots_for_week(week_id)
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Failed to auto-create default slots for week %s: %s", week_id, exc)
            created = 0
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

    # Build ordering for stands to keep rows aligned on public page
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


@api.get("/config")
def api_config():
    settings = get_settings()
    return {
        "loginBotUsername": settings.telegram_login_bot_username,
        "clientsPageSize": settings.clients_page_size,
        "baseUrl": settings.base_url,
    }


@api.get("/session")
def api_session(user=Depends(require_user)):
    return {
        "user": jsonable_encoder(user.to_dict()),
        "isAdmin": is_admin_user(user),
    }


@api.get("/summary")
def api_summary(user=Depends(require_user)):
    return {
        "clients": client_repository.count_clients(),
        "bikes": bikes_repository.bikes_count(),
        "trainers": trainers_repository.trainers_count(),
        "admins": len(admin_repository.list_admins()),
        "links": len(client_link_repository.list_links()),
    }


@api.get("/clients")
def api_clients(
    page: int = 1,
    search: str | None = None,
    sort: str | None = None,
    direction: str = "asc",
    user=Depends(require_user),
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

    return _json_success(
        {
            "items": jsonable_encoder(rows),
            "pagination": {
                "page": page,
                "pageSize": limit,
                "total": total,
                "totalPages": total_pages,
            },
        }
    )


@api.get("/clients/{client_id}")
def api_get_client(client_id: int, user=Depends(require_user)):
    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"item": jsonable_encoder(record)}


def _serialize_activity_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized = dict(row)
    for ts_field in ("created_at", "start_time"):
        value = serialized.get(ts_field)
        if hasattr(value, "isoformat"):
            serialized[ts_field] = value.isoformat()
    return serialized


@api.get("/clients/{client_id}/activities")
def api_get_client_activities(client_id: int, user=Depends(require_user)):
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


@api.patch("/clients/{client_id}")
async def api_update_client(client_id: int, request: Request, user=Depends(require_admin)):
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

    # remove keys that remain unchanged compared to existing values
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
    updated = client_repository.get_client(client_id)
    if not updated:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"item": jsonable_encoder(updated)}


@api.post("/clients")
async def api_create_client(request: Request, user=Depends(require_admin)):
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


@api.delete("/clients/{client_id}")
def api_delete_client(client_id: int, user=Depends(require_admin)):
    # Remove linked Telegram accounts if present (FK may handle cascade but ensure consistency)
    client_link_repository.remove_link(client_id=client_id)

    deleted = client_repository.delete_client(client_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"ok": True}


@api.get("/bikes")
def api_bikes(user=Depends(require_user)):
    rows = bikes_repository.list_bikes()
    return _json_success({"items": jsonable_encoder(rows)})


@api.post("/bikes")
async def api_create_bike(request: Request, user=Depends(require_admin)):
    payload = await request.json()

    title = payload.get("title")
    if not isinstance(title, str) or not title.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Title is required")

    def _clean_str(value: object, *, allowed: set[str] | None = None) -> str | None:
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                return None
            if allowed is not None and trimmed not in allowed:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid value")
            return trimmed
        if value is None:
            return None
        if allowed is not None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid value")
        return None

    def _parse_height(value: object, field: str) -> float | None:
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

    try:
        record = bikes_repository.create_bike(
            title=title.strip(),
            owner=_clean_str(payload.get("owner")),
            size_label=_clean_str(payload.get("size_label")),
            frame_size_cm=_clean_str(payload.get("frame_size_cm")),
            height_min_cm=_parse_height(payload.get("height_min_cm"), "height_min_cm"),
            height_max_cm=_parse_height(payload.get("height_max_cm"), "height_max_cm"),
            gears=_clean_str(payload.get("gears")),
            axle_type=_clean_str(payload.get("axle_type"), allowed={"ЭКС", "ОСЬ"}),
            cassette=_clean_str(payload.get("cassette"), allowed={"7", "8", "9", "10", "11", "12"}),
        )
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Bike with this title already exists") from exc

    return {"item": jsonable_encoder(record)}


@api.patch("/bikes/{bike_id}")
async def api_update_bike(bike_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    updates: dict[str, object] = {}

    for key in ("height_min_cm", "height_max_cm"):
        if key in payload:
            value = payload[key]
            updates[key] = value

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    bikes_repository.update_bike_fields(bike_id, **updates)
    record = bikes_repository.get_bike(bike_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Bike not found")
    return {"item": jsonable_encoder(record)}


@api.get("/trainers")
def api_trainers(user=Depends(require_user)):
    rows = trainers_repository.list_trainers()
    return _json_success({"items": jsonable_encoder(rows)})


@api.patch("/trainers/{trainer_id}")
async def api_update_trainer(trainer_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    updates: dict[str, object] = {}
    for key in ("title", "display_name", "owner", "axle_types", "cassette"):
        if key in payload:
            updates[key] = payload[key]

    bike_assignment_handled = False
    if "bike_id" in payload:
        bike_assignment_handled = True
        bike_value = payload["bike_id"]
        if isinstance(bike_value, str):
            bike_value = bike_value.strip()
        if bike_value in (None, "", "null"):
            layout_repository.ensure_layout_table()
            layout_repository.clear_bike_assignment_for_stand(trainer_id)
        else:
            try:
                bike_id = int(bike_value)
            except (TypeError, ValueError):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid bike_id")
            bike_record = bikes_repository.get_bike(bike_id)
            if not bike_record:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Bike not found")
            layout_repository.ensure_layout_table()
            layout_repository.set_bike_assignment(trainer_id, bike_id, assigned_by=getattr(user, "id", None))

    for key, value in list(updates.items()):
        if isinstance(value, str):
            trimmed = value.strip()
            updates[key] = trimmed or None
        elif value in ("", None):
            updates[key] = None

    if not updates and not bike_assignment_handled:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    if updates:
        trainers_repository.update_trainer_fields(trainer_id, **updates)

    record = trainers_repository.get_trainer(trainer_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Trainer not found")
    return {"item": jsonable_encoder(record)}


@api.get("/instructors")
def api_instructors(user=Depends(require_user)):
    rows = instructors_repository.list_instructors()
    return _json_success({"items": jsonable_encoder(rows)})


@api.post("/instructors")
async def api_create_instructor(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    name = payload.get("full_name") or payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "full_name is required")
    try:
        record = instructors_repository.create_instructor(name.strip())
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to create instructor") from exc
    return {"item": jsonable_encoder(record)}


@api.delete("/instructors/{instructor_id}")
def api_delete_instructor(instructor_id: int, user=Depends(require_admin)):
    deleted = instructors_repository.delete_instructor(instructor_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Instructor not found")
    return {"status": "ok"}


@api.get("/schedule/weeks")
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

    return _json_success(
        {
            "items": jsonable_encoder(rows),
            "pagination": {
                "page": page,
                "pageSize": page_size,
                "total": total,
                "totalPages": total_pages,
            },
        }
    )


@api.post("/schedule/weeks")
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


@api.get("/schedule/weeks/{week_id}")
def api_get_schedule_week(week_id: int, user=Depends(require_user)):
    payload = _load_schedule_week_payload(week_id)
    if not payload:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    return payload


@api.patch("/schedule/weeks/{week_id}")
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


@api.delete("/schedule/weeks/{week_id}")
def api_delete_schedule_week(week_id: int, user=Depends(require_admin)):
    if not schedule_repository.get_week(week_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Week not found")
    schedule_repository.delete_week(week_id)
    log.info("schedule: deleted week %s", week_id)
    return {"status": "ok"}


@api.post("/schedule/weeks/{week_id}/copy")
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


@api.post("/schedule/slots")
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


@api.patch("/schedule/slots/{slot_id}")
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


@api.delete("/schedule/slots/{slot_id}")
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


@api.get("/schedule/slots/{slot_id}")
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


@api.post("/schedule/weeks/{week_id}/sync")
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


@api.post("/schedule/weeks/{week_id}/fill-template")
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




@api.patch("/schedule/reservations/{reservation_id}")
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


@api.post("/schedule/reservations/{reservation_id}/clear")
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


@api.get("/schedule/notifications")
def api_get_workout_notifications(page: int = 1, user=Depends(require_user)):
    """Get list of workout notifications."""
    page = max(page, 1)
    limit = 50
    offset = (page - 1) * limit
    
    try:
        notifications = schedule_repository.list_workout_notifications(limit=limit, offset=offset)
        total_count = len(notifications)  # In a real implementation, we'd get the actual count
        total_pages = max(math.ceil(total_count / limit), 1) if total_count > 0 else 1
        
        return _json_success({
            "items": jsonable_encoder(notifications),
            "pagination": {
                "page": page,
                "pageSize": limit,
                "total": total_count,
                "totalPages": total_pages,
            },
        })
    except Exception as exc:
        log.exception("Failed to fetch workout notifications")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch notifications") from exc


@api.get("/schedule/notification-settings")
def api_get_notification_settings(user=Depends(require_admin)):
    """Get workout notification settings."""
    try:
        settings = schedule_repository.get_workout_notification_settings()
        return {"settings": settings}
    except Exception as exc:
        log.exception("Failed to fetch notification settings")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch settings") from exc


@api.post("/schedule/notification-settings")
async def api_update_notification_settings(request: Request, user=Depends(require_admin)):
    """Update workout notification settings."""
    try:
        payload = await request.json()
        reminder_hours = payload.get("reminder_hours")
        
        if reminder_hours is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reminder_hours is required")
        
        if not isinstance(reminder_hours, int):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reminder_hours must be an integer")
        
        if reminder_hours < 1 or reminder_hours > 168:  # 1 hour to 1 week
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "reminder_hours must be between 1 and 168")
        
        # Update the settings (in a real implementation, this would be stored in the database)
        success = schedule_repository.update_workout_notification_settings(reminder_hours)
        
        if not success:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Failed to update settings")
        
        return {"settings": {"reminder_hours": reminder_hours}}
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("Failed to update notification settings")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to update settings") from exc


@api.get("/admins")
def api_admins(user=Depends(require_admin)):
    rows = admin_repository.list_admins()
    return _json_success({"items": jsonable_encoder(rows)})


@api.post("/admins")
async def api_add_admin(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    tg_id = payload.get("tg_id")
    username = payload.get("username")
    display_name = payload.get("display_name")

    if tg_id is None and not username:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Username or tg_id required")

    if tg_id is not None:
        try:
            tg_id = int(tg_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid tg_id") from exc
    if isinstance(username, str):
        username = username.strip() or None
    if isinstance(display_name, str):
        display_name = display_name.strip() or None

    _, record = admin_repository.add_admin(
        tg_id=tg_id,
        username=username,
        display_name=display_name,
    )
    return {"item": jsonable_encoder(record)}


@api.delete("/admins/{admin_id}")
def api_remove_admin(
    admin_id: int,
    tg_id: Optional[int] = None,
    username: Optional[str] = None,
    user=Depends(require_admin),
):
    if tg_id is None and (username is None or username == ""):
        # fallback: find admin by id to determine identifiers
        for row in admin_repository.list_admins():
            if row.get("id") == admin_id:
                tg_id = row.get("tg_id")
                username = row.get("username")
                break
    admin_repository.remove_admin(tg_id=tg_id, username=username)
    return {"status": "ok"}


@api.get("/client-links")
def api_client_links(user=Depends(require_admin)):
    rows = client_link_repository.list_links()

    straver_statuses = {}
    try:
        straver = StraverClient()
        if straver.is_configured():
            tg_ids = [row["tg_user_id"] for row in rows if row.get("tg_user_id")]
            straver_statuses = straver.connection_status(tg_ids)
    except Exception:
        log.exception("Failed to fetch Straver statuses")

    enriched = []
    for row in rows:
        status = straver_statuses.get(int(row["tg_user_id"])) if row.get("tg_user_id") else None
        merged = dict(row)
        merged["strava_connected"] = bool(status and status.get("connected"))
        merged["strava_athlete_name"] = status.get("athlete_name") if status else None
        if status and status.get("athlete_id") and not merged.get("strava_athlete_id"):
            merged["strava_athlete_id"] = status.get("athlete_id")
        enriched.append(merged)

    return _json_success({"items": jsonable_encoder(enriched)})


def _parse_client_link_payload(payload: dict) -> tuple[int, str | None, str | None]:
    tg_user_id = payload.get("tg_user_id")
    if tg_user_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_id required")

    try:
        tg_user_id_int = int(tg_user_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid tg_user_id") from exc
    if tg_user_id_int <= 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_id must be positive")

    tg_username = payload.get("tg_username")
    if tg_username is not None and not isinstance(tg_username, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_username must be a string")
    if isinstance(tg_username, str):
        tg_username = tg_username.strip() or None

    tg_full_name = payload.get("tg_full_name")
    if tg_full_name is not None and not isinstance(tg_full_name, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_full_name must be a string")
    if isinstance(tg_full_name, str):
        tg_full_name = tg_full_name.strip() or None

    return tg_user_id_int, tg_username, tg_full_name


@api.post("/client-links")
async def api_create_client_link(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    client_id_raw = payload.get("client_id")
    if client_id_raw is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "client_id required")
    try:
        client_id = int(client_id_raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid client_id") from exc
    if client_id <= 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "client_id must be positive")

    client = client_repository.get_client(client_id)
    if not client:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    tg_user_id_int, tg_username, tg_full_name = _parse_client_link_payload(payload)
    try:
        record = client_link_repository.link_user_to_client(
            tg_user_id=tg_user_id_int,
            client_id=client_id,
            tg_username=tg_username,
            tg_full_name=tg_full_name,
        )
    except Exception:
        log.exception("Failed to create client link for client %s", client_id)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to create link")

    return {"item": jsonable_encoder(record)}


@api.patch("/client-links/{client_id}")
async def api_update_client_link(client_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    if not client_repository.get_client(client_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    tg_user_id_int, tg_username, tg_full_name = _parse_client_link_payload(payload)
    try:
        record = client_link_repository.link_user_to_client(
            tg_user_id=tg_user_id_int,
            client_id=client_id,
            tg_username=tg_username,
            tg_full_name=tg_full_name,
        )
    except Exception:
        log.exception("Failed to update client link for client %s", client_id)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to update link")

    return {"item": jsonable_encoder(record)}


@api.delete("/client-links/{client_id}")
def api_delete_client_link(client_id: int, user=Depends(require_admin)):
    client_link_repository.remove_link(client_id=client_id)
    return {"status": "ok"}


@api.get("/strava/authorize")
def strava_authorize(state: str = None, user=Depends(require_admin)):
    """Generate Strava authorization URL for a client."""
    try:
        straver = StraverClient()
        if not straver.is_configured():
            raise RuntimeError("STRAVER_BASE_URL/STRAVER_INTERNAL_SECRET are not configured")
        auth_url = straver.build_authorize_url(state=state or "")
        return RedirectResponse(url=auth_url)
    except Exception as exc:
        log.exception("Failed to generate Strava authorization URL")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to generate Strava authorization URL") from exc


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(title="Крутилка Admin API")
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret_key,
        same_site="lax",
        https_only=False,
    )
    app.include_router(api)

    @app.on_event("startup")
    def _startup_seed_instructors() -> None:
        """Ensure instructor directory exists before first request."""
        try:
            instructors_repository.ensure_instructors_table()
            message_repository.ensure_user_messages_table()
            ensure_uploads_dir()
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Failed to ensure instructors table on startup: %s", exc)

    @app.get("/")
    def root():
        return RedirectResponse(url="/app", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/schedule/{slug}")
    def schedule_week(slug: str, request: Request):
        # Convert slug to week start date
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
                        "error_message": "Недопустимый формат даты в URL"
                    }
                )
                response.headers["Cache-Control"] = "no-store"
                return response
        
        # Get or create the week
        try:
            week = schedule_repository.get_or_create_week(week_start_date=week_start)
        except Exception as e:
            log.error("Failed to get or create week for %s: %s", week_start, e)
            response = templates.TemplateResponse(
                "public_schedule.html",
                {
                    "request": request,
                    "error_message": "Не удалось загрузить расписание"
                }
            )
            response.headers["Cache-Control"] = "no-store"
            return response
        
        # Load schedule data
        payload = _load_schedule_week_payload(week["id"])
        if not payload:
            response = templates.TemplateResponse(
                "public_schedule.html",
                {
                    "request": request,
                    "error_message": "Не удалось загрузить данные расписания"
                }
            )
            response.headers["Cache-Control"] = "no-store"
            return response
        
        # Build day columns for display
        day_columns = _build_day_columns(
            payload["slots"],
            week["week_start_date"],
            payload["instructors"]
        )
        
        # Calculate navigation slugs
        current_week_start = date.today() - timedelta(days=date.today().weekday())
        prev_week_start = week_start - timedelta(days=7)
        next_week_start = week_start + timedelta(days=7)
        
        prev_week_slug = _format_week_slug(prev_week_start)
        next_week_slug = _format_week_slug(next_week_start)
        current_week_slug = _format_week_slug(current_week_start)
        
        # Format week range label
        week_range_label = _format_week_range_label(week["week_start_date"])
        
        # Context for template
        context = {
            "request": request,
            "week": week,
            "day_columns": day_columns,
            "prev_week_slug": prev_week_slug,
            "next_week_slug": next_week_slug,
            "canonical_slug": slug,
            "week_range_label": week_range_label,
            "share_url": f"{request.url.scheme}://{request.url.netloc}/schedule/{slug}"
        }
        
        response = templates.TemplateResponse("public_schedule.html", context)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/schedule")
    def schedule_default():
        return RedirectResponse(url="/schedule/current_week", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/race/{slug}", response_class=HTMLResponse)
    def public_race_page(slug: str, request: Request):
        context = {"request": request}
        race = race_repository.get_race_by_slug(slug)
        if not race:
            context["error"] = "Гонка не найдена или ещё не опубликована."
            return templates.TemplateResponse("public_race.html", context)

        race_date_raw = race.get("race_date")
        if isinstance(race_date_raw, date):
            race_date = race_date_raw
        elif isinstance(race_date_raw, str):
            try:
                race_date = datetime.strptime(race_date_raw, "%Y-%m-%d").date()
            except ValueError:
                race_date = None
        else:
            race_date = None

        registrations = race_repository.list_registrations(race["id"])
        participants: list[dict] = []
        pending_count = 0
        clusters_meta = race.get("clusters") or []
        cluster_times: dict[str, dict[str, Optional[str]]] = {}
        cluster_order_keys: list[tuple] = []
        for cluster in clusters_meta:
            label = (cluster.get("label") or cluster.get("code") or "").strip()
            start_time = (cluster.get("start_time") or "").strip() or None
            end_time = (cluster.get("end_time") or "").strip() or None
            if label:
                cluster_times[label] = {
                    "start_time": start_time,
                    "end_time": end_time,
                }
                try:
                    start_key = datetime.strptime(start_time, "%H:%M").time() if start_time else None
                except ValueError:
                    start_key = None
                cluster_order_keys.append((start_key is None, start_key, label.lower(), label))

        ordered_cluster_labels = [item[3] for item in sorted(cluster_order_keys, key=lambda entry: entry)]

        client_stand_map: dict[int, str] = {}
        if race_date:
            week = schedule_repository.get_week_by_start(race_date)
            if week:
                slots = schedule_repository.list_slots_with_reservations(week["id"])
                trainers = trainers_repository.list_trainers()
                trainers_map = {row["id"]: row for row in trainers if isinstance(row.get("id"), int)}

                for slot in slots:
                    slot_date = slot.get("slot_date")
                    if slot_date and str(slot_date) != race_date.isoformat():
                        continue
                    if slot.get("session_kind") != "race":
                        continue
                    for res in slot.get("reservations") or []:
                        if res.get("status") != "booked":
                            continue
                        client_id = res.get("client_id")
                        if not isinstance(client_id, int):
                            continue
                        label_parts: list[str] = []
                        stand_id = res.get("stand_id")
                        trainer = trainers_map.get(stand_id)
                        stand_code = (res.get("stand_code") or "").strip()
                        if trainer:
                            code = (trainer.get("code") or "").strip()
                            if code:
                                label_parts.append(code)
                        if not label_parts and stand_code:
                            label_parts.append(stand_code)
                        if not label_parts and stand_id is not None:
                            label_parts.append(f"Станок {stand_id}")
                        if label_parts:
                            client_stand_map[client_id] = " · ".join(label_parts)

        for entry in registrations:
            status_value = (entry.get("status") or "").lower()
            stand_label = None
            client_id = entry.get("client_id")
            if isinstance(client_id, int):
                stand_label = client_stand_map.get(client_id)
            payload = {
                "name": entry.get("client_name")
                or entry.get("tg_full_name")
                or (f"@{entry.get('tg_username')}" if entry.get("tg_username") else f"ID {entry.get('client_id')}"),
                "cluster": entry.get("cluster_label"),
                "notes": entry.get("notes"),
                "is_pending": False,
                "race_mode": entry.get("race_mode"),
                "stand_label": stand_label,
            }
            submitted = entry.get("payment_submitted_at")
            if hasattr(submitted, "isoformat"):
                payload["submitted"] = submitted.isoformat()
            elif submitted:
                payload["submitted"] = str(submitted)
            if status_value == race_repository.RACE_STATUS_APPROVED:
                participants.append(payload)
            elif status_value == race_repository.RACE_STATUS_PENDING:
                payload["is_pending"] = True
                pending_count += 1
                participants.append(payload)

        price_value = race.get("price_rub")
        price_label = (
            f"{int(price_value):,}".replace(",", " ") if isinstance(price_value, (int, float)) else None
        )

        description_raw = (race.get("description") or "").strip()
        if description_raw:
            description_formatted = "<br>".join(description_raw.splitlines())
        else:
            description_formatted = None

        grouped_participants_offline: list[dict] = []
        grouped_participants_online: list[dict] = []
        groups_map_offline: dict[str, list] = {}
        groups_map_online: dict[str, list] = {}
        unassigned_label = "Кластер не назначен"
        for item in participants:
            mode_key = (item.get("race_mode") or "").strip().lower()
            target_map = groups_map_offline if mode_key != "online" else groups_map_online
            label = (item.get("cluster") or "").strip() or unassigned_label
            target_map.setdefault(label, []).append(item)

        for label in ordered_cluster_labels:
            if label in groups_map_offline:
                members = groups_map_offline.pop(label)
                members.sort(key=lambda x: ((x.get("stand_label") or "станокzzz").lower(), x.get("name") or ""))
                grouped_participants_offline.append({"label": label, "participants": members})
        for label in sorted(groups_map_offline.keys()):
            members = groups_map_offline[label]
            members.sort(key=lambda x: ((x.get("stand_label") or "станокzzz").lower(), x.get("name") or ""))
            grouped_participants_offline.append({"label": label, "participants": members})

        for label in ordered_cluster_labels:
            if label in groups_map_online:
                members = groups_map_online.pop(label)
                members.sort(key=lambda x: (x.get("name") or ""))
                grouped_participants_online.append({"label": label, "participants": members})
        for label in sorted(groups_map_online.keys()):
            members = groups_map_online[label]
            members.sort(key=lambda x: (x.get("name") or ""))
            grouped_participants_online.append({"label": label, "participants": members})

        race_payload = {
            "title": race.get("title"),
            "date_label": _format_race_date_label(race.get("race_date")),
            "price_label": price_label,
            "sbp_phone": race.get("sbp_phone"),
            "payment_instructions": race.get("payment_instructions"),
            "notes": race.get("notes"),
            "description": description_formatted,
            "slug": race.get("slug"),
            "is_active": race.get("is_active"),
            "clusters": race.get("clusters") or [],
        }

        share_url = f"{request.url.scheme}://{request.url.netloc}/race/{race_payload['slug']}"

        context.update(
            {
                "race": race_payload,
                "participants": participants,
                "participant_groups_offline": grouped_participants_offline,
                "participant_groups_online": grouped_participants_online,
                "participants_count": len(participants),
                "pending_count": pending_count,
                "share_url": share_url,
                "cluster_times": cluster_times,
            }
        )
        return templates.TemplateResponse("public_race.html", context)

    @app.get("/race")
    def race_default_redirect():
        return RedirectResponse(url="/schedule", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/auth/telegram")
    async def telegram_auth(request: Request, next: Optional[str] = None):
        settings = get_settings()
        allowed_keys = {
            "id",
            "first_name",
            "last_name",
            "username",
            "photo_url",
            "auth_date",
            "hash",
        }
        payload = {key: value for key, value in request.query_params.items() if key in allowed_keys}

        try:
            user = verify_telegram_payload(
                payload,
                bot_token=settings.telegram_login_bot_token,
            )
        except TelegramAuthError as exc:
            target = _safe_next(next)
            separator = "&" if "?" in target else "?"
            target = f"{target}{separator}error={quote(str(exc))}"
            return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)

        request.session[SESSION_KEY_USER] = user.to_dict()
        target = _safe_next(next)
        return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)

    @app.get("/strava/callback")
    async def strava_oauth_callback(request: Request, code: str = None, error: str = None, state: str = None):
        """Forward Strava OAuth callback to the Straver service."""
        try:
            straver = StraverClient()
            if not straver.base_url:
                raise RuntimeError("STRAVER_BASE_URL is not configured")
            forward_url = f"{straver.base_url}/strava/callback"
            if request.url.query:
                forward_url = f"{forward_url}?{request.url.query}"
            return RedirectResponse(url=forward_url)
        except Exception as exc:
            log.exception("Failed to forward Strava callback")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Strava callback is now handled by Straver: {exc}",
            ) from exc

    @app.get("/api/health")
    def api_health():
        return {"status": "ok"}

    @app.get("/logout")
    def logout(request: Request):
        request.session.pop(SESSION_KEY_USER, None)
        return RedirectResponse(url="/app", status_code=status.HTTP_303_SEE_OTHER)

    @app.get("/api/logout")
    def api_logout(request: Request):
        request.session.pop(SESSION_KEY_USER, None)
        return {"status": "ok"}

    dist_root = FRONTEND_DIST.resolve()
    assets_dir = dist_root / "assets"
    if assets_dir.exists():
        app.mount("/app/assets", StaticFiles(directory=str(assets_dir), html=False), name="frontend-assets")

    if MESSAGING_UPLOADS_DIR.exists():
        app.mount("/uploads", StaticFiles(directory=str(MESSAGING_UPLOADS_DIR), html=False), name="uploads")
    try:
        fit_dir = schedule_repository.ensure_fit_files_dir()
        if fit_dir.exists():
            app.mount("/fitfiles", StaticFiles(directory=str(fit_dir), html=False), name="fitfiles")
    except Exception:
        log.exception("Failed to mount FIT files directory")

    index_file = dist_root / "index.html"

    @lru_cache()
    def _index_html() -> str | None:
        if not index_file.exists():
            return None
        try:
            return index_file.read_text(encoding="utf-8")
        except Exception:  # pylint: disable=broad-except
            return None

    def _spa_response_for(request: Request) -> Response:
        html = _index_html()
        if html is None:
            return JSONResponse(
                {
                    "detail": "Frontend is not built yet. Run npm install && npm run build inside webapp/frontend.",
                },
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return HTMLResponse(html)

    @app.get("/app", response_class=HTMLResponse)
    def serve_spa_root(request: Request):
        user = get_current_user(request)
        if not user:
            return _spa_response_for(request)

        if is_admin_user(user):
            return _spa_response_for(request)

        context = {
            "request": request,
        }
        return templates.TemplateResponse("non_admin_placeholder.html", context)

    @app.get("/app/{path:path}")
    def serve_spa_asset(request: Request, path: str):
        user = get_current_user(request)
        if not user:
            target = (dist_root / path).resolve()
            if dist_root in target.parents or target == dist_root:
                if target.is_file():
                    return FileResponse(target)
            return _spa_response_for(request)

        if not is_admin_user(user):
            context = {
                "request": request,
            }
            return templates.TemplateResponse("non_admin_placeholder.html", context)

        target = (dist_root / path).resolve()
        if dist_root in target.parents or target == dist_root:
            if target.is_file():
                return FileResponse(target)
        return _spa_response_for(request)

    return app


app = create_app()
