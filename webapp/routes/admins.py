"""Admin management routes."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder

from repositories import admin_repository
from ..dependencies import require_admin

router = APIRouter(prefix="/admins", tags=["admins"], dependencies=[Depends(require_admin)])


@router.get("")
def api_admins():
    rows = admin_repository.list_admins()
    return {"items": jsonable_encoder(rows)}


@router.post("")
async def api_add_admin(request: Request):
    payload = await request.json()
    tg_id = payload.get("tg_id")
    username = payload.get("username")
    display_name = payload.get("display_name")
    instructor_id = payload.get("instructor_id")
    notify_booking_events = payload.get("notify_booking_events")
    notify_instructor_only = payload.get("notify_instructor_only")

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
    if instructor_id is not None:
        try:
            instructor_id = int(instructor_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid instructor_id") from exc
    if notify_booking_events is not None:
        notify_booking_events = bool(notify_booking_events)
    if notify_instructor_only is not None:
        notify_instructor_only = bool(notify_instructor_only)

    _, record = admin_repository.add_admin(
        tg_id=tg_id,
        username=username,
        display_name=display_name,
        instructor_id=instructor_id,
        notify_booking_events=notify_booking_events,
        notify_instructor_only=notify_instructor_only,
    )
    return {"item": jsonable_encoder(record)}


@router.delete("/{admin_id}")
def api_remove_admin(
    admin_id: int,
    tg_id: Optional[int] = None,
    username: Optional[str] = None,
):
    if tg_id is None and (username is None or username == ""):
        for row in admin_repository.list_admins():
            if row.get("id") == admin_id:
                tg_id = row.get("tg_id")
                username = row.get("username")
                break
    admin_repository.remove_admin(tg_id=tg_id, username=username)
    return {"status": "ok"}


@router.patch("/{admin_id}")
async def api_update_admin(admin_id: int, request: Request):
    payload = await request.json()
    tg_id = payload.get("tg_id")
    username = payload.get("username")
    display_name = payload.get("display_name")
    instructor_id = payload.get("instructor_id")
    notify_booking_events = payload.get("notify_booking_events")
    notify_instructor_only = payload.get("notify_instructor_only")

    if tg_id is not None:
        try:
            tg_id = int(tg_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid tg_id") from exc
    if isinstance(username, str):
        username = username.strip() or None
    if isinstance(display_name, str):
        display_name = display_name.strip() or None
    if instructor_id is not None:
        try:
            instructor_id = int(instructor_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid instructor_id") from exc
    if notify_booking_events is not None:
        notify_booking_events = bool(notify_booking_events)
    if notify_instructor_only is not None:
        notify_instructor_only = bool(notify_instructor_only)

    updated = admin_repository.update_admin(
        admin_id,
        tg_id=tg_id,
        username=username,
        display_name=display_name,
        instructor_id=instructor_id,
        notify_booking_events=notify_booking_events,
        notify_instructor_only=notify_instructor_only,
    )
    if not updated:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Admin not found")
    return {"item": jsonable_encoder(updated)}


@router.get("/{admin_id}")
def api_get_admin(admin_id: int):
    admin = admin_repository.get_admin(admin_id)
    if not admin:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Admin not found")
    return {"item": jsonable_encoder(admin)}
