"""Pedals management routes."""
from __future__ import annotations

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from repositories import pedals_repository
from ..dependencies import require_admin, require_user

router = APIRouter(prefix="/pedals", tags=["pedals"])

ALLOWED_TYPES = set(pedals_repository.PEDAL_TYPES)


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


@router.get("")
def api_pedals(user=Depends(require_user)):
    rows = pedals_repository.list_pedals()
    return _json_success({"items": jsonable_encoder(rows)})


@router.post("")
async def api_create_pedal(request: Request, user=Depends(require_admin)):
    payload = await request.json()

    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Name is required")

    pedal_type = payload.get("pedal_type")
    if not isinstance(pedal_type, str) or pedal_type not in ALLOWED_TYPES:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid pedal_type")

    try:
        record = pedals_repository.create_pedal(name=name.strip(), pedal_type=pedal_type)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Pedal with this name already exists") from exc

    return {"item": jsonable_encoder(record)}


@router.delete("/{pedal_id}")
def api_delete_pedal(pedal_id: int, user=Depends(require_admin)):
    deleted = pedals_repository.delete_pedal(pedal_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Pedal not found")
    return _json_success({"ok": True})
