"""Routes for managing Telegram client links."""
from __future__ import annotations

import logging
from typing import Iterable

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder

from repositories import client_link_repository, client_repository
from straver_client import StraverClient

router = APIRouter(prefix="/client-links", tags=["client-links"])
log = logging.getLogger(__name__)


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


def _parse_client_id(payload: dict) -> int:
    client_id_raw = payload.get("client_id")
    if client_id_raw is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "client_id required")
    try:
        client_id = int(client_id_raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid client_id") from exc
    if client_id <= 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "client_id must be positive")
    return client_id


def _ensure_client_exists(client_id: int) -> None:
    if not client_repository.get_client(client_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")


def _straver_statuses(rows: Iterable[dict]) -> dict[int, dict]:
    try:
        straver = StraverClient()
        if not straver.is_configured():
            return {}
        tg_ids = [row["tg_user_id"] for row in rows if row.get("tg_user_id")]
        return straver.connection_status(tg_ids)
    except Exception:
        log.exception("Failed to fetch Straver statuses")
        return {}


def _enrich_with_strava(rows: list[dict]) -> list[dict]:
    statuses = _straver_statuses(rows)
    enriched: list[dict] = []
    for row in rows:
        status_row = statuses.get(int(row["tg_user_id"])) if row.get("tg_user_id") else None
        merged = dict(row)
        merged["strava_connected"] = bool(status_row and status_row.get("connected"))
        merged["strava_athlete_name"] = status_row.get("athlete_name") if status_row else None
        if status_row and status_row.get("athlete_id") and not merged.get("strava_athlete_id"):
            merged["strava_athlete_id"] = status_row.get("athlete_id")
        enriched.append(merged)
    return enriched


@router.get("")
def api_client_links():
    rows = client_link_repository.list_links()
    return {"items": jsonable_encoder(_enrich_with_strava(rows))}


@router.post("")
async def api_create_client_link(request: Request):
    payload = await request.json()
    client_id = _parse_client_id(payload)
    _ensure_client_exists(client_id)

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


@router.patch("/{client_id}")
async def api_update_client_link(client_id: int, request: Request):
    payload = await request.json()
    _ensure_client_exists(client_id)

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


@router.delete("/{client_id}")
def api_delete_client_link(client_id: int):
    _ensure_client_exists(client_id)
    client_link_repository.remove_link(client_id=client_id)
    return {"status": "ok"}
