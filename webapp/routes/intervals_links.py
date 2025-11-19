"""Routes for managing Intervals.icu links (Telegram user ID -> API key)."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder

from repositories import intervals_link_repository, client_link_repository, client_repository

router = APIRouter(prefix="/intervals-links", tags=["intervals"])


def _parse_intervals_payload(payload: dict) -> tuple[int, str, str]:
    tg_user_id = payload.get("tg_user_id")
    if tg_user_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_id required")
    try:
        tg_user_id_int = int(tg_user_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid tg_user_id") from exc
    if tg_user_id_int <= 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_id must be positive")

    api_key = payload.get("intervals_api_key")
    if not api_key or not isinstance(api_key, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "intervals_api_key required")
    api_key = api_key.strip()
    if not api_key:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "intervals_api_key cannot be empty")

    athlete_id_raw = payload.get("intervals_athlete_id")
    athlete_id = str(athlete_id_raw or "0")

    return tg_user_id_int, api_key, athlete_id


@router.get("")
def list_intervals_links():
    rows = intervals_link_repository.list_links()
    enriched = [_enrich_link(row) for row in rows]
    return {"items": jsonable_encoder(enriched)}


@router.post("")
async def create_intervals_link(request: Request):
    payload = await request.json()
    tg_user_id, api_key, athlete_id = _parse_intervals_payload(payload)
    try:
        record = intervals_link_repository.upsert_link(
            tg_user_id=tg_user_id, api_key=api_key, athlete_id=athlete_id
        )
    except Exception:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to create intervals link")
    return {"item": jsonable_encoder(_enrich_link(record))}


@router.patch("/{tg_user_id}")
async def update_intervals_link(tg_user_id: int, request: Request):
    payload = await request.json()
    payload["tg_user_id"] = tg_user_id
    tg_user_id, api_key, athlete_id = _parse_intervals_payload(payload)
    try:
        record = intervals_link_repository.upsert_link(
            tg_user_id=tg_user_id, api_key=api_key, athlete_id=athlete_id
        )
    except Exception:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to update intervals link")
    return {"item": jsonable_encoder(_enrich_link(record))}


@router.delete("/{tg_user_id}")
def delete_intervals_link(tg_user_id: int):
    intervals_link_repository.remove_link(tg_user_id=tg_user_id)
    return {"status": "ok"}


def _enrich_link(row: dict) -> dict:
    link = dict(row)
    link["client_id"] = None
    link["client_name"] = None
    try:
        client_link = client_link_repository.get_link_by_user(link["tg_user_id"])
        if client_link and client_link.get("client_id"):
            client = client_repository.get_client(client_link["client_id"])
            link["client_id"] = client_link["client_id"]
            if client:
                full_name = client.get("full_name") or ""
                if not full_name:
                    first = (client.get("first_name") or "").strip()
                    last = (client.get("last_name") or "").strip()
                    full_name = f"{first} {last}".strip()
                link["client_name"] = full_name or None
    except Exception:
        pass
    return link
