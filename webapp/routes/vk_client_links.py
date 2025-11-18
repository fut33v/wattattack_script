"""Routes for managing VK client links."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder

from repositories import client_repository, vk_client_link_repository

router = APIRouter(prefix="/vk-client-links", tags=["client-links"])


def _parse_vk_client_link_payload(payload: dict) -> tuple[int, str | None, str | None]:
    vk_user_id = payload.get("vk_user_id")
    if vk_user_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "vk_user_id required")

    try:
        vk_user_id_int = int(vk_user_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid vk_user_id") from exc
    if vk_user_id_int <= 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "vk_user_id must be positive")

    vk_username = payload.get("vk_username")
    if vk_username is not None and not isinstance(vk_username, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "vk_username must be a string")
    if isinstance(vk_username, str):
        vk_username = vk_username.strip() or None

    vk_full_name = payload.get("vk_full_name")
    if vk_full_name is not None and not isinstance(vk_full_name, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "vk_full_name must be a string")
    if isinstance(vk_full_name, str):
        vk_full_name = vk_full_name.strip() or None

    return vk_user_id_int, vk_username, vk_full_name


@router.get("")
def api_vk_client_links():
    rows = vk_client_link_repository.list_links()
    return {"items": jsonable_encoder(rows)}


@router.post("")
async def api_create_vk_client_link(request: Request):
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

    vk_user_id_int, vk_username, vk_full_name = _parse_vk_client_link_payload(payload)
    try:
        record = vk_client_link_repository.link_vk_user_to_client(
            vk_user_id=vk_user_id_int,
            client_id=client_id,
            vk_username=vk_username,
            vk_full_name=vk_full_name,
        )
    except Exception:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to create vk link")

    return {"item": jsonable_encoder(record)}


@router.patch("/{client_id}")
async def api_update_vk_client_link(client_id: int, request: Request):
    payload = await request.json()
    if not client_repository.get_client(client_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    vk_user_id_int, vk_username, vk_full_name = _parse_vk_client_link_payload(payload)
    try:
        record = vk_client_link_repository.link_vk_user_to_client(
            vk_user_id=vk_user_id_int,
            client_id=client_id,
            vk_username=vk_username,
            vk_full_name=vk_full_name,
        )
    except Exception:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to update vk link")

    return {"item": jsonable_encoder(record)}


@router.delete("/{client_id}")
def api_delete_vk_client_link(client_id: int):
    vk_client_link_repository.remove_link(client_id=client_id)
    return {"status": "ok"}
