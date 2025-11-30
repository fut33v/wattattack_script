from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from repositories import client_groups_repository
from ..dependencies import require_admin, require_user

router = APIRouter(
    prefix="/groups",
    tags=["groups"],
    dependencies=[Depends(require_user), Depends(require_admin)],
)


@router.get("")
def api_list_groups():
    groups = client_groups_repository.list_all_groups()
    return {"items": groups}


@router.post("")
async def api_create_group(request: Request):
    payload = await request.json()
    group_name = payload.get("group_name") if isinstance(payload, dict) else None
    if not isinstance(group_name, str) or not group_name.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "group_name is required")

    created = client_groups_repository.upsert_group(group_name)
    if not created:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "group_name is required")

    groups = client_groups_repository.list_all_groups()
    return {"items": groups}


@router.delete("")
def api_delete_group(group_name: str):
    if not group_name or not group_name.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "group_name is required")

    client_groups_repository.delete_group(group_name)
    groups = client_groups_repository.list_all_groups()
    return {"items": groups}
