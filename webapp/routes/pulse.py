"""Pulse dashboard endpoints for booking/client notifications."""
from __future__ import annotations

import math

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder

from repositories import booking_notifications_repository
from ..dependencies import require_admin

router = APIRouter(prefix="/pulse", tags=["pulse"], dependencies=[Depends(require_admin)])


@router.get("/notifications")
def list_notifications(page: int = 1):
    """List booking/client notifications with simple pagination."""
    if page < 1:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "page must be >= 1")

    page_size = 50
    offset = (page - 1) * page_size
    total = booking_notifications_repository.count_notifications()
    items = booking_notifications_repository.list_notifications(limit=page_size, offset=offset)
    total_pages = max(1, math.ceil(total / page_size)) if total else 1

    return {
        "items": jsonable_encoder(items),
        "pagination": {
            "page": page,
            "pageSize": page_size,
            "total": total,
            "totalPages": total_pages,
        },
    }
