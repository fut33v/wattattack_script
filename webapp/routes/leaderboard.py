from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from repositories import schedule_repository

router = APIRouter(prefix="/leaderboard", tags=["leaderboard"])


@router.get("")
def api_public_leaderboard(limit: int = Query(100, ge=1, le=500)):
    """Expose leaderboard for public pages without authentication."""

    try:
        return schedule_repository.get_distance_leaderboard(limit=limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to load leaderboard") from exc
