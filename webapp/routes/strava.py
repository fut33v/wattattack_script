"""Strava integration callbacks and redirects."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from ..config import get_settings
from ..dependencies import require_admin
from straver_client import StraverClient

log = logging.getLogger(__name__)

router = APIRouter(tags=["strava"])


@router.get("/strava/authorize")
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


@router.get("/strava/callback")
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
