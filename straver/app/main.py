"""Straver – a thin Strava-facing service with its own storage."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import requests
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from . import db
from .config import get_settings
from .strava_client import StravaClient

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = get_settings()
db.configure(settings.database_url)
db.ensure_tables()

app = FastAPI(title="Straver")


def _strava_client(access_token: Optional[str] = None) -> StravaClient:
    return StravaClient(
        client_id=settings.strava_client_id,
        client_secret=settings.strava_client_secret,
        redirect_uri=settings.strava_redirect_uri,
        access_token=access_token,
    )


def _persist_token(tg_user_id: int, token_data: Dict) -> Dict:
    athlete = token_data.get("athlete") or {}
    expires_at_value = datetime.utcfromtimestamp(int(token_data["expires_at"]))
    return db.save_tokens(
        tg_user_id=tg_user_id,
        access_token=token_data["access_token"],
        refresh_token=token_data["refresh_token"],
        expires_at=expires_at_value,
        athlete_id=athlete.get("id"),
        athlete_firstname=athlete.get("firstname"),
        athlete_lastname=athlete.get("lastname"),
    )


def _refresh_if_needed(record: Dict) -> Dict:
    expires_at = record.get("expires_at")
    if not expires_at:
        return record
    try:
        expires_dt = datetime.fromisoformat(str(expires_at))
        expires_ts = int(expires_dt.timestamp())
    except Exception:  # pylint: disable=broad-except
        log.warning("Failed to parse expires_at for %s: %s", record.get("tg_user_id"), expires_at)
        return record

    if not StravaClient.is_token_expired(expires_ts):
        return record

    log.info("Refreshing Strava token for user %s", record.get("tg_user_id"))
    refreshed = _strava_client().refresh_access_token(record["refresh_token"])
    return _persist_token(int(record["tg_user_id"]), refreshed)


def require_internal_secret(x_internal_secret: str = Header(default="")) -> None:
    """Ensure internal calls include the shared secret."""
    if not settings.internal_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Internal secret is not configured",
        )
    if x_internal_secret != settings.internal_secret:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid internal secret")


def _login_bot_redirect(message: str) -> Optional[str]:
    """Build redirect to Telegram login bot if configured."""
    username = settings.telegram_login_bot_username
    if not username:
        return None
    encoded = quote_plus(message)
    return f"https://t.me/{username}?text={encoded}"


def _notify_user(tg_user_id: int, text: str) -> None:
    """Send a Telegram message if bot token is configured."""
    if not settings.telegram_bot_token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
            json={"chat_id": tg_user_id, "text": text},
            timeout=10,
        )
    except Exception:  # pylint: disable=broad-except
        log.exception("Failed to send Telegram notification to %s", tg_user_id)


class StatusRequest(BaseModel):
    user_ids: List[int] = Field(default_factory=list)


class UploadResponse(BaseModel):
    status: str
    upload: Dict


class DisconnectRequest(BaseModel):
    tg_user_id: int


@app.get("/api/health")
def api_health():
    return {"status": "ok"}


@app.get("/strava/authorize")
def strava_authorize(state: str = ""):
    """Generate Strava authorization URL for the user."""
    try:
        auth_url = _strava_client().get_authorization_url(state=state or "")
        return RedirectResponse(url=auth_url)
    except Exception as exc:  # pylint: disable=broad-except
        log.exception("Failed to generate Strava authorization URL")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to generate Strava authorization URL") from exc


@app.get("/strava/callback")
def strava_callback(code: Optional[str] = None, error: Optional[str] = None, state: Optional[str] = None):
    """Handle Strava OAuth callback and save tokens."""
    if error:
        log.warning("Strava callback returned error: %s", error)
        target = _login_bot_redirect(f"Strava authorization failed: {error}")
        if target:
            return RedirectResponse(url=target)
        raise HTTPException(status.HTTP_400_BAD_REQUEST, error)

    if not code:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing authorization code")
    if not state:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing state with Telegram user ID")

    try:
        tg_user_id = int(state)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid Telegram user ID") from exc

    try:
        token_data = _strava_client().exchange_code_for_token(code)
        record = _persist_token(tg_user_id, token_data)
        athlete = token_data.get("athlete") or {}
        athlete_name = f"{athlete.get('firstname', '')} {athlete.get('lastname', '')}".strip() or "Strava user"
        _notify_user(
            tg_user_id,
            f"✅ Strava успешно подключена!\n\nАккаунт: {athlete_name}\nТеперь ваши тренировки будут автоматически загружаться в Strava.",
        )
        target = _login_bot_redirect("Strava authorization successful! Your workouts will now be uploaded to Strava.")
        if target:
            return RedirectResponse(url=target)
        return JSONResponse({"status": "ok", "record": record})
    except HTTPException:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        log.exception("Strava callback processing failed for user %s", tg_user_id)
        target = _login_bot_redirect(f"Strava authorization failed: {exc}")
        if target:
            return RedirectResponse(url=target)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to process Strava callback") from exc


@app.post("/internal/strava/status")
def strava_status(payload: StatusRequest, _: None = Depends(require_internal_secret)):
    """Return connection status for requested users."""
    records = db.get_tokens_for_users(payload.user_ids)
    statuses = {}
    for user_id in payload.user_ids:
        record = records.get(user_id)
        statuses[user_id] = {
            "connected": bool(record),
            "athlete_id": record.get("athlete_id") if record else None,
            "athlete_name": (
                f"{record.get('athlete_firstname', '')} {record.get('athlete_lastname', '')}".strip()
                if record
                else None
            ),
            "expires_at": record.get("expires_at") if record else None,
        }
    return {"items": statuses}


@app.post("/internal/strava/upload", response_model=UploadResponse)
async def strava_upload(
    tg_user_id: int = Form(...),
    name: str = Form("WattAttack Activity"),
    description: str = Form(""),
    file: UploadFile = File(...),
    _: None = Depends(require_internal_secret),
):
    """Upload a FIT activity on behalf of a Telegram user."""
    record = db.get_token(tg_user_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No Strava tokens stored for this user")

    refreshed = _refresh_if_needed(record)
    client = _strava_client(access_token=refreshed["access_token"])
    try:
        upload_response = client.upload_activity(
            file.file,
            filename=file.filename or "activity.fit",
            name=name,
            description=description,
        )
        return UploadResponse(status="ok", upload=upload_response)
    except Exception as exc:  # pylint: disable=broad-except
        log.exception("Failed to upload activity for user %s", tg_user_id)
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"Failed to upload activity: {exc}") from exc


@app.post("/internal/strava/disconnect")
def strava_disconnect(payload: DisconnectRequest, _: None = Depends(require_internal_secret)):
    """Remove stored Strava credentials for a user."""
    deleted = db.delete_token(payload.tg_user_id)
    return {"status": "ok", "deleted": deleted}
