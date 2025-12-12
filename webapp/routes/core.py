"""Core auth/session/config routes."""
from __future__ import annotations

from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse

from repositories import admin_repository, bikes_repository, client_link_repository, client_repository, trainers_repository

from ..auth import TelegramAuthError, verify_telegram_payload
from ..config import get_settings
from ..dependencies import SESSION_KEY_USER, is_admin_user, require_user


api_router = APIRouter(tags=["core"])
public_router = APIRouter()


def _safe_next(next_param: Optional[str]) -> str:
    if not next_param:
        return "/app"
    if next_param.startswith("/"):
        return next_param
    return "/app"


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


@api_router.get("/config")
def api_config():
    settings = get_settings()
    return {
        "loginBotUsername": settings.telegram_login_bot_username,
        "clientsPageSize": settings.clients_page_size,
        "baseUrl": settings.base_url,
        "vkBroadcastEnabled": bool(settings.vk_community_key),
    }


@api_router.get("/session")
def api_session(user=Depends(require_user)):
    return {
        "user": jsonable_encoder(user.to_dict()),
        "isAdmin": is_admin_user(user),
    }


@api_router.get("/summary")
def api_summary(user=Depends(require_user)):
    return {
        "clients": client_repository.count_clients(),
        "bikes": bikes_repository.bikes_count(),
        "trainers": trainers_repository.trainers_count(),
        "admins": len(admin_repository.list_admins()),
        "links": len(client_link_repository.list_links()),
    }


@api_router.get("/health")
def api_health():
    return {"status": "ok"}


@api_router.get("/logout")
def api_logout(request: Request):
    request.session.pop(SESSION_KEY_USER, None)
    return {"status": "ok"}


@public_router.get("/auth/telegram")
async def telegram_auth(request: Request, next: Optional[str] = None):
    settings = get_settings()
    allowed_keys = {
        "id",
        "first_name",
        "last_name",
        "username",
        "photo_url",
        "auth_date",
        "hash",
    }
    payload = {key: value for key, value in request.query_params.items() if key in allowed_keys}

    try:
        user = verify_telegram_payload(
            payload,
            bot_token=settings.telegram_login_bot_token,
        )
    except TelegramAuthError as exc:
        target = _safe_next(next)
        separator = "&" if "?" in target else "?"
        target = f"{target}{separator}error={quote(str(exc))}"
        return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)

    request.session[SESSION_KEY_USER] = user.to_dict()
    target = _safe_next(next)
    return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)


@public_router.get("/logout")
def logout(request: Request):
    request.session.pop(SESSION_KEY_USER, None)
    return RedirectResponse(url="/app", status_code=status.HTTP_303_SEE_OTHER)
