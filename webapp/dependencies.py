"""Shared FastAPI dependencies for the web app."""
from __future__ import annotations

from typing import Optional

from fastapi import HTTPException, Request, status

from repositories import admin_repository

from .auth import TelegramUser


SESSION_KEY_USER = "telegram_user"


def _deserialize_user(data: dict) -> TelegramUser:
    return TelegramUser(
        id=int(data["id"]),
        auth_date=int(data.get("auth_date", 0)),
        first_name=data.get("first_name"),
        last_name=data.get("last_name"),
        username=data.get("username"),
        photo_url=data.get("photo_url"),
    )


def get_current_user(request: Request) -> Optional[TelegramUser]:
    stored = request.session.get(SESSION_KEY_USER)
    if not stored:
        return None
    try:
        return _deserialize_user(stored)
    except (KeyError, TypeError, ValueError):
        request.session.pop(SESSION_KEY_USER, None)
        return None


def is_admin_user(user: TelegramUser) -> bool:
    return admin_repository.is_admin(user.id, user.username)


def require_user(request: Request) -> TelegramUser:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return user


def require_admin(request: Request) -> TelegramUser:
    user = require_user(request)
    if not is_admin_user(user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admins only")
    return user
