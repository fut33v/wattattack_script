"""Configuration helpers for the WattAttack web application."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


def _env(name: str, default: Optional[str] = None, *, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable {name} is required for the web app")
    return value or ""


@dataclass(slots=True)
class Settings:
    telegram_bot_token: str
    telegram_login_bot_username: str
    session_secret_key: str
    telegram_login_bot_token: str
    krutilkavn_bot_token: str
    base_url: Optional[str] = None
    clients_page_size: int = 25


@lru_cache()
def get_settings() -> Settings:
    """Return cached application settings."""
    default_page_size = int(os.environ.get("WEBAPP_CLIENTS_PAGE_SIZE", os.environ.get("CLIENTS_PAGE_SIZE", "100")))
    telegram_bot_token = _env("TELEGRAM_BOT_TOKEN", required=True)
    login_token = (
        os.environ.get("TELEGRAM_LOGIN_BOT_TOKEN")
        or os.environ.get("KRUTILKAVN_BOT_TOKEN")
        or telegram_bot_token
    )
    krutilkavn_token = (
        os.environ.get("KRUTILKAVN_BOT_TOKEN")
        or telegram_bot_token
    )
    if not login_token:
        raise RuntimeError("Telegram login bot token is required")
    return Settings(
        telegram_bot_token=telegram_bot_token,
        telegram_login_bot_username=_env("TELEGRAM_LOGIN_BOT_USERNAME", required=True),
        telegram_login_bot_token=login_token,
        krutilkavn_bot_token=krutilkavn_token,
        session_secret_key=_env("WEBAPP_SECRET_KEY", required=True),
        base_url=os.environ.get("WEBAPP_BASE_URL"),
        clients_page_size=default_page_size,
    )