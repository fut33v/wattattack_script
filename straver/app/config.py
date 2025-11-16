"""Configuration helpers for the Straver service."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


def _env(name: str, default: Optional[str] = None, *, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable {name} is required for Straver")
    return value or ""


@dataclass(slots=True)
class Settings:
    database_url: str
    strava_client_id: str
    strava_client_secret: str
    strava_redirect_uri: str
    internal_secret: str
    telegram_bot_token: Optional[str]
    telegram_login_bot_username: Optional[str]


@lru_cache()
def get_settings() -> Settings:
    """Return cached settings for the Straver service."""
    return Settings(
        database_url=_env(
            "STRAVER_DATABASE_URL",
            default="postgresql://straver:straver@straver-db:5432/straver",
            required=True,
        ),
        strava_client_id=_env("STRAVA_CLIENT_ID", required=True),
        strava_client_secret=_env("STRAVA_CLIENT_SECRET", required=True),
        strava_redirect_uri=_env(
            "STRAVA_REDIRECT_URI",
            default="http://localhost:8098/strava/callback",
            required=True,
        ),
        internal_secret=_env("STRAVER_INTERNAL_SECRET", required=True),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN"),
        telegram_login_bot_username=os.environ.get("TELEGRAM_LOGIN_BOT_USERNAME"),
    )
