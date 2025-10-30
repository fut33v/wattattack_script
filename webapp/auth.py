"""Telegram authentication helpers for the WattAttack web app."""
from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass, asdict
from typing import Mapping, MutableMapping, Optional


@dataclass(slots=True)
class TelegramUser:
    id: int
    auth_date: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    username: Optional[str] = None
    photo_url: Optional[str] = None

    @property
    def display_name(self) -> str:
        parts = [self.first_name or "", self.last_name or ""]
        name = " ".join(part for part in parts if part).strip()
        return name or (self.username or str(self.id))

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["display_name"] = self.display_name
        return data


class TelegramAuthError(Exception):
    """Raised when Telegram login data cannot be verified."""


def _data_check_string(data: Mapping[str, str]) -> str:
    pairs = [f"{key}={value}" for key, value in sorted(data.items())]
    return "\n".join(pairs)


def verify_telegram_payload(
    data: Mapping[str, str],
    *,
    bot_token: str,
    max_age_seconds: int = 86400,
) -> TelegramUser:
    """Validate Telegram login payload and return the authenticated user."""

    if "hash" not in data:
        raise TelegramAuthError("Missing hash in Telegram payload")

    payload = dict(data)
    hash_value = payload.pop("hash")

    secret_key = hashlib.sha256(bot_token.encode()).digest()
    data_check_string = _data_check_string(payload)
    expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected_hash, hash_value):
        raise TelegramAuthError("Invalid signature in Telegram payload")

    try:
        auth_date = int(payload["auth_date"])
    except (KeyError, ValueError) as exc:
        raise TelegramAuthError("Missing or invalid auth_date") from exc

    if time.time() - auth_date > max_age_seconds:
        raise TelegramAuthError("Telegram payload expired")

    try:
        user_id = int(payload["id"])
    except (KeyError, ValueError) as exc:
        raise TelegramAuthError("Missing or invalid Telegram user id") from exc

    telegram_user = TelegramUser(
        id=user_id,
        auth_date=auth_date,
        first_name=payload.get("first_name"),
        last_name=payload.get("last_name"),
        username=payload.get("username"),
        photo_url=payload.get("photo_url"),
    )
    return telegram_user
