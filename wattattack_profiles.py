"""Utilities for applying client data to WattAttack accounts."""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient

LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
DEFAULT_CLIENT_FTP = int(os.environ.get("WATTATTACK_DEFAULT_FTP", "150"))


def split_full_name(full_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Split a full name into first/last parts."""

    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if not parts:
        return None, None
    first = parts[0]
    last = " ".join(parts[1:]) or None
    return first, last


def apply_client_profile(
    *,
    account_id: str,
    account_label: str,
    email: str,
    password: str,
    base_url: Optional[str],
    client_record: Dict[str, Any],
    timeout: Optional[float] = None,
    default_ftp: Optional[int] = None,
) -> None:
    """Apply up-to-date client data to the given WattAttack account."""

    target_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    ftp_fallback = int(default_ftp if default_ftp is not None else DEFAULT_CLIENT_FTP)
    base_url = base_url or DEFAULT_BASE_URL
    account_label = account_label or account_id

    client = WattAttackClient(base_url)
    client.login(email, password, timeout=target_timeout)

    existing_profile: Dict[str, Any] = {}
    try:
        existing_profile = client.fetch_profile(timeout=target_timeout)
        if not isinstance(existing_profile, dict):
            existing_profile = {}
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch current profile for %s: %s", account_id, exc)
        existing_profile = {}

    athlete_section = existing_profile.get("athlete") if isinstance(existing_profile, dict) else {}
    if not isinstance(athlete_section, dict):
        athlete_section = {}

    first = client_record.get("first_name") or None
    last = client_record.get("last_name") or None
    if not first and not last:
        first, last = split_full_name(client_record.get("full_name", ""))

    user_payload: Dict[str, Any] = {}
    if first:
        user_payload["firstName"] = str(first)
    if last:
        user_payload["lastName"] = str(last)

    profile_payload: Dict[str, Any] = {}
    weight = client_record.get("weight")
    height = client_record.get("height")
    ftp = client_record.get("ftp")
    gender_value = client_record.get("gender")

    if weight is not None:
        try:
            profile_payload["weight"] = float(weight)
        except (TypeError, ValueError):
            pass
    if height is not None:
        try:
            profile_payload["height"] = float(height)
        except (TypeError, ValueError):
            pass
    ftp_value = ftp
    if ftp_value is None:
        ftp_value = ftp_fallback
    elif isinstance(ftp_value, str):
        ftp_value = ftp_value.strip()
        if not ftp_value:
            ftp_value = ftp_fallback
    try:
        profile_payload["ftp"] = int(float(ftp_value))
    except (TypeError, ValueError):
        profile_payload["ftp"] = ftp_fallback
    if gender_value:
        gender_norm = str(gender_value).strip().lower()
        if gender_norm in {"m", "male", "м", "муж", "мужской"}:
            profile_payload["gender"] = "male"
        elif gender_norm in {"f", "female", "ж", "жен", "женский"}:
            profile_payload["gender"] = "female"

    # Preserve existing required fields to avoid validation errors
    if "birthDate" not in profile_payload and athlete_section.get("birthDate"):
        profile_payload["birthDate"] = athlete_section.get("birthDate")
    if "gender" not in profile_payload and athlete_section.get("gender"):
        profile_payload["gender"] = athlete_section.get("gender")
    if not profile_payload.get("birthDate"):
        profile_payload["birthDate"] = "2000-01-01"

    if user_payload:
        LOGGER.info("Updating WattAttack user %s (%s) user data", account_id, account_label)
        client.update_user(user_payload, timeout=target_timeout)
        LOGGER.info("User update for %s completed", account_id)
    if profile_payload:
        LOGGER.info("Updating WattAttack athlete %s (%s) profile payload: %s", account_id, account_label, profile_payload)
        response = client.update_profile(profile_payload, timeout=target_timeout)
        LOGGER.debug("Profile update response for %s: %s", account_id, response)
