"""Helpers to upload activities to Intervals.icu."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from intervals_client import IntervalsClient
from repositories.intervals_link_repository import get_link as get_intervals_link

LOGGER = logging.getLogger(__name__)


def upload_activity(
    *,
    tg_user_id: int,
    temp_file: Path,
    description: str,
    activity_id: Any,
    timeout: float,
    activity_name: str = "КРУТИЛКА!",
) -> bool:
    """Upload a FIT file to Intervals.icu for the user if linked."""
    if not temp_file or not temp_file.exists():
        LOGGER.debug("No temp_file provided for Intervals upload for user %s", tg_user_id)
        return False

    link = get_intervals_link(int(tg_user_id))
    if not link:
        return False

    api_key = link.get("intervals_api_key")
    athlete_id = link.get("intervals_athlete_id") or "0"
    if not api_key:
        LOGGER.debug("Intervals.icu link missing api_key for tg_user_id %s", tg_user_id)
        return False

    client = IntervalsClient(api_key=api_key, athlete_id=athlete_id, timeout=timeout)
    try:
        upload_response = client.upload_activity(
            file_path=temp_file,
            name=activity_name,
            description=description,
            external_id=str(activity_id),
        )
        LOGGER.info(
            "SUCCESS: Uploaded activity to Intervals.icu for user %s: %s",
            tg_user_id,
            upload_response,
        )
        return True
    except Exception:  # noqa: BLE001
        LOGGER.exception("Intervals.icu upload failed for user %s", tg_user_id)
        return False
