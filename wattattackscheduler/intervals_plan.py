"""Fetch Intervals.icu planned workouts and notify users via Telegram."""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, timedelta
from typing import Dict, List

from intervals_client import IntervalsClient
from repositories.intervals_link_repository import list_links
from repositories import intervals_plan_repository as plan_repo

LOGGER = logging.getLogger(__name__)


def _format_duration(seconds: int | float | None) -> str:
    if seconds is None:
        return ""
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return ""
    if total <= 0:
        return ""
    minutes = total // 60
    hours = minutes // 60
    mins = minutes % 60
    if hours:
        return f"{hours}ч {mins:02d}м"
    return f"{minutes}м"


def _format_event_line(event: Dict) -> str:
    start_date = (event.get("start_date_local") or "")[:10] or "unknown"
    name = event.get("name") or "Без названия"
    duration = _format_duration(event.get("moving_time"))
    duration_part = f" • {duration}" if duration else ""
    return f"{start_date} — {name}{duration_part}"


def _hash_events(events: List[Dict]) -> str:
    """Build a stable hash for the plan to avoid resending unchanged data."""
    items = []
    for ev in events:
        items.append(
            {
                "start": (ev.get("start_date_local") or "")[:10],
                "name": ev.get("name") or "",
                "duration": ev.get("moving_time") or 0,
                "updated": ev.get("updated") or "",
            }
        )
    payload = json.dumps(items, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def notify_week_plan(*, bot_token: str, timeout: float) -> None:
    """Fetch planned workouts for the next 7 days and send to linked users."""
    if not bot_token:
        LOGGER.debug("KRUTILKAVN_BOT_TOKEN is not set; skipping Intervals plan notifications")
        return
    # Import lazily to avoid circular import with notifier
    from wattattackscheduler.notifier import telegram_send_message

    links = list_links()
    if not links:
        LOGGER.debug("No Intervals.icu links found; skipping plan notifications")
        return

    oldest = date.today()
    newest = oldest + timedelta(days=7)
    oldest_str = oldest.isoformat()
    newest_str = newest.isoformat()

    for link in links:
        tg_user_id = link.get("tg_user_id")
        api_key = link.get("intervals_api_key")
        athlete_id = link.get("intervals_athlete_id") or "0"
        if not tg_user_id or not api_key:
            continue

        try:
            client = IntervalsClient(api_key=api_key, athlete_id=athlete_id, timeout=timeout)
            events: List[Dict] = client.fetch_events(
                category="WORKOUT",
                oldest=oldest_str,
                newest=newest_str,
                resolve=True,
            )
            if not events:
                LOGGER.debug("No planned workouts for user %s in next 7 days", tg_user_id)
                continue

            plan_hash = _hash_events(events)
            previous_hash = plan_repo.get_plan_hash(int(tg_user_id))
            if previous_hash == plan_hash:
                LOGGER.debug("Intervals plan unchanged for user %s, skipping notification", tg_user_id)
                continue

            lines = [
                "📅 План на неделю (Intervals.icu):",
                *[_format_event_line(ev) for ev in events],
            ]
            text = "\n".join(lines)
            telegram_send_message(
                bot_token,
                str(tg_user_id),
                text,
                timeout=timeout,
                parse_mode="HTML",
            )
            plan_repo.upsert_plan_hash(int(tg_user_id), plan_hash)
            LOGGER.info("Sent Intervals plan to user %s (%d items)", tg_user_id, len(events))
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to send Intervals plan to user %s", tg_user_id)
