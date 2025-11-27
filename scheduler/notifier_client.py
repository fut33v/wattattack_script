#!/usr/bin/env python3
"""Check WattAttack accounts for new activities and ping Telegram admins."""
from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import shutil
import time as time_module
from datetime import datetime, timedelta, date, time as dt_time, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from zoneinfo import ZoneInfo
from straver_client import StraverClient
from scheduler import intervals_sync

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient
from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    get_admin_ids,
)
from repositories.schedule_repository import (
    list_upcoming_reservations,
    ensure_workout_notifications_table,
    was_notification_sent,
    record_notification_sent,
    ensure_activity_ids_table,
    was_activity_id_seen,
    record_seen_activity_id,
    get_seen_activity_ids_for_account,
    find_reservation_for_activity,
    find_reservation_by_client_name,
    ensure_fit_files_dir,
)
from repositories.client_link_repository import get_link_by_client
from repositories.intervals_link_repository import get_link as get_intervals_link
from repositories.client_repository import get_client, search_clients

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "KRUTILKAFIT_BOT_TOKEN"
BOT_TOKEN_FALLBACK_ENV = "TELEGRAM_BOT_TOKEN"
KRUTILKAVN_BOT_TOKEN_ENV = "KRUTILKAVN_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
STATE_ENV = "WATTATTACK_STATE_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_STATE_PATH = Path("notifier_state.json")
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
MAX_TRACKED_IDS = int(os.environ.get("WATTATTACK_TRACKED_LIMIT", "200"))
DEFAULT_ADMIN_SEED = os.environ.get("TELEGRAM_ADMIN_IDS", "")
DEFAULT_REMINDER_HOURS = int(os.environ.get("WORKOUT_REMINDER_HOURS", "4"))
STRAVER_BASE_URL = os.environ.get("STRAVER_BASE_URL")
STRAVER_INTERNAL_SECRET = os.environ.get("STRAVER_INTERNAL_SECRET")
STRAVER_HTTP_TIMEOUT = float(os.environ.get("STRAVER_HTTP_TIMEOUT", os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30")))
LOCAL_TIMEZONE = ZoneInfo(os.environ.get("WATTATTACK_LOCAL_TZ", "Europe/Moscow"))
FIT_WAIT_SECONDS = int(os.environ.get("WATTATTACK_FIT_WAIT_SECONDS", "600"))
MATCH_GRACE_MINUTES = int(os.environ.get("WATTATTACK_MATCH_GRACE_MINUTES", "30"))


class TZFormatter(logging.Formatter):
    """Formatter that renders timestamps in the configured timezone."""

    def __init__(self, fmt: str, tz: ZoneInfo, datefmt: Optional[str] = None) -> None:
        super().__init__(fmt, datefmt)
        self._tz = tz

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone(self._tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


def configure_logging(level: int = logging.DEBUG) -> None:
    """Configure logging only when running notifier_client standalone."""

    root = logging.getLogger()
    if root.handlers:
        return

    handler = logging.StreamHandler()
    handler.setFormatter(
        TZFormatter("%(asctime)s %(levelname)s %(message)s", tz=LOCAL_TIMEZONE)
    )
    root.addHandler(handler)
    root.setLevel(level)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Notify Telegram admins about new WattAttack activities.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--accounts",
        type=Path,
        default=Path(os.environ.get(ACCOUNTS_ENV, DEFAULT_ACCOUNTS_PATH)),
        help="Path to JSON file with WattAttack accounts (same format as accounts.sample.json)",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=Path(os.environ.get(STATE_ENV, DEFAULT_STATE_PATH)),
        help="Path to JSON file used to remember previously seen activities (deprecated - now using database)",
    )
    parser.add_argument(
        "--admins",
        default=DEFAULT_ADMIN_SEED,
        help="Comma-separated admin IDs/юзернеймы для добавления в базу (опционально)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get(BOT_TOKEN_ENV)
        or os.environ.get(BOT_TOKEN_FALLBACK_ENV, ""),
        help="Telegram bot token. Falls back to TELEGRAM_BOT_TOKEN env variable",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="HTTP timeout for WattAttack and Telegram requests",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch activities and update state without sending Telegram messages",
    )
    parser.add_argument(
        "--reminder-hours",
        type=int,
        default=DEFAULT_REMINDER_HOURS,
        help="Hours before workout to send reminder (default: 4)",
    )
    return parser.parse_args(argv)


def load_accounts(config_path: Path) -> Dict[str, Dict[str, Any]]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Accounts config file not found: {config_path}. "
            "Supply --accounts or set WATTATTACK_ACCOUNTS_FILE."
        )

    data = json.loads(config_path.read_text(encoding="utf-8"))
    accounts: Dict[str, Dict[str, Any]] = {}
    for entry in data:
        identifier = entry["id"]
        accounts[identifier] = {
            "id": identifier,
            "name": entry.get("name", identifier),
            "email": entry["email"],
            "password": entry["password"],
            "base_url": entry.get("base_url", DEFAULT_BASE_URL),
        }
    if not accounts:
        raise ValueError("Accounts list is empty")
    return accounts


def load_state(path: Path) -> Dict[str, Any]:
    if path.is_dir():
        raise IsADirectoryError(f"State path points to a directory: {path}")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file is corrupted, starting fresh: %s", path)
        return {}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def telegram_send_message(
    token: str,
    chat_id: str,
    text: str,
    *,
    timeout: float,
    parse_mode: str = "HTML",
) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    response = requests.post(url, json=payload, timeout=timeout)
    if response.status_code != 200:
        LOGGER.error(
            "Failed to send Telegram message to %s (%s): %s",
            chat_id,
            response.status_code,
            response.text,
        )
        response.raise_for_status()


def telegram_send_document(
    token: str,
    chat_id: str,
    file_path: Path,
    filename: str,
    *,
    caption: str = "",
    timeout: float,
) -> None:
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    with file_path.open("rb") as file_handle:
        files = {"document": (filename, file_handle, "application/octet-stream")}
        data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
        response = requests.post(url, data=data, files=files, timeout=timeout)
    if response.status_code != 200:
        LOGGER.error(
            "Failed to send document to %s (%s): %s",
            chat_id,
            response.status_code,
            response.text,
        )
        response.raise_for_status()


def format_activity(activity: Dict[str, Any]) -> str:
    start_time = activity.get("startTime")
    if start_time:
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            date_str = start_time
    else:
        date_str = "?"

    distance = activity.get("distance", 0) or 0
    try:
        distance_km = float(distance) / 1000
    except (TypeError, ValueError):
        distance_km = 0.0

    elapsed = activity.get("elapsedTime")
    duration = format_duration(elapsed)
    name = activity.get("mapNameRu") or activity.get("name") or "Без названия"
    fit_hint = " (FIT доступен)" if activity.get("fitFileId") else ""

    return (
        f"\u2022 <b>{name}</b>{fit_hint}\n"
        f"  Дистанция: {distance_km:.1f} км\n"
        f"  Время: {duration}\n"
        f"  Старт: {date_str}"
    )


def format_duration(seconds: Any) -> str:
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return "?"
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}ч {minutes:02d}м"
    return f"{minutes}м {secs:02d}с"


def format_start_time(activity: Dict[str, Any]) -> str:
    start_time = activity.get("startTime")
    if not start_time:
        return "?"
    try:
        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        dt += timedelta(hours=3)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return str(start_time)


def format_activity_meta(
    activity: Dict[str, Any],
    account_name: Optional[str],
    profile: Optional[Dict[str, Any]],
    scheduled_name: Optional[str] = None,
) -> str:
    name = activity.get("mapNameRu") or activity.get("name") or "Без названия"
    date_str = format_start_time(activity)
    distance = activity.get("distance", 0) or 0
    try:
        distance_km = float(distance) / 1000
    except (TypeError, ValueError):
        distance_km = 0.0
    duration = format_duration(activity.get("elapsedTime"))
    elevation = activity.get("totalElevationGain")
    power_avg = activity.get("averageWatts")
    cadence_avg = activity.get("averageCadence")
    heartrate_avg = activity.get("averageHeartrate")

    lines: List[str] = []
    if account_name:
        lines.append(f"<b>{account_name}</b>")
    lines.append(f"<b>{name}</b>")
    athlete_name = None
    if scheduled_name:
        athlete_name = scheduled_name
        lines.append(f"Атлет: {scheduled_name} (по расписанию)")
    if profile:
        profile_name = extract_athlete_name(profile)
        if profile_name and profile_name != athlete_name:
            lines.append(f"Атлет (профиль): {profile_name}")
        gender = extract_athlete_field(profile, "gender")
        if gender:
            lines.append(f"Пол: {'М' if gender.upper().startswith('M') else 'Ж'}")
        weight = extract_athlete_field(profile, "weight")
        if weight:
            lines.append(f"Вес: {weight} кг")
        ftp_value = extract_athlete_field(profile, "ftp")
        if ftp_value:
            lines.append(f"FTP: {ftp_value} Вт")

    lines.append(f"Дата: {date_str}")
    lines.append(f"Дистанция: {distance_km:.1f} км")
    lines.append(f"Время: {duration}")
    if elevation is not None:
        lines.append(f"Набор высоты: {elevation} м")
    if power_avg:
        lines.append(f"Средняя мощность: {power_avg} Вт")
    if cadence_avg:
        lines.append(f"Средний каденс: {cadence_avg} об/мин")
    if heartrate_avg:
        lines.append(f"Средний пульс: {heartrate_avg} уд/мин")

    return "\n".join(lines)


def format_strava_activity_description(
    activity: Dict[str, Any],
    account_name: Optional[str],
    profile: Optional[Dict[str, Any]],
    scheduled_name: Optional[str] = None,
) -> str:
    """Format activity description for Strava (without HTML tags)."""
    name = activity.get("mapNameRu") or activity.get("name") or "Без названия"
    date_str = format_start_time(activity)
    distance = activity.get("distance", 0) or 0
    try:
        distance_km = float(distance) / 1000
    except (TypeError, ValueError):
        distance_km = 0.0
    duration = format_duration(activity.get("elapsedTime"))
    elevation = activity.get("totalElevationGain")
    power_avg = activity.get("averageWatts")
    cadence_avg = activity.get("averageCadence")
    heartrate_avg = activity.get("averageHeartrate")

    lines: List[str] = []
    lines.append("Прямиком из Крутилки!")
    if account_name:
        lines.append(f"{account_name}")
    lines.append(f"{name}")
    athlete_name = None
    if scheduled_name:
        athlete_name = scheduled_name
        lines.append(f"Атлет: {scheduled_name} (по расписанию)")
    if profile:
        profile_name = extract_athlete_name(profile)
        if profile_name and profile_name != athlete_name:
            lines.append(f"Атлет (профиль): {profile_name}")
        gender = extract_athlete_field(profile, "gender")
        if gender:
            lines.append(f"Пол: {'М' if gender.upper().startswith('M') else 'Ж'}")
        weight = extract_athlete_field(profile, "weight")
        if weight:
            lines.append(f"Вес: {weight} кг")
        ftp_value = extract_athlete_field(profile, "ftp")
        if ftp_value:
            lines.append(f"FTP: {ftp_value} Вт")

    lines.append(f"Дата: {date_str}")
    lines.append(f"Дистанция: {distance_km:.1f} км")
    lines.append(f"Время: {duration}")
    if elevation is not None:
        lines.append(f"Набор высоты: {elevation} м")
    if power_avg:
        lines.append(f"Средняя мощность: {power_avg} Вт")
    if cadence_avg:
        lines.append(f"Средний каденс: {cadence_avg} об/мин")
    if heartrate_avg:
        lines.append(f"Средний пульс: {heartrate_avg} уд/мин")

    return "\n".join(lines)


def extract_athlete_name(profile: Dict[str, Any]) -> str:
    candidate = profile
    if isinstance(profile.get("user"), dict):
        candidate = profile["user"]
    elif isinstance(profile.get("athlete"), dict):
        candidate = profile["athlete"]

    first = candidate.get("firstName") if isinstance(candidate, dict) else None
    last = candidate.get("lastName") if isinstance(candidate, dict) else None

    parts = [str(part) for part in (first, last) if part]
    if parts:
        return " ".join(parts)

    if isinstance(candidate, dict):
        for key in ("nickname", "name", "displayName"):
            value = candidate.get(key)
            if value:
                return str(value)
    return ""


def extract_athlete_field(profile: Dict[str, Any], field: str) -> str:
    candidate = profile
    if isinstance(profile.get("user"), dict):
        candidate = profile["user"]
    elif isinstance(profile.get("athlete"), dict):
        candidate = profile["athlete"]

    if isinstance(candidate, dict):
        value = candidate.get(field)
        if value:
            return str(value)
    if isinstance(profile, dict):
        value = profile.get(field)
        if value:
            return str(value)
    return ""


def _activity_age_seconds(activity: Dict[str, Any]) -> Optional[float]:
    """Return how many seconds ago the activity timestamp happened."""

    keys = (
        "startTime",
        "start_time",
        "startDate",
        "start_date",
        "startTimestamp",
        "createdAt",
        "created_at",
        "updatedAt",
        "updated_at",
    )
    now = time_module.time()
    for key in keys:
        value = activity.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)):
            return now - float(value)
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return now - dt.timestamp()
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return now - dt.timestamp()
    return None


def should_wait_for_fit_file(activity: Dict[str, Any]) -> tuple[bool, Optional[float]]:
    """
    Decide whether to delay processing an activity that currently lacks a FIT file.

    Returns (should_wait, age_seconds).
    """

    age_seconds = _activity_age_seconds(activity)
    if activity.get("fitFileId"):
        return False, age_seconds
    if age_seconds is None:
        return False, None
    return age_seconds < FIT_WAIT_SECONDS, age_seconds


def parse_activity_start_dt(activity: Dict[str, Any]) -> Optional[datetime]:
    """Return activity start datetime (UTC) if present."""

    candidates = (
        activity.get("startTime"),
        activity.get("start_time"),
        activity.get("startDate"),
        activity.get("start_date"),
    )
    for raw in candidates:
        if not raw:
            continue
        if isinstance(raw, datetime):
            dt_value = raw
        elif isinstance(raw, (int, float)):
            dt_value = datetime.fromtimestamp(float(raw), tz=timezone.utc)
        elif isinstance(raw, str):
            try:
                dt_value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                continue
        else:
            continue

        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc)
    return None


def resolve_scheduled_client(
    account: Optional[Dict[str, Any]],
    activity: Dict[str, Any],
    profile: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Resolve which client was scheduled on the account's stand at the activity time.

    Returns dict with keys client_id, client_name if found, otherwise None.
    """

    if not account:
        return None
    stand_ids = account.get("stand_ids") or []
    if not stand_ids:
        return None

    start_dt = parse_activity_start_dt(activity)
    if not start_dt:
        return None

    start_local = start_dt.astimezone(LOCAL_TIMEZONE)
    athlete_name = extract_athlete_name(profile or {})

    try:
        reservation = find_reservation_for_activity(
            stand_ids,
            start_local,
            grace_minutes=MATCH_GRACE_MINUTES,
        )
    except Exception:
        LOGGER.exception("Failed to resolve reservation for account %s", account.get("id") or account.get("name"))
        reservation = None

    fallback_reservation = None
    if athlete_name:
        try:
            fallback_reservation = find_reservation_by_client_name(
                start_local,
                athlete_name,
                grace_minutes=MATCH_GRACE_MINUTES,
            )
        except Exception:
            LOGGER.exception("Failed to resolve reservation by athlete name for account %s", account.get("id") or account.get("name"))

    chosen = None
    if reservation and athlete_name:
        scheduled_name = (
            reservation.get("client_full_name")
            or " ".join(
                part
                for part in [
                    reservation.get("client_first_name"),
                    reservation.get("client_last_name"),
                ]
                if part
            ).strip()
        )
        if not scheduled_name and reservation.get("client_name"):
            scheduled_name = reservation.get("client_name")
        if scheduled_name and scheduled_name.lower() == athlete_name.lower():
            chosen = reservation
    if not chosen:
        chosen = fallback_reservation or reservation

    if not chosen:
        return None

    client_id = chosen.get("client_id")
    if not client_id:
        return None

    client_name = (
        chosen.get("client_full_name")
        or " ".join(
            part
            for part in [
                chosen.get("client_first_name"),
                chosen.get("client_last_name"),
            ]
            if part
        ).strip()
    )
    if not client_name and chosen.get("client_name"):
        client_name = chosen.get("client_name")

    return {
        "client_id": client_id,
        "client_name": client_name or f"ID {client_id}",
        "reservation": chosen,
    }


def send_workout_reminders(*, timeout: float, reminder_hours: int = DEFAULT_REMINDER_HOURS) -> None:
    """Send workout reminders to clients via clientbot."""
    # Get the clientbot token
    krutilkavn_token = os.environ.get("KRUTILKAVN_BOT_TOKEN")
    if not krutilkavn_token:
        LOGGER.info("KRUTILKAVN_BOT_TOKEN not set, skipping workout reminders")
        return

    # Ensure the notifications table exists
    try:
        ensure_workout_notifications_table()
    except Exception:
        LOGGER.exception("Failed to ensure workout notifications table")
        return

    # Calculate time window for reminders (N hours before the workout)
    now = datetime.now(tz=LOCAL_TIMEZONE)
    since = now + timedelta(hours=reminder_hours-1)  # Slightly wider window to ensure we catch everything
    until = now + timedelta(hours=reminder_hours+1)

    try:
        # Get all upcoming reservations in the time window
        reservations = list_upcoming_reservations(since, until)
        LOGGER.info("Found %d upcoming reservations for workout reminders", len(reservations))
    except Exception as exc:
        LOGGER.exception("Failed to fetch upcoming reservations: %s", exc)
        return

    # Filter out reservations for which we've already sent notifications
    notification_type = f"reminder_{reminder_hours}h"
    unsent_reservations = []
    for reservation in reservations:
        reservation_id = reservation.get("id")
        if reservation_id and not was_notification_sent(reservation_id, notification_type):
            unsent_reservations.append(reservation)
        else:
            LOGGER.debug("Skipping reservation %s - notification already sent", reservation_id)

    if not unsent_reservations:
        LOGGER.info("No new workout reminders to send")
        return

    # Group reservations by client
    client_reservations = {}
    for reservation in unsent_reservations:
        client_id = reservation.get("client_id")
        if client_id:
            if client_id not in client_reservations:
                client_reservations[client_id] = []
            client_reservations[client_id].append(reservation)

    # Send reminders to each client
    sent_count = 0
    for client_id, reservations in client_reservations.items():
        try:
            # Get client information
            client = get_client(client_id)
            if not client:
                LOGGER.warning("Client %s not found", client_id)
                continue

            # Get Telegram user ID for the client
            link = get_link_by_client(client_id)
            if not link:
                LOGGER.debug("Client %s is not linked to Telegram", client_id)
                continue

            tg_user_id = link.get("tg_user_id")
            if not tg_user_id:
                LOGGER.debug("Client %s has no Telegram user ID", client_id)
                continue

            # Format reminder message
            message = format_workout_reminder(client, reservations, reminder_hours)
            
            # Send reminder via clientbot
            try:
                telegram_send_message(
                    krutilkavn_token,
                    str(tg_user_id),
                    message,
                    timeout=timeout,
                    parse_mode="HTML",
                )
                
                # Record that we sent the notification for each reservation
                for reservation in reservations:
                    reservation_id = reservation.get("id")
                    if reservation_id:
                        record_notification_sent(reservation_id, notification_type)
                
                LOGGER.info("Sent workout reminder to client %s (Telegram user %s)", client_id, tg_user_id)
                sent_count += len(reservations)
            except requests.HTTPError as exc:
                LOGGER.warning("Failed to send workout reminder to client %s: %s", client_id, exc)
        except Exception as exc:
            LOGGER.exception("Error processing workout reminder for client %s: %s", client_id, exc)

    LOGGER.info("Sent %d workout reminders", sent_count)


def format_workout_reminder(client: Dict[str, Any], reservations: List[Dict[str, Any]], reminder_hours: int) -> str:
    """Format a workout reminder message for a client."""
    # Get client name
    first_name = client.get("first_name", "")
    last_name = client.get("last_name", "")
    full_name = client.get("full_name", "")
    
    if first_name and last_name:
        client_name = f"{first_name} {last_name}"
    elif full_name:
        client_name = full_name
    else:
        client_name = first_name or last_name or "Клиент"

    # Format reservation details
    if len(reservations) == 1:
        reservation = reservations[0]
        slot_date = reservation.get("slot_date")
        start_time = reservation.get("start_time")
        end_time = reservation.get("end_time")
        
        # Format date and time
        if isinstance(slot_date, date):
            date_str = slot_date.strftime("%d.%m.%Y")
        else:
            date_str = str(slot_date)
            
        if isinstance(start_time, dt_time):
            start_str = start_time.strftime("%H:%M")
        else:
            start_str = str(start_time)

        if isinstance(end_time, dt_time):
            end_str = end_time.strftime("%H:%M")
        else:
            end_str = str(end_time)

        # Session type
        session_kind = reservation.get("session_kind", "self_service")
        instructor_name = reservation.get("instructor_name", "")
        
        if session_kind == "instructor":
            if instructor_name:
                session_info = f"с инструктором {instructor_name}"
            else:
                session_info = "с инструктором"
        else:
            session_info = "самокрутка"

        # Stand information
        stand_code = reservation.get("stand_code", "")
        stand_title = reservation.get("stand_title", "")
        
        if stand_code and stand_title and stand_code != stand_title:
            stand_info = f"{stand_code} ({stand_title})"
        else:
            stand_info = stand_code or stand_title or "станок"

        message = (
            f"👋 <b>{client_name}</b>, напоминаем о предстоящей тренировке!\n\n"
            f"📅 Дата: {date_str}\n"
            f"🕘 Время: {start_str}-{end_str}\n"
            f"🏋️ Станок: {stand_info}\n"
            f"🧑‍🏫 Тип: {session_info}\n\n"
            "До встречи в «Крутилке»! 🚴‍♀️"
        )
    else:
        # Multiple reservations
        message = f"👋 <b>{client_name}</b>, у вас запланированы следующие тренировки:\n\n"
        
        for reservation in reservations:
            slot_date = reservation.get("slot_date")
            start_time = reservation.get("start_time")
            
            # Format date and time
            if isinstance(slot_date, date):
                date_str = slot_date.strftime("%d.%m")
            else:
                date_str = str(slot_date)
                
            if isinstance(start_time, dt_time):
                time_str = start_time.strftime("%H:%M")
            else:
                time_str = str(start_time)

            # Session type
            session_kind = reservation.get("session_kind", "self_service")
            instructor_name = reservation.get("instructor_name", "")
            
            if session_kind == "instructor":
                if instructor_name:
                    session_info = f"с инструктором {instructor_name}"
                else:
                    session_info = "с инструктором"
            else:
                session_info = "самокрутка"

            message += f"• {date_str} в {time_str} ({session_info})\n"
        
        message += "\nДо встречи в «Крутилке»! 🚴‍♀️"

    return message


def send_activity_fit(
    *,
    account_id: str,
    client: WattAttackClient,
    activity: Dict[str, Any],
    account_name: str,
    account: Optional[Dict[str, Any]],
    profile: Dict[str, Any],
    token: str,
    admin_ids: Sequence[int],
    timeout: float,
) -> Tuple[bool, Optional[int], Optional[str], Optional[datetime], Optional[str], bool, bool, bool, Optional[str]]:
    fit_id = activity.get("fitFileId")
    scheduled_match = resolve_scheduled_client(account, activity, profile)
    matched_client_id = scheduled_match.get("client_id") if scheduled_match else None
    matched_client_name = scheduled_match.get("client_name") if scheduled_match else None
    start_dt = parse_activity_start_dt(activity)
    profile_name = extract_athlete_name(profile) if profile else None
    caption = format_activity_meta(activity, account_name, profile, matched_client_name)
    sent_clientbot = False
    sent_strava = False
    sent_intervals = False
    fit_path: Optional[str] = None
    
    # Get clientbot token for sending to clients
    krutilkavn_token = os.environ.get(KRUTILKAVN_BOT_TOKEN_ENV)
    
    final_client_id = matched_client_id

    if not fit_id:
        should_wait, age_seconds = should_wait_for_fit_file(activity)
        if should_wait:
            LOGGER.info(
                "Activity %s has no FIT yet (age=%.0fs < %ss), will retry later",
                activity.get("id"),
                age_seconds or 0,
                FIT_WAIT_SECONDS,
            )
            return False, final_client_id, matched_client_name, start_dt, profile_name, sent_clientbot, sent_strava, sent_intervals, fit_path

        LOGGER.info("Activity %s has no FIT file", activity.get("id"))
        # Send to admins
        for chat_id in admin_ids:
            try:
                telegram_send_message(
                    token,
                    str(chat_id),
                    caption,
                    timeout=timeout,
                )
            except requests.HTTPError:
                pass
        
        # Send to matching clients if clientbot token is available
        if krutilkavn_token:
            (
                sent_clientbot,
                sent_strava,
                sent_intervals,
                resolved_client_id,
                resolved_client_name,
            ) = send_to_matching_clients(
                activity,
                profile,
                caption,
                krutilkavn_token,
                timeout,
                None,
                account_name,
                matched_client_id,
                matched_client_name,
            )
            if not final_client_id and resolved_client_id:
                final_client_id = resolved_client_id
        return True, final_client_id, matched_client_name, start_dt, profile_name, sent_clientbot, sent_strava, sent_intervals, fit_path
    
    # For activities with FIT files, we need to download the file first
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            temp_file = Path(tmp.name)
        client.download_fit_file(str(fit_id), temp_file, timeout=timeout)
        filename = f"activity_{activity.get('id')}.fit"
        try:
            dest_dir = ensure_fit_files_dir() / account_id
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_file = dest_dir / f"{activity.get('id')}.fit"
            if not dest_file.exists():
                shutil.copy2(temp_file, dest_file)
            if dest_file.exists():
                fit_path = f"/fitfiles/{account_id}/{activity.get('id')}.fit"
        except Exception:
            LOGGER.exception("Failed to archive FIT file for %s %s", account_id, activity.get("id"))
        # Send to admins
        for chat_id in admin_ids:
            try:
                telegram_send_document(
                    token,
                    str(chat_id),
                    temp_file,
                    filename,
                    caption=caption,
                    timeout=timeout,
                )
            except requests.HTTPError:
                pass
                
        # Send to matching clients if clientbot token is available
        # Only send to matching clients AFTER we have downloaded the FIT file
        if krutilkavn_token:
            (
                sent_clientbot,
                sent_strava,
                sent_intervals,
                resolved_client_id,
                resolved_client_name,
            ) = send_to_matching_clients(
                activity,
                profile,
                caption,
                krutilkavn_token,
                timeout,
                temp_file,
                account_name,
                matched_client_id,
                matched_client_name,
            )
            if not final_client_id and resolved_client_id:
                final_client_id = resolved_client_id
    except Exception:
        LOGGER.exception("Failed to download/send FIT %s", fit_id)
        # Send error message to admins
        for chat_id in admin_ids:
            try:
                telegram_send_message(
                    token,
                    str(chat_id),
                    f"Не удалось отправить FIT для активности {activity.get('id')}",
                    timeout=timeout,
                )
            except requests.HTTPError:
                pass
        # Send error message to matching clients if clientbot token is available
        # Send without FIT file when there was an error downloading it
        if krutilkavn_token:
            error_caption = f"Не удалось отправить FIT для активности {activity.get('id')}"
            send_to_matching_clients(
                activity,
                profile,
                error_caption,
                krutilkavn_token,
                timeout,
                None,
                account_name,
                matched_client_id,
                matched_client_name,
            )
    finally:
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
            except OSError:
                LOGGER.debug("Failed to remove temp file %s", temp_file)
    return (
        True,
        final_client_id,
        matched_client_name,
        start_dt,
        profile_name,
        sent_clientbot,
        sent_strava,
        sent_intervals,
    )


def send_to_matching_clients(
    activity: Dict[str, Any],
    profile: Dict[str, Any],
    caption: str,
    krutilkavn_token: str,
    timeout: float,
    temp_file: Optional[Path] = None,
    account_name: Optional[str] = None,
    scheduled_client_id: Optional[int] = None,
    scheduled_client_name: Optional[str] = None,
) -> Tuple[bool, bool, bool, Optional[int], Optional[str]]:
    """Send activity information or file to clients whose names match the athlete profile."""
    target_clients: List[Dict[str, Any]] = []
    athlete_name = extract_athlete_name(profile)
    used_schedule = False
    resolved_client_id: Optional[int] = None
    resolved_client_name: Optional[str] = None

    if scheduled_client_id:
        try:
            scheduled_client = get_client(scheduled_client_id)
        except Exception:
            LOGGER.exception("Failed to load scheduled client %s", scheduled_client_id)
            scheduled_client = None

        if scheduled_client:
            target_clients = [scheduled_client]
            used_schedule = True
            if scheduled_client_name:
                scheduled_client["full_name"] = scheduled_client_name
            elif not scheduled_client.get("full_name"):
                full_name = " ".join(
                    part
                    for part in [
                        scheduled_client.get("first_name"),
                        scheduled_client.get("last_name"),
                    ]
                    if part
                ).strip()
                if full_name:
                    scheduled_client["full_name"] = full_name
        else:
            LOGGER.info("Scheduled client %s not found; falling back to profile name matching", scheduled_client_id)

    if not target_clients:
        if not athlete_name:
            LOGGER.debug("No athlete name and no scheduled client, skipping client matching")
            return False, False, False, None, None

        LOGGER.info("Searching for clients matching athlete name: %s", athlete_name)
        matching_clients = search_clients(athlete_name, limit=100)
        if not matching_clients:
            LOGGER.info("No clients found matching athlete name: %s", athlete_name)
            return False, False, False, None, None

        athlete_name_lower = athlete_name.lower()
        for client in matching_clients:
            first_name = client.get("first_name", "") or ""
            last_name = client.get("last_name", "") or ""
            full_name = client.get("full_name", "") or ""

            client_names = [
                f"{first_name} {last_name}".strip(),
                full_name.strip(),
                first_name.strip(),
                last_name.strip(),
            ]

            for client_name in client_names:
                if client_name and client_name.lower() == athlete_name_lower:
                    target_clients.append(client)
                    break

        if not target_clients:
            LOGGER.info("No exact name matches found for athlete: %s", athlete_name)
            return False, False, False, None, None
        LOGGER.info("Found %d exact client matches for athlete: %s", len(target_clients), athlete_name)
    else:
        LOGGER.info(
            "Using scheduled client match for activity: client_id=%s name=%s",
            target_clients[0].get("id"),
            target_clients[0].get("full_name") or target_clients[0].get("first_name"),
        )

    # Prepare Straver client and prefetch statuses to avoid per-user calls
    straver_client = StraverClient(
        base_url=STRAVER_BASE_URL,
        secret=STRAVER_INTERNAL_SECRET,
        timeout=STRAVER_HTTP_TIMEOUT,
    )
    if target_clients:
        first = target_clients[0]
        resolved_client_id = first.get("id")
        resolved_client_name = (
            first.get("full_name")
            or " ".join(
                part for part in [first.get("first_name"), first.get("last_name")] if part
            ).strip()
        ) or athlete_name

    links_cache: Dict[int, Dict[str, Any]] = {}
    user_ids_for_status: List[int] = []
    for client in target_clients:
        client_id = client.get("id")
        if not client_id:
            continue
        link = get_link_by_client(client_id)
        links_cache[client_id] = link
        if link and link.get("tg_user_id"):
            user_ids_for_status.append(link["tg_user_id"])

    straver_statuses: Dict[int, Dict[str, Any]] = {}
    if straver_client.is_configured() and user_ids_for_status:
        try:
            straver_statuses = straver_client.connection_status(user_ids_for_status)
        except Exception:
            LOGGER.exception("Failed to fetch Straver statuses for matching clients")
    
    # For each matching client, check if they're linked to a Telegram user and send the message/file
    sent_count = 0
    strava_uploaded_count = 0
    intervals_uploaded_count = 0
    LOGGER.debug("Processing %d client matches", len(target_clients))
    for client in target_clients:
        client_id = client.get("id")
        LOGGER.debug("=== PROCESSING CLIENT MATCH ===")
        LOGGER.debug("Processing client: %s", client)
        if not client_id:
            LOGGER.debug("Client has no ID, skipping")
            continue
            
        LOGGER.debug("Processing client %s for athlete %s", client_id, athlete_name)
        
        # Get the Telegram link for this client
        link = links_cache.get(client_id) or get_link_by_client(client_id)
        LOGGER.debug("Retrieved link for client %s: %s", client_id, link)
        
        if not link:
            LOGGER.debug("Client %s is not linked to Telegram, skipping", client_id)
            continue
            
        tg_user_id = link.get("tg_user_id")
        if not tg_user_id:
            LOGGER.debug("Client %s has no Telegram user ID, skipping", client_id)
            continue
            
        LOGGER.debug("Client %s is linked to Telegram user %s", client_id, tg_user_id)
            
        # Send the message/file to the client via Telegram
        try:
            LOGGER.debug("Attempting to send to client %s (Telegram user %s)", client_id, tg_user_id)
            if temp_file and temp_file.exists():
                LOGGER.debug("Temp file exists: %s", temp_file)
                LOGGER.debug("Sending FIT file to client %s", client_id)
                filename = f"activity_{activity.get('id')}.fit"
                telegram_send_document(
                    krutilkavn_token,
                    str(tg_user_id),
                    temp_file,
                    filename,
                    caption=caption,
                    timeout=timeout,
                )
                LOGGER.info("Sent FIT file to client %s (Telegram user %s)", client_id, tg_user_id)
            else:
                LOGGER.debug("Sending activity info (no FIT file) to client %s", client_id)
                telegram_send_message(
                    krutilkavn_token,
                    str(tg_user_id),
                    caption,
                    timeout=timeout,
                )
                LOGGER.info("Sent activity info to client %s (Telegram user %s)", client_id, tg_user_id)
            sent_count += 1
        except requests.HTTPError as exc:
            LOGGER.warning("Failed to send to client %s (Telegram user %s): %s", client_id, tg_user_id, exc)
        except Exception as exc:
            LOGGER.exception("Unexpected error sending to client %s (Telegram user %s): %s", client_id, tg_user_id, exc)
        
        # Upload to Strava if the client has connected their Strava account
        # This is in a separate try-catch block to ensure Strava upload happens even if Telegram sending fails
        try:
            LOGGER.debug("=== STRAVA UPLOAD SECTION START ===")
            LOGGER.info("Checking Strava upload for client %s (Telegram user %s)", client_id, tg_user_id)
            
            # Log the current state before checking conditions
            LOGGER.debug("Strava upload pre-checks:")
            LOGGER.debug("  - temp_file parameter: %s", temp_file)
            LOGGER.debug("  - temp_file exists: %s", temp_file.exists() if temp_file else False)
            LOGGER.debug("  - link data: %s", link)

            straver_status = straver_statuses.get(int(tg_user_id)) if straver_statuses else {}
            activity_name = "КРУТИЛКА!"
            activity_description = format_strava_activity_description(
                activity,
                account_name,
                profile,
                scheduled_name=matched_client_name,
            )
            if temp_file and temp_file.exists() and straver_client.is_configured():
                LOGGER.debug("✓ Temp file exists for Strava upload for client %s", client_id)
                if straver_status.get("connected"):
                    LOGGER.info("Attempting Strava upload via Straver for client %s (Telegram user %s)", client_id, tg_user_id)
                    LOGGER.debug(
                        "Uploading activity to Straver for client %s: name=%s, description=%s, file=%s",
                        client_id,
                        activity_name,
                        activity_description,
                        temp_file,
                    )
                    upload_response = straver_client.upload_activity(
                        tg_user_id=int(tg_user_id),
                        file_path=temp_file,
                        name=activity_name,
                        description=activity_description,
                    )
                    
                    LOGGER.info(
                        "SUCCESS: Uploaded activity via Straver for client %s (Telegram user %s): %s",
                        client_id,
                        tg_user_id,
                        upload_response,
                    )
                    strava_uploaded_count += 1
                    LOGGER.debug("Strava upload counter incremented to: %d", strava_uploaded_count)
                else:
                    LOGGER.debug("✗ Strava integration not enabled on Straver for client %s", client_id)
            else:
                LOGGER.debug("✗ Skipping Straver upload for client %s: temp_file_exists=%s, straver_configured=%s", client_id, bool(temp_file and temp_file.exists()), straver_client.is_configured())

            # Intervals.icu upload
            try:
                if temp_file and temp_file.exists():
                    if intervals_sync.upload_activity(
                        tg_user_id=int(tg_user_id),
                        temp_file=temp_file,
                        description=activity_description,
                        activity_id=activity.get("id"),
                        timeout=STRAVER_HTTP_TIMEOUT,
                        activity_name=activity_name,
                    ):
                        intervals_uploaded_count += 1
            except Exception as exc:
                LOGGER.exception("FAILED: Intervals.icu upload failed for client %s (Telegram user %s): %s", client_id, tg_user_id, exc)
            LOGGER.debug("=== STRAVA UPLOAD SECTION END ===")
        except Exception as exc:
            LOGGER.exception("FAILED: Strava upload failed for client %s (Telegram user %s): %s", client_id, tg_user_id, exc)
    
    LOGGER.info("=== FINAL RESULTS ===")
    LOGGER.info("Sent activity information to %d matching clients", sent_count)
    LOGGER.info("Uploaded activities to Strava for %d clients", strava_uploaded_count)
    LOGGER.info("Uploaded activities to Intervals.icu for %d clients", intervals_uploaded_count)
    LOGGER.info("=== END OF CLIENT PROCESSING ===")

    return (
        sent_count > 0,
        strava_uploaded_count > 0,
        intervals_uploaded_count > 0,
        resolved_client_id,
        resolved_client_name,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    if not args.token:
        LOGGER.error(
            "Telegram bot token not provided (set KRUTILKAFIT_BOT_TOKEN or --token)",
        )
        return 2

    ensure_admin_table()
    ensure_activity_ids_table()  # Ensure our activity IDs table exists
    seed_admins_from_env(args.admins)
    admin_ids = get_admin_ids()
    if not admin_ids:
        LOGGER.error(
            "Администраторы не настроены. Добавьте их через /addadmin или переменную TELEGRAM_ADMIN_IDS."
        )
        return 2

    try:
        accounts = load_accounts(args.accounts)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to load accounts: %s", exc)
        return 2

    # Note: We're not using the state file anymore, but keeping it for backward compatibility
    # state = load_state(args.state)
    # state.setdefault("accounts", {})

    any_changes = False

    for account_id, account in accounts.items():
        LOGGER.info("Checking account %s", account.get("name", account_id))
        
        # Get known activity IDs from database instead of JSON file
        known_ids = set(get_seen_activity_ids_for_account(account_id))
        LOGGER.debug("Found %d previously seen activity IDs for account %s", len(known_ids), account_id)

        client = WattAttackClient(account["base_url"])
        try:
            client.login(account["email"], account["password"], timeout=args.timeout)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to login for %s", account_id)
            continue

        try:
            activities, metadata = client.fetch_activity_feed(
                limit=MAX_TRACKED_IDS,
                timeout=args.timeout,
            )
            LOGGER.debug(
                "Fetched %d activities for %s (strategy=%s)",
                len(activities),
                account_id,
                metadata.get("_pagination_strategy"),
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch activities for %s", account_id)
            continue

        try:
            profile = client.fetch_profile(timeout=args.timeout)
            if not isinstance(profile, dict):
                profile = {}
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch profile for %s: %s", account_id, exc)
            profile = {}

        try:
            auth_info = client.auth_check(timeout=args.timeout)
            if isinstance(auth_info, dict) and isinstance(auth_info.get("user"), dict):
                profile.setdefault("user", auth_info["user"])
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch auth info for %s: %s", account_id, exc)

        new_items: List[Dict[str, Any]] = []
        for activity in activities:
            activity_id = str(activity.get("id"))
            if activity_id and not was_activity_id_seen(account_id, activity_id):
                new_items.append(activity)

        if new_items:
            any_changes = True
            LOGGER.info("Found %d new activities for %s", len(new_items), account_id)
            if not args.dry_run:
                for activity in new_items:
                    (
                        processed,
                        matched_client_id,
                        matched_client_name,
                        start_dt,
                        profile_name,
                        sent_clientbot,
                        sent_strava,
                        sent_intervals,
                        fit_path,
                    ) = send_activity_fit(
                        account_id=account_id,
                        client=client,
                        activity=activity,
                        account_name=account.get("name", account_id),
                        account=account,
                        profile=profile,
                        token=args.token,
                        admin_ids=admin_ids,
                        timeout=args.timeout,
                    )
                    if processed:
                        distance = activity.get("distance")
                        elapsed_time = activity.get("elapsedTime")
                        elevation_gain = activity.get("totalElevationGain")
                        average_power = activity.get("averageWatts")
                        average_cadence = activity.get("averageCadence")
                        average_heartrate = activity.get("averageHeartrate")
                        record_seen_activity_id(
                            account_id,
                            str(activity.get("id")),
                            client_id=matched_client_id,
                            scheduled_name=matched_client_name,
                            start_time=start_dt,
                            profile_name=profile_name,
                            sent_clientbot=sent_clientbot,
                            sent_strava=sent_strava,
                            sent_intervals=sent_intervals,
                            fit_path=fit_path,
                            distance=distance,
                            elapsed_time=elapsed_time,
                            elevation_gain=elevation_gain,
                            average_power=average_power,
                            average_cadence=average_cadence,
                            average_heartrate=average_heartrate,
                        )
                    else:
                        LOGGER.info(
                            "Deferring activity %s for account %s until FIT appears",
                            activity.get("id"),
                            account_id,
                        )
        else:
            LOGGER.info("No new activities for %s", account_id)

    # Note: We're not saving state to JSON file anymore
    # if any_changes:
    #     save_state(args.state, state)
    # else:
    #     # still save to keep state in sync if first run
    #     if not args.state.exists():
    #         save_state(args.state, state)

    # Send workout reminders to clients
    if not args.dry_run:
        try:
            send_workout_reminders(timeout=args.timeout, reminder_hours=args.reminder_hours)
        except Exception:
            LOGGER.exception("Failed to send workout reminders")

    return 0


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(main())
