#!/usr/bin/env python3
"""Check WattAttack accounts for new activities and ping Telegram admins."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timedelta, date, time, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from zoneinfo import ZoneInfo

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient
from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    get_admin_ids,
)
from repositories.schedule_repository import (
    list_upcoming_reservations,
    ensure_activity_ids_table,
    was_activity_id_seen,
    record_seen_activity_id,
    get_seen_activity_ids_for_account,
    record_account_assignment,
    was_account_assignment_done,
    record_assignment_notification,
    was_assignment_notification_sent,
    find_reservation_by_client_name,
    ensure_fit_files_dir,
    list_activities_missing_fit,
)
from repositories.client_repository import get_client, search_clients

# Import the send_to_matching_clients function from notifier_client
from scheduler.notifier_client import (
    FIT_WAIT_SECONDS,
    MATCH_GRACE_MINUTES,
    parse_activity_start_dt,
    resolve_scheduled_client,
    send_to_matching_clients,
    should_wait_for_fit_file,
)
from scheduler import reminders
from scheduler import intervals_plan
from scheduler import intervals_upload
from scheduler import accounts as accounts_utils
from wattattack_profiles import apply_client_profile as apply_wattattack_profile

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "KRUTILKAFIT_BOT_TOKEN"
BOT_TOKEN_FALLBACK_ENV = "TELEGRAM_BOT_TOKEN"
KRUTILKAVN_BOT_TOKEN_ENV = "KRUTILKAVN_BOT_TOKEN"
STATE_ENV = "WATTATTACK_STATE_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_STATE_PATH = Path("notifier_state.json")
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
MAX_TRACKED_IDS = int(os.environ.get("WATTATTACK_TRACKED_LIMIT", "200"))
DEFAULT_ADMIN_SEED = os.environ.get("TELEGRAM_ADMIN_IDS", "")
DEFAULT_ASSIGN_LEAD_MINUTES = int(os.environ.get("WATTATTACK_ASSIGN_LEAD_MINUTES", "20"))
DEFAULT_ASSIGN_WINDOW_MINUTES = int(os.environ.get("WATTATTACK_ASSIGN_WINDOW_MINUTES", "10"))
ASSIGN_ENABLE = os.environ.get("WATTATTACK_ASSIGN_ENABLED", "false").lower() in {"1", "true", "yes"}
LOCAL_TIMEZONE = ZoneInfo(os.environ.get("WATTATTACK_LOCAL_TZ", "Europe/Moscow"))
DEV_BUILD = os.environ.get("DEV_BUILD", "").lower() in {"1", "true", "yes"}
LAST_ASSIGNMENT_NOTIFICATION_KEYS: set[tuple[str, int]] = set()
LAST_ASSIGNMENT_NOTIFICATION_STATUS: Optional[str] = None


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


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger to use the local timezone when standalone."""

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
        default=Path(os.environ.get(accounts_utils.ACCOUNTS_ENV, accounts_utils.DEFAULT_ACCOUNTS_PATH)),
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
        default=reminders.DEFAULT_REMINDER_HOURS,
        help="Hours before workout to send reminder (default: 4)",
    )
    parser.add_argument(
        "--assign-lead-minutes",
        type=int,
        default=DEFAULT_ASSIGN_LEAD_MINUTES,
        help="Minutes before slot start to set WattAttack accounts from the schedule (0 disables)",
    )
    parser.add_argument(
        "--assign-window-minutes",
        type=int,
        default=DEFAULT_ASSIGN_WINDOW_MINUTES,
        help="Window length in minutes when scanning slots for automatic assignments",
    )
    return parser.parse_args(argv)


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


def assign_clients_to_accounts(
    *,
    accounts: Dict[str, Dict[str, Any]],
    lead_minutes: int,
    window_minutes: int,
    timeout: float,
    dry_run: bool,
    admin_ids: Sequence[int],
    bot_token: str,
) -> None:
    """Apply client data to WattAttack accounts ahead of the scheduled slot."""

    if lead_minutes <= 0 or window_minutes <= 0:
        LOGGER.debug("Auto-assignment disabled (lead=%s, window=%s)", lead_minutes, window_minutes)
        return

    notification_status_default = "observed" if dry_run or not ASSIGN_ENABLE or DEV_BUILD else "applied"

    stand_accounts: Dict[int, Dict[str, Any]] = {}
    for account_id, account in accounts.items():
        for stand_id in account.get("stand_ids") or []:
            if stand_id in stand_accounts:
                LOGGER.warning(
                    "Stand %s already mapped to %s, overriding with %s",
                    stand_id,
                    stand_accounts[stand_id]["id"],
                    account_id,
                )
            stand_accounts[stand_id] = {**account, "id": account_id}

    if not stand_accounts:
        LOGGER.debug("No stand mappings in accounts file, skipping auto-assignment")
        return

    now = datetime.now(tz=LOCAL_TIMEZONE)
    window_start = now + timedelta(minutes=lead_minutes)
    window_end = window_start + timedelta(minutes=window_minutes)

    try:
        reservations = list_upcoming_reservations(window_start, window_end)
    except Exception:
        LOGGER.exception("Failed to load reservations for auto-assignment window")
        return

    if not reservations:
        LOGGER.debug(
            "No reservations scheduled between %s and %s for auto-assignment",
            window_start,
            window_end,
        )
        return

    notifications: List[Dict[str, Any]] = []
    applied = 0
    for reservation in reservations:
        account = None
        stand_id = reservation.get("stand_id")
        if not isinstance(stand_id, int):
            continue
        account = stand_accounts.get(stand_id)
        if not account:
            continue

        reservation_id = reservation.get("id")
        client_id = reservation.get("client_id")
        if not reservation_id or not client_id:
            continue
        account_id = account["id"]
        if was_account_assignment_done(reservation_id, account_id):
            LOGGER.debug(
                "Reservation %s already applied to account %s, skipping",
                reservation_id,
                account_id,
            )
            continue

        notification_status = notification_status_default
        if was_assignment_notification_sent(reservation_id, account_id, notification_status):
            LOGGER.debug(
                "Notification already sent for reservation %s to account %s with status %s",
                reservation_id,
                account_id,
                notification_status,
            )
            continue

        try:
            client = get_client(client_id)
        except Exception:
            LOGGER.exception("Failed to load client %s for reservation %s", client_id, reservation_id)
            continue

        if not client:
            LOGGER.warning(
                "Reservation %s references missing client %s; cannot assign to %s",
                reservation_id,
                client_id,
                account_id,
            )
            continue

        client_name = client.get("full_name") or f"ID {client_id}"
        stand_label = reservation.get("stand_code") or reservation.get("stand_display_name") or f"Stand {stand_id}"
        slot_date = reservation.get("slot_date")
        start_time = reservation.get("start_time")
        date_str = slot_date.strftime("%d.%m") if isinstance(slot_date, date) else str(slot_date)
        time_str = start_time.strftime("%H:%M") if isinstance(start_time, time) else str(start_time)
        account_label = account.get("name") or account_id

        if dry_run:
            LOGGER.info(
                "[dry-run] Would assign client %s (%s) to account %s for %s %s (%s)",
                client_name,
                client_id,
                account_label,
                date_str,
                time_str,
                stand_label,
            )
            continue

        if ASSIGN_ENABLE and not dry_run and not DEV_BUILD:
            try:
                apply_wattattack_profile(
                    account_id=account_id,
                    account_label=account_label,
                    email=account["email"],
                    password=account["password"],
                    base_url=account.get("base_url"),
                    client_record=client,
                    timeout=timeout,
                )
                record_account_assignment(reservation_id, account_id, client_id)
                LOGGER.info(
                    "Assigned client %s (%s) to account %s for %s %s (%s)",
                    client_name,
                    client_id,
                    account_label,
                    date_str,
                    time_str,
                    stand_label,
                )
            except Exception:
                LOGGER.exception(
                    "Failed to apply client %s (%s) to account %s (reservation %s)",
                    client_name,
                    client_id,
                account_id,
                reservation_id,
            )
        applied += 1
        notifications.append(
            {
                "reservation_id": reservation_id,
                "account_label": account_label,
                "account_id": account_id,
                "client_name": client_name,
                "client_id": client_id,
                "slot_label": f"{date_str} {time_str}",
                "stand_label": stand_label,
            }
        )

    if applied:
        LOGGER.info("Applied %s client(s) to WattAttack accounts for upcoming slots", applied)
    else:
        LOGGER.debug("No auto-assignments were performed in this cycle")

    if notification_status_default == "observed" and DEV_BUILD:
        status_note = "наблюдаем (DEV_BUILD)"
    elif notification_status_default == "observed":
        status_note = "наблюдаем"
    else:
        status_note = "назначили"
    notification_keys = {(row["account_id"], row["reservation_id"]) for row in notifications}
    global LAST_ASSIGNMENT_NOTIFICATION_KEYS, LAST_ASSIGNMENT_NOTIFICATION_STATUS
    if (
        notifications
        and notification_keys == LAST_ASSIGNMENT_NOTIFICATION_KEYS
        and status_note == LAST_ASSIGNMENT_NOTIFICATION_STATUS
    ):
        LOGGER.debug(
            "Assignment notification already sent for %d reservations, skipping duplicate",
            len(notification_keys),
        )
        return

    if notifications and bot_token and admin_ids:
        header = f"🕘 Ближайшие смены ({status_note}):"
        lines = []
        for row in notifications:
            line = (
                f"{row['account_label']}: {row['client_name']} (ID {row['client_id']}) "
                f"— {row['slot_label']}, {row['stand_label']}"
            )
            lines.append(line)
        message = f"{header}\n" + "\n".join(lines)
        for chat_id in admin_ids:
            try:
                telegram_send_message(
                    bot_token,
                    str(chat_id),
                    message,
                    timeout=timeout,
                )
            except requests.HTTPError:
                LOGGER.warning("Failed to notify admin %s about assignments list", chat_id)
        for row in notifications:
            try:
                record_assignment_notification(
                    row["reservation_id"],
                    row["account_id"],
                    notification_status_default,
                )
            except Exception:
                LOGGER.exception(
                    "Failed to record assignment notification for reservation %s, account %s",
                    row["reservation_id"],
                    row["account_id"],
                )
    LAST_ASSIGNMENT_NOTIFICATION_KEYS = notification_keys
    LAST_ASSIGNMENT_NOTIFICATION_STATUS = status_note


def send_activity_fit(
    *,
    account_id: str,
    client: WattAttackClient,
    activity: Dict[str, Any],
    account_name: str,
    account: Dict[str, Any],
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
    final_client_id = matched_client_id
    
    # Get clientbot token for sending to clients
    krutilkavn_token = os.environ.get(KRUTILKAVN_BOT_TOKEN_ENV)
    
    if not fit_id:
        should_wait, age_seconds = should_wait_for_fit_file(activity)
        if should_wait:
            LOGGER.info(
                "Activity %s has no FIT yet (age=%.0fs < %ss), will retry later",
                activity.get("id"),
                age_seconds or 0,
                FIT_WAIT_SECONDS,
            )
            return (
                False,
                final_client_id,
                matched_client_name,
                start_dt,
                profile_name,
                sent_clientbot,
                sent_strava,
                sent_intervals,
                fit_path,
            )

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
        return (
            True,
            final_client_id,
            matched_client_name,
            start_dt,
            profile_name,
            sent_clientbot,
            sent_strava,
            sent_intervals,
            fit_path,
        )

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
        fit_path,
    )


def backfill_missing_fit_files(
    *,
    account_id: str,
    client: WattAttackClient,
    activities: Sequence[Dict[str, Any]],
    account_name: Optional[str],
    profile: Dict[str, Any],
    clientbot_token: Optional[str],
    timeout: float,
    max_missing: int = 200,
) -> int:
    """
    Attempt to download FIT files for activities that are already stored but missing fit_path.

    Returns the number of FIT files successfully downloaded or linked.
    """

    missing = list_activities_missing_fit(account_id, limit=max_missing)
    if not missing:
        return 0

    LOGGER.info("%s: ищем FIT для %d активностей без файла", account_id, len(missing))

    activity_map = {
        str(item.get("id")): item for item in activities if isinstance(item, dict) and item.get("id") is not None
    }

    if any(str(item.get("activity_id")) not in activity_map for item in missing):
        try:
            extra_activities, _ = client.fetch_activity_feed(limit=2000, timeout=timeout)
            for item in extra_activities:
                key = item.get("id")
                if key is not None:
                    activity_map[str(key)] = item
        except Exception:  # noqa: BLE001
            LOGGER.warning("%s: не удалось обновить ленту для поиска FIT", account_id)

    downloaded = 0
    base_dir = ensure_fit_files_dir()

    for row in missing:
        activity_id = str(row.get("activity_id"))
        scheduled_client_id = row.get("manual_client_id") or row.get("client_id")
        scheduled_client_name = row.get("manual_client_name") or row.get("scheduled_name")
        activity = activity_map.get(activity_id)
        if not activity:
            LOGGER.info("%s: активность %s не найдена в ленте, пропускаем", account_id, activity_id)
            continue

        fit_id = activity.get("fitFileId")
        if not fit_id:
            LOGGER.info("%s: у активности %s нет fitFileId, пропускаем", account_id, activity_id)
            continue

        dest_dir = base_dir / account_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / f"{activity_id}.fit"

        if not dest_file.exists():
            try:
                client.download_fit_file(str(fit_id), dest_file, timeout=timeout)
            except Exception:  # noqa: BLE001
                LOGGER.warning("%s: ошибка скачивания FIT %s для активности %s", account_id, fit_id, activity_id)
                dest_file.unlink(missing_ok=True)
                continue

        fit_path = None
        if dest_file.exists():
            fit_path = f"/fitfiles/{account_id}/{activity_id}.fit"
            record_seen_activity_id(account_id, activity_id, fit_path=fit_path)
            downloaded += 1

        needs_delivery = (
            not row.get("sent_clientbot")
            or not row.get("sent_strava")
            or not row.get("sent_intervals")
        )
        if not needs_delivery or not dest_file.exists():
            continue

        caption = format_activity_meta(activity, account_name, profile, scheduled_client_name)
        try:
            sent_clientbot, sent_strava, sent_intervals, resolved_client_id, resolved_client_name = (
                send_to_matching_clients(
                    activity,
                    profile,
                    caption,
                    clientbot_token or "",
                    timeout,
                    dest_file,
                    account_name,
                    scheduled_client_id,
                    scheduled_client_name,
                )
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("%s: не удалось отправить восстановленную активность %s", account_id, activity_id)
            continue

        final_client_id = resolved_client_id or scheduled_client_id
        final_client_name = resolved_client_name or scheduled_client_name

        record_seen_activity_id(
            account_id,
            activity_id,
            client_id=final_client_id,
            scheduled_name=final_client_name,
            fit_path=fit_path,
            sent_clientbot=sent_clientbot,
            sent_strava=sent_strava,
            sent_intervals=sent_intervals,
            start_time=row.get("start_time"),
            profile_name=row.get("profile_name"),
        )

    if downloaded:
        LOGGER.info("%s: скачано %d FIT-файлов из пропущенных", account_id, downloaded)
    else:
        LOGGER.info("%s: нет скачанных FIT-файлов из пропущенных", account_id)

    return downloaded


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
        accounts = accounts_utils.load_accounts(args.accounts)
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
        profile_name = extract_athlete_name(profile) if profile else None

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

        if not args.dry_run:
            try:
                recovered = backfill_missing_fit_files(
                    account_id=account_id,
                    client=client,
                    activities=activities,
                    account_name=account.get("name", account_id),
                    profile=profile,
                    clientbot_token=os.environ.get(KRUTILKAVN_BOT_TOKEN_ENV),
                    timeout=args.timeout,
                )
                if recovered:
                    any_changes = True
            except Exception:  # noqa: BLE001
                LOGGER.exception("Failed to backfill FIT files for %s", account_id)

    try:
        assign_clients_to_accounts(
            accounts=accounts,
            lead_minutes=args.assign_lead_minutes,
            window_minutes=args.assign_window_minutes,
            timeout=args.timeout,
            dry_run=args.dry_run,
            admin_ids=admin_ids,
            bot_token=args.token,
        )
    except Exception:
        LOGGER.exception("Failed to process automatic WattAttack account assignments")

    # Send workout reminders to clients
    if not args.dry_run:
        try:
            reminders.send_workout_reminders(
                timeout=args.timeout,
                reminder_hours=args.reminder_hours,
                clientbot_token=os.environ.get(KRUTILKAVN_BOT_TOKEN_ENV),
            )
        except Exception:
            LOGGER.exception("Failed to send workout reminders")

        # Send Intervals.icu plan for the next 7 days to linked users via client bot
        krutilkavn_token = os.environ.get(KRUTILKAVN_BOT_TOKEN_ENV)
        try:
            intervals_plan.notify_week_plan(
                bot_token=krutilkavn_token or "",
                timeout=args.timeout,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to send Intervals.icu weekly plan notifications")

        # Upload Intervals.icu planned workouts to WattAttack accounts
        try:
            intervals_upload.sync_intervals_workouts(
                accounts=accounts,
                bot_token=args.token,
                timeout=args.timeout,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to sync Intervals.icu workouts to WattAttack")

    return 0


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(main())
