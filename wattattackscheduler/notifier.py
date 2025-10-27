#!/usr/bin/env python3
"""Check WattAttack accounts for new activities and ping Telegram admins."""
from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient
from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    get_admin_ids,
)

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
STATE_ENV = "WATTATTACK_STATE_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_STATE_PATH = Path("notifier_state.json")
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
MAX_TRACKED_IDS = int(os.environ.get("WATTATTACK_TRACKED_LIMIT", "200"))
DEFAULT_ADMIN_SEED = os.environ.get("TELEGRAM_ADMIN_IDS", "")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
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
        help="Path to JSON file used to remember previously seen activities",
    )
    parser.add_argument(
        "--admins",
        default=DEFAULT_ADMIN_SEED,
        help="Comma-separated admin IDs/юзернеймы для добавления в базу (опционально)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get(BOT_TOKEN_ENV, ""),
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
    if profile:
        athlete_name = extract_athlete_name(profile)
        if athlete_name:
            lines.append(f"Атлет: {athlete_name}")
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


def send_activity_fit(
    *,
    client: WattAttackClient,
    activity: Dict[str, Any],
    account_name: str,
    profile: Dict[str, Any],
    token: str,
    admin_ids: Sequence[int],
    timeout: float,
) -> None:
    fit_id = activity.get("fitFileId")
    caption = format_activity_meta(activity, account_name, profile)

    if not fit_id:
        LOGGER.info("Activity %s has no FIT file", activity.get("id"))
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
        return

    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            temp_file = Path(tmp.name)
        client.download_fit_file(str(fit_id), temp_file, timeout=timeout)
        filename = f"activity_{activity.get('id')}.fit"
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
    except Exception:
        LOGGER.exception("Failed to download/send FIT %s", fit_id)
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
    finally:
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
            except OSError:
                LOGGER.debug("Failed to remove temp file %s", temp_file)



def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    if not args.token:
        LOGGER.error("Telegram bot token not provided (set TELEGRAM_BOT_TOKEN or --token)")
        return 2

    ensure_admin_table()
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

    state = load_state(args.state)
    state.setdefault("accounts", {})

    any_changes = False

    for account_id, account in accounts.items():
        LOGGER.info("Checking account %s", account.get("name", account_id))
        known_ids = set(
            str(item)
            for item in state.get("accounts", {}).get(account_id, {}).get("known_ids", [])
        )

        client = WattAttackClient(account["base_url"])
        try:
            client.login(account["email"], account["password"], timeout=args.timeout)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to login for %s", account_id)
            continue

        try:
            payload = client.fetch_activities(timeout=args.timeout)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch activities for %s", account_id)
            continue

        activities = payload.get("activities", [])
        if not isinstance(activities, list):
            LOGGER.warning("Unexpected activities payload for %s", account_id)
            activities = []

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
            if activity_id and activity_id not in known_ids:
                new_items.append(activity)

        if new_items:
            any_changes = True
            LOGGER.info("Found %d new activities for %s", len(new_items), account_id)
            if not args.dry_run:
                for activity in new_items:
                    send_activity_fit(
                        client=client,
                        activity=activity,
                        account_name=account.get("name", account_id),
                        profile=profile,
                        token=args.token,
                        admin_ids=admin_ids,
                        timeout=args.timeout,
                    )
        else:
            LOGGER.info("No new activities for %s", account_id)

        updated_ids = [
            str(activity.get("id"))
            for activity in activities
            if activity.get("id") is not None
        ]
        updated_ids = updated_ids[:MAX_TRACKED_IDS]
        state["accounts"][account_id] = {"known_ids": updated_ids}

    if any_changes:
        save_state(args.state, state)
    else:
        # still save to keep state in sync if first run
        if not args.state.exists():
            save_state(args.state, state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
