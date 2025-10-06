#!/usr/bin/env python3
"""Check WattAttack accounts for new activities and ping Telegram admins."""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import requests

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
STATE_ENV = "WATTATTACK_STATE_FILE"
ADMINS_ENV = "TELEGRAM_ADMIN_IDS"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_STATE_PATH = Path("notifier_state.json")
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
MAX_TRACKED_IDS = int(os.environ.get("WATTATTACK_TRACKED_LIMIT", "200"))


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
        default=os.environ.get(ADMINS_ENV, ""),
        help="Comma-separated Telegram chat IDs that receive notifications",
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
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file is corrupted, starting fresh: %s", path)
        return {}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_admin_ids(raw: str) -> Sequence[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


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


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    if not args.token:
        LOGGER.error("Telegram bot token not provided (set TELEGRAM_BOT_TOKEN or --token)")
        return 2

    admin_ids = parse_admin_ids(args.admins)
    if not admin_ids:
        LOGGER.error("Admin chat IDs not provided (set TELEGRAM_ADMIN_IDS or --admins)")
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

        def worker() -> List[Dict[str, Any]]:
            client = WattAttackClient(account["base_url"])
            client.login(account["email"], account["password"], timeout=args.timeout)
            payload = client.fetch_activities(timeout=args.timeout)
            return payload.get("activities", [])

        try:
            activities = worker()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch activities for %s", account_id)
            continue

        new_items: List[Dict[str, Any]] = []
        for activity in activities:
            activity_id = str(activity.get("id"))
            if activity_id and activity_id not in known_ids:
                new_items.append(activity)

        if new_items:
            any_changes = True
            message = (
                f"<b>Новые активности для {account.get('name', account_id)}</b>"\
                + "\n\n"\
                + "\n\n".join(format_activity(item) for item in new_items)
            )
            LOGGER.info("Found %d new activities for %s", len(new_items), account_id)
            if not args.dry_run:
                for chat_id in admin_ids:
                    try:
                        telegram_send_message(
                            args.token,
                            chat_id,
                            message,
                            timeout=args.timeout,
                        )
                    except requests.HTTPError:
                        # already logged inside telegram_send_message
                        pass
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
