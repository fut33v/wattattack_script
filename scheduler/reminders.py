from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

import requests
from zoneinfo import ZoneInfo

from repositories.schedule_repository import (
    ensure_workout_notifications_table,
    list_upcoming_reservations,
    record_notification_sent,
    was_notification_sent,
)
from repositories.client_link_repository import get_link_by_client
from repositories.client_repository import get_client
from scheduler.notifier_client import telegram_send_message

LOGGER = logging.getLogger(__name__)

CLIENTBOT_TOKEN_ENV = "KRUTILKAVN_BOT_TOKEN"
DEFAULT_REMINDER_HOURS = int(os.environ.get("WORKOUT_REMINDER_HOURS", "4"))
LOCAL_TIMEZONE = ZoneInfo(os.environ.get("WATTATTACK_LOCAL_TZ", "Europe/Moscow"))


def send_workout_reminders(
    *,
    timeout: float,
    reminder_hours: int = DEFAULT_REMINDER_HOURS,
    clientbot_token: Optional[str] = None,
) -> None:
    """Send workout reminders to clients via clientbot."""
    token = clientbot_token or os.environ.get(CLIENTBOT_TOKEN_ENV)
    if not token:
        LOGGER.info("%s not set, skipping workout reminders", CLIENTBOT_TOKEN_ENV)
        return

    try:
        ensure_workout_notifications_table()
    except Exception:
        LOGGER.exception("Failed to ensure workout notifications table")
        return

    now = datetime.now(tz=LOCAL_TIMEZONE)
    since = now + timedelta(hours=reminder_hours - 1)
    until = now + timedelta(hours=reminder_hours + 1)

    try:
        reservations = list_upcoming_reservations(since, until)
        LOGGER.info("Found %d upcoming reservations for workout reminders", len(reservations))
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch upcoming reservations: %s", exc)
        return

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

    client_reservations: Dict[int, List[Dict[str, object]]] = {}
    for reservation in unsent_reservations:
        client_id = reservation.get("client_id")
        if client_id:
            client_reservations.setdefault(client_id, []).append(reservation)

    sent_count = 0
    for client_id, reservations_for_client in client_reservations.items():
        try:
            client = get_client(client_id)
            if not client:
                LOGGER.warning("Client %s not found", client_id)
                continue

            link = get_link_by_client(client_id)
            if not link:
                LOGGER.debug("Client %s is not linked to Telegram", client_id)
                continue

            tg_user_id = link.get("tg_user_id")
            if not tg_user_id:
                LOGGER.debug("Client %s has no Telegram user ID", client_id)
                continue

            message = format_workout_reminder(client, reservations_for_client, reminder_hours)

            try:
                telegram_send_message(
                    token,
                    str(tg_user_id),
                    message,
                    timeout=timeout,
                    parse_mode="HTML",
                )

                for reservation in reservations_for_client:
                    reservation_id = reservation.get("id")
                    if reservation_id:
                        record_notification_sent(reservation_id, notification_type)

                LOGGER.info("Sent workout reminder to client %s (Telegram user %s)", client_id, tg_user_id)
                sent_count += len(reservations_for_client)
            except requests.HTTPError as exc:  # noqa: PERF203
                LOGGER.warning("Failed to send workout reminder to client %s: %s", client_id, exc)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Error processing workout reminder for client %s: %s", client_id, exc)

    LOGGER.info("Sent %d workout reminders", sent_count)


def format_workout_reminder(client: Dict[str, Any], reservations: List[Dict[str, Any]], reminder_hours: int) -> str:
    """Format a workout reminder message for a client."""
    first_name = client.get("first_name", "")
    last_name = client.get("last_name", "")
    full_name = client.get("full_name", "")

    if first_name and last_name:
        client_name = f"{first_name} {last_name}"
    elif full_name:
        client_name = full_name
    else:
        client_name = first_name or last_name or "Клиент"

    if len(reservations) == 1:
        reservation = reservations[0]
        slot_date = reservation.get("slot_date")
        start_time = reservation.get("start_time")
        end_time = reservation.get("end_time")

        if isinstance(slot_date, date):
            date_str = slot_date.strftime("%d.%m.%Y")
        else:
            date_str = str(slot_date)

        if isinstance(start_time, time):
            start_str = start_time.strftime("%H:%M")
        else:
            start_str = str(start_time)

        if isinstance(end_time, time):
            end_str = end_time.strftime("%H:%M")
        else:
            end_str = str(end_time)

        time_remaining_msg = "До встречи в «Крутилке»! 🚴‍♀️"

        session_kind = reservation.get("session_kind", "self_service")
        instructor_name = reservation.get("instructor_name", "")

        if session_kind == "instructor":
            if instructor_name:
                session_info = f"с инструктором {instructor_name}"
            else:
                session_info = "с инструктором"
        else:
            session_info = "самокрутка"

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
            f"{time_remaining_msg}"
        )
    else:
        message = f"👋 <b>{client_name}</b>, у вас запланированы следующие тренировки:\n\n"

        for reservation in reservations:
            slot_date = reservation.get("slot_date")
            start_time = reservation.get("start_time")

            if isinstance(slot_date, date):
                date_str = slot_date.strftime("%d.%m")
            else:
                date_str = str(slot_date)

            if isinstance(start_time, time):
                time_str = start_time.strftime("%H:%M")
            else:
                time_str = str(start_time)

            session_kind = reservation.get("session_kind", "self_service")
            instructor_name = reservation.get("instructor_name", "")

            if session_kind == "instructor":
                if instructor_name:
                    session_info = f"с инструктором {instructor_name}"
                else:
                    session_info = "с инструктором"
            else:
                session_info = "самокрутка"

            stand_code = reservation.get("stand_code", "")
            stand_title = reservation.get("stand_title", "")

            if stand_code and stand_title and stand_code != stand_title:
                stand_info = f"{stand_code} ({stand_title})"
            else:
                stand_info = stand_code or stand_title or "станок"

            message += (
                f"• {date_str} в {time_str} ({session_info}) — {stand_info}\n"
            )

        message += "\nДо встречи в «Крутилке»! 🚴‍♀️"

    return message
