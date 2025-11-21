"""Wizard flow for quick WattAttack account assignments."""
from __future__ import annotations

import asyncio
import html
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from repositories.client_repository import get_client
from repositories.schedule_repository import (
    get_slot_with_reservations,
    list_upcoming_reservations,
    record_account_assignment,
    was_account_assignment_done,
)
from repositories.trainers_repository import get_trainer
from wattattack_profiles import apply_client_profile as apply_wattattack_profile
from adminbot.accounts import AccountConfig

LOGGER = logging.getLogger(__name__)

LOOKAHEAD_HOURS = int(os.environ.get("ADMINBOT_WIZARD_LOOKAHEAD_HOURS", "12"))
LOOKBACK_HOURS = int(os.environ.get("ADMINBOT_WIZARD_LOOKBACK_HOURS", "3"))
SLOTS_LIMIT = max(3, int(os.environ.get("ADMINBOT_WIZARD_SLOTS_LIMIT", "8")))


def _local_now(tz) -> datetime:
    return datetime.now(tz=tz)


def _format_time(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        value = value.time()
    try:
        return value.strftime("%H:%M")  # type: ignore[union-attr]
    except Exception:
        return str(value)


def _format_date_range(start: datetime, end: datetime) -> str:
    same_day = start.date() == end.date()
    if same_day:
        return f"{start.strftime('%d.%m')} {start.strftime('%H:%M')}–{end.strftime('%H:%M')}"
    return f"{start.strftime('%d.%m %H:%M')} – {end.strftime('%d.%m %H:%M')}"


def _format_slot_summary(slot: Dict[str, Any]) -> str:
    slot_date = slot.get("slot_date")
    start_time = slot.get("start_time")
    end_time = slot.get("end_time")
    date_part = slot_date.strftime("%d.%m") if hasattr(slot_date, "strftime") else str(slot_date)
    time_part = f"{_format_time(start_time)}–{_format_time(end_time)}"
    title = str(slot.get("label") or "").strip()
    session_kind = (slot.get("session_kind") or "").lower()
    instructor = str(slot.get("instructor_name") or "").strip()
    descriptor = title
    if not descriptor and session_kind == "instructor":
        descriptor = f"инструктор {instructor}" if instructor else "с инструктором"
    parts = [date_part, time_part]
    if descriptor:
        parts.append(descriptor)
    return " · ".join(parts)


def _format_client_label(record: Optional[Dict[str, Any]], fallback: str = "") -> str:
    if not record:
        return fallback or "Клиент не найден"
    first = record.get("first_name")
    last = record.get("last_name")
    full_name = " ".join(part for part in [first, last] if part).strip()
    if not full_name:
        full_name = record.get("full_name") or fallback or "Без имени"
    return f"{full_name} (ID {record.get('id')})"


def _format_client_short(record: Optional[Dict[str, Any]]) -> str:
    if not record:
        return "Без имени"
    first = record.get("first_name")
    last = record.get("last_name")
    full_name = " ".join(part for part in [first, last] if part).strip()
    if not full_name:
        full_name = record.get("full_name") or "Без имени"
    return full_name


def _format_slot_button_label(slot: Dict[str, Any]) -> str:
    slot_date = slot.get("slot_date")
    start_time = slot.get("start_time")
    end_time = slot.get("end_time")
    date_part = slot_date.strftime("%d.%m") if hasattr(slot_date, "strftime") else str(slot_date)
    time_part = f"{_format_time(start_time)}–{_format_time(end_time)}"
    return f"{date_part} {time_part}"


def _format_digit_emoji(value: int) -> str:
    mapping = {
        0: "0️⃣",
        1: "1️⃣",
        2: "2️⃣",
        3: "3️⃣",
        4: "4️⃣",
        5: "5️⃣",
        6: "6️⃣",
        7: "7️⃣",
        8: "8️⃣",
        9: "9️⃣",
        10: "🔟",
    }
    return mapping.get(value, f"{value}.")


def _build_stand_account_map(accounts: Mapping[str, AccountConfig]) -> Dict[int, AccountConfig]:
    mapping: Dict[int, AccountConfig] = {}
    for account in accounts.values():
        for stand_id in account.stand_ids:
            if stand_id in mapping:
                LOGGER.warning(
                    "Stand %s already mapped to %s, overriding with %s for wizard",
                    stand_id,
                    mapping[stand_id].identifier,
                    account.identifier,
                )
            mapping[stand_id] = account
    return mapping


def _format_stand_label(reservation: Dict[str, Any], trainers: Mapping[int, Dict[str, Any]]) -> str:
    stand_id = reservation.get("stand_id")
    trainer = trainers.get(stand_id)
    if trainer:
        code = trainer.get("code") or trainer.get("display_name") or trainer.get("title")
        return code or f"Станок {stand_id}"
    stand_code = reservation.get("stand_code")
    if stand_code:
        return str(stand_code)
    if stand_id:
        return f"Станок {stand_id}"
    return "Станок"


def _valid_reservations(reservations: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    usable: List[Dict[str, Any]] = []
    for reservation in reservations:
        status = str(reservation.get("status") or "").lower()
        if status in {"available", "cancelled", "legacy", "blocked"}:
            continue
        if reservation.get("client_id") is None:
            continue
        usable.append(reservation)
    return usable


async def _load_trainers(stand_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    trainers: Dict[int, Dict[str, Any]] = {}
    for stand_id in stand_ids:
        try:
            trainer = await asyncio.to_thread(get_trainer, stand_id)
            if trainer:
                trainers[stand_id] = trainer
        except Exception:
            LOGGER.exception("Failed to load trainer %s for wizard", stand_id)
    return trainers


async def _load_clients(client_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    clients: Dict[int, Dict[str, Any]] = {}
    for client_id in client_ids:
        try:
            client = await asyncio.to_thread(get_client, client_id)
            if client:
                clients[client_id] = client
        except Exception:
            LOGGER.exception("Failed to load client %s for wizard", client_id)
    return clients


async def _render_slots_overview(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    accounts: Mapping[str, AccountConfig],
    timezone,
    query=None,
) -> None:
    message = query.message if query is not None else update.effective_message
    if message is None:
        return

    now = _local_now(timezone)
    window_start = now - timedelta(hours=LOOKBACK_HOURS)
    window_end = now + timedelta(hours=LOOKAHEAD_HOURS)

    try:
        reservations = await asyncio.to_thread(
            list_upcoming_reservations,
            window_start,
            window_end,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load reservations for wizard overview")
        if query:
            await query.edit_message_text(f"❌ Не удалось получить расписание: {exc}")
        else:
            await message.reply_text(f"❌ Не удалось получить расписание: {exc}")
        return

    grouped: Dict[int, Dict[str, Any]] = {}
    for reservation in reservations:
        reservation_slot_id = reservation.get("slot_id")
        if reservation_slot_id is None:
            continue
        bucket = grouped.setdefault(
            reservation_slot_id,
            {
                "slot_id": reservation_slot_id,
                "slot_date": reservation.get("slot_date"),
                "start_time": reservation.get("start_time"),
                "end_time": reservation.get("end_time"),
                "label": reservation.get("label"),
                "session_kind": reservation.get("session_kind"),
                "instructor_name": reservation.get("instructor_name"),
                "reservations": [],
            },
        )
        bucket["reservations"].append(reservation)

    slots: List[Dict[str, Any]] = list(grouped.values())
    slots.sort(
        key=lambda slot: (
            slot.get("slot_date") or window_end.date(),
            slot.get("start_time") or window_end.time(),
        )
    )
    slots = slots[:SLOTS_LIMIT]

    lines = [
        "🧙‍♂️ Выберите слот, чтобы увидеть список посетителей и посадить их на аккаунты.",
        f"Окно: {html.escape(_format_date_range(window_start, window_end))}",
    ]
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    stand_accounts = _build_stand_account_map(accounts)

    if not slots:
        lines.append("ℹ️ Нет ближайших записей в выбранном окне.")
    else:
        for slot in slots:
            entries = _valid_reservations(slot.get("reservations", []))
            planned = len(entries)
            accounted = sum(
                1
                for reservation in entries
                if (
                    isinstance(reservation.get("stand_id"), int)
                    and reservation["stand_id"] in stand_accounts
                )
            )
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{_format_slot_button_label(slot)} ({planned}/{accounted})",
                        callback_data=f"wizard|slot|{slot['slot_id']}",
                    )
                ]
            )

    keyboard_rows.append(
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="wizard|refresh")]
    )

    if query:
        await query.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )
    else:
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )


def _combine_slot_start(slot: Dict[str, Any], timezone) -> datetime:
    slot_date = slot.get("slot_date")
    start_time = slot.get("start_time")
    if slot_date is None or start_time is None:
        return _local_now(timezone)
    combined = datetime.combine(slot_date, start_time)
    return combined.replace(tzinfo=timezone)


async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    accounts: Mapping[str, AccountConfig],
    timezone,
) -> None:
    await _render_slots_overview(update=update, context=context, accounts=accounts, timezone=timezone)


async def _send_slot_detail(
    *,
    query,
    slot_id: int,
    accounts: Mapping[str, AccountConfig],
    timezone,
) -> None:
    try:
        slot = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for wizard", slot_id)
        await query.edit_message_text(f"❌ Не удалось загрузить слот: {exc}")
        return

    if not slot:
        await query.edit_message_text("🔍 Слот не найден или удалён.")
        return

    reservations = _valid_reservations(slot.get("reservations") or [])
    if not reservations:
        await query.edit_message_text(
            "ℹ️ В этом слоте нет активных записей.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(text="↩️ К слотам", callback_data="wizard|refresh")]]
            ),
        )
        return

    stand_ids = {
        reservation["stand_id"]
        for reservation in reservations
        if isinstance(reservation.get("stand_id"), int)
    }
    client_ids = {
        reservation["client_id"]
        for reservation in reservations
        if isinstance(reservation.get("client_id"), int)
    }

    trainers = await _load_trainers(stand_ids)
    clients = await _load_clients(client_ids)
    stand_accounts = _build_stand_account_map(accounts)

    lines: List[str] = [f"🗓 Слот: {html.escape(_format_slot_summary(slot))}", ""]
    assignable = 0

    for idx, reservation in enumerate(reservations, start=1):
        client_id = reservation.get("client_id")
        client_record = clients.get(client_id)
        client_label = _format_client_short(client_record)
        client_height = client_record.get("height") if isinstance(client_record, dict) else None
        client_ftp = client_record.get("ftp") if isinstance(client_record, dict) else None
        height_part = ""
        ftp_part = ""
        try:
            if client_height not in (None, ""):
                height_part = f", {float(client_height):g} см"
        except (TypeError, ValueError):
            height_part = ""
        try:
            if client_ftp not in (None, ""):
                ftp_part = f", {int(float(client_ftp))} FTP"
        except (TypeError, ValueError):
            ftp_part = ""
        stand_label = _format_stand_label(reservation, trainers)
        account = stand_accounts.get(reservation.get("stand_id"))
        prefix = _format_digit_emoji(idx)
        lines.append(
            f"{prefix} {html.escape(stand_label)} — {html.escape(client_label)}{height_part}{ftp_part}"
        )
        if account:
            assignable += 1

    buttons = [
        [
            InlineKeyboardButton(
                text="↩️ К слотам",
                callback_data="wizard|refresh",
            ),
            InlineKeyboardButton(
                text="🔄 Обновить",
                callback_data=f"wizard|slot|{slot_id}",
            ),
        ]
    ]
    if assignable:
        buttons.insert(
            0,
            [
                InlineKeyboardButton(
                    text="🚀 Посадить всех на аккаунты",
                    callback_data=f"wizard|apply|{slot_id}",
                )
            ],
        )

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _apply_slot_accounts(
    *,
    query,
    slot_id: int,
    accounts: Mapping[str, AccountConfig],
    default_timeout: float,
    default_ftp: int,
) -> None:
    try:
        slot = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s before apply", slot_id)
        await query.edit_message_text(f"❌ Не удалось загрузить слот: {exc}")
        return

    if not slot:
        await query.edit_message_text("🔍 Слот не найден или удалён.")
        return

    reservations = _valid_reservations(slot.get("reservations") or [])
    if not reservations:
        await query.edit_message_text("ℹ️ В этом слоте нет активных записей.")
        return

    stand_accounts = _build_stand_account_map(accounts)
    trainers = await _load_trainers(
        {
            reservation["stand_id"]
            for reservation in reservations
            if isinstance(reservation.get("stand_id"), int)
        }
    )
    clients = await _load_clients(
        {
            reservation["client_id"]
            for reservation in reservations
            if isinstance(reservation.get("client_id"), int)
        }
    )

    successes: List[str] = []
    failures: List[str] = []
    skipped: List[str] = []

    for reservation in reservations:
        reservation_id = reservation.get("id")
        client_id = reservation.get("client_id")
        stand_id = reservation.get("stand_id")
        account = stand_accounts.get(stand_id)

        if not account:
            skipped.append(
                f"{_format_stand_label(reservation, trainers)} — нет маппинга аккаунта"
            )
            continue
        if not isinstance(client_id, int):
            skipped.append(f"{_format_stand_label(reservation, trainers)} — нет клиента")
            continue

        account_id = account.identifier
        already_applied = await asyncio.to_thread(
            was_account_assignment_done, reservation_id, account_id
        )
        if already_applied:
            skipped.append(
                f"{account.name}: {_format_client_short(clients.get(client_id))} уже посажен"
            )
            continue

        client_record = clients.get(client_id)
        if not client_record:
            skipped.append(
                f"{account.name}: {_format_stand_label(reservation, trainers)} — клиент не найден"
            )
            continue

        def worker() -> None:
            apply_wattattack_profile(
                account_id=account_id,
                account_label=account.name,
                email=account.email,
                password=account.password,
                base_url=account.base_url,
                client_record=client_record,
                timeout=default_timeout,
                default_ftp=default_ftp,
            )
            record_account_assignment(reservation_id, account_id, client_id)

        try:
            await asyncio.to_thread(worker)
            successes.append(
                f"{account.name}: {_format_client_short(client_record)} → {_format_stand_label(reservation, trainers)}"
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception(
                "Failed to apply client %s to account %s for slot %s", client_id, account_id, slot_id
            )
            failures.append(f"{account.name}: {_format_client_short(client_record)} — {exc}")

    lines: List[str] = [f"🧙‍♂️ Посадка на {html.escape(_format_slot_summary(slot))}"]
    if successes:
        lines.append("✅ Успешно:")
        lines.extend(html.escape(item) for item in successes)
    if failures:
        lines.append("")
        lines.append("❌ Ошибки:")
        lines.extend(html.escape(item) for item in failures)
    if skipped:
        lines.append("")
        lines.append("ℹ️ Пропущено:")
        lines.extend(html.escape(item) for item in skipped)

    buttons = [
        [
            InlineKeyboardButton(
                text="↩️ К слотам",
                callback_data="wizard|refresh",
            ),
            InlineKeyboardButton(
                text="🔄 Обновить слот",
                callback_data=f"wizard|slot|{slot_id}",
            ),
        ]
    ]

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    accounts: Mapping[str, AccountConfig],
    timezone,
    default_timeout: float,
    default_ftp: int,
) -> bool:
    query = update.callback_query
    if not query or not query.data:
        return False

    parts = query.data.split("|")
    if not parts or parts[0] != "wizard":
        return False

    action = parts[1] if len(parts) > 1 else ""
    if action == "refresh":
        await _render_slots_overview(
            update=update, context=context, accounts=accounts, timezone=timezone, query=query
        )
        return True
    if action == "slot" and len(parts) >= 3:
        try:
            slot_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный слот.")
            return True
        await _send_slot_detail(
            query=query,
            slot_id=slot_id,
            accounts=accounts,
            timezone=timezone,
        )
        return True
    if action == "apply" and len(parts) >= 3:
        try:
            slot_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный слот.")
            return True
        await _apply_slot_accounts(
            query=query,
            slot_id=slot_id,
            accounts=accounts,
            default_timeout=default_timeout,
            default_ftp=default_ftp,
        )
        return True

    return False
