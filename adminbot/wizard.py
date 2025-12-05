"""Wizard flow for quick WattAttack account assignments."""
from __future__ import annotations

import asyncio
import html
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from repositories.client_repository import get_client
from repositories.schedule_repository import (
    get_slot_with_reservations,
    list_upcoming_reservations,
    record_account_assignment,
    create_reservation,
    get_reservation_for_stand,
    update_reservation,
    was_account_assignment_done,
)
from repositories.trainers_repository import get_trainer, list_trainers
from repositories.client_repository import search_clients
from wattattack_profiles import apply_client_profile as apply_wattattack_profile
from adminbot.accounts import AccountConfig

LOGGER = logging.getLogger(__name__)

LOOKAHEAD_HOURS = int(os.environ.get("ADMINBOT_WIZARD_LOOKAHEAD_HOURS", "12"))
LOOKAHEAD_DAYS = int(os.environ.get("ADMINBOT_WIZARD_LOOKAHEAD_DAYS", "7"))
LOOKBACK_HOURS = int(os.environ.get("ADMINBOT_WIZARD_LOOKBACK_HOURS", "3"))
MIN_SLOTS_LIMIT = 6
SLOTS_LIMIT = max(
    MIN_SLOTS_LIMIT,
    int(os.environ.get("ADMINBOT_WIZARD_SLOTS_LIMIT", str(MIN_SLOTS_LIMIT))),
)
WIZARD_SEARCH_RESULTS_LIMIT = 8

PEDAL_CODE_TO_LABEL = {
    "platform": "топталки (под кроссовки)",
    "road_look": "контакты шоссе Look",
    "road_shimano": "контакты шоссе Shimano",
    "mtb_shimano": "контакты MTB Shimano",
    "own": "принесу свои",
}
PEDAL_LABELS = {label for label in PEDAL_CODE_TO_LABEL.values()}
PLATFORM_ALIASES = {"platform", "топталки", "топталки (под кроссовки)"}


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


def _normalize_pedals_label(raw: Optional[Any]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    lower = text.lower()
    if lower in PEDAL_CODE_TO_LABEL:
        return PEDAL_CODE_TO_LABEL[lower]
    for label in PEDAL_LABELS:
        if lower == label.lower():
            return label
    return text


def _format_pedals(record: Optional[Dict[str, Any]]) -> str:
    label = _normalize_pedals_label(record.get("pedals") if isinstance(record, dict) else None)
    return label or "—"


def _has_platform_pedals(pedals_label: str) -> bool:
    normalized = pedals_label.strip().lower()
    if not normalized:
        return True
    if normalized in PLATFORM_ALIASES:
        return True
    return normalized.startswith("топталки")


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


def _free_stands_for_slot(slot: Dict[str, Any]) -> List[int]:
    """Return stand ids available for new bookings."""

    try:
        trainers = list_trainers()
    except Exception:
        LOGGER.exception("Failed to list trainers for wizard free-stands")
        trainers = []
    trainer_ids = [t.get("id") for t in trainers if isinstance(t.get("id"), int)]
    reserved = {
        reservation["stand_id"]
        for reservation in _valid_reservations(slot.get("reservations") or [])
        if isinstance(reservation.get("stand_id"), int)
    }
    placeholders = [
        reservation["stand_id"]
        for reservation in slot.get("reservations", [])
        if isinstance(reservation.get("stand_id"), int) and reservation.get("client_id") is None
    ]
    free_from_placeholders = sorted(
        stand_id for stand_id in placeholders if stand_id not in reserved
    )
    free_from_inventory = sorted(
        stand_id for stand_id in trainer_ids if stand_id not in reserved
    )
    combined: List[int] = []
    seen: set[int] = set()
    for stand_id in free_from_placeholders + free_from_inventory:
        if stand_id in seen:
            continue
        combined.append(stand_id)
        seen.add(stand_id)
    return combined


def _stand_number_emoji(stand_id: int, trainers: Iterable[Dict[str, Any]]) -> str:
    """Return emoji number for a stand position in trainer list (fallback to id)."""

    trainer_ids: List[int] = [t.get("id") for t in trainers if isinstance(t.get("id"), int)]
    trainer_ids.sort()
    try:
        idx = trainer_ids.index(stand_id) + 1
        return _format_digit_emoji(idx)
    except ValueError:
        return _format_digit_emoji(stand_id)


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
    window_end = now + timedelta(days=LOOKAHEAD_DAYS)

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


def _assign_client_to_slot(slot_id: int, stand_id: int, client_id: int) -> Dict[str, Any]:
    """Assign a client to a stand within a slot, creating a reservation if needed."""

    existing = get_reservation_for_stand(slot_id, stand_id)
    if existing:
        update_reservation(existing["id"], client_id=client_id, status="booked")
        return get_reservation_for_stand(slot_id, stand_id) or existing

    created = create_reservation(
        slot_id=slot_id,
        stand_id=stand_id,
        stand_code=None,
        client_id=client_id,
        client_name=None,
        status="booked",
        source="wizard-add",
        notes=None,
    )
    return created


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

    reservations.sort(
        key=lambda reservation: (
            not isinstance(reservation.get("stand_id"), int),
            reservation.get("stand_id") or 0,
        )
    )
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
    non_platform_clients: List[str] = []
    free_stands = _free_stands_for_slot(slot)

    for idx, reservation in enumerate(reservations, start=1):
        client_id = reservation.get("client_id")
        client_record = clients.get(client_id)
        client_label = _format_client_short(client_record)
        client_height = client_record.get("height") if isinstance(client_record, dict) else None
        client_ftp = client_record.get("ftp") if isinstance(client_record, dict) else None
        height_part = ""
        try:
            if client_height not in (None, ""):
                height_part = f"{float(client_height):g} см"
        except (TypeError, ValueError):
            height_part = ""
        pedals_label = _format_pedals(client_record)
        has_non_platform_pedals = pedals_label != "—" and not _has_platform_pedals(pedals_label)
        ftp_part = ""
        try:
            if client_ftp not in (None, ""):
                ftp_part = f"{int(float(client_ftp))} FTP"
        except (TypeError, ValueError):
            ftp_part = ""
        extras_parts = [part for part in [height_part, ftp_part] if part]
        extras = f" — {', '.join(extras_parts)}" if extras_parts else ""
        stand_label = _format_stand_label(reservation, trainers)
        account = stand_accounts.get(reservation.get("stand_id"))
        prefix = _format_digit_emoji(idx)
        lines.append(
            f"{prefix} {html.escape(client_label)}{extras}"
        )
        if account:
            assignable += 1
        if has_non_platform_pedals:
            non_platform_clients.append(
                f"{prefix} {html.escape(_format_client_short(client_record))}: {html.escape(pedals_label)}"
            )

    if non_platform_clients:
        lines.append("")
        lines.append("⚙️ Педали отличные от топталок:")
        lines.extend(non_platform_clients)

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
    buttons.insert(
        0,
        [
            InlineKeyboardButton(
                text="↔️ Пересадить",
                callback_data=f"wizard|swap|{slot_id}",
            )
        ],
    )
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
    if free_stands:
        buttons.insert(
            0,
            [
                InlineKeyboardButton(
                    text="➕ Записать клиента",
                    callback_data=f"wizard|add|{slot_id}",
                )
            ],
        )

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _render_swap_from(
    *,
    query,
    slot_id: int,
    timezone,
    title: str = "Выберите кого пересадить:",
) -> None:
    try:
        slot = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for swap", slot_id)
        await query.edit_message_text(f"❌ Не удалось загрузить слот: {exc}")
        return

    if not slot:
        await query.edit_message_text("🔍 Слот не найден или удалён.")
        return

    reservations = _valid_reservations(slot.get("reservations") or [])
    reservations.sort(
        key=lambda reservation: (
            not isinstance(reservation.get("stand_id"), int),
            reservation.get("stand_id") or 0,
        )
    )
    stand_ids = [
        reservation["stand_id"]
        for reservation in reservations
        if isinstance(reservation.get("stand_id"), int)
    ]
    if not stand_ids:
        await query.edit_message_text(
            "ℹ️ Нет записей со станками, пересаживать некого.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(text="↩️ К слоту", callback_data=f"wizard|slot|{slot_id}")]]
            ),
        )
        return

    client_ids = {
        reservation["client_id"]
        for reservation in reservations
        if isinstance(reservation.get("client_id"), int)
    }
    clients = await _load_clients(client_ids)
    trainers = await _load_trainers(stand_ids)

    lines = [f"🗓 Слот: {html.escape(_format_slot_summary(slot))}", title, ""]
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for idx, reservation in enumerate(reservations, start=1):
        stand_id = reservation.get("stand_id")
        if not isinstance(stand_id, int):
            continue
        client_record = clients.get(reservation.get("client_id"))
        client_label = _format_client_short(client_record)
        stand_label = _format_stand_label(reservation, trainers)
        prefix = _format_digit_emoji(idx)
        lines.append(f"{prefix} {html.escape(stand_label)} — {html.escape(client_label)}")
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=f"{prefix} {client_label}",
                    callback_data=f"wizard|swapfrom|{slot_id}|{stand_id}",
                )
            ]
        )

    keyboard_rows.append(
        [InlineKeyboardButton(text="↩️ К слоту", callback_data=f"wizard|slot|{slot_id}")]
    )

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def _render_swap_to(
    *,
    query,
    slot_id: int,
    stand_from: int,
    timezone,
) -> None:
    try:
        slot = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load slot %s for swap target", slot_id)
        await query.edit_message_text(f"❌ Не удалось загрузить слот: {exc}")
        return

    if not slot:
        await query.edit_message_text("🔍 Слот не найден или удалён.")
        return

    reservations = _valid_reservations(slot.get("reservations") or [])
    reservations.sort(
        key=lambda reservation: (
            not isinstance(reservation.get("stand_id"), int),
            reservation.get("stand_id") or 0,
        )
    )
    client_ids = {
        reservation["client_id"]
        for reservation in reservations
        if isinstance(reservation.get("client_id"), int)
    }
    clients = await _load_clients(client_ids)
    trainers = await _load_trainers(
        {
            reservation["stand_id"]
            for reservation in reservations
            if isinstance(reservation.get("stand_id"), int)
        }
    )

    target_rows: List[List[InlineKeyboardButton]] = []
    lines = [
        f"🗓 Слот: {html.escape(_format_slot_summary(slot))}",
        "Выберите, на какой станок пересадить:",
        "",
    ]
    for idx, reservation in enumerate(reservations, start=1):
        stand_id = reservation.get("stand_id")
        if not isinstance(stand_id, int):
            continue
        client_label = _format_client_short(clients.get(reservation.get("client_id")))
        stand_label = _format_stand_label(reservation, trainers)
        prefix = _format_digit_emoji(idx)
        marker = " (отсюда)" if stand_id == stand_from else ""
        lines.append(f"{prefix} {html.escape(stand_label)} — {html.escape(client_label)}{marker}")
        if stand_id == stand_from:
            continue
        target_rows.append(
            [
                InlineKeyboardButton(
                    text=f"{prefix} {stand_label}",
                    callback_data=f"wizard|swapto|{slot_id}|{stand_from}|{stand_id}",
                )
            ]
        )

    target_rows.append(
        [InlineKeyboardButton(text="↩️ Назад", callback_data=f"wizard|swap|{slot_id}")]
    )

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(target_rows),
    )


def _swap_reservations_slot_stands(slot_id: int, stand_a: int, stand_b: int) -> Tuple[str, str]:
    slot = get_slot_with_reservations(slot_id)
    if not slot:
        raise ValueError("Слот не найден")
    reservations = _valid_reservations(slot.get("reservations") or [])
    stand_map = {
        reservation.get("stand_id"): reservation
        for reservation in reservations
        if isinstance(reservation.get("stand_id"), int)
    }
    if stand_a not in stand_map or stand_b not in stand_map:
        raise ValueError("Не найден один из станков в слоте")
    res_a = stand_map[stand_a]
    res_b = stand_map[stand_b]
    # Free stand A, assign stand B to A, then A to B to avoid unique constraint collisions
    update_reservation(res_a["id"], stand_id=None)
    update_reservation(res_b["id"], stand_id=stand_a)
    update_reservation(res_a["id"], stand_id=stand_b)
    client_a = res_a.get("client_id")
    client_b = res_b.get("client_id")
    return str(client_a or ""), str(client_b or "")


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
    if action == "swap" and len(parts) >= 3:
        try:
            slot_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный слот.")
            return True
        await _render_swap_from(query=query, slot_id=slot_id, timezone=timezone)
        return True
    if action == "swapfrom" and len(parts) >= 4:
        try:
            slot_id = int(parts[2])
            stand_from = int(parts[3])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный выбор.")
            return True
        await _render_swap_to(query=query, slot_id=slot_id, stand_from=stand_from, timezone=timezone)
        return True
    if action == "swapto" and len(parts) >= 5:
        try:
            slot_id = int(parts[2])
            stand_from = int(parts[3])
            stand_to = int(parts[4])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный выбор.")
            return True
        try:
            client_a, client_b = await asyncio.to_thread(
                _swap_reservations_slot_stands, slot_id, stand_from, stand_to
            )
            await query.answer("Пересадили", show_alert=False)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Swap failed for slot %s: %s -> %s", slot_id, stand_from, stand_to)
            await query.edit_message_text(f"❌ Не удалось пересадить: {exc}")
            return True
        await _send_slot_detail(
            query=query,
            slot_id=slot_id,
            accounts=accounts,
            timezone=timezone,
        )
        return True
    if action == "add" and len(parts) >= 3:
        try:
            slot_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный слот.")
            return True
        slot = await asyncio.to_thread(get_slot_with_reservations, slot_id)
        if not slot:
            await query.edit_message_text("🔍 Слот не найден или удалён.")
            return True
        free_stands = _free_stands_for_slot(slot)
        if not free_stands:
            await query.answer("Нет свободных станков", show_alert=True)
            return True
        context.user_data["wizard_add"] = {"slot_id": slot_id}
        await query.message.reply_text(
            "✏️ Введите фамилию или часть имени, чтобы добавить клиента в слот.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(text="↩️ К слоту", callback_data=f"wizard|slot|{slot_id}")]]
            ),
        )
        return True
    if action == "addpick" and len(parts) >= 5:
        try:
            slot_id = int(parts[2])
            client_id = int(parts[3])
            stand_id = int(parts[4])
        except ValueError:
            await query.edit_message_text("⚠️ Некорректный выбор.")
            return True
        try:
            await asyncio.to_thread(_assign_client_to_slot, slot_id, stand_id, client_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to add client %s to slot %s stand %s", client_id, slot_id, stand_id)
            await query.edit_message_text(f"❌ Не удалось добавить клиента: {exc}")
            return True
        context.user_data.pop("wizard_add", None)
        await query.answer("Добавлен", show_alert=False)
        await _send_slot_detail(query=query, slot_id=slot_id, accounts=accounts, timezone=timezone)
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


async def handle_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    accounts: Mapping[str, AccountConfig],
    timezone,
) -> bool:
    """Handle text input for wizard add flow."""

    message = update.effective_message
    if message is None or message.text is None:
        return False

    add_ctx = context.user_data.get("wizard_add")
    if not isinstance(add_ctx, dict):
        return False

    slot_id = add_ctx.get("slot_id")
    if not isinstance(slot_id, int):
        return False

    term = message.text.strip()
    if not term:
        await message.reply_text("⚠️ Введите фамилию или имя для поиска.")
        return True

    try:
        slot = await asyncio.to_thread(get_slot_with_reservations, slot_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to reload slot %s for add flow", slot_id)
        await message.reply_text(f"❌ Не удалось загрузить слот: {exc}")
        return True

    if not slot:
        await message.reply_text("🔍 Слот не найден или удалён.")
        context.user_data.pop("wizard_add", None)
        return True

    free_stands = _free_stands_for_slot(slot)
    if not free_stands:
        await message.reply_text("ℹ️ Нет свободных станков в этом слоте.")
        context.user_data.pop("wizard_add", None)
        return True

    stand_id = free_stands[0]
    try:
        trainers = list_trainers()
    except Exception:
        trainers = []
    stand_number_emoji = _stand_number_emoji(stand_id, trainers)

    results = search_clients(term, limit=WIZARD_SEARCH_RESULTS_LIMIT)
    if not results:
        await message.reply_text("Ничего не найдено. Попробуйте другую фамилию.")
        return True

    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for client in results:
        label = _format_client_short(client)
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"wizard|addpick|{slot_id}|{client.get('id')}|{stand_id}",
                )
            ]
        )
    keyboard_rows.append(
        [InlineKeyboardButton(text="↩️ К слоту", callback_data=f"wizard|slot|{slot_id}")]
    )

    await message.reply_text(
        f"Свободный станок: {stand_number_emoji}. Записать клиента:",
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )
    return True
